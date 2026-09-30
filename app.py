#!/usr/bin/env python3
"""Pharmaceutical batch deviation, rework and release decision service."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sqlite3
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

DB_PATH = Path(__file__).with_name("data.db")

# 放行依据清单：basis 名称 -> (表名, 业务字段, 中文名)
BASIS_SPECS: dict[str, tuple[str, tuple[str, ...], str]] = {
    "deviation": ("deviations", ("severity", "title", "due_at", "status", "corrective_action",
                                 "exception_reason", "exception_until", "exception_approved_by"), "偏差"),
    "test": ("tests", ("test_type", "result", "spec_min", "spec_max", "passed", "round"), "检验"),
    "rework": ("rework", ("description", "status", "completed_by", "completed_at"), "返工"),
    "supplier_change": ("supplier_changes", ("supplier", "change_type", "description"), "供应商变更"),
    "stability": ("stability", ("condition", "timepoint", "result", "spec_limit", "passed"), "稳定性"),
}

FIELD_LABELS = {
    "severity": "等级", "title": "标题", "due_at": "截止日期", "status": "状态",
    "corrective_action": "纠正措施", "exception_reason": "例外原因",
    "exception_until": "例外有效期", "exception_approved_by": "例外批准人",
    "test_type": "检验项目", "result": "结果", "spec_min": "下限", "spec_max": "上限",
    "passed": "合格判定", "round": "轮次", "description": "描述", "completed_by": "完成人",
    "completed_at": "完成时间", "supplier": "供应商", "change_type": "变更类型",
    "condition": "条件", "timepoint": "时间点", "spec_limit": "限度",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def after_now(value: str | None = None) -> bool:
    if not value:
        return False
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) > datetime.now(timezone.utc)
    except ValueError:
        return False


def j(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def canonical(value: object) -> str:
    return hashlib.sha256(j(value).encode("utf-8")).hexdigest()


class ApiError(Exception):
    def __init__(self, status: int, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status, self.message, self.extra = status, message, extra or {}


class Store:
    def __init__(self, path: str | Path = DB_PATH):
        self.path = str(path)
        # autocommit 模式，事务由 tx() 显式管理；全局锁保证多线程共用一个连接时串行化
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._tx_depth = 0
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.init_schema()

    def init_schema(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS factories (
          id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, country TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS batches (
          id INTEGER PRIMARY KEY AUTOINCREMENT, factory_id INTEGER NOT NULL REFERENCES factories(id),
          batch_no TEXT NOT NULL, product TEXT NOT NULL, mfg_date TEXT NOT NULL, expiry_date TEXT NOT NULL,
          state TEXT NOT NULL CHECK(state IN ('manufactured','investigation','awaiting_resample','conditional','released','rejected','awaiting_review')),
          revision INTEGER NOT NULL DEFAULT 1, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          UNIQUE(factory_id,batch_no)
        );
        CREATE TABLE IF NOT EXISTS deviations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          severity TEXT NOT NULL CHECK(severity IN ('critical','minor')), title TEXT NOT NULL, due_at TEXT,
          status TEXT NOT NULL CHECK(status IN ('open','closed')), corrective_action TEXT,
          exception_reason TEXT, exception_until TEXT, exception_approved_by TEXT,
          closed_by TEXT, closed_at TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tests (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          test_type TEXT NOT NULL, result REAL NOT NULL, spec_min REAL NOT NULL, spec_max REAL NOT NULL,
          passed INTEGER NOT NULL, round INTEGER NOT NULL DEFAULT 1, recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rework (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          description TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('planned','completed')),
          created_by TEXT NOT NULL, created_at TEXT NOT NULL, completed_by TEXT, completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS supplier_changes (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          supplier TEXT NOT NULL, change_type TEXT NOT NULL, description TEXT NOT NULL,
          recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS stability (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          condition TEXT NOT NULL, timepoint TEXT NOT NULL, result REAL NOT NULL, spec_limit REAL NOT NULL,
          passed INTEGER NOT NULL, recorded_by TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS decisions (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id), revision INTEGER NOT NULL,
          decision TEXT NOT NULL CHECK(decision IN ('release','reject','conditional','resample')), rationale TEXT NOT NULL,
          exception_code TEXT, decided_by TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(batch_id,revision)
        );
        CREATE TABLE IF NOT EXISTS release_credentials (
          id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL REFERENCES batches(id),
          credential_no TEXT NOT NULL UNIQUE, batch_revision INTEGER NOT NULL,
          decision TEXT NOT NULL CHECK(decision IN ('release','conditional')), rationale TEXT NOT NULL,
          exception_code TEXT, status TEXT NOT NULL CHECK(status IN ('active','invalidated','superseded')),
          issued_by TEXT NOT NULL, issued_at TEXT NOT NULL, invalidated_at TEXT,
          UNIQUE(batch_id,batch_revision)
        );
        CREATE TABLE IF NOT EXISTS credential_snapshots (
          credential_id INTEGER NOT NULL REFERENCES release_credentials(id) ON DELETE CASCADE,
          basis TEXT NOT NULL, item_id INTEGER NOT NULL, item_hash TEXT NOT NULL, snapshot_json TEXT NOT NULL,
          PRIMARY KEY(credential_id,basis,item_id)
        );
        CREATE TABLE IF NOT EXISTS invalidation_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT, credential_id INTEGER NOT NULL REFERENCES release_credentials(id),
          batch_id INTEGER NOT NULL, at TEXT NOT NULL, triggered_by TEXT NOT NULL, trigger_action TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS invalidation_reasons (
          id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL REFERENCES invalidation_events(id) ON DELETE CASCADE,
          code TEXT NOT NULL, detail TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_credential_active_per_batch
          ON release_credentials(batch_id) WHERE status='active';
        CREATE TABLE IF NOT EXISTS idempotent_requests (
          request_id TEXT PRIMARY KEY, actor TEXT NOT NULL, route TEXT NOT NULL, body_hash TEXT NOT NULL,
          status_code INTEGER NOT NULL, response_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
          entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, details_json TEXT NOT NULL
        );
        """)

    @contextlib.contextmanager
    def tx(self):
        """整笔事务：可重入；任一步失败（含审计写入）全部回滚。"""
        with self.lock:
            started = self._tx_depth == 0
            if started:
                self.conn.execute("BEGIN IMMEDIATE")
            self._tx_depth += 1
            try:
                yield
            except BaseException:
                self._tx_depth -= 1
                if started:
                    self.conn.execute("ROLLBACK")
                raise
            self._tx_depth -= 1
            if started:
                self.conn.commit()

    def audit(self, actor: str, action: str, entity_type: str, entity_id: object, details: dict) -> None:
        self.conn.execute("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,details_json) VALUES(?,?,?,?,?,?)",
                          (now(), actor, action, entity_type, str(entity_id), j(details)))

    def save_idempotent(self, request_id: str, actor: str, route: str, body_hash: str, response: dict) -> None:
        self.conn.execute("""INSERT INTO idempotent_requests(request_id,actor,route,body_hash,status_code,response_json,created_at)
                             VALUES(?,?,?,?,200,?,?)""",
                          (request_id, actor, route, body_hash, j(response), now()))

    def get_idempotent(self, request_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM idempotent_requests WHERE request_id=?", (request_id,)).fetchone()

    def close(self) -> None:
        with self.lock:
            self.conn.close()


class BatchService:
    def __init__(self, store: Store): self.store, self.conn = store, store.conn

    @staticmethod
    def _actor(actor: str | None, role: str | None, allowed: set[str]) -> str:
        if not actor: raise ApiError(401, "缺少身份")
        if role not in allowed: raise ApiError(403, "角色无权执行此操作")
        return actor

    def _row(self, table: str, identity: int) -> sqlite3.Row:
        row = self.conn.execute(f"SELECT * FROM {table} WHERE id=?", (identity,)).fetchone()
        if not row: raise ApiError(404, "对象不存在")
        return row

    def _factory_check(self, actor: str, factory_id: int, batch: sqlite3.Row | None = None) -> None:
        factory = self.conn.execute("SELECT * FROM factories WHERE id=?", (factory_id,)).fetchone()
        if not factory: raise ApiError(404, "工厂不存在")
        if batch is not None and int(batch["factory_id"]) != int(factory_id):
            raise ApiError(403, "不能修改其他工厂的批次")

    # ------------------------------------------------------------------ 依据快照

    def _basis_manifest(self, batch_id: int) -> dict[str, list[tuple[int, str, dict]]]:
        """当前批次五类放行依据的内容指纹，用于签发冻结与失效比对。"""
        manifest: dict[str, list[tuple[int, str, dict]]] = {}
        for basis, (table, fields, _label) in BASIS_SPECS.items():
            items = []
            for row in self.conn.execute(f"SELECT * FROM {table} WHERE batch_id=? ORDER BY id", (batch_id,)):
                content = {field: row[field] for field in fields}
                items.append((row["id"], canonical(content), content))
            manifest[basis] = items
        return manifest

    @staticmethod
    def _describe_item(basis: str, content: dict) -> str:
        if basis == "deviation":
            return f"偏差「{content['title']}」（{content['severity']}，状态 {content['status']}）"
        if basis == "test":
            verdict = "合格" if content["passed"] else "不合格"
            return f"检验 {content['test_type']} 第{content['round']}轮 结果 {content['result']}（{verdict}）"
        if basis == "rework":
            return f"返工「{content['description']}」（状态 {content['status']}）"
        if basis == "supplier_change":
            return f"供应商变更 {content['supplier']}/{content['change_type']}"
        return f"稳定性 {content['condition']} {content['timepoint']} 结果 {content['result']}" + (
            "（合格）" if content["passed"] else "（不合格）")

    def _credential_diff(self, credential: sqlite3.Row) -> list[tuple[str, str]]:
        """比对冻结快照与当前清单，返回 (原因代码, 中文说明) 列表。"""
        snapshots: dict[tuple[str, int], tuple[str, dict]] = {}
        for row in self.conn.execute("SELECT * FROM credential_snapshots WHERE credential_id=?", (credential["id"],)):
            snapshots[(row["basis"], row["item_id"])] = (row["item_hash"], json.loads(row["snapshot_json"]))
        reasons: list[tuple[str, str]] = []
        for basis, items in self._basis_manifest(credential["batch_id"]).items():
            label = BASIS_SPECS[basis][2]
            for item_id, item_hash, content in items:
                key = (basis, item_id)
                if key not in snapshots:
                    reasons.append((f"{basis}.added", f"新增{label}：{self._describe_item(basis, content)}"))
                    continue
                old_hash, old_content = snapshots[key]
                if old_hash != item_hash:
                    changed = [f"{FIELD_LABELS.get(k, k)} {old_content[k]!r}→{content[k]!r}"
                               for k in content if old_content.get(k) != content.get(k)]
                    reasons.append((f"{basis}.changed",
                                    f"{label} #{item_id} 被更正：{'、'.join(changed)}"))
            snap_ids = {item_id for (b, item_id) in snapshots if b == basis}
            for missing in sorted(snap_ids - {item_id for item_id, _, _ in items}):
                reasons.append((f"{basis}.removed", f"{label} #{missing} 被删除"))
        return reasons

    def _active_credential(self, batch_id: int) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM release_credentials WHERE batch_id=? AND status='active'",
                                 (batch_id,)).fetchone()

    def _record_invalidation(self, credential: sqlite3.Row, batch_id: int, actor: str,
                             trigger_action: str, reasons: list[tuple[str, str]]) -> int:
        stamp = now()
        cur = self.conn.execute("""INSERT INTO invalidation_events(credential_id,batch_id,at,triggered_by,trigger_action)
                                   VALUES(?,?,?,?,?)""",
                                (credential["id"], batch_id, stamp, actor, trigger_action))
        event_id = cur.lastrowid
        self.conn.executemany("INSERT INTO invalidation_reasons(event_id,code,detail) VALUES(?,?,?)",
                              [(event_id, code, detail) for code, detail in reasons])
        return event_id

    def _invalidate_for_evidence(self, batch_id: int, actor: str, trigger_action: str) -> None:
        """依据补录/更正后：冻结快照对不上则凭据失效，批次退回待复核。"""
        credential = self._active_credential(batch_id)
        if not credential:
            return
        batch = self._row("batches", batch_id)
        reasons = self._credential_diff(credential)
        if int(batch["revision"]) != int(credential["batch_revision"]):
            reasons.insert(0, ("revision.changed",
                               f"批次修订号已从冻结的 {credential['batch_revision']} 变更为 {batch['revision']}"))
        if not reasons:
            return
        self._record_invalidation(credential, batch_id, actor, trigger_action, reasons)
        self.conn.execute("UPDATE release_credentials SET status='invalidated',invalidated_at=? WHERE id=? AND status='active'",
                          (now(), credential["id"]))
        self.conn.execute("UPDATE batches SET state='awaiting_review' WHERE id=?", (batch_id,))
        self.store.audit(actor, "credential.invalidate", "release_credential", credential["id"],
                         {"batch_id": batch_id, "credential_no": credential["credential_no"],
                          "trigger_action": trigger_action, "reasons": [detail for _, detail in reasons]})

    def _close_active_credential(self, batch_id: int, actor: str, trigger_action: str,
                                 reason_code: str, detail: str) -> None:
        """新决定取代旧凭据（如再次放行、拒收）。"""
        credential = self._active_credential(batch_id)
        if not credential:
            return
        self._record_invalidation(credential, batch_id, actor, trigger_action, [(reason_code, detail)])
        self.conn.execute("UPDATE release_credentials SET status='superseded',invalidated_at=? WHERE id=?",
                          (now(), credential["id"]))

    # ------------------------------------------------------------------ 登记与录入

    def register_factory(self, actor: str | None, role: str | None, code: str, name: str, country: str) -> dict:
        actor = self._actor(actor, role, {"qa"})
        if not code or not name: raise ApiError(400, "工厂代号和名称不能为空")
        with self.store.tx():
            try:
                cur = self.conn.execute("INSERT INTO factories(code,name,country) VALUES(?,?,?)", (code, name, country))
                self.store.audit(actor, "factory.register", "factory", cur.lastrowid, {"code": code})
            except sqlite3.IntegrityError as exc: raise ApiError(409, "工厂代号已存在") from exc
            return {"id": cur.lastrowid, "code": code, "name": name, "country": country}

    def create_batch(self, actor: str | None, role: str | None, factory_id: int, batch_no: str, product: str, mfg_date: str, expiry_date: str) -> dict:
        actor = self._actor(actor, role, {"operator"})
        with self.store.tx():
            self._factory_check(actor, factory_id)
            if not batch_no.strip() or not product.strip() or expiry_date <= mfg_date: raise ApiError(400, "批号、产品或有效期不合法")
            stamp = now()
            try:
                cur = self.conn.execute("""INSERT INTO batches(factory_id,batch_no,product,mfg_date,expiry_date,state,created_by,created_at,updated_at)
                                         VALUES(?,?,?,?,?, 'manufactured',?,?,?)""",
                                        (factory_id, batch_no, product, mfg_date, expiry_date, actor, stamp, stamp))
                self.store.audit(actor, "batch.create", "batch", cur.lastrowid, {"factory_id": factory_id, "batch_no": batch_no})
            except sqlite3.IntegrityError as exc: raise ApiError(409, "该工厂批号已存在") from exc
            return self._batch_dict(self._row("batches", cur.lastrowid))

    def add_deviation(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, severity: str, title: str, due_at: str | None, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator", "inspector"})
        if severity not in {"critical", "minor"} or not title.strip(): raise ApiError(400, "偏差等级或描述不合法")
        with self.store.tx():
            batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不能新增偏差")
            cur = self.conn.execute("""INSERT INTO deviations(batch_id,severity,title,due_at,status,created_by,created_at)
                                     VALUES(?,?,?,?,'open',?,?)""", (batch_id, severity, title, due_at, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self.store.audit(actor, "deviation.open", "deviation", cur.lastrowid, {"batch_id": batch_id, "severity": severity})
            self._invalidate_for_evidence(batch_id, actor, "deviation.open")
            return self._deviation_dict(self._row("deviations", cur.lastrowid))

    def close_deviation(self, actor: str | None, role: str | None, deviation_id: int, corrective_action: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"qa"})
        if not corrective_action.strip(): raise ApiError(400, "必须填写纠正措施")
        with self.store.tx():
            deviation = self._row("deviations", deviation_id); batch = self._row("batches", deviation["batch_id"])
            if deviation["status"] != "open": raise ApiError(409, "偏差已经关闭")
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不可修改")
            self.conn.execute("UPDATE deviations SET status='closed',corrective_action=?,closed_by=?,closed_at=? WHERE id=? AND status='open'",
                              (corrective_action, actor, now(), deviation_id))
            self._advance_batch(batch["id"], expected_revision, "investigation")
            self.store.audit(actor, "deviation.close", "deviation", deviation_id, {"batch_id": batch["id"], "corrective_action": corrective_action})
            self._invalidate_for_evidence(batch["id"], actor, "deviation.close")
            return self._deviation_dict(self._row("deviations", deviation_id))

    def approve_exception(self, actor: str | None, role: str | None, deviation_id: int, reason: str, until: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"qa"})
        with self.store.tx():
            deviation = self._row("deviations", deviation_id); batch = self._row("batches", deviation["batch_id"])
            if deviation["severity"] == "critical": raise ApiError(409, "关键偏差不允许例外批准")
            if deviation["status"] != "open" or not reason.strip() or not after_now(until): raise ApiError(400, "例外原因或有效期不合法")
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不可修改")
            self.conn.execute("UPDATE deviations SET exception_reason=?,exception_until=?,exception_approved_by=? WHERE id=?", (reason, until, actor, deviation_id))
            self._advance_batch(batch["id"], expected_revision, batch["state"])
            self.store.audit(actor, "deviation.exception", "deviation", deviation_id, {"batch_id": batch["id"], "reason": reason, "until": until})
            self._invalidate_for_evidence(batch["id"], actor, "deviation.exception")
            return self._deviation_dict(self._row("deviations", deviation_id))

    def record_test(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, test_type: str, result: float, spec_min: float, spec_max: float, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"lab"})
        if not test_type.strip() or spec_min > spec_max: raise ApiError(400, "检验项目或标准不合法")
        with self.store.tx():
            batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不能补录检验")
            round_no = self.conn.execute("SELECT COALESCE(MAX(round),0)+1 FROM tests WHERE batch_id=? AND test_type=?", (batch_id, test_type)).fetchone()[0]
            passed = int(spec_min <= result <= spec_max)
            cur = self.conn.execute("""INSERT INTO tests(batch_id,test_type,result,spec_min,spec_max,passed,round,recorded_by,created_at)
                                     VALUES(?,?,?,?,?,?,?,?,?)""", (batch_id, test_type, result, spec_min, spec_max, passed, round_no, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation" if (batch["state"] == "awaiting_resample" or not passed) else batch["state"])
            self.store.audit(actor, "test.record", "batch", batch_id, {"test_type": test_type, "result": result, "passed": bool(passed), "round": round_no})
            self._invalidate_for_evidence(batch_id, actor, "test.record")
            return self._test_dict(self._row("tests", cur.lastrowid))

    def plan_rework(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, description: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator"})
        with self.store.tx():
            batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不能返工")
            cur = self.conn.execute("INSERT INTO rework(batch_id,description,status,created_by,created_at) VALUES(?,?,'planned',?,?)", (batch_id, description, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self.store.audit(actor, "rework.plan", "rework", cur.lastrowid, {"batch_id": batch_id, "description": description})
            self._invalidate_for_evidence(batch_id, actor, "rework.plan")
            return dict(self._row("rework", cur.lastrowid))

    def complete_rework(self, actor: str | None, role: str | None, factory_id: int, rework_id: int, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator"})
        with self.store.tx():
            row = self._row("rework", rework_id); batch = self._row("batches", row["batch_id"]); self._factory_check(actor, factory_id, batch)
            if row["status"] != "planned": raise ApiError(409, "返工记录已经完成")
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不可修改")
            self.conn.execute("UPDATE rework SET status='completed',completed_by=?,completed_at=? WHERE id=?", (actor, now(), rework_id))
            self._advance_batch(batch["id"], expected_revision, "investigation")
            self.store.audit(actor, "rework.complete", "rework", rework_id, {"batch_id": batch["id"]})
            self._invalidate_for_evidence(batch["id"], actor, "rework.complete")
            return dict(self._row("rework", rework_id))

    def record_supplier_change(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, supplier: str, change_type: str, description: str, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"operator", "qa"})
        with self.store.tx():
            batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不可修改")
            cur = self.conn.execute("INSERT INTO supplier_changes(batch_id,supplier,change_type,description,recorded_by,created_at) VALUES(?,?,?,?,?,?)",
                                    (batch_id, supplier, change_type, description, actor, now()))
            self._advance_batch(batch_id, expected_revision, "investigation")
            self.store.audit(actor, "supplier_change.record", "batch", batch_id, {"supplier": supplier, "change_type": change_type})
            self._invalidate_for_evidence(batch_id, actor, "supplier_change.record")
            return dict(self._row("supplier_changes", cur.lastrowid))

    def record_stability(self, actor: str | None, role: str | None, factory_id: int, batch_id: int, condition: str, timepoint: str, result: float, spec_limit: float, expected_revision: int) -> dict:
        actor = self._actor(actor, role, {"lab"})
        with self.store.tx():
            batch = self._row("batches", batch_id); self._factory_check(actor, factory_id, batch)
            if batch["state"] == "rejected": raise ApiError(409, "拒收批次不可修改")
            passed = int(result <= spec_limit)
            cur = self.conn.execute("""INSERT INTO stability(batch_id,condition,timepoint,result,spec_limit,passed,recorded_by,created_at)
                                     VALUES(?,?,?,?,?,?,?,?)""",
                                    (batch_id, condition, timepoint, result, spec_limit, passed, actor, now()))
            self._advance_batch(batch_id, expected_revision, batch["state"])
            self.store.audit(actor, "stability.record", "batch", batch_id, {"condition": condition, "timepoint": timepoint, "passed": bool(passed)})
            self._invalidate_for_evidence(batch_id, actor, "stability.record")
            return dict(self._row("stability", cur.lastrowid))

    # ------------------------------------------------------------------ 放行决定

    def _latest_tests(self, batch_id: int) -> dict[str, sqlite3.Row]:
        latest: dict[str, sqlite3.Row] = {}
        for row in self.conn.execute("SELECT * FROM tests WHERE batch_id=? ORDER BY id", (batch_id,)):
            latest[row["test_type"]] = row
        return latest

    def _decision_blockers(self, batch_id: int, decision: str, exception_code: str) -> list[dict]:
        """结构化列出阻止本次签发的阻塞项。"""
        blockers: list[dict] = []
        deviations = self.conn.execute("SELECT * FROM deviations WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
        open_deviations = [d for d in deviations if d["status"] == "open"]
        latest_tests = self._latest_tests(batch_id)
        if decision in {"release", "conditional"}:
            if not latest_tests:
                blockers.append({"code": "test.missing", "basis": "test", "message": "放行前至少需要一项检验结果"})
            for test in latest_tests.values():
                if not test["passed"]:
                    blockers.append({"code": "test.failed", "basis": "test", "item_id": test["id"],
                                     "message": f"检验 {test['test_type']} 第{test['round']}轮结果 {test['result']} 不合格"})
            for deviation in open_deviations:
                if deviation["severity"] == "critical":
                    blockers.append({"code": "deviation.critical_open", "basis": "deviation", "item_id": deviation["id"],
                                     "message": f"未关闭的关键偏差 #{deviation['id']}（{deviation['title']}）阻止放行"})
                elif decision == "release":
                    blockers.append({"code": "deviation.open", "basis": "deviation", "item_id": deviation["id"],
                                     "message": f"偏差 #{deviation['id']}（{deviation['title']}）尚未关闭，不能正式放行"})
                elif not deviation["exception_reason"] or not after_now(deviation["exception_until"]):
                    blockers.append({"code": "deviation.exception_invalid", "basis": "deviation", "item_id": deviation["id"],
                                     "message": f"偏差 #{deviation['id']}（{deviation['title']}）没有有效例外批准"})
            if decision == "conditional" and not exception_code.strip():
                blockers.append({"code": "exception_code.missing", "basis": "decision",
                                 "message": "有条件放行必须提供例外编号"})
        return blockers

    def decide(self, actor: str | None, role: str | None, batch_id: int, decision: str, rationale: str, expected_revision: int, exception_code: str = "") -> dict:
        actor = self._actor(actor, role, {"qa"})
        if decision not in {"release", "reject", "conditional", "resample"}: raise ApiError(400, "放行决定不合法")
        if not rationale.strip(): raise ApiError(400, "必须填写决定依据")
        with self.store.tx():
            batch = self._row("batches", batch_id)
            # 只接受当前修订号：两个终端同时提交时，后到者基于旧版本必须被拒绝
            if int(expected_revision) != int(batch["revision"]):
                raise ApiError(409, f"批次修订号已变更（当前 {batch['revision']}，提交基于 {expected_revision}），请刷新后按当前版本重新提交")
            if batch["state"] == "rejected":
                raise ApiError(409, "批次已拒收")
            if batch["state"] == "released":
                raise ApiError(409, "批次已放行；依据变更凭据失效退回待复核后才能再次签发")
            if decision == "resample" and batch["state"] == "conditional":
                raise ApiError(409, "有条件放行后不能直接改为再取样")
            blockers = self._decision_blockers(batch_id, decision, exception_code)
            if blockers:
                raise ApiError(409, "存在放行阻塞项：" + "；".join(b["message"] for b in blockers),
                               extra={"blockers": blockers})
            if decision == "resample":
                new_state = "awaiting_resample"
            elif decision == "reject":
                new_state = "rejected"
            elif decision == "conditional":
                new_state = "conditional"
            else:
                new_state = "released"

            stamp, frozen_revision = now(), int(batch["revision"]) + 1
            cur = self.conn.execute("""INSERT INTO decisions(batch_id,revision,decision,rationale,exception_code,decided_by,created_at)
                                     VALUES(?,?,?,?,?,?,?)""",
                                    (batch_id, batch["revision"], decision, rationale, exception_code or None, actor, stamp))
            credential = None
            if decision in {"release", "conditional"}:
                # 取代或撤销可能仍挂着的旧凭据
                self._close_active_credential(batch_id, actor, "batch.decision",
                                              "credential.superseded", f"批次按修订 {frozen_revision} 重新签发，旧凭据被取代")
                credential_no = f"CR-{batch_id}-{frozen_revision}"
                cred_cur = self.conn.execute("""INSERT INTO release_credentials(batch_id,credential_no,batch_revision,decision,rationale,exception_code,status,issued_by,issued_at)
                                                VALUES(?,?,?,?,?,?,'active',?,?)""",
                                             (batch_id, credential_no, frozen_revision, decision, rationale,
                                              exception_code or None, actor, stamp))
                # 冻结五类依据清单
                for basis, items in self._basis_manifest(batch_id).items():
                    self.conn.executemany(
                        "INSERT INTO credential_snapshots(credential_id,basis,item_id,item_hash,snapshot_json) VALUES(?,?,?,?,?)",
                        [(cred_cur.lastrowid, basis, item_id, item_hash, j(content))
                         for item_id, item_hash, content in items])
                credential = self._credential_dict(self._row("release_credentials", cred_cur.lastrowid))
            elif decision == "reject":
                self._close_active_credential(batch_id, actor, "batch.decision",
                                              "credential.revoked_by_reject", "批次被拒收，放行凭据撤销")

            updated = self.conn.execute("UPDATE batches SET state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                        (new_state, stamp, batch_id, expected_revision))
            if updated.rowcount != 1:
                raise ApiError(409, "并发放行冲突：该批次修订号已被其他终端提交，请刷新后重试")
            self.store.audit(actor, "batch.decision", "batch", batch_id,
                             {"decision": decision, "revision": batch["revision"], "state": new_state,
                              "exception_code": exception_code, "credential_no": credential["credential_no"] if credential else None})
            return {"decision": dict(self._row("decisions", cur.lastrowid)),
                    "batch": self.batch_detail(batch_id)["batch"],
                    "credential": credential}

    def _advance_batch(self, batch_id: int, expected_revision: int, next_state: str) -> None:
        batch = self._row("batches", batch_id)
        if batch["state"] == "rejected": raise ApiError(409, "拒收批次不可修改")
        if batch["state"] == "awaiting_review": next_state = "awaiting_review"  # 待复核期间保持，直到 QA 重新签发
        if int(expected_revision) != int(batch["revision"]):
            raise ApiError(409, f"批次版本冲突（当前 {batch['revision']}，提交基于 {expected_revision}）")
        cur = self.conn.execute("UPDATE batches SET state=?,revision=revision+1,updated_at=? WHERE id=? AND revision=?",
                                (next_state, now(), batch_id, expected_revision))
        if cur.rowcount != 1: raise ApiError(409, "并发更新冲突：只接受批次当前版本")

    # ------------------------------------------------------------------ 查询

    def _invalidation_dicts(self, batch_id: int) -> list[dict]:
        events = []
        for event in self.conn.execute("SELECT * FROM invalidation_events WHERE batch_id=? ORDER BY id", (batch_id,)):
            reasons = [{"code": row["code"], "detail": row["detail"]}
                       for row in self.conn.execute("SELECT * FROM invalidation_reasons WHERE event_id=? ORDER BY id", (event["id"],))]
            credential = self._row("release_credentials", event["credential_id"])
            events.append({"id": event["id"], "credential_id": event["credential_id"],
                           "credential_no": credential["credential_no"], "at": event["at"],
                           "triggered_by": event["triggered_by"], "trigger_action": event["trigger_action"],
                           "reasons": reasons})
        return events

    def _credential_dict(self, row: sqlite3.Row) -> dict:
        counts = {basis: 0 for basis in BASIS_SPECS}
        for r in self.conn.execute("SELECT basis, COUNT(*) AS n FROM credential_snapshots WHERE credential_id=? GROUP BY basis", (row["id"],)):
            counts[r["basis"]] = r["n"]
        return {"id": row["id"], "credential_no": row["credential_no"], "batch_revision": row["batch_revision"],
                "decision": row["decision"], "rationale": row["rationale"], "exception_code": row["exception_code"],
                "status": row["status"], "issued_by": row["issued_by"], "issued_at": row["issued_at"],
                "invalidated_at": row["invalidated_at"],
                "frozen": {"revision": row["batch_revision"], "counts": {BASIS_SPECS[b][2]: counts.get(b, 0) for b in BASIS_SPECS}}}

    def batch_detail(self, batch_id: int) -> dict:
        with self.store.lock:
            batch = self._batch_dict(self._row("batches", batch_id))
            def rows(name: str) -> list[dict]: return [dict(row) for row in self.conn.execute(f"SELECT * FROM {name} WHERE batch_id=? ORDER BY id", (batch_id,))]
            active = self._active_credential(batch_id)
            return {"batch": batch, "deviations": rows("deviations"), "tests": rows("tests"), "rework": rows("rework"),
                    "supplier_changes": rows("supplier_changes"), "stability": rows("stability"),
                    "decisions": rows("decisions"),
                    "credential": self._credential_dict(active) if active else None,
                    "invalidations": self._invalidation_dicts(batch_id),
                    "release_blockers": self._decision_blockers(batch_id, "release", "")}

    def _batch_dict(self, row: sqlite3.Row) -> dict:
        return {"id": row["id"], "factory_id": row["factory_id"], "batch_no": row["batch_no"], "product": row["product"],
                "mfg_date": row["mfg_date"], "expiry_date": row["expiry_date"], "state": row["state"], "revision": row["revision"]}

    @staticmethod
    def _deviation_dict(row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "severity": row["severity"], "title": row["title"], "due_at": row["due_at"],
                "status": row["status"], "corrective_action": row["corrective_action"], "exception_reason": row["exception_reason"],
                "exception_until": row["exception_until"], "exception_approved_by": row["exception_approved_by"]}

    @staticmethod
    def _test_dict(row: sqlite3.Row) -> dict:
        return {"id": row["id"], "batch_id": row["batch_id"], "test_type": row["test_type"], "result": row["result"],
                "spec_min": row["spec_min"], "spec_max": row["spec_max"], "passed": bool(row["passed"]), "round": row["round"]}

    def state(self) -> dict:
        with self.store.lock:
            batches = []
            for row in self.conn.execute("SELECT * FROM batches ORDER BY id DESC"):
                entry = self._batch_dict(row)
                entry["release_blockers"] = self._decision_blockers(row["id"], "release", "")
                active = self._active_credential(row["id"])
                entry["credential"] = {"credential_no": active["credential_no"], "status": active["status"],
                                       "issued_at": active["issued_at"]} if active else None
                invalidations = self._invalidation_dicts(row["id"])
                entry["last_invalidation"] = invalidations[-1] if invalidations else None
                batches.append(entry)
            return {"factories": [dict(row) for row in self.conn.execute("SELECT * FROM factories ORDER BY id")],
                    "batches": batches,
                    "audits": [dict(row) for row in self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT 30")]}

    def seed(self) -> None:
        if not self.conn.execute("SELECT id FROM factories LIMIT 1").fetchone():
            self.register_factory("qa-demo", "qa", "F-DEMO", "演示工厂", "CN")


# 请求编号 -> 服务方法的幂等包装，供 HTTP 与测试共用
def execute(service: BatchService, parts: list[str], body: dict, actor: str | None, role: str | None,
            request_id: str | None = None) -> tuple[dict, bool]:
    route = "/" + "/".join(parts)

    def call() -> dict:
        b = body
        if parts == ["api", "factories"]: return service.register_factory(actor, role, b.get("code", ""), b.get("name", ""), b.get("country", ""))
        if parts == ["api", "batches"]: return service.create_batch(actor, role, int(b.get("factory_id", 0)), b.get("batch_no", ""), b.get("product", ""), b.get("mfg_date", ""), b.get("expiry_date", ""))
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "deviations": return service.add_deviation(actor, role, int(b.get("factory_id", 0)), int(parts[2]), b.get("severity", ""), b.get("title", ""), b.get("due_at"), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "deviations"] and parts[3] == "close": return service.close_deviation(actor, role, int(parts[2]), b.get("corrective_action", ""), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "deviations"] and parts[3] == "exception": return service.approve_exception(actor, role, int(parts[2]), b.get("reason", ""), b.get("until", ""), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "tests": return service.record_test(actor, role, int(b.get("factory_id", 0)), int(parts[2]), b.get("test_type", ""), float(b.get("result", 0)), float(b.get("spec_min", 0)), float(b.get("spec_max", 0)), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "rework": return service.plan_rework(actor, role, int(b.get("factory_id", 0)), int(parts[2]), b.get("description", ""), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "rework"] and parts[3] == "complete": return service.complete_rework(actor, role, int(b.get("factory_id", 0)), int(parts[2]), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "supplier-changes": return service.record_supplier_change(actor, role, int(b.get("factory_id", 0)), int(parts[2]), b.get("supplier", ""), b.get("change_type", ""), b.get("description", ""), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "stability": return service.record_stability(actor, role, int(b.get("factory_id", 0)), int(parts[2]), b.get("condition", ""), b.get("timepoint", ""), float(b.get("result", 0)), float(b.get("spec_limit", 0)), int(b.get("expected_revision", -1)))
        if len(parts) == 4 and parts[:2] == ["api", "batches"] and parts[3] == "decide": return service.decide(actor, role, int(parts[2]), b.get("decision", ""), b.get("rationale", ""), int(b.get("expected_revision", -1)), b.get("exception_code", ""))
        raise ApiError(404, "接口不存在")

    store = service.store
    body_hash = canonical(body) if body else ""
    with store.tx():
        if request_id:
            existing = store.get_idempotent(request_id)
            if existing is not None:
                if existing["actor"] != actor or existing["route"] != route or existing["body_hash"] != body_hash:
                    raise ApiError(409, "请求编号已绑定其他请求或操作人，不能复用")
                return json.loads(existing["response_json"]), True
        out = call()
        if request_id:
            store.save_idempotent(request_id, actor or "", route, body_hash, out)
        return out, False


class Handler(BaseHTTPRequestHandler):
    service: BatchService

    def log_message(self, fmt: str, *args: object) -> None: sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, status: int, body: object, extra_headers: dict | None = None) -> None:
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        for key, value in (extra_headers or {}).items(): self.send_header(key, value)
        self.end_headers(); self.wfile.write(data)

    def _send_error(self, exc: ApiError) -> None:
        payload: dict = {"error": exc.message}
        payload.update(exc.extra)
        self._send(exc.status, payload)

    def _body(self) -> dict:
        size = int(self.headers.get("Content-Length", "0"))
        try: return json.loads(self.rfile.read(size)) if size else {}
        except json.JSONDecodeError as exc: raise ApiError(400, "JSON 请求体无效") from exc

    def _parts(self) -> list[str]: return [p for p in urlparse(self.path).path.strip("/").split("/") if p]

    def do_GET(self) -> None:
        try:
            p = self._parts()
            if p in (["health"], ["api", "health"]): out = {"status": "ok"}
            elif p == ["api", "state"]: out = self.service.state()
            elif len(p) == 3 and p[:2] == ["api", "batches"]: out = self.service.batch_detail(int(p[2]))
            elif not p:
                page = (Path(__file__).parent / "static" / "index.html").read_bytes()
                self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page))); self.end_headers(); self.wfile.write(page); return
            else: raise ApiError(404, "接口不存在")
            self._send(200, out)
        except ApiError as exc: self._send_error(exc)
        except Exception as exc: self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            p, b = self._parts(), self._body()
            actor, role = self.headers.get("X-Actor"), self.headers.get("X-Role")
            request_id = self.headers.get("X-Request-Id") or b.pop("request_id", None)
            out, replayed = execute(self.service, p, b, actor, role, request_id or None)
            self._send(200, out, {"X-Idempotency-Replayed": "1"} if replayed else None)
        except ApiError as exc: self._send_error(exc)
        except (ValueError, TypeError, sqlite3.IntegrityError) as exc: self._send(400, {"error": str(exc)})
        except Exception as exc: self._send(500, {"error": str(exc)})


def run(port: int, db_path: str, seed: bool) -> None:
    store = Store(db_path); service = BatchService(store)
    if seed: service.seed()
    Handler.service = service
    print(f"batch release listening on http://127.0.0.1:{port}")
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--port", type=int, default=8214); parser.add_argument("--db", default=str(DB_PATH)); parser.add_argument("--init", action="store_true"); parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    if args.init: Store(args.db).close()
    if args.seed or not args.init: run(args.port, args.db, args.seed)


if __name__ == "__main__": main()
