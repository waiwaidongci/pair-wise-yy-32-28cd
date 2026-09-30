import sys, tempfile, threading, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, BatchService, Store, execute


class BatchFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.s = BatchService(Store(Path(self.tmp.name) / "b.db"))
        self.f1 = self.s.register_factory("qa", "qa", "F1", "一厂", "CN")["id"]
        self.f2 = self.s.register_factory("qa", "qa", "F2", "二厂", "CN")["id"]
        self.future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat().replace("+00:00", "Z")

    def tearDown(self): self.s.store.close(); self.tmp.cleanup()

    def _rev(self, batch_id): return self.s.batch_detail(batch_id)["batch"]["revision"]

    def _release_ready(self, batch_id, *, stability=False):
        """补一项合格检验，返回当前修订号。"""
        self.s.record_test("lab", "lab", self.f1, batch_id, "含量", 100, 95, 105, self._rev(batch_id))
        if stability:
            self.s.record_stability("lab", "lab", self.f1, batch_id, "25C/60RH", "3m", 99, 105, self._rev(batch_id))
        return self._rev(batch_id)

    def test_full_investigation_retest_rework_and_release(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-1", "药片", "2026-01-01", "2028-01-01")
        dev = self.s.add_deviation("operator", "operator", self.f1, batch["id"], "minor", "装量轻微偏离", self.future, batch["revision"])
        failed = self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 89, 95, 105, dev["batch_id"] and self._rev(batch["id"]))
        self.assertFalse(failed["passed"])
        current = self._rev(batch["id"])
        passed = self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 99, 95, 105, current)
        self.assertTrue(passed["passed"])
        current = self._rev(batch["id"])
        self.s.close_deviation("qa", "qa", dev["id"], "调整灌装参数", current)
        current = self._rev(batch["id"])
        rw = self.s.plan_rework("operator", "operator", self.f1, batch["id"], "返工包装", current)
        current = self._rev(batch["id"])
        self.s.complete_rework("operator", "operator", self.f1, rw["id"], current)
        current = self._rev(batch["id"])
        self.s.record_stability("lab", "lab", self.f1, batch["id"], "25C/60RH", "3m", 99, 105, current)
        current = self._rev(batch["id"])
        result = self.s.decide("qa", "qa", batch["id"], "release", "调查关闭，复测合格", current)
        self.assertEqual("released", result["batch"]["state"])
        self.assertIsNotNone(result["credential"])
        self.assertEqual(1, len(result["batch"] and self.s.batch_detail(batch["id"])["decisions"]))

    def test_critical_block_conditional_exception_and_factory_conflict(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-2", "胶囊", "2026-02-01", "2028-02-01")
        current = batch["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, current)
        current = self._rev(batch["id"])
        crit = self.s.add_deviation("inspector", "inspector", self.f1, batch["id"], "critical", "无菌数据异常", self.future, current)
        current = self._rev(batch["id"])
        with self.assertRaises(ApiError) as blocked:
            self.s.decide("qa", "qa", batch["id"], "release", "尝试放行", current)
        self.assertIn("关键偏差", blocked.exception.message)
        with self.assertRaises(ApiError):
            self.s.approve_exception("qa", "qa", crit["id"], "暂时接受", self.future, current)
        with self.assertRaises(ApiError):
            self.s.record_test("lab", "lab", self.f2, batch["id"], "水分", 1, 0, 2, current)
        with self.assertRaises(ApiError) as stale:
            self.s.record_test("lab", "lab", self.f1, batch["id"], "水分", 1, 0, 2, 1)
        self.assertEqual(409, stale.exception.status)

    # ------------------------------------------------------------------ 冻结与失效

    def test_credential_freezes_lists_and_is_invalidated_back_to_review(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-3", "注射液", "2026-03-01", "2028-03-01")
        dev = self.s.add_deviation("operator", "operator", self.f1, batch["id"], "minor", "标签错贴", None, self._rev(batch["id"]))
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        self.s.close_deviation("qa", "qa", dev["id"], "重新贴标", self._rev(batch["id"]))
        released = self.s.decide("qa", "qa", batch["id"], "release", "全部关闭，检验合格", self._rev(batch["id"]))
        self.assertEqual("active", released["credential"]["status"])
        frozen_counts = released["credential"]["frozen"]["counts"]
        self.assertEqual({"偏差": 1, "检验": 1, "返工": 0, "供应商变更": 0, "稳定性": 0}, frozen_counts)

        # 放行后补录稳定性：不再禁止，但凭据必须失效并退回待复核
        self.s.record_stability("lab", "lab", self.f1, batch["id"], "40C/75RH", "1m", 101, 105, self._rev(batch["id"]))
        detail = self.s.batch_detail(batch["id"])
        self.assertEqual("awaiting_review", detail["batch"]["state"])
        self.assertIsNone(detail["credential"])
        self.assertEqual(1, len(detail["invalidations"]))
        inv = detail["invalidations"][0]
        self.assertEqual("stability.record", inv["trigger_action"])
        self.assertTrue(any(r["code"] == "stability.added" for r in inv["reasons"]))
        self.assertTrue(any(r["code"] == "revision.changed" for r in inv["reasons"]))
        self.assertTrue(any("修订号" in r["detail"] for r in inv["reasons"]))

        # 待复核期间补充的依据在复核前可继续修正；重新放行需要通过全部阻塞项
        with self.assertRaises(ApiError) as stale:
            self.s.decide("qa", "qa", batch["id"], "release", "按旧版本复核", released["batch"]["revision"])
        self.assertEqual(409, stale.exception.status)
        reissued = self.s.decide("qa", "qa", batch["id"], "release", "稳定性合格，重新签发", self._rev(batch["id"]))
        self.assertEqual("released", reissued["batch"]["state"])
        self.assertEqual("active", reissued["credential"]["status"])
        credentials = self.s.conn.execute("SELECT status FROM release_credentials WHERE batch_id=?", (batch["id"],)).fetchall()
        self.assertEqual({"invalidated", "active"}, {r["status"] for r in credentials})

    def test_post_release_deviation_correction_invalidates_with_change_detail(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-4", "软膏", "2026-04-01", "2028-04-01")
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        self.s.decide("qa", "qa", batch["id"], "release", "合格放行", self._rev(batch["id"]))

        # 放行后登记新偏差（补录），凭据失效退回待复核
        dev = self.s.add_deviation("operator", "operator", self.f1, batch["id"], "minor", "包装破损", None, self._rev(batch["id"]))
        detail = self.s.batch_detail(batch["id"])
        self.assertEqual("awaiting_review", detail["batch"]["state"])
        self.assertTrue(any(r["code"] == "deviation.added" for r in detail["invalidations"][-1]["reasons"]))
        # 凭据已失效后继续关闭偏差不会产生新的失效事件，也不能发运
        self.s.close_deviation("qa", "qa", dev["id"], "更换包装", self._rev(batch["id"]))
        detail = self.s.batch_detail(batch["id"])
        self.assertEqual("awaiting_review", detail["batch"]["state"])
        self.assertIsNone(detail["credential"])
        self.assertEqual(1, len(detail["invalidations"]))

        # 字段级更正：对有效凭据冻结过的偏差延长例外有效期，应产生 changed 明细
        batch2 = self.s.create_batch("operator", "operator", self.f1, "B-4B", "乳膏", "2026-04-02", "2028-04-02")
        dev2 = self.s.add_deviation("operator", "operator", self.f1, batch2["id"], "minor", "外观瑕疵", None, self._rev(batch2["id"]))
        self.s.record_test("lab", "lab", self.f1, batch2["id"], "含量", 100, 95, 105, self._rev(batch2["id"]))
        self.s.approve_exception("qa", "qa", dev2["id"], "限期使用", self.future, self._rev(batch2["id"]))
        self.s.decide("qa", "qa", batch2["id"], "conditional", "有条件放行", self._rev(batch2["id"]), exception_code="EX-2")
        extended = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat().replace("+00:00", "Z")
        self.s.approve_exception("qa", "qa", dev2["id"], "限期使用并延长", extended, self._rev(batch2["id"]))
        detail2 = self.s.batch_detail(batch2["id"])
        self.assertEqual("awaiting_review", detail2["batch"]["state"])
        change = [r for inv in detail2["invalidations"] for r in inv["reasons"] if r["code"] == "deviation.changed"]
        self.assertTrue(change)
        self.assertIn("例外有效期", change[-1]["detail"])

    def test_conditional_credential_superseded_on_reissue(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-5", "颗粒", "2026-05-01", "2028-05-01")
        dev = self.s.add_deviation("operator", "operator", self.f1, batch["id"], "minor", "外观瑕疵", None, self._rev(batch["id"]))
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        self.s.approve_exception("qa", "qa", dev["id"], "限期使用", self.future, self._rev(batch["id"]))
        first = self.s.decide("qa", "qa", batch["id"], "conditional", "有条件放行", self._rev(batch["id"]), exception_code="EX-1")
        self.assertEqual("conditional", first["credential"]["decision"])

        self.s.close_deviation("qa", "qa", dev["id"], "挑选后合格", self._rev(batch["id"]))
        self.assertEqual("awaiting_review", self.s.batch_detail(batch["id"])["batch"]["state"])
        second = self.s.decide("qa", "qa", batch["id"], "release", "偏差关闭，正式放行", self._rev(batch["id"]))
        statuses = {r["status"] for r in self.s.conn.execute("SELECT status FROM release_credentials WHERE batch_id=?", (batch["id"],))}
        self.assertEqual({"invalidated", "active"}, statuses)
        self.assertEqual("released", second["batch"]["state"])

    # ------------------------------------------------------------------ 并发

    def test_two_terminals_decide_only_current_version_wins(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-6", "冻干针", "2026-06-01", "2028-06-01")
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        rev = self._rev(batch["id"])
        errors: list[Exception] = []

        def decide():
            try:
                self.s.decide("qa", "qa", batch["id"], "release", "终端放行", rev)
            except Exception as exc:
                errors.append(exc)

        t1, t2 = threading.Thread(target=decide), threading.Thread(target=decide)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(1, len(errors))
        self.assertEqual(409, errors[0].status if hasattr(errors[0], "status") else None)
        detail = self.s.batch_detail(batch["id"])
        self.assertEqual("released", detail["batch"]["state"])
        self.assertEqual(rev + 1, detail["batch"]["revision"])
        actives = self.s.conn.execute("SELECT COUNT(*) AS n FROM release_credentials WHERE batch_id=? AND status='active'", (batch["id"],)).fetchone()["n"]
        self.assertEqual(1, actives)

    # ------------------------------------------------------------------ 审计回滚

    def test_audit_write_failure_rolls_back_entire_decision(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-7", "糖浆", "2026-07-01", "2028-07-01")
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        rev = self._rev(batch["id"])
        original = self.s.store.audit

        def boom(*a, **k): raise RuntimeError("simulated audit disk failure")
        self.s.store.audit = boom
        try:
            with self.assertRaises(RuntimeError):
                self.s.decide("qa", "qa", batch["id"], "release", "应整笔回滚", rev)
        finally:
            self.s.store.audit = original

        self.assertEqual(rev, self._rev(batch["id"]))
        self.assertEqual("manufactured", self.s.batch_detail(batch["id"])["batch"]["state"])
        self.assertEqual(0, self.s.conn.execute("SELECT COUNT(*) AS n FROM decisions WHERE batch_id=?", (batch["id"],)).fetchone()["n"])
        self.assertEqual(0, self.s.conn.execute("SELECT COUNT(*) AS n FROM release_credentials WHERE batch_id=?", (batch["id"],)).fetchone()["n"])
        self.assertEqual(0, self.s.conn.execute("SELECT COUNT(*) AS n FROM credential_snapshots").fetchone()["n"])
        # 回滚后服务可继续正常使用
        out = self.s.decide("qa", "qa", batch["id"], "release", "重新签发", rev)
        self.assertEqual("active", out["credential"]["status"])

    # ------------------------------------------------------------------ 幂等重试

    def test_retry_reuses_request_id_and_blocks_mismatched_replay(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-8", "滴剂", "2026-08-01", "2028-08-01")
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        rev = self._rev(batch["id"])
        parts = ["api", "batches", str(batch["id"]), "decide"]
        body = {"decision": "release", "rationale": "带请求编号的放行", "expected_revision": rev}

        first, replayed1 = execute(self.s, parts, dict(body), "qa", "qa", request_id="REQ-1")
        self.assertFalse(replayed1)
        second, replayed2 = execute(self.s, parts, dict(body), "qa", "qa", request_id="REQ-1")
        self.assertTrue(replayed2)
        self.assertEqual(first["credential"]["credential_no"], second["credential"]["credential_no"])
        self.assertEqual(1, self.s.conn.execute("SELECT COUNT(*) AS n FROM decisions WHERE batch_id=?", (batch["id"],)).fetchone()["n"])

        other = dict(body); other["rationale"] = "不同请求体复用编号"
        with self.assertRaises(ApiError) as mismatch:
            execute(self.s, parts, other, "qa", "qa", request_id="REQ-1")
        self.assertEqual(409, mismatch.exception.status)

    def test_failed_request_is_not_stored_under_request_id(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-9", "散剂", "2026-09-01", "2028-09-01")
        parts = ["api", "batches", str(batch["id"]), "decide"]
        body = {"decision": "release", "rationale": "无检验放行", "expected_revision": self._rev(batch["id"])}
        with self.assertRaises(ApiError):
            execute(self.s, parts, dict(body), "qa", "qa", request_id="REQ-2")
        self.assertIsNone(self.s.store.get_idempotent("REQ-2"))
        # 补齐检验后同一编号可重新使用
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, self._rev(batch["id"]))
        body["expected_revision"] = self._rev(batch["id"])
        out, replayed = execute(self.s, parts, body, "qa", "qa", request_id="REQ-2")
        self.assertFalse(replayed)
        self.assertEqual("active", out["credential"]["status"])

    # ------------------------------------------------------------------ 阻塞项清单

    def test_blockers_listed_structurally_in_api_and_detail(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-10", "栓剂", "2026-10-01", "2028-10-01")
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 80, 95, 105, self._rev(batch["id"]))
        self.s.add_deviation("inspector", "inspector", self.f1, batch["id"], "critical", "内毒素超标", None, self._rev(batch["id"]))
        self.s.plan_rework("operator", "operator", self.f1, batch["id"], "计划返工", self._rev(batch["id"]))
        self.s.record_supplier_change("operator", "operator", self.f1, batch["id"], "ACME", "原料产地变更", "备案中", self._rev(batch["id"]))
        self.s.record_stability("lab", "lab", self.f1, batch["id"], "25C", "3m", 120, 105, self._rev(batch["id"]))

        with self.assertRaises(ApiError) as blocked:
            self.s.decide("qa", "qa", batch["id"], "release", "尝试放行", self._rev(batch["id"]))
        codes = {b["code"] for b in blocked.exception.extra["blockers"]}
        self.assertIn("test.failed", codes)
        self.assertIn("deviation.critical_open", codes)
        # 接口错误正文与结构化清单一致，页面/调用方都能拿到
        for item in blocked.exception.extra["blockers"]:
            self.assertIn(item["message"], blocked.exception.message)

        detail = self.s.batch_detail(batch["id"])
        detail_codes = {b["code"] for b in detail["release_blockers"]}
        self.assertIn("test.failed", detail_codes)
        self.assertIn("deviation.critical_open", detail_codes)
        # 返工、供应商变更、稳定性属于冻结清单但不额外阻塞；详情中应完整列出
        self.assertEqual(1, len(detail["rework"]))
        self.assertEqual(1, len(detail["supplier_changes"]))
        self.assertEqual(1, len(detail["stability"]))

        state = self.s.state()
        entry = next(b for b in state["batches"] if b["id"] == batch["id"])
        self.assertTrue(any(b["code"] == "deviation.critical_open" for b in entry["release_blockers"]))


if __name__ == "__main__": unittest.main()
