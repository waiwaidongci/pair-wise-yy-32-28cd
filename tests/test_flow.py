import sys, tempfile, unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, BatchService, Store

class BatchFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.s = BatchService(Store(Path(self.tmp.name) / "b.db"))
        self.f1 = self.s.register_factory("qa", "qa", "F1", "一厂", "CN")["id"]
        self.f2 = self.s.register_factory("qa", "qa", "F2", "二厂", "CN")["id"]
        self.future = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat().replace("+00:00", "Z")

    def tearDown(self): self.s.store.close(); self.tmp.cleanup()

    def test_full_investigation_retest_rework_and_release(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-1", "药片", "2026-01-01", "2028-01-01")
        dev = self.s.add_deviation("operator", "operator", self.f1, batch["id"], "minor", "装量轻微偏离", self.future, batch["revision"])
        failed = self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 89, 95, 105, dev["batch_id"] and self.s.batch_detail(batch["id"])["batch"]["revision"])
        self.assertFalse(failed["passed"])
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        passed = self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 99, 95, 105, current)
        self.assertTrue(passed["passed"])
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.close_deviation("qa", "qa", dev["id"], "调整灌装参数", current)
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        rw = self.s.plan_rework("operator", "operator", self.f1, batch["id"], "返工包装", current)
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.complete_rework("operator", "operator", self.f1, rw["id"], current)
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_stability("lab", "lab", self.f1, batch["id"], "25C/60RH", "3m", 99, 105, current)
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        result = self.s.decide("qa", "qa", batch["id"], "release", "调查关闭，复测合格", current)
        self.assertEqual("released", result["batch"]["state"])
        self.assertEqual(1, len(result["batch"] and self.s.batch_detail(batch["id"])["decisions"]))

    def test_critical_block_conditional_exception_and_factory_conflict(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-2", "胶囊", "2026-02-01", "2028-02-01")
        current = batch["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 100, 95, 105, current)
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
        crit = self.s.add_deviation("inspector", "inspector", self.f1, batch["id"], "critical", "无菌数据异常", self.future, current)
        current = self.s.batch_detail(batch["id"])["batch"]["revision"]
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

    def _releaseable_batch(self, batch_no="B-REL"):
        batch = self.s.create_batch("operator", "operator", self.f1, batch_no, "产品", "2026-01-01", "2028-01-01")
        rev = batch["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 99, 95, 105, rev)
        return batch["id"], self.s.batch_detail(batch["id"])["batch"]["revision"]

    def test_release_freezes_basis_and_invalidates_on_change(self):
        batch_id, rev = self._releaseable_batch()
        self.s.decide("qa", "qa", batch_id, "release", "合格放行", rev, request_no="R-1")
        detail = self.s.batch_detail(batch_id)
        cred = detail["active_credential"]
        self.assertEqual("active", cred["status"])
        self.assertEqual(rev, cred["basis"]["revision"])
        self.assertEqual(1, len(cred["basis"]["tests"]))
        # 依据补录（新增偏差）后凭据失效，批次退回待复核
        cur = self.s.batch_detail(batch_id)["batch"]["revision"]
        dev = self.s.add_deviation("operator", "operator", self.f1, batch_id, "minor", "装量偏差", self.future, cur)
        detail = self.s.batch_detail(batch_id)
        self.assertEqual("pending_review", detail["batch"]["state"])
        cred = detail["active_credential"]
        self.assertEqual("invalid", cred["status"])
        self.assertTrue(any("偏差" in r["message"] for r in cred["invalid_reasons"]))
        # 关闭偏差并重新签发后，新凭据有效且冻结新清单
        cur = detail["batch"]["revision"]
        self.s.close_deviation("qa", "qa", dev["id"], "纠正完成", cur)
        cur = self.s.batch_detail(batch_id)["batch"]["revision"]
        self.s.decide("qa", "qa", batch_id, "release", "重新放行", cur, request_no="R-2")
        cred = self.s.batch_detail(batch_id)["active_credential"]
        self.assertEqual("active", cred["status"])
        self.assertEqual(1, len(cred["basis"]["deviations"]))
        self.assertEqual("closed", cred["basis"]["deviations"][0]["status"])

    def test_every_basis_change_invalidates_credential(self):
        batch_id, rev = self._releaseable_batch()
        self.s.decide("qa", "qa", batch_id, "release", "放行", rev, request_no="R-1")
        cur = self.s.batch_detail(batch_id)["batch"]["revision"]
        self.s.record_stability("lab", "lab", self.f1, batch_id, "25C/60RH", "3m", 99, 105, cur)
        self.assertEqual("invalid", self.s.batch_detail(batch_id)["active_credential"]["status"])
        cur = self.s.batch_detail(batch_id)["batch"]["revision"]
        self.s.plan_rework("operator", "operator", self.f1, batch_id, "返工", cur)
        self.assertEqual("invalid", self.s.batch_detail(batch_id)["active_credential"]["status"])
        cur = self.s.batch_detail(batch_id)["batch"]["revision"]
        self.s.record_supplier_change("operator", "operator", self.f1, batch_id, "供应商A", "变更", "产地变更", cur)
        self.assertEqual("invalid", self.s.batch_detail(batch_id)["active_credential"]["status"])
        reasons = [r["message"] for r in self.s.batch_detail(batch_id)["active_credential"]["invalid_reasons"]]
        self.assertEqual(3, len(reasons))

    def test_idempotent_retry_reuses_request_number(self):
        batch_id, rev = self._releaseable_batch()
        first = self.s.decide("qa", "qa", batch_id, "release", "放行", rev, request_no="R-1")
        retry = self.s.decide("qa", "qa", batch_id, "release", "放行", rev, request_no="R-1")
        self.assertEqual(first["decision"]["id"], retry["decision"]["id"])
        # 凭据失效后沿用旧编号重试被拒，要求换新编号
        cur = self.s.batch_detail(batch_id)["batch"]["revision"]
        self.s.record_stability("lab", "lab", self.f1, batch_id, "25C/60RH", "3m", 99, 105, cur)
        with self.assertRaises(ApiError) as cm:
            self.s.decide("qa", "qa", batch_id, "release", "放行", cur, request_no="R-1")
        self.assertEqual(409, cm.exception.status)
        # 不同决定占用同一编号被拒
        batch2_id, rev2 = self._releaseable_batch("B-OTHER")
        with self.assertRaises(ApiError):
            self.s.decide("qa", "qa", batch2_id, "reject", "拒绝", rev2, request_no="R-1")

    def test_audit_failure_rolls_back_and_retry_reuses_request_no(self):
        batch_id, rev = self._releaseable_batch()
        original_audit = self.s.store.audit
        def boom(*args, **kwargs): raise RuntimeError("审计写入失败")
        self.s.store.audit = boom
        with self.assertRaises(RuntimeError):
            self.s.decide("qa", "qa", batch_id, "release", "放行", rev, request_no="R-1")
        self.s.store.audit = original_audit
        detail = self.s.batch_detail(batch_id)
        self.assertEqual("manufactured", detail["batch"]["state"])
        self.assertEqual(0, len(detail["decisions"]))
        self.assertEqual(rev, detail["batch"]["revision"])
        # 重试沿用同一请求编号后成功
        result = self.s.decide("qa", "qa", batch_id, "release", "放行", detail["batch"]["revision"], request_no="R-1")
        self.assertEqual("released", result["batch"]["state"])
        self.assertEqual(1, len(self.s.batch_detail(batch_id)["decisions"]))

    def test_blockers_listed_and_release_check(self):
        batch = self.s.create_batch("operator", "operator", self.f1, "B-BLK", "胶囊", "2026-02-01", "2028-02-01")
        rev = batch["revision"]
        self.s.add_deviation("inspector", "inspector", self.f1, batch["id"], "critical", "无菌异常", self.future, rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        self.s.record_test("lab", "lab", self.f1, batch["id"], "含量", 80, 95, 105, rev)
        rev = self.s.batch_detail(batch["id"])["batch"]["revision"]
        with self.assertRaises(ApiError) as cm:
            self.s.decide("qa", "qa", batch["id"], "release", "尝试", rev)
        self.assertEqual(409, cm.exception.status)
        codes = {b["code"] for b in cm.exception.payload["blockers"]}
        self.assertIn("test_failed", codes)
        self.assertIn("critical_deviation", codes)
        # 接口预检列出同样的阻塞项
        check = self.s.release_check(batch["id"])
        self.assertEqual(codes, {b["code"] for b in check["blockers"]})

    def test_two_terminals_only_current_version_accepted(self):
        batch_id, rev = self._releaseable_batch("B-CONC")
        # 终端 A 先提交（当前版本）
        self.s.decide("qa", "qa", batch_id, "release", "放行", rev, request_no="A-1")
        # 终端 B 持旧版本提交同一批次，被乐观锁拒绝
        with self.assertRaises(ApiError) as cm:
            self.s.decide("qa", "qa", batch_id, "release", "放行", rev, request_no="B-1")
        self.assertEqual(409, cm.exception.status)


if __name__ == "__main__": unittest.main()
