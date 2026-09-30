import sys, tempfile, threading, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class RevisionChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        base = utcnow() + timedelta(days=1)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC3", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR3", "name": "丙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.base = base

    def tearDown(self): self.tmp.cleanup()

    def make_flight(self, number, aircraft="AC1", crew="CR1"):
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB", "std": iso(self.base), "sta": iso(self.base + timedelta(hours=2)), "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 150})

    def build_locked_processed(self, number="AB100"):
        flight = self.make_flight(number)
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "换飞机并延误", "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5))}]})
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        processed = self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": locked["revision"]})
        return flight, disruption, processed

    def test_disruption_revision_stales_locked_plan_and_pending_flights(self):
        flight, disruption, plan = self.build_locked_processed("AB100")
        self.assertEqual(plan["status"], "locked")
        self.assertEqual(plan["apply_status"], "succeeded")
        # 中断窗口更新：锁定方案基于旧版本 -> stale，未执行航班转待复核。
        out = self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                         {"expected_revision": disruption["revision"],
                                          "starts_at": iso(self.base - timedelta(hours=1)),
                                          "ends_at": iso(self.base + timedelta(hours=8))})
        self.assertEqual(out["disruption"]["revision"], 2)
        self.assertEqual(out["staled_plan_ids"], [plan["id"]])
        self.assertEqual(out["pending_review_flight_ids"], [flight["id"]])
        stale = self.svc.get_plan(plan["id"])
        self.assertTrue(stale["stale"])
        self.assertEqual(stale["status"], "stale")
        f1 = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])
        self.assertEqual(f1["status"], "pending_review")
        # 旧修订号更新中断 -> 冲突并带最新状态。
        with self.assertRaises(ApiError) as ctx:
            self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                       {"expected_revision": 1, "ends_at": iso(self.base + timedelta(hours=9))})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        self.assertEqual(ctx.exception.details["current_revision"], 2)

    def test_stale_plan_restore_only_reverts_own_writes_executed_kept(self):
        # 同一方案承载 f1、f2；f2 先执行。方案过期后只有 f1 转待复核，恢复时只撤未执行的 f1。
        f1 = self.make_flight("AB201", "AC1", "CR1")
        f2 = self.make_flight("AB202", "AC1", "CR1")
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "双航班方案", "assignments": [
            {"flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5))},
            {"flight_id": f2["id"], "aircraft_id": "AC3", "crew_id": "CR3", "new_std": iso(self.base + timedelta(hours=4)), "new_sta": iso(self.base + timedelta(hours=6))},
        ]})
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        processed = self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": locked["revision"]})
        flights = {f["id"]: f for f in self.svc.state()["flights"]}
        self.assertEqual(flights[f2["id"]]["status"], "scheduled")
        executed = self.svc.execute_flight(f2["id"], "sched", "scheduler", {"expected_revision": flights[f2["id"]]["revision"]})["flight"]
        self.assertEqual(executed["status"], "executed")
        out = self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                         {"expected_revision": 1, "ends_at": iso(self.base + timedelta(hours=8))})
        self.assertEqual(out["pending_review_flight_ids"], [f1["id"]])
        restored = self.svc.restore_plan(plan["id"], "ops", "ops_manager", {"expected_revision": processed["revision"]})
        self.assertEqual(restored["restore_result"]["restored"], [f1["id"]])
        self.assertEqual(restored["restore_result"]["skipped"], [{"flight_id": f2["id"], "reason": "executed"}])
        flights = {f["id"]: f for f in self.svc.state()["flights"]}
        self.assertEqual(flights[f1["id"]]["aircraft_id"], "AC1")
        self.assertEqual(flights[f1["id"]]["crew_id"], "CR1")
        self.assertEqual(flights[f1["id"]]["status"], "scheduled")
        self.assertEqual(flights[f2["id"]]["status"], "executed")
        self.assertEqual(flights[f2["id"]]["aircraft_id"], "AC3")  # 已执行的值不动
        with self.assertRaises(ApiError) as ctx:
            self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": restored["revision"]})
        self.assertEqual(ctx.exception.code, "plan_stale")

    def test_process_failure_restores_original_and_retry_is_idempotent(self):
        flight_a, disruption, plan_a = self.build_locked_processed("AB301")
        # 再挂一个基于当前修订(1)的草案，随后更新中断到修订 2。
        flight = self.make_flight("AB302")
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "失败方案", "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5))}]})
        self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                   {"expected_revision": 1, "ends_at": iso(self.base + timedelta(hours=8))})
        # 草案依赖中断修订 1，锁定时被告知过期，需要 rebase。
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "plan_outdated")
        plan = self.svc.rebase_plan(plan["id"], "sched", "scheduler", {"expected_revision": 1})
        self.assertEqual(plan["disruption_revision"], 2)
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": plan["revision"]})
        # 第一次处理失败：自动恢复原方案（旧方案 A 已 stale，不占资源）。
        failed = self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": locked["revision"], "force_fail": True})
        self.assertEqual(failed["apply_status"], "failed")
        self.assertTrue(failed["processing_failed"])
        self.assertEqual(failed["result"]["restored"], [flight["id"]])
        flights = {f["id"]: f for f in self.svc.state()["flights"]}
        self.assertEqual(flights[flight["id"]]["aircraft_id"], "AC1")
        self.assertIsNone(flights[flight["id"]]["writer_assignment_id"])
        # 重试不重复改航班（applied 已回滚，只落一次值）。
        retried = self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": locked["revision"]})
        self.assertEqual(retried["apply_status"], "succeeded")
        self.assertEqual(retried["result"]["attempts"], 2)
        flights = {f["id"]: f for f in self.svc.state()["flights"]}
        self.assertEqual(flights[flight["id"]]["aircraft_id"], "AC2")
        # 成功后再次处理 -> 幂等返回，不再改航班。
        again = self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": retried["revision"]})
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["result"]["attempts"], 2)

    def test_restore_only_touches_own_plan_values(self):
        # plan A 写 f1；人工取消恢复/另一个方案写 f2，恢复 A 只动 f1。
        f1 = self.make_flight("AB401", "AC1", "CR1")
        f2 = self.make_flight("AB402", "AC2", "CR2")
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "airport_closure", "resource_id": "AAA", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=2))})
        plan_a = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "甲方案", "assignments": [{"flight_id": f1["id"], "aircraft_id": "AC3", "crew_id": "CR3", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5))}]})
        la = self.svc.lock_plan(plan_a["id"], "ops", "ops_manager", {"expected_revision": 1})
        pa = self.svc.process_plan(plan_a["id"], "ops", "ops_manager", {"expected_revision": la["revision"]})
        # f2 随后被另一个方案写到 AC3——时间不重叠（晚 5 小时），不会资源冲突。
        plan_b = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "乙方案", "assignments": [{"flight_id": f2["id"], "aircraft_id": "AC3", "crew_id": "CR3", "new_std": iso(self.base + timedelta(hours=8)), "new_sta": iso(self.base + timedelta(hours=10))}]})
        plan_b = self.svc.rebase_plan(plan_b["id"], "sched", "scheduler", {"expected_revision": plan_b["revision"]})
        lb = self.svc.lock_plan(plan_b["id"], "ops", "ops_manager", {"expected_revision": plan_b["revision"]})
        self.svc.process_plan(plan_b["id"], "ops", "ops_manager", {"expected_revision": lb["revision"]})
        # 恢复甲方案：只撤 f1；f2 保留乙方案写入。
        out = self.svc.restore_plan(plan_a["id"], "ops", "ops_manager", {"expected_revision": pa["revision"]})
        self.assertEqual(out["restore_result"]["restored"], [f1["id"]])
        flights = {f["id"]: f for f in self.svc.state()["flights"]}
        self.assertEqual(flights[f1["id"]]["aircraft_id"], "AC1")
        self.assertEqual(flights[f2["id"]]["aircraft_id"], "AC3")

    def test_concurrent_lock_same_revision_second_gets_conflict(self):
        flight = self.make_flight("AB501")
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "并发锁", "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5))}]})
        errors, locked_holder = [], {}
        barrier = threading.Barrier(2)
        def lock_scheduler():
            barrier.wait()
            try:
                locked_holder.setdefault("plan", self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1}))
            except ApiError as exc:
                errors.append(exc)
        t1 = threading.Thread(target=lock_scheduler); t2 = threading.Thread(target=lock_scheduler)
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "revision_conflict")
        self.assertEqual(errors[0].details["current_status"], "locked")
        self.assertEqual(locked_holder["plan"]["revision"], 2)
        # 成功方处理后，后到一方拿旧修订号处理 -> 冲突。
        with self.assertRaises(ApiError) as ctx:
            self.svc.process_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "revision_conflict")

    def test_review_flight_keep_and_restore(self):
        flight, disruption, plan = self.build_locked_processed("AB601")
        self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                   {"expected_revision": 1, "ends_at": iso(self.base + timedelta(hours=8))})
        state = {f["id"]: f for f in self.svc.state()["flights"]}
        pending_rev = state[flight["id"]]["revision"]
        # 待复核航班不能直接执行。
        with self.assertRaises(ApiError) as ctx:
            self.svc.execute_flight(flight["id"], "sched", "scheduler", {"expected_revision": pending_rev})
        self.assertEqual(ctx.exception.code, "review_required")
        reviewed = self.svc.review_flight(flight["id"], "sched", "scheduler", {"expected_revision": pending_rev, "decision": "keep"})["flight"]
        self.assertEqual(reviewed["status"], "scheduled")
        self.assertEqual(reviewed["aircraft_id"], "AC2")  # 接受现值
        self.assertIsNone(reviewed["writer_assignment_id"])
        # 旧版本号复核 -> 冲突。
        with self.assertRaises(ApiError) as ctx:
            self.svc.review_flight(flight["id"], "sched", "scheduler", {"expected_revision": pending_rev, "decision": "keep"})
        self.assertEqual(ctx.exception.code, "revision_conflict")


if __name__ == "__main__": unittest.main()
