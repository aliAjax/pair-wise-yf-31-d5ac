import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class RevisionChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        base = utcnow() + timedelta(days=1)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.base = base

    def tearDown(self):
        self.tmp.cleanup()

    def make_flight(self, number, aircraft="AC1", crew="CR1", std=None, sta=None):
        std = std or self.base
        sta = sta or (self.base + timedelta(hours=2))
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB",
                                                              "std": iso(std), "sta": iso(sta), "aircraft_id": aircraft,
                                                              "crew_id": crew, "passenger_count": 150})

    def make_disruption(self, resource="AC1", starts_at=None, ends_at=None):
        return self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": resource,
                                                                 "starts_at": iso(starts_at or self.base - timedelta(hours=1)),
                                                                 "ends_at": iso(ends_at or self.base + timedelta(hours=3))})

    def assignment(self, flight_id, aircraft="AC2", crew="CR2", std=None, sta=None):
        return {"flight_id": flight_id, "aircraft_id": aircraft, "crew_id": crew,
                "new_std": iso(std or self.base + timedelta(hours=3)),
                "new_sta": iso(sta or self.base + timedelta(hours=5)), "missed_connections": 2}

    def test_disruption_update_expires_locked_plan_and_pending_review(self):
        flight = self.make_flight("AB200")
        disruption = self.make_disruption()
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "换飞机",
                                                           "assignments": [self.assignment(flight["id"])]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        # 锁定后航班值已写入
        applied = self.svc.get_plan(plan["id"])
        self.assertEqual(applied["status"], "locked")
        # 中断窗口更新（延长 1 小时），修订递增
        updated = self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                             {"starts_at": iso(self.base - timedelta(hours=1)),
                                              "ends_at": iso(self.base + timedelta(hours=4))})
        self.assertEqual(updated["disruption"]["revision"], 2)
        self.assertIn(plan["id"], updated["expired_plan_ids"])
        self.assertIn(flight["id"], updated["pending_review_flight_ids"])
        # 依赖旧版本的锁定方案失效
        expired = self.svc.get_plan(plan["id"])
        self.assertEqual(expired["status"], "expired")
        # 未执行航班转待复核
        flight_row = self.svc.state()["flights"]
        target = next(f for f in flight_row if f["id"] == flight["id"])
        self.assertEqual(target["status"], "pending_review")
        self.assertEqual(target["applied_plan_id"], plan["id"])

    def test_revert_only_withdraws_this_plan_writes(self):
        flight = self.make_flight("AB201")
        disruption = self.make_disruption()
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "换飞机",
                                                           "assignments": [self.assignment(flight["id"])]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        # 恢复方案：只撤掉本方案写入的航班值
        result = self.svc.revert_plan(plan["id"], "sched", "scheduler", {})
        self.assertEqual(result["plan"]["status"], "reverted")
        self.assertEqual(result["reverted"][0]["flight_id"], flight["id"])
        flight_after = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])
        self.assertEqual(flight_after["std"], iso(self.base))
        self.assertEqual(flight_after["sta"], iso(self.base + timedelta(hours=2)))
        self.assertEqual(flight_after["aircraft_id"], "AC1")
        self.assertEqual(flight_after["crew_id"], "CR1")
        self.assertIsNone(flight_after["applied_plan_id"])
        self.assertEqual(result["plan"]["assignments"][0]["status"], "reverted")

    def test_revert_skips_manual_override_and_other_plan_writes(self):
        flight = self.make_flight("AB202")
        disruption = self.make_disruption()
        plan1 = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "方案一",
                                                            "assignments": [self.assignment(flight["id"])]})
        self.svc.lock_plan(plan1["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                   {"ends_at": iso(self.base + timedelta(hours=4))})
        # 人工取消覆盖方案写入
        self.svc.cancel_flight(flight["id"], "sched", "scheduler", {"reason": "机务检查"})
        result = self.svc.revert_plan(plan1["id"], "sched", "scheduler", {})
        self.assertEqual(result["skipped"][0]["reason"], "not_current_applier")
        flight_after = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])
        self.assertEqual(flight_after["status"], "canceled")

        # 另一方案接管同一航班后，原方案恢复不得覆盖
        flight2 = self.make_flight("AB203", "AC2", "CR2")
        plan_a = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "方案A",
                                                             "assignments": [self.assignment(flight2["id"])]})
        self.svc.lock_plan(plan_a["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                   {"ends_at": iso(self.base + timedelta(hours=5))})
        plan_b = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "方案B",
                                                             "assignments": [self.assignment(flight2["id"], aircraft="AC1", crew="CR1",
                                                                                            std=self.base + timedelta(hours=6),
                                                                                            sta=self.base + timedelta(hours=8))]})
        self.svc.lock_plan(plan_b["id"], "ops", "ops_manager", {"expected_revision": 1})
        result_a = self.svc.revert_plan(plan_a["id"], "sched", "scheduler", {})
        self.assertEqual(result_a["skipped"][0]["reason"], "not_current_applier")
        flight2_after = next(f for f in self.svc.state()["flights"] if f["id"] == flight2["id"])
        self.assertEqual(flight2_after["applied_plan_id"], plan_b["id"])
        self.assertEqual(flight2_after["std"], iso(self.base + timedelta(hours=6)))

    def test_concurrent_assignment_submit_conflicts_with_latest_state(self):
        flight = self.make_flight("AB204")
        disruption = self.make_disruption()
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "改派",
                                                           "assignments": [self.assignment(flight["id"])]})
        # 两人都按修订 1 提交，先到者成功
        first = self.svc.add_assignment(plan["id"], "sched", "scheduler",
                                        {"expected_revision": 1, "flight_id": flight["id"], "aircraft_id": "AC2",
                                         "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=4)),
                                         "new_sta": iso(self.base + timedelta(hours=6))})
        self.assertEqual(first["revision"], 2)
        # 后到者按最新状态报冲突
        with self.assertRaises(ApiError) as ctx:
            self.svc.add_assignment(plan["id"], "sched", "scheduler",
                                    {"expected_revision": 1, "flight_id": flight["id"], "aircraft_id": "AC1",
                                     "crew_id": "CR1", "new_std": iso(self.base + timedelta(hours=4)),
                                     "new_sta": iso(self.base + timedelta(hours=6))})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        self.assertEqual(ctx.exception.details["latest_revision"], 2)
        self.assertIsNotNone(ctx.exception.details["latest"])

    def test_concurrent_lock_conflicts_with_latest_state(self):
        flight = self.make_flight("AB205")
        disruption = self.make_disruption()
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "锁定",
                                                           "assignments": [self.assignment(flight["id"])]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        self.assertEqual(ctx.exception.details["latest_revision"], 2)

    def test_revert_then_retry_does_not_double_apply(self):
        flight = self.make_flight("AB206")
        disruption = self.make_disruption()
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "重试",
                                                           "assignments": [self.assignment(flight["id"])]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        flight_rev_after_lock = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])["revision"]
        # 重复锁定是幂等重试：不再改航班、不再占用资源
        again = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 2})
        self.assertEqual(again["status"], "locked")
        flight_rev_after_retry = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])["revision"]
        self.assertEqual(flight_rev_after_retry, flight_rev_after_lock)
        # 恢复原方案后重试：重新应用且只应用一次
        self.svc.revert_plan(plan["id"], "sched", "scheduler", {})
        reverted = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])
        self.assertIsNone(reverted["applied_plan_id"])
        relocked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 2})
        self.assertEqual(relocked["status"], "locked")
        flight_final = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])
        self.assertEqual(flight_final["std"], iso(self.base + timedelta(hours=3)))
        self.assertEqual(flight_final["applied_plan_id"], plan["id"])
        self.assertEqual(relocked["assignments"][0]["status"], "active")

    def test_executed_flight_not_reviewed_or_reverted(self):
        now = utcnow()
        # 放开宵禁与航线许可窗口，避免过去时刻触发约束校验
        for code in ("AAA", "BBB"):
            self.svc.seed_airport("ops", "ops_manager", {"code": code, "country": "CN", "curfew_start": "00:00", "curfew_end": "00:00"})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB",
                                                       "valid_from": iso(now - timedelta(days=2)),
                                                       "valid_to": iso(now + timedelta(days=10))})
        flight = self.make_flight("AB207", std=now - timedelta(hours=2), sta=now - timedelta(hours=1))
        disruption = self.make_disruption(starts_at=now - timedelta(hours=3), ends_at=now + timedelta(hours=1))
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "已执行",
                                                           "assignments": [self.assignment(flight["id"],
                                                                                          std=now - timedelta(hours=2),
                                                                                          sta=now - timedelta(hours=1))]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                   {"ends_at": iso(now + timedelta(hours=2))})
        # 已执行航班不再转待复核
        flight_after = next(f for f in self.svc.state()["flights"] if f["id"] == flight["id"])
        self.assertEqual(flight_after["status"], "scheduled")
        result = self.svc.revert_plan(plan["id"], "sched", "scheduler", {})
        self.assertEqual(result["skipped"][0]["reason"], "already_executed")

    def test_state_shows_expired_plans_and_pending_flights(self):
        flight = self.make_flight("AB208")
        disruption = self.make_disruption()
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "展示",
                                                           "assignments": [self.assignment(flight["id"])]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.svc.update_disruption(disruption["id"], "sched", "scheduler",
                                   {"ends_at": iso(self.base + timedelta(hours=4))})
        state = self.svc.state()
        plan_row = next(p for p in state["plans"] if p["id"] == plan["id"])
        self.assertEqual(plan_row["status"], "expired")
        self.assertEqual(plan_row["disruption_revision"], 1)
        flight_row = next(f for f in state["flights"] if f["id"] == flight["id"])
        self.assertEqual(flight_row["status"], "pending_review")
        disruption_row = next(d for d in state["disruptions"] if d["id"] == disruption["id"])
        self.assertEqual(disruption_row["revision"], 2)


if __name__ == "__main__":
    unittest.main()
