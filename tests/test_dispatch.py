"""飞行窗口与任务承诺服务的领域测试。"""
import json
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from polar_station_foundation.clock import FixedClock
from polar_station_foundation.dispatch import DispatchService
from polar_station_foundation.errors import (
    ConflictError, PermissionDenied, ValidationError)
from polar_station_foundation.service import DomainService
from polar_station_foundation.storage import Database

T0 = datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)
W_OPEN = "2026-10-08T00:00:00Z"
W_CLOSE = "2026-10-08T12:00:00Z"
FX_VALID_TO = "2026-10-09T00:00:00Z"


class World:
    """构造一套可直接排任务的机场、飞机、机组、航程、油料与预报。"""

    def __init__(self, path=":memory:", ttl_minutes=30, clock=None):
        self.clock = clock or FixedClock(T0)
        self.database = Database(path)
        self.base = DomainService(self.database, self.clock)
        self.service = DispatchService(self.database, self.clock,
                                       lease_ttl_minutes=ttl_minutes)
        b, s = self.base, self.service
        b.register_organization(request_id="org-req", actor_id="bootstrap",
                                organization_id="org1", name="极地中心")
        b.register_actor(request_id="req-adm", actor_id="bootstrap", new_actor_id="adm",
                         display_name="管理员", role="admin", organization_id="org1")
        b.register_actor(request_id="req-op", actor_id="adm", new_actor_id="op",
                         display_name="运行调度", role="operator", organization_id="org1")
        b.register_actor(request_id="req-st", actor_id="adm", new_actor_id="st",
                         display_name="站点代表", role="reviewer", organization_id="org1")
        b.register_site(request_id="req-site", actor_id="op", site_id="s1",
                        organization_id="org1", name="内陆营地", timezone_name="UTC")
        for aircraft_id, name, payload, seats, fuel, burn, speed, xwind, rating, caps in [
                ("AC1", "双水獭", 1200, 9, 1000, 200, 150, 25, "DHC6", ["medevac_kit"]),
                ("AC2", "巴斯勒", 3000, 19, 2000, 400, 180, 30, "BT67",
                 ["medevac_kit", "oversize"])]:
            s.register_aircraft(request_id=f"req-{aircraft_id.lower()}", actor_id="op",
                                site_id="s1", aircraft_id=aircraft_id, display_name=name,
                                payload_capacity_kg=payload, seat_capacity=seats,
                                fuel_capacity_kg=fuel, burn_kg_per_hour=burn, speed_kt=speed,
                                crosswind_limit_kt=xwind, required_rating=rating,
                                capabilities=caps)
        for code, name, med in [("CB", "沿海基地", True), ("IN", "内陆营地", False),
                                ("ALT", "备降点", True)]:
            s.register_airfield(request_id=f"req-af-{code}", actor_id="op", code=code,
                                display_name=name, has_fuel=True,
                                has_ground_handling=True, medevac_capable=med, site_id="s1")
            s.add_airfield_window(request_id=f"req-win-{code}", actor_id="op",
                                  window_id=f"W-{code}", airfield_code=code,
                                  opens_at=W_OPEN, closes_at=W_CLOSE)
        s.register_crew(request_id="req-p1", actor_id="op", crew_id="P1", display_name="机长一",
                        role="pilot", qualifications=["DHC6", "BT67"],
                        duty_starts_at=W_OPEN, duty_ends_at="2026-10-08T20:00:00Z")
        s.register_crew(request_id="req-lm", actor_id="op", crew_id="LM", display_name="装卸员",
                        role="loadmaster", duty_starts_at=W_OPEN,
                        duty_ends_at="2026-10-08T20:00:00Z")
        s.register_crew(request_id="req-md", actor_id="op", crew_id="MD", display_name="医护",
                        role="medic", duty_starts_at=W_OPEN,
                        duty_ends_at="2026-10-08T20:00:00Z")
        s.register_distance(request_id="req-dist-ci", actor_id="op", origin_code="CB",
                            destination_code="IN", distance_nm=300)
        s.register_distance(request_id="req-dist-ia", actor_id="op", origin_code="IN",
                            destination_code="ALT", distance_nm=100)
        s.register_distance(request_id="req-dist-ca", actor_id="op", origin_code="CB",
                            destination_code="ALT", distance_nm=250)
        for stock_id, code, qty in [("FS-CB", "CB", 5000), ("FS-IN", "IN", 3000)]:
            s.add_fuel_stock(request_id=f"req-{stock_id.lower()}", actor_id="op",
                             stock_id=stock_id, airfield_code=code, quantity_kg=qty,
                             starts_at=W_OPEN, ends_at=FX_VALID_TO)
        for code, ceiling, vis, xwind in [
                ("CB", 2000, 10, 10), ("IN", 1500, 8, 12), ("ALT", 1800, 9, 8)]:
            s.issue_forecast(request_id=f"req-fx-{code}", actor_id="op", airfield_code=code,
                             valid_from=W_OPEN, valid_to=FX_VALID_TO, ceiling_ft=ceiling,
                             visibility_km=vis, crosswind_kt=xwind)

    def close(self):
        self.database.close()


def seal(world: World, plan_id: str, ops_req="req-ops", st_req="req-sta"):
    world.service.approve_plan(request_id=ops_req, actor_id="op", plan_id=plan_id,
                               party="operations")
    return world.service.approve_plan(request_id=st_req, actor_id="st", plan_id=plan_id,
                                      party="station")


def commitment_id(world: World, mission_id: str):
    explanation = world.service.get_mission_explanation("op", mission_id)
    return explanation["commitments"][0]["commitment_id"]


class CandidateTest(unittest.TestCase):
    def setUp(self):
        self.world = World()

    def tearDown(self):
        self.world.close()

    def test_candidates_compare_all_eight_factors(self):
        w = self.world
        w.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                 kind="supply", origin_code="CB", destination_code="IN",
                                 passengers=2, cargo_kg=800)
        candidates = w.service.build_candidates("op", "M1")
        self.assertTrue(candidates)
        codes = {f["code"] for f in candidates[0]["factors"]}
        self.assertEqual(
            {"airfield_window", "crew_qualification", "fuel_range", "payload", "alternate",
             "weather_forecast", "passenger_restriction", "ground_handling"}, codes)
        self.assertTrue(candidates[0]["feasible"])
        self.assertTrue(all(c["feasible"] for c in candidates))
        # 排序：分数高者在前。
        self.assertGreaterEqual(candidates[0]["score"], candidates[-1]["score"])

    def test_bad_weather_makes_candidate_infeasible(self):
        w = self.world
        w.service.issue_forecast(request_id="req-fx-bad", actor_id="op", airfield_code="IN",
                                 valid_from=W_OPEN, valid_to=FX_VALID_TO, ceiling_ft=200,
                                 visibility_km=1.0, crosswind_kt=40)
        w.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                 kind="supply", origin_code="CB", destination_code="IN",
                                 passengers=1, cargo_kg=100)
        candidates = w.service.build_candidates("op", "M1")
        self.assertFalse(any(c["feasible"] for c in candidates))
        bad = next(f for c in candidates for f in c["factors"]
                   if f["code"] == "weather_forecast" and f["status"] == "violation")
        self.assertFalse(bad["detail"]["destination"]["ceiling_ok"])
        with self.assertRaises(ConflictError):
            w.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")

    def test_overweight_payload_is_violation(self):
        w = self.world
        w.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                 kind="supply", origin_code="CB", destination_code="IN",
                                 passengers=0, cargo_kg=5000)
        candidates = w.service.build_candidates("op", "M1")
        self.assertFalse(any(c["feasible"] for c in candidates))
        self.assertTrue(all(
            next(f for f in c["factors"] if f["code"] == "payload")["status"] == "violation"
            for c in candidates))

    def test_missing_crew_rating_excludes_aircraft(self):
        w = self.world
        w.service.register_aircraft(request_id="req-ac3", actor_id="op", site_id="s1",
                                    aircraft_id="AC3", display_name="直升机",
                                    payload_capacity_kg=900, seat_capacity=6,
                                    fuel_capacity_kg=800, burn_kg_per_hour=180, speed_kt=120,
                                    crosswind_limit_kt=20, required_rating="HELI")
        w.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                 kind="calibration", origin_code="CB", destination_code="IN",
                                 passengers=1, cargo_kg=50)
        candidates = w.service.build_candidates("op", "M1")
        self.assertTrue(candidates)
        self.assertNotIn("AC3", {c["aircraft_id"] for c in candidates})
        self.assertEqual({"AC1", "AC2"}, {c["aircraft_id"] for c in candidates})

    def test_ground_handling_required_at_both_ends(self):
        w = self.world
        w.service.register_airfield(request_id="req-af-xx", actor_id="op", code="XX",
                                    display_name="无保障场", has_fuel=False,
                                    has_ground_handling=False, medevac_capable=False,
                                    site_id="s1")
        w.service.add_airfield_window(request_id="req-win-xx", actor_id="op", window_id="W-XX",
                                      airfield_code="XX", opens_at=W_OPEN, closes_at=W_CLOSE)
        w.service.register_distance(request_id="req-dist-cx", actor_id="op", origin_code="CB",
                                    destination_code="XX", distance_nm=200)
        w.service.issue_forecast(request_id="req-fx-xx", actor_id="op", airfield_code="XX",
                                 valid_from=W_OPEN, valid_to=FX_VALID_TO, ceiling_ft=2000,
                                 visibility_km=10, crosswind_kt=5)
        w.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                 kind="supply", origin_code="CB", destination_code="XX",
                                 passengers=1, cargo_kg=100)
        candidates = w.service.build_candidates("op", "M1")
        self.assertFalse(any(c["feasible"] for c in candidates))


class LeaseAndApprovalTest(unittest.TestCase):
    def setUp(self):
        self.world = World()

    def tearDown(self):
        self.world.close()

    def _supply(self, mission_id="M1", req="req-m1", **kwargs):
        params = dict(kind="supply", origin_code="CB", destination_code="IN",
                      passengers=1, cargo_kg=100)
        params.update(kwargs)
        self.world.service.create_mission(request_id=req, actor_id="op",
                                          mission_id=mission_id, **params)

    def test_reserve_holds_timed_leases_and_seals_after_dual_approval(self):
        w = self.world
        self._supply()
        result = w.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        self.assertEqual("reserved", result["status"])
        self.assertTrue(result["lease_ids"])
        leases = w.service.list_leases("op", status="held")["items"]
        self.assertEqual(len(result["lease_ids"]), len(leases))
        sealed = seal(w, result["plan_id"])
        self.assertTrue(sealed["sealed"])
        committed = w.service.list_leases("op", status="committed")["items"]
        self.assertEqual(len(result["lease_ids"]), len(committed))

    def test_same_party_approval_is_idempotent(self):
        w = self.world
        self._supply()
        plan_id = w.service.reserve_best(request_id="req-r1", actor_id="op",
                                         mission_id="M1")["plan_id"]
        first = w.service.approve_plan(request_id="req-a1", actor_id="op", plan_id=plan_id,
                                       party="operations")
        second = w.service.approve_plan(request_id="req-a2", actor_id="adm", plan_id=plan_id,
                                        party="operations")
        self.assertFalse(first["sealed"])
        self.assertTrue(second["already_approved"])
        self.assertFalse(second["sealed"])

    def test_station_party_requires_station_role(self):
        w = self.world
        self._supply()
        plan_id = w.service.reserve_best(request_id="req-r1", actor_id="op",
                                         mission_id="M1")["plan_id"]
        with self.assertRaises(PermissionDenied):
            w.service.approve_plan(request_id="req-a1", actor_id="op", plan_id=plan_id,
                                   party="station")

    def test_reject_releases_leases(self):
        w = self.world
        self._supply()
        plan_id = w.service.reserve_best(request_id="req-r1", actor_id="op",
                                         mission_id="M1")["plan_id"]
        w.service.reject_plan(request_id="req-rej", actor_id="st", plan_id=plan_id,
                              party="station", reason="窗口不合适")
        self.assertEqual(0, len(w.service.list_leases("op", status="held")["items"]))
        explanation = w.service.get_mission_explanation("op", "M1")
        self.assertEqual("delayed", explanation["plans"][0]["decision"]["type"])
        # 资源释放后可以重新保留。
        again = w.service.reserve_best(request_id="req-r2", actor_id="op", mission_id="M1")
        self.assertEqual("reserved", again["status"])

    def test_lease_expiry_sweeps_and_allows_new_reservation(self):
        world = World(ttl_minutes=30)
        world.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                     kind="supply", origin_code="CB", destination_code="IN",
                                     passengers=1, cargo_kg=100)
        plan_id = world.service.reserve_best(request_id="req-r1", actor_id="op",
                                             mission_id="M1")["plan_id"]
        world.clock._value = datetime(2026, 10, 7, 1, 0, tzinfo=timezone.utc)
        # 租约过期后批准必须失败。
        with self.assertRaises(ConflictError):
            world.service.approve_plan(request_id="req-a1", actor_id="op", plan_id=plan_id,
                                       party="operations")
        # 过期方案不再挡路，可在新一轮保留。
        again = world.service.reserve_best(request_id="req-r2", actor_id="op", mission_id="M1")
        self.assertEqual("reserved", again["status"])
        world.close()

    def test_concurrent_seal_has_single_winner(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = str(Path(directory.name) / "concurrent.sqlite3")
        world = World(path)
        world.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                     kind="supply", origin_code="CB", destination_code="IN",
                                     passengers=1, cargo_kg=100)
        plan_id = world.service.reserve_best(request_id="req-r1", actor_id="op",
                                             mission_id="M1")["plan_id"]
        outcomes = []

        def approve(actor, party, req):
            db = Database(path)
            try:
                svc = DispatchService(db, world.clock)
                barrier.wait()
                try:
                    result = svc.approve_plan(request_id=req, actor_id=actor,
                                              plan_id=plan_id, party=party)
                    outcomes.append(result["sealed"])
                except ConflictError:
                    outcomes.append(False)
            finally:
                db.close()

        barrier = threading.Barrier(2)
        threads = [
            threading.Thread(target=approve, args=("op", "operations", "req-ops-x")),
            threading.Thread(target=approve, args=("st", "station", "req-sta-x")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        sealed_count = world.database.connection.execute(
            "SELECT COUNT(*) FROM dispatch_plans WHERE plan_id=? AND status='sealed'",
            (plan_id,)).fetchone()[0]
        commitments = world.database.connection.execute(
            "SELECT COUNT(*) FROM dispatch_commitments WHERE plan_id=?", (plan_id,)).fetchone()[0]
        self.assertEqual(1, sealed_count)
        self.assertEqual(1, commitments)
        self.assertEqual(1, sum(1 for x in outcomes if x))
        world.close()

    def test_reserve_is_idempotent_and_does_not_double_hold(self):
        w = self.world
        self._supply()
        first = w.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        second = w.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["plan_id"], second["plan_id"])
        held = w.service.list_leases("op", status="held")["items"]
        self.assertEqual(len(first["lease_ids"]), len(held))


class ContentionAndWaitlistTest(unittest.TestCase):
    def setUp(self):
        self.world = World()

    def tearDown(self):
        self.world.close()

    def test_overlapping_lease_blocks_and_enqueues_waitlist(self):
        w = self.world
        for mission_id, req in [("M1", "req-m1"), ("M2", "req-m2")]:
            w.service.create_mission(request_id=req, actor_id="op", mission_id=mission_id,
                                     kind="supply", origin_code="CB", destination_code="IN",
                                     passengers=1, cargo_kg=100)
        first = w.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        # 仅运行一方批准，方案仍处于待审批，持有的是软租约。
        w.service.approve_plan(request_id="req-o1", actor_id="op", plan_id=first["plan_id"],
                               party="operations")
        with self.assertRaises(ConflictError) as caught:
            w.service.reserve_best(request_id="req-r2", actor_id="op", mission_id="M2")
        detail = json.loads(str(caught.exception))
        self.assertTrue(detail["blockers"])
        waiting = [item for item in w.service.list_waitlist("op")["items"]
                   if item["mission_id"] == "M2"]
        self.assertTrue(waiting)

        # 驳回第一方案后释放资源，队首候补被提升；M2 保留成功并消费候补。
        w.service.reject_plan(request_id="req-rej", actor_id="adm", plan_id=first["plan_id"],
                              party="operations", reason="调整")
        promoted = [item for item in w.service.list_waitlist("op")["items"]
                    if item["mission_id"] == "M2" and item["status"] == "promoted"]
        self.assertTrue(promoted)
        second = w.service.reserve_best(request_id="req-r3", actor_id="op", mission_id="M2")
        self.assertEqual("reserved", second["status"])

    def test_fuel_quantity_is_shared_capacity(self):
        w = self.world
        w.database.connection.execute(
            "UPDATE dispatch_fuel_stocks SET quantity_kg=600 WHERE stock_id='FS-CB'")
        for mission_id, req in [("M1", "req-m1"), ("M2", "req-m2")]:
            w.service.create_mission(request_id=req, actor_id="op", mission_id=mission_id,
                                     kind="supply", origin_code="CB", destination_code="IN",
                                     passengers=1, cargo_kg=100)
        w.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        with self.assertRaises(ConflictError):
            w.service.reserve_best(request_id="req-r2", actor_id="op", mission_id="M2")


class AlertReplanTest(unittest.TestCase):
    def setUp(self):
        self.world = World()

    def tearDown(self):
        self.world.close()

    def _reserve_and_seal(self, mission_id="M1", req_m="req-m1", req_r="req-r1",
                          req_o="req-o1", req_s="req-s1", **kwargs):
        params = dict(kind="supply", origin_code="CB", destination_code="IN",
                      passengers=1, cargo_kg=100)
        params.update(kwargs)
        w = self.world
        w.service.create_mission(request_id=req_m, actor_id="op", mission_id=mission_id,
                                 **params)
        plan = w.service.reserve_best(request_id=req_r, actor_id="op", mission_id=mission_id)
        seal(w, plan["plan_id"], req_o, req_s)
        return plan["plan_id"]

    def test_weather_revision_replans_only_pending_commitments(self):
        w = self.world
        self._reserve_and_seal()
        cmt = commitment_id(w, "M1")
        w.service.mark_departed(request_id="req-dep", actor_id="op", commitment_id=cmt)
        result = w.service.ingest_alert(
            request_id="req-alert", actor_id="op", alert_key="WX-1",
            kind="weather_revision",
            payload={"airfield_code": "IN", "valid_from": W_OPEN, "valid_to": FX_VALID_TO,
                     "ceiling_ft": 200, "visibility_km": 1.0, "crosswind_kt": 40})
        actions = [e["action"] for e in result["effects"]]
        self.assertIn("forecast.versioned", actions)
        self.assertNotIn("mission.replanned", [a for a in actions])
        explanation = w.service.get_mission_explanation("op", "M1")
        self.assertEqual("in_flight", explanation["state"])
        self.assertEqual("departed", explanation["commitments"][0]["state"])
        w.service.mark_handover(request_id="req-ho", actor_id="st", commitment_id=cmt)

    def test_weather_revision_replans_sealed_pending_mission(self):
        w = self.world
        self._reserve_and_seal()
        result = w.service.ingest_alert(
            request_id="req-alert", actor_id="op", alert_key="WX-1",
            kind="weather_revision",
            payload={"airfield_code": "IN", "valid_from": W_OPEN, "valid_to": FX_VALID_TO,
                     "ceiling_ft": 200, "visibility_km": 1.0, "crosswind_kt": 40})
        self.assertIn({"action": "mission.replanned", "mission_id": "M1",
                       "alert_key": "WX-1"}, result["effects"])
        explanation = w.service.get_mission_explanation("op", "M1")
        self.assertEqual(2, explanation["current_round"])
        self.assertEqual("superseded", explanation["commitments"][0]["state"])
        reasons = {entry["reason"] for entry in explanation["resource_ledger"]}
        self.assertIn("weather_revision", reasons)
        movements = [entry["movement"] for entry in explanation["resource_ledger"]]
        self.assertIn("lease.release", movements)

    def test_benign_weather_revision_keeps_commitment(self):
        w = self.world
        self._reserve_and_seal()
        before = w.service.get_mission_explanation("op", "M1")
        result = w.service.ingest_alert(
            request_id="req-alert-benign", actor_id="op", alert_key="WX-OK",
            kind="weather_revision",
            payload={"airfield_code": "IN", "valid_from": W_OPEN, "valid_to": FX_VALID_TO,
                     "ceiling_ft": 1200, "visibility_km": 7.0, "crosswind_kt": 14})
        actions = [e["action"] for e in result["effects"]]
        self.assertIn("commitment.kept", actions)
        self.assertNotIn("mission.replanned", actions)
        after = w.service.get_mission_explanation("op", "M1")
        self.assertEqual(1, after["current_round"])
        self.assertEqual("committed", after["commitments"][0]["state"])
        self.assertEqual(len(before["commitments"]), len(after["commitments"]))

    def test_duplicate_alert_does_not_cancel_twice(self):
        w = self.world
        self._reserve_and_seal()
        payload = {"airfield_code": "IN", "valid_from": W_OPEN, "valid_to": FX_VALID_TO,
                   "ceiling_ft": 200, "visibility_km": 1.0, "crosswind_kt": 40}
        first = w.service.ingest_alert(request_id="req-alert", actor_id="op",
                                       alert_key="WX-1", kind="weather_revision",
                                       payload=payload)
        second = w.service.ingest_alert(request_id="req-alert2", actor_id="op",
                                        alert_key="WX-1", kind="weather_revision",
                                        payload=payload)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual([], second["effects"])
        explanation = w.service.get_mission_explanation("op", "M1")
        self.assertEqual(2, explanation["current_round"])

    def test_aircraft_fault_scoped_to_affected_aircraft(self):
        w = self.world
        # 为第二任务增开一对下午窗口，使其与第一任务时刻不重叠。
        w.service.add_airfield_window(request_id="req-win-cb2", actor_id="op",
                                      window_id="W-CB2", airfield_code="CB",
                                      opens_at="2026-10-08T13:00:00Z",
                                      closes_at="2026-10-08T23:00:00Z")
        w.service.add_airfield_window(request_id="req-win-in2", actor_id="op",
                                      window_id="W-IN2", airfield_code="IN",
                                      opens_at="2026-10-08T13:00:00Z",
                                      closes_at="2026-10-08T23:00:00Z")
        plan_one = self._reserve_and_seal(mission_id="M1", req_m="req-m1", req_r="req-r1",
                                          req_o="req-o1", req_s="req-s1", cargo_kg=100)
        aircraft_one = w.database.connection.execute(
            "SELECT aircraft_id FROM dispatch_plans WHERE plan_id=?", (plan_one,)).fetchone()[0]
        other = "AC2" if aircraft_one == "AC1" else "AC1"
        w.service.create_mission(request_id="req-m2", actor_id="op", mission_id="M2",
                                 kind="supply", origin_code="CB", destination_code="IN",
                                 passengers=1, cargo_kg=100)
        selection = next(c for c in w.service.build_candidates("op", "M2")
                         if c["aircraft_id"] == other and c["origin_window_id"] == "W-CB2")
        plan_two = w.service.reserve_plan(request_id="req-r2", actor_id="op",
                                          mission_id="M2", selection=selection)["plan_id"]
        seal(w, plan_two, "req-o2", "req-s2")
        aircraft_two = w.database.connection.execute(
            "SELECT aircraft_id FROM dispatch_plans WHERE plan_id=?", (plan_two,)).fetchone()[0]
        self.assertNotEqual(aircraft_one, aircraft_two)
        result = w.service.ingest_alert(
            request_id="req-fault", actor_id="op", alert_key="F-1",
            kind="aircraft_fault", payload={"aircraft_id": aircraft_one})
        replanned = {e.get("mission_id") for e in result["effects"]
                     if e["action"] == "mission.replanned"}
        self.assertEqual({"M1"}, replanned)
        explanation_two = w.service.get_mission_explanation("op", "M2")
        self.assertEqual(1, explanation_two["current_round"])
        self.assertEqual("sealed", explanation_two["plans"][0]["status"])

    def test_partial_unload_splits_child_and_replans_pending(self):
        w = self.world
        self._reserve_and_seal(cargo_kg=300,
                               cargo_items=[{"id": "i1", "kg": 200}, {"id": "i2", "kg": 100}])
        result = w.service.ingest_alert(
            request_id="req-unload", actor_id="op", alert_key="U-1", kind="partial_unload",
            payload={"mission_id": "M1", "cargo_item_ids": ["i2"]})
        actions = [e["action"] for e in result["effects"]]
        self.assertIn("mission.split", actions)
        self.assertIn("mission.replanned", actions)
        explanation = w.service.get_mission_explanation("op", "M1")
        self.assertEqual(200, explanation["cargo_kg"])
        self.assertEqual(1, len(explanation["child_missions"]))
        child = w.service.get_mission_explanation("op", explanation["child_missions"][0])
        self.assertEqual(100, child["cargo_kg"])

    def test_partial_unload_after_departure_keeps_commitment(self):
        w = self.world
        self._reserve_and_seal(cargo_kg=300,
                               cargo_items=[{"id": "i1", "kg": 200}, {"id": "i2", "kg": 100}])
        cmt = commitment_id(w, "M1")
        w.service.mark_departed(request_id="req-dep", actor_id="op", commitment_id=cmt)
        result = w.service.ingest_alert(
            request_id="req-unload", actor_id="op", alert_key="U-1", kind="partial_unload",
            payload={"mission_id": "M1", "cargo_item_ids": ["i2"]})
        actions = [e["action"] for e in result["effects"]]
        self.assertIn("mission.split", actions)
        self.assertNotIn("mission.replanned", actions)
        explanation = w.service.get_mission_explanation("op", "M1")
        self.assertEqual("in_flight", explanation["state"])

    def test_medevac_preempt_bumps_only_lower_priority_pending(self):
        w = self.world
        # 已完成的补给不被挤走。
        w.service.create_mission(request_id="req-mdone", actor_id="op", mission_id="DONE",
                                 kind="supply", origin_code="CB", destination_code="IN",
                                 passengers=1, cargo_kg=100)
        done_plan = w.service.reserve_best(request_id="req-rdone", actor_id="op",
                                           mission_id="DONE")["plan_id"]
        seal(w, done_plan, "req-od", "req-sd")
        done_cmt = commitment_id(w, "DONE")
        w.service.mark_departed(request_id="req-dd", actor_id="op", commitment_id=done_cmt)
        w.service.mark_handover(request_id="req-hd", actor_id="st", commitment_id=done_cmt)
        # 未起飞的低优先级任务将被挤走。
        pending_plan = self._reserve_and_seal(
            mission_id="LOW", req_m="req-mlow", req_r="req-rlow", req_o="req-ol",
            req_s="req-sl", cargo_kg=100)
        aircraft = w.database.connection.execute(
            "SELECT aircraft_id FROM dispatch_plans WHERE plan_id=?",
            (pending_plan,)).fetchone()[0]
        w.service.create_mission(request_id="req-mmed", actor_id="op", mission_id="MED",
                                 kind="medevac", origin_code="IN", destination_code="CB",
                                 passengers=2, cargo_kg=20,
                                 passenger_restriction={"medical_attendant": True})
        result = w.service.ingest_alert(
            request_id="req-pre", actor_id="op", alert_key="MED-1", kind="medevac_preempt",
            payload={"aircraft_id": aircraft, "medevac_mission_id": "MED",
                     "starts_at": W_OPEN, "ends_at": W_CLOSE})
        actions = {(e["action"], e.get("mission_id")) for e in result["effects"]}
        self.assertIn(("mission.preempted", "LOW"), actions)
        self.assertNotIn(("mission.preempted", "DONE"), actions)
        reserved = [e for e in result["effects"] if e["action"] == "plan.priority_reserved"]
        self.assertEqual(1, len(reserved))
        low = w.service.get_mission_explanation("op", "LOW")
        self.assertEqual(2, low["current_round"])
        waiting = [item for item in w.service.list_waitlist("op")["items"]
                   if item["mission_id"] == "LOW" and item["resource_key"] == aircraft]
        self.assertTrue(waiting)
        med = w.service.get_mission_explanation("op", "MED")
        self.assertEqual("pending_approval", med["plans"][0]["decision"]["type"])


class ExplanationAndRecoveryTest(unittest.TestCase):
    def test_explanation_tracks_every_release_and_reacquire(self):
        world = World()
        world.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                     kind="supply", origin_code="CB", destination_code="IN",
                                     passengers=1, cargo_kg=100)
        first = world.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        world.service.reject_plan(request_id="req-rej", actor_id="st", plan_id=first["plan_id"],
                                  party="station", reason="改期")
        world.service.reserve_best(request_id="req-r2", actor_id="op", mission_id="M1")
        explanation = world.service.get_mission_explanation("op", "M1")
        movements = [(entry["movement"], entry["resource_type"]) for entry in
                     explanation["resource_ledger"]]
        self.assertIn(("lease.release", "aircraft"), movements)
        self.assertGreaterEqual(movements.count(("lease.acquire", "aircraft")), 2)
        self.assertTrue(all(entry["reason"] for entry in explanation["resource_ledger"]))
        world.close()

    def test_recovery_preserves_leases_waitlist_and_pending_plan(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = str(Path(directory.name) / "recovery.sqlite3")
        world = World(path)
        world.service.create_mission(request_id="req-m1", actor_id="op", mission_id="M1",
                                     kind="supply", origin_code="CB", destination_code="IN",
                                     passengers=1, cargo_kg=100)
        result = world.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        world.close()

        database = Database(path)
        service = DispatchService(database, world.clock)
        held = service.list_leases("op", status="held")["items"]
        self.assertEqual(len(result["lease_ids"]), len(held))
        sealed = service.approve_plan(request_id="req-a1", actor_id="op",
                                      plan_id=result["plan_id"], party="operations")
        self.assertFalse(sealed["sealed"])
        final = service.approve_plan(request_id="req-a2", actor_id="st",
                                     plan_id=result["plan_id"], party="station")
        self.assertTrue(final["sealed"])
        database.close()

    def test_waitlist_survives_restart_in_order(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = str(Path(directory.name) / "order.sqlite3")
        world = World(path)
        for mission_id, req in [("M1", "req-m1"), ("M2", "req-m2"), ("M3", "req-m3")]:
            world.service.create_mission(request_id=req, actor_id="op", mission_id=mission_id,
                                         kind="supply", origin_code="CB",
                                         destination_code="IN", passengers=1, cargo_kg=100)
        first = world.service.reserve_best(request_id="req-r1", actor_id="op", mission_id="M1")
        seal(world, first["plan_id"], "req-o1", "req-s1")
        for req, mission_id in [("req-r2", "M2"), ("req-r3", "M3")]:
            with self.assertRaises(ConflictError):
                world.service.reserve_best(request_id=req, actor_id="op", mission_id=mission_id)
        world.close()

        database = Database(path)
        service = DispatchService(database, world.clock)
        items = [row for row in service.list_waitlist("op")["items"]
                 if row["resource_type"] == "aircraft"]
        order = [(row["mission_id"], row["seq"]) for row in items]
        m2_seq = next(seq for mid, seq in order if mid == "M2")
        m3_seq = next(seq for mid, seq in order if mid == "M3")
        self.assertLess(m2_seq, m3_seq)
        database.close()


if __name__ == "__main__":
    unittest.main()
