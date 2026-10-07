import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from polar_station_foundation.clock import ManualClock
from polar_station_foundation.errors import ConflictError, PermissionDenied, ValidationError
from polar_station_foundation.storage import Database

from polar_flight_ops.planning import CONSTRAINT_NAMES
from polar_flight_ops.service import FlightOpsService

T0 = datetime(2026, 10, 7, 8, 0, tzinfo=timezone.utc)


def ts(day, hour, minute=0):
    base = datetime(2026, 10, 6 + day, hour, minute, tzinfo=timezone.utc)
    return base.isoformat().replace("+00:00", "Z")


def bootstrap(service):
    service.register_organization(request_id="org", actor_id="bootstrap", organization_id="o1",
                                  name="科考机构")
    service.register_actor(request_id="admin", actor_id="bootstrap", new_actor_id="admin1",
                           display_name="管理员", role="admin", organization_id="o1")
    service.register_actor(request_id="disp-a", actor_id="admin1", new_actor_id="dispA",
                           display_name="调度员甲", role="dispatcher", organization_id="o1")
    service.register_actor(request_id="disp-b", actor_id="admin1", new_actor_id="dispB",
                           display_name="调度员乙", role="dispatcher", organization_id="o1")
    service.register_actor(request_id="ops", actor_id="admin1", new_actor_id="ops1",
                           display_name="运行批准", role="ops_approver", organization_id="o1")
    service.register_actor(request_id="stn", actor_id="admin1", new_actor_id="stn1",
                           display_name="站点批准", role="station_approver", organization_id="o1")
    service.register_actor(request_id="aud", actor_id="admin1", new_actor_id="aud1",
                           display_name="审计员", role="auditor", organization_id="o1")
    for site_id, name in (("main", "主站机场"), ("camp", "内陆营地"), ("alt", "备降点")):
        service.register_site(request_id=f"site-{site_id}", actor_id="admin1", site_id=site_id,
                              organization_id="o1", name=name, timezone_name="Antarctica/Casey")
    service.register_aircraft(request_id="ac1", actor_id="dispA", aircraft_id="AC1",
                              registration="B-7001", aircraft_type="BT67", cruise_speed_kt=180,
                              fuel_capacity_kg=3000, burn_kg_per_nm=6, reserve_fuel_kg=500,
                              max_payload_kg=2000, max_passengers=12,
                              capabilities=["stretcher", "oxygen"], min_ceiling_m=100,
                              min_visibility_m=3000, max_wind_kt=25, home_site_id="main")
    service.register_crew(request_id="cap1", actor_id="dispA", crew_id="CAP1",
                          display_name="机长一", crew_role="captain", ratings=["BT67"],
                          max_duty_hours=10, base_site_id="main")
    service.register_crew(request_id="co1", actor_id="dispA", crew_id="CO1",
                          display_name="副驾一", crew_role="copilot", ratings=["BT67"],
                          max_duty_hours=10, base_site_id="main")
    for window_id, site_id, opens, closes in (
            ("W_MAIN1", "main", ts(1, 9), ts(1, 13)),
            ("W_CAMP1", "camp", ts(1, 10), ts(1, 14)),
            ("W_ALT1", "alt", ts(1, 8), ts(1, 20)),
            ("W_MAIN2", "main", ts(2, 9), ts(2, 13)),
            ("W_CAMP2", "camp", ts(2, 10), ts(2, 14)),
            ("W_ALT2", "alt", ts(2, 8), ts(2, 20))):
        service.register_window(request_id=f"w-{window_id}", actor_id="dispA", window_id=window_id,
                                site_id=site_id, opens_at=opens, closes_at=closes,
                                movement_capacity=4)
    service.register_weather_forecast(request_id="f1", actor_id="dispA", forecast_id="F1",
                                      site_id="camp", valid_from=ts(1, 6), valid_to=ts(3, 6),
                                      ceiling_m=300, visibility_m=8000, wind_kt=10)
    for support_id, site_id, support_type, capacity in (
            ("SUP_MAIN_GH", "main", "ground_handling", 3),
            ("SUP_CAMP_GH", "camp", "ground_handling", 3),
            ("SUP_MAIN_MED", "main", "medical", 1),
            ("SUP_CAMP_MED", "camp", "medical", 1)):
        service.register_ground_support(request_id=f"s-{support_id}", actor_id="dispA",
                                        support_id=support_id, site_id=site_id,
                                        support_type=support_type, available_from=ts(1, 8),
                                        available_to=ts(3, 8), capacity=capacity)


class FlightOpsTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.clock = ManualClock(T0)
        self.service = FlightOpsService(self.database, self.clock)
        bootstrap(self.service)

    def tearDown(self):
        self.database.close()

    def _mission(self, request_id, mission_id, **overrides):
        params = dict(actor_id="dispA", mission_id=mission_id, mission_type="supply",
                      origin_site_id="main", destination_site_id="camp", distance_nm=300,
                      alternate_site_id="alt", alternate_distance_nm=60, payload_kg=1000,
                      passengers=0, passenger_restrictions=[], required_support=["ground_handling"],
                      earliest_departure=ts(1, 9), latest_arrival=ts(2, 18), allow_split=False)
        params.update(overrides)
        return self.service.register_mission(request_id=request_id, **params)

    def _standard_missions(self):
        self._mission("m-med", "MED", mission_type="medical_evacuation", payload_kg=200,
                      passengers=2, passenger_restrictions=["stretcher"],
                      required_support=["ground_handling", "medical"])
        self._mission("m-cal", "CAL", mission_type="instrument_calibration", payload_kg=400,
                      passengers=1)
        self._mission("m-sup", "SUP", mission_type="supply", payload_kg=1200)

    def _approve_and_seal(self, plan_id, version, seq=""):
        self.service.approve_plan(request_id=f"app-ops{seq}", actor_id="ops1", plan_id=plan_id,
                                  side="operations", decision="approve")
        self.service.approve_plan(request_id=f"app-stn{seq}", actor_id="stn1", plan_id=plan_id,
                                  side="station", decision="approve")
        return self.service.seal_plan(request_id=f"seal{seq}", actor_id="dispA", plan_id=plan_id,
                                      expected_schedule_version=version)

    # ------------------------------------------------------------------
    # 候选方案
    # ------------------------------------------------------------------

    def test_candidates_are_comparable(self):
        self._standard_missions()
        result = self.service.plan_candidates(mission_id="CAL")
        self.assertGreaterEqual(result["feasible_count"], 1)
        first = result["candidates"][0]
        self.assertTrue(first["feasible"])
        self.assertEqual(set(CONSTRAINT_NAMES), {item["name"] for item in first["constraints"]})
        leg = first["legs"][0]
        self.assertEqual("2026-10-07T09:00:00Z", leg["planned_departure"])
        self.assertEqual("2026-10-07T10:40:00Z", leg["planned_arrival"])
        self.assertEqual(2660.0, leg["fuel_required_kg"])
        self.assertEqual("F1", leg["forecast_id"])

    def test_candidate_respects_fuel_range(self):
        self._mission("m-far", "FAR", distance_nm=800)
        result = self.service.plan_candidates(mission_id="FAR")
        self.assertEqual(0, result["feasible_count"])
        fuel = [item for candidate in result["candidates"] for item in candidate["constraints"]
                if item["name"] == "fuel_range"]
        self.assertTrue(fuel)
        self.assertFalse(any(item["satisfied"] for item in fuel))

    def test_candidate_respects_crew_rating(self):
        self._standard_missions()
        self.service.register_aircraft(request_id="ac9", actor_id="dispA", aircraft_id="AC9",
                                       registration="B-7009", aircraft_type="L100",
                                       cruise_speed_kt=250, fuel_capacity_kg=9000,
                                       burn_kg_per_nm=8, reserve_fuel_kg=800, max_payload_kg=5000,
                                       max_passengers=20, capabilities=[], min_ceiling_m=60,
                                       min_visibility_m=1600, max_wind_kt=30, home_site_id="main")
        result = self.service.plan_candidates(mission_id="CAL")
        wrong_type = [candidate for candidate in result["candidates"]
                      if candidate["legs"][0]["aircraft_id"] == "AC9"]
        self.assertTrue(wrong_type)
        for candidate in wrong_type:
            self.assertFalse(candidate["feasible"])
            crew = next(item for item in candidate["constraints"]
                        if item["name"] == "crew_qualification")
            self.assertFalse(crew["satisfied"])

    def test_candidate_respects_weather_minima(self):
        self._standard_missions()
        self.service.register_weather_forecast(request_id="f2", actor_id="dispA", forecast_id="F2",
                                               site_id="camp", valid_from=ts(1, 6), valid_to=ts(3, 6),
                                               ceiling_m=50, visibility_m=8000, wind_kt=10)
        result = self.service.plan_candidates(mission_id="CAL")
        self.assertEqual(0, result["feasible_count"])
        weather = [item for candidate in result["candidates"] for item in candidate["constraints"]
                   if item["name"] == "weather_minima"]
        self.assertTrue(weather)
        self.assertFalse(any(item["satisfied"] for item in weather))

    def test_candidate_respects_passenger_restrictions(self):
        self._standard_missions()
        self.service.register_aircraft(request_id="ac3", actor_id="dispA", aircraft_id="AC3",
                                       registration="B-7003", aircraft_type="BT67",
                                       cruise_speed_kt=180, fuel_capacity_kg=3000,
                                       burn_kg_per_nm=6, reserve_fuel_kg=500, max_payload_kg=2000,
                                       max_passengers=12, capabilities=[], min_ceiling_m=100,
                                       min_visibility_m=3000, max_wind_kt=25, home_site_id="main")
        result = self.service.plan_candidates(mission_id="MED")
        plain = [candidate for candidate in result["candidates"]
                 if candidate["legs"][0]["aircraft_id"] == "AC3"]
        self.assertTrue(plain)
        for candidate in plain:
            self.assertFalse(candidate["feasible"])
            pax = next(item for item in candidate["constraints"] if item["name"] == "passenger_limits")
            self.assertFalse(pax["satisfied"])
        capable = [candidate for candidate in result["candidates"]
                   if candidate["legs"][0]["aircraft_id"] == "AC1"]
        self.assertTrue(any(candidate["feasible"] for candidate in capable))

    # ------------------------------------------------------------------
    # 租约与方案
    # ------------------------------------------------------------------

    def test_plan_leases_resources_and_blocks_competitor(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        self.assertEqual("leased", plan["status"])
        self.assertTrue(all(lease["status"] == "active" for lease in plan["leases"]))
        self.assertEqual("planned", self.service.get_mission("CAL")["mission"]["status"])
        conflict_leg = dict(plan["legs"][0], mission_id="SUP", leg_sequence=1)
        with self.assertRaises(ConflictError):
            self.service.create_plan(request_id="p2", actor_id="dispB", mission_ids=["SUP"],
                                     legs=[conflict_leg])

    def test_lease_expiry_releases_resources(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=600)
        self.clock.advance(601)
        self.service.process_waitlist(request_id="sweep", actor_id="dispA")
        expired = self.service.get_plan(plan["plan_id"])
        self.assertEqual("expired", expired["status"])
        self.assertTrue(all(lease["status"] == "expired" for lease in expired["leases"]))
        self.assertEqual("open", self.service.get_mission("CAL")["mission"]["status"])
        explanation = self.service.mission_explanation("CAL")
        self.assertIn("lease_expired",
                      [item["action"] for item in explanation["resource_changes"]])
        again = self.service.create_plan(request_id="p2", actor_id="dispA", mission_ids=["CAL"],
                                         auto=True, lease_ttl_seconds=3600)
        self.assertEqual("leased", again["status"])

    def test_explicit_leg_violating_constraint_rejected(self):
        self._standard_missions()
        bad_leg = dict(mission_id="CAL", leg_sequence=1, aircraft_id="AC1", captain_id="CAP1",
                       copilot_id="CO1", departure_window_id="W_MAIN1",
                       arrival_window_id="W_CAMP1", forecast_id="F1", alternate_site_id="alt",
                       planned_departure=ts(1, 9), planned_arrival=ts(1, 10, 40),
                       payload_kg=5000, passengers=1,
                       support_bindings=[{"support_id": "SUP_MAIN_GH", "site_id": "main",
                                          "support_type": "ground_handling"},
                                         {"support_id": "SUP_CAMP_GH", "site_id": "camp",
                                          "support_type": "ground_handling"}])
        with self.assertRaises(ValidationError):
            self.service.create_plan(request_id="p9", actor_id="dispA", mission_ids=["CAL"],
                                     legs=[bad_leg])

    # ------------------------------------------------------------------
    # 审批与封存
    # ------------------------------------------------------------------

    def test_dual_approval_required_before_seal(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        plan_id = plan["plan_id"]
        with self.assertRaises(ConflictError):
            self.service.seal_plan(request_id="s0", actor_id="dispA", plan_id=plan_id,
                                   expected_schedule_version=0)
        self.service.approve_plan(request_id="a1", actor_id="ops1", plan_id=plan_id,
                                  side="operations", decision="approve")
        with self.assertRaises(ConflictError):
            self.service.seal_plan(request_id="s1", actor_id="dispA", plan_id=plan_id,
                                   expected_schedule_version=0)
        self.service.approve_plan(request_id="a2", actor_id="stn1", plan_id=plan_id,
                                  side="station", decision="approve")
        sealed = self.service.seal_plan(request_id="s2", actor_id="dispA", plan_id=plan_id,
                                        expected_schedule_version=0)
        self.assertEqual("sealed", sealed["status"])
        self.assertEqual(1, sealed["sealed_version"])
        self.assertEqual("committed", self.service.get_mission("CAL")["mission"]["status"])

    def test_approval_roles_are_enforced(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        with self.assertRaises(PermissionDenied):
            self.service.approve_plan(request_id="a1", actor_id="dispA", plan_id=plan["plan_id"],
                                      side="operations", decision="approve")
        with self.assertRaises(PermissionDenied):
            self.service.approve_plan(request_id="a2", actor_id="ops1", plan_id=plan["plan_id"],
                                      side="station", decision="approve")

    def test_seal_allows_single_effective_version(self):
        self._standard_missions()
        first = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                         auto=True, lease_ttl_seconds=3600)
        second = self.service.create_plan(request_id="p2", actor_id="dispB", mission_ids=["CAL"],
                                          auto=True, lease_ttl_seconds=3600)
        sealed = self._approve_and_seal(first["plan_id"], 0, seq="-1")
        self.assertEqual(1, sealed["sealed_version"])
        with self.assertRaises(ConflictError):
            self._approve_and_seal(second["plan_id"], 0, seq="-2")
        self.assertEqual(1, self.service.schedule_status()["sealed_version"])
        third = self.service.create_plan(request_id="p3", actor_id="dispA", mission_ids=["SUP"],
                                         auto=True, lease_ttl_seconds=3600)
        self.service.approve_plan(request_id="a-ops-3", actor_id="ops1", plan_id=third["plan_id"],
                                  side="operations", decision="approve")
        self.service.approve_plan(request_id="a-stn-3", actor_id="stn1", plan_id=third["plan_id"],
                                  side="station", decision="approve")
        with self.assertRaises(ConflictError):
            self.service.seal_plan(request_id="s-stale", actor_id="dispA",
                                   plan_id=third["plan_id"], expected_schedule_version=0)
        sealed_third = self.service.seal_plan(request_id="s-ok-3", actor_id="dispA",
                                              plan_id=third["plan_id"], expected_schedule_version=1)
        self.assertEqual(2, sealed_third["sealed_version"])

    def test_seal_supersedes_unexecuted_and_keeps_departed(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA",
                                        mission_ids=["MED", "CAL", "SUP"], auto=True,
                                        lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        med_leg = next(leg for leg in plan["legs"] if leg["mission_id"] == "MED")
        self.service.record_leg_event(request_id="dep1", actor_id="dispA",
                                      leg_id=med_leg["leg_id"], event="depart")
        self.service.register_weather_forecast(request_id="f2", actor_id="dispA", forecast_id="F2",
                                               site_id="camp", valid_from=ts(1, 6), valid_to=ts(3, 6),
                                               ceiling_m=50, visibility_m=8000, wind_kt=10)
        result = self.service.process_disruption(request_id="d1", actor_id="dispA",
                                                 alert_id="alert-001",
                                                 disruption_type="weather_revision",
                                                 forecast_id="F2")
        cancelled = set(result["cancelled_legs"])
        self.assertEqual(2, len(cancelled))
        self.assertNotIn(med_leg["leg_id"], cancelled)
        self.assertEqual("in_progress", self.service.get_mission("MED")["mission"]["status"])
        self.assertEqual("open", self.service.get_mission("CAL")["mission"]["status"])
        self.assertEqual("open", self.service.get_mission("SUP")["mission"]["status"])
        again = self.service.process_disruption(request_id="d2", actor_id="dispA",
                                                alert_id="alert-001",
                                                disruption_type="weather_revision",
                                                forecast_id="F2")
        self.assertTrue(again["duplicate"])
        self.assertEqual(cancelled, set(again["cancelled_legs"]))
        cal_legs = self.service.get_mission("CAL")["legs"]
        self.assertEqual(1, len(cal_legs))
        self.assertEqual("cancelled", cal_legs[0]["status"])

    def test_weather_revision_keeps_unaffected_legs(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        # 修订后的预报仍然满足机型标准 → 航段不受影响
        self.service.register_weather_forecast(request_id="f2", actor_id="dispA", forecast_id="F2",
                                               site_id="camp", valid_from=ts(1, 6), valid_to=ts(3, 6),
                                               ceiling_m=200, visibility_m=6000, wind_kt=15)
        result = self.service.process_disruption(request_id="d1", actor_id="dispA",
                                                 alert_id="alert-002",
                                                 disruption_type="weather_revision",
                                                 forecast_id="F2")
        self.assertEqual([], result["cancelled_legs"])
        self.assertEqual("committed", self.service.get_mission("CAL")["mission"]["status"])

    # ------------------------------------------------------------------
    # 扰动
    # ------------------------------------------------------------------

    def test_aircraft_failure_only_cancels_unexecuted(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA",
                                        mission_ids=["CAL", "SUP"], auto=True,
                                        lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        cal_leg = next(leg for leg in plan["legs"] if leg["mission_id"] == "CAL")
        self.service.record_leg_event(request_id="dep1", actor_id="dispA",
                                      leg_id=cal_leg["leg_id"], event="depart")
        result = self.service.process_disruption(request_id="d1", actor_id="dispA",
                                                 alert_id="alert-010",
                                                 disruption_type="aircraft_failure",
                                                 aircraft_id="AC1")
        self.assertEqual(1, len(result["cancelled_legs"]))
        self.assertNotIn(cal_leg["leg_id"], result["cancelled_legs"])
        self.assertEqual("in_progress", self.service.get_mission("CAL")["mission"]["status"])
        self.assertEqual("open", self.service.get_mission("SUP")["mission"]["status"])
        duplicate = self.service.process_disruption(request_id="d2", actor_id="dispA",
                                                    alert_id="alert-010",
                                                    disruption_type="aircraft_failure",
                                                    aircraft_id="AC1")
        self.assertTrue(duplicate["duplicate"])

    def test_partial_offload_creates_child_mission(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["SUP"],
                                        auto=True, lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        result = self.service.process_disruption(request_id="d1", actor_id="dispA",
                                                 alert_id="alert-020",
                                                 disruption_type="partial_offload",
                                                 mission_id="SUP", offloaded_kg=500)
        mission = self.service.get_mission("SUP")["mission"]
        self.assertEqual(700.0, mission["payload_kg"])
        child = self.service.get_mission(result["child_mission_id"])["mission"]
        self.assertEqual(500.0, child["payload_kg"])
        self.assertEqual("SUP", child["parent_mission_id"])
        explanation = self.service.mission_explanation("SUP")
        self.assertIn("payload_reduced",
                      [item["action"] for item in explanation["resource_changes"]])
        duplicate = self.service.process_disruption(request_id="d2", actor_id="dispA",
                                                    alert_id="alert-020",
                                                    disruption_type="partial_offload",
                                                    mission_id="SUP", offloaded_kg=500)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(result["child_mission_id"], duplicate["child_mission_id"])
        self.assertEqual(700.0, self.service.get_mission("SUP")["mission"]["payload_kg"])

    def test_medical_insertion_preempts_lower_priority(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["SUP"],
                                        auto=True, lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        self._mission("m-emg", "EMG", mission_type="emergency_medical", payload_kg=100,
                      passengers=1, passenger_restrictions=["stretcher"],
                      required_support=["ground_handling", "medical"], latest_arrival=ts(1, 11))
        result = self.service.process_disruption(request_id="d1", actor_id="dispA",
                                                 alert_id="alert-030",
                                                 disruption_type="medical_insertion",
                                                 mission_id="EMG")
        self.assertEqual("planned_after_preemption", result["outcome"])
        self.assertEqual(["SUP"], result["preempted_missions"])
        # 被挤出的 SUP 进入候补后随即被重排到更晚空位
        self.assertEqual(["SUP"], [item["mission_id"] for item in result["waitlist_granted"]])
        self.assertEqual("planned", self.service.get_mission("SUP")["mission"]["status"])
        self.assertEqual([], self.service.list_waitlist())
        emergency_plan = self.service.get_plan(result["plan_id"])
        self.assertEqual("leased", emergency_plan["status"])
        self.assertEqual("2026-10-07T09:00:00Z", emergency_plan["legs"][0]["planned_departure"])
        supply_legs = [leg for leg in self.service.get_mission("SUP")["legs"]
                       if leg["status"] == "leased"]
        self.assertEqual("2026-10-07T10:40:00Z", supply_legs[0]["planned_departure"])
        explanation = self.service.mission_explanation("SUP")
        decisions = [item["decision"] for item in explanation["decisions"]]
        self.assertIn("preempted", decisions)
        self.assertIn("replanned", decisions)
        self.assertIn("delayed", decisions)

    def test_waitlist_order_and_processing(self):
        self._standard_missions()
        blocker = self.service.create_plan(request_id="p0", actor_id="dispA",
                                           mission_ids=["SUP", "CAL"], auto=True,
                                           lease_ttl_seconds=600)
        self.assertEqual("leased", blocker["status"])
        self._mission("m-med2", "MED2", mission_type="medical_evacuation", payload_kg=100,
                      passengers=1, passenger_restrictions=["stretcher"],
                      required_support=["ground_handling", "medical"], latest_arrival=ts(1, 11))
        self._mission("m-cal2", "CAL2", mission_type="instrument_calibration", payload_kg=100,
                      latest_arrival=ts(1, 13))
        first = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["MED2"],
                                         auto=True, waitlist_if_blocked=True)
        second = self.service.create_plan(request_id="p2", actor_id="dispA", mission_ids=["CAL2"],
                                          auto=True, waitlist_if_blocked=True)
        self.assertEqual(["MED2"], first["waitlisted"])
        self.assertEqual(["CAL2"], second["waitlisted"])
        self.assertEqual(["MED2", "CAL2"],
                         [item["mission_id"] for item in self.service.list_waitlist()])
        self.clock.advance(601)
        result = self.service.process_waitlist(request_id="sweep", actor_id="dispA")
        self.assertEqual(["MED2", "CAL2"],
                         [item["mission_id"] for item in result["granted"]])
        self.assertEqual([], self.service.list_waitlist())
        self.assertEqual("planned", self.service.get_mission("MED2")["mission"]["status"])

    # ------------------------------------------------------------------
    # 恢复与解释
    # ------------------------------------------------------------------

    def test_recovery_preserves_leases_waitlist_and_pending_plans(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "flight.sqlite3"
            database = Database(path)
            clock = ManualClock(T0)
            service = FlightOpsService(database, clock)
            bootstrap(service)
            service.register_mission(request_id="m-cal", actor_id="dispA", mission_id="CAL",
                                     mission_type="instrument_calibration", origin_site_id="main",
                                     destination_site_id="camp", distance_nm=300,
                                     alternate_site_id="alt", alternate_distance_nm=60,
                                     payload_kg=400, passengers=1,
                                     earliest_departure=ts(1, 9), latest_arrival=ts(2, 18))
            plan = service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                       auto=True, lease_ttl_seconds=3600)
            service.approve_plan(request_id="a1", actor_id="ops1", plan_id=plan["plan_id"],
                                 side="operations", decision="approve")
            service.register_mission(request_id="m-med2", actor_id="dispA", mission_id="MED2",
                                     mission_type="medical_evacuation", origin_site_id="main",
                                     destination_site_id="camp", distance_nm=300,
                                     alternate_site_id="alt", alternate_distance_nm=60,
                                     payload_kg=100, passengers=1,
                                     passenger_restrictions=["stretcher"],
                                     required_support=["ground_handling", "medical"],
                                     earliest_departure=ts(1, 9), latest_arrival=ts(1, 11))
            blocked = service.create_plan(request_id="p2", actor_id="dispA", mission_ids=["MED2"],
                                          auto=True, waitlist_if_blocked=True)
            self.assertEqual(["MED2"], blocked["waitlisted"])
            database.close()

            recovered_db = Database(path)
            recovered_clock = ManualClock(datetime(2026, 10, 7, 8, 10, tzinfo=timezone.utc))
            recovered = FlightOpsService(recovered_db, recovered_clock)
            restored = recovered.get_plan(plan["plan_id"])
            self.assertEqual("leased", restored["status"])
            self.assertTrue(all(lease["status"] == "active" for lease in restored["leases"]))
            self.assertEqual(1, len(restored["approvals"]))
            self.assertEqual(["MED2"], [item["mission_id"] for item in recovered.list_waitlist()])
            recovered.approve_plan(request_id="a2", actor_id="stn1", plan_id=plan["plan_id"],
                                   side="station", decision="approve")
            sealed = recovered.seal_plan(request_id="s1", actor_id="dispA",
                                         plan_id=plan["plan_id"], expected_schedule_version=0)
            self.assertEqual(1, sealed["sealed_version"])
            recovered_db.close()

    def test_explanation_reports_reasons_and_resource_changes(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        self.service.register_weather_forecast(request_id="f2", actor_id="dispA", forecast_id="F2",
                                               site_id="camp", valid_from=ts(1, 6), valid_to=ts(3, 6),
                                               ceiling_m=50, visibility_m=8000, wind_kt=10)
        self.service.process_disruption(request_id="d1", actor_id="dispA", alert_id="alert-040",
                                        disruption_type="weather_revision", forecast_id="F2")
        explanation = self.service.mission_explanation("CAL")
        decisions = [item["decision"] for item in explanation["decisions"]]
        self.assertIn("approved", decisions)
        self.assertIn("cancelled", decisions)
        cancelled = next(item for item in explanation["decisions"] if item["decision"] == "cancelled")
        self.assertIn("气象", cancelled["reason"])
        actions = [item["action"] for item in explanation["resource_changes"]]
        self.assertIn("lease_acquired", actions)
        self.assertIn("committed", actions)
        self.assertIn("released", actions)
        for change in explanation["resource_changes"]:
            self.assertTrue(change["resource_type"])
            self.assertTrue(change["resource_id"])
            self.assertTrue(change["reason"])

    def test_delayed_decision_when_replanned_later(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        # 上午气象恶化 → 取消；下午好转 → 重排到更晚窗口
        self.service.register_weather_forecast(request_id="f2", actor_id="dispA", forecast_id="F2",
                                               site_id="camp", valid_from=ts(1, 6), valid_to=ts(1, 12),
                                               ceiling_m=50, visibility_m=8000, wind_kt=10)
        self.service.process_disruption(request_id="d1", actor_id="dispA", alert_id="alert-050",
                                        disruption_type="weather_revision", forecast_id="F2")
        self.service.register_weather_forecast(request_id="f3", actor_id="dispA", forecast_id="F3",
                                               site_id="camp", valid_from=ts(1, 12),
                                               valid_to=ts(3, 6), ceiling_m=300,
                                               visibility_m=8000, wind_kt=10)
        replanned = self.service.create_plan(request_id="p2", actor_id="dispA",
                                             mission_ids=["CAL"], auto=True, lease_ttl_seconds=3600)
        new_departure = replanned["legs"][0]["planned_departure"]
        self.assertGreater(new_departure, "2026-10-07T09:00:00Z")
        explanation = self.service.mission_explanation("CAL")
        self.assertIn("delayed", [item["decision"] for item in explanation["decisions"]])

    # ------------------------------------------------------------------
    # 拆分
    # ------------------------------------------------------------------

    def test_split_mission_when_payload_exceeds_capacity(self):
        self._mission("m-big", "BIG", payload_kg=3000, allow_split=True, latest_arrival=ts(3, 18))
        candidates = self.service.plan_candidates(mission_id="BIG")
        feasible = [candidate for candidate in candidates["candidates"] if candidate["feasible"]]
        self.assertTrue(feasible)
        self.assertEqual(2, len(feasible[0]["legs"]))
        self.assertEqual([2000.0, 1000.0],
                         [leg["payload_kg"] for leg in feasible[0]["legs"]])
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["BIG"],
                                        auto=True, lease_ttl_seconds=3600)
        self.assertEqual(2, len(plan["legs"]))
        self._approve_and_seal(plan["plan_id"], 0)
        explanation = self.service.mission_explanation("BIG")
        self.assertIn("split", [item["decision"] for item in explanation["decisions"]])

    def test_mission_not_splittable_when_disallowed(self):
        self._mission("m-big", "BIG2", payload_kg=3000, allow_split=False)
        candidates = self.service.plan_candidates(mission_id="BIG2")
        self.assertEqual(0, candidates["feasible_count"])
        with self.assertRaises(ConflictError):
            self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["BIG2"],
                                     auto=True)

    # ------------------------------------------------------------------
    # 权限
    # ------------------------------------------------------------------

    def test_auditor_cannot_register_resources(self):
        with self.assertRaises(PermissionDenied):
            self.service.register_aircraft(request_id="acX", actor_id="aud1", aircraft_id="ACX",
                                           registration="B-7999", aircraft_type="BT67",
                                           cruise_speed_kt=180, fuel_capacity_kg=3000,
                                           burn_kg_per_nm=6, reserve_fuel_kg=500,
                                           max_payload_kg=2000, max_passengers=12,
                                           capabilities=[], min_ceiling_m=100,
                                           min_visibility_m=3000, max_wind_kt=25,
                                           home_site_id="main")

    def test_audit_chain_stays_valid(self):
        self._standard_missions()
        plan = self.service.create_plan(request_id="p1", actor_id="dispA", mission_ids=["CAL"],
                                        auto=True, lease_ttl_seconds=3600)
        self._approve_and_seal(plan["plan_id"], 0)
        valid, count = self.service.verify_audit()
        self.assertTrue(valid)
        self.assertGreater(count, 0)


if __name__ == "__main__":
    unittest.main()
