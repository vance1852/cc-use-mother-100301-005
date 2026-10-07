import unittest
from datetime import datetime, timezone

from polar_station_foundation.clock import ManualClock
from polar_station_foundation.storage import Database

from polar_flight_ops.api import route
from polar_flight_ops.service import FlightOpsService
from test_flight_ops import bootstrap, ts

T0 = datetime(2026, 10, 7, 8, 0, tzinfo=timezone.utc)
HEADERS = {"X-Actor-Id": "dispA"}


class FlightOpsApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.service = FlightOpsService(self.database, ManualClock(T0))
        bootstrap(self.service)
        self.service.register_mission(request_id="m-cal", actor_id="dispA", mission_id="CAL",
                                      mission_type="instrument_calibration", origin_site_id="main",
                                      destination_site_id="camp", distance_nm=300,
                                      alternate_site_id="alt", alternate_distance_nm=60,
                                      payload_kg=400, passengers=1,
                                      earliest_departure=ts(1, 9), latest_arrival=ts(2, 18))

    def tearDown(self):
        self.database.close()

    def test_health_delegates_to_foundation(self):
        status, payload = route(self.service, "GET", "/health", None)
        self.assertEqual(200, status)
        self.assertEqual("ok", payload["status"])

    def test_candidates_endpoint(self):
        status, payload = route(self.service, "GET", "/flight-ops/candidates?mission_id=CAL", None)
        self.assertEqual(200, status)
        self.assertGreaterEqual(payload["feasible_count"], 1)

    def test_plan_lifecycle_over_http(self):
        status, plan = route(self.service, "POST", "/flight-ops/plans",
                             {"request_id": "p1", "mission_ids": ["CAL"], "auto": True,
                              "lease_ttl_seconds": 3600}, HEADERS)
        self.assertEqual(201, status)
        self.assertEqual("leased", plan["status"])
        plan_id = plan["plan_id"]
        for side, actor in (("operations", "ops1"), ("station", "stn1")):
            status, _ = route(self.service, "POST", f"/flight-ops/plans/{plan_id}/approvals",
                              {"request_id": f"a-{side}", "side": side, "decision": "approve"},
                              {"X-Actor-Id": actor})
            self.assertEqual(201, status)
        status, sealed = route(self.service, "POST", f"/flight-ops/plans/{plan_id}/seal",
                               {"request_id": "s1", "expected_schedule_version": 0}, HEADERS)
        self.assertEqual(201, status)
        self.assertEqual("sealed", sealed["status"])
        status, schedule = route(self.service, "GET", "/flight-ops/schedule", None)
        self.assertEqual(200, status)
        self.assertEqual(1, schedule["sealed_version"])
        status, explanation = route(self.service, "GET",
                                    "/flight-ops/missions/CAL/explanation", None)
        self.assertEqual(200, status)
        self.assertEqual("committed", explanation["status"])
        self.assertEqual("获准起飞", explanation["status_text"])

    def test_unknown_flight_route_returns_404(self):
        status, payload = route(self.service, "GET", "/flight-ops/unknown", None)
        self.assertEqual(404, status)
        self.assertEqual("route_not_found", payload["error"])

    def test_invalid_body_returns_400(self):
        status, payload = route(self.service, "POST", "/flight-ops/plans",
                                {"request_id": "bad"}, HEADERS)
        self.assertEqual(400, status)
        self.assertEqual("validation_error", payload["error"])

    def test_write_without_actor_is_rejected(self):
        status, payload = route(self.service, "POST", "/flight-ops/plans",
                                {"request_id": "p1", "mission_ids": ["CAL"], "auto": True})
        self.assertEqual(404, status)
        self.assertEqual("not_found", payload["error"])


if __name__ == "__main__":
    unittest.main()
