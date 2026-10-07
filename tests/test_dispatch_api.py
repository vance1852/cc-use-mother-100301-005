"""调度服务 HTTP 路由测试。"""
import json
import unittest

from polar_station_foundation.api import route
from polar_station_foundation.dispatch import DispatchService
from polar_station_foundation.service import DomainService
from polar_station_foundation.storage import Database


class DispatchApiTest(unittest.TestCase):
    def setUp(self):
        self.database = Database()
        self.base = DomainService(self.database)
        self.service = DispatchService(self.database)
        self.base.register_organization(request_id="org-req", actor_id="bootstrap",
                                        organization_id="org1", name="极地中心")
        self.base.register_actor(request_id="req-adm", actor_id="bootstrap", new_actor_id="adm",
                                 display_name="管理员", role="admin", organization_id="org1")
        self.base.register_actor(request_id="req-op", actor_id="adm", new_actor_id="op",
                                 display_name="运行调度", role="operator",
                                 organization_id="org1")
        self.base.register_actor(request_id="req-st", actor_id="adm", new_actor_id="st",
                                 display_name="站点代表", role="reviewer",
                                 organization_id="org1")
        self.base.register_site(request_id="req-site", actor_id="op", site_id="s1",
                                organization_id="org1", name="营地", timezone_name="UTC")
        headers = {"X-Actor-Id": "op"}
        self.call("POST", "/dispatch/aircraft", {
            "request_id": "req-ac1", "site_id": "s1", "aircraft_id": "AC1",
            "display_name": "双水獭", "payload_capacity_kg": 1200, "seat_capacity": 9,
            "fuel_capacity_kg": 1000, "burn_kg_per_hour": 200, "speed_kt": 150,
            "crosswind_limit_kt": 25, "required_rating": "DHC6"}, headers)
        for code in ("CB", "IN", "ALT"):
            self.call("POST", "/dispatch/airfields", {
                "request_id": f"req-af-{code}", "code": code, "display_name": code,
                "has_fuel": True, "has_ground_handling": True,
                "medevac_capable": True, "site_id": "s1"}, headers)
            self.call("POST", "/dispatch/airfield-windows", {
                "request_id": f"req-win-{code}", "window_id": f"W-{code}",
                "airfield_code": code, "opens_at": "2026-10-08T00:00:00Z",
                "closes_at": "2026-10-08T12:00:00Z"}, headers)
            self.call("POST", "/dispatch/forecasts", {
                "request_id": f"req-fx-{code}", "airfield_code": code,
                "valid_from": "2026-10-08T00:00:00Z",
                "valid_to": "2026-10-09T00:00:00Z", "ceiling_ft": 2000,
                "visibility_km": 10, "crosswind_kt": 10}, headers)
        self.call("POST", "/dispatch/crew", {
            "request_id": "req-p1", "crew_id": "P1", "display_name": "机长",
            "role": "pilot", "qualifications": ["DHC6"],
            "duty_starts_at": "2026-10-08T00:00:00Z",
            "duty_ends_at": "2026-10-08T20:00:00Z"}, headers)
        self.call("POST", "/dispatch/distances", {
            "request_id": "req-dist-ci", "origin_code": "CB", "destination_code": "IN",
            "distance_nm": 300}, headers)
        self.call("POST", "/dispatch/distances", {
            "request_id": "req-dist-ia", "origin_code": "IN", "destination_code": "ALT",
            "distance_nm": 100}, headers)
        self.call("POST", "/dispatch/fuel-stocks", {
            "request_id": "req-fs", "stock_id": "FS", "airfield_code": "CB",
            "quantity_kg": 5000, "starts_at": "2026-10-08T00:00:00Z",
            "ends_at": "2026-10-09T00:00:00Z"}, headers)

    def tearDown(self):
        self.database.close()

    def call(self, method, path, body=None, headers=None, actor="op"):
        head = {"X-Actor-Id": actor}
        if headers:
            head.update(headers)
        return route(self.base, method, path, body or {}, head,
                     dispatch_service=self.service)

    def test_full_flow_over_http(self):
        status, body = self.call("POST", "/dispatch/missions", {
            "request_id": "req-m1", "mission_id": "M1", "kind": "supply",
            "origin_code": "CB", "destination_code": "IN", "passengers": 1,
            "cargo_kg": 100})
        self.assertEqual(201, status)

        status, body = self.call("GET", "/dispatch/missions/M1/candidates")
        self.assertEqual(200, status)
        self.assertTrue(body["items"])
        self.assertTrue(body["items"][0]["feasible"])

        status, body = self.call("POST", "/dispatch/missions/M1/reserve",
                                 {"request_id": "req-r1"})
        self.assertEqual(201, status)
        plan_id = body["plan_id"]

        status, body = self.call("POST", f"/dispatch/plans/{plan_id}/approvals",
                                 {"request_id": "req-a1", "party": "operations"})
        self.assertFalse(body["sealed"])
        status, body = self.call("POST", f"/dispatch/plans/{plan_id}/approvals",
                                 {"request_id": "req-a2", "party": "station"}, actor="st")
        self.assertEqual(200, status)
        self.assertTrue(body["sealed"])

        status, explanation = self.call("GET", "/dispatch/missions/M1/explanation")
        self.assertEqual(200, status)
        self.assertEqual("sealed", explanation["plans"][0]["status"])
        self.assertEqual("cleared", explanation["plans"][0]["decision"]["type"])
        self.assertTrue(explanation["resource_ledger"])

    def test_station_role_cannot_approve_operations_party(self):
        self.call("POST", "/dispatch/missions", {
            "request_id": "req-m1", "mission_id": "M1", "kind": "supply",
            "origin_code": "CB", "destination_code": "IN", "passengers": 1,
            "cargo_kg": 100})
        _, reserved = self.call("POST", "/dispatch/missions/M1/reserve",
                                {"request_id": "req-r1"})
        status, body = self.call("POST",
                                 f"/dispatch/plans/{reserved['plan_id']}/approvals",
                                 {"request_id": "req-a1", "party": "operations"},
                                 actor="st")
        self.assertEqual(403, status)
        self.assertEqual("permission_denied", body["error"])

    def test_alert_route_replans_and_is_idempotent(self):
        self.call("POST", "/dispatch/missions", {
            "request_id": "req-m1", "mission_id": "M1", "kind": "supply",
            "origin_code": "CB", "destination_code": "IN", "passengers": 1,
            "cargo_kg": 100})
        _, reserved = self.call("POST", "/dispatch/missions/M1/reserve",
                                {"request_id": "req-r1"})
        self.call("POST", f"/dispatch/plans/{reserved['plan_id']}/approvals",
                  {"request_id": "req-a1", "party": "operations"})
        self.call("POST", f"/dispatch/plans/{reserved['plan_id']}/approvals",
                  {"request_id": "req-a2", "party": "station"}, actor="st")
        payload = {"alert_key": "WX-1", "kind": "weather_revision", "payload": {
            "airfield_code": "IN", "valid_from": "2026-10-08T00:00:00Z",
            "valid_to": "2026-10-09T00:00:00Z", "ceiling_ft": 200,
            "visibility_km": 1.0, "crosswind_kt": 40}}
        status, first = self.call("POST", "/dispatch/alerts",
                                  {"request_id": "req-al1", **payload})
        self.assertEqual(201, status)
        status, second = self.call("POST", "/dispatch/alerts",
                                   {"request_id": "req-al2", **payload})
        self.assertTrue(second["replayed"])
        self.assertEqual([], second["effects"])


if __name__ == "__main__":
    unittest.main()
