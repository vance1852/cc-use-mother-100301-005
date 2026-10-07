"""飞行窗口与任务承诺服务的 HTTP/JSON 路由。"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs, urlparse

from .dispatch import DispatchService
from .errors import DomainError

_PREFIX = "/dispatch"


def route_dispatch(service: DispatchService, method: str, path: str,
                   body: dict[str, Any], actor_id: str) -> tuple[int, dict[str, Any]] | None:
    """处理 /dispatch 下的请求，不匹配时返回 None。"""

    parsed = urlparse(path)
    p = parsed.path.rstrip("/")
    query = parse_qs(parsed.query)
    try:
        if not p.startswith(_PREFIX):
            return None

        if method == "POST" and p == f"{_PREFIX}/aircraft":
            result = service.register_aircraft(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/airfields":
            result = service.register_airfield(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/airfield-windows":
            result = service.add_airfield_window(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/crew":
            result = service.register_crew(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/distances":
            result = service.register_distance(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/fuel-stocks":
            result = service.add_fuel_stock(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/forecasts":
            result = service.issue_forecast(actor_id=actor_id, **body)
            return _status(result), result
        if method == "POST" and p == f"{_PREFIX}/missions":
            result = service.create_mission(actor_id=actor_id, **body)
            return _status(result), result

        # /dispatch/missions/<mission_id>/...
        segments = p.split("/")
        if len(segments) == 5 and segments[1] == "dispatch" and segments[2] == "missions":
            mission_id = segments[3]
            action = segments[4]
            if method == "GET" and action == "candidates":
                return 200, {"items": service.build_candidates(actor_id, mission_id)}
            if method == "GET" and action == "explanation":
                return 200, service.get_mission_explanation(actor_id, mission_id)
            if method == "POST" and action == "reserve":
                if "selection" in body:
                    result = service.reserve_plan(actor_id=actor_id, mission_id=mission_id, **body)
                else:
                    result = service.reserve_best(actor_id=actor_id, mission_id=mission_id, **body)
                return _status(result), result

        # /dispatch/plans/<plan_id>/...
        if len(segments) == 5 and segments[1] == "dispatch" and segments[2] == "plans":
            plan_id = segments[3]
            action = segments[4]
            if method == "POST" and action == "approvals":
                result = service.approve_plan(actor_id=actor_id, plan_id=plan_id,
                                              party=body.get("party", ""),
                                              request_id=body.get("request_id", ""))
                return 200, result
            if method == "POST" and action == "rejections":
                result = service.reject_plan(actor_id=actor_id, plan_id=plan_id,
                                             party=body.get("party", "operations"),
                                             reason=body.get("reason", ""),
                                             request_id=body.get("request_id", ""))
                return 200, result

        # /dispatch/commitments/<commitment_id>/...
        if len(segments) == 5 and segments[1] == "dispatch" and segments[2] == "commitments":
            commitment_id = segments[3]
            action = segments[4]
            if method == "POST" and action == "departures":
                result = service.mark_departed(actor_id=actor_id, commitment_id=commitment_id,
                                               request_id=body.get("request_id", ""))
                return 200, result
            if method == "POST" and action == "handovers":
                result = service.mark_handover(actor_id=actor_id, commitment_id=commitment_id,
                                               notes=body.get("notes"),
                                               request_id=body.get("request_id", ""))
                return 200, result

        if method == "POST" and p == f"{_PREFIX}/alerts":
            result = service.ingest_alert(actor_id=actor_id, **body)
            return _status(result), result
        if method == "GET" and p == f"{_PREFIX}/waitlist":
            return 200, service.list_waitlist(
                actor_id, query.get("resource_type", [None])[0],
                query.get("resource_key", [None])[0])
        if method == "GET" and p == f"{_PREFIX}/leases":
            return 200, service.list_leases(actor_id, query.get("status", [None])[0])
        return None
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}


def _status(result: dict[str, Any]) -> int:
    return 200 if result.get("replayed") else 201
