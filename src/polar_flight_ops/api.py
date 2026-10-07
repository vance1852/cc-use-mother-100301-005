"""飞行窗口与任务承诺服务的 HTTP/JSON 边界。

/flight-ops/* 路径由本模块处理，其余路径回退到基础服务路由。
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from polar_station_foundation.api import route as foundation_route
from polar_station_foundation.errors import DomainError, ValidationError
from polar_station_foundation.storage import Database

from .service import FlightOpsService


def _created(response: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    return (200 if response.get("replayed") else 201), response


def _flight_route(service: FlightOpsService, method: str, segments: list[str],
                  query: dict[str, list[str]], body: dict[str, Any],
                  actor_id: str) -> tuple[int, dict[str, Any]]:
    if segments == ["flight-ops", "schedule"] and method == "GET":
        return 200, service.schedule_status()
    if segments == ["flight-ops", "aircraft"] and method == "POST":
        return _created(service.register_aircraft(actor_id=actor_id, **body))
    if segments == ["flight-ops", "crew"] and method == "POST":
        return _created(service.register_crew(actor_id=actor_id, **body))
    if segments == ["flight-ops", "windows"] and method == "POST":
        return _created(service.register_window(actor_id=actor_id, **body))
    if segments == ["flight-ops", "weather-forecasts"] and method == "POST":
        return _created(service.register_weather_forecast(actor_id=actor_id, **body))
    if segments == ["flight-ops", "ground-support"] and method == "POST":
        return _created(service.register_ground_support(actor_id=actor_id, **body))
    if segments == ["flight-ops", "missions"] and method == "POST":
        return _created(service.register_mission(actor_id=actor_id, **body))
    if segments == ["flight-ops", "candidates"] and method == "GET":
        mission_id = query.get("mission_id", [""])[0]
        if not mission_id:
            raise ValidationError("mission_id 不能为空")
        max_results = int(query.get("max_results", ["5"])[0])
        return 200, service.plan_candidates(mission_id=mission_id, max_results=max_results)
    if segments == ["flight-ops", "plans"] and method == "POST":
        return _created(service.create_plan(actor_id=actor_id, **body))
    if segments == ["flight-ops", "plans"] and method == "GET":
        status = query.get("status", [None])[0]
        return 200, {"items": service.list_plans(status)}
    if len(segments) == 3 and segments[:2] == ["flight-ops", "plans"] and method == "GET":
        return 200, service.get_plan(segments[2])
    if (len(segments) == 4 and segments[:2] == ["flight-ops", "plans"]
            and segments[3] == "approvals" and method == "POST"):
        return _created(service.approve_plan(actor_id=actor_id, plan_id=segments[2], **body))
    if (len(segments) == 4 and segments[:2] == ["flight-ops", "plans"]
            and segments[3] == "seal" and method == "POST"):
        return _created(service.seal_plan(actor_id=actor_id, plan_id=segments[2], **body))
    if (len(segments) == 4 and segments[:2] == ["flight-ops", "legs"]
            and segments[3] == "events" and method == "POST"):
        return _created(service.record_leg_event(actor_id=actor_id, leg_id=segments[2], **body))
    if segments == ["flight-ops", "disruptions"] and method == "POST":
        return _created(service.process_disruption(actor_id=actor_id, **body))
    if segments == ["flight-ops", "waitlist"] and method == "GET":
        return 200, {"items": service.list_waitlist()}
    if segments == ["flight-ops", "waitlist", "process"] and method == "POST":
        return _created(service.process_waitlist(actor_id=actor_id, **body))
    if len(segments) == 3 and segments[:2] == ["flight-ops", "missions"] and method == "GET":
        return 200, service.get_mission(segments[2])
    if (len(segments) == 4 and segments[:2] == ["flight-ops", "missions"]
            and segments[3] == "explanation" and method == "GET"):
        return 200, service.mission_explanation(segments[2])
    return 404, {"error": "route_not_found", "message": "接口不存在"}


def route(service: FlightOpsService, method: str, path: str, body: dict[str, Any] | None,
          headers: dict[str, str] | None = None) -> tuple[int, dict[str, Any]]:
    """把一个 HTTP 语义请求分派到飞行服务或基础服务。"""

    headers = headers or {}
    body = body or {}
    parsed = urlparse(path)
    segments = [segment for segment in parsed.path.split("/") if segment]
    actor_id = headers.get("X-Actor-Id", "")
    try:
        if segments and segments[0] == "flight-ops":
            return _flight_route(service, method, segments, parse_qs(parsed.query), body, actor_id)
        return foundation_route(service, method, path, body, headers)
    except DomainError as exc:
        return exc.status, {"error": exc.code, "message": str(exc)}
    except (TypeError, ValueError) as exc:
        return 400, {"error": "invalid_request", "message": str(exc)}


class Handler(BaseHTTPRequestHandler):
    """把标准库 HTTP 请求转换为路由调用。"""

    service: FlightOpsService

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._write(400, {"error": "invalid_json", "message": "请求体必须是 UTF-8 JSON"})
            return
        status, payload = route(self.service, self.command, self.path, body,
                                {"X-Actor-Id": self.headers.get("X-Actor-Id", "")})
        self._write(status, payload)

    def _write(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> int:
    """启动飞行窗口与任务承诺 HTTP 服务。"""

    parser = argparse.ArgumentParser(description="启动极地飞行窗口与任务承诺服务")
    parser.add_argument("--database", default="flight_ops.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    database = Database(args.database)
    Handler.service = FlightOpsService(database)
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        database.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
