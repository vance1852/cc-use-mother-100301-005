"""飞行窗口与任务承诺服务。

在基础服务的权限、幂等、事务与审计边界上实现：
候选方案生成、关键资源限时租约、运行与站点双方批准后封存、
扰动（气象修订/飞机故障/部分卸载/紧急医疗插队）的有限重排、
候补队列、进程恢复延续以及任务级解释查询。
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import timedelta
from typing import Any, Callable

from polar_station_foundation.audit import append_event, canonical_json, digest
from polar_station_foundation.clock import Clock
from polar_station_foundation.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from polar_station_foundation.service import DomainService
from polar_station_foundation.storage import Database

from .planning import (CONSTRAINT_NAMES, Candidate, LegSpec, ReservationBook,
                       evaluate_leg_constraints, generate_candidates, iso,
                       leg_resource_requirements, parse_time)
from .schema import FLIGHT_SCHEMA


MISSION_PRIORITIES = {
    "emergency_medical": 0,
    "medical_evacuation": 1,
    "instrument_calibration": 2,
    "personnel_rotation": 3,
    "supply": 4,
}
MISSION_TYPES = frozenset(MISSION_PRIORITIES)

DEFAULT_LEASE_TTL_SECONDS = 1800
MAX_LEASE_TTL_SECONDS = 86400

EXECUTION_EVENTS = {
    "depart": ("committed", "departed"),
    "arrive": ("departed", "arrived"),
    "complete_handoff": ("arrived", "handoff_completed"),
}
UNEXECUTED_LEG_STATUSES = ("leased", "committed")

MISSION_STATUS_TEXT = {
    "open": "待规划",
    "planned": "已租约待审批",
    "committed": "获准起飞",
    "in_progress": "执行中",
    "completed": "已完成",
    "cancelled": "已取消",
}


class FlightOpsService(DomainService):
    """调度席的飞行窗口与任务承诺服务。"""

    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        super().__init__(database, clock)
        self._lock = threading.RLock()
        self.database.connection.executescript(FLIGHT_SCHEMA)
        with self._lock, self.database.transaction(immediate=True) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO schedule_state(id,sealed_version,sealed_plan_id) VALUES(1,0,NULL)")
            self._sweep_expired_leases(connection, actor_id="system")

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now(self) -> str:
        return self.clock.now().replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _number(self, value: Any, field: str, *, gt: float | None = None,
                ge: float | None = None) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError):
            raise ValidationError(f"{field} 必须是有效数字") from None
        if gt is not None and not number > gt:
            raise ValidationError(f"{field} 必须大于 {gt}")
        if ge is not None and not number >= ge:
            raise ValidationError(f"{field} 不能小于 {ge}")
        return number

    def _integer(self, value: Any, field: str, *, ge: int | None = None) -> int:
        if isinstance(value, bool):
            raise ValidationError(f"{field} 必须是整数")
        try:
            number = int(value)
        except (TypeError, ValueError):
            raise ValidationError(f"{field} 必须是整数") from None
        if ge is not None and number < ge:
            raise ValidationError(f"{field} 不能小于 {ge}")
        return number

    def _time(self, value: Any, field: str) -> str:
        try:
            return iso(parse_time(str(value)))
        except (ValueError, AttributeError):
            raise ValidationError(f"{field} 必须是 ISO 时间") from None

    def _string_list(self, value: Any, field: str) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, (list, tuple)):
            raise ValidationError(f"{field} 必须是字符串数组")
        items = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValidationError(f"{field} 必须是非空字符串数组")
            items.append(item.strip())
        return items

    def _ttl(self, value: Any) -> int:
        if value is None:
            return DEFAULT_LEASE_TTL_SECONDS
        ttl = self._integer(value, "lease_ttl_seconds")
        if ttl < 60 or ttl > MAX_LEASE_TTL_SECONDS:
            raise ValidationError("lease_ttl_seconds 需在 60 到 86400 秒之间")
        return ttl

    def _idempotent_response(self, connection, *, request_id: str, action: str,
                             payload: dict[str, Any],
                             create: Callable[[], tuple[str, str, dict[str, Any]]]) -> dict[str, Any]:
        """与基础服务同表的幂等控制，但回放时返回完整响应。"""

        request_id = self._identifier(request_id, "request_id")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            stored = json.loads(row["response_json"])
            stored["replayed"] = True
            return stored
        resource_type, resource_id, response = create()
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, resource_type, resource_id,
             canonical_json(response), self._now()))
        response["replayed"] = False
        return response

    # ------------------------------------------------------------------
    # 行读取与字典转换
    # ------------------------------------------------------------------

    def _site_row(self, connection, site_id: str):
        row = connection.execute("SELECT * FROM sites WHERE site_id=?", (site_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"场所 {site_id} 不存在")
        return row

    def _aircraft_row(self, connection, aircraft_id: str):
        row = connection.execute(
            "SELECT * FROM flight_aircraft WHERE aircraft_id=?", (aircraft_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"飞机 {aircraft_id} 不存在")
        return row

    def _crew_row(self, connection, crew_id: str):
        row = connection.execute("SELECT * FROM flight_crew WHERE crew_id=?", (crew_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"机组 {crew_id} 不存在")
        return row

    def _window_row(self, connection, window_id: str):
        row = connection.execute(
            "SELECT * FROM flight_windows WHERE window_id=?", (window_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"飞行窗口 {window_id} 不存在")
        return row

    def _mission_row(self, connection, mission_id: str):
        row = connection.execute("SELECT * FROM missions WHERE mission_id=?", (mission_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"任务 {mission_id} 不存在")
        return row

    def _plan_row(self, connection, plan_id: str):
        row = connection.execute("SELECT * FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"方案 {plan_id} 不存在")
        return row

    def _leg_row(self, connection, leg_id: str):
        row = connection.execute("SELECT * FROM legs WHERE leg_id=?", (leg_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"航段 {leg_id} 不存在")
        return row

    @staticmethod
    def _aircraft_dict(row) -> dict[str, Any]:
        data = dict(row)
        data["capabilities"] = json.loads(data["capabilities_json"])
        return data

    @staticmethod
    def _crew_dict(row) -> dict[str, Any]:
        data = dict(row)
        data["ratings"] = json.loads(data["ratings_json"])
        data["active"] = bool(data["active"])
        return data

    @staticmethod
    def _mission_dict(row) -> dict[str, Any]:
        data = dict(row)
        data["passenger_restrictions"] = json.loads(data["passenger_restrictions_json"])
        data["required_support"] = json.loads(data["required_support_json"])
        data["allow_split"] = bool(data["allow_split"])
        return data

    @staticmethod
    def _leg_dict(row) -> dict[str, Any]:
        data = dict(row)
        data["support_bindings"] = json.loads(data["support_bindings_json"])
        del data["support_bindings_json"]
        return data

    def _planning_data(self, connection) -> dict[str, Any]:
        return {
            "aircrafts": [self._aircraft_dict(row) for row in
                          connection.execute("SELECT * FROM flight_aircraft")],
            "crews": [self._crew_dict(row) for row in
                      connection.execute("SELECT * FROM flight_crew")],
            "windows": [dict(row) for row in connection.execute("SELECT * FROM flight_windows")],
            "forecasts": [dict(row) for row in connection.execute("SELECT * FROM weather_forecasts")],
            "supports": [dict(row) for row in connection.execute("SELECT * FROM ground_support")],
        }

    def _capacities(self, connection) -> dict[tuple[str, str], int]:
        capacities: dict[tuple[str, str], int] = {}
        for row in connection.execute("SELECT window_id, movement_capacity FROM flight_windows"):
            capacities[("window_movement", row["window_id"])] = int(row["movement_capacity"])
        for row in connection.execute("SELECT support_id, capacity FROM ground_support"):
            capacities[("ground_support", row["support_id"])] = int(row["capacity"])
        return capacities

    def _load_book(self, connection) -> ReservationBook:
        """把有效租约与已承诺占用加载为冲突检测账本。"""

        book = ReservationBook(self._capacities(connection))
        lease_rows = connection.execute(
            "SELECT l.lease_id AS holder_id, l.resource_type, l.resource_id, l.slot_start, l.slot_end,"
            " l.quantity, g.leg_id, g.mission_id, g.plan_id FROM leases l"
            " JOIN legs g ON g.leg_id = l.leg_id"
            " WHERE l.status='active' AND l.expires_at > ?", (self._now(),))
        for row in lease_rows:
            book.add({"resource_type": row["resource_type"], "resource_id": row["resource_id"],
                      "slot_start": row["slot_start"], "slot_end": row["slot_end"],
                      "quantity": row["quantity"], "holder": f"租约 {row['holder_id']}",
                      "holder_leg_id": row["leg_id"], "holder_mission_id": row["mission_id"],
                      "holder_plan_id": row["plan_id"]})
        allocation_rows = connection.execute(
            "SELECT a.allocation_id AS holder_id, a.resource_type, a.resource_id, a.slot_start,"
            " a.slot_end, a.quantity, g.leg_id, g.mission_id, g.plan_id FROM allocations a"
            " JOIN legs g ON g.leg_id = a.leg_id WHERE a.status='committed'")
        for row in allocation_rows:
            book.add({"resource_type": row["resource_type"], "resource_id": row["resource_id"],
                      "slot_start": row["slot_start"], "slot_end": row["slot_end"],
                      "quantity": row["quantity"], "holder": f"承诺 {row['holder_id']}",
                      "holder_leg_id": row["leg_id"], "holder_mission_id": row["mission_id"],
                      "holder_plan_id": row["plan_id"]})
        return book

    # ------------------------------------------------------------------
    # 台账与决定
    # ------------------------------------------------------------------

    def _ledger(self, connection, *, mission_id: str, leg_id: str | None, plan_id: str | None,
                action: str, resource_type: str, resource_id: str, slot_start: str | None,
                slot_end: str | None, quantity: float, reason: str, source: str) -> None:
        connection.execute(
            "INSERT INTO resource_changes(mission_id,leg_id,plan_id,action,resource_type,resource_id,"
            "slot_start,slot_end,quantity,reason,source,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (mission_id, leg_id, plan_id, action, resource_type, resource_id, slot_start, slot_end,
             quantity, reason, source, self._now()))

    def _decide(self, connection, *, mission_id: str, leg_id: str | None, decision: str,
                reason: str, actor_id: str) -> None:
        connection.execute(
            "INSERT INTO mission_decisions(mission_id,leg_id,decision,reason,actor_id,occurred_at)"
            " VALUES(?,?,?,?,?,?)",
            (mission_id, leg_id, decision, reason, actor_id, self._now()))

    def _refresh_mission(self, connection, mission_id: str) -> None:
        mission = self._mission_row(connection, mission_id)
        if mission["status"] in ("completed", "cancelled"):
            return
        rows = connection.execute(
            "SELECT status FROM legs WHERE mission_id=? AND status != 'cancelled'",
            (mission_id,)).fetchall()
        statuses = [row["status"] for row in rows]
        if not statuses:
            new_status = "open"
        elif any(status in ("departed", "arrived") for status in statuses):
            new_status = "in_progress"
        elif all(status == "handoff_completed" for status in statuses):
            new_status = "completed"
        elif any(status == "committed" for status in statuses):
            new_status = "committed"
        elif any(status == "leased" for status in statuses):
            new_status = "planned"
        else:
            new_status = "open"
        if new_status != mission["status"]:
            connection.execute("UPDATE missions SET status=? WHERE mission_id=?",
                               (new_status, mission_id))

    # ------------------------------------------------------------------
    # 资源注册
    # ------------------------------------------------------------------

    def register_aircraft(self, *, request_id: str, actor_id: str, aircraft_id: str,
                          registration: str, aircraft_type: str, cruise_speed_kt: float,
                          fuel_capacity_kg: float, burn_kg_per_nm: float, reserve_fuel_kg: float,
                          max_payload_kg: float, max_passengers: int, capabilities: list[str],
                          min_ceiling_m: float, min_visibility_m: float, max_wind_kt: float,
                          home_site_id: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "aircraft_id": aircraft_id, "registration": registration}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            aircraft_id = self._identifier(aircraft_id, "aircraft_id")
            registration = self._text(registration, "registration", 40)
            aircraft_type = self._text(aircraft_type, "aircraft_type", 40)
            numbers = {
                "cruise_speed_kt": self._number(cruise_speed_kt, "cruise_speed_kt", gt=0),
                "fuel_capacity_kg": self._number(fuel_capacity_kg, "fuel_capacity_kg", gt=0),
                "burn_kg_per_nm": self._number(burn_kg_per_nm, "burn_kg_per_nm", gt=0),
                "reserve_fuel_kg": self._number(reserve_fuel_kg, "reserve_fuel_kg", ge=0),
                "max_payload_kg": self._number(max_payload_kg, "max_payload_kg", gt=0),
                "min_ceiling_m": self._number(min_ceiling_m, "min_ceiling_m", ge=0),
                "min_visibility_m": self._number(min_visibility_m, "min_visibility_m", ge=0),
                "max_wind_kt": self._number(max_wind_kt, "max_wind_kt", ge=0),
            }
            max_passengers = self._integer(max_passengers, "max_passengers", ge=0)
            capabilities = self._string_list(capabilities, "capabilities")
            self._site_row(connection, home_site_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO flight_aircraft(aircraft_id,organization_id,registration,aircraft_type,"
                        "cruise_speed_kt,fuel_capacity_kg,burn_kg_per_nm,reserve_fuel_kg,max_payload_kg,"
                        "max_passengers,capabilities_json,min_ceiling_m,min_visibility_m,max_wind_kt,"
                        "home_site_id,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (aircraft_id, actor.organization_id, registration, aircraft_type,
                         numbers["cruise_speed_kt"], numbers["fuel_capacity_kg"], numbers["burn_kg_per_nm"],
                         numbers["reserve_fuel_kg"], numbers["max_payload_kg"], max_passengers,
                         canonical_json(capabilities), numbers["min_ceiling_m"], numbers["min_visibility_m"],
                         numbers["max_wind_kt"], home_site_id, "serviceable", self._now()))
                except Exception as exc:
                    raise ConflictError("飞机编号或注册号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="aircraft.registered",
                             resource_type="aircraft", resource_id=aircraft_id,
                             detail={"registration": registration, "aircraft_type": aircraft_type},
                             occurred_at=self._now())
                return "aircraft", aircraft_id, {"aircraft_id": aircraft_id, "registration": registration}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="register_aircraft", payload=payload, create=create)

    def register_crew(self, *, request_id: str, actor_id: str, crew_id: str, display_name: str,
                      crew_role: str, ratings: list[str], max_duty_hours: float,
                      base_site_id: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "crew_id": crew_id}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            crew_id = self._identifier(crew_id, "crew_id")
            display_name = self._text(display_name, "display_name")
            if crew_role not in ("captain", "copilot"):
                raise ValidationError("crew_role 必须是 captain 或 copilot")
            ratings = self._string_list(ratings, "ratings")
            if not ratings:
                raise ValidationError("ratings 不能为空")
            max_duty_hours = self._number(max_duty_hours, "max_duty_hours", gt=0)
            self._site_row(connection, base_site_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO flight_crew(crew_id,organization_id,display_name,crew_role,ratings_json,"
                        "max_duty_hours,base_site_id,active,created_at) VALUES(?,?,?,?,?,?,?,1,?)",
                        (crew_id, actor.organization_id, display_name, crew_role, canonical_json(ratings),
                         max_duty_hours, base_site_id, self._now()))
                except Exception as exc:
                    raise ConflictError("机组编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="crew.registered",
                             resource_type="crew", resource_id=crew_id,
                             detail={"display_name": display_name, "crew_role": crew_role},
                             occurred_at=self._now())
                return "crew", crew_id, {"crew_id": crew_id}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="register_crew", payload=payload, create=create)

    def register_window(self, *, request_id: str, actor_id: str, window_id: str, site_id: str,
                        opens_at: str, closes_at: str, movement_capacity: int) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "window_id": window_id}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            window_id = self._identifier(window_id, "window_id")
            self._site_row(connection, site_id)
            opens_at = self._time(opens_at, "opens_at")
            closes_at = self._time(closes_at, "closes_at")
            if not opens_at < closes_at:
                raise ValidationError("opens_at 必须早于 closes_at")
            movement_capacity = self._integer(movement_capacity, "movement_capacity", ge=1)

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO flight_windows(window_id,site_id,opens_at,closes_at,movement_capacity,"
                        "status,created_at) VALUES(?,?,?,?,?,'open',?)",
                        (window_id, site_id, opens_at, closes_at, movement_capacity, self._now()))
                except Exception as exc:
                    raise ConflictError("窗口编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="window.registered",
                             resource_type="flight_window", resource_id=window_id,
                             detail={"site_id": site_id, "opens_at": opens_at, "closes_at": closes_at},
                             occurred_at=self._now())
                return "flight_window", window_id, {"window_id": window_id}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="register_window", payload=payload, create=create)

    def register_weather_forecast(self, *, request_id: str, actor_id: str, forecast_id: str,
                                  site_id: str, valid_from: str, valid_to: str, ceiling_m: float,
                                  visibility_m: float, wind_kt: float) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "forecast_id": forecast_id}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            forecast_id = self._identifier(forecast_id, "forecast_id")
            self._site_row(connection, site_id)
            valid_from = self._time(valid_from, "valid_from")
            valid_to = self._time(valid_to, "valid_to")
            if not valid_from < valid_to:
                raise ValidationError("valid_from 必须早于 valid_to")
            ceiling_m = self._number(ceiling_m, "ceiling_m", ge=0)
            visibility_m = self._number(visibility_m, "visibility_m", ge=0)
            wind_kt = self._number(wind_kt, "wind_kt", ge=0)

            def create() -> tuple[str, str, dict[str, Any]]:
                previous = connection.execute(
                    "SELECT * FROM weather_forecasts WHERE site_id=? AND status='current'"
                    " ORDER BY version DESC LIMIT 1", (site_id,)).fetchone()
                version_row = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) AS max_version FROM weather_forecasts WHERE site_id=?",
                    (site_id,)).fetchone()
                version = int(version_row["max_version"]) + 1
                supersedes = previous["forecast_id"] if previous else None
                if previous:
                    connection.execute(
                        "UPDATE weather_forecasts SET status='superseded' WHERE forecast_id=?",
                        (supersedes,))
                try:
                    connection.execute(
                        "INSERT INTO weather_forecasts(forecast_id,site_id,version,valid_from,valid_to,"
                        "ceiling_m,visibility_m,wind_kt,supersedes_forecast_id,status,issued_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?,'current',?)",
                        (forecast_id, site_id, version, valid_from, valid_to, ceiling_m, visibility_m,
                         wind_kt, supersedes, self._now()))
                except Exception as exc:
                    raise ConflictError("预报编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="forecast.registered",
                             resource_type="weather_forecast", resource_id=forecast_id,
                             detail={"site_id": site_id, "version": version, "supersedes": supersedes},
                             occurred_at=self._now())
                return "weather_forecast", forecast_id, {
                    "forecast_id": forecast_id, "version": version, "supersedes": supersedes}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="register_weather_forecast", payload=payload,
                                             create=create)

    def register_ground_support(self, *, request_id: str, actor_id: str, support_id: str,
                                site_id: str, support_type: str, available_from: str,
                                available_to: str, capacity: int) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "support_id": support_id}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            support_id = self._identifier(support_id, "support_id")
            self._site_row(connection, site_id)
            support_type = self._identifier(support_type, "support_type")
            available_from = self._time(available_from, "available_from")
            available_to = self._time(available_to, "available_to")
            if not available_from < available_to:
                raise ValidationError("available_from 必须早于 available_to")
            capacity = self._integer(capacity, "capacity", ge=1)

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO ground_support(support_id,site_id,support_type,available_from,"
                        "available_to,capacity,created_at) VALUES(?,?,?,?,?,?,?)",
                        (support_id, site_id, support_type, available_from, available_to, capacity,
                         self._now()))
                except Exception as exc:
                    raise ConflictError("地面保障编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="support.registered",
                             resource_type="ground_support", resource_id=support_id,
                             detail={"site_id": site_id, "support_type": support_type},
                             occurred_at=self._now())
                return "ground_support", support_id, {"support_id": support_id}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="register_ground_support", payload=payload,
                                             create=create)

    def register_mission(self, *, request_id: str, actor_id: str, mission_id: str,
                         mission_type: str, origin_site_id: str, destination_site_id: str,
                         distance_nm: float, alternate_site_id: str | None = None,
                         alternate_distance_nm: float = 0.0, payload_kg: float = 0.0,
                         passengers: int = 0, passenger_restrictions: list[str] | None = None,
                         required_support: list[str] | None = None,
                         earliest_departure: str, latest_arrival: str,
                         allow_split: bool = False,
                         priority_class: int | None = None) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "mission_id": mission_id}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher", "operator")
            mission_id = self._identifier(mission_id, "mission_id")
            if mission_type not in MISSION_TYPES:
                raise ValidationError(f"mission_type 必须是 {sorted(MISSION_TYPES)} 之一")
            if origin_site_id == destination_site_id:
                raise ValidationError("始发与到达机场不能相同")
            self._site_row(connection, origin_site_id)
            self._site_row(connection, destination_site_id)
            if alternate_site_id is not None:
                if alternate_site_id == destination_site_id:
                    raise ValidationError("备降点不能与到达机场相同")
                self._site_row(connection, alternate_site_id)
            distance_nm = self._number(distance_nm, "distance_nm", gt=0)
            alternate_distance_nm = self._number(alternate_distance_nm, "alternate_distance_nm", ge=0)
            payload_kg = self._number(payload_kg, "payload_kg", ge=0)
            passengers = self._integer(passengers, "passengers", ge=0)
            restrictions = self._string_list(passenger_restrictions, "passenger_restrictions")
            support = self._string_list(required_support, "required_support") or ["ground_handling"]
            earliest_departure = self._time(earliest_departure, "earliest_departure")
            latest_arrival = self._time(latest_arrival, "latest_arrival")
            if not earliest_departure < latest_arrival:
                raise ValidationError("earliest_departure 必须早于 latest_arrival")
            if priority_class is None:
                priority_class = MISSION_PRIORITIES[mission_type]
            priority_class = self._integer(priority_class, "priority_class", ge=0)

            def create() -> tuple[str, str, dict[str, Any]]:
                try:
                    connection.execute(
                        "INSERT INTO missions(mission_id,organization_id,mission_type,priority_class,"
                        "origin_site_id,destination_site_id,distance_nm,alternate_site_id,"
                        "alternate_distance_nm,payload_kg,passengers,passenger_restrictions_json,"
                        "required_support_json,earliest_departure,latest_arrival,allow_split,"
                        "parent_mission_id,status,created_by,created_at)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,'open',?,?)",
                        (mission_id, actor.organization_id, mission_type, priority_class, origin_site_id,
                         destination_site_id, distance_nm, alternate_site_id, alternate_distance_nm,
                         payload_kg, passengers, canonical_json(restrictions), canonical_json(support),
                         earliest_departure, latest_arrival, 1 if allow_split else 0,
                         actor_id, self._now()))
                except Exception as exc:
                    raise ConflictError("任务编号已经存在") from exc
                append_event(connection, actor_id=actor_id, action="mission.registered",
                             resource_type="mission", resource_id=mission_id,
                             detail={"mission_type": mission_type, "priority_class": priority_class},
                             occurred_at=self._now())
                return "mission", mission_id, {"mission_id": mission_id,
                                               "priority_class": priority_class}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="register_mission", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 候选方案
    # ------------------------------------------------------------------

    def _generate(self, connection, mission: dict[str, Any], *, max_results: int,
                  check_availability: bool,
                  book: ReservationBook | None = None) -> tuple[list[Candidate], list[str]]:
        if book is None:
            book = self._load_book(connection) if check_availability else ReservationBook(
                self._capacities(connection))
        candidates, notes = generate_candidates(
            mission=mission, book=book, now=self._now(),
            check_availability=check_availability, **self._planning_data(connection))
        return candidates[:max_results], notes

    def plan_candidates(self, *, mission_id: str, max_results: int = 5) -> dict[str, Any]:
        """为任务生成可比较的候选方案（只读）。"""

        with self._lock, self.database.transaction() as connection:
            mission = self._mission_dict(self._mission_row(connection, mission_id))
            candidates, notes = self._generate(connection, mission, max_results=int(max_results),
                                               check_availability=True)
            return {"mission_id": mission_id,
                    "candidates": [candidate.to_dict() for candidate in candidates],
                    "feasible_count": sum(1 for candidate in candidates if candidate.feasible),
                    "notes": notes}

    # ------------------------------------------------------------------
    # 方案创建与租约
    # ------------------------------------------------------------------

    def _assert_mission_plannable(self, mission) -> None:
        if mission["status"] in ("completed", "cancelled"):
            raise ConflictError(f"任务 {mission['mission_id']} 已结束，不能再规划")

    def _waitlist_add(self, connection, mission: dict[str, Any], *, reason: str) -> str:
        existing = connection.execute(
            "SELECT entry_id FROM waitlist WHERE mission_id=? AND status='waiting'",
            (mission["mission_id"],)).fetchone()
        if existing:
            return existing["entry_id"]
        entry_id = uuid.uuid4().hex
        row = connection.execute("SELECT COALESCE(MAX(queue_seq), 0) AS max_seq FROM waitlist").fetchone()
        connection.execute(
            "INSERT INTO waitlist(entry_id,mission_id,priority_class,reason,status,queue_seq,created_at)"
            " VALUES(?,?,?,?,'waiting',?,?)",
            (entry_id, mission["mission_id"], int(mission["priority_class"]), reason,
             int(row["max_seq"]) + 1, self._now()))
        self._decide(connection, mission_id=mission["mission_id"], leg_id=None,
                     decision="waitlisted", reason=reason, actor_id="system")
        return entry_id

    def _create_plan_from_candidates(self, connection, *, actor_id: str,
                                     mission_specs: dict[str, list[LegSpec]],
                                     ttl_seconds: int) -> str:
        """创建方案、航段与限时租约；任何资源冲突都会整体回滚。"""

        now = self._now()
        state = connection.execute("SELECT * FROM schedule_state WHERE id=1").fetchone()
        plan_id = uuid.uuid4().hex
        expires_at = iso(parse_time(now) + timedelta(seconds=ttl_seconds))
        connection.execute(
            "INSERT INTO plans(plan_id,base_schedule_version,status,created_by,created_at)"
            " VALUES(?,?,'leased',?,?)",
            (plan_id, int(state["sealed_version"]), actor_id, now))
        windows_by_id = {row["window_id"]: dict(row) for row in
                         connection.execute("SELECT * FROM flight_windows")}
        pools_by_id = {row["support_id"]: dict(row) for row in
                       connection.execute("SELECT * FROM ground_support")}
        book = self._load_book(connection)
        lease_rows: list[tuple] = []
        for mission_id, specs in mission_specs.items():
            connection.execute("INSERT INTO plan_missions(plan_id,mission_id) VALUES(?,?)",
                               (plan_id, mission_id))
            for spec in specs:
                leg_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO legs(leg_id,plan_id,mission_id,leg_sequence,aircraft_id,captain_id,"
                    "copilot_id,departure_site_id,arrival_site_id,departure_window_id,arrival_window_id,"
                    "forecast_id,alternate_site_id,planned_departure,planned_arrival,payload_kg,passengers,"
                    "fuel_required_kg,support_bindings_json,status,created_at)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'leased',?)",
                    (leg_id, plan_id, mission_id, spec.leg_sequence, spec.aircraft_id, spec.captain_id,
                     spec.copilot_id, spec.departure_site_id, spec.arrival_site_id,
                     spec.departure_window_id, spec.arrival_window_id, spec.forecast_id,
                     spec.alternate_site_id, spec.planned_departure, spec.planned_arrival,
                     spec.payload_kg, spec.passengers, spec.fuel_required_kg,
                     canonical_json(list(spec.support_bindings)), now))
                for requirement in leg_resource_requirements(
                        spec, windows_by_id=windows_by_id, pools_by_id=pools_by_id):
                    conflicts = book.conflicts(**requirement)
                    if conflicts:
                        detail = "；".join(
                            f"{requirement['resource_type']}:{requirement['resource_id']} "
                            f"与 {hit.get('holder', '既有占用')} 冲突" for hit in conflicts)
                        raise ConflictError(f"关键资源冲突，无法建立租约：{detail}")
                    book.add({**requirement, "holder": f"方案 {plan_id}"})
                    lease_rows.append((uuid.uuid4().hex, plan_id, leg_id, mission_id,
                                       requirement["resource_type"], requirement["resource_id"],
                                       requirement["slot_start"], requirement["slot_end"],
                                       requirement["quantity"], expires_at, now))
        for (lease_id, plan_id_, leg_id, mission_id, resource_type, resource_id, slot_start,
             slot_end, quantity, expires, created) in lease_rows:
            connection.execute(
                "INSERT INTO leases(lease_id,plan_id,leg_id,resource_type,resource_id,slot_start,"
                "slot_end,quantity,expires_at,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,'active',?)",
                (lease_id, plan_id_, leg_id, resource_type, resource_id, slot_start, slot_end,
                 quantity, expires, created))
            self._ledger(connection, mission_id=mission_id, leg_id=leg_id, plan_id=plan_id_,
                         action="lease_acquired", resource_type=resource_type, resource_id=resource_id,
                         slot_start=slot_start, slot_end=slot_end, quantity=quantity,
                         reason=f"方案 {plan_id_} 建立限时租约，{expires} 过期", source=plan_id_)
        for mission_id, specs in mission_specs.items():
            self._decide(connection, mission_id=mission_id, leg_id=None, decision="planned",
                         reason=f"方案 {plan_id} 已创建，关键资源以限时租约保留", actor_id=actor_id)
            if len(specs) > 1:
                self._decide(connection, mission_id=mission_id, leg_id=None, decision="split",
                             reason=f"载荷超出单机运力，任务拆分为 {len(specs)} 个航段",
                             actor_id=actor_id)
            last_cancelled = connection.execute(
                "SELECT planned_departure FROM legs WHERE mission_id=? AND status='cancelled'"
                " ORDER BY rowid DESC LIMIT 1", (mission_id,)).fetchone()
            if last_cancelled:
                new_first = min(spec.planned_departure for spec in specs)
                if new_first > last_cancelled["planned_departure"]:
                    self._decide(connection, mission_id=mission_id, leg_id=None, decision="delayed",
                                 reason=f"航段由 {last_cancelled['planned_departure']} 延后至 {new_first}",
                                 actor_id=actor_id)
            self._refresh_mission(connection, mission_id)
        return plan_id

    def _explicit_leg_spec(self, connection, leg_input: dict[str, Any],
                           mission_ids: list[str]) -> tuple[LegSpec, dict[str, Any]]:
        if not isinstance(leg_input, dict):
            raise ValidationError("航段必须是对象")
        mission_id = leg_input.get("mission_id")
        if mission_id not in mission_ids:
            raise ValidationError("航段引用的任务不在方案任务列表中")
        mission_row = self._mission_row(connection, mission_id)
        self._assert_mission_plannable(mission_row)
        mission = self._mission_dict(mission_row)
        aircraft = self._aircraft_dict(self._aircraft_row(connection, leg_input.get("aircraft_id")))
        captain = self._crew_dict(self._crew_row(connection, leg_input.get("captain_id")))
        copilot = self._crew_dict(self._crew_row(connection, leg_input.get("copilot_id")))
        departure_window = dict(self._window_row(connection, leg_input.get("departure_window_id")))
        arrival_window = dict(self._window_row(connection, leg_input.get("arrival_window_id")))
        if departure_window["site_id"] != mission["origin_site_id"]:
            raise ValidationError("起飞窗口不属于任务始发机场")
        if arrival_window["site_id"] != mission["destination_site_id"]:
            raise ValidationError("到达窗口不属于任务到达机场")
        forecast_id = leg_input.get("forecast_id")
        forecast = None
        if forecast_id:
            row = connection.execute(
                "SELECT * FROM weather_forecasts WHERE forecast_id=?", (forecast_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"气象预报 {forecast_id} 不存在")
            forecast = dict(row)
        departure = self._time(leg_input.get("planned_departure"), "planned_departure")
        arrival = self._time(leg_input.get("planned_arrival"), "planned_arrival")
        payload_kg = self._number(leg_input.get("payload_kg"), "payload_kg", ge=0)
        passengers = self._integer(leg_input.get("passengers", 0), "passengers", ge=0)
        bindings = leg_input.get("support_bindings") or []
        if not isinstance(bindings, list):
            raise ValidationError("support_bindings 必须是数组")
        pools_by_id = {row["support_id"]: dict(row) for row in
                       connection.execute("SELECT * FROM ground_support")}
        for binding in bindings:
            if not isinstance(binding, dict) or binding.get("support_id") not in pools_by_id:
                raise ValidationError("support_bindings 引用了不存在的地面保障")
        fuel_required = ((mission["distance_nm"] + mission["alternate_distance_nm"])
                         * aircraft["burn_kg_per_nm"] + aircraft["reserve_fuel_kg"])
        spec = LegSpec(
            leg_sequence=self._integer(leg_input.get("leg_sequence", 1), "leg_sequence", ge=1),
            aircraft_id=aircraft["aircraft_id"], captain_id=captain["crew_id"],
            copilot_id=copilot["crew_id"], departure_site_id=mission["origin_site_id"],
            arrival_site_id=mission["destination_site_id"],
            departure_window_id=departure_window["window_id"],
            arrival_window_id=arrival_window["window_id"], forecast_id=forecast_id,
            alternate_site_id=leg_input.get("alternate_site_id"),
            planned_departure=departure, planned_arrival=arrival, payload_kg=payload_kg,
            passengers=passengers, fuel_required_kg=round(fuel_required, 3),
            support_bindings=tuple(bindings))
        alternate_window = None
        if spec.alternate_site_id:
            row = connection.execute(
                "SELECT * FROM flight_windows WHERE site_id=? AND status='open'"
                " AND opens_at<=? AND closes_at>=? ORDER BY opens_at LIMIT 1",
                (spec.alternate_site_id, arrival, arrival)).fetchone()
            alternate_window = dict(row) if row else None
        reports = evaluate_leg_constraints(
            mission=mission, aircraft=aircraft, captain=captain, copilot=copilot,
            departure_window=departure_window, arrival_window=arrival_window, forecast=forecast,
            alternate_window=alternate_window, pools_by_id=pools_by_id, spec=spec,
            book=ReservationBook(self._capacities(connection)))
        violations = [item for item in reports
                      if not item["satisfied"] and item["name"] != "resource_availability"]
        if violations:
            raise ValidationError(
                "航段不满足约束：" + "；".join(item["detail"] for item in violations))
        return spec, mission

    def create_plan(self, *, request_id: str, actor_id: str, mission_ids: list[str] | None = None,
                    legs: list[dict[str, Any]] | None = None, auto: bool = False,
                    lease_ttl_seconds: int | None = None,
                    waitlist_if_blocked: bool = False) -> dict[str, Any]:
        """创建候选方案并以限时租约保留关键资源。"""

        payload = {"actor_id": actor_id, "mission_ids": mission_ids, "legs": legs, "auto": auto,
                   "lease_ttl_seconds": lease_ttl_seconds, "waitlist_if_blocked": waitlist_if_blocked}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            if self._sweep_expired_leases(connection, actor_id):
                self._process_waitlist(connection, actor_id)
            ttl = self._ttl(lease_ttl_seconds)

            def create() -> tuple[str, str, dict[str, Any]]:
                specs_by_mission: dict[str, list[LegSpec]] = {}
                waitlisted: list[str] = []
                if legs:
                    ids = list(mission_ids or sorted({item.get("mission_id") for item in legs}))
                    for leg_input in legs:
                        spec, _ = self._explicit_leg_spec(connection, leg_input, ids)
                        specs_by_mission.setdefault(spec_mission(leg_input), []).append(spec)
                else:
                    if not mission_ids:
                        raise ValidationError("mission_ids 不能为空")
                    if not auto:
                        raise ValidationError("未提供航段时必须指定 auto=true")
                    shared_book = self._load_book(connection)
                    for mission_id in mission_ids:
                        mission_row = self._mission_row(connection, mission_id)
                        self._assert_mission_plannable(mission_row)
                        mission = self._mission_dict(mission_row)
                        candidates, _ = self._generate(connection, mission, max_results=1,
                                                       check_availability=True, book=shared_book)
                        chosen = candidates[0] if candidates and candidates[0].feasible else None
                        if chosen is None:
                            if waitlist_if_blocked:
                                self._waitlist_add(connection, mission,
                                                   reason="没有可行的候选方案，进入候补")
                                waitlisted.append(mission_id)
                                continue
                            violations = []
                            if candidates:
                                violations = [item["detail"] for item in candidates[0].constraints
                                              if not item["satisfied"]]
                            raise ConflictError(
                                f"任务 {mission_id} 没有可行候选方案："
                                + ("；".join(violations) if violations else "缺少基础资源"))
                        specs_by_mission[mission_id] = list(chosen.legs)
                        windows_by_id = {row["window_id"]: dict(row) for row in connection.execute(
                            "SELECT * FROM flight_windows")}
                        pools_by_id = {row["support_id"]: dict(row) for row in connection.execute(
                            "SELECT * FROM ground_support")}
                        for spec in chosen.legs:
                            for requirement in leg_resource_requirements(
                                    spec, windows_by_id=windows_by_id, pools_by_id=pools_by_id):
                                shared_book.add({**requirement, "holder": f"方案预占 {mission_id}"})
                if specs_by_mission:
                    plan_id = self._create_plan_from_candidates(
                        connection, actor_id=actor_id, mission_specs=specs_by_mission,
                        ttl_seconds=ttl)
                    append_event(connection, actor_id=actor_id, action="plan.created",
                                 resource_type="plan", resource_id=plan_id,
                                 detail={"missions": sorted(specs_by_mission),
                                         "leg_count": sum(len(items) for items in specs_by_mission.values())},
                                 occurred_at=self._now())
                    response = self._plan_detail(connection, plan_id)
                else:
                    plan_id = "none"
                    response = {"plan_id": None, "status": "none", "legs": [], "leases": []}
                response["waitlisted"] = waitlisted
                return "plan", plan_id, response

            return self._idempotent_response(connection, request_id=request_id,
                                             action="create_plan", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 审批与封存
    # ------------------------------------------------------------------

    def approve_plan(self, *, request_id: str, actor_id: str, plan_id: str, side: str,
                     decision: str, comment: str = "") -> dict[str, Any]:
        payload = {"actor_id": actor_id, "plan_id": plan_id, "side": side, "decision": decision}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            if side == "operations":
                self._require(actor, "admin", "ops_approver")
            elif side == "station":
                self._require(actor, "admin", "station_approver")
            else:
                raise ValidationError("side 必须是 operations 或 station")
            if decision not in ("approve", "reject"):
                raise ValidationError("decision 必须是 approve 或 reject")
            self._sweep_expired_leases(connection, actor_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                plan = self._plan_row(connection, plan_id)
                if plan["status"] != "leased":
                    raise ConflictError("方案不在待审批状态，不能审批")
                try:
                    connection.execute(
                        "INSERT INTO plan_approvals(plan_id,approver_side,actor_id,decision,comment,"
                        "created_at) VALUES(?,?,?,?,?,?)",
                        (plan_id, side, actor_id, decision, str(comment or ""), self._now()))
                except Exception as exc:
                    raise ConflictError("该侧已经作出审批决定") from exc
                if decision == "reject":
                    legs = connection.execute(
                        "SELECT * FROM legs WHERE plan_id=? AND status='leased'", (plan_id,)).fetchall()
                    for leg in legs:
                        self._cancel_leg(connection, leg, reason=f"方案被{side}方否决",
                                         source=plan_id, actor_id=actor_id)
                    connection.execute("UPDATE plans SET status='rejected' WHERE plan_id=?", (plan_id,))
                append_event(connection, actor_id=actor_id,
                             action=f"plan.{decision}d" if decision == "approve" else "plan.rejected",
                             resource_type="plan", resource_id=plan_id,
                             detail={"side": side, "decision": decision}, occurred_at=self._now())
                return "plan", plan_id, {"plan_id": plan_id, "side": side, "decision": decision}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="approve_plan", payload=payload, create=create)

    def seal_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                  expected_schedule_version: int) -> dict[str, Any]:
        """双方批准后封存方案；同一时刻只允许一个版本生效。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id,
                   "expected_schedule_version": expected_schedule_version}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            if self._sweep_expired_leases(connection, actor_id):
                self._process_waitlist(connection, actor_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                plan = self._plan_row(connection, plan_id)
                if plan["status"] != "leased":
                    raise ConflictError("方案不在待审批状态，不能封存")
                approvals = {row["approver_side"]: row for row in connection.execute(
                    "SELECT * FROM plan_approvals WHERE plan_id=?", (plan_id,))}
                operations = approvals.get("operations")
                station = approvals.get("station")
                if not (operations and operations["decision"] == "approve"
                        and station and station["decision"] == "approve"):
                    raise ConflictError("运行与站点双方批准后方可封存")
                now = self._now()
                leases = connection.execute(
                    "SELECT * FROM leases WHERE plan_id=?", (plan_id,)).fetchall()
                if not leases or any(lease["status"] != "active" or lease["expires_at"] <= now
                                     for lease in leases):
                    raise ConflictError("方案租约已过期或已处理，不能封存")
                state = connection.execute("SELECT * FROM schedule_state WHERE id=1").fetchone()
                if int(expected_schedule_version) != int(state["sealed_version"]):
                    raise ConflictError("已有其他方案版本封存生效，请基于最新版本重新起草")
                covered = [row["mission_id"] for row in connection.execute(
                    "SELECT mission_id FROM plan_missions WHERE plan_id=?", (plan_id,))]
                released_total: list[dict[str, Any]] = []
                superseded_plan_ids: set[str] = set()
                for mission_id in covered:
                    old_legs = connection.execute(
                        "SELECT * FROM legs WHERE mission_id=? AND plan_id!=?"
                        " AND status IN ('leased','committed')", (mission_id, plan_id)).fetchall()
                    new_first = min(row["planned_departure"] for row in connection.execute(
                        "SELECT planned_departure FROM legs WHERE mission_id=? AND plan_id=?",
                        (mission_id, plan_id)))
                    for leg in old_legs:
                        released_total += self._cancel_leg(
                            connection, leg, reason=f"被方案 {plan_id} 封存取代",
                            source=plan_id, actor_id=actor_id)
                        superseded_plan_ids.add(leg["plan_id"])
                        if new_first > leg["planned_departure"]:
                            self._decide(connection, mission_id=mission_id, leg_id=None,
                                         decision="delayed",
                                         reason=f"航段由 {leg['planned_departure']} 改期至 {new_first}"
                                                f"（方案 {plan_id} 封存）", actor_id=actor_id)
                self._retire_empty_plans(connection, superseded_plan_ids)
                committed_count = 0
                for lease in leases:
                    connection.execute("UPDATE leases SET status='consumed' WHERE lease_id=?",
                                       (lease["lease_id"],))
                    allocation_id = uuid.uuid4().hex
                    connection.execute(
                        "INSERT INTO allocations(allocation_id,leg_id,plan_id,resource_type,resource_id,"
                        "slot_start,slot_end,quantity,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (allocation_id, lease["leg_id"], plan_id, lease["resource_type"],
                         lease["resource_id"], lease["slot_start"], lease["slot_end"],
                         lease["quantity"], "committed", now))
                    leg = self._leg_row(connection, lease["leg_id"])
                    self._ledger(connection, mission_id=leg["mission_id"], leg_id=lease["leg_id"],
                                 plan_id=plan_id, action="committed",
                                 resource_type=lease["resource_type"], resource_id=lease["resource_id"],
                                 slot_start=lease["slot_start"], slot_end=lease["slot_end"],
                                 quantity=lease["quantity"], reason="方案封存，租约转为正式承诺",
                                 source=plan_id)
                    committed_count += 1
                connection.execute(
                    "UPDATE legs SET status='committed' WHERE plan_id=? AND status='leased'", (plan_id,))
                for mission_id in covered:
                    self._decide(connection, mission_id=mission_id, leg_id=None, decision="approved",
                                 reason="运行与站点双方批准，方案封存，获准按计划起飞", actor_id=actor_id)
                    self._refresh_mission(connection, mission_id)
                connection.execute(
                    "UPDATE plans SET status='sealed', sealed_at=?, sealed_by=? WHERE plan_id=?",
                    (now, actor_id, plan_id))
                connection.execute(
                    "UPDATE schedule_state SET sealed_version=sealed_version+1, sealed_plan_id=?"
                    " WHERE id=1", (plan_id,))
                append_event(connection, actor_id=actor_id, action="plan.sealed",
                             resource_type="plan", resource_id=plan_id,
                             detail={"missions": covered,
                                     "sealed_version": int(state["sealed_version"]) + 1},
                             occurred_at=self._now())
                granted = self._process_waitlist(connection, actor_id)
                return "plan", plan_id, {
                    "plan_id": plan_id, "status": "sealed",
                    "sealed_version": int(state["sealed_version"]) + 1,
                    "committed_allocations": committed_count,
                    "superseded_plans": sorted(superseded_plan_ids),
                    "released_resources": released_total, "waitlist_granted": granted}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="seal_plan", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 航段执行
    # ------------------------------------------------------------------

    def record_leg_event(self, *, request_id: str, actor_id: str, leg_id: str,
                         event: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "leg_id": leg_id, "event": event}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher", "operator")
            self._sweep_expired_leases(connection, actor_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                if event not in EXECUTION_EVENTS:
                    raise ValidationError(f"event 必须是 {sorted(EXECUTION_EVENTS)} 之一")
                leg = self._leg_row(connection, leg_id)
                required, new_status = EXECUTION_EVENTS[event]
                if leg["status"] != required:
                    raise ConflictError(f"航段当前状态 {leg['status']} 不允许事件 {event}")
                connection.execute("UPDATE legs SET status=? WHERE leg_id=?", (new_status, leg_id))
                if event == "complete_handoff":
                    allocations = connection.execute(
                        "SELECT * FROM allocations WHERE leg_id=? AND status='committed'",
                        (leg_id,)).fetchall()
                    for allocation in allocations:
                        connection.execute(
                            "UPDATE allocations SET status='consumed' WHERE allocation_id=?",
                            (allocation["allocation_id"],))
                        self._ledger(connection, mission_id=leg["mission_id"], leg_id=leg_id,
                                     plan_id=leg["plan_id"], action="consumed",
                                     resource_type=allocation["resource_type"],
                                     resource_id=allocation["resource_id"],
                                     slot_start=allocation["slot_start"],
                                     slot_end=allocation["slot_end"],
                                     quantity=allocation["quantity"],
                                     reason="交接完成，资源占用结束", source=leg_id)
                decision_text = {
                    "depart": "航段已起飞，进入不可重排状态",
                    "arrive": "航段已到达",
                    "complete_handoff": "交接完成，任务该航段结案",
                }[event]
                self._decide(connection, mission_id=leg["mission_id"], leg_id=leg_id,
                             decision=new_status, reason=decision_text, actor_id=actor_id)
                self._refresh_mission(connection, leg["mission_id"])
                append_event(connection, actor_id=actor_id, action=f"leg.{event}",
                             resource_type="leg", resource_id=leg_id,
                             detail={"mission_id": leg["mission_id"], "status": new_status},
                             occurred_at=self._now())
                return "leg", leg_id, {"leg_id": leg_id, "status": new_status,
                                       "mission_id": leg["mission_id"]}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="record_leg_event", payload=payload, create=create)

    # ------------------------------------------------------------------
    # 扰动处理
    # ------------------------------------------------------------------

    def _cancel_leg(self, connection, leg, *, reason: str, source: str,
                    actor_id: str) -> list[dict[str, Any]]:
        """取消未执行航段并释放其全部资源占用，返回释放清单。"""

        released: list[dict[str, Any]] = []
        for row in connection.execute(
                "SELECT * FROM leases WHERE leg_id=? AND status='active'", (leg["leg_id"],)):
            connection.execute("UPDATE leases SET status='released' WHERE lease_id=?",
                               (row["lease_id"],))
            self._ledger(connection, mission_id=leg["mission_id"], leg_id=leg["leg_id"],
                         plan_id=leg["plan_id"], action="lease_released",
                         resource_type=row["resource_type"], resource_id=row["resource_id"],
                         slot_start=row["slot_start"], slot_end=row["slot_end"],
                         quantity=row["quantity"], reason=reason, source=source)
            released.append({"action": "lease_released", "resource_type": row["resource_type"],
                             "resource_id": row["resource_id"]})
        for row in connection.execute(
                "SELECT * FROM allocations WHERE leg_id=? AND status='committed'", (leg["leg_id"],)):
            connection.execute("UPDATE allocations SET status='released' WHERE allocation_id=?",
                               (row["allocation_id"],))
            self._ledger(connection, mission_id=leg["mission_id"], leg_id=leg["leg_id"],
                         plan_id=leg["plan_id"], action="released",
                         resource_type=row["resource_type"], resource_id=row["resource_id"],
                         slot_start=row["slot_start"], slot_end=row["slot_end"],
                         quantity=row["quantity"], reason=reason, source=source)
            released.append({"action": "released", "resource_type": row["resource_type"],
                             "resource_id": row["resource_id"]})
        connection.execute("UPDATE legs SET status='cancelled', cancelled_reason=? WHERE leg_id=?",
                           (reason, leg["leg_id"]))
        self._decide(connection, mission_id=leg["mission_id"], leg_id=leg["leg_id"],
                     decision="cancelled", reason=reason, actor_id=actor_id)
        self._refresh_mission(connection, leg["mission_id"])
        return released

    def _retire_empty_plans(self, connection, plan_ids) -> None:
        for plan_id in plan_ids:
            remaining = connection.execute(
                "SELECT COUNT(*) AS count FROM legs WHERE plan_id=? AND status != 'cancelled'",
                (plan_id,)).fetchone()["count"]
            if remaining:
                continue
            plan = self._plan_row(connection, plan_id)
            if plan["status"] == "sealed":
                connection.execute("UPDATE plans SET status='superseded' WHERE plan_id=?", (plan_id,))
            elif plan["status"] == "leased":
                connection.execute("UPDATE plans SET status='cancelled' WHERE plan_id=?", (plan_id,))

    def _handle_weather_revision(self, connection, actor_id: str, alert_id: str,
                                 payload: dict[str, Any]) -> dict[str, Any]:
        forecast_id = payload.get("forecast_id")
        row = connection.execute(
            "SELECT * FROM weather_forecasts WHERE forecast_id=?", (forecast_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"气象预报 {forecast_id} 不存在")
        forecast = dict(row)
        cancelled: list[str] = []
        released: list[dict[str, Any]] = []
        affected_missions: set[str] = set()
        touched_plans: set[str] = set()
        legs = connection.execute(
            "SELECT * FROM legs WHERE status IN ('leased','committed') AND arrival_site_id=?",
            (forecast["site_id"],)).fetchall()
        for leg in legs:
            if leg["forecast_id"] == forecast_id:
                continue
            if not forecast["valid_from"] <= leg["planned_arrival"] <= forecast["valid_to"]:
                continue
            aircraft = self._aircraft_row(connection, leg["aircraft_id"])
            if (float(forecast["ceiling_m"]) >= float(aircraft["min_ceiling_m"])
                    and float(forecast["visibility_m"]) >= float(aircraft["min_visibility_m"])
                    and float(forecast["wind_kt"]) <= float(aircraft["max_wind_kt"])):
                continue
            reason = (f"气象预报修订为 {forecast_id}（v{forecast['version']}）后，"
                      f"到达时刻不再满足机型最低气象标准")
            released += self._cancel_leg(connection, leg, reason=reason, source=alert_id,
                                         actor_id=actor_id)
            cancelled.append(leg["leg_id"])
            affected_missions.add(leg["mission_id"])
            touched_plans.add(leg["plan_id"])
        self._retire_empty_plans(connection, touched_plans)
        return {"forecast_id": forecast_id, "cancelled_legs": cancelled,
                "affected_missions": sorted(affected_missions), "released_resources": released}

    def _handle_aircraft_failure(self, connection, actor_id: str, alert_id: str,
                                 payload: dict[str, Any]) -> dict[str, Any]:
        aircraft_id = payload.get("aircraft_id")
        self._aircraft_row(connection, aircraft_id)
        connection.execute("UPDATE flight_aircraft SET status='failed' WHERE aircraft_id=?",
                           (aircraft_id,))
        cancelled: list[str] = []
        released: list[dict[str, Any]] = []
        affected_missions: set[str] = set()
        touched_plans: set[str] = set()
        legs = connection.execute(
            "SELECT * FROM legs WHERE status IN ('leased','committed') AND aircraft_id=?",
            (aircraft_id,)).fetchall()
        for leg in legs:
            released += self._cancel_leg(connection, leg, reason=f"飞机 {aircraft_id} 故障停飞",
                                         source=alert_id, actor_id=actor_id)
            cancelled.append(leg["leg_id"])
            affected_missions.add(leg["mission_id"])
            touched_plans.add(leg["plan_id"])
        self._retire_empty_plans(connection, touched_plans)
        return {"aircraft_id": aircraft_id, "cancelled_legs": cancelled,
                "affected_missions": sorted(affected_missions), "released_resources": released}

    def _handle_partial_offload(self, connection, actor_id: str, alert_id: str,
                                payload: dict[str, Any]) -> dict[str, Any]:
        mission_id = payload.get("mission_id")
        mission = self._mission_row(connection, mission_id)
        offloaded = self._number(payload.get("offloaded_kg"), "offloaded_kg", gt=0)
        if offloaded >= float(mission["payload_kg"]):
            raise ValidationError("offloaded_kg 必须小于任务当前载荷")
        new_payload = float(mission["payload_kg"]) - offloaded
        connection.execute("UPDATE missions SET payload_kg=? WHERE mission_id=?",
                           (new_payload, mission_id))
        remaining = offloaded
        adjusted: list[dict[str, Any]] = []
        legs = connection.execute(
            "SELECT * FROM legs WHERE mission_id=? AND status IN ('leased','committed')"
            " ORDER BY leg_sequence DESC", (mission_id,)).fetchall()
        for leg in legs:
            if remaining <= 0:
                break
            take = min(float(leg["payload_kg"]), remaining)
            if take <= 0:
                continue
            connection.execute("UPDATE legs SET payload_kg=payload_kg-? WHERE leg_id=?",
                               (take, leg["leg_id"]))
            self._ledger(connection, mission_id=mission_id, leg_id=leg["leg_id"],
                         plan_id=leg["plan_id"], action="payload_reduced",
                         resource_type="payload_capacity", resource_id=leg["aircraft_id"],
                         slot_start=leg["planned_departure"], slot_end=leg["planned_arrival"],
                         quantity=take, reason=f"部分卸载 {take:.0f} kg，运力释放", source=alert_id)
            adjusted.append({"leg_id": leg["leg_id"], "offloaded_kg": take})
            remaining -= take
        child_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO missions(mission_id,organization_id,mission_type,priority_class,"
            "origin_site_id,destination_site_id,distance_nm,alternate_site_id,alternate_distance_nm,"
            "payload_kg,passengers,passenger_restrictions_json,required_support_json,"
            "earliest_departure,latest_arrival,allow_split,parent_mission_id,status,created_by,"
            "created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'open',?,?)",
            (child_id, mission["organization_id"], mission["mission_type"], mission["priority_class"],
             mission["origin_site_id"], mission["destination_site_id"], mission["distance_nm"],
             mission["alternate_site_id"], mission["alternate_distance_nm"], offloaded, 0,
             mission["passenger_restrictions_json"], mission["required_support_json"],
             mission["earliest_departure"], mission["latest_arrival"], mission["allow_split"],
             mission_id, actor_id, self._now()))
        self._decide(connection, mission_id=mission_id, leg_id=None, decision="offloaded",
                     reason=f"部分卸载 {offloaded:.0f} kg，剩余载荷转入后续任务 {child_id}",
                     actor_id=actor_id)
        return {"child_mission_id": child_id, "new_payload_kg": new_payload,
                "adjusted_legs": adjusted}

    def _handle_medical_insertion(self, connection, actor_id: str, alert_id: str,
                                  payload: dict[str, Any]) -> dict[str, Any]:
        mission_id = payload.get("mission_id")
        mission_row = self._mission_row(connection, mission_id)
        mission = self._mission_dict(mission_row)
        if int(mission["priority_class"]) > 1:
            raise ValidationError("仅医疗类任务可以紧急插队")
        if mission_row["status"] not in ("open",):
            raise ConflictError("任务已有生效安排，不能重复插队")
        candidates, _ = self._generate(connection, mission, max_results=1, check_availability=True)
        if candidates and candidates[0].feasible:
            plan_id = self._create_plan_from_candidates(
                connection, actor_id=actor_id,
                mission_specs={mission_id: list(candidates[0].legs)},
                ttl_seconds=DEFAULT_LEASE_TTL_SECONDS)
            return {"outcome": "planned", "plan_id": plan_id, "preempted_missions": []}
        relaxed, _ = self._generate(connection, mission, max_results=1, check_availability=False)
        if not relaxed or not relaxed[0].feasible:
            self._waitlist_add(connection, mission, reason="紧急医疗插队：没有满足约束的候选方案")
            return {"outcome": "waitlisted", "reason": "没有满足约束的候选方案",
                    "preempted_missions": []}
        book = self._load_book(connection)
        windows_by_id = {row["window_id"]: dict(row) for row in
                         connection.execute("SELECT * FROM flight_windows")}
        pools_by_id = {row["support_id"]: dict(row) for row in
                       connection.execute("SELECT * FROM ground_support")}
        blocking_leg_ids: dict[str, None] = {}
        for spec in relaxed[0].legs:
            for requirement in leg_resource_requirements(
                    spec, windows_by_id=windows_by_id, pools_by_id=pools_by_id):
                for hit in book.conflicts(**requirement):
                    if hit.get("holder_leg_id"):
                        blocking_leg_ids[hit["holder_leg_id"]] = None
        blocking_legs = [self._leg_row(connection, leg_id) for leg_id in blocking_leg_ids]
        preemptable = True
        for leg in blocking_legs:
            holder = self._mission_row(connection, leg["mission_id"])
            if int(holder["priority_class"]) <= int(mission["priority_class"]):
                preemptable = False
                break
        if not preemptable:
            self._waitlist_add(connection, mission,
                               reason="紧急医疗插队：资源被同级或更高优先级任务占用")
            return {"outcome": "waitlisted", "reason": "资源被同级或更高优先级任务占用",
                    "preempted_missions": []}
        preempted: set[str] = set()
        touched_plans: set[str] = set()
        for leg in blocking_legs:
            self._cancel_leg(connection, leg,
                             reason=f"紧急医疗任务 {mission_id} 插队，让出资源",
                             source=alert_id, actor_id=actor_id)
            self._decide(connection, mission_id=leg["mission_id"], leg_id=leg["leg_id"],
                         decision="preempted",
                         reason=f"紧急医疗任务 {mission_id} 插队，航段让出资源", actor_id=actor_id)
            holder = self._mission_dict(self._mission_row(connection, leg["mission_id"]))
            self._waitlist_add(connection, holder, reason="被紧急医疗任务插队，等待资源释放")
            preempted.add(leg["mission_id"])
            touched_plans.add(leg["plan_id"])
        self._retire_empty_plans(connection, touched_plans)
        candidates, _ = self._generate(connection, mission, max_results=1, check_availability=True)
        if not candidates or not candidates[0].feasible:
            self._waitlist_add(connection, mission, reason="紧急医疗插队：让出资源后仍不可行")
            return {"outcome": "waitlisted", "reason": "让出资源后仍不可行",
                    "preempted_missions": sorted(preempted)}
        plan_id = self._create_plan_from_candidates(
            connection, actor_id=actor_id,
            mission_specs={mission_id: list(candidates[0].legs)},
            ttl_seconds=DEFAULT_LEASE_TTL_SECONDS)
        return {"outcome": "planned_after_preemption", "plan_id": plan_id,
                "preempted_missions": sorted(preempted)}

    def process_disruption(self, *, request_id: str, actor_id: str, alert_id: str,
                           disruption_type: str, **payload: Any) -> dict[str, Any]:
        """处理扰动告警；同一 alert_id 重复送达时返回原处理结果。"""

        full_payload = {"actor_id": actor_id, "alert_id": alert_id,
                        "disruption_type": disruption_type, **payload}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")
            if self._sweep_expired_leases(connection, actor_id):
                self._process_waitlist(connection, actor_id)

            def create() -> tuple[str, str, dict[str, Any]]:
                existing = connection.execute(
                    "SELECT * FROM disruptions WHERE alert_id=?", (alert_id,)).fetchone()
                if existing:
                    result = json.loads(existing["result_json"])
                    result["duplicate"] = True
                    return "disruption", alert_id, result
                handlers = {
                    "weather_revision": self._handle_weather_revision,
                    "aircraft_failure": self._handle_aircraft_failure,
                    "partial_offload": self._handle_partial_offload,
                    "medical_insertion": self._handle_medical_insertion,
                }
                handler = handlers.get(disruption_type)
                if handler is None:
                    raise ValidationError(f"disruption_type 必须是 {sorted(handlers)} 之一")
                result = handler(connection, actor_id, alert_id, payload)
                result["waitlist_granted"] = self._process_waitlist(connection, actor_id)
                connection.execute(
                    "INSERT INTO disruptions(alert_id,disruption_type,payload_json,result_json,"
                    "processed_by,processed_at) VALUES(?,?,?,?,?,?)",
                    (alert_id, disruption_type, canonical_json(payload), canonical_json(result),
                     actor_id, self._now()))
                append_event(connection, actor_id=actor_id,
                             action=f"disruption.{disruption_type}",
                             resource_type="disruption", resource_id=alert_id,
                             detail={"result": result}, occurred_at=self._now())
                result["duplicate"] = False
                return "disruption", alert_id, result

            return self._idempotent_response(connection, request_id=request_id,
                                             action="process_disruption", payload=full_payload,
                                             create=create)

    # ------------------------------------------------------------------
    # 租约清扫与候补
    # ------------------------------------------------------------------

    def _sweep_expired_leases(self, connection, actor_id: str) -> int:
        """把过期租约标记为失效，并作废仍未封存的方案。"""

        now = self._now()
        expired = connection.execute(
            "SELECT * FROM leases WHERE status='active' AND expires_at <= ?", (now,)).fetchall()
        if not expired:
            return 0
        plan_ids = set()
        for lease in expired:
            connection.execute("UPDATE leases SET status='expired' WHERE lease_id=?",
                               (lease["lease_id"],))
            leg = self._leg_row(connection, lease["leg_id"])
            self._ledger(connection, mission_id=leg["mission_id"], leg_id=lease["leg_id"],
                         plan_id=lease["plan_id"], action="lease_expired",
                         resource_type=lease["resource_type"], resource_id=lease["resource_id"],
                         slot_start=lease["slot_start"], slot_end=lease["slot_end"],
                         quantity=lease["quantity"], reason="租约到期未封存，自动释放",
                         source="lease_sweep")
            plan_ids.add(lease["plan_id"])
        for plan_id in plan_ids:
            plan = self._plan_row(connection, plan_id)
            if plan["status"] != "leased":
                continue
            legs = connection.execute(
                "SELECT * FROM legs WHERE plan_id=? AND status='leased'", (plan_id,)).fetchall()
            for leg in legs:
                for row in connection.execute(
                        "SELECT * FROM leases WHERE leg_id=? AND status='active'",
                        (leg["leg_id"],)).fetchall():
                    connection.execute("UPDATE leases SET status='expired' WHERE lease_id=?",
                                       (row["lease_id"],))
                    self._ledger(connection, mission_id=leg["mission_id"], leg_id=leg["leg_id"],
                                 plan_id=plan_id, action="lease_expired",
                                 resource_type=row["resource_type"], resource_id=row["resource_id"],
                                 slot_start=row["slot_start"], slot_end=row["slot_end"],
                                 quantity=row["quantity"], reason="租约到期未封存，自动释放",
                                 source="lease_sweep")
                connection.execute(
                    "UPDATE legs SET status='cancelled', cancelled_reason=? WHERE leg_id=?",
                    ("租约过期，方案未封存", leg["leg_id"]))
                self._decide(connection, mission_id=leg["mission_id"], leg_id=leg["leg_id"],
                             decision="cancelled", reason="租约过期，方案未封存",
                             actor_id=actor_id)
                self._refresh_mission(connection, leg["mission_id"])
            connection.execute("UPDATE plans SET status='expired' WHERE plan_id=?", (plan_id,))
        return len(expired)

    def _process_waitlist(self, connection, actor_id: str) -> list[dict[str, Any]]:
        """按优先级与入队次序处理候补，资源可行时自动重新排入计划。"""

        granted: list[dict[str, Any]] = []
        entries = connection.execute(
            "SELECT * FROM waitlist WHERE status='waiting' ORDER BY priority_class, queue_seq"
        ).fetchall()
        for entry in entries:
            mission_row = self._mission_row(connection, entry["mission_id"])
            if mission_row["status"] != "open":
                connection.execute("UPDATE waitlist SET status='cancelled' WHERE entry_id=?",
                                   (entry["entry_id"],))
                continue
            mission = self._mission_dict(mission_row)
            candidates, _ = self._generate(connection, mission, max_results=1,
                                           check_availability=True)
            if not candidates or not candidates[0].feasible:
                continue
            plan_id = self._create_plan_from_candidates(
                connection, actor_id=actor_id,
                mission_specs={mission["mission_id"]: list(candidates[0].legs)},
                ttl_seconds=DEFAULT_LEASE_TTL_SECONDS)
            connection.execute("UPDATE waitlist SET status='granted' WHERE entry_id=?",
                               (entry["entry_id"],))
            self._decide(connection, mission_id=mission["mission_id"], leg_id=None,
                         decision="replanned",
                         reason=f"候补资源释放，已重新纳入方案 {plan_id}", actor_id=actor_id)
            granted.append({"entry_id": entry["entry_id"], "mission_id": mission["mission_id"],
                            "plan_id": plan_id})
        return granted

    def process_waitlist(self, *, request_id: str, actor_id: str) -> dict[str, Any]:
        """显式触发一次租约清扫与候补处理。"""

        payload = {"actor_id": actor_id}
        with self._lock, self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require(actor, "admin", "dispatcher")

            def create() -> tuple[str, str, dict[str, Any]]:
                swept = self._sweep_expired_leases(connection, actor_id)
                granted = self._process_waitlist(connection, actor_id)
                return "waitlist", "waitlist", {"swept_leases": swept, "granted": granted}

            return self._idempotent_response(connection, request_id=request_id,
                                             action="process_waitlist", payload=payload,
                                             create=create)

    # ------------------------------------------------------------------
    # 查询与解释
    # ------------------------------------------------------------------

    def _plan_detail(self, connection, plan_id: str) -> dict[str, Any]:
        plan = self._plan_row(connection, plan_id)
        legs = [self._leg_dict(row) for row in connection.execute(
            "SELECT * FROM legs WHERE plan_id=? ORDER BY planned_departure, leg_sequence",
            (plan_id,))]
        leases = [dict(row) for row in connection.execute(
            "SELECT * FROM leases WHERE plan_id=? ORDER BY resource_type, resource_id", (plan_id,))]
        approvals = [dict(row) for row in connection.execute(
            "SELECT * FROM plan_approvals WHERE plan_id=? ORDER BY created_at", (plan_id,))]
        missions = [row["mission_id"] for row in connection.execute(
            "SELECT mission_id FROM plan_missions WHERE plan_id=? ORDER BY mission_id", (plan_id,))]
        return {"plan_id": plan_id, "status": plan["status"],
                "base_schedule_version": plan["base_schedule_version"],
                "created_by": plan["created_by"], "created_at": plan["created_at"],
                "sealed_at": plan["sealed_at"], "sealed_by": plan["sealed_by"],
                "missions": missions, "legs": legs, "leases": leases, "approvals": approvals}

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        with self._lock, self.database.transaction() as connection:
            return self._plan_detail(connection, plan_id)

    def list_plans(self, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock, self.database.transaction() as connection:
            if status:
                rows = connection.execute(
                    "SELECT plan_id FROM plans WHERE status=? ORDER BY created_at", (status,)).fetchall()
            else:
                rows = connection.execute("SELECT plan_id FROM plans ORDER BY created_at").fetchall()
            return [self._plan_detail(connection, row["plan_id"]) for row in rows]

    def get_mission(self, mission_id: str) -> dict[str, Any]:
        with self._lock, self.database.transaction() as connection:
            mission = self._mission_dict(self._mission_row(connection, mission_id))
            legs = [self._leg_dict(row) for row in connection.execute(
                "SELECT * FROM legs WHERE mission_id=? ORDER BY planned_departure, leg_sequence",
                (mission_id,))]
            return {"mission": mission, "legs": legs}

    def mission_explanation(self, mission_id: str) -> dict[str, Any]:
        """说明任务为何获准、延后、拆分或取消，并列出逐项资源变化。"""

        with self._lock, self.database.transaction() as connection:
            mission = self._mission_dict(self._mission_row(connection, mission_id))
            legs = [self._leg_dict(row) for row in connection.execute(
                "SELECT * FROM legs WHERE mission_id=? ORDER BY planned_departure, leg_sequence",
                (mission_id,))]
            decisions = [dict(row) for row in connection.execute(
                "SELECT * FROM mission_decisions WHERE mission_id=? ORDER BY decision_id",
                (mission_id,))]
            changes = [dict(row) for row in connection.execute(
                "SELECT * FROM resource_changes WHERE mission_id=? ORDER BY change_id",
                (mission_id,))]
            return {"mission_id": mission_id, "status": mission["status"],
                    "status_text": MISSION_STATUS_TEXT[mission["status"]],
                    "summary": decisions[-1] if decisions else None,
                    "legs": legs, "decisions": decisions, "resource_changes": changes}

    def list_waitlist(self) -> list[dict[str, Any]]:
        with self._lock, self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM waitlist WHERE status='waiting'"
                " ORDER BY priority_class, queue_seq").fetchall()
            return [{"position": index + 1, "entry_id": row["entry_id"],
                     "mission_id": row["mission_id"], "priority_class": row["priority_class"],
                     "reason": row["reason"], "created_at": row["created_at"]}
                    for index, row in enumerate(rows)]

    def schedule_status(self) -> dict[str, Any]:
        with self._lock, self.database.transaction() as connection:
            row = connection.execute("SELECT * FROM schedule_state WHERE id=1").fetchone()
            return {"sealed_version": row["sealed_version"],
                    "sealed_plan_id": row["sealed_plan_id"]}


def spec_mission(leg_input: dict[str, Any]) -> str:
    return str(leg_input.get("mission_id"))
