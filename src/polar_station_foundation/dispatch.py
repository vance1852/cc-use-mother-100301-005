"""飞行窗口与任务承诺调度服务。

在基础登记服务之上实现：

* 候选方案：机场窗口、机组资质、油量、载荷、备降点、气象版本、旅客限制、
  地面保障八类因子逐项评估并可比较；
* 限时租约：关键资源先以带 TTL 的软租约保留，经运行与站点双方批准后才
  随承诺一起封存；
* 重排边界：气象修订、飞机故障、部分卸载、医疗插队只释放尚未执行且确实
  受影响的航段，已起飞与已完成交接不可撤销，重复告警不重复生效；
* 并发封存：所有写操作走 IMMEDIATE 事务，封存用条件更新保证唯一生效版本；
* 可恢复：租约、候补次序、待审批版本全部落在 SQLite，进程重启后延续；
* 可解释：每个任务返回获准/延后/拆分的依据，以及逐次资源释放与重占台账。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from .audit import append_event, canonical_json, digest
from .clock import Clock, SystemClock
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .planning import evaluate_candidate
from .storage import Database

PAX_WEIGHT_KG = 90.0
FUEL_RESERVE_RATIO = 0.2
WINDOW_SLOT_MINUTES = 30
DEFAULT_LEASE_TTL_MINUTES = 30
PROMOTION_TTL_MINUTES = 15

MISSION_KINDS = frozenset({"medevac", "calibration", "supply"})
DEFAULT_PRIORITY = {"medevac": 100, "calibration": 50, "supply": 10}
APPROVAL_ROLES = {"operations": ("admin", "operator"), "station": ("admin", "reviewer")}

# 仍占用资源的租约状态。
ACTIVE_LEASE_STATES = ("held", "committed", "departed")
# 尚未执行、仍可被重排影响的承诺状态。
PENDING_COMMITMENT_STATES = ("committed",)


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def iso_z(value: datetime) -> str:
    """把时间统一格式化为 UTC 的 Z 文本，便于字典序比较。"""

    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class DispatchService:
    """实现候选评估、租约、封存、告警重排与资源台账。"""

    def __init__(self, database: Database, clock: Clock | None = None,
                 lease_ttl_minutes: int = DEFAULT_LEASE_TTL_MINUTES) -> None:
        self.database = database
        self.clock = clock or SystemClock()
        self.lease_ttl = timedelta(minutes=lease_ttl_minutes)

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _now_dt(self) -> datetime:
        value = self.clock.now()
        if value.tzinfo is None:
            raise ValidationError("时钟必须带时区")
        return value

    def _now(self) -> str:
        return iso_z(self._now_dt())

    def _actor(self, connection, actor_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM actors WHERE actor_id=?", (actor_id,)).fetchone()
        if row is None:
            raise NotFoundError("操作者不存在")
        if not row["active"]:
            raise PermissionDenied("操作者已停用")
        return dict(row)

    def _require_roles(self, actor: dict[str, Any], *roles: str) -> None:
        if actor["role"] not in roles:
            raise PermissionDenied("当前角色不能执行该动作")

    def _idempotent(self, connection, *, request_id: str, action: str,
                    payload: dict[str, Any], create) -> dict[str, Any]:
        if not request_id:
            raise ValidationError("request_id 不能为空")
        payload_hash = digest(payload)
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row:
            if row["action"] != action or row["payload_hash"] != payload_hash:
                raise ConflictError("request_id 已被不同内容使用")
            return {**json.loads(row["response_json"]), "replayed": True}
        result = create()
        self._store_receipt(connection, request_id=request_id, action=action,
                            payload_hash=payload_hash, result=result)
        return {**result["response"], "replayed": False}

    def _replay(self, connection, *, request_id: str, action: str,
                payload: dict[str, Any]) -> dict[str, Any] | None:
        """在任何状态修改之前处理幂等重放。"""

        if not request_id:
            raise ValidationError("request_id 不能为空")
        row = connection.execute(
            "SELECT * FROM request_receipts WHERE request_id=?", (request_id,)
        ).fetchone()
        if row is None:
            return None
        if row["action"] != action or row["payload_hash"] != digest(payload):
            raise ConflictError("request_id 已被不同内容使用")
        return {**json.loads(row["response_json"]), "replayed": True}

    def _store_receipt(self, connection, *, request_id: str, action: str,
                       payload_hash: str, result: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO request_receipts(request_id,action,payload_hash,resource_type,resource_id,"
            "response_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (request_id, action, payload_hash, result["resource_type"], result["resource_id"],
             canonical_json(result["response"]), self._now()),
        )

    def _audit(self, connection, *, actor_id: str, action: str, resource_type: str,
               resource_id: str, detail: dict[str, Any]) -> None:
        append_event(connection, actor_id=actor_id, action=action, resource_type=resource_type,
                     resource_id=resource_id, detail=detail, occurred_at=self._now())

    def _ledger(self, connection, *, movement: str, resource_type: str, resource_key: str,
                reason: str, mission_id: str | None = None, plan_id: str | None = None,
                leg_id: str | None = None, commitment_id: str | None = None,
                starts_at: str | None = None, ends_at: str | None = None,
                quantity: float | None = None, alert_key: str | None = None,
                lease_id: str | None = None) -> None:
        connection.execute(
            "INSERT INTO dispatch_ledger(ledger_id,mission_id,plan_id,leg_id,commitment_id,"
            "movement,resource_type,resource_key,starts_at,ends_at,quantity,reason,alert_key,"
            "lease_id,occurred_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, mission_id, plan_id, leg_id, commitment_id, movement,
             resource_type, resource_key, starts_at, ends_at, quantity, reason, alert_key,
             lease_id, self._now()),
        )

    def _sweep_expirations(self, connection) -> None:
        """把超时软租约、失效候补提升标记为过期，并推进候补队列。"""

        now = self._now()
        expired = connection.execute(
            "SELECT * FROM dispatch_leases WHERE status='held' AND expires_at<=?", (now,)
        ).fetchall()
        for row in expired:
            self._release_lease(connection, row, reason="lease_expired", alert_key=None)
        stale_promotions = connection.execute(
            "SELECT * FROM dispatch_waitlist WHERE status='promoted' AND promoted_at IS NOT NULL "
            "AND promoted_at<=?",
            (iso_z(self._now_dt() - timedelta(minutes=PROMOTION_TTL_MINUTES)),),
        ).fetchall()
        for row in stale_promotions:
            connection.execute(
                "UPDATE dispatch_waitlist SET status='expired' WHERE entry_id=?", (row["entry_id"],)
            )
            self._promote_waitlist(connection, row["resource_type"], row["resource_key"])

    # ------------------------------------------------------------------
    # 参考资料登记
    # ------------------------------------------------------------------

    def register_aircraft(self, *, request_id: str, actor_id: str, site_id: str, aircraft_id: str,
                          display_name: str, payload_capacity_kg: float, seat_capacity: int,
                          fuel_capacity_kg: float, burn_kg_per_hour: float, speed_kt: float,
                          crosswind_limit_kt: int, required_rating: str,
                          capabilities: list[str] | None = None) -> dict[str, Any]:
        payload = locals().copy()
        payload.pop("self")
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            if connection.execute("SELECT 1 FROM sites WHERE site_id=?", (site_id,)).fetchone() is None:
                raise NotFoundError("场所不存在")

            def create():
                if connection.execute("SELECT 1 FROM dispatch_aircraft WHERE aircraft_id=?",
                                      (aircraft_id,)).fetchone():
                    raise ConflictError("飞机编号已经存在")
                connection.execute(
                    "INSERT INTO dispatch_aircraft(aircraft_id,site_id,display_name,payload_capacity_kg,"
                    "seat_capacity,fuel_capacity_kg,burn_kg_per_hour,speed_kt,crosswind_limit_kt,"
                    "required_rating,capabilities_json,grounded,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,0,?,?)",
                    (aircraft_id, site_id, display_name, float(payload_capacity_kg), int(seat_capacity),
                     float(fuel_capacity_kg), float(burn_kg_per_hour), float(speed_kt),
                     int(crosswind_limit_kt), required_rating,
                     canonical_json(capabilities or []), actor_id, self._now()),
                )
                self._audit(connection, actor_id=actor_id, action="aircraft.registered",
                            resource_type="dispatch_aircraft", resource_id=aircraft_id,
                            detail={"display_name": display_name, "required_rating": required_rating})
                return {"resource_type": "dispatch_aircraft", "resource_id": aircraft_id,
                        "response": {"aircraft_id": aircraft_id}}

            return self._idempotent(connection, request_id=request_id, action="register_aircraft",
                                    payload=payload, create=create)

    def register_airfield(self, *, request_id: str, actor_id: str, code: str, display_name: str,
                          has_fuel: bool, has_ground_handling: bool, medevac_capable: bool,
                          site_id: str | None = None) -> dict[str, Any]:
        payload = locals().copy()
        payload.pop("self")
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")

            def create():
                connection.execute(
                    "INSERT INTO dispatch_airfields(code,site_id,display_name,has_fuel,"
                    "has_ground_handling,medevac_capable,created_at) VALUES(?,?,?,?,?,?,?)",
                    (code, site_id, display_name, 1 if has_fuel else 0,
                     1 if has_ground_handling else 0, 1 if medevac_capable else 0, self._now()),
                )
                self._audit(connection, actor_id=actor_id, action="airfield.registered",
                            resource_type="dispatch_airfield", resource_id=code,
                            detail={"display_name": display_name})
                return {"resource_type": "dispatch_airfield", "resource_id": code,
                        "response": {"code": code}}

            return self._idempotent(connection, request_id=request_id, action="register_airfield",
                                    payload=payload, create=create)

    def add_airfield_window(self, *, request_id: str, actor_id: str, window_id: str,
                            airfield_code: str, opens_at: str, closes_at: str) -> dict[str, Any]:
        payload = locals().copy()
        payload.pop("self")
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            if _parse(closes_at) <= _parse(opens_at):
                raise ValidationError("窗口关闭时间必须晚于开放时间")

            def create():
                if connection.execute("SELECT 1 FROM dispatch_airfields WHERE code=?",
                                      (airfield_code,)).fetchone() is None:
                    raise NotFoundError("机场不存在")
                connection.execute(
                    "INSERT INTO dispatch_airfield_windows(window_id,airfield_code,opens_at,closes_at) "
                    "VALUES(?,?,?,?)", (window_id, airfield_code, opens_at, closes_at))
                self._audit(connection, actor_id=actor_id, action="airfield_window.added",
                            resource_type="dispatch_airfield_window", resource_id=window_id,
                            detail={"airfield_code": airfield_code, "opens_at": opens_at,
                                    "closes_at": closes_at})
                return {"resource_type": "dispatch_airfield_window", "resource_id": window_id,
                        "response": {"window_id": window_id}}

            return self._idempotent(connection, request_id=request_id, action="add_airfield_window",
                                    payload=payload, create=create)

    def register_crew(self, *, request_id: str, actor_id: str, crew_id: str, display_name: str,
                      role: str, qualifications: list[str] | None = None,
                      duty_starts_at: str | None = None, duty_ends_at: str | None = None) -> dict[str, Any]:
        payload = locals().copy()
        payload.pop("self")
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            if role not in {"pilot", "co_pilot", "loadmaster", "medic"}:
                raise ValidationError("crew role 不被支持")
            if (duty_starts_at is None) != (duty_ends_at is None):
                raise ValidationError("执勤起止时间必须同时提供")

            def create():
                connection.execute(
                    "INSERT INTO dispatch_crew(crew_id,display_name,role,qualifications_json,"
                    "duty_starts_at,duty_ends_at,active,created_at) VALUES(?,?,?,?,?,?,1,?)",
                    (crew_id, display_name, role, canonical_json(qualifications or []),
                     duty_starts_at, duty_ends_at, self._now()),
                )
                self._audit(connection, actor_id=actor_id, action="crew.registered",
                            resource_type="dispatch_crew", resource_id=crew_id,
                            detail={"role": role, "qualifications": qualifications or []})
                return {"resource_type": "dispatch_crew", "resource_id": crew_id,
                        "response": {"crew_id": crew_id}}

            return self._idempotent(connection, request_id=request_id, action="register_crew",
                                    payload=payload, create=create)

    def register_distance(self, *, request_id: str, actor_id: str, origin_code: str,
                          destination_code: str, distance_nm: float) -> dict[str, Any]:
        payload = locals().copy()
        payload.pop("self")
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")

            def create():
                for code in (origin_code, destination_code):
                    if connection.execute("SELECT 1 FROM dispatch_airfields WHERE code=?",
                                          (code,)).fetchone() is None:
                        raise NotFoundError(f"机场 {code} 不存在")
                connection.execute(
                    "INSERT OR REPLACE INTO dispatch_distances(origin_code,destination_code,distance_nm) "
                    "VALUES(?,?,?)", (origin_code, destination_code, float(distance_nm)))
                connection.execute(
                    "INSERT OR REPLACE INTO dispatch_distances(origin_code,destination_code,distance_nm) "
                    "VALUES(?,?,?)", (destination_code, origin_code, float(distance_nm)))
                return {"resource_type": "dispatch_distance",
                        "resource_id": f"{origin_code}:{destination_code}",
                        "response": {"origin_code": origin_code,
                                     "destination_code": destination_code,
                                     "distance_nm": float(distance_nm)}}

            return self._idempotent(connection, request_id=request_id, action="register_distance",
                                    payload=payload, create=create)

    def add_fuel_stock(self, *, request_id: str, actor_id: str, stock_id: str, airfield_code: str,
                       quantity_kg: float, starts_at: str, ends_at: str) -> dict[str, Any]:
        payload = locals().copy()
        payload.pop("self")
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")

            def create():
                if connection.execute("SELECT 1 FROM dispatch_airfields WHERE code=?",
                                      (airfield_code,)).fetchone() is None:
                    raise NotFoundError("机场不存在")
                connection.execute(
                    "INSERT INTO dispatch_fuel_stocks(stock_id,airfield_code,quantity_kg,starts_at,ends_at) "
                    "VALUES(?,?,?,?,?)",
                    (stock_id, airfield_code, float(quantity_kg), starts_at, ends_at))
                return {"resource_type": "dispatch_fuel_stock", "resource_id": stock_id,
                        "response": {"stock_id": stock_id}}

            return self._idempotent(connection, request_id=request_id, action="add_fuel_stock",
                                    payload=payload, create=create)

    def issue_forecast(self, *, request_id: str, actor_id: str, airfield_code: str,
                       valid_from: str, valid_to: str, ceiling_ft: int | None,
                       visibility_km: float | None, crosswind_kt: int | None) -> dict[str, Any]:
        """发布一个新版本的预报，并把该场旧版本标记为失效。"""

        payload = {k: v for k, v in locals().copy().items() if k != "self"}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            if connection.execute("SELECT 1 FROM dispatch_airfields WHERE code=?",
                                  (airfield_code,)).fetchone() is None:
                raise NotFoundError("机场不存在")

            def create():
                row = connection.execute(
                    "SELECT COALESCE(MAX(version),0) AS v FROM dispatch_forecasts WHERE airfield_code=?",
                    (airfield_code,)).fetchone()
                version = row["v"] + 1
                connection.execute(
                    "UPDATE dispatch_forecasts SET superseded=1 WHERE airfield_code=?", (airfield_code,))
                connection.execute(
                    "INSERT INTO dispatch_forecasts(airfield_code,version,issued_at,valid_from,valid_to,"
                    "ceiling_ft,visibility_km,crosswind_kt,superseded) VALUES(?,?,?,?,?,?,?,?,0)",
                    (airfield_code, version, self._now(), valid_from, valid_to,
                     ceiling_ft, visibility_km, crosswind_kt))
                self._audit(connection, actor_id=actor_id, action="forecast.issued",
                            resource_type="dispatch_forecast", resource_id=f"{airfield_code}:v{version}",
                            detail={"airfield_code": airfield_code, "version": version})
                return {"resource_type": "dispatch_forecast",
                        "resource_id": f"{airfield_code}:v{version}",
                        "response": {"airfield_code": airfield_code, "version": version}}

            return self._idempotent(connection, request_id=request_id, action="issue_forecast",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 任务登记
    # ------------------------------------------------------------------

    def create_mission(self, *, request_id: str, actor_id: str, mission_id: str, kind: str,
                       origin_code: str, destination_code: str, passengers: int, cargo_kg: float,
                       cargo_items: list[dict[str, Any]] | None = None,
                       passenger_restriction: dict[str, Any] | None = None,
                       required_by: str | None = None, priority: int | None = None) -> dict[str, Any]:
        payload = {k: v for k, v in locals().copy().items() if k != "self"}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            if kind not in MISSION_KINDS:
                raise ValidationError("任务类型不被支持")
            for code in (origin_code, destination_code):
                if connection.execute("SELECT 1 FROM dispatch_airfields WHERE code=?",
                                      (code,)).fetchone() is None:
                    raise NotFoundError(f"机场 {code} 不存在")

            def create():
                if connection.execute("SELECT 1 FROM dispatch_missions WHERE mission_id=?",
                                      (mission_id,)).fetchone():
                    raise ConflictError("任务编号已经存在")
                items = cargo_items or []
                connection.execute(
                    "INSERT INTO dispatch_missions(mission_id,parent_mission_id,kind,priority,"
                    "origin_code,destination_code,passengers,cargo_kg,cargo_items_json,"
                    "passenger_restriction_json,required_by,current_round,state,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,1,'planning',?,?)",
                    (mission_id, None, kind,
                     int(priority) if priority is not None else DEFAULT_PRIORITY[kind],
                     origin_code, destination_code, int(passengers), float(cargo_kg),
                     canonical_json(items), canonical_json(passenger_restriction or {}),
                     required_by, actor_id, self._now()),
                )
                self._audit(connection, actor_id=actor_id, action="mission.created",
                            resource_type="dispatch_mission", resource_id=mission_id,
                            detail={"kind": kind, "origin_code": origin_code,
                                    "destination_code": destination_code})
                return {"resource_type": "dispatch_mission", "resource_id": mission_id,
                        "response": {"mission_id": mission_id, "state": "planning", "round": 1}}

            return self._idempotent(connection, request_id=request_id, action="create_mission",
                                    payload=payload, create=create)

    # ------------------------------------------------------------------
    # 候选方案
    # ------------------------------------------------------------------

    def _distance(self, connection, origin: str, destination: str) -> float | None:
        row = connection.execute(
            "SELECT distance_nm FROM dispatch_distances WHERE origin_code=? AND destination_code=?",
            (origin, destination)).fetchone()
        return None if row is None else row["distance_nm"]

    def _forecast_at(self, connection, airfield_code: str, at: str) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM dispatch_forecasts WHERE airfield_code=? AND valid_from<=? AND valid_to>=? "
            "ORDER BY version DESC LIMIT 1", (airfield_code, at, at)).fetchone()
        return None if row is None else dict(row)

    def _window(self, connection, window_id: str) -> dict[str, Any]:
        row = connection.execute(
            "SELECT w.*, a.has_fuel, a.has_ground_handling, a.medevac_capable, a.display_name AS airfield_name "
            "FROM dispatch_airfield_windows w JOIN dispatch_airfields a ON a.code=w.airfield_code "
            "WHERE w.window_id=?", (window_id,)).fetchone()
        if row is None:
            raise NotFoundError("机场窗口不存在")
        return dict(row)

    def build_candidates(self, actor_id: str, mission_id: str) -> list[dict[str, Any]]:
        """枚举飞机 × 窗口 × 机组组合，按八类因子评估并排序。"""

        with self.database.transaction() as connection:
            self._actor(connection, actor_id)
            mission = self._get_mission(connection, mission_id)
            distance = self._distance(connection, mission["origin_code"], mission["destination_code"])
            if distance is None:
                raise ValidationError("起降机场之间缺少航程资料")
            candidates: list[dict[str, Any]] = []
            aircraft_rows = connection.execute(
                "SELECT * FROM dispatch_aircraft WHERE grounded=0").fetchall()
            origin_windows = connection.execute(
                "SELECT * FROM dispatch_airfield_windows WHERE airfield_code=?",
                (mission["origin_code"],)).fetchall()
            dest_windows = connection.execute(
                "SELECT * FROM dispatch_airfield_windows WHERE airfield_code=?",
                (mission["destination_code"],)).fetchall()
            for aircraft_row in aircraft_rows:
                aircraft = dict(aircraft_row)
                aircraft["capabilities"] = json.loads(aircraft["capabilities_json"])
                crew_rows = self._eligible_crew(connection, aircraft)
                if not crew_rows:
                    continue
                for ow in origin_windows:
                    for dw in dest_windows:
                        candidate = self._candidate_for_window(
                            connection, mission=mission, aircraft=aircraft, crew_rows=crew_rows,
                            distance_nm=distance, origin_window=dict(ow), dest_window=dict(dw))
                        if candidate is not None:
                            candidates.append(candidate)
            candidates.sort(key=lambda item: item.rank_key())
            return [c.as_dict() for c in candidates]

    def _eligible_crew(self, connection, aircraft: dict[str, Any]) -> list[dict[str, Any]]:
        """挑一组满足机型签注的机组：一名机长，必要时加装卸/医疗岗。"""

        pilots = connection.execute(
            "SELECT * FROM dispatch_crew WHERE active=1 AND role IN ('pilot','co_pilot') "
            "AND qualifications_json LIKE ? ORDER BY crew_id",
            (f'%"{aircraft["required_rating"]}"%',)).fetchall()
        if not pilots:
            return []
        return [dict(pilots[0])]

    def _candidate_for_window(self, connection, *, mission: dict[str, Any], aircraft: dict[str, Any],
                              crew_rows: list[dict[str, Any]], distance_nm: float,
                              origin_window: dict[str, Any], dest_window: dict[str, Any]):
        flight_hours = distance_nm / aircraft["speed_kt"]
        fuel_required = flight_hours * aircraft["burn_kg_per_hour"] * (1 + FUEL_RESERVE_RATIO)
        payload_kg = mission["cargo_kg"] + mission["passengers"] * PAX_WEIGHT_KG

        step = timedelta(minutes=WINDOW_SLOT_MINUTES)
        dep = _parse(origin_window["opens_at"])
        origin_close = _parse(origin_window["closes_at"])
        dest_open = _parse(dest_window["opens_at"])
        dest_close = _parse(dest_window["closes_at"])
        chosen_dep = None
        flight_delta = timedelta(hours=flight_hours)
        while dep < origin_close:
            arr = dep + flight_delta
            if dest_open <= arr < dest_close:
                chosen_dep = dep
                break
            dep += step
        if chosen_dep is None:
            return None
        arr = chosen_dep + flight_delta

        # 机组执勤期过滤并补齐岗位。
        duty_rows: list[dict[str, Any]] = []
        for member in crew_rows:
            member = dict(member)
            member["qualifications"] = json.loads(member["qualifications_json"])
            if member["duty_starts_at"] and member["duty_ends_at"]:
                if not (_parse(member["duty_starts_at"]) <= chosen_dep
                        and arr <= _parse(member["duty_ends_at"])):
                    return None
            duty_rows.append(member)
        extra_role = "medic" if mission["kind"] == "medevac" else (
            "loadmaster" if mission["cargo_kg"] > 0 else None)
        if extra_role:
            extra = connection.execute(
                "SELECT * FROM dispatch_crew WHERE active=1 AND role=? ORDER BY crew_id LIMIT 1",
                (extra_role,)).fetchone()
            if extra is not None:
                extra_dict = dict(extra)
                extra_dict["qualifications"] = json.loads(extra_dict["qualifications_json"])
                if extra_dict["duty_starts_at"] and extra_dict["duty_ends_at"]:
                    if (_parse(extra_dict["duty_starts_at"]) <= chosen_dep
                            and arr <= _parse(extra_dict["duty_ends_at"])):
                        duty_rows.append(extra_dict)

        dep_s, arr_s = iso_z(chosen_dep), iso_z(arr)
        origin_fx = self._forecast_at(connection, origin_window["airfield_code"], dep_s)
        dest_fx = self._forecast_at(connection, dest_window["airfield_code"], arr_s)
        alternate, alternate_fx = self._pick_alternate(connection, mission=mission,
                                                       aircraft=aircraft, arr_at=arr_s)
        stock = connection.execute(
            "SELECT MIN(quantity_kg) AS q FROM dispatch_fuel_stocks WHERE airfield_code=? "
            "AND starts_at<=? AND ends_at>=?", (mission["origin_code"], dep_s, dep_s)).fetchone()
        fuel_available = None if stock["q"] is None else stock["q"]
        origin_af = connection.execute(
            "SELECT * FROM dispatch_airfields WHERE code=?", (mission["origin_code"],)).fetchone()
        dest_af = connection.execute(
            "SELECT * FROM dispatch_airfields WHERE code=?", (mission["destination_code"],)).fetchone()
        origin_window = {**origin_window, "airfield_code": origin_window["airfield_code"],
                         "has_ground_handling": origin_af["has_ground_handling"],
                         "medevac_capable": origin_af["medevac_capable"]}
        return evaluate_candidate(
            aircraft=aircraft, crew_rows=duty_rows, distance_nm=distance_nm, mission=mission,
            dep_at=dep_s, arr_at=arr_s, flight_hours=flight_hours, fuel_required_kg=fuel_required,
            payload_kg=payload_kg, origin_window=origin_window, dest_window={
                **dest_window, "airfield_code": dest_window["airfield_code"],
                "has_ground_handling": dest_af["has_ground_handling"],
                "medevac_capable": dest_af["medevac_capable"]},
            origin_fx=origin_fx, dest_fx=dest_fx, alternate=alternate, alternate_fx=alternate_fx,
            origin_handling=bool(origin_af["has_ground_handling"]),
            dest_handling=bool(dest_af["has_ground_handling"]),
            fuel_available_kg=fuel_available)

    def _pick_alternate(self, connection, *, mission: dict[str, Any], aircraft: dict[str, Any],
                        arr_at: str):
        rows = connection.execute(
            "SELECT d.distance_nm, a.* FROM dispatch_distances d JOIN dispatch_airfields a "
            "ON a.code=d.destination_code WHERE d.origin_code=? AND a.code<>? "
            "ORDER BY d.distance_nm", (mission["destination_code"], mission["destination_code"])
        ).fetchall()
        for row in rows:
            if not row["has_ground_handling"]:
                continue
            extra_hours = row["distance_nm"] / aircraft["speed_kt"]
            if extra_hours * aircraft["burn_kg_per_hour"] <= aircraft["fuel_capacity_kg"] * FUEL_RESERVE_RATIO:
                alternate = dict(row)
                fx = self._forecast_at(connection, row["code"], arr_at)
                if fx is None:
                    continue
                return alternate, fx
        return None, None

    # ------------------------------------------------------------------
    # 租约保留与候补
    # ------------------------------------------------------------------

    def reserve_best(self, *, request_id: str, actor_id: str, mission_id: str) -> dict[str, Any]:
        """按评估排序自动保留排名第一的可执行候选。"""

        candidates = self.build_candidates(actor_id, mission_id)
        feasible = [c for c in candidates if c["feasible"]]
        if not feasible:
            raise ConflictError("当前没有可执行候选，无法保留资源")
        chosen = feasible[0]
        return self.reserve_plan(request_id=request_id, actor_id=actor_id,
                                 mission_id=mission_id, selection=chosen)

    def reserve_plan(self, *, request_id: str, actor_id: str, mission_id: str,
                     selection: dict[str, Any]) -> dict[str, Any]:
        """以限时软租约保留候选占用的全部资源，生成待审批方案。"""

        payload = {"actor_id": actor_id, "mission_id": mission_id, "selection": selection,
                  "request_id": request_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            replay_result = self._replay(connection, request_id=request_id,
                                         action="reserve_plan", payload=payload)
            if replay_result is not None:
                return replay_result
            self._sweep_expirations(connection)
            mission = self._get_mission(connection, mission_id)
            if mission["state"] in ("in_flight", "completed"):
                raise ConflictError("任务已进入执行阶段，不能再保留方案")

            round_no = mission["current_round"]
            existing = connection.execute(
                "SELECT plan_id FROM dispatch_plans WHERE mission_id=? AND round=? AND status IN "
                "('reserved','sealed')", (mission_id, round_no)).fetchone()
            if existing:
                raise ConflictError("该轮次已存在有效方案，请先处理或等待租约过期")

            candidate = self._recompute_candidate(connection, mission, selection)
            if not candidate.feasible:
                violations = [f.code for f in candidate.factors if f.status == "violation"]
                raise ConflictError(f"候选存在违反因子，不能保留: {','.join(violations)}")

            wanted = self._wanted_resources(connection, mission, candidate)
            blockers = self._resource_blockers(connection, wanted, mission_id)
            wait_ahead = self._waitlist_ahead(connection, wanted, mission_id)
            if blockers or wait_ahead:
                # 候补登记必须在异常返回后仍然保留，因此先提交再抛出冲突。
                for want in wanted:
                    self._join_waitlist(connection, mission_id=mission_id, plan_id=None,
                                        resource_type=want["resource_type"],
                                        resource_key=want["resource_key"])
                self._audit(connection, actor_id=actor_id, action="plan.waitlisted",
                            resource_type="dispatch_mission", resource_id=mission_id,
                            detail={"blockers": blockers, "waitlist_ahead": wait_ahead})
                connection.commit()
                raise ConflictError(json.dumps({
                    "error": "resource_busy",
                    "blockers": blockers,
                    "waitlist_ahead": wait_ahead,
                }, ensure_ascii=False))

            plan_id, leg_id, lease_ids, expires = self._create_reserved_plan(
                connection, actor_id=actor_id, mission=mission, round_no=round_no,
                candidate=candidate, reason=None)
            self._consume_waitlist(connection, wanted, mission_id)
            self._audit(connection, actor_id=actor_id, action="plan.reserved",
                        resource_type="dispatch_plan", resource_id=plan_id,
                        detail={"mission_id": mission_id, "round": round_no,
                                "aircraft_id": candidate.aircraft_id, "expires_at": expires,
                                "score": candidate.score})
            response = {"plan_id": plan_id, "mission_id": mission_id, "round": round_no,
                        "status": "reserved", "version_token": self._plan_token(connection, plan_id),
                        "expires_at": expires, "lease_ids": lease_ids,
                        "candidate": candidate.as_dict()}
            result = {"resource_type": "dispatch_plan", "resource_id": plan_id,
                      "response": response}
            self._store_receipt(connection, request_id=request_id, action="reserve_plan",
                                payload_hash=digest(payload), result=result)
            return {**response, "replayed": False}

    def _recompute_candidate(self, connection, mission: dict[str, Any], selection: dict[str, Any]):
        aircraft_row = connection.execute(
            "SELECT * FROM dispatch_aircraft WHERE aircraft_id=?", (selection["aircraft_id"],)
        ).fetchone()
        if aircraft_row is None:
            raise NotFoundError("飞机不存在")
        aircraft = dict(aircraft_row)
        aircraft["capabilities"] = json.loads(aircraft["capabilities_json"])
        crew_rows = []
        for crew_id in selection.get("crew_ids", []):
            row = connection.execute("SELECT * FROM dispatch_crew WHERE crew_id=?", (crew_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"机组成员 {crew_id} 不存在")
            member = dict(row)
            member["qualifications"] = json.loads(member["qualifications_json"])
            crew_rows.append(member)
        distance = self._distance(connection, mission["origin_code"], mission["destination_code"])
        if distance is None:
            raise ValidationError("缺少航程资料")
        dep = _parse(selection["dep_at"])
        flight_hours = distance / aircraft["speed_kt"]
        arr = dep + timedelta(hours=flight_hours)
        fuel_required = flight_hours * aircraft["burn_kg_per_hour"] * (1 + FUEL_RESERVE_RATIO)
        payload_kg = mission["cargo_kg"] + mission["passengers"] * PAX_WEIGHT_KG
        ow = self._window(connection, selection["origin_window_id"])
        dw = self._window(connection, selection["destination_window_id"])
        dep_s, arr_s = iso_z(dep), iso_z(arr)
        origin_fx = self._forecast_at(connection, ow["airfield_code"], dep_s)
        dest_fx = self._forecast_at(connection, dw["airfield_code"], arr_s)
        alternate, alternate_fx = (None, None)
        if selection.get("alternate_code"):
            row = connection.execute("SELECT * FROM dispatch_airfields WHERE code=?",
                                     (selection["alternate_code"],)).fetchone()
            if row is not None:
                alternate = dict(row)
                alternate_fx = self._forecast_at(connection, row["code"], arr_s)
        stock = connection.execute(
            "SELECT MIN(quantity_kg) AS q FROM dispatch_fuel_stocks WHERE airfield_code=? "
            "AND starts_at<=? AND ends_at>=?", (mission["origin_code"], dep_s, dep_s)).fetchone()
        return evaluate_candidate(
            aircraft=aircraft, crew_rows=crew_rows, distance_nm=distance, mission=mission,
            dep_at=dep_s, arr_at=arr_s, flight_hours=flight_hours, fuel_required_kg=fuel_required,
            payload_kg=payload_kg, origin_window=ow, dest_window=dw, origin_fx=origin_fx,
            dest_fx=dest_fx, alternate=alternate, alternate_fx=alternate_fx,
            origin_handling=bool(ow["has_ground_handling"]),
            dest_handling=bool(dw["has_ground_handling"]),
            fuel_available_kg=None if stock["q"] is None else stock["q"])

    def _wanted_resources(self, connection, mission: dict[str, Any], candidate) -> list[dict[str, Any]]:
        dep, arr = candidate.dep_at, candidate.arr_at
        wanted = [
            {"resource_type": "aircraft", "resource_key": candidate.aircraft_id,
             "starts_at": dep, "ends_at": arr, "quantity": 1.0},
            {"resource_type": "window", "resource_key": candidate.origin_window_id,
             "starts_at": dep, "ends_at": arr, "quantity": 1.0},
            {"resource_type": "window", "resource_key": candidate.destination_window_id,
             "starts_at": dep, "ends_at": arr, "quantity": 1.0},
            {"resource_type": "ground", "resource_key": mission["origin_code"],
             "starts_at": dep, "ends_at": arr, "quantity": 1.0},
            {"resource_type": "ground", "resource_key": mission["destination_code"],
             "starts_at": dep, "ends_at": arr, "quantity": 1.0},
            {"resource_type": "fuel", "resource_key": mission["origin_code"],
             "starts_at": dep, "ends_at": iso_z(_parse(dep) + timedelta(minutes=1)),
             "quantity": candidate.fuel_required_kg},
        ]
        for crew_id in candidate.crew_ids:
            wanted.append({"resource_type": "crew", "resource_key": crew_id,
                           "starts_at": dep, "ends_at": arr, "quantity": 1.0})
        return wanted

    def _active_overlaps(self, connection, *, resource_type: str, resource_key: str,
                         starts_at: str, ends_at: str):
        return connection.execute(
            "SELECT * FROM dispatch_leases WHERE resource_type=? AND resource_key=? "
            "AND status IN ('held','committed','departed') AND starts_at<? AND ends_at>? "
            "ORDER BY acquired_at",
            (resource_type, resource_key, ends_at, starts_at)).fetchall()

    def _resource_blockers(self, connection, wanted: list[dict[str, Any]],
                           mission_id: str) -> list[dict[str, Any]]:
        blockers = []
        for want in wanted:
            overlaps = [row for row in self._active_overlaps(connection, **{
                k: want[k] for k in ("resource_type", "resource_key", "starts_at", "ends_at")})
                if row["mission_id"] != mission_id]
            if want["resource_type"] == "fuel":
                used = sum(row["quantity"] for row in overlaps)
                stock = connection.execute(
                    "SELECT MIN(quantity_kg) AS q FROM dispatch_fuel_stocks WHERE airfield_code=? "
                    "AND starts_at<=? AND ends_at>=?",
                    (want["resource_key"], want["starts_at"], want["starts_at"])).fetchone()["q"]
                if stock is None or used + want["quantity"] > stock + 1e-6:
                    blockers.append({"resource_type": "fuel", "resource_key": want["resource_key"],
                                     "used_kg": used, "available_kg": stock,
                                     "needed_kg": want["quantity"]})
            elif overlaps:
                row = overlaps[0]
                blockers.append({"resource_type": want["resource_type"],
                                 "resource_key": want["resource_key"],
                                 "held_by_mission": row["mission_id"],
                                 "lease_id": row["lease_id"],
                                 "lease_status": row["status"]})
        return blockers

    def _waitlist_ahead(self, connection, wanted: list[dict[str, Any]],
                        mission_id: str) -> list[dict[str, Any]]:
        ahead = []
        for want in wanted:
            rows = connection.execute(
                "SELECT * FROM dispatch_waitlist WHERE resource_type=? AND resource_key=? "
                "AND status='waiting' ORDER BY seq",
                (want["resource_type"], want["resource_key"])).fetchall()
            for row in rows:
                if row["mission_id"] != mission_id:
                    ahead.append({"resource_type": want["resource_type"],
                                  "resource_key": want["resource_key"],
                                  "mission_id": row["mission_id"], "seq": row["seq"]})
                    break
            promoted = connection.execute(
                "SELECT * FROM dispatch_waitlist WHERE resource_type=? AND resource_key=? "
                "AND status='promoted' AND mission_id<>? ORDER BY promoted_at",
                (want["resource_type"], want["resource_key"], mission_id)).fetchall()
            for row in promoted:
                ahead.append({"resource_type": want["resource_type"],
                              "resource_key": want["resource_key"],
                              "mission_id": row["mission_id"], "seq": row["seq"],
                              "promoted": True})
        return ahead

    def _join_waitlist(self, connection, *, mission_id: str, plan_id: str | None,
                       resource_type: str, resource_key: str) -> None:
        exists = connection.execute(
            "SELECT 1 FROM dispatch_waitlist WHERE resource_type=? AND resource_key=? "
            "AND mission_id=? AND status IN ('waiting','promoted')",
            (resource_type, resource_key, mission_id)).fetchone()
        if exists:
            return
        seq_row = connection.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS next FROM dispatch_waitlist WHERE resource_type=? "
            "AND resource_key=?", (resource_type, resource_key)).fetchone()
        entry_id = f"wl-{uuid.uuid4().hex[:12]}"
        connection.execute(
            "INSERT INTO dispatch_waitlist(entry_id,resource_type,resource_key,mission_id,plan_id,"
            "seq,status,enqueued_at) VALUES(?,?,?,?,?,?,'waiting',?)",
            (entry_id, resource_type, resource_key, mission_id, plan_id, seq_row["next"],
             self._now()))
        self._ledger(connection, movement="waitlist.join", resource_type=resource_type,
                     resource_key=resource_key, reason="resource_busy", mission_id=mission_id,
                     plan_id=plan_id)

    def _consume_waitlist(self, connection, wanted: list[dict[str, Any]], mission_id: str) -> None:
        for want in wanted:
            connection.execute(
                "UPDATE dispatch_waitlist SET status='consumed' WHERE resource_type=? "
                "AND resource_key=? AND mission_id=? AND status IN ('waiting','promoted')",
                (want["resource_type"], want["resource_key"], mission_id))

    def _create_reserved_plan(self, connection, *, actor_id: str, mission: dict[str, Any],
                              round_no: int, candidate, reason: str | None
                              ) -> tuple[str, str, list[str], str]:
        """插入待审批方案、首航段并获取全部资源租约。"""

        plan_id = f"plan-{uuid.uuid4().hex[:12]}"
        leg_id = f"leg-{uuid.uuid4().hex[:12]}"
        now = self._now()
        expires = iso_z(self._now_dt() + self.lease_ttl)
        connection.execute(
            "INSERT INTO dispatch_plans(plan_id,mission_id,round,status,aircraft_id,"
            "origin_window_id,destination_window_id,alternate_code,dep_at,arr_at,"
            "fuel_required_kg,payload_kg,crew_json,forecast_versions_json,factors_json,score,"
            "version_token,reason,created_by,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (plan_id, mission["mission_id"], round_no, "reserved", candidate.aircraft_id,
             candidate.origin_window_id, candidate.destination_window_id,
             candidate.alternate_code, candidate.dep_at, candidate.arr_at,
             candidate.fuel_required_kg, candidate.payload_kg,
             canonical_json(list(candidate.crew_ids)),
             canonical_json(candidate.forecast_versions),
             canonical_json([f.as_dict() for f in candidate.factors]),
             candidate.score, uuid.uuid4().hex, reason, actor_id, now))
        connection.execute(
            "INSERT INTO dispatch_plan_legs(leg_id,plan_id,sequence,aircraft_id,origin_code,"
            "destination_code,alternate_code,dep_at,arr_at,crew_json,fuel_required_kg,"
            "payload_kg,pax,origin_window_id,destination_window_id,forecast_versions_json) "
            "VALUES(?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (leg_id, plan_id, candidate.aircraft_id, mission["origin_code"],
             mission["destination_code"], candidate.alternate_code, candidate.dep_at,
             candidate.arr_at, canonical_json(list(candidate.crew_ids)),
             candidate.fuel_required_kg, candidate.payload_kg, mission["passengers"],
             candidate.origin_window_id, candidate.destination_window_id,
             canonical_json(candidate.forecast_versions)))
        lease_ids = []
        for want in self._wanted_resources(connection, mission, candidate):
            lease_ids.append(self._acquire_lease(
                connection, plan_id=plan_id, mission_id=mission["mission_id"], leg_id=leg_id,
                expires_at=expires, **want))
        return plan_id, leg_id, lease_ids, expires

    def _plan_token(self, connection, plan_id: str) -> str:
        return connection.execute("SELECT version_token FROM dispatch_plans WHERE plan_id=?",
                                  (plan_id,)).fetchone()["version_token"]

    def _acquire_lease(self, connection, *, plan_id: str, mission_id: str, leg_id: str,
                       resource_type: str, resource_key: str, starts_at: str, ends_at: str,
                       quantity: float, expires_at: str) -> str:
        lease_id = f"lease-{uuid.uuid4().hex[:12]}"
        connection.execute(
            "INSERT INTO dispatch_leases(lease_id,plan_id,mission_id,leg_id,resource_type,"
            "resource_key,starts_at,ends_at,quantity,status,expires_at,acquired_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,'held',?,?)",
            (lease_id, plan_id, mission_id, leg_id, resource_type, resource_key,
             starts_at, ends_at, quantity, expires_at, self._now()))
        self._ledger(connection, movement="lease.acquire", resource_type=resource_type,
                     resource_key=resource_key, reason="plan_reserved", mission_id=mission_id,
                     plan_id=plan_id, leg_id=leg_id, starts_at=starts_at, ends_at=ends_at,
                     quantity=quantity, lease_id=lease_id)
        return lease_id

    def _release_lease(self, connection, row, *, reason: str, alert_key: str | None) -> None:
        connection.execute(
            "UPDATE dispatch_leases SET status='released', released_at=?, release_reason=? "
            "WHERE lease_id=? AND status IN ('held','committed')",
            (self._now(), reason, row["lease_id"]))
        self._ledger(connection, movement="lease.release", resource_type=row["resource_type"],
                     resource_key=row["resource_key"], reason=reason, mission_id=row["mission_id"],
                     plan_id=row["plan_id"], leg_id=row["leg_id"],
                     commitment_id=row["commitment_id"], starts_at=row["starts_at"],
                     ends_at=row["ends_at"], quantity=row["quantity"], alert_key=alert_key,
                     lease_id=row["lease_id"])
        self._promote_waitlist(connection, row["resource_type"], row["resource_key"])

    def _promote_waitlist(self, connection, resource_type: str, resource_key: str) -> None:
        """资源释放后把队首候补提升为下一个获得保留机会的任务。"""

        if connection.execute(
            "SELECT 1 FROM dispatch_waitlist WHERE resource_type=? AND resource_key=? "
            "AND status='promoted'", (resource_type, resource_key)).fetchone():
            return
        if connection.execute(
            "SELECT 1 FROM dispatch_leases WHERE resource_type=? AND resource_key=? "
            "AND status IN ('held','committed','departed')",
                (resource_type, resource_key)).fetchone():
            return
        head = connection.execute(
            "SELECT * FROM dispatch_waitlist WHERE resource_type=? AND resource_key=? "
            "AND status='waiting' ORDER BY seq LIMIT 1",
            (resource_type, resource_key)).fetchone()
        if head is None:
            return
        connection.execute(
            "UPDATE dispatch_waitlist SET status='promoted', promoted_at=? WHERE entry_id=?",
            (self._now(), head["entry_id"]))
        self._ledger(connection, movement="waitlist.promote", resource_type=resource_type,
                     resource_key=resource_key, reason="resource_freed",
                     mission_id=head["mission_id"], plan_id=head["plan_id"])

    # ------------------------------------------------------------------
    # 双方批准与封存
    # ------------------------------------------------------------------

    def reject_plan(self, *, request_id: str, actor_id: str, plan_id: str,
                    party: str, reason: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "plan_id": plan_id, "party": party, "reason": reason,
                   "request_id": request_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator", "reviewer")
            replay_result = self._replay(connection, request_id=request_id,
                                         action="reject_plan", payload=payload)
            if replay_result is not None:
                return replay_result
            self._sweep_expirations(connection)
            plan = self._get_plan(connection, plan_id)
            if plan["status"] != "reserved":
                raise ConflictError("只有待审批方案可以驳回")
            connection.execute(
                "UPDATE dispatch_plans SET status='rejected', reason=? WHERE plan_id=?",
                (reason, plan_id))
            for row in connection.execute("SELECT * FROM dispatch_leases WHERE plan_id=? AND status='held'",
                                          (plan_id,)).fetchall():
                self._release_lease(connection, row, reason=f"rejected_by_{party}", alert_key=None)
            self._audit(connection, actor_id=actor_id, action="plan.rejected",
                        resource_type="dispatch_plan", resource_id=plan_id,
                        detail={"party": party, "reason": reason})
            response = {"plan_id": plan_id, "status": "rejected"}
            result = {"resource_type": "dispatch_plan", "resource_id": plan_id,
                      "response": response}
            self._store_receipt(connection, request_id=request_id, action="reject_plan",
                                payload_hash=digest(payload), result=result)
            return {**response, "replayed": False}

    def approve_plan(self, *, request_id: str, actor_id: str, plan_id: str, party: str) -> dict[str, Any]:
        """记录运行或站点一方的批准；双方齐备时唯一版本封存为承诺。"""

        payload = {"actor_id": actor_id, "plan_id": plan_id, "party": party,
                   "request_id": request_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            if party not in APPROVAL_ROLES:
                raise ValidationError("party 必须是 operations 或 station")
            self._require_roles(actor, *APPROVAL_ROLES[party])
            replay_result = self._replay(connection, request_id=request_id,
                                         action="approve_plan", payload=payload)
            if replay_result is not None:
                return replay_result
            self._sweep_expirations(connection)
            plan = self._get_plan(connection, plan_id)
            if plan["status"] != "reserved":
                raise ConflictError(f"方案当前状态 {plan['status']}，不能批准")
            active_leases = connection.execute(
                "SELECT COUNT(*) AS c FROM dispatch_leases WHERE plan_id=? AND status='held'",
                (plan_id,)).fetchone()["c"]
            if not active_leases:
                connection.execute(
                    "UPDATE dispatch_plans SET status='expired' WHERE plan_id=? AND status='reserved'",
                    (plan_id,))
                self._audit(connection, actor_id=actor_id, action="plan.expired",
                            resource_type="dispatch_plan", resource_id=plan_id,
                            detail={"reason": "leases_expired_before_approval"})
                # 过期状态与已释放租约必须保留，先提交再返回冲突。
                connection.commit()
                raise ConflictError("资源租约已全部过期，方案需重新保留")

            column = "operations_approved_by" if party == "operations" else "station_approved_by"
            at_column = "operations_approved_at" if party == "operations" else "station_approved_at"
            if plan[column] is not None:
                return {"plan_id": plan_id, "status": "reserved", "party": party,
                        "already_approved": True, "sealed": False}
            other_column = "station_approved_by" if party == "operations" else "operations_approved_by"
            if plan[other_column] == actor_id:
                raise PermissionDenied("运行与站点批准必须由不同操作者完成")
            connection.execute(
                f"UPDATE dispatch_plans SET {column}=?, {at_column}=? WHERE plan_id=?",
                (actor_id, self._now(), plan_id))
            self._audit(connection, actor_id=actor_id, action="plan.approved",
                        resource_type="dispatch_plan", resource_id=plan_id,
                        detail={"party": party})

            sealed = self._seal_if_both_approved(connection, plan_id, actor_id)
            response = {"plan_id": plan_id, "sealed": sealed,
                        "status": "sealed" if sealed else "reserved",
                        "party": party, "already_approved": False}
            result = {"resource_type": "dispatch_plan", "resource_id": plan_id,
                      "response": response}
            self._store_receipt(connection, request_id=request_id, action="approve_plan",
                                payload_hash=digest(payload), result=result)
            return {**response, "replayed": False}

    def _seal_if_both_approved(self, connection, plan_id: str, actor_id: str) -> bool:
        """条件更新：只有仍是待审批且双方齐备的方案才能封存，并发下仅一行生效。"""

        cursor = connection.execute(
            "UPDATE dispatch_plans SET status='sealed', sealed_at=? "
            "WHERE plan_id=? AND status='reserved' AND operations_approved_by IS NOT NULL "
            "AND station_approved_by IS NOT NULL",
            (self._now(), plan_id))
        if cursor.rowcount != 1:
            return False
        plan = self._get_plan(connection, plan_id)
        connection.execute(
            "UPDATE dispatch_leases SET status='committed' WHERE plan_id=? AND status='held'",
            (plan_id,))
        legs = connection.execute(
            "SELECT * FROM dispatch_plan_legs WHERE plan_id=? ORDER BY sequence", (plan_id,)).fetchall()
        for leg in legs:
            commitment_id = f"cmt-{uuid.uuid4().hex[:12]}"
            connection.execute(
                "INSERT INTO dispatch_commitments(commitment_id,mission_id,round,plan_id,leg_id,"
                "seq,state,created_at) VALUES(?,?,?,?,?,?, 'committed', ?)",
                (commitment_id, plan["mission_id"], plan["round"], plan_id, leg["leg_id"],
                 leg["sequence"], self._now()))
            connection.execute(
                "UPDATE dispatch_leases SET commitment_id=? WHERE leg_id=?",
                (commitment_id, leg["leg_id"]))
            self._ledger(connection, movement="commitment.sealed", resource_type="leg",
                         resource_key=leg["leg_id"], reason="both_parties_approved",
                         mission_id=plan["mission_id"], plan_id=plan_id, leg_id=leg["leg_id"])
        connection.execute(
            "UPDATE dispatch_missions SET state='sealed' WHERE mission_id=? AND state<>'in_flight'",
            (plan["mission_id"],))
        self._audit(connection, actor_id=actor_id, action="plan.sealed",
                    resource_type="dispatch_plan", resource_id=plan_id,
                    detail={"mission_id": plan["mission_id"], "round": plan["round"],
                            "version_token": plan["version_token"]})
        return True

    # ------------------------------------------------------------------
    # 执行交接
    # ------------------------------------------------------------------

    def mark_departed(self, *, request_id: str, actor_id: str, commitment_id: str) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "commitment_id": commitment_id, "request_id": request_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            replay_result = self._replay(connection, request_id=request_id,
                                         action="mark_departed", payload=payload)
            if replay_result is not None:
                return replay_result
            self._sweep_expirations(connection)
            commitment = self._get_commitment(connection, commitment_id)
            if commitment["state"] != "committed":
                raise ConflictError("只有已封存未执行的承诺可以标记起飞")
            connection.execute(
                "UPDATE dispatch_commitments SET state='departed', departed_at=? WHERE commitment_id=?",
                (self._now(), commitment_id))
            connection.execute(
                "UPDATE dispatch_leases SET status='departed' WHERE commitment_id=? "
                "AND status='committed'", (commitment_id,))
            connection.execute(
                "UPDATE dispatch_missions SET state='in_flight' WHERE mission_id=?",
                (commitment["mission_id"],))
            self._ledger(connection, movement="commitment.departed", resource_type="leg",
                         resource_key=commitment["leg_id"], reason="aircraft_departed",
                         mission_id=commitment["mission_id"], plan_id=commitment["plan_id"],
                         leg_id=commitment["leg_id"], commitment_id=commitment_id)
            self._audit(connection, actor_id=actor_id, action="commitment.departed",
                        resource_type="dispatch_commitment", resource_id=commitment_id,
                        detail={"mission_id": commitment["mission_id"]})
            response = {"commitment_id": commitment_id, "state": "departed"}
            result = {"resource_type": "dispatch_commitment", "resource_id": commitment_id,
                      "response": response}
            self._store_receipt(connection, request_id=request_id, action="mark_departed",
                                payload_hash=digest(payload), result=result)
            return {**response, "replayed": False}

    def mark_handover(self, *, request_id: str, actor_id: str, commitment_id: str,
                      notes: str | None = None) -> dict[str, Any]:
        payload = {"actor_id": actor_id, "commitment_id": commitment_id, "notes": notes,
                   "request_id": request_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator", "reviewer")
            replay_result = self._replay(connection, request_id=request_id,
                                         action="mark_handover", payload=payload)
            if replay_result is not None:
                return replay_result
            self._sweep_expirations(connection)
            commitment = self._get_commitment(connection, commitment_id)
            if commitment["state"] != "departed":
                raise ConflictError("只有已起飞的承诺可以完成交接")
            connection.execute(
                "UPDATE dispatch_commitments SET state='handed_over', handed_over_at=? "
                "WHERE commitment_id=?", (self._now(), commitment_id))
            for row in connection.execute(
                "SELECT * FROM dispatch_leases WHERE commitment_id=? AND status='departed'",
                    (commitment_id,)).fetchall():
                connection.execute(
                    "UPDATE dispatch_leases SET status='done', released_at=?, release_reason='handover' "
                    "WHERE lease_id=?", (self._now(), row["lease_id"]))
                self._ledger(connection, movement="lease.release", resource_type=row["resource_type"],
                             resource_key=row["resource_key"], reason="handover_completed",
                             mission_id=row["mission_id"], plan_id=row["plan_id"],
                             leg_id=row["leg_id"], commitment_id=commitment_id,
                             lease_id=row["lease_id"])
                self._promote_waitlist(connection, row["resource_type"], row["resource_key"])
            pending = connection.execute(
                "SELECT COUNT(*) AS c FROM dispatch_commitments WHERE mission_id=? "
                "AND state IN ('committed','departed')", (commitment["mission_id"],)).fetchone()["c"]
            if pending == 0:
                connection.execute(
                    "UPDATE dispatch_missions SET state='completed' WHERE mission_id=?",
                    (commitment["mission_id"],))
            self._ledger(connection, movement="commitment.handed_over", resource_type="leg",
                         resource_key=commitment["leg_id"], reason=notes or "handover_completed",
                         mission_id=commitment["mission_id"], plan_id=commitment["plan_id"],
                         leg_id=commitment["leg_id"], commitment_id=commitment_id)
            self._audit(connection, actor_id=actor_id, action="commitment.handed_over",
                        resource_type="dispatch_commitment", resource_id=commitment_id,
                        detail={"mission_id": commitment["mission_id"], "notes": notes})
            response = {"commitment_id": commitment_id, "state": "handed_over"}
            result = {"resource_type": "dispatch_commitment", "resource_id": commitment_id,
                      "response": response}
            self._store_receipt(connection, request_id=request_id, action="mark_handover",
                                payload_hash=digest(payload), result=result)
            return {**response, "replayed": False}

    # ------------------------------------------------------------------
    # 告警：气象修订 / 飞机故障 / 部分卸载 / 医疗插队
    # ------------------------------------------------------------------

    def ingest_alert(self, *, request_id: str, actor_id: str, alert_key: str, kind: str,
                     payload: dict[str, Any]) -> dict[str, Any]:
        envelope = {"actor_id": actor_id, "alert_key": alert_key, "kind": kind,
                    "payload": payload, "request_id": request_id}
        with self.database.transaction(immediate=True) as connection:
            actor = self._actor(connection, actor_id)
            self._require_roles(actor, "admin", "operator")
            replay_result = self._replay(connection, request_id=request_id,
                                         action="ingest_alert", payload=envelope)
            if replay_result is not None:
                return replay_result
            self._sweep_expirations(connection)
            duplicate = connection.execute(
                "SELECT * FROM dispatch_alerts WHERE alert_key=?", (alert_key,)).fetchone()
            if duplicate:
                return {"alert_key": alert_key, "status": duplicate["status"],
                        "replayed": True, "effects": []}
            if kind not in {"weather_revision", "aircraft_fault", "partial_unload",
                            "medevac_preempt"}:
                raise ValidationError("告警类型不被支持")
            connection.execute(
                "INSERT INTO dispatch_alerts(alert_key,kind,payload_json,status,received_at,processed_at) "
                "VALUES(?,?,?,'processed',?,?)",
                (alert_key, kind, canonical_json(payload), self._now(), self._now()))
            effects: list[dict[str, Any]] = []
            if kind == "weather_revision":
                effects = self._effect_weather(connection, actor_id, alert_key, payload)
            elif kind == "aircraft_fault":
                effects = self._effect_aircraft_fault(connection, actor_id, alert_key, payload)
            elif kind == "partial_unload":
                effects = self._effect_partial_unload(connection, actor_id, alert_key, payload)
            elif kind == "medevac_preempt":
                effects = self._effect_medevac_preempt(connection, actor_id, alert_key, payload)
            self._audit(connection, actor_id=actor_id, action=f"alert.{kind}",
                        resource_type="dispatch_alert", resource_id=alert_key,
                        detail={"effects": len(effects)})
            response = {"alert_key": alert_key, "status": "processed", "replayed": False,
                        "effects": effects}
            result = {"resource_type": "dispatch_alert", "resource_id": alert_key,
                      "response": response}
            self._store_receipt(connection, request_id=request_id, action="ingest_alert",
                                payload_hash=digest(envelope), result=result)
            return response

    def _affected_pending(self, connection, *, predicate_sql: str, parameters: tuple):
        """选取尚未起飞且确实受影响的租约（held 或 committed）。"""

        return connection.execute(
            "SELECT * FROM dispatch_leases WHERE status IN ('held','committed') AND ("
            + predicate_sql + ")", parameters).fetchall()

    def _replan_mission_round(self, connection, *, mission_id: str, reason: str,
                              alert_key: str, actor_id: str) -> int:
        """把任务当前轮次的未执行方案/承诺作废并开启新一轮，返回新轮次。"""

        mission = self._get_mission(connection, mission_id)
        round_no = mission["current_round"]
        plan_rows = connection.execute(
            "SELECT * FROM dispatch_plans WHERE mission_id=? AND round=? AND status IN "
            "('reserved','sealed')", (mission_id, round_no)).fetchall()
        if not plan_rows:
            return round_no
        for plan in plan_rows:
            connection.execute(
                "UPDATE dispatch_plans SET status='superseded', reason=? WHERE plan_id=?",
                (reason, plan["plan_id"]))
            for row in connection.execute(
                "SELECT * FROM dispatch_leases WHERE plan_id=? AND status IN ('held','committed')",
                    (plan["plan_id"],)).fetchall():
                self._release_lease(connection, row, reason=reason, alert_key=alert_key)
                self._record_alert_effect(connection, alert_key=alert_key,
                                          commitment_id=row["commitment_id"], mission_id=mission_id,
                                          action="lease.released",
                                          detail={"lease_id": row["lease_id"],
                                                  "resource_type": row["resource_type"],
                                                  "resource_key": row["resource_key"]})
            for cmt in connection.execute(
                "SELECT * FROM dispatch_commitments WHERE plan_id=? AND state='committed'",
                    (plan["plan_id"],)).fetchall():
                connection.execute(
                    "UPDATE dispatch_commitments SET state='superseded', replaced_at=?, "
                    "replace_reason=?, alert_key=? WHERE commitment_id=?",
                    (self._now(), reason, alert_key, cmt["commitment_id"]))
                self._record_alert_effect(connection, alert_key=alert_key,
                                          commitment_id=cmt["commitment_id"], mission_id=mission_id,
                                          action="commitment.superseded",
                                          detail={"plan_id": plan["plan_id"]})
        new_round = round_no + 1
        connection.execute(
            "UPDATE dispatch_missions SET current_round=?, state='replanning' WHERE mission_id=?",
            (new_round, mission_id))
        self._ledger(connection, movement="mission.replan", resource_type="mission",
                     resource_key=mission_id, reason=reason, mission_id=mission_id,
                     alert_key=alert_key)
        return new_round

    def _record_alert_effect(self, connection, *, alert_key: str, commitment_id: str | None,
                             mission_id: str | None, action: str, detail: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO dispatch_alert_effects(effect_id,alert_key,commitment_id,mission_id,"
            "action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
            (f"eff-{uuid.uuid4().hex[:12]}", alert_key, commitment_id, mission_id, action,
             canonical_json(detail), self._now()))

    def _effect_weather(self, connection, actor_id: str, alert_key: str,
                        payload: dict[str, Any]) -> list[dict[str, Any]]:
        airfield = payload["airfield_code"]
        # 告警载荷携带最新预报，直接登记新版本并失效旧版本。
        row = connection.execute(
            "SELECT COALESCE(MAX(version),0) AS v FROM dispatch_forecasts WHERE airfield_code=?",
            (airfield,)).fetchone()
        version = row["v"] + 1
        connection.execute(
            "UPDATE dispatch_forecasts SET superseded=1 WHERE airfield_code=?", (airfield,))
        connection.execute(
            "INSERT INTO dispatch_forecasts(airfield_code,version,issued_at,valid_from,valid_to,"
            "ceiling_ft,visibility_km,crosswind_kt,superseded) VALUES(?,?,?,?,?,?,?,?,0)",
            (airfield, version, self._now(), payload["valid_from"], payload["valid_to"],
             payload.get("ceiling_ft"), payload.get("visibility_km"),
             payload.get("crosswind_kt")))
        # 选取当前轮次仍待执行（reserved/sealed）且使用该机场窗口的方案，
        # 已起飞或完成交接的任务不参与重评，用新预报重新评估其余原候选；
        # 只有原方案因此变为不可执行才重排。
        plan_rows = connection.execute(
            "SELECT p.* FROM dispatch_plans p JOIN dispatch_missions m ON m.mission_id=p.mission_id "
            "WHERE p.status IN ('reserved','sealed') AND m.state NOT IN ('in_flight','completed') "
            "AND m.current_round=p.round AND ("
            "p.origin_window_id IN (SELECT window_id FROM dispatch_airfield_windows "
            "WHERE airfield_code=?) OR p.destination_window_id IN (SELECT window_id FROM "
            "dispatch_airfield_windows WHERE airfield_code=?) OR p.alternate_code=?)",
            (airfield, airfield, airfield)).fetchall()
        effects = [{"action": "forecast.versioned", "airfield_code": airfield, "version": version}]
        for plan_row in plan_rows:
            mission_id = plan_row["mission_id"]
            mission = self._get_mission(connection, mission_id)
            selection = {
                "aircraft_id": plan_row["aircraft_id"],
                "crew_ids": json.loads(plan_row["crew_json"]),
                "dep_at": plan_row["dep_at"],
                "arr_at": plan_row["arr_at"],
                "origin_window_id": plan_row["origin_window_id"],
                "destination_window_id": plan_row["destination_window_id"],
                "alternate_code": plan_row["alternate_code"],
            }
            reevaluated = self._recompute_candidate(connection, mission, selection)
            weather_factor = next(f for f in reevaluated.factors
                                  if f.code == "weather_forecast")
            if reevaluated.feasible and weather_factor.status != "violation":
                effects.append({"action": "commitment.kept", "mission_id": mission_id,
                                "plan_id": plan_row["plan_id"],
                                "note": "预报修订后原候选仍可执行，承诺保留"})
                continue
            self._replan_mission_round(connection, mission_id=mission_id,
                                       reason="weather_revision", alert_key=alert_key,
                                       actor_id=actor_id)
            effects.append({"action": "mission.replanned", "mission_id": mission_id,
                            "alert_key": alert_key})
        return effects

    def _effect_aircraft_fault(self, connection, actor_id: str, alert_key: str,
                               payload: dict[str, Any]) -> list[dict[str, Any]]:
        aircraft_id = payload["aircraft_id"]
        grounded = bool(payload.get("grounded", True))
        connection.execute(
            "UPDATE dispatch_aircraft SET grounded=? WHERE aircraft_id=?",
            (1 if grounded else 0, aircraft_id))
        effects = [{"action": "aircraft.grounded", "aircraft_id": aircraft_id, "grounded": grounded}]
        if not grounded:
            return effects
        affected = self._affected_pending(
            connection,
            predicate_sql="resource_type='aircraft' AND resource_key=?",
            parameters=(aircraft_id,))
        missions = sorted({row["mission_id"] for row in affected})
        for mission_id in missions:
            self._replan_mission_round(connection, mission_id=mission_id,
                                       reason="aircraft_fault", alert_key=alert_key,
                                       actor_id=actor_id)
            effects.append({"action": "mission.replanned", "mission_id": mission_id,
                            "alert_key": alert_key})
        return effects

    def _effect_partial_unload(self, connection, actor_id: str, alert_key: str,
                               payload: dict[str, Any]) -> list[dict[str, Any]]:
        mission_id = payload["mission_id"]
        offloaded_ids = set(payload.get("cargo_item_ids", ()))
        mission = self._get_mission(connection, mission_id)
        items = [item for item in json.loads(mission["cargo_items_json"])
                 if item["id"] not in offloaded_ids]
        removed = [item for item in json.loads(mission["cargo_items_json"])
                   if item["id"] in offloaded_ids]
        if not removed:
            return [{"action": "partial_unload.noop", "mission_id": mission_id}]
        effects = [{"action": "cargo.unloaded", "mission_id": mission_id,
                    "items": removed, "note": "卸载货物就地交接，等待后继航班"}]
        # 只有存在未执行承诺时才重排；已起飞航段不处理。
        pending = connection.execute(
            "SELECT 1 FROM dispatch_leases WHERE mission_id=? AND status IN ('held','committed') "
            "LIMIT 1", (mission_id,)).fetchone()
        if pending:
            self._replan_mission_round(connection, mission_id=mission_id,
                                       reason="partial_unload", alert_key=alert_key,
                                       actor_id=actor_id)
            effects.append({"action": "mission.replanned", "mission_id": mission_id})
        new_cargo = sum(float(item.get("kg", 0)) for item in items)
        connection.execute(
            "UPDATE dispatch_missions SET cargo_kg=?, cargo_items_json=? WHERE mission_id=?",
            (new_cargo, canonical_json(items), mission_id))
        # 拆出的货物生成后继子任务。
        child_id = f"{mission_id}-c-{uuid.uuid4().hex[:6]}"
        connection.execute(
            "INSERT INTO dispatch_missions(mission_id,parent_mission_id,kind,priority,origin_code,"
            "destination_code,passengers,cargo_kg,cargo_items_json,passenger_restriction_json,"
            "current_round,state,created_by,created_at) VALUES(?,?,?,?,"
            "(SELECT origin_code FROM dispatch_missions WHERE mission_id=?),"
            "(SELECT destination_code FROM dispatch_missions WHERE mission_id=?),0,?,?,'{}',1,"
            "'planning',?,?)",
            (child_id, mission_id, "supply", DEFAULT_PRIORITY["supply"],
             mission_id, mission_id, sum(float(i.get("kg", 0)) for i in removed),
             canonical_json(removed), actor_id, self._now()))
        self._record_alert_effect(connection, alert_key=alert_key, commitment_id=None,
                                  mission_id=mission_id, action="mission.split",
                                  detail={"child_mission_id": child_id,
                                          "offloaded_item_ids": sorted(offloaded_ids)})
        effects.append({"action": "mission.split", "mission_id": mission_id,
                        "child_mission_id": child_id})
        self._audit(connection, actor_id=actor_id, action="mission.split",
                    resource_type="dispatch_mission", resource_id=child_id,
                    detail={"parent_mission_id": mission_id, "alert_key": alert_key})
        return effects

    def _effect_medevac_preempt(self, connection, actor_id: str, alert_key: str,
                                payload: dict[str, Any]) -> list[dict[str, Any]]:
        aircraft_id = payload["aircraft_id"]
        starts_at, ends_at = payload["starts_at"], payload["ends_at"]
        medevac_mission_id = payload["medevac_mission_id"]
        medevac = self._get_mission(connection, medevac_mission_id)
        if medevac["kind"] != "medevac":
            raise ValidationError("插队任务必须是医疗撤离类型")
        effects: list[dict[str, Any]] = []
        bumped_mission_ids: list[str] = []
        # 仅挤走尚未起飞、优先级更低且时刻重叠的占用。
        bumped = connection.execute(
            "SELECT DISTINCT mission_id FROM dispatch_leases WHERE resource_type='aircraft' "
            "AND resource_key=? AND status IN ('held','committed') AND starts_at<? AND ends_at>? "
            "AND mission_id IN (SELECT mission_id FROM dispatch_missions WHERE priority<?)",
            (aircraft_id, ends_at, starts_at, medevac["priority"])).fetchall()
        for row in bumped:
            mission_id = row["mission_id"]
            self._replan_mission_round(connection, mission_id=mission_id,
                                       reason="medevac_preempt", alert_key=alert_key,
                                       actor_id=actor_id)
            bumped_mission_ids.append(mission_id)
            self._record_alert_effect(connection, alert_key=alert_key, commitment_id=None,
                                      mission_id=mission_id, action="mission.preempted",
                                      detail={"aircraft_id": aircraft_id,
                                              "medevac_mission_id": medevac_mission_id})
            effects.append({"action": "mission.preempted", "mission_id": mission_id,
                            "medevac_mission_id": medevac_mission_id})
        # 为医疗任务保留该飞机的最佳候选（仍需双方批准才封存）。
        candidates = self._build_candidates_locked(connection, medevac)
        reserved_plan = None
        round_no = medevac["current_round"]
        already = connection.execute(
            "SELECT 1 FROM dispatch_plans WHERE mission_id=? AND round=? AND status IN "
            "('reserved','sealed')", (medevac_mission_id, round_no)).fetchone()
        if already is None:
            for candidate in (c for c in candidates
                              if c.feasible and c.aircraft_id == aircraft_id):
                wanted = self._wanted_resources(connection, medevac, candidate)
                # 硬冲突仍然要避让（其它高优先级任务占用），但医疗任务按优先级越过候补次序。
                blockers = [b for b in self._resource_blockers(connection, wanted,
                                                               medevac_mission_id)
                            if b.get("held_by_mission") not in bumped_mission_ids]
                if blockers:
                    continue
                plan_id, _leg_id, _lease_ids, _expires = self._create_reserved_plan(
                    connection, actor_id=actor_id, mission=medevac, round_no=round_no,
                    candidate=candidate, reason="medevac_preempt")
                self._consume_waitlist(connection, wanted, medevac_mission_id)
                connection.execute(
                    "UPDATE dispatch_missions SET state='planning' WHERE mission_id=? "
                    "AND state='replanning'", (medevac_mission_id,))
                self._record_alert_effect(connection, alert_key=alert_key, commitment_id=None,
                                          mission_id=medevac_mission_id,
                                          action="plan.priority_reserved",
                                          detail={"plan_id": plan_id, "aircraft_id": aircraft_id})
                effects.append({"action": "plan.priority_reserved",
                                "mission_id": medevac_mission_id, "plan_id": plan_id,
                                "note": "已保留资源，仍需运行与站点双方批准"})
                reserved_plan = plan_id
                break
        # 被挤走的低优先级任务在医疗保留之后入候补，避免反向阻塞。
        for mission_id in bumped_mission_ids:
            self._join_waitlist(connection, mission_id=mission_id, plan_id=None,
                                resource_type="aircraft", resource_key=aircraft_id)
        if reserved_plan is None and not any(
                e["action"] == "plan.priority_reserved" for e in effects):
            effects.append({"action": "medevac.no_feasible_candidate",
                            "mission_id": medevac_mission_id})
        return effects

    def _build_candidates_locked(self, connection, mission: dict[str, Any]):
        distance = self._distance(connection, mission["origin_code"], mission["destination_code"])
        result = []
        if distance is None:
            return result
        for aircraft_row in connection.execute("SELECT * FROM dispatch_aircraft WHERE grounded=0"):
            aircraft = dict(aircraft_row)
            aircraft["capabilities"] = json.loads(aircraft["capabilities_json"])
            pilots = connection.execute(
                "SELECT * FROM dispatch_crew WHERE active=1 AND role IN ('pilot','co_pilot') "
                "AND qualifications_json LIKE ? ORDER BY crew_id LIMIT 1",
                (f'%"{aircraft["required_rating"]}"%',)).fetchall()
            if not pilots:
                continue
            crew_rows = []
            member = dict(pilots[0])
            member["qualifications"] = json.loads(member["qualifications_json"])
            crew_rows.append(member)
            for ow in connection.execute(
                "SELECT * FROM dispatch_airfield_windows WHERE airfield_code=?",
                    (mission["origin_code"],)).fetchall():
                for dw in connection.execute(
                    "SELECT * FROM dispatch_airfield_windows WHERE airfield_code=?",
                        (mission["destination_code"],)).fetchall():
                    candidate = self._candidate_for_window(
                        connection, mission=mission, aircraft=aircraft, crew_rows=crew_rows,
                        distance_nm=distance, origin_window=dict(ow), dest_window=dict(dw))
                    if candidate is not None:
                        result.append(candidate)
        result.sort(key=lambda c: c.rank_key())
        return result

    # ------------------------------------------------------------------
    # 查询与解释
    # ------------------------------------------------------------------

    def _get_mission(self, connection, mission_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM dispatch_missions WHERE mission_id=?",
                                 (mission_id,)).fetchone()
        if row is None:
            raise NotFoundError("任务不存在")
        return dict(row)

    def _get_plan(self, connection, plan_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM dispatch_plans WHERE plan_id=?",
                                 (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError("方案不存在")
        return dict(row)

    def _get_commitment(self, connection, commitment_id: str) -> dict[str, Any]:
        row = connection.execute("SELECT * FROM dispatch_commitments WHERE commitment_id=?",
                                 (commitment_id,)).fetchone()
        if row is None:
            raise NotFoundError("承诺不存在")
        return dict(row)

    def get_mission_explanation(self, actor_id: str, mission_id: str) -> dict[str, Any]:
        """说明任务为何获准起飞、延后或拆分，并列出每次资源变化台账。"""

        with self.database.transaction() as connection:
            self._actor(connection, actor_id)
            mission = self._get_mission(connection, mission_id)
            plans = []
            for row in connection.execute(
                "SELECT * FROM dispatch_plans WHERE mission_id=? ORDER BY round, created_at",
                    (mission_id,)).fetchall():
                plan = dict(row)
                factors = json.loads(plan["factors_json"])
                violated = [f["code"] for f in factors if f["status"] == "violation"]
                warned = [f["code"] for f in factors if f["status"] == "warning"]
                decision = self._decision_for_plan(connection, plan, violated)
                plans.append({
                    "plan_id": plan["plan_id"], "round": plan["round"], "status": plan["status"],
                    "aircraft_id": plan["aircraft_id"], "dep_at": plan["dep_at"],
                    "arr_at": plan["arr_at"], "score": plan["score"],
                    "operations_approved_by": plan["operations_approved_by"],
                    "station_approved_by": plan["station_approved_by"],
                    "sealed_at": plan["sealed_at"], "reason": plan["reason"],
                    "violating_factors": violated, "warning_factors": warned,
                    "factors": factors, "decision": decision,
                })
            commitments = []
            for row in connection.execute(
                "SELECT c.* FROM dispatch_commitments c WHERE c.mission_id=? "
                "ORDER BY c.created_at", (mission_id,)).fetchall():
                commitments.append({
                    "commitment_id": row["commitment_id"], "round": row["round"],
                    "plan_id": row["plan_id"], "state": row["state"],
                    "created_at": row["created_at"], "departed_at": row["departed_at"],
                    "handed_over_at": row["handed_over_at"], "replaced_at": row["replaced_at"],
                    "replace_reason": row["replace_reason"], "alert_key": row["alert_key"]})
            ledger = []
            for row in connection.execute(
                "SELECT * FROM dispatch_ledger WHERE mission_id=? ORDER BY seq",
                    (mission_id,)).fetchall():
                ledger.append({k: row[k] for k in (
                    "seq", "movement", "resource_type", "resource_key", "starts_at", "ends_at",
                    "quantity", "reason", "alert_key", "lease_id", "plan_id", "commitment_id",
                    "occurred_at")})
            children = [r["mission_id"] for r in connection.execute(
                "SELECT mission_id FROM dispatch_missions WHERE parent_mission_id=?",
                (mission_id,)).fetchall()]
            return {
                "mission_id": mission_id, "kind": mission["kind"],
                "priority": mission["priority"], "state": mission["state"],
                "current_round": mission["current_round"],
                "origin_code": mission["origin_code"],
                "destination_code": mission["destination_code"],
                "passengers": mission["passengers"], "cargo_kg": mission["cargo_kg"],
                "cargo_items": json.loads(mission["cargo_items_json"]),
                "passenger_restriction": json.loads(mission["passenger_restriction_json"]),
                "summary": self._summarize_state(mission, plans, ledger),
                "plans": plans, "commitments": commitments,
                "resource_ledger": ledger, "child_missions": children,
            }

    def _decision_for_plan(self, connection, plan: dict[str, Any], violated: list[str]) -> dict[str, Any]:
        if plan["status"] == "sealed":
            return {"type": "cleared", "at": plan["sealed_at"],
                    "why": "运行与站点双方批准，租约全部有效，候选无违反因子",
                    "score": plan["score"]}
        if plan["status"] == "superseded":
            return {"type": "delayed", "at": plan["sealed_at"],
                    "why": f"封存后被告警重排：{plan['reason']}"}
        if plan["status"] == "rejected":
            return {"type": "delayed", "why": f"方案被驳回：{plan['reason']}"}
        if plan["status"] == "expired":
            return {"type": "delayed", "why": "限时租约到期未获双方批准，资源已释放"}
        return {"type": "pending_approval",
                "why": "资源已以限时租约保留，等待运行与站点双方批准",
                "violating_factors": violated}

    def _summarize_state(self, mission, plans, ledger) -> str:
        if mission["state"] == "completed":
            return "任务所有航段已完成交接"
        if mission["state"] == "in_flight":
            return "任务已起飞，正在执行，未执行航段仍可被后续事件重排"
        if mission["state"] == "replanning":
            return f"第 {mission['current_round'] - 1} 轮方案受事件影响已释放，正在第 " \
                   f"{mission['current_round']} 轮重新保留资源"
        latest = plans[-1] if plans else None
        if latest and latest["status"] == "reserved":
            return "已保留资源并等待双方批准"
        if any(p["status"] == "sealed" for p in plans):
            sealed = next(p for p in plans if p["status"] == "sealed")
            return f"第 {sealed['round']} 轮方案已经运行与站点双方批准封存，承诺生效"
        splits = [e for e in ledger if e["movement"] == "mission.replan"]
        if splits:
            return "任务经历过重排，最新轮次尚未保留资源"
        return "任务尚未保留资源"

    def list_waitlist(self, actor_id: str, resource_type: str | None = None,
                      resource_key: str | None = None) -> dict[str, Any]:
        with self.database.transaction() as connection:
            self._actor(connection, actor_id)
            sql = "SELECT * FROM dispatch_waitlist"
            parameters: list[Any] = []
            clauses = []
            if resource_type:
                clauses.append("resource_type=?")
                parameters.append(resource_type)
            if resource_key:
                clauses.append("resource_key=?")
                parameters.append(resource_key)
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY resource_type, resource_key, seq"
            items = [dict(row) for row in connection.execute(sql, parameters).fetchall()]
            return {"items": items}

    def list_leases(self, actor_id: str, status: str | None = None) -> dict[str, Any]:
        with self.database.transaction() as connection:
            self._actor(connection, actor_id)
            if status:
                rows = connection.execute(
                    "SELECT * FROM dispatch_leases WHERE status=? ORDER BY acquired_at", (status,))
            else:
                rows = connection.execute(
                    "SELECT * FROM dispatch_leases ORDER BY acquired_at")
            return {"items": [dict(row) for row in rows]}
