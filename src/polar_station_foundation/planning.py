"""飞行候选方案的纯评估逻辑。

候选方案（Candidate）把机场开放期、机组资质、航程油量、载荷、备降点、
气象预报版本、旅客限制和地面保障组合成可逐项比较的结果：每个候选携带
一组 Factor（因子），因子有状态（满足/违反/警告）和贡献分，调度席可以
按分数比较，也可以逐项看到差异。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# 因子状态的严重程度，分值越低越不可执行。
STATUS_ORDER = {"violation": 0, "warning": 1, "satisfied": 2}


@dataclass(frozen=True)
class Factor:
    """候选方案比较中的一个可解释因子。"""

    code: str
    label: str
    status: str  # satisfied | warning | violation
    detail: dict[str, Any] = field(default_factory=dict)
    score: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "label": self.label, "status": self.status,
                "detail": self.detail, "score": round(self.score, 3)}


@dataclass
class Candidate:
    """一个具体的飞机/机组/窗口组合及其评估结果。"""

    aircraft_id: str
    crew_ids: tuple[str, ...]
    dep_at: str
    arr_at: str
    flight_hours: float
    fuel_required_kg: float
    payload_kg: float
    origin_window_id: str
    destination_window_id: str
    alternate_code: str | None
    forecast_versions: dict[str, int]
    factors: list[Factor] = field(default_factory=list)

    @property
    def feasible(self) -> bool:
        """没有任何违反因子时候选才可执行。"""

        return all(f.status != "violation" for f in self.factors)

    @property
    def score(self) -> float:
        return round(sum(f.score for f in self.factors), 3)

    def rank_key(self) -> tuple[int, int, float]:
        """可执行优先，警告少者优先，分数高者优先；按升序排序。"""

        violations = sum(1 for f in self.factors if f.status == "violation")
        warnings = sum(1 for f in self.factors if f.status == "warning")
        return (violations, warnings, -self.score)

    def as_dict(self) -> dict[str, Any]:
        return {
            "aircraft_id": self.aircraft_id,
            "crew_ids": list(self.crew_ids),
            "dep_at": self.dep_at,
            "arr_at": self.arr_at,
            "flight_hours": round(self.flight_hours, 3),
            "fuel_required_kg": round(self.fuel_required_kg, 1),
            "payload_kg": round(self.payload_kg, 1),
            "origin_window_id": self.origin_window_id,
            "destination_window_id": self.destination_window_id,
            "alternate_code": self.alternate_code,
            "forecast_versions": self.forecast_versions,
            "feasible": self.feasible,
            "score": self.score,
            "factors": [f.as_dict() for f in self.factors],
        }


def _factor(code: str, label: str, status: str, detail: dict[str, Any], score: float) -> Factor:
    return Factor(code, label, status, detail, score if status == "satisfied" else 0.0)


def evaluate_candidate(*, aircraft: dict[str, Any], crew_rows: list[dict[str, Any]],
                        distance_nm: float, mission: dict[str, Any],
                        dep_at: str, arr_at: str, flight_hours: float,
                        fuel_required_kg: float, payload_kg: float,
                        origin_window: dict[str, Any], dest_window: dict[str, Any],
                        origin_fx: dict[str, Any] | None, dest_fx: dict[str, Any] | None,
                        alternate: dict[str, Any] | None,
                        alternate_fx: dict[str, Any] | None,
                        origin_handling: bool, dest_handling: bool,
                        fuel_available_kg: float | None) -> Candidate:
    """对单个组合逐项检查八个约束维度并打分。

    违反（violation）使候选不可执行；警告（warning）保留候选但扣分；
    满足（satisfied）按安全裕度加分。所有判断依据都落在 Factor.detail 中。
    """

    factors: list[Factor] = []
    forecast_versions: dict[str, int] = {}
    if origin_fx is not None:
        forecast_versions[origin_window["airfield_code"]] = origin_fx["version"]
    if dest_fx is not None:
        forecast_versions[dest_window["airfield_code"]] = dest_fx["version"]
    if alternate is not None and alternate_fx is not None:
        forecast_versions[alternate["code"]] = alternate_fx["version"]

    # 1. 机场开放期：起降时刻必须落在双方窗口内，并保留闭合裕度。
    origin_margin = _minutes_between(dep_at, origin_window["closes_at"])
    dep_open_ok = origin_window["opens_at"] <= dep_at < origin_window["closes_at"]
    arr_open_ok = dest_window["opens_at"] <= arr_at < dest_window["closes_at"]
    window_status = "satisfied" if (dep_open_ok and arr_open_ok) else "violation"
    factors.append(_factor(
        "airfield_window", "机场开放期", window_status,
        {"origin_window": origin_window["window_id"], "dep_at": dep_at,
         "origin_opens": origin_window["opens_at"], "origin_closes": origin_window["closes_at"],
         "destination_window": dest_window["window_id"], "arr_at": arr_at,
         "destination_opens": dest_window["opens_at"], "destination_closes": dest_window["closes_at"],
         "minutes_before_origin_close": origin_margin},
        min(30.0, max(0.0, origin_margin) / 10.0)))

    # 2. 机组资质：机型签注、岗位完整性与执勤期。
    quals = {q for member in crew_rows for q in member["qualifications"]}
    has_rating = aircraft["required_rating"] in quals
    roles = {member["role"] for member in crew_rows}
    has_roles = {"pilot"} <= roles
    duty_ok = True
    duty_detail: list[dict[str, Any]] = []
    for member in crew_rows:
        within = True
        if member["duty_starts_at"] and member["duty_ends_at"]:
            within = member["duty_starts_at"] <= dep_at and arr_at <= member["duty_ends_at"]
        duty_ok = duty_ok and within
        duty_detail.append({"crew_id": member["crew_id"], "within_duty": within})
    crew_status = "satisfied" if (has_rating and has_roles and duty_ok) else "violation"
    factors.append(_factor(
        "crew_qualification", "机组资质与执勤期", crew_status,
        {"required_rating": aircraft["required_rating"], "rating_present": has_rating,
         "roles": sorted(roles), "crew": duty_detail},
        25.0 if crew_status == "satisfied" else 0.0))

    # 3. 航程油量：需求不得超过飞机油箱与起飞机场可用库存。
    fuel_cap_ok = fuel_required_kg <= aircraft["fuel_capacity_kg"]
    stock_ok = fuel_available_kg is None or fuel_required_kg <= fuel_available_kg
    fuel_status = "satisfied" if (fuel_cap_ok and stock_ok) else "violation"
    reserve_ratio = (aircraft["fuel_capacity_kg"] - fuel_required_kg) / aircraft["fuel_capacity_kg"]
    factors.append(_factor(
        "fuel_range", "航程油量", fuel_status,
        {"distance_nm": distance_nm, "flight_hours": round(flight_hours, 3),
         "fuel_required_kg": round(fuel_required_kg, 1),
         "fuel_capacity_kg": aircraft["fuel_capacity_kg"],
         "capacity_ok": fuel_cap_ok, "fuel_available_kg": fuel_available_kg,
         "stock_ok": stock_ok, "reserve_ratio": round(reserve_ratio, 3)},
        5.0 + 15.0 * max(0.0, reserve_ratio)))

    # 4. 载荷：旅客座位与货物重量（旅客按 90kg 折算）。
    pax = mission["passengers"]
    seat_ok = pax <= aircraft["seat_capacity"]
    payload_ok = payload_kg <= aircraft["payload_capacity_kg"]
    payload_status = "satisfied" if (seat_ok and payload_ok) else "violation"
    payload_margin = (aircraft["payload_capacity_kg"] - payload_kg) / aircraft["payload_capacity_kg"]
    factors.append(_factor(
        "payload", "座位与载荷", payload_status,
        {"passengers": pax, "seat_capacity": aircraft["seat_capacity"], "seat_ok": seat_ok,
         "payload_kg": round(payload_kg, 1), "payload_capacity_kg": aircraft["payload_capacity_kg"],
         "payload_ok": payload_ok, "margin_ratio": round(payload_margin, 3)},
        5.0 + 10.0 * max(0.0, payload_margin)))

    # 5. 备降点：需存在、能保障本机型（地面/气象），且航程可达。
    if alternate is None:
        alt_status = "warning"
        alt_detail = {"alternate": None, "reason": "未指定备降点"}
    else:
        alt_reachable = (flight_hours * aircraft["burn_kg_per_hour"] * 2
                         <= aircraft["fuel_capacity_kg"])
        alt_handling = bool(alternate["has_ground_handling"])
        alt_wx = _wx_ok(alternate_fx, aircraft)
        alt_status = "satisfied" if (alt_reachable and alt_handling and alt_wx[0]) else (
            "warning" if alt_handling else "violation")
        alt_detail = {"alternate": alternate["code"], "reachable_with_reserve": alt_reachable,
                      "ground_handling": alt_handling, "weather": alt_wx[1]}
    factors.append(_factor("alternate", "备降点", alt_status, alt_detail,
                           12.0 if alt_status == "satisfied" else 0.0))

    # 6. 气象预报版本：使用两场均未失效的最新版本；旧版本只给警告。
    wx_origin = _wx_ok(origin_fx, aircraft)
    wx_dest = _wx_ok(dest_fx, aircraft)
    wx_current = bool(origin_fx and not origin_fx["superseded"]
                      and dest_fx and not dest_fx["superseded"])
    if wx_origin[0] and wx_dest[0]:
        wx_status = "satisfied" if wx_current else "warning"
    else:
        wx_status = "violation"
    factors.append(_factor(
        "weather_forecast", "气象预报版本", wx_status,
        {"origin": wx_origin[1], "destination": wx_dest[1], "current_version": wx_current},
        12.0 if wx_status == "satisfied" else 0.0))

    # 7. 旅客限制：医疗任务需目的地医疗能力；旅客限制标签需飞机能力覆盖。
    restriction = mission.get("passenger_restriction") or {}
    needs_medevac = mission["kind"] == "medevac" or restriction.get("medical_attendant")
    medevac_ok = (not needs_medevac) or bool(
        (alternate is not None and alternate["medevac_capable"])
        or _airfield_row_flag(dest_window, "medevac_capable"))
    required_caps = set(restriction.get("required_capabilities", []))
    caps_ok = required_caps <= set(aircraft["capabilities"])
    pax_status = "satisfied" if (medevac_ok and caps_ok) else "violation"
    factors.append(_factor(
        "passenger_restriction", "旅客限制", pax_status,
        {"needs_medevac": bool(needs_medevac), "medevac_ok": medevac_ok,
         "required_capabilities": sorted(required_caps),
         "aircraft_capabilities": aircraft["capabilities"], "capabilities_ok": caps_ok},
        10.0 if pax_status == "satisfied" else 0.0))

    # 8. 地面保障：起降两场均需地面装卸与加油保障。
    ground_status = "satisfied" if (origin_handling and dest_handling) else "violation"
    factors.append(_factor(
        "ground_handling", "地面保障", ground_status,
        {"origin_handling": origin_handling, "destination_handling": dest_handling},
        10.0 if ground_status == "satisfied" else 0.0))

    # 飞机故障（适航）作为最高优先否决项。
    if aircraft.get("grounded"):
        factors.append(_factor(
            "airworthiness", "飞机适航", "violation",
            {"grounded": True}, 0.0))

    return Candidate(
        aircraft_id=aircraft["aircraft_id"],
        crew_ids=tuple(m["crew_id"] for m in crew_rows),
        dep_at=dep_at, arr_at=arr_at, flight_hours=flight_hours,
        fuel_required_kg=fuel_required_kg, payload_kg=payload_kg,
        origin_window_id=origin_window["window_id"],
        destination_window_id=dest_window["window_id"],
        alternate_code=alternate["code"] if alternate else None,
        forecast_versions=forecast_versions, factors=factors)


def _wx_ok(forecast: dict[str, Any] | None, aircraft: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    """判断预报是否满足本机型最低条件。"""

    if forecast is None:
        return False, {"available": False}
    ceiling_ok = forecast["ceiling_ft"] is None or forecast["ceiling_ft"] >= 800
    visibility_ok = forecast["visibility_km"] is None or forecast["visibility_km"] >= 3.0
    crosswind_ok = forecast["crosswind_kt"] is None or forecast["crosswind_kt"] <= aircraft["crosswind_limit_kt"]
    detail = {"available": True, "version": forecast["version"], "superseded": bool(forecast["superseded"]),
              "ceiling_ft": forecast["ceiling_ft"], "visibility_km": forecast["visibility_km"],
              "crosswind_kt": forecast["crosswind_kt"],
              "ceiling_ok": ceiling_ok, "visibility_ok": visibility_ok, "crosswind_ok": crosswind_ok}
    return ceiling_ok and visibility_ok and crosswind_ok, detail


def _airfield_row_flag(window: dict[str, Any], flag: str) -> bool:
    return bool(window.get(flag))


def _minutes_between(start: str, end: str) -> int:
    from datetime import datetime

    return int((datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() // 60)
