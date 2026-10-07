"""评估飞行约束并生成可比较的候选方案。

本模块只做纯计算：服务层把数据库行转换成字典后传入，
这里负责逐项评估机场开放期、机组资质、航程油量、载荷、备降点、
气象预报版本、旅客限制、地面保障和资源可用性，
输出可排序比较的候选方案。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


def parse_time(value: str) -> datetime:
    """把 ISO 时间文本解析为秒级精度的 UTC 时间。"""

    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def iso(value: datetime) -> str:
    """输出秒级精度的 UTC ISO 文本，保证字典序与时间序一致。"""

    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


EXCLUSIVE_RESOURCES = ("aircraft", "crew")
POOLED_RESOURCES = ("window_movement", "ground_support")
RESOURCE_TYPES = EXCLUSIVE_RESOURCES + POOLED_RESOURCES

CONSTRAINT_NAMES = (
    "window_containment",
    "crew_qualification",
    "fuel_range",
    "payload_capacity",
    "passenger_limits",
    "weather_minima",
    "alternate_coverage",
    "ground_support",
    "resource_availability",
)

TURNAROUND = timedelta(hours=1)
MAX_SHIFT_ATTEMPTS = 6
MAX_SPLIT_LEGS = 3


class ReservationBook:
    """汇总有效租约、已承诺占用与临时占用，用于冲突检测。"""

    def __init__(self, capacities: dict[tuple[str, str], int] | None = None) -> None:
        self._rows: list[dict[str, Any]] = []
        self._capacities = dict(capacities or {})

    def add(self, row: dict[str, Any]) -> None:
        self._rows.append(dict(row))

    def extend(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            self.add(row)

    def fork(self) -> "ReservationBook":
        """复制当前占用，用于在候选内部叠加临时占用。"""

        clone = ReservationBook(self._capacities)
        clone.extend(self._rows)
        return clone

    def _overlapping(self, resource_type: str, resource_id: str,
                     start: datetime, end: datetime) -> list[dict[str, Any]]:
        return [row for row in self._rows
                if row["resource_type"] == resource_type and row["resource_id"] == resource_id
                and parse_time(row["slot_start"]) < end and start < parse_time(row["slot_end"])]

    def conflicts(self, *, resource_type: str, resource_id: str, slot_start: str,
                  slot_end: str, quantity: int = 1) -> list[dict[str, Any]]:
        """返回阻止本次占用的既有占用；空列表表示可以占用。"""

        start, end = parse_time(slot_start), parse_time(slot_end)
        overlapping = self._overlapping(resource_type, resource_id, start, end)
        if resource_type in EXCLUSIVE_RESOURCES:
            return overlapping
        capacity = self._capacities.get((resource_type, resource_id), 1)
        used = sum(int(row.get("quantity", 1)) for row in overlapping)
        return overlapping if used + quantity > capacity else []


@dataclass(frozen=True)
class LegSpec:
    """描述候选方案中一个航段的全部资源绑定。"""

    leg_sequence: int
    aircraft_id: str
    captain_id: str
    copilot_id: str
    departure_site_id: str
    arrival_site_id: str
    departure_window_id: str
    arrival_window_id: str
    forecast_id: str | None
    alternate_site_id: str | None
    planned_departure: str
    planned_arrival: str
    payload_kg: float
    passengers: int
    fuel_required_kg: float
    support_bindings: tuple[dict[str, str], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "leg_sequence": self.leg_sequence,
            "aircraft_id": self.aircraft_id,
            "captain_id": self.captain_id,
            "copilot_id": self.copilot_id,
            "departure_site_id": self.departure_site_id,
            "arrival_site_id": self.arrival_site_id,
            "departure_window_id": self.departure_window_id,
            "arrival_window_id": self.arrival_window_id,
            "forecast_id": self.forecast_id,
            "alternate_site_id": self.alternate_site_id,
            "planned_departure": self.planned_departure,
            "planned_arrival": self.planned_arrival,
            "payload_kg": self.payload_kg,
            "passengers": self.passengers,
            "fuel_required_kg": self.fuel_required_kg,
            "support_bindings": [dict(binding) for binding in self.support_bindings],
        }


@dataclass(frozen=True)
class Candidate:
    """一组可比较的候选航段及其约束评估结果。"""

    legs: tuple[LegSpec, ...]
    constraints: tuple[dict[str, Any], ...]
    feasible: bool

    def to_dict(self) -> dict[str, Any]:
        total_fuel = round(sum(leg.fuel_required_kg for leg in self.legs), 3)
        return {
            "feasible": self.feasible,
            "legs": [leg.to_dict() for leg in self.legs],
            "constraints": [dict(item) for item in self.constraints],
            "score": {
                "first_departure": self.legs[0].planned_departure,
                "total_fuel_kg": total_fuel,
                "leg_count": len(self.legs),
            },
        }


def _report(name: str, satisfied: bool, detail: str, leg_sequence: int) -> dict[str, Any]:
    return {"name": name, "satisfied": satisfied, "detail": detail, "leg_sequence": leg_sequence}


def leg_resource_requirements(spec: LegSpec, *, windows_by_id: dict[str, dict[str, Any]],
                              pools_by_id: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """列出一个航段需要占用的全部关键资源。"""

    departure_window = windows_by_id[spec.departure_window_id]
    arrival_window = windows_by_id[spec.arrival_window_id]
    requirements = [
        {"resource_type": "aircraft", "resource_id": spec.aircraft_id,
         "slot_start": spec.planned_departure, "slot_end": spec.planned_arrival, "quantity": 1},
        {"resource_type": "crew", "resource_id": spec.captain_id,
         "slot_start": spec.planned_departure, "slot_end": spec.planned_arrival, "quantity": 1},
        {"resource_type": "crew", "resource_id": spec.copilot_id,
         "slot_start": spec.planned_departure, "slot_end": spec.planned_arrival, "quantity": 1},
        {"resource_type": "window_movement", "resource_id": departure_window["window_id"],
         "slot_start": departure_window["opens_at"], "slot_end": departure_window["closes_at"], "quantity": 1},
        {"resource_type": "window_movement", "resource_id": arrival_window["window_id"],
         "slot_start": arrival_window["opens_at"], "slot_end": arrival_window["closes_at"], "quantity": 1},
    ]
    for binding in spec.support_bindings:
        pool = pools_by_id[binding["support_id"]]
        moment = parse_time(spec.planned_departure if binding["site_id"] == spec.departure_site_id
                            else spec.planned_arrival)
        requirements.append({"resource_type": "ground_support", "resource_id": pool["support_id"],
                             "slot_start": iso(moment),
                             "slot_end": iso(moment + TURNAROUND),
                             "quantity": 1})
    return requirements


def evaluate_leg_constraints(*, mission: dict[str, Any], aircraft: dict[str, Any],
                             captain: dict[str, Any], copilot: dict[str, Any],
                             departure_window: dict[str, Any], arrival_window: dict[str, Any],
                             forecast: dict[str, Any] | None, alternate_window: dict[str, Any] | None,
                             pools_by_id: dict[str, dict[str, Any]], spec: LegSpec,
                             book: ReservationBook) -> tuple[dict[str, Any], ...]:
    """逐项评估一个航段的九类约束。"""

    seq = spec.leg_sequence
    reports: list[dict[str, Any]] = []
    departure = parse_time(spec.planned_departure)
    arrival = parse_time(spec.planned_arrival)

    dep_ok = parse_time(departure_window["opens_at"]) <= departure <= parse_time(departure_window["closes_at"])
    arr_ok = parse_time(arrival_window["opens_at"]) <= arrival <= parse_time(arrival_window["closes_at"])
    early_ok = departure >= parse_time(mission["earliest_departure"])
    late_ok = arrival <= parse_time(mission["latest_arrival"])
    reports.append(_report(
        "window_containment", dep_ok and arr_ok and early_ok and late_ok,
        f"计划 {spec.planned_departure} 起飞 / {spec.planned_arrival} 到达；"
        f"始发窗口 {departure_window['opens_at']}~{departure_window['closes_at']}，"
        f"到达窗口 {arrival_window['opens_at']}~{arrival_window['closes_at']}，"
        f"任务期限 {mission['earliest_departure']}~{mission['latest_arrival']}", seq))

    duration_hours = (arrival - departure).total_seconds() / 3600.0
    captain_ok = aircraft["aircraft_type"] in captain["ratings"]
    copilot_ok = aircraft["aircraft_type"] in copilot["ratings"]
    duty_ok = (duration_hours <= float(captain["max_duty_hours"])
               and duration_hours <= float(copilot["max_duty_hours"]))
    reports.append(_report(
        "crew_qualification", captain_ok and copilot_ok and duty_ok,
        f"机长资质 {'符合' if captain_ok else '不符合'}、副驾资质 {'符合' if copilot_ok else '不符合'}，"
        f"航段历时 {duration_hours:.1f} 小时，值勤上限 "
        f"{captain['max_duty_hours']}/{copilot['max_duty_hours']} 小时", seq))

    fuel_ok = spec.fuel_required_kg <= float(aircraft["fuel_capacity_kg"]) + 1e-9
    reports.append(_report(
        "fuel_range", fuel_ok,
        f"航程加备降需油 {spec.fuel_required_kg:.0f} kg，机载燃油上限 {aircraft['fuel_capacity_kg']:.0f} kg", seq))

    payload_ok = spec.payload_kg <= float(aircraft["max_payload_kg"]) + 1e-9
    reports.append(_report(
        "payload_capacity", payload_ok,
        f"航段载荷 {spec.payload_kg:.0f} kg，飞机运力上限 {aircraft['max_payload_kg']:.0f} kg", seq))

    missing = [item for item in mission["passenger_restrictions"] if item not in aircraft["capabilities"]]
    seats_ok = spec.passengers <= int(aircraft["max_passengers"])
    reports.append(_report(
        "passenger_limits", seats_ok and not missing,
        f"旅客 {spec.passengers} 人 / 座位 {aircraft['max_passengers']}；"
        f"限制条件 {mission['passenger_restrictions'] or '无'}，"
        f"飞机能力 {aircraft['capabilities'] or '无'}", seq))

    if forecast is None:
        reports.append(_report("weather_minima", False, "没有覆盖到达时刻的现行气象预报版本", seq))
    else:
        ceiling_ok = float(forecast["ceiling_m"]) >= float(aircraft["min_ceiling_m"])
        visibility_ok = float(forecast["visibility_m"]) >= float(aircraft["min_visibility_m"])
        wind_ok = float(forecast["wind_kt"]) <= float(aircraft["max_wind_kt"])
        reports.append(_report(
            "weather_minima", ceiling_ok and visibility_ok and wind_ok,
            f"预报版本 v{forecast['version']}（{forecast['valid_from']}~{forecast['valid_to']}）："
            f"云高 {forecast['ceiling_m']:.0f} m / 能见度 {forecast['visibility_m']:.0f} m / 风 {forecast['wind_kt']:.0f} kt；"
            f"机型标准 云高≥{aircraft['min_ceiling_m']:.0f} m / 能见度≥{aircraft['min_visibility_m']:.0f} m / "
            f"风≤{aircraft['max_wind_kt']:.0f} kt", seq))

    if mission.get("alternate_site_id"):
        alt_ok = (alternate_window is not None
                  and parse_time(alternate_window["opens_at"]) <= arrival <= parse_time(alternate_window["closes_at"]))
        detail = (f"备降点 {mission['alternate_site_id']} 窗口 "
                  f"{alternate_window['opens_at']}~{alternate_window['closes_at']}"
                  if alternate_window is not None else f"备降点 {mission['alternate_site_id']} 没有可用开放窗口")
        reports.append(_report("alternate_coverage", alt_ok, detail, seq))
    else:
        reports.append(_report("alternate_coverage", True, "任务不要求备降点", seq))

    problems: list[str] = []
    for site_id, moment, label in ((spec.departure_site_id, departure, "始发"),
                                   (spec.arrival_site_id, arrival, "到达")):
        for support_type in mission["required_support"]:
            binding = next((item for item in spec.support_bindings
                            if item["site_id"] == site_id and item["support_type"] == support_type), None)
            pool = pools_by_id.get(binding["support_id"]) if binding else None
            if pool is None:
                problems.append(f"{label}机场缺少 {support_type} 保障")
            elif not parse_time(pool["available_from"]) <= moment <= parse_time(pool["available_to"]):
                problems.append(f"{label}机场 {support_type} 保障时段不覆盖计划时刻")
    reports.append(_report("ground_support", not problems,
                           "；".join(problems) if problems else "两端地面保障类型与时段齐备", seq))

    blockers: list[str] = []
    windows_by_id = {departure_window["window_id"]: departure_window,
                     arrival_window["window_id"]: arrival_window}
    for requirement in leg_resource_requirements(spec, windows_by_id=windows_by_id, pools_by_id=pools_by_id):
        for hit in book.conflicts(**requirement):
            blockers.append(f"{requirement['resource_type']}:{requirement['resource_id']} "
                            f"与 {hit.get('holder', '既有占用')} 冲突")
    reports.append(_report("resource_availability", not blockers,
                           "；".join(blockers) if blockers else "关键资源在计划时段均可占用", seq))
    return tuple(reports)


def _covering(windows: list[dict[str, Any]], moment: datetime) -> dict[str, Any] | None:
    for window in windows:
        if parse_time(window["opens_at"]) <= moment <= parse_time(window["closes_at"]):
            return window
    return None


def _bind_supports(*, mission: dict[str, Any], departure: datetime, arrival: datetime,
                   pools_by_id: dict[str, dict[str, Any]], book: ReservationBook,
                   check_availability: bool) -> tuple[dict[str, str], ...]:
    """为航段两端选择每类所需地面保障的资源池。"""

    bindings: list[dict[str, str]] = []
    for site_id, moment in ((mission["origin_site_id"], departure),
                            (mission["destination_site_id"], arrival)):
        for support_type in mission["required_support"]:
            pools = sorted(
                (pool for pool in pools_by_id.values()
                 if pool["site_id"] == site_id and pool["support_type"] == support_type
                 and parse_time(pool["available_from"]) <= moment <= parse_time(pool["available_to"])),
                key=lambda pool: (pool["available_from"], pool["support_id"]))
            chosen = None
            for pool in pools:
                if check_availability and book.conflicts(
                        resource_type="ground_support", resource_id=pool["support_id"],
                        slot_start=iso(moment), slot_end=iso(moment + TURNAROUND)):
                    continue
                chosen = pool
                break
            if chosen is None and pools:
                chosen = pools[0]
            if chosen is not None:
                bindings.append({"support_id": chosen["support_id"], "site_id": site_id,
                                 "support_type": support_type})
    return tuple(bindings)


def _blocker_shift(book: ReservationBook, spec: LegSpec, departure_window: dict[str, Any],
                   arrival_window: dict[str, Any],
                   pools_by_id: dict[str, dict[str, Any]]) -> datetime | None:
    """根据排他资源的冲突占用计算可顺延到的最早时刻。"""

    windows_by_id = {departure_window["window_id"]: departure_window,
                     arrival_window["window_id"]: arrival_window}
    ends: list[datetime] = []
    for requirement in leg_resource_requirements(spec, windows_by_id=windows_by_id, pools_by_id=pools_by_id):
        hits = book.conflicts(**requirement)
        if not hits:
            continue
        if requirement["resource_type"] == "window_movement":
            return None
        ends.extend(parse_time(hit["slot_end"]) for hit in hits)
    return max(ends) if ends else None


def _attempt_leg(*, leg_sequence: int, mission: dict[str, Any], aircraft: dict[str, Any],
                 captain: dict[str, Any], copilot: dict[str, Any], dep_window: dict[str, Any],
                 dest_windows: list[dict[str, Any]], alt_windows: list[dict[str, Any]],
                 current_forecasts: list[dict[str, Any]], pools_by_id: dict[str, dict[str, Any]],
                 departure: datetime, flight_hours: float, fuel_required: float,
                 payload_kg: float, passengers: int, book: ReservationBook,
                 check_availability: bool, latest: datetime
                 ) -> tuple[LegSpec | None, tuple[dict[str, Any], ...], dict[str, Any]]:
    """构建单个航段并评估约束；资源冲突或气象不满足时顺延起飞时刻重试。"""

    reports: tuple[dict[str, Any], ...] = ()
    spec: LegSpec | None = None
    arr_window: dict[str, Any] | None = None
    for _ in range(MAX_SHIFT_ATTEMPTS):
        arrival = departure + timedelta(hours=flight_hours)
        arr_window = _covering(dest_windows, arrival)
        if arr_window is None:
            return None, (_report("window_containment", False,
                                  f"到达时刻 {iso(arrival)} 不在任何开放窗口内", leg_sequence),), {}
        forecast = next((item for item in current_forecasts
                         if parse_time(item["valid_from"]) <= arrival <= parse_time(item["valid_to"])), None)
        alternate_window = _covering(alt_windows, arrival) if alt_windows else None
        bindings = _bind_supports(mission=mission, departure=departure, arrival=arrival,
                                  pools_by_id=pools_by_id, book=book,
                                  check_availability=check_availability)
        spec = LegSpec(
            leg_sequence=leg_sequence, aircraft_id=aircraft["aircraft_id"],
            captain_id=captain["crew_id"], copilot_id=copilot["crew_id"],
            departure_site_id=mission["origin_site_id"], arrival_site_id=mission["destination_site_id"],
            departure_window_id=dep_window["window_id"], arrival_window_id=arr_window["window_id"],
            forecast_id=forecast["forecast_id"] if forecast else None,
            alternate_site_id=mission.get("alternate_site_id"),
            planned_departure=iso(departure), planned_arrival=iso(arrival),
            payload_kg=payload_kg, passengers=passengers,
            fuel_required_kg=round(fuel_required, 3), support_bindings=bindings)
        eval_book = book if check_availability else ReservationBook()
        reports = evaluate_leg_constraints(
            mission=mission, aircraft=aircraft, captain=captain, copilot=copilot,
            departure_window=dep_window, arrival_window=arr_window, forecast=forecast,
            alternate_window=alternate_window, pools_by_id=pools_by_id, spec=spec, book=eval_book)
        context = {"departure_window": dep_window, "arrival_window": arr_window}
        if all(item["satisfied"] for item in reports) or not check_availability:
            return spec, reports, context
        shifts: list[datetime] = []
        availability = next(item for item in reports if item["name"] == "resource_availability")
        if not availability["satisfied"]:
            shift_to = _blocker_shift(book, spec, dep_window, arr_window, pools_by_id)
            if shift_to is not None:
                shifts.append(shift_to)
        weather = next(item for item in reports if item["name"] == "weather_minima")
        if not weather["satisfied"]:
            if forecast is not None:
                shifts.append(parse_time(forecast["valid_to"]) + timedelta(seconds=1)
                              - timedelta(hours=flight_hours))
            else:
                later = [item for item in current_forecasts if parse_time(item["valid_from"]) > arrival]
                if later:
                    shifts.append(min(parse_time(item["valid_from"]) for item in later)
                                  - timedelta(hours=flight_hours))
        shifts = [moment for moment in shifts if moment > departure]
        if not shifts:
            return spec, reports, context
        departure = min(shifts)
        if (departure > parse_time(dep_window["closes_at"])
                or departure + timedelta(hours=flight_hours) > latest):
            return spec, reports, context
    return spec, reports, {"departure_window": dep_window,
                           "arrival_window": arr_window} if arr_window else {}


def _site_windows(windows: list[dict[str, Any]], site_id: str | None) -> list[dict[str, Any]]:
    if not site_id:
        return []
    return sorted((window for window in windows
                   if window["site_id"] == site_id and window["status"] == "open"),
                  key=lambda window: (window["opens_at"], window["window_id"]))


def generate_candidates(*, mission: dict[str, Any], aircrafts: list[dict[str, Any]],
                        crews: list[dict[str, Any]], windows: list[dict[str, Any]],
                        forecasts: list[dict[str, Any]], supports: list[dict[str, Any]],
                        book: ReservationBook, now: str,
                        check_availability: bool = True) -> tuple[list[Candidate], list[str]]:
    """为任务生成按可执行性排序的候选方案列表。"""

    notes: list[str] = []
    origin_windows = _site_windows(windows, mission["origin_site_id"])
    dest_windows = _site_windows(windows, mission["destination_site_id"])
    alt_windows = _site_windows(windows, mission.get("alternate_site_id"))
    if not origin_windows:
        notes.append("始发机场没有开放的飞行窗口")
    if not dest_windows:
        notes.append("到达机场没有开放的飞行窗口")
    current_forecasts = sorted(
        (item for item in forecasts
         if item["site_id"] == mission["destination_site_id"] and item["status"] == "current"),
        key=lambda item: int(item["version"]), reverse=True)
    if not current_forecasts:
        notes.append("到达机场没有现行气象预报版本")
    pools_by_id = {pool["support_id"]: pool for pool in supports}
    captains = sorted((crew for crew in crews if crew["crew_role"] == "captain" and crew["active"]),
                      key=lambda crew: crew["crew_id"])
    copilots = sorted((crew for crew in crews if crew["crew_role"] == "copilot" and crew["active"]),
                      key=lambda crew: crew["crew_id"])
    if not captains or not copilots:
        notes.append("缺少在役的机长或副驾驶")
    earliest = parse_time(mission["earliest_departure"])
    latest = parse_time(mission["latest_arrival"])
    now_dt = parse_time(now)
    serviceable = [aircraft for aircraft in sorted(aircrafts, key=lambda item: item["registration"])
                   if aircraft["status"] == "serviceable"]
    if not serviceable:
        notes.append("没有处于可用状态的飞机")

    candidates: list[Candidate] = []
    for aircraft in serviceable:
        flight_hours = float(mission["distance_nm"]) / float(aircraft["cruise_speed_kt"])
        fuel_required = ((float(mission["distance_nm"]) + float(mission.get("alternate_distance_nm") or 0.0))
                         * float(aircraft["burn_kg_per_nm"]) + float(aircraft["reserve_fuel_kg"]))
        for window in origin_windows:
            best: Candidate | None = None
            for captain in captains:
                for copilot in copilots:
                    spec, reports, _ = _attempt_leg(
                        leg_sequence=1, mission=mission, aircraft=aircraft, captain=captain,
                        copilot=copilot, dep_window=window, dest_windows=dest_windows,
                        alt_windows=alt_windows, current_forecasts=current_forecasts,
                        pools_by_id=pools_by_id,
                        departure=max(parse_time(window["opens_at"]), earliest, now_dt),
                        flight_hours=flight_hours, fuel_required=fuel_required,
                        payload_kg=float(mission["payload_kg"]), passengers=int(mission["passengers"]),
                        book=book, check_availability=check_availability, latest=latest)
                    if spec is None:
                        continue
                    candidate = Candidate(legs=(spec,), constraints=tuple(reports),
                                          feasible=all(item["satisfied"] for item in reports))
                    if best is None:
                        best = candidate
                    if candidate.feasible:
                        best = candidate
                        break
                if best is not None and best.feasible:
                    break
            if best is not None:
                candidates.append(best)

    if mission.get("allow_split") and serviceable and origin_windows:
        payload = float(mission["payload_kg"])
        for aircraft in serviceable:
            capacity = float(aircraft["max_payload_kg"])
            if payload <= capacity:
                continue
            legs_needed = math.ceil(payload / capacity)
            if legs_needed > MAX_SPLIT_LEGS or legs_needed > len(origin_windows):
                notes.append(f"飞机 {aircraft['registration']} 需要拆分 {legs_needed} 段，"
                             f"超出可用窗口数量")
                continue
            flight_hours = float(mission["distance_nm"]) / float(aircraft["cruise_speed_kt"])
            fuel_required = ((float(mission["distance_nm"]) + float(mission.get("alternate_distance_nm") or 0.0))
                             * float(aircraft["burn_kg_per_nm"]) + float(aircraft["reserve_fuel_kg"]))
            for start_index in range(0, len(origin_windows) - legs_needed + 1):
                best_split: Candidate | None = None
                for captain in captains:
                    for copilot in copilots:
                        chain_book = book.fork()
                        specs: list[LegSpec] = []
                        reports_all: list[dict[str, Any]] = []
                        remaining = payload
                        previous_arrival: datetime | None = None
                        chain_ok = True
                        for index in range(legs_needed):
                            window = origin_windows[start_index + index]
                            leg_payload = min(capacity, remaining)
                            departure = max(parse_time(window["opens_at"]), earliest, now_dt)
                            if previous_arrival is not None:
                                departure = max(departure, previous_arrival + TURNAROUND)
                            spec, reports, context = _attempt_leg(
                                leg_sequence=index + 1, mission=mission, aircraft=aircraft,
                                captain=captain, copilot=copilot, dep_window=window,
                                dest_windows=dest_windows, alt_windows=alt_windows,
                                current_forecasts=current_forecasts, pools_by_id=pools_by_id,
                                departure=departure, flight_hours=flight_hours,
                                fuel_required=fuel_required, payload_kg=leg_payload,
                                passengers=int(mission["passengers"]) if index == 0 else 0,
                                book=chain_book, check_availability=check_availability, latest=latest)
                            if spec is None:
                                chain_ok = False
                                break
                            specs.append(spec)
                            reports_all.extend(reports)
                            remaining -= leg_payload
                            previous_arrival = parse_time(spec.planned_arrival)
                            if check_availability and context:
                                windows_by_id = {context["departure_window"]["window_id"]: context["departure_window"],
                                                 context["arrival_window"]["window_id"]: context["arrival_window"]}
                                for requirement in leg_resource_requirements(
                                        spec, windows_by_id=windows_by_id, pools_by_id=pools_by_id):
                                    chain_book.add({**requirement, "holder": "同一拆分方案"})
                        candidate = Candidate(
                            legs=tuple(specs), constraints=tuple(reports_all),
                            feasible=chain_ok and bool(specs)
                            and all(item["satisfied"] for item in reports_all))
                        if best_split is None:
                            best_split = candidate
                        if candidate.feasible:
                            best_split = candidate
                            break
                    if best_split is not None and best_split.feasible:
                        break
                if best_split is not None:
                    candidates.append(best_split)

    def sort_key(candidate: Candidate) -> tuple:
        violations = sum(1 for item in candidate.constraints if not item["satisfied"])
        return (0 if candidate.feasible else 1, violations, candidate.legs[0].planned_departure,
                round(sum(leg.fuel_required_kg for leg in candidate.legs), 3),
                candidate.legs[0].aircraft_id, len(candidate.legs))

    candidates.sort(key=sort_key)
    return candidates, notes
