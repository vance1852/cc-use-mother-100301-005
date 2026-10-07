"""飞行窗口与任务承诺服务的离线端到端验收。

完整走一遍调度席故事：登记资源、比较候选方案、租约保留、
双方批准封存、封存冲突、起飞、气象修订重排、重复告警幂等、
解释查询以及进程恢复后的状态延续。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from polar_station_foundation.clock import ManualClock
from polar_station_foundation.errors import ConflictError
from polar_station_foundation.storage import Database

from .service import FlightOpsService

T0 = datetime(2026, 10, 7, 8, 0, tzinfo=timezone.utc)


def _ts(day: int, hour: int, minute: int = 0) -> str:
    base = datetime(2026, 10, 6 + day, hour, minute, tzinfo=timezone.utc)
    return base.isoformat().replace("+00:00", "Z")


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(f"验收失败：{message}")


def _bootstrap(service: FlightOpsService) -> None:
    service.register_organization(request_id="req-org", actor_id="bootstrap",
                                  organization_id="org-001", name="极地科考机构")
    service.register_actor(request_id="req-admin", actor_id="bootstrap", new_actor_id="admin-001",
                           display_name="系统管理员", role="admin", organization_id="org-001")
    service.register_actor(request_id="req-disp-a", actor_id="admin-001", new_actor_id="disp-a",
                           display_name="调度员甲", role="dispatcher", organization_id="org-001")
    service.register_actor(request_id="req-disp-b", actor_id="admin-001", new_actor_id="disp-b",
                           display_name="调度员乙", role="dispatcher", organization_id="org-001")
    service.register_actor(request_id="req-ops", actor_id="admin-001", new_actor_id="ops-001",
                           display_name="运行批准人", role="ops_approver", organization_id="org-001")
    service.register_actor(request_id="req-stn", actor_id="admin-001", new_actor_id="stn-001",
                           display_name="站点批准人", role="station_approver", organization_id="org-001")
    for site_id, name in (("main", "主站机场"), ("camp", "内陆营地"), ("alt", "备降点")):
        service.register_site(request_id=f"req-site-{site_id}", actor_id="admin-001",
                              site_id=site_id, organization_id="org-001", name=name,
                              timezone_name="Antarctica/Casey")
    service.register_aircraft(request_id="req-ac1", actor_id="disp-a", aircraft_id="AC1",
                              registration="B-7001", aircraft_type="BT67", cruise_speed_kt=180,
                              fuel_capacity_kg=3000, burn_kg_per_nm=6, reserve_fuel_kg=500,
                              max_payload_kg=2000, max_passengers=12,
                              capabilities=["stretcher", "oxygen"], min_ceiling_m=100,
                              min_visibility_m=3000, max_wind_kt=25, home_site_id="main")
    service.register_crew(request_id="req-cap", actor_id="disp-a", crew_id="CAP1",
                          display_name="机长一", crew_role="captain", ratings=["BT67"],
                          max_duty_hours=10, base_site_id="main")
    service.register_crew(request_id="req-co", actor_id="disp-a", crew_id="CO1",
                          display_name="副驾一", crew_role="copilot", ratings=["BT67"],
                          max_duty_hours=10, base_site_id="main")
    for window_id, site_id, opens, closes, capacity in (
            ("W-MAIN-1", "main", _ts(1, 9), _ts(1, 13), 4),
            ("W-CAMP-1", "camp", _ts(1, 10), _ts(1, 14), 4),
            ("W-ALT-1", "alt", _ts(1, 8), _ts(1, 20), 2),
            ("W-MAIN-2", "main", _ts(2, 9), _ts(2, 13), 4),
            ("W-CAMP-2", "camp", _ts(2, 10), _ts(2, 14), 4),
            ("W-ALT-2", "alt", _ts(2, 8), _ts(2, 20), 2)):
        service.register_window(request_id=f"req-{window_id}", actor_id="disp-a",
                                window_id=window_id, site_id=site_id, opens_at=opens,
                                closes_at=closes, movement_capacity=capacity)
    service.register_weather_forecast(request_id="req-f1", actor_id="disp-a", forecast_id="F1",
                                      site_id="camp", valid_from=_ts(1, 6), valid_to=_ts(3, 6),
                                      ceiling_m=300, visibility_m=8000, wind_kt=10)
    for support_id, site_id, support_type, capacity in (
            ("SUP-MAIN-GH", "main", "ground_handling", 3),
            ("SUP-CAMP-GH", "camp", "ground_handling", 3),
            ("SUP-MAIN-MED", "main", "medical", 1),
            ("SUP-CAMP-MED", "camp", "medical", 1)):
        service.register_ground_support(request_id=f"req-{support_id}", actor_id="disp-a",
                                        support_id=support_id, site_id=site_id,
                                        support_type=support_type, available_from=_ts(1, 8),
                                        available_to=_ts(3, 8), capacity=capacity)


def _missions(service: FlightOpsService) -> None:
    service.register_mission(request_id="req-m-med", actor_id="disp-a", mission_id="MED",
                             mission_type="medical_evacuation", origin_site_id="main",
                             destination_site_id="camp", distance_nm=300, alternate_site_id="alt",
                             alternate_distance_nm=60, payload_kg=200, passengers=2,
                             passenger_restrictions=["stretcher"],
                             required_support=["ground_handling", "medical"],
                             earliest_departure=_ts(1, 9), latest_arrival=_ts(2, 18))
    service.register_mission(request_id="req-m-cal", actor_id="disp-a", mission_id="CAL",
                             mission_type="instrument_calibration", origin_site_id="main",
                             destination_site_id="camp", distance_nm=300, alternate_site_id="alt",
                             alternate_distance_nm=60, payload_kg=400, passengers=1,
                             earliest_departure=_ts(1, 9), latest_arrival=_ts(2, 18))
    service.register_mission(request_id="req-m-sup", actor_id="disp-a", mission_id="SUP",
                             mission_type="supply", origin_site_id="main",
                             destination_site_id="camp", distance_nm=300, alternate_site_id="alt",
                             alternate_distance_nm=60, payload_kg=1200, passengers=0,
                             earliest_departure=_ts(1, 9), latest_arrival=_ts(2, 18))


def run() -> dict[str, object]:
    """执行完整验收链并返回结果。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "flight_ops.sqlite3"
        database = Database(path)
        service = FlightOpsService(database, ManualClock(T0))
        _bootstrap(service)
        _missions(service)

        # 1. 候选方案可比较
        candidates = service.plan_candidates(mission_id="CAL")
        _check(candidates["feasible_count"] >= 1, "CAL 应有可行候选方案")
        first = candidates["candidates"][0]
        _check(len({item["name"] for item in first["constraints"]}) == 9,
               "候选方案应包含九项约束评估")

        # 2. 调度员甲为三个任务创建方案并租约保留
        plan = service.create_plan(request_id="req-plan-1", actor_id="disp-a",
                                   mission_ids=["MED", "CAL", "SUP"], auto=True,
                                   lease_ttl_seconds=3600)
        _check(plan["status"] == "leased" and len(plan["legs"]) == 3,
               "方案应包含三个航段并处于待审批状态")
        _check(all(lease["status"] == "active" for lease in plan["leases"]),
               "全部租约应处于有效状态")

        # 3. 调度员乙的竞争方案在同一资源上立刻可见冲突
        conflict_leg = dict(plan["legs"][0], mission_id="SUP", leg_sequence=1)
        conflict_leg.pop("leg_id", None)
        conflict_leg.pop("status", None)
        conflict_leg.pop("support_bindings_json", None)
        try:
            service.create_plan(request_id="req-plan-2", actor_id="disp-b",
                                mission_ids=["SUP"], legs=[conflict_leg])
            raise RuntimeError("验收失败：竞争方案未被拒绝")
        except ConflictError:
            pass

        # 4. 双方批准前不能封存；批准后封存生效
        try:
            service.seal_plan(request_id="req-seal-0", actor_id="disp-a",
                              plan_id=plan["plan_id"], expected_schedule_version=0)
            raise RuntimeError("验收失败：未批准即封存")
        except ConflictError:
            pass
        service.approve_plan(request_id="req-app-ops", actor_id="ops-001",
                             plan_id=plan["plan_id"], side="operations", decision="approve")
        service.approve_plan(request_id="req-app-stn", actor_id="stn-001",
                             plan_id=plan["plan_id"], side="station", decision="approve")
        sealed = service.seal_plan(request_id="req-seal-1", actor_id="disp-a",
                                   plan_id=plan["plan_id"], expected_schedule_version=0)
        _check(sealed["status"] == "sealed" and sealed["sealed_version"] == 1,
               "封存后版本应推进到 1")

        # 5. 基于旧版本的另一个方案封存时必须失败（只有一个版本生效）
        rival = service.create_plan(request_id="req-plan-3", actor_id="disp-b",
                                    mission_ids=["CAL"], auto=True, lease_ttl_seconds=3600)
        service.approve_plan(request_id="req-app-ops-2", actor_id="ops-001",
                             plan_id=rival["plan_id"], side="operations", decision="approve")
        service.approve_plan(request_id="req-app-stn-2", actor_id="stn-001",
                             plan_id=rival["plan_id"], side="station", decision="approve")
        try:
            service.seal_plan(request_id="req-seal-2", actor_id="disp-b",
                              plan_id=rival["plan_id"], expected_schedule_version=0)
            raise RuntimeError("验收失败：过期基线仍能封存")
        except ConflictError:
            pass

        # 6. 医疗航段起飞后不受重排影响
        med_leg = next(leg for leg in plan["legs"] if leg["mission_id"] == "MED")
        service.record_leg_event(request_id="req-dep-1", actor_id="disp-a",
                                 leg_id=med_leg["leg_id"], event="depart")

        # 7. 气象预报修订：只取消未执行且确实受影响的航段（含竞争方案的次日航段）
        service.register_weather_forecast(request_id="req-f2", actor_id="disp-a", forecast_id="F2",
                                          site_id="camp", valid_from=_ts(1, 6), valid_to=_ts(3, 6),
                                          ceiling_m=50, visibility_m=8000, wind_kt=10)
        disruption = service.process_disruption(request_id="req-dis-1", actor_id="disp-a",
                                                alert_id="alert-001",
                                                disruption_type="weather_revision",
                                                forecast_id="F2")
        _check(len(disruption["cancelled_legs"]) == 3, "应取消三个未执行且受影响的航段")
        _check(med_leg["leg_id"] not in disruption["cancelled_legs"], "已起飞航段必须保留")
        duplicate = service.process_disruption(request_id="req-dis-2", actor_id="disp-a",
                                               alert_id="alert-001",
                                               disruption_type="weather_revision",
                                               forecast_id="F2")
        _check(duplicate["duplicate"] is True, "重复告警应识别为重复")
        _check(len(duplicate["cancelled_legs"]) == 3, "重复告警不应再次取消")

        # 8. 解释：能看到获准、取消的原因与逐项资源变化
        explanation = service.mission_explanation("CAL")
        decisions = [item["decision"] for item in explanation["decisions"]]
        _check("approved" in decisions and "cancelled" in decisions,
               "解释应包含获准与取消决定")
        actions = [item["action"] for item in explanation["resource_changes"]]
        _check("lease_acquired" in actions and "committed" in actions and "released" in actions,
               "资源台账应包含占用、承诺与释放")

        # 9. 气象依然恶劣时 CAL 进入候补；新预报发布后 SUP 重排为待审批方案
        blocked = service.create_plan(request_id="req-plan-4", actor_id="disp-a",
                                      mission_ids=["CAL"], auto=True, waitlist_if_blocked=True)
        _check(blocked["waitlisted"] == ["CAL"], "气象不满足时 CAL 应进入候补")
        service.register_weather_forecast(request_id="req-f3", actor_id="disp-a", forecast_id="F3",
                                          site_id="camp", valid_from=_ts(2, 6), valid_to=_ts(4, 6),
                                          ceiling_m=300, visibility_m=8000, wind_kt=10)
        pending = service.create_plan(request_id="req-plan-5", actor_id="disp-a",
                                      mission_ids=["SUP"], auto=True, lease_ttl_seconds=7200)
        _check(pending["status"] == "leased", "SUP 应重排为待审批方案")
        database.close()

        # 10. 进程恢复：有效租约、候补次序与待审批版本延续
        recovered_db = Database(path)
        recovered_clock = ManualClock(datetime(2026, 10, 7, 8, 10, tzinfo=timezone.utc))
        recovered = FlightOpsService(recovered_db, recovered_clock)
        _check(recovered.schedule_status()["sealed_version"] == 1, "恢复后封存版本应延续")
        med_state = recovered.get_mission("MED")
        _check(med_state["mission"]["status"] == "in_progress", "恢复后已起飞任务应保持执行中")
        _check([item["mission_id"] for item in recovered.list_waitlist()] == ["CAL"],
               "恢复后候补次序应延续")
        restored = recovered.get_plan(pending["plan_id"])
        _check(restored["status"] == "leased", "恢复后待审批方案应延续")
        _check(all(lease["status"] == "active" for lease in restored["leases"]),
               "恢复后有效租约应延续")
        valid, event_count = recovered.verify_audit()
        _check(valid, "审计链应完整")
        result = {"status": "ok", "audit_events": event_count, "audit_valid": valid,
                  "sealed_version": recovered.schedule_status()["sealed_version"],
                  "cancelled_by_weather": len(disruption["cancelled_legs"]),
                  "duplicate_alert": duplicate["duplicate"],
                  "waitlist": len(recovered.list_waitlist())}
        recovered_db.close()
        return result


def main() -> int:
    """打印验收结果并设置退出码。"""

    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ok" and result["audit_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
