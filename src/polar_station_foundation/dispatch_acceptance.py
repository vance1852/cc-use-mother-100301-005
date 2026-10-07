"""飞行窗口与任务承诺服务的离线端到端验收。

在临时 SQLite 数据库中走完：参考资料登记 → 候选比较 → 限时租约保留 →
运行/站点双方批准封存 → 飞机故障重排未执行承诺 → 重新保留并封存 →
医疗插队 → 进程恢复，并核对资源台账、候补次序与审计链。
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .clock import FixedClock
from .dispatch import DispatchService
from .service import DomainService
from .storage import Database

W_OPEN = "2026-10-08T00:00:00Z"
W_CLOSE = "2026-10-08T12:00:00Z"
FX_TO = "2026-10-09T00:00:00Z"


def _seed(base: DomainService, dispatch: DispatchService) -> None:
    base.register_organization(request_id="req-org", actor_id="bootstrap",
                               organization_id="org1", name="极地科考机构")
    base.register_actor(request_id="req-adm", actor_id="bootstrap", new_actor_id="adm",
                        display_name="管理员", role="admin", organization_id="org1")
    base.register_actor(request_id="req-op", actor_id="adm", new_actor_id="op",
                        display_name="运行调度", role="operator", organization_id="org1")
    base.register_actor(request_id="req-st", actor_id="adm", new_actor_id="st",
                        display_name="站点代表", role="reviewer", organization_id="org1")
    base.register_site(request_id="req-site", actor_id="op", site_id="s1",
                       organization_id="org1", name="内陆营地", timezone_name="UTC")
    dispatch.register_aircraft(request_id="req-ac1", actor_id="op", site_id="s1",
                               aircraft_id="AC1", display_name="双水獭",
                               payload_capacity_kg=1200, seat_capacity=9,
                               fuel_capacity_kg=1000, burn_kg_per_hour=200, speed_kt=150,
                               crosswind_limit_kt=25, required_rating="DHC6",
                               capabilities=["medevac_kit"])
    for code, name, medevac in [("CB", "沿海基地", True), ("IN", "内陆营地", False),
                                ("ALT", "备降点", True)]:
        dispatch.register_airfield(request_id=f"req-af-{code}", actor_id="op", code=code,
                                   display_name=name, has_fuel=True,
                                   has_ground_handling=True, medevac_capable=medevac,
                                   site_id="s1")
        dispatch.add_airfield_window(request_id=f"req-win-{code}", actor_id="op",
                                     window_id=f"W-{code}", airfield_code=code,
                                     opens_at=W_OPEN, closes_at=W_CLOSE)
    dispatch.register_crew(request_id="req-p1", actor_id="op", crew_id="P1",
                           display_name="机长一", role="pilot",
                           qualifications=["DHC6"], duty_starts_at=W_OPEN,
                           duty_ends_at="2026-10-08T20:00:00Z")
    dispatch.register_crew(request_id="req-lm", actor_id="op", crew_id="LM",
                           display_name="装卸员", role="loadmaster",
                           duty_starts_at=W_OPEN, duty_ends_at="2026-10-08T20:00:00Z")
    dispatch.register_crew(request_id="req-md", actor_id="op", crew_id="MD",
                           display_name="随机医护", role="medic",
                           duty_starts_at=W_OPEN, duty_ends_at="2026-10-08T20:00:00Z")
    dispatch.register_distance(request_id="req-dist-ci", actor_id="op", origin_code="CB",
                               destination_code="IN", distance_nm=300)
    dispatch.register_distance(request_id="req-dist-ia", actor_id="op", origin_code="IN",
                               destination_code="ALT", distance_nm=100)
    dispatch.add_fuel_stock(request_id="req-fs1", actor_id="op", stock_id="FS-CB",
                            airfield_code="CB", quantity_kg=5000, starts_at=W_OPEN,
                            ends_at=FX_TO)
    dispatch.add_fuel_stock(request_id="req-fs2", actor_id="op", stock_id="FS-IN",
                            airfield_code="IN", quantity_kg=3000, starts_at=W_OPEN,
                            ends_at=FX_TO)
    for code, ceiling, vis, xwind in [("CB", 2000, 10, 10), ("IN", 1500, 8, 12),
                                      ("ALT", 1800, 9, 8)]:
        dispatch.issue_forecast(request_id=f"req-fx-{code}", actor_id="op",
                                airfield_code=code, valid_from=W_OPEN, valid_to=FX_TO,
                                ceiling_ft=ceiling, visibility_km=vis, crosswind_kt=xwind)


def run() -> dict[str, object]:
    """执行调度完整链路并返回可核对的结果。"""

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "dispatch_acceptance.sqlite3"
        clock = FixedClock(datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc))
        database = Database(path)
        base = DomainService(database, clock)
        dispatch = DispatchService(database, clock, lease_ttl_minutes=30)
        _seed(base, dispatch)

        dispatch.create_mission(request_id="req-sup", actor_id="op", mission_id="SUP-1",
                                kind="supply", origin_code="CB", destination_code="IN",
                                passengers=2, cargo_kg=800,
                                cargo_items=[{"id": "i1", "kg": 500}, {"id": "i2", "kg": 300}])
        candidates = dispatch.build_candidates("op", "SUP-1")
        reserved = dispatch.reserve_best(request_id="req-res1", actor_id="op",
                                         mission_id="SUP-1")
        lease_count = len(reserved["lease_ids"])
        ops = dispatch.approve_plan(request_id="req-ops1", actor_id="op",
                                    plan_id=reserved["plan_id"], party="operations")
        sealed = dispatch.approve_plan(request_id="req-sta1", actor_id="st",
                                       plan_id=reserved["plan_id"], party="station")

        # 飞机故障只重排尚未起飞的承诺。
        fault = dispatch.ingest_alert(request_id="req-fault", actor_id="op",
                                      alert_key="FAULT-1", kind="aircraft_fault",
                                      payload={"aircraft_id": "AC1"})
        replanned_once = [e for e in fault["effects"] if e["action"] == "mission.replanned"]
        fault_replay = dispatch.ingest_alert(request_id="req-fault2", actor_id="op",
                                             alert_key="FAULT-1", kind="aircraft_fault",
                                             payload={"aircraft_id": "AC1"})

        # 解除故障后重新保留并双方封存，随后起飞与交接不得再被撤销。
        dispatch.ingest_alert(request_id="req-clear", actor_id="op", alert_key="CLEAR-1",
                              kind="aircraft_fault",
                              payload={"aircraft_id": "AC1", "grounded": False})
        reserved2 = dispatch.reserve_best(request_id="req-res2", actor_id="op",
                                          mission_id="SUP-1")
        dispatch.approve_plan(request_id="req-ops2", actor_id="op",
                              plan_id=reserved2["plan_id"], party="operations")
        dispatch.approve_plan(request_id="req-sta2", actor_id="st",
                              plan_id=reserved2["plan_id"], party="station")
        explanation = dispatch.get_mission_explanation("op", "SUP-1")
        commitment = next(c for c in explanation["commitments"] if c["state"] == "committed")
        dispatch.mark_departed(request_id="req-dep", actor_id="op",
                               commitment_id=commitment["commitment_id"])
        wx_after = dispatch.ingest_alert(request_id="req-wx", actor_id="op",
                                         alert_key="WX-AFTER", kind="weather_revision",
                                         payload={"airfield_code": "IN",
                                                  "valid_from": W_OPEN, "valid_to": FX_TO,
                                                  "ceiling_ft": 200, "visibility_km": 1.0,
                                                  "crosswind_kt": 40})
        dispatch.mark_handover(request_id="req-ho", actor_id="st",
                               commitment_id=commitment["commitment_id"], notes="签收")
        final = dispatch.get_mission_explanation("op", "SUP-1")

        # 气象恢复后，低优先级补给任务才能重新评估并保留。
        dispatch.issue_forecast(request_id="req-fx-in-ok", actor_id="op",
                                airfield_code="IN", valid_from=W_OPEN, valid_to=FX_TO,
                                ceiling_ft=1600, visibility_km=9, crosswind_kt=10)
        dispatch.create_mission(request_id="req-low", actor_id="op", mission_id="LOW-1",
                                kind="supply", origin_code="CB", destination_code="IN",
                                passengers=1, cargo_kg=50)
        dispatch.reserve_best(request_id="req-lowr", actor_id="op", mission_id="LOW-1")
        dispatch.create_mission(request_id="req-med", actor_id="op", mission_id="MED-1",
                                kind="medevac", origin_code="IN", destination_code="CB",
                                passengers=2, cargo_kg=20,
                                passenger_restriction={"medical_attendant": True})
        preempt = dispatch.ingest_alert(request_id="req-pre", actor_id="op",
                                        alert_key="MED-1", kind="medevac_preempt",
                                        payload={"aircraft_id": "AC1",
                                                 "medevac_mission_id": "MED-1",
                                                 "starts_at": W_OPEN, "ends_at": W_CLOSE})
        waitlist = dispatch.list_waitlist("op")["items"]

        valid, audit_count = base.verify_audit()
        database.close()

        # 进程恢复：租约、候补、待审批版本都在 SQLite 中延续。
        database2 = Database(path)
        recovered = DispatchService(database2, clock)
        held = recovered.list_leases("op", status="held")["items"]
        pending_plans = database2.connection.execute(
            "SELECT plan_id FROM dispatch_plans WHERE status='reserved'").fetchall()
        recovered_waitlist = recovered.list_waitlist("op")["items"]
        database2.close()

        movements = {entry["movement"] for entry in final["resource_ledger"]}
        result = {
            "status": "ok",
            "candidate_count": len(candidates),
            "first_candidate_feasible": candidates[0]["feasible"],
            "lease_count": lease_count,
            "ops_sealed": ops["sealed"],
            "dual_sealed": sealed["sealed"],
            "fault_replanned_once": len(replanned_once) == 1,
            "duplicate_fault_replayed": fault_replay["replayed"],
            "duplicate_fault_no_effects": fault_replay["effects"] == [],
            "final_mission_state": final["state"],
            "departed_commitment_kept": "mission.replanned" not in [
                e["action"] for e in wx_after["effects"]],
            "ledger_has_acquire": "lease.acquire" in movements,
            "ledger_has_release": "lease.release" in movements,
            "medevac_preempted_low": any(
                e["action"] == "mission.preempted" and e.get("mission_id") == "LOW-1"
                for e in preempt["effects"]),
            "medevac_plan_pending_approval": any(
                e["action"] == "plan.priority_reserved" for e in preempt["effects"]),
            "recovered_held_leases": len(held),
            "recovered_pending_plans": len(pending_plans),
            "recovered_waitlist": len(recovered_waitlist),
            "waitlist_preserved_order": [w["mission_id"] for w in waitlist]
            == [w["mission_id"] for w in recovered_waitlist],
            "audit_valid": valid,
            "audit_events": audit_count,
        }
        return result


def main() -> int:
    result = run()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    expected_true = [
        "first_candidate_feasible", "dual_sealed", "fault_replanned_once",
        "duplicate_fault_replayed", "duplicate_fault_no_effects",
        "departed_commitment_kept", "ledger_has_acquire", "ledger_has_release",
        "medevac_preempted_low", "medevac_plan_pending_approval",
        "waitlist_preserved_order", "audit_valid"]
    ok = result["status"] == "ok" and all(result[key] for key in expected_true) \
        and result["final_mission_state"] == "completed" \
        and result["recovered_held_leases"] > 0 and result["recovered_pending_plans"] >= 1
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
