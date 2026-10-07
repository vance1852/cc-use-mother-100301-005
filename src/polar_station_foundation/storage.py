"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_aircraft (
    aircraft_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    payload_capacity_kg REAL NOT NULL CHECK(payload_capacity_kg > 0),
    seat_capacity INTEGER NOT NULL CHECK(seat_capacity >= 0),
    fuel_capacity_kg REAL NOT NULL CHECK(fuel_capacity_kg > 0),
    burn_kg_per_hour REAL NOT NULL CHECK(burn_kg_per_hour > 0),
    speed_kt REAL NOT NULL CHECK(speed_kt > 0),
    crosswind_limit_kt INTEGER NOT NULL CHECK(crosswind_limit_kt >= 0),
    required_rating TEXT NOT NULL,
    capabilities_json TEXT NOT NULL DEFAULT '[]',
    grounded INTEGER NOT NULL DEFAULT 0 CHECK(grounded IN (0, 1)),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_airfields (
    code TEXT PRIMARY KEY,
    site_id TEXT,
    display_name TEXT NOT NULL,
    has_fuel INTEGER NOT NULL CHECK(has_fuel IN (0, 1)),
    has_ground_handling INTEGER NOT NULL CHECK(has_ground_handling IN (0, 1)),
    medevac_capable INTEGER NOT NULL CHECK(medevac_capable IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_airfield_windows (
    window_id TEXT PRIMARY KEY,
    airfield_code TEXT NOT NULL REFERENCES dispatch_airfields(code),
    opens_at TEXT NOT NULL,
    closes_at TEXT NOT NULL,
    CHECK(closes_at > opens_at)
);
CREATE TABLE IF NOT EXISTS dispatch_crew (
    crew_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    qualifications_json TEXT NOT NULL DEFAULT '[]',
    duty_starts_at TEXT,
    duty_ends_at TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_distances (
    origin_code TEXT NOT NULL,
    destination_code TEXT NOT NULL,
    distance_nm REAL NOT NULL CHECK(distance_nm >= 0),
    PRIMARY KEY(origin_code, destination_code)
);
CREATE TABLE IF NOT EXISTS dispatch_fuel_stocks (
    stock_id TEXT PRIMARY KEY,
    airfield_code TEXT NOT NULL REFERENCES dispatch_airfields(code),
    quantity_kg REAL NOT NULL CHECK(quantity_kg >= 0),
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    CHECK(ends_at > starts_at)
);
CREATE TABLE IF NOT EXISTS dispatch_forecasts (
    airfield_code TEXT NOT NULL REFERENCES dispatch_airfields(code),
    version INTEGER NOT NULL CHECK(version >= 1),
    issued_at TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL,
    ceiling_ft INTEGER,
    visibility_km REAL,
    crosswind_kt INTEGER,
    superseded INTEGER NOT NULL DEFAULT 0 CHECK(superseded IN (0, 1)),
    PRIMARY KEY(airfield_code, version)
);
CREATE TABLE IF NOT EXISTS dispatch_missions (
    mission_id TEXT PRIMARY KEY,
    parent_mission_id TEXT REFERENCES dispatch_missions(mission_id),
    kind TEXT NOT NULL,
    priority INTEGER NOT NULL,
    origin_code TEXT NOT NULL REFERENCES dispatch_airfields(code),
    destination_code TEXT NOT NULL REFERENCES dispatch_airfields(code),
    passengers INTEGER NOT NULL CHECK(passengers >= 0),
    cargo_kg REAL NOT NULL CHECK(cargo_kg >= 0),
    cargo_items_json TEXT NOT NULL DEFAULT '[]',
    passenger_restriction_json TEXT NOT NULL DEFAULT '{}',
    required_by TEXT,
    current_round INTEGER NOT NULL DEFAULT 1 CHECK(current_round >= 1),
    state TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_plans (
    plan_id TEXT PRIMARY KEY,
    mission_id TEXT NOT NULL REFERENCES dispatch_missions(mission_id),
    round INTEGER NOT NULL CHECK(round >= 1),
    status TEXT NOT NULL,
    aircraft_id TEXT REFERENCES dispatch_aircraft(aircraft_id),
    origin_window_id TEXT,
    destination_window_id TEXT,
    alternate_code TEXT,
    dep_at TEXT,
    arr_at TEXT,
    fuel_required_kg REAL,
    payload_kg REAL,
    crew_json TEXT NOT NULL DEFAULT '[]',
    forecast_versions_json TEXT NOT NULL DEFAULT '{}',
    factors_json TEXT NOT NULL DEFAULT '[]',
    score REAL,
    version_token TEXT NOT NULL,
    operations_approved_by TEXT,
    operations_approved_at TEXT,
    station_approved_by TEXT,
    station_approved_at TEXT,
    sealed_at TEXT,
    reason TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS dispatch_one_sealed_per_round
    ON dispatch_plans(mission_id, round) WHERE status = 'sealed';
CREATE TABLE IF NOT EXISTS dispatch_plan_legs (
    leg_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES dispatch_plans(plan_id),
    sequence INTEGER NOT NULL DEFAULT 1,
    aircraft_id TEXT,
    origin_code TEXT,
    destination_code TEXT,
    alternate_code TEXT,
    dep_at TEXT,
    arr_at TEXT,
    crew_json TEXT NOT NULL DEFAULT '[]',
    fuel_required_kg REAL,
    payload_kg REAL,
    pax INTEGER,
    origin_window_id TEXT,
    destination_window_id TEXT,
    forecast_versions_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE(plan_id, sequence)
);
CREATE TABLE IF NOT EXISTS dispatch_leases (
    lease_id TEXT PRIMARY KEY,
    plan_id TEXT,
    mission_id TEXT,
    leg_id TEXT,
    commitment_id TEXT,
    resource_type TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    quantity REAL NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    released_at TEXT,
    release_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_dispatch_leases_resource
    ON dispatch_leases(resource_type, resource_key, status);
CREATE INDEX IF NOT EXISTS idx_dispatch_leases_mission
    ON dispatch_leases(mission_id);
CREATE TABLE IF NOT EXISTS dispatch_waitlist (
    entry_id TEXT PRIMARY KEY,
    resource_type TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    mission_id TEXT NOT NULL,
    plan_id TEXT,
    seq INTEGER NOT NULL,
    status TEXT NOT NULL,
    enqueued_at TEXT NOT NULL,
    promoted_at TEXT,
    UNIQUE(resource_type, resource_key, seq)
);
CREATE TABLE IF NOT EXISTS dispatch_commitments (
    commitment_id TEXT PRIMARY KEY,
    mission_id TEXT NOT NULL REFERENCES dispatch_missions(mission_id),
    round INTEGER NOT NULL,
    plan_id TEXT NOT NULL REFERENCES dispatch_plans(plan_id),
    leg_id TEXT NOT NULL REFERENCES dispatch_plan_legs(leg_id),
    seq INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    departed_at TEXT,
    handed_over_at TEXT,
    replaced_at TEXT,
    replace_reason TEXT,
    alert_key TEXT
);
CREATE INDEX IF NOT EXISTS idx_dispatch_commitments_mission
    ON dispatch_commitments(mission_id);
CREATE TABLE IF NOT EXISTS dispatch_alerts (
    alert_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    received_at TEXT NOT NULL,
    processed_at TEXT
);
CREATE TABLE IF NOT EXISTS dispatch_alert_effects (
    effect_id TEXT PRIMARY KEY,
    alert_key TEXT NOT NULL REFERENCES dispatch_alerts(alert_key),
    commitment_id TEXT,
    mission_id TEXT,
    action TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispatch_ledger (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ledger_id TEXT NOT NULL UNIQUE,
    mission_id TEXT,
    plan_id TEXT,
    leg_id TEXT,
    commitment_id TEXT,
    movement TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    starts_at TEXT,
    ends_at TEXT,
    quantity REAL,
    reason TEXT NOT NULL,
    alert_key TEXT,
    lease_id TEXT,
    occurred_at TEXT NOT NULL
);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
