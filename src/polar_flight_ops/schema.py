"""飞行窗口与任务承诺服务的 SQLite 表结构。"""

FLIGHT_SCHEMA = """
CREATE TABLE IF NOT EXISTS flight_aircraft (
    aircraft_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    registration TEXT NOT NULL UNIQUE,
    aircraft_type TEXT NOT NULL,
    cruise_speed_kt REAL NOT NULL CHECK(cruise_speed_kt > 0),
    fuel_capacity_kg REAL NOT NULL CHECK(fuel_capacity_kg > 0),
    burn_kg_per_nm REAL NOT NULL CHECK(burn_kg_per_nm > 0),
    reserve_fuel_kg REAL NOT NULL CHECK(reserve_fuel_kg >= 0),
    max_payload_kg REAL NOT NULL CHECK(max_payload_kg > 0),
    max_passengers INTEGER NOT NULL CHECK(max_passengers >= 0),
    capabilities_json TEXT NOT NULL,
    min_ceiling_m REAL NOT NULL,
    min_visibility_m REAL NOT NULL,
    max_wind_kt REAL NOT NULL,
    home_site_id TEXT NOT NULL REFERENCES sites(site_id),
    status TEXT NOT NULL CHECK(status IN ('serviceable','failed')),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS flight_crew (
    crew_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    display_name TEXT NOT NULL,
    crew_role TEXT NOT NULL CHECK(crew_role IN ('captain','copilot')),
    ratings_json TEXT NOT NULL,
    max_duty_hours REAL NOT NULL CHECK(max_duty_hours > 0),
    base_site_id TEXT NOT NULL REFERENCES sites(site_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS flight_windows (
    window_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    opens_at TEXT NOT NULL,
    closes_at TEXT NOT NULL,
    movement_capacity INTEGER NOT NULL CHECK(movement_capacity > 0),
    status TEXT NOT NULL CHECK(status IN ('open','closed')),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS weather_forecasts (
    forecast_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    version INTEGER NOT NULL CHECK(version >= 1),
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL,
    ceiling_m REAL NOT NULL,
    visibility_m REAL NOT NULL,
    wind_kt REAL NOT NULL,
    supersedes_forecast_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('current','superseded')),
    issued_at TEXT NOT NULL,
    UNIQUE(site_id, version)
);
CREATE TABLE IF NOT EXISTS ground_support (
    support_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    support_type TEXT NOT NULL,
    available_from TEXT NOT NULL,
    available_to TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity > 0),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS missions (
    mission_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    mission_type TEXT NOT NULL,
    priority_class INTEGER NOT NULL,
    origin_site_id TEXT NOT NULL REFERENCES sites(site_id),
    destination_site_id TEXT NOT NULL REFERENCES sites(site_id),
    distance_nm REAL NOT NULL CHECK(distance_nm > 0),
    alternate_site_id TEXT REFERENCES sites(site_id),
    alternate_distance_nm REAL NOT NULL DEFAULT 0,
    payload_kg REAL NOT NULL CHECK(payload_kg >= 0),
    passengers INTEGER NOT NULL CHECK(passengers >= 0),
    passenger_restrictions_json TEXT NOT NULL,
    required_support_json TEXT NOT NULL,
    earliest_departure TEXT NOT NULL,
    latest_arrival TEXT NOT NULL,
    allow_split INTEGER NOT NULL CHECK(allow_split IN (0, 1)),
    parent_mission_id TEXT,
    status TEXT NOT NULL CHECK(status IN ('open','planned','committed','in_progress','completed','cancelled')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    base_schedule_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('leased','sealed','superseded','expired','cancelled','rejected')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sealed_at TEXT,
    sealed_by TEXT
);
CREATE TABLE IF NOT EXISTS plan_missions (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    mission_id TEXT NOT NULL REFERENCES missions(mission_id),
    PRIMARY KEY(plan_id, mission_id)
);
CREATE TABLE IF NOT EXISTS legs (
    leg_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    mission_id TEXT NOT NULL REFERENCES missions(mission_id),
    leg_sequence INTEGER NOT NULL,
    aircraft_id TEXT NOT NULL REFERENCES flight_aircraft(aircraft_id),
    captain_id TEXT NOT NULL REFERENCES flight_crew(crew_id),
    copilot_id TEXT NOT NULL REFERENCES flight_crew(crew_id),
    departure_site_id TEXT NOT NULL,
    arrival_site_id TEXT NOT NULL,
    departure_window_id TEXT NOT NULL REFERENCES flight_windows(window_id),
    arrival_window_id TEXT NOT NULL REFERENCES flight_windows(window_id),
    forecast_id TEXT,
    alternate_site_id TEXT,
    planned_departure TEXT NOT NULL,
    planned_arrival TEXT NOT NULL,
    payload_kg REAL NOT NULL,
    passengers INTEGER NOT NULL,
    fuel_required_kg REAL NOT NULL,
    support_bindings_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('leased','committed','departed','arrived','handoff_completed','cancelled')),
    cancelled_reason TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS legs_mission ON legs(mission_id);
CREATE INDEX IF NOT EXISTS legs_plan ON legs(plan_id);
CREATE TABLE IF NOT EXISTS leases (
    lease_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    leg_id TEXT NOT NULL REFERENCES legs(leg_id),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    slot_start TEXT NOT NULL,
    slot_end TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    expires_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active','consumed','released','expired')),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS leases_resource ON leases(resource_type, resource_id, status);
CREATE TABLE IF NOT EXISTS allocations (
    allocation_id TEXT PRIMARY KEY,
    leg_id TEXT NOT NULL REFERENCES legs(leg_id),
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    slot_start TEXT NOT NULL,
    slot_end TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL CHECK(status IN ('committed','released','consumed')),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS allocations_resource ON allocations(resource_type, resource_id, status);
CREATE TABLE IF NOT EXISTS schedule_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    sealed_version INTEGER NOT NULL,
    sealed_plan_id TEXT
);
CREATE TABLE IF NOT EXISTS plan_approvals (
    plan_id TEXT NOT NULL REFERENCES plans(plan_id),
    approver_side TEXT NOT NULL CHECK(approver_side IN ('operations','station')),
    actor_id TEXT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('approve','reject')),
    comment TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY(plan_id, approver_side)
);
CREATE TABLE IF NOT EXISTS disruptions (
    alert_id TEXT PRIMARY KEY,
    disruption_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    processed_by TEXT NOT NULL,
    processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS waitlist (
    entry_id TEXT PRIMARY KEY,
    mission_id TEXT NOT NULL REFERENCES missions(mission_id),
    priority_class INTEGER NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('waiting','granted','cancelled')),
    queue_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS waitlist_order ON waitlist(status, priority_class, queue_seq);
CREATE TABLE IF NOT EXISTS resource_changes (
    change_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mission_id TEXT NOT NULL,
    leg_id TEXT,
    plan_id TEXT,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    slot_start TEXT,
    slot_end TEXT,
    quantity REAL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS resource_changes_mission ON resource_changes(mission_id);
CREATE TABLE IF NOT EXISTS mission_decisions (
    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    mission_id TEXT NOT NULL,
    leg_id TEXT,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS mission_decisions_mission ON mission_decisions(mission_id);
"""
