//! SQLite schema for the capsule store. Migrations are versioned and applied
//! linearly. See `DESIGN.md` §4.

use rusqlite::{Connection, Result as SqlResult};

pub const SCHEMA_VERSION: i64 = 1;

const MIGRATIONS: &[&str] = &[
    // v1: initial schema. Capsule is the aggregate root; attempts and events
    // are normalized; attestations and landing live inline on the capsule
    // (only one of each per capsule lifetime — verification is locked at
    // accepted, landing is terminal). PendingLand is inline JSON for the
    // crash-recovery transactional invariant in DESIGN.md §7.1.2.
    r#"
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS capsule (
        id              TEXT PRIMARY KEY,
        title           TEXT NOT NULL,
        description     TEXT NOT NULL,
        acceptance_json TEXT NOT NULL,
        scope_json      TEXT NOT NULL,    -- JSON array of canonical paths
        base_ref        TEXT NOT NULL,
        depends_on_json TEXT NOT NULL,    -- JSON array of capsule_id
        status          TEXT NOT NULL CHECK (status IN
                          ('planned','active','accepted','landed','abandoned')),
        active_attempt  INTEGER,
        verification_json TEXT,           -- nullable; locked once status=accepted
        pending_land_json TEXT,           -- nullable; reclaim-frozen while non-null (§7.2)
        landing_json    TEXT,             -- nullable; terminal
        created_at      TEXT NOT NULL,
        updated_at      TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_capsule_status ON capsule(status);
    CREATE INDEX IF NOT EXISTS idx_capsule_pending_land
        ON capsule(id) WHERE pending_land_json IS NOT NULL;

    CREATE TABLE IF NOT EXISTS attempt (
        capsule_id      TEXT NOT NULL REFERENCES capsule(id),
        attempt_id      INTEGER NOT NULL,
        lease_json      TEXT NOT NULL,
        branch          TEXT NOT NULL,
        witness_branch  TEXT NOT NULL,
        base_sha        TEXT NOT NULL,
        tip_sha         TEXT,
        last_heartbeat  TEXT NOT NULL,
        outcome         TEXT NOT NULL CHECK (outcome IN
                          ('in_flight','released','expired','abandoned','landed')),
        opened_at       TEXT NOT NULL,
        closed_at       TEXT,
        PRIMARY KEY (capsule_id, attempt_id)
    );

    CREATE INDEX IF NOT EXISTS idx_attempt_outcome
        ON attempt(capsule_id, outcome);

    CREATE TABLE IF NOT EXISTS event (
        rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
        at          TEXT NOT NULL,
        capsule_id  TEXT NOT NULL REFERENCES capsule(id),
        attempt_id  INTEGER,
        actor       TEXT NOT NULL,
        kind        TEXT NOT NULL,
        payload_json TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_event_capsule ON event(capsule_id, rowid);
    "#,
];

pub fn ensure(conn: &Connection) -> SqlResult<()> {
    conn.execute_batch(
        "BEGIN; CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY); COMMIT;",
    )?;

    let current: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_version",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    for (idx, sql) in MIGRATIONS.iter().enumerate() {
        let v = (idx as i64) + 1;
        if v <= current {
            continue;
        }
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(sql)?;
        tx.execute("INSERT INTO schema_version(version) VALUES (?1)", [v])?;
        tx.commit()?;
    }

    Ok(())
}
