//! SQLite schema for the capsule store. Migrations are versioned and applied
//! linearly. See `DESIGN.md` §4.

use rusqlite::{Connection, Result as SqlResult};

/// Derived from `MIGRATIONS` so the two cannot drift. Bumping the schema is a
/// one-step change: append a new entry to `MIGRATIONS` and `SCHEMA_VERSION`
/// follows.
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

const MIGRATIONS: &[&str] = &[
    // v1: initial schema. Capsule is the aggregate root; attempts and events
    // are normalized; attestations and landing live inline on the capsule
    // (only one of each per capsule lifetime — verification is locked at
    // accepted, landing is terminal). PendingLand is inline JSON for the
    // crash-recovery transactional invariant in DESIGN.md §7.1.2.
    r"
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
    ",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ensure_records_current_schema_version() {
        let conn = Connection::open_in_memory().unwrap();
        ensure(&conn).unwrap();
        let recorded: i64 = conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(recorded, SCHEMA_VERSION);
    }

    #[test]
    fn ensure_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        ensure(&conn).unwrap();
        ensure(&conn).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM schema_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, SCHEMA_VERSION);
    }

    /// Read the live CREATE TABLE SQL for `table_name` from `sqlite_schema`.
    /// Relies on SQLite preserving the migration's CREATE TABLE statement
    /// verbatim (modulo comments and whitespace). Used by the CHECK-list
    /// pin tests as the source of truth.
    fn live_table_sql(conn: &Connection, table_name: &str) -> String {
        conn.query_row(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?1",
            [table_name],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Extract the single-quoted literal set of a `CHECK (<col> IN (...))`
    /// clause from a CREATE TABLE statement. Looks for `CHECK (<col> IN`
    /// and parses literals between the next `(` and matching `)`. Tight
    /// enough for the v1 schema's hand-written CHECK clauses; extending to
    /// a CHECK with parens or escaped quotes inside literals would require
    /// a real parser.
    fn extract_check_in_list(sql: &str, column: &str) -> std::collections::HashSet<String> {
        let needle = format!("CHECK ({column} IN");
        let after = sql
            .split_once(&needle)
            .unwrap_or_else(|| panic!("CHECK ({column} IN ...) not found in:\n{sql}"))
            .1;
        let after_open = after.split_once('(').expect("IN list open paren").1;
        let inside = after_open.split_once(')').expect("IN list close paren").0;
        inside
            .split(',')
            .map(|tok| {
                let t = tok.trim();
                // Each token must be a single-quoted literal; refuse to
                // silently strip absent quotes so a bare identifier in the
                // CHECK list (e.g. a typo dropping the quotes) fails loudly
                // instead of round-tripping through the same trim_matches
                // as the legitimate quoted form.
                assert!(
                    t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2,
                    "expected single-quoted SQL literal in CHECK IN list, got {t:?}",
                );
                t[1..t.len() - 1].to_string()
            })
            .collect()
    }

    /// The two tests below pin set-equality between a SQL `CHECK (col IN
    /// (...))` accept-list and the Rust enum's `as_wire_str` set, in **both**
    /// directions. Drift either way is a bug we want caught at test time:
    /// - SQL omission ⇒ `INSERT` fails at runtime when Rust emits the new variant.
    /// - SQL extra ⇒ accept-list entry that no Rust code can produce, masking
    ///   a removed/renamed variant.
    #[test]
    fn capsule_status_check_set_equals_status_wire_set() {
        let conn = Connection::open_in_memory().unwrap();
        ensure(&conn).unwrap();
        let sql = live_table_sql(&conn, "capsule");
        let in_list = extract_check_in_list(&sql, "status");
        let expected: std::collections::HashSet<String> = [
            capsule_core::Status::Planned,
            capsule_core::Status::Active,
            capsule_core::Status::Accepted,
            capsule_core::Status::Landed,
            capsule_core::Status::Abandoned,
        ]
        .iter()
        .map(|s| s.as_wire_str().to_string())
        .collect();
        assert_eq!(
            in_list, expected,
            "capsule.status CHECK list disagrees with Status::as_wire_str set",
        );
    }

    #[test]
    fn attempt_outcome_check_set_equals_outcome_wire_set() {
        let conn = Connection::open_in_memory().unwrap();
        ensure(&conn).unwrap();
        let sql = live_table_sql(&conn, "attempt");
        let in_list = extract_check_in_list(&sql, "outcome");
        let expected: std::collections::HashSet<String> = [
            capsule_core::AttemptOutcome::InFlight,
            capsule_core::AttemptOutcome::Released,
            capsule_core::AttemptOutcome::Expired,
            capsule_core::AttemptOutcome::Abandoned,
            capsule_core::AttemptOutcome::Landed,
        ]
        .iter()
        .map(|o| o.as_wire_str().to_string())
        .collect();
        assert_eq!(
            in_list, expected,
            "attempt.outcome CHECK list disagrees with AttemptOutcome::as_wire_str set",
        );
    }

    #[test]
    fn extract_check_in_list_parses_quoted_literals() {
        // Pin the parser shape used by the two CHECK-set tests against
        // realistic whitespace variations (`'a','b', 'c' ` — leading-space,
        // no-space, trailing-space) so a future schema reformat doesn't
        // silently break extraction.
        let sql = "CREATE TABLE x (\n  c TEXT NOT NULL CHECK (c IN ('a','b', 'c' )),\n  d INT)";
        let got = extract_check_in_list(sql, "c");
        let want: std::collections::HashSet<String> =
            ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        assert_eq!(got, want);
    }

    #[test]
    #[should_panic(expected = "expected single-quoted SQL literal")]
    fn extract_check_in_list_rejects_unquoted_token() {
        // Pin the contract: an IN-list element without enclosing quotes
        // (e.g. a typo where someone wrote `bare` instead of `'bare'`)
        // panics rather than silently round-tripping through the
        // trim/strip path. Without this guard, a malformed CHECK could
        // produce values that compare equal to the enum wire strings
        // and pass the set-equality tests.
        let sql = "CHECK (c IN ('a', bare, 'c'))";
        let _ = extract_check_in_list(sql, "c");
    }
}
