//! SQLite schema for the capsule store. Migrations are versioned and applied
//! linearly. See `DESIGN.md` §4.

use rusqlite::{Connection, Result as SqlResult};

/// Derived from `MIGRATIONS` so the two cannot drift. Bumping the schema is a
/// one-step change: append a new entry to `MIGRATIONS` and `SCHEMA_VERSION`
/// follows.
pub const SCHEMA_VERSION: i64 = MIGRATIONS.len() as i64;

const MIGRATIONS: &[&str] = &[V1_INITIAL, V2_DEPLOY_VERIFY_GATE];

/// v1: initial schema. Capsule is the aggregate root; attempts and events
/// are normalized; attestations and landing live inline on the capsule
/// (only one of each per capsule lifetime — verification is locked at
/// accepted, landing is terminal). PendingLand is inline JSON for the
/// crash-recovery transactional invariant in DESIGN.md §7.1.2.
const V1_INITIAL: &str = r"
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS capsule (
        id              TEXT PRIMARY KEY,
        title           TEXT NOT NULL,
        description     TEXT NOT NULL,
        acceptance_json TEXT NOT NULL,
        scope_json      TEXT NOT NULL,
        base_ref        TEXT NOT NULL,
        depends_on_json TEXT NOT NULL,
        status          TEXT NOT NULL CHECK (status IN
                          ('planned','active','accepted','landed','abandoned')),
        active_attempt  INTEGER,
        verification_json TEXT,           -- locked once status=accepted
        pending_land_json TEXT,           -- reclaim-frozen while non-null (§7.2)
        landing_json    TEXT,             -- terminal
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
    ";

/// v2: deploy-verify gate (DESIGN.md §8.2). `deploy_verify_pass` records
/// the most recent successful run of the ACL test suite for this store.
/// `Store::land` requires a non-null row before committing PendingLand,
/// unless `LandRequest::skip_deploy_verify_gate` is set. Single-row table
/// (PRIMARY KEY = 1) — the gate is a tri-state (never-run / passed /
/// bypassed-by-flag), not a history.
const V2_DEPLOY_VERIFY_GATE: &str = r"
    CREATE TABLE IF NOT EXISTS deploy_verify_pass (
        id        INTEGER PRIMARY KEY CHECK (id = 1),
        at        TEXT NOT NULL,
        mode      TEXT NOT NULL,
        base_ref  TEXT NOT NULL
    );
    ";

pub fn ensure(conn: &Connection) -> SqlResult<()> {
    bootstrap_version_table(conn)?;

    let current: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_version",
        [],
        |r| r.get(0),
    )?;

    let skip = usize::try_from(current).unwrap_or(usize::MAX);
    for (idx, sql) in MIGRATIONS.iter().enumerate().skip(skip) {
        let v = (idx as i64) + 1;
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(sql)?;
        tx.execute("INSERT INTO schema_version(version) VALUES (?1)", [v])?;
        tx.commit()?;
    }

    Ok(())
}

/// Create `schema_version` if absent so the version-read in `ensure` has a
/// table to hit before any migration runs. `V1_INITIAL` also declares this
/// table (`CREATE TABLE IF NOT EXISTS`); the duplication is intentional —
/// V1 is skipped on already-stamped DBs, so without this bootstrap the
/// version-read would fail on first open of a v1+ store.
fn bootstrap_version_table(conn: &Connection) -> SqlResult<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
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

    /// Hand-stamp a fresh in-memory DB at v1: runs `V1_INITIAL` directly
    /// (bypassing `ensure`) and records `version=1`. Used to seed the
    /// partial-upgrade path test.
    fn stamp_at_v1(conn: &Connection) {
        conn.execute_batch(V1_INITIAL).unwrap();
        conn.execute("INSERT INTO schema_version(version) VALUES (1)", [])
            .unwrap();
    }

    fn table_exists(conn: &Connection, name: &str) -> bool {
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name=?1",
                [name],
                |r| r.get(0),
            )
            .unwrap();
        n > 0
    }

    fn recorded_versions(conn: &Connection) -> Vec<i64> {
        let mut stmt = conn
            .prepare("SELECT version FROM schema_version ORDER BY version")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    /// Pin the partial-upgrade path: a DB stamped at v1 must skip V1_INITIAL
    /// (its `INSERT INTO schema_version VALUES (1)` would PK-conflict if
    /// re-run) and apply V2 only. Without this, an off-by-one in the skip
    /// guard (`v <= current` flipped to `v < current`) re-runs V1 on every
    /// upgrade and surfaces as a confusing PK conflict at INSERT time
    /// instead of a clean idempotent skip. Also covers the symmetric case
    /// for a future v3+ migration: the loop must apply only the new tail.
    #[test]
    fn ensure_skips_already_applied_migrations() {
        let conn = Connection::open_in_memory().unwrap();
        stamp_at_v1(&conn);
        assert!(
            !table_exists(&conn, "deploy_verify_pass"),
            "precondition: V2 not yet applied",
        );

        ensure(&conn).unwrap();

        assert!(
            table_exists(&conn, "deploy_verify_pass"),
            "V2 must apply on top of v1-stamped DB",
        );
        assert_eq!(recorded_versions(&conn), vec![1, 2]);
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
    ///
    /// Each token must be a single-quoted literal; absent quotes panic
    /// instead of silently round-tripping through `trim_matches`, so a typo
    /// dropping the quotes in the CHECK list fails loudly. Without this
    /// guard, a malformed CHECK could produce values that compare equal to
    /// the enum wire strings and pass the set-equality tests.
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

    /// `deploy_verify_pass` is single-row by construction: the gate is a
    /// tri-state (never-run / passed / bypassed-by-flag), not a history.
    /// `Store::record_deploy_verify_pass` hardcodes `VALUES (1, ...)` and
    /// `check_deploy_verify_pass` reads `WHERE id = 1`, so the table's
    /// `CHECK (id = 1)` is the runtime guard catching any caller that
    /// drifts (e.g. a refactor switching to AUTOINCREMENT). A direct
    /// `INSERT (id=2)` must fail at the SQLite layer.
    #[test]
    fn deploy_verify_pass_check_rejects_non_unit_id() {
        let conn = Connection::open_in_memory().unwrap();
        ensure(&conn).unwrap();
        let err = conn
            .execute(
                "INSERT INTO deploy_verify_pass(id, at, mode, base_ref) \
                 VALUES (2, '2026-01-01T00:00:00Z', 'hermetic', 'main')",
                [],
            )
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("check"),
            "expected CHECK constraint failure, got {err}",
        );
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM deploy_verify_pass", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected row must not have partially landed");
    }

    /// Pin the parser shape used by the two CHECK-set tests against
    /// realistic whitespace variations (`'a','b', 'c' ` — leading-space,
    /// no-space, trailing-space) so a future schema reformat doesn't
    /// silently break extraction.
    #[test]
    fn extract_check_in_list_parses_quoted_literals() {
        let sql = "CREATE TABLE x (\n  c TEXT NOT NULL CHECK (c IN ('a','b', 'c' )),\n  d INT)";
        let got = extract_check_in_list(sql, "c");
        let want: std::collections::HashSet<String> =
            ["a", "b", "c"].into_iter().map(String::from).collect();
        assert_eq!(got, want);
    }

    /// Pin the contract: an IN-list element without enclosing quotes
    /// (e.g. a typo where someone wrote `bare` instead of `'bare'`)
    /// panics rather than silently round-tripping through the
    /// trim/strip path. Without this guard, a malformed CHECK could
    /// produce values that compare equal to the enum wire strings
    /// and pass the set-equality tests.
    #[test]
    #[should_panic(expected = "expected single-quoted SQL literal")]
    fn extract_check_in_list_rejects_unquoted_token() {
        let sql = "CHECK (c IN ('a', bare, 'c'))";
        let _ = extract_check_in_list(sql, "c");
    }
}
