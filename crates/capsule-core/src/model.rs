use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::path::CanonicalPath;

pub type CapsuleId = String;
pub type AttemptId = u64;
pub type SessionId = String;
pub type Sha = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Planned,
    Active,
    Accepted,
    Landed,
    Abandoned,
}

impl Status {
    /// Wire-format string for SQL CHECK constraints, JSON output, and CLI
    /// display. Round-trip with `Status::from_wire`.
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Active => "active",
            Self::Accepted => "accepted",
            Self::Landed => "landed",
            Self::Abandoned => "abandoned",
        }
    }

    /// Inverse of `as_wire_str`. Returns `None` for unknown values; callers
    /// reading from the DB or schema-validated input may treat that as
    /// corruption (the SQL CHECK constraint enforces the membership) and
    /// panic, while less-trusted callers can surface a clean error.
    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "planned" => Self::Planned,
            "active" => Self::Active,
            "accepted" => Self::Accepted,
            "landed" => Self::Landed,
            "abandoned" => Self::Abandoned,
            _ => return None,
        })
    }

    /// Terminal: no further state transitions allowed. Used to short-circuit
    /// dep mutations (DESIGN.md §7.1.3 makes them explicit no-ops on terminal)
    /// and other guards.
    ///
    /// Written as an explicit `match` (not `matches!`) so adding a `Status`
    /// variant forces compile-time review of whether it belongs in the
    /// terminal set — a `matches!` arm would silently classify new variants
    /// as non-terminal.
    pub const fn is_terminal(self) -> bool {
        match self {
            Self::Landed | Self::Abandoned => true,
            Self::Planned | Self::Active | Self::Accepted => false,
        }
    }

    /// True iff a capsule in this status has a live lease bound to an
    /// `active_attempt` (DESIGN.md §3.3). Heartbeat and other lease-window
    /// operations gate on this. Same exhaustive-match discipline as
    /// `is_terminal` — a new `Status` variant must be classified explicitly.
    pub const fn holds_lease(self) -> bool {
        match self {
            Self::Active | Self::Accepted => true,
            Self::Planned | Self::Landed | Self::Abandoned => false,
        }
    }

    /// SQL fragment listing the wire strings of statuses for which
    /// `holds_lease()` is true, single-quoted and comma-joined for direct
    /// interpolation into `... WHERE status IN ({Status::HOLDS_LEASE_SQL_IN_LIST}) ...`.
    /// Pinned against `holds_lease` by `status_holds_lease_sql_list_matches_predicate`
    /// — adding a new lease-holding variant without updating this list is a
    /// test failure rather than a runtime miss. Backed by
    /// `holds_lease_sql_in_list!()`, which expands to the same literal and lets
    /// callers `concat!` it into a fully `&'static str` query.
    pub const HOLDS_LEASE_SQL_IN_LIST: &'static str = crate::holds_lease_sql_in_list!();
}

/// Literal form of `Status::HOLDS_LEASE_SQL_IN_LIST` for `concat!` callers
/// (which require token-level string literals). Single source of truth: the
/// const above is defined in terms of this macro.
#[macro_export]
macro_rules! holds_lease_sql_in_list {
    () => {
        "'active','accepted'"
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    InFlight,
    Released,
    Expired,
    Abandoned,
    Landed,
}

impl AttemptOutcome {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::InFlight => "in_flight",
            Self::Released => "released",
            Self::Expired => "expired",
            Self::Abandoned => "abandoned",
            Self::Landed => "landed",
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "in_flight" => Self::InFlight,
            "released" => Self::Released,
            "expired" => Self::Expired,
            "abandoned" => Self::Abandoned,
            "landed" => Self::Landed,
            _ => return None,
        })
    }

    /// True when an attempt has a meaningful `closed_at` and will see no
    /// further state transitions. Same exhaustiveness rationale as
    /// `Status::is_terminal`; the terminal sets intentionally differ
    /// (`AttemptOutcome::Expired` is terminal here, but the capsule's
    /// `Status` is not — an expired attempt is reclaimed and the capsule
    /// re-enters `Planned`).
    pub const fn is_terminal(self) -> bool {
        match self {
            Self::Landed | Self::Abandoned | Self::Expired => true,
            Self::InFlight | Self::Released => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acceptance {
    pub run: String,
    pub expect_exit: ExpectExit,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_sec: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExpectExit {
    Code(i32),
    Sentinel(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub owner: String,
    pub session_id: SessionId,
    #[serde(with = "time::serde::iso8601")]
    pub acquired_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub expires_at: OffsetDateTime,
    /// TTL set at claim. Heartbeat extends `expires_at` by this amount; workers
    /// cannot specify a different TTL post-claim. See DESIGN.md §3.3.
    pub ttl_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub id: AttemptId,
    pub lease: Lease,
    pub branch: String,
    pub witness_branch: String,
    pub base_sha: Sha,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_sha: Option<Sha>,
    #[serde(with = "time::serde::iso8601")]
    pub last_heartbeat: OffsetDateTime,
    pub outcome: AttemptOutcome,
    #[serde(with = "time::serde::iso8601")]
    pub opened_at: OffsetDateTime,
    #[serde(
        default,
        with = "time::serde::iso8601::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub closed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verification {
    #[serde(with = "time::serde::iso8601")]
    pub at: OffsetDateTime,
    pub attestor: SessionId,
    pub attempt_id: AttemptId,
    pub verified_sha: Sha,
    pub command: String,
    pub exit_code: ExitCode,
    pub duration_ms: u64,
    pub log_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExitCode {
    Code(i32),
    Sentinel(String),
}

impl std::fmt::Display for ExitCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Code(n) => write!(f, "{n}"),
            Self::Sentinel(s) => f.write_str(s),
        }
    }
}

/// Parse a CLI-supplied exit-code string: numeric → `Code(n)`, anything else
/// → `Sentinel(s)`. The conversion is total (no `Err`) under the current
/// wire model, since DESIGN §5 lets sentinels carry arbitrary strings (e.g.
/// "timeout", "killed:SIGKILL") and any non-`i32` string falls through to
/// the Sentinel arm. If a future variant constrains sentinels, this impl
/// becomes part of the API surface that must be re-reviewed.
///
/// Round-trip: canonical `i32` spellings (e.g. "0", "-1", "127") round-trip
/// through `Display`; non-numeric sentinels are preserved verbatim. Numeric
/// strings with non-canonical spellings ("01", "+1", "-0") are parsed AND
/// CANONICALIZED — they survive as `Code(n)` whose `Display` produces the
/// canonical form, so they do NOT round-trip byte-identically.
///
/// Takes `String` (not `&str`) so the Sentinel arm avoids a clone — the
/// expected caller is a CLI that already owns its argv string.
impl From<String> for ExitCode {
    fn from(s: String) -> Self {
        match s.parse::<i32>() {
            Ok(n) => Self::Code(n),
            Err(_) => Self::Sentinel(s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingLand {
    #[serde(with = "time::serde::iso8601")]
    pub at: OffsetDateTime,
    pub attempt_id: AttemptId,
    pub verified_sha: Sha,
    pub prior_base_sha: Sha,
    pub witness_branch: String,
    pub lander: String,
}

impl PendingLand {
    /// Promote this pending record to the canonical `Landing` once the atomic
    /// push has been observed to advance the witness ref. `at` is the land
    /// commit time; `advanced_base_ref` is `verified_sha != prior_base_sha`
    /// at the moment the push observed the remote. `landed_by` is the actor
    /// that finalized the landing — usually `self.lander`, but the reconciler
    /// (or an operator on `force_unfreeze`) records itself instead.
    pub fn into_landing(
        self,
        at: OffsetDateTime,
        advanced_base_ref: bool,
        landed_by: String,
    ) -> Landing {
        Landing {
            at,
            landed_sha: self.verified_sha,
            prior_base_sha: self.prior_base_sha,
            landed_by,
            attempt_id: self.attempt_id,
            witness_branch: self.witness_branch,
            advanced_base_ref,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Landing {
    #[serde(with = "time::serde::iso8601")]
    pub at: OffsetDateTime,
    pub landed_sha: Sha,
    pub prior_base_sha: Sha,
    pub landed_by: String,
    pub attempt_id: AttemptId,
    pub witness_branch: String,
    pub advanced_base_ref: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capsule {
    pub id: CapsuleId,
    pub title: String,
    pub description: String,
    pub acceptance: Acceptance,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope_prefixes: Vec<CanonicalPath>,
    pub base_ref: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<CapsuleId>,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_attempt: Option<AttemptId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<Attempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<Verification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_land: Option<PendingLand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landing: Option<Landing>,
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,
}

impl Capsule {
    /// Return the `Attempt` row whose id equals `active_attempt`, or `None` if
    /// the capsule has no active attempt. A `Some(active_attempt)` for which
    /// no attempt row exists is a state-shape violation and also yields
    /// `None` here — call sites that need to distinguish "nothing claimed"
    /// from "corrupt state" should re-check `active_attempt.is_some()` after
    /// receiving `None`.
    pub fn active_attempt_record(&self) -> Option<&Attempt> {
        let aid = self.active_attempt?;
        self.attempts.iter().find(|a| a.id == aid)
    }

    /// Consuming sibling of `active_attempt_record`. Returns the matching
    /// `Attempt` by value so callers can move owned fields out (branch,
    /// base_sha, etc.) without cloning. Same `None` cases as the borrowing
    /// form: no `active_attempt`, or `active_attempt` points at a missing row.
    pub fn into_active_attempt(mut self) -> Option<Attempt> {
        let aid = self.active_attempt?;
        let pos = self.attempts.iter().position(|a| a.id == aid)?;
        Some(self.attempts.swap_remove(pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ExpectExit` is `#[serde(untagged)]` — the wire form is a bare
    /// JSON number or a bare JSON string (no `{kind: ...}` tag). Pin
    /// the contract: numbers → `Code`, strings → `Sentinel` (including
    /// the numeric-string `"0"`, which `Code(i32)` rejects). A future
    /// move to a tagged or stringly-typed wire form would silently
    /// flip the decision for `"0"` and is the regression this guards.
    #[test]
    fn expect_exit_untagged_serde_round_trip() {
        let code: ExpectExit = serde_json::from_str("0").unwrap();
        assert!(matches!(code, ExpectExit::Code(0)));
        assert_eq!(serde_json::to_string(&code).unwrap(), "0");

        let sentinel: ExpectExit = serde_json::from_str("\"timeout\"").unwrap();
        match sentinel {
            ExpectExit::Sentinel(ref s) => assert_eq!(s, "timeout"),
            ExpectExit::Code(_) => panic!("expected Sentinel"),
        }
        assert_eq!(serde_json::to_string(&sentinel).unwrap(), "\"timeout\"");

        let numeric_string: ExpectExit = serde_json::from_str("\"0\"").unwrap();
        match numeric_string {
            ExpectExit::Sentinel(ref s) => assert_eq!(s, "0"),
            ExpectExit::Code(_) => panic!("expected Sentinel for \"0\""),
        }
    }

    /// `ExitCode` is `#[serde(untagged)]` and rides through SQLite inside
    /// `verification_json` — the on-disk audit record (DESIGN §6). The
    /// `ExpectExit` test above pins the same contract for the spec side;
    /// this one pins it for the persisted side, where dropping `untagged`
    /// would make existing bare-scalar audit rows unreadable and change
    /// future row shape. Numbers → `Code`, strings → `Sentinel` (including
    /// the numeric-string `"0"`).
    #[test]
    fn exit_code_untagged_serde_round_trip() {
        let code: ExitCode = serde_json::from_str("0").unwrap();
        assert!(matches!(code, ExitCode::Code(0)));
        assert_eq!(serde_json::to_string(&code).unwrap(), "0");

        let sentinel: ExitCode = serde_json::from_str("\"timeout\"").unwrap();
        match sentinel {
            ExitCode::Sentinel(ref s) => assert_eq!(s, "timeout"),
            ExitCode::Code(_) => panic!("expected Sentinel"),
        }
        assert_eq!(serde_json::to_string(&sentinel).unwrap(), "\"timeout\"");

        let numeric_string: ExitCode = serde_json::from_str("\"0\"").unwrap();
        match numeric_string {
            ExitCode::Sentinel(ref s) => assert_eq!(s, "0"),
            ExitCode::Code(_) => panic!("expected Sentinel for \"0\""),
        }
        assert_eq!(serde_json::to_string(&numeric_string).unwrap(), "\"0\"");
    }

    #[test]
    fn exit_code_display() {
        assert_eq!(ExitCode::Code(0).to_string(), "0");
        assert_eq!(ExitCode::Code(-1).to_string(), "-1");
        assert_eq!(ExitCode::Sentinel("timeout".into()).to_string(), "timeout");
    }

    #[test]
    fn exit_code_from_string_parses_numeric_as_code() {
        let parsed: ExitCode = "0".to_string().into();
        assert!(matches!(parsed, ExitCode::Code(0)));
        let parsed: ExitCode = "-1".to_string().into();
        assert!(matches!(parsed, ExitCode::Code(-1)));
        let parsed: ExitCode = "127".to_string().into();
        assert!(matches!(parsed, ExitCode::Code(127)));
    }

    /// Anything that does not parse as `i32` becomes a `Sentinel`, verbatim
    /// — CLIs lean on this for sentinels like "timeout" (DESIGN §5). The
    /// failing arm names `ExitCode::Code(_)` exactly (not a wildcard) so a
    /// future variant added to `ExitCode` forces compile-time review here,
    /// same exhaustiveness discipline as `Status::is_terminal` /
    /// `as_wire_str`. Numeric overflow (e.g. `i32::MAX + 1`) is also covered
    /// — it does not parse, so it falls into `Sentinel` too.
    #[test]
    fn exit_code_from_string_falls_back_to_sentinel() {
        let parsed: ExitCode = "timeout".to_string().into();
        match parsed {
            ExitCode::Sentinel(s) => assert_eq!(s, "timeout"),
            other @ ExitCode::Code(_) => panic!("expected Sentinel, got {other:?}"),
        }
        let big = "2147483648".to_string();
        let parsed: ExitCode = big.clone().into();
        match parsed {
            ExitCode::Sentinel(s) => assert_eq!(s, big),
            other @ ExitCode::Code(_) => panic!("expected Sentinel, got {other:?}"),
        }
    }

    /// Pin the narrowed contract: canonical `i32` spellings and non-numeric
    /// sentinels round-trip through `From<String>` → `Display`. CLIs that
    /// emit canonical `i32` output depend on this.
    #[test]
    fn exit_code_canonical_strings_round_trip() {
        for s in ["0", "-1", "127", "timeout", "killed:SIGKILL"] {
            let ec: ExitCode = s.to_string().into();
            assert_eq!(ec.to_string(), s);
        }
    }

    /// Pin the doc-comment's "parsed AND canonicalized" clause: numeric
    /// strings with non-canonical spellings survive as `Code(n)` whose
    /// `Display` produces the canonical form. The String → ExitCode →
    /// Display round-trip is therefore NOT byte-identical for these inputs —
    /// intentional, since storage and audit consumers prefer one canonical
    /// spelling per integer.
    #[test]
    fn exit_code_non_canonical_numerics_canonicalize() {
        let cases = [("01", "1"), ("+1", "1"), ("-0", "0"), ("007", "7")];
        for (input, canonical) in cases {
            let ec: ExitCode = input.to_string().into();
            assert!(matches!(ec, ExitCode::Code(_)));
            assert_eq!(ec.to_string(), canonical);
        }
    }

    /// Pin BOTH the (variant ↔ wire string) bijection AND the literal
    /// spelling of each wire string. A composed `from_wire(as_wire_str(v))`
    /// round-trip would still pass if both functions silently agreed on a
    /// bogus spelling — explicit `(variant, "wire")` tuples are the
    /// spelling oracle for SQL CHECK strings (schema.rs) and audit-log
    /// payload consumers (DESIGN §6).
    #[test]
    fn status_wire_table_pinned() {
        let cases = [
            (Status::Planned, "planned"),
            (Status::Active, "active"),
            (Status::Accepted, "accepted"),
            (Status::Landed, "landed"),
            (Status::Abandoned, "abandoned"),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
            assert_eq!(Status::from_wire(wire), Some(v));
        }
        assert_eq!(Status::from_wire("not_a_status"), None);
    }

    #[test]
    fn attempt_outcome_wire_table_pinned() {
        let cases = [
            (AttemptOutcome::InFlight, "in_flight"),
            (AttemptOutcome::Released, "released"),
            (AttemptOutcome::Expired, "expired"),
            (AttemptOutcome::Abandoned, "abandoned"),
            (AttemptOutcome::Landed, "landed"),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
            assert_eq!(AttemptOutcome::from_wire(wire), Some(v));
        }
        assert_eq!(AttemptOutcome::from_wire("not_an_outcome"), None);
    }

    /// Wire parsing is intentionally case-sensitive. CLI filters
    /// (`parse_status_arg`) must reject non-canonical values instead of
    /// accepting `"Active"` and later constructing a `WHERE status = 'Active'`
    /// query that can never match the lowercase schema values.
    #[test]
    fn from_wire_is_case_sensitive() {
        for s in ["Active", "PLANNED", "Accepted"] {
            assert!(Status::from_wire(s).is_none(), "Status::from_wire({s:?})");
        }
        for s in ["In_Flight", "IN_FLIGHT", "Expired"] {
            assert!(
                AttemptOutcome::from_wire(s).is_none(),
                "AttemptOutcome::from_wire({s:?})",
            );
        }
    }

    /// The four tests below pin set-membership predicates over `Status` and
    /// `AttemptOutcome`. Each predicate is an exhaustive match — adding a
    /// future variant fails compile until it's classified, so these tests
    /// don't catch *missing* classification; they catch *wrong* classification
    /// (a refactor that flips a bit). Store guards rely on these splits:
    /// `Status::is_terminal` (dep mutations), `Status::holds_lease`
    /// (`HOLDS_LEASE_SQL_IN_LIST`, the SQL `status IN (...)` filter), and
    /// `AttemptOutcome::is_terminal` (closed_at presence).
    #[test]
    fn status_terminal_set_pinned() {
        assert!(Status::Landed.is_terminal());
        assert!(Status::Abandoned.is_terminal());
        assert!(!Status::Planned.is_terminal());
        assert!(!Status::Active.is_terminal());
        assert!(!Status::Accepted.is_terminal());
    }

    #[test]
    fn status_lease_set_pinned() {
        assert!(Status::Active.holds_lease());
        assert!(Status::Accepted.holds_lease());
        assert!(!Status::Planned.holds_lease());
        assert!(!Status::Landed.holds_lease());
        assert!(!Status::Abandoned.holds_lease());
    }

    /// Derive the SQL fragment from the predicate and assert it equals the
    /// hand-written const, so the SQL bind can never drift from `holds_lease`.
    #[test]
    fn status_holds_lease_sql_list_matches_predicate() {
        let computed = [
            Status::Planned,
            Status::Active,
            Status::Accepted,
            Status::Landed,
            Status::Abandoned,
        ]
        .into_iter()
        .filter(|s| s.holds_lease())
        .map(|s| format!("'{}'", s.as_wire_str()))
        .collect::<Vec<_>>()
        .join(",");
        assert_eq!(computed, Status::HOLDS_LEASE_SQL_IN_LIST);
    }

    #[test]
    fn attempt_outcome_terminal_set_pinned() {
        assert!(AttemptOutcome::Landed.is_terminal());
        assert!(AttemptOutcome::Abandoned.is_terminal());
        assert!(AttemptOutcome::Expired.is_terminal());
        assert!(!AttemptOutcome::InFlight.is_terminal());
        assert!(!AttemptOutcome::Released.is_terminal());
    }

    /// Two parallel wire forms exist for `Status` and `AttemptOutcome`:
    /// serde enum serialization (`rename_all = ...`), used wherever the
    /// enum is emitted as JSON (e.g. via the `Capsule`/`Attempt` derives
    /// on the CLI `--json` path), and the explicit `as_wire_str`, used
    /// at SQL boundaries (`capsule.status` / `attempt.outcome` TEXT
    /// columns and CHECK constraints). The two must agree both ways,
    /// or JSON-emitted state drifts from `WHERE status = '...'` queries
    /// — and a `#[serde(alias = ...)]` / asymmetric `deserialize_with`
    /// could let in values the SQL CHECK would refuse. Pin Serialize
    /// and Deserialize every variant so a `rename_all` change, a
    /// per-variant `#[serde(rename)]`, or an asymmetric attr surfaces
    /// here rather than at runtime.
    #[test]
    fn status_serde_form_matches_as_wire_str() {
        for v in [
            Status::Planned,
            Status::Active,
            Status::Accepted,
            Status::Landed,
            Status::Abandoned,
        ] {
            let expected = serde_json::Value::String(v.as_wire_str().into());
            assert_eq!(serde_json::to_value(v).unwrap(), expected, "ser for {v:?}");
            assert_eq!(
                serde_json::from_value::<Status>(expected).unwrap(),
                v,
                "de for {v:?}",
            );
        }
    }

    #[test]
    fn attempt_outcome_serde_form_matches_as_wire_str() {
        for v in [
            AttemptOutcome::InFlight,
            AttemptOutcome::Released,
            AttemptOutcome::Expired,
            AttemptOutcome::Abandoned,
            AttemptOutcome::Landed,
        ] {
            let expected = serde_json::Value::String(v.as_wire_str().into());
            assert_eq!(serde_json::to_value(v).unwrap(), expected, "ser for {v:?}");
            assert_eq!(
                serde_json::from_value::<AttemptOutcome>(expected).unwrap(),
                v,
                "de for {v:?}",
            );
        }
    }

    fn synthetic_attempt(id: AttemptId) -> Attempt {
        let now = OffsetDateTime::UNIX_EPOCH;
        Attempt {
            id,
            lease: Lease {
                owner: "o".into(),
                session_id: "s".into(),
                acquired_at: now,
                expires_at: now,
                ttl_sec: 60,
            },
            branch: "b".into(),
            witness_branch: "w".into(),
            base_sha: "0".repeat(40),
            tip_sha: None,
            last_heartbeat: now,
            outcome: AttemptOutcome::InFlight,
            opened_at: now,
            closed_at: None,
        }
    }

    fn synthetic_capsule(active_attempt: Option<AttemptId>, attempts: Vec<Attempt>) -> Capsule {
        let now = OffsetDateTime::UNIX_EPOCH;
        Capsule {
            id: "c".into(),
            title: "t".into(),
            description: "d".into(),
            acceptance: Acceptance {
                run: "true".into(),
                expect_exit: ExpectExit::Code(0),
                cwd: None,
                timeout_sec: None,
            },
            scope_prefixes: vec![],
            base_ref: "main".into(),
            depends_on: vec![],
            status: Status::Planned,
            active_attempt,
            attempts,
            verification: None,
            pending_land: None,
            landing: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn active_attempt_record_returns_matching_row() {
        let cap = synthetic_capsule(Some(2), vec![synthetic_attempt(1), synthetic_attempt(2)]);
        let att = cap.active_attempt_record().expect("attempt 2 present");
        assert_eq!(att.id, 2);
    }

    #[test]
    fn active_attempt_record_none_when_unset() {
        let cap = synthetic_capsule(None, vec![synthetic_attempt(1)]);
        assert!(cap.active_attempt_record().is_none());
    }

    /// State-shape violation: `active_attempt` points at an id with no row.
    /// The doc-comment promises `None` here; pin so a future refactor that
    /// panics instead would fail this test.
    #[test]
    fn active_attempt_record_none_when_missing_row() {
        let cap = synthetic_capsule(Some(99), vec![synthetic_attempt(1)]);
        assert!(cap.active_attempt_record().is_none());
    }

    /// Symmetric to `active_attempt_record_returns_matching_row` for the
    /// consuming sibling. Pinned separately because `into_active_attempt`
    /// uses `swap_remove` (O(1) but reorders), whereas `active_attempt_record`
    /// scans linearly — the two could drift independently.
    ///
    /// The active attempt sits in the *middle* (id 2 of `[1,2,3]`) so a
    /// hypothetically broken `pop()` or `first()`-style impl would fail this
    /// test rather than coincidentally pass. The body also moves a non-`Copy`
    /// field (`branch`) out of the returned `Attempt`, exercising the by-value
    /// contract at the call site (the signature alone proves the type, not
    /// that callers can move owned fields without cloning).
    #[test]
    fn into_active_attempt_returns_matching_row_by_value() {
        let mut active = synthetic_attempt(2);
        active.branch = "active-branch".into();
        let cap = synthetic_capsule(
            Some(2),
            vec![synthetic_attempt(1), active, synthetic_attempt(3)],
        );
        let att = cap.into_active_attempt().expect("attempt 2 present");
        assert_eq!(att.id, 2);
        let branch: String = att.branch;
        assert_eq!(branch, "active-branch");
    }

    #[test]
    fn into_active_attempt_none_when_unset() {
        let cap = synthetic_capsule(None, vec![synthetic_attempt(1)]);
        assert!(cap.into_active_attempt().is_none());
    }

    /// Same state-shape violation as the borrowing form: `active_attempt`
    /// points at an id with no row → `None`, not panic.
    #[test]
    fn into_active_attempt_none_when_missing_row() {
        let cap = synthetic_capsule(Some(99), vec![synthetic_attempt(1)]);
        assert!(cap.into_active_attempt().is_none());
    }

    /// `PendingLand::into_landing` is the §7.1.2 step-4 promotion: it
    /// moves verified_sha / prior_base_sha / attempt_id / witness_branch
    /// into Landing, takes `at` / `advanced_base_ref` / `landed_by` as
    /// arguments, and drops `self.lander` (intentional — the reconciler
    /// or operator on `force_unfreeze` records itself as `landed_by`,
    /// not the original lander). Pin the move-and-drop contract so a
    /// future re-add of `lander` to Landing is a deliberate change.
    #[test]
    fn pending_land_into_landing_carries_fields_and_drops_lander() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let verified = "v".repeat(40);
        let prior = "p".repeat(40);
        let pending = PendingLand {
            at: now,
            attempt_id: 7,
            verified_sha: verified.clone(),
            prior_base_sha: prior.clone(),
            witness_branch: "capsule-witness/foo/a7".into(),
            lander: "worker-A".into(),
        };
        let land_at = now + time::Duration::seconds(5);
        let landing = pending.into_landing(land_at, true, "reconciler".into());
        assert_eq!(landing.at, land_at);
        assert_eq!(landing.landed_sha, verified);
        assert_eq!(landing.prior_base_sha, prior);
        assert_eq!(landing.attempt_id, 7);
        assert_eq!(landing.witness_branch, "capsule-witness/foo/a7");
        assert!(landing.advanced_base_ref);
        assert_eq!(landing.landed_by, "reconciler");
    }

    /// `advanced_base_ref` is the caller's post-push observation, not derived
    /// from `verified_sha != prior_base_sha`. Pins the case missed by
    /// `_carries_fields_and_drops_lander`, where distinct SHAs plus
    /// `advanced=true` would still pass an implementation that inferred
    /// the flag.
    #[test]
    fn pending_land_into_landing_honors_advanced_arg_not_field_diff() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let pending = PendingLand {
            at: now,
            attempt_id: 1,
            verified_sha: "v".repeat(40),
            prior_base_sha: "p".repeat(40),
            witness_branch: "w".into(),
            lander: "l".into(),
        };
        let landing = pending.into_landing(now, false, "r".into());
        assert!(
            !landing.advanced_base_ref,
            "advanced_base_ref must come from the arg, not from verified != prior",
        );
    }

    /// `Attempt.tip_sha` (None until first push) and `closed_at` (None for
    /// in-flight) are routinely None at every `--json` render. Pin the
    /// shape: `skip_serializing_if` omits the keys for None (agent output
    /// stays minimal), and `default` lets the omitted-key form deserialize
    /// back to None.
    #[test]
    fn attempt_json_omits_and_defaults_none_optionals() {
        let att = synthetic_attempt(1);
        assert!(att.tip_sha.is_none() && att.closed_at.is_none());
        let v = serde_json::to_value(&att).unwrap();
        let obj = v.as_object().expect("attempt serializes as JSON object");
        assert!(!obj.contains_key("tip_sha"), "got: {v}");
        assert!(!obj.contains_key("closed_at"), "got: {v}");

        let parsed: Attempt = serde_json::from_value(v).unwrap();
        assert!(parsed.tip_sha.is_none());
        assert!(parsed.closed_at.is_none());
    }

    /// `Lease::acquired_at` and `expires_at` use `time::serde::iso8601`,
    /// which emits a JSON *string* with a 6-digit padded year prefix
    /// (`"+0YYYYY-MM-DDTHH:MM:SS.fffffffffZ"`). Dropping the annotation
    /// silently falls back to the time crate's default — a bare JSON
    /// array of struct fields — and the persisted `lease_json` contract
    /// breaks: SQLite `json_extract($.expires_at)` returns a JSON array
    /// instead of a string, which downstream `parse_iso8601` then panics
    /// on. Pin the field shape (string + canonical bytes) and the value
    /// round-trip so the regression surfaces here.
    ///
    /// The `+002024` prefix is distinct from capsule-store's
    /// `format_iso8601` helper (4-digit year) — both formats are accepted
    /// by `parse_iso8601`. Don't "normalize" them.
    #[test]
    fn lease_serde_iso8601_round_trip() {
        let lease = Lease {
            owner: "alice".into(),
            session_id: "session-1".into(),
            acquired_at: time::macros::datetime!(2024-06-15 12:00:00 UTC),
            expires_at: time::macros::datetime!(2024-06-15 12:05:00 UTC),
            ttl_sec: 300,
        };
        let v: serde_json::Value = serde_json::to_value(&lease).unwrap();
        assert_eq!(v["acquired_at"], "+002024-06-15T12:00:00.000000000Z");
        assert_eq!(v["expires_at"], "+002024-06-15T12:05:00.000000000Z");
        let parsed: Lease = serde_json::from_value(v).unwrap();
        assert_eq!(parsed.owner, lease.owner);
        assert_eq!(parsed.session_id, lease.session_id);
        assert_eq!(parsed.acquired_at, lease.acquired_at);
        assert_eq!(parsed.expires_at, lease.expires_at);
        assert_eq!(parsed.ttl_sec, lease.ttl_sec);
    }

    /// `Capsule` carries seven optional fields tagged
    /// `#[serde(default, skip_serializing_if = ...)]`. The two attributes
    /// pin two independent invariants — split into two tests so a refactor
    /// that drops one but not the other surfaces precisely.
    ///
    /// (a) `skip_serializing_if`: producer omits the key when empty/None,
    ///     keeping hand-authored / CLI-emitted capsule JSON minimal.
    /// (b) `default`: consumer accepts JSON missing those keys, so a
    ///     hand-authored minimal capsule and a future schema-migration
    ///     read of an older record both deserialize cleanly.
    #[test]
    fn capsule_json_omits_empty_optionals() {
        let cap = synthetic_capsule(None, vec![]);
        let v = serde_json::to_value(&cap).unwrap();
        let obj = v.as_object().expect("capsule serializes as JSON object");
        for key in [
            "scope_prefixes",
            "depends_on",
            "active_attempt",
            "attempts",
            "verification",
            "pending_land",
            "landing",
        ] {
            assert!(
                !obj.contains_key(key),
                "expected {key} omitted from minimal capsule JSON, got {v}",
            );
        }
        for required in [
            "id",
            "title",
            "description",
            "acceptance",
            "base_ref",
            "status",
            "created_at",
            "updated_at",
        ] {
            assert!(
                obj.contains_key(required),
                "expected required key {required} present, got {v}",
            );
        }
    }

    /// Hand-typed minimal JSON missing the seven optional keys must
    /// deserialize via `#[serde(default)]` — distinct from the omits
    /// test, which only serializes (and so would pass even if `default`
    /// were removed). Pin defaults for every optional so dropping
    /// `default` from any one field fails here.
    #[test]
    fn capsule_json_defaults_missing_optionals() {
        let json = serde_json::json!({
            "id": "c",
            "title": "t",
            "description": "d",
            "acceptance": {"run": "true", "expect_exit": 0},
            "base_ref": "main",
            "status": "planned",
            "created_at": "+001970-01-01T00:00:00.000000000Z",
            "updated_at": "+001970-01-01T00:00:00.000000000Z",
        });
        let parsed: Capsule = serde_json::from_value(json).unwrap();
        assert!(parsed.scope_prefixes.is_empty());
        assert!(parsed.depends_on.is_empty());
        assert!(parsed.active_attempt.is_none());
        assert!(parsed.attempts.is_empty());
        assert!(parsed.verification.is_none());
        assert!(parsed.pending_land.is_none());
        assert!(parsed.landing.is_none());
    }

    /// `Acceptance` is a standalone public JSON shape; minimal hand-authored
    /// JSON only requires `run` and `expect_exit`. Same split-discipline as
    /// the Capsule pair: serialize-omit and deserialize-default each pin one
    /// of the two paired serde attributes (`skip_serializing_if`, `default`)
    /// independently, so dropping one does not pass the other.
    #[test]
    fn acceptance_json_omits_empty_optionals() {
        let acc = Acceptance {
            run: "true".into(),
            expect_exit: ExpectExit::Code(0),
            cwd: None,
            timeout_sec: None,
        };
        assert_eq!(
            serde_json::to_value(&acc).unwrap(),
            serde_json::json!({"run": "true", "expect_exit": 0}),
        );
    }

    #[test]
    fn acceptance_json_defaults_missing_optionals() {
        let minimal = serde_json::json!({"run": "true", "expect_exit": 0});
        let parsed: Acceptance = serde_json::from_value(minimal).unwrap();
        assert_eq!(parsed.run, "true");
        assert!(matches!(parsed.expect_exit, ExpectExit::Code(0)));
        assert!(parsed.cwd.is_none());
        assert!(parsed.timeout_sec.is_none());
    }
}

