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
    /// test failure rather than a runtime miss.
    pub const HOLDS_LEASE_SQL_IN_LIST: &'static str = "'active','accepted'";
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

    #[test]
    fn exit_code_from_string_falls_back_to_sentinel() {
        // Pin: anything that does not parse as `i32` becomes a Sentinel,
        // verbatim. CLIs lean on this for sentinel values like "timeout"
        // (DESIGN §5). The non-target arm names `ExitCode::Code(_)` exactly
        // (not a wildcard) so a future variant added to `ExitCode` forces a
        // compile-time review here — same exhaustiveness discipline as
        // `Status::is_terminal` / `as_wire_str`.
        let parsed: ExitCode = "timeout".to_string().into();
        match parsed {
            ExitCode::Sentinel(s) => assert_eq!(s, "timeout"),
            other @ ExitCode::Code(_) => panic!("expected Sentinel, got {other:?}"),
        }
        // Numeric overflow falls into Sentinel: i32::MAX + 1 doesn't parse.
        let big = "2147483648".to_string();
        let parsed: ExitCode = big.clone().into();
        match parsed {
            ExitCode::Sentinel(s) => assert_eq!(s, big),
            other @ ExitCode::Code(_) => panic!("expected Sentinel, got {other:?}"),
        }
    }

    #[test]
    fn exit_code_canonical_strings_round_trip() {
        // Pin the narrowed contract: canonical i32 spellings and non-numeric
        // sentinels round-trip through `From<String>` → `Display`. This is
        // what CLIs producing canonical i32 output rely on.
        for s in ["0", "-1", "127", "timeout", "killed:SIGKILL"] {
            let ec: ExitCode = s.to_string().into();
            assert_eq!(ec.to_string(), s);
        }
    }

    #[test]
    fn exit_code_non_canonical_numerics_canonicalize() {
        // Pin the doc-comment's "parsed AND canonicalized" clause: numeric
        // strings with non-canonical spellings survive as Code(n) whose
        // Display produces the canonical form. The String→ExitCode→Display
        // round-trip is therefore NOT byte-identical for these inputs —
        // intentional, since storage and audit consumers prefer one
        // canonical spelling per integer.
        let cases = [("01", "1"), ("+1", "1"), ("-0", "0"), ("007", "7")];
        for (input, canonical) in cases {
            let ec: ExitCode = input.to_string().into();
            assert!(matches!(ec, ExitCode::Code(_)));
            assert_eq!(ec.to_string(), canonical);
        }
    }

    #[test]
    fn status_wire_table_pinned() {
        // Pin BOTH the (variant ↔ wire string) bijection AND the literal
        // spelling of each wire string. A composed `from_wire(as_wire_str(v))`
        // round-trip would still pass if both functions silently agreed on a
        // bogus spelling — explicit `(variant, "wire")` tuples are the
        // spelling oracle for SQL CHECK strings (schema.rs) and audit-log
        // payload consumers (DESIGN §6).
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

    #[test]
    fn active_attempt_record_none_when_missing_row() {
        // State-shape violation: active_attempt points at an id with no row.
        // Doc-comment promises None here; pin so future refactors that
        // panic instead would fail this test.
        let cap = synthetic_capsule(Some(99), vec![synthetic_attempt(1)]);
        assert!(cap.active_attempt_record().is_none());
    }
}

