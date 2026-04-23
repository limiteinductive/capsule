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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptOutcome {
    InFlight,
    Released,
    Expired,
    Abandoned,
    Landed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acceptance {
    pub run: String,
    pub expect_exit: ExpectExit,
    pub cwd: Option<String>,
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
    pub tip_sha: Option<Sha>,
    #[serde(with = "time::serde::iso8601")]
    pub last_heartbeat: OffsetDateTime,
    pub outcome: AttemptOutcome,
    #[serde(with = "time::serde::iso8601")]
    pub opened_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601::option")]
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
    pub scope_prefixes: Vec<CanonicalPath>,
    pub base_ref: String,
    pub depends_on: Vec<CapsuleId>,
    pub status: Status,
    pub active_attempt: Option<AttemptId>,
    pub attempts: Vec<Attempt>,
    pub verification: Option<Verification>,
    pub pending_land: Option<PendingLand>,
    pub landing: Option<Landing>,
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,
}

pub const ZERO_OID: &str = "0000000000000000000000000000000000000000";
