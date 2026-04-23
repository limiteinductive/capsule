//! SQLite-backed capsule store. See `DESIGN.md` §4 (data model) and §7.1 (protocols).

pub mod schema;

use std::path::{Path, PathBuf};

use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, Capsule, CapsuleId, Status};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json as json;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] json::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("time format: {0}")]
    TimeFormat(#[from] time::error::Format),
    #[error("capsule {0} not found")]
    NotFound(CapsuleId),
    #[error("capsule {0} already exists")]
    DuplicateId(CapsuleId),
    #[error("invalid capsule id {0}: {1}")]
    InvalidId(CapsuleId, String),
    #[error("capsule {0} not claimable: status={1}")]
    NotClaimable(CapsuleId, &'static str),
    #[error("capsule {0} has unmet deps: {1:?}")]
    UnmetDeps(CapsuleId, Vec<CapsuleId>),
    #[error("capsule {0} scope overlaps in-flight capsule {1}")]
    ScopeConflict(CapsuleId, CapsuleId),
    #[error("session does not match active attempt lease")]
    CrossSession,
    #[error("lease expired at {0}")]
    LeaseExpired(String),
    #[error("capsule has pending_land — reclaim/claim frozen until reconciled")]
    PendingLandFrozen,
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct Store {
    conn: Connection,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl Store {
    /// Open or create the store at `db_path`. Idempotent — applies any
    /// pending schema migrations on open. Used by both `init` and the
    /// per-command open path.
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(&db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        schema::ensure(&conn)?;
        Ok(Self { conn, db_path })
    }

    /// Create a new capsule. Caller supplies the id (typically a uuid). All
    /// fields validated; status starts at `planned`.
    pub fn create_capsule(&mut self, c: NewCapsule) -> Result<Capsule> {
        capsule_core::id::validate(&c.id)
            .map_err(|e| StoreError::InvalidId(c.id.clone(), e.to_string()))?;
        let now = OffsetDateTime::now_utc();
        let capsule = Capsule {
            id: c.id.clone(),
            title: c.title,
            description: c.description,
            acceptance: c.acceptance,
            scope_prefixes: c.scope_prefixes,
            base_ref: c.base_ref,
            depends_on: c.depends_on,
            status: Status::Planned,
            active_attempt: None,
            attempts: vec![],
            verification: None,
            pending_land: None,
            landing: None,
            created_at: now,
            updated_at: now,
        };

        let tx = self.conn.transaction()?;

        let exists: bool = tx
            .query_row(
                "SELECT 1 FROM capsule WHERE id = ?1",
                params![capsule.id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if exists {
            return Err(StoreError::DuplicateId(capsule.id.clone()));
        }

        let now_str = format_iso8601(now)?;
        tx.execute(
            "INSERT INTO capsule (
                id, title, description, acceptance_json, scope_json, base_ref,
                depends_on_json, status, active_attempt, verification_json,
                pending_land_json, landing_json, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'planned', NULL, NULL, NULL, NULL, ?8, ?8)",
            params![
                capsule.id,
                capsule.title,
                capsule.description,
                json::to_string(&capsule.acceptance)?,
                json::to_string(&capsule.scope_prefixes)?,
                capsule.base_ref,
                json::to_string(&capsule.depends_on)?,
                now_str,
            ],
        )?;

        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, NULL, 'system', 'capsule_created', ?3)",
            params![
                now_str,
                capsule.id,
                json::to_string(&CreatedPayload {
                    acceptance: &capsule.acceptance,
                    scope_prefixes: &capsule.scope_prefixes,
                    base_ref: &capsule.base_ref,
                    depends_on: &capsule.depends_on,
                })?,
            ],
        )?;

        tx.commit()?;
        Ok(capsule)
    }

    pub fn list_capsules(&mut self, filter: ListFilter) -> Result<Vec<Capsule>> {
        let now = OffsetDateTime::now_utc();
        let tx = self.conn.transaction()?;
        reclaim_expired_in_tx(&tx, now)?;

        let mut q = String::from(
            "SELECT id, title, description, acceptance_json, scope_json, base_ref,
                    depends_on_json, status, active_attempt, verification_json,
                    pending_land_json, landing_json, created_at, updated_at
             FROM capsule",
        );
        let mut conds: Vec<String> = vec![];
        if let Some(s) = filter.status {
            conds.push(format!("status = '{}'", status_to_str(s)));
        }
        if !conds.is_empty() {
            q.push_str(" WHERE ");
            q.push_str(&conds.join(" AND "));
        }
        q.push_str(" ORDER BY created_at ASC");

        let mut stmt = tx.prepare(&q)?;
        let rows = stmt
            .query_map([], |r| {
                Ok(RowCapsule {
                    id: r.get(0)?,
                    title: r.get(1)?,
                    description: r.get(2)?,
                    acceptance_json: r.get(3)?,
                    scope_json: r.get(4)?,
                    base_ref: r.get(5)?,
                    depends_on_json: r.get(6)?,
                    status: r.get(7)?,
                    active_attempt: r.get(8)?,
                    verification_json: r.get(9)?,
                    pending_land_json: r.get(10)?,
                    landing_json: r.get(11)?,
                    created_at: r.get(12)?,
                    updated_at: r.get(13)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        let mut capsules: Vec<Capsule> = rows
            .into_iter()
            .map(|r| r.into_capsule(&tx))
            .collect::<Result<Vec<_>>>()?;

        // Status of dependencies — needed for `--available`.
        if filter.available {
            let landed_ids: std::collections::HashSet<String> = tx
                .prepare("SELECT id FROM capsule WHERE status = 'landed'")?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;
            let in_flight_scopes: Vec<(String, Vec<CanonicalPath>)> = tx
                .prepare(
                    "SELECT id, scope_json FROM capsule
                     WHERE status IN ('active','accepted')",
                )?
                .query_map([], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .map(|(id, j)| Ok::<_, json::Error>((id, json::from_str(&j)?)))
                .collect::<std::result::Result<Vec<_>, _>>()?;

            capsules.retain(|c| {
                if c.status != Status::Planned {
                    return false;
                }
                if !c.depends_on.iter().all(|d| landed_ids.contains(d)) {
                    return false;
                }
                for (other_id, other_scope) in &in_flight_scopes {
                    if other_id == &c.id {
                        continue;
                    }
                    for a in &c.scope_prefixes {
                        for b in other_scope {
                            if a.overlaps(b) {
                                return false;
                            }
                        }
                    }
                }
                true
            });
        }

        if let Some(probe) = &filter.scope_overlaps {
            capsules.retain(|c| c.scope_prefixes.iter().any(|p| p.overlaps(probe)));
        }

        tx.commit()?;
        Ok(capsules)
    }

    /// Atomic claim. See DESIGN.md §7.1.1.
    /// Returns the new `Attempt` on success.
    pub fn claim(&mut self, req: ClaimRequest) -> Result<capsule_core::Attempt> {
        use capsule_core::{Attempt, AttemptOutcome, Lease};

        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;
        let lease_ttl = time::Duration::seconds(req.lease_ttl_sec as i64);
        let expires = now + lease_ttl;
        let expires_str = format_iso8601(expires)?;

        let tx = self.conn.transaction()?;

        // Reclaim every expired lease across the store before evaluating this
        // claim. Skips capsules with pending_land != null (§7.2 reclaim freeze).
        reclaim_expired_in_tx(&tx, now)?;

        // Re-read after reclaim.
        let (status_str, _active_attempt, pending, depends_on_json, scope_json): (
            String, Option<i64>, Option<String>, String, String,
        ) = tx
            .query_row(
                "SELECT status, active_attempt, pending_land_json, depends_on_json, scope_json
                 FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;

        if pending.is_some() {
            return Err(StoreError::PendingLandFrozen);
        }

        let status = parse_status(&status_str);
        if status != Status::Planned {
            return Err(StoreError::NotClaimable(
                req.capsule_id.clone(),
                status_to_str(status),
            ));
        }

        // §7.1.1 step 3: deps must be landed.
        let depends_on: Vec<String> = json::from_str(&depends_on_json)?;
        if !depends_on.is_empty() {
            let mut unmet = vec![];
            for dep in &depends_on {
                let s: Option<String> = tx
                    .query_row(
                        "SELECT status FROM capsule WHERE id = ?1",
                        params![dep],
                        |r| r.get(0),
                    )
                    .optional()?;
                if s.as_deref() != Some("landed") {
                    unmet.push(dep.clone());
                }
            }
            if !unmet.is_empty() {
                return Err(StoreError::UnmetDeps(req.capsule_id.clone(), unmet));
            }
        }

        // §7.1.1 step 4: scope-overlap check vs other in-flight capsules.
        let our_scope: Vec<CanonicalPath> = json::from_str(&scope_json)?;
        let mut stmt = tx.prepare(
            "SELECT id, scope_json FROM capsule
             WHERE status IN ('active','accepted') AND id != ?1",
        )?;
        let rows = stmt
            .query_map(params![req.capsule_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        for (other_id, other_scope_json) in rows {
            let other: Vec<CanonicalPath> = json::from_str(&other_scope_json)?;
            for a in &our_scope {
                for b in &other {
                    if a.overlaps(b) {
                        return Err(StoreError::ScopeConflict(
                            req.capsule_id.clone(),
                            other_id,
                        ));
                    }
                }
            }
        }

        // §7.1.1 step 5: allocate attempt_id.
        let next_id: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(attempt_id), 0) + 1 FROM attempt WHERE capsule_id = ?1",
                params![req.capsule_id],
                |r| r.get(0),
            )?;

        let branch = format!("capsules/{}/a{}", req.capsule_id, next_id);
        let witness_branch = format!("capsule-witness/{}/a{}", req.capsule_id, next_id);
        let lease = Lease {
            owner: req.owner.clone(),
            session_id: req.session_id.clone(),
            acquired_at: now,
            expires_at: expires,
            ttl_sec: req.lease_ttl_sec,
        };
        let lease_json = json::to_string(&lease)?;

        tx.execute(
            "INSERT INTO attempt (
                capsule_id, attempt_id, lease_json, branch, witness_branch,
                base_sha, tip_sha, last_heartbeat, outcome, opened_at, closed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'in_flight', ?7, NULL)",
            params![
                req.capsule_id,
                next_id,
                lease_json,
                branch,
                witness_branch,
                req.base_sha,
                now_str,
            ],
        )?;

        tx.execute(
            "UPDATE capsule SET status='active', active_attempt=?1, updated_at=?2 WHERE id=?3",
            params![next_id, now_str, req.capsule_id],
        )?;

        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, ?3, ?4, 'attempt_claimed',
                     json_object('session_id', ?4, 'base_sha', ?5,
                                 'lease_expires_at', ?6))",
            params![
                now_str,
                req.capsule_id,
                next_id,
                req.session_id,
                req.base_sha,
                expires_str,
            ],
        )?;

        tx.commit()?;

        Ok(Attempt {
            id: next_id as u64,
            lease,
            branch,
            witness_branch,
            base_sha: req.base_sha,
            tip_sha: None,
            last_heartbeat: now,
            outcome: AttemptOutcome::InFlight,
            opened_at: now,
            closed_at: None,
        })
    }

    /// Attest: record verification, transition active → accepted iff exit_code matches.
    /// See DESIGN.md §7.1.0.
    pub fn attest(&mut self, req: AttestRequest) -> Result<AttestAck> {
        use capsule_core::{Lease, Verification};

        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;

        let tx = self.conn.transaction()?;

        let (status_str, active_attempt, acceptance_json): (String, Option<i64>, String) = tx
            .query_row(
                "SELECT status, active_attempt, acceptance_json FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;

        let status = parse_status(&status_str);
        if status != Status::Active {
            return Err(StoreError::NotClaimable(
                req.capsule_id.clone(),
                status_to_str(status),
            ));
        }
        let aid = active_attempt.expect("active ⇒ active_attempt set");

        let lease_json: String = tx.query_row(
            "SELECT lease_json FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
            params![req.capsule_id, aid],
            |r| r.get(0),
        )?;
        let lease: Lease = json::from_str(&lease_json)?;
        if lease.session_id != req.session_id {
            return Err(StoreError::CrossSession);
        }
        if now > lease.expires_at {
            return Err(StoreError::LeaseExpired(format_iso8601(lease.expires_at)?));
        }

        let acceptance: Acceptance = json::from_str(&acceptance_json)?;
        let verification = Verification {
            at: now,
            attestor: req.session_id.clone(),
            attempt_id: aid as u64,
            verified_sha: req.verified_sha.clone(),
            command: req.command,
            exit_code: req.exit_code.clone(),
            duration_ms: req.duration_ms,
            log_ref: req.log_ref,
        };
        let verification_json = json::to_string(&verification)?;

        let pass = exit_codes_match(&acceptance.expect_exit, &req.exit_code);
        let new_status = if pass { Status::Accepted } else { Status::Active };

        tx.execute(
            "UPDATE capsule SET verification_json=?1, status=?2, updated_at=?3 WHERE id=?4",
            params![
                verification_json,
                status_to_str(new_status),
                now_str,
                req.capsule_id,
            ],
        )?;
        tx.execute(
            "UPDATE attempt SET tip_sha=?1 WHERE capsule_id=?2 AND attempt_id=?3",
            params![req.verified_sha, req.capsule_id, aid],
        )?;
        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, ?3, ?4, 'attempt_attested', ?5)",
            params![now_str, req.capsule_id, aid, req.session_id, verification_json],
        )?;

        tx.commit()?;
        Ok(AttestAck {
            accepted: pass,
            new_status,
        })
    }

    /// Heartbeat: refresh `lease.expires_at = now + lease.ttl_sec`. See DESIGN.md §3.3.
    /// TTL is fixed at claim time; heartbeat does not let the worker change it.
    pub fn heartbeat(&mut self, req: HeartbeatRequest) -> Result<HeartbeatAck> {
        use capsule_core::Lease;

        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;

        let tx = self.conn.transaction()?;

        let (status_str, active_attempt, pending): (String, Option<i64>, Option<String>) = tx
            .query_row(
                "SELECT status, active_attempt, pending_land_json FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;

        // Heartbeat is allowed during active OR accepted (lease retained, §3.3).
        let status = parse_status(&status_str);
        if !matches!(status, Status::Active | Status::Accepted) {
            return Err(StoreError::NotClaimable(
                req.capsule_id.clone(),
                status_to_str(status),
            ));
        }
        let aid = active_attempt.ok_or_else(|| {
            StoreError::NotClaimable(req.capsule_id.clone(), status_to_str(status))
        })?;
        // Allowed even with pending_land set — §7.2 says heartbeats not required
        // while pending_land != null, but we don't reject them either; they're
        // a no-op against an effective lease that won't expire.
        let _ = pending;

        let lease_json: String = tx.query_row(
            "SELECT lease_json FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
            params![req.capsule_id, aid],
            |r| r.get(0),
        )?;
        let lease: Lease = json::from_str(&lease_json)?;

        if lease.session_id != req.session_id {
            return Err(StoreError::CrossSession);
        }
        if now > lease.expires_at {
            return Err(StoreError::LeaseExpired(format_iso8601(lease.expires_at)?));
        }

        let new_expires = now + time::Duration::seconds(lease.ttl_sec as i64);
        let new_lease = Lease {
            owner: lease.owner,
            session_id: lease.session_id,
            acquired_at: lease.acquired_at,
            expires_at: new_expires,
            ttl_sec: lease.ttl_sec,
        };
        let new_lease_json = json::to_string(&new_lease)?;

        tx.execute(
            "UPDATE attempt SET lease_json=?1, last_heartbeat=?2
             WHERE capsule_id=?3 AND attempt_id=?4",
            params![new_lease_json, now_str, req.capsule_id, aid],
        )?;
        tx.execute(
            "UPDATE capsule SET updated_at=?1 WHERE id=?2",
            params![now_str, req.capsule_id],
        )?;

        tx.commit()?;
        Ok(HeartbeatAck {
            lease_expires_at: new_expires,
        })
    }

    pub fn get_capsule(&self, id: &str) -> Result<Capsule> {
        let row: RowCapsule = self
            .conn
            .query_row(
                "SELECT id, title, description, acceptance_json, scope_json, base_ref,
                        depends_on_json, status, active_attempt, verification_json,
                        pending_land_json, landing_json, created_at, updated_at
                 FROM capsule WHERE id = ?1",
                params![id],
                |r| {
                    Ok(RowCapsule {
                        id: r.get(0)?,
                        title: r.get(1)?,
                        description: r.get(2)?,
                        acceptance_json: r.get(3)?,
                        scope_json: r.get(4)?,
                        base_ref: r.get(5)?,
                        depends_on_json: r.get(6)?,
                        status: r.get(7)?,
                        active_attempt: r.get(8)?,
                        verification_json: r.get(9)?,
                        pending_land_json: r.get(10)?,
                        landing_json: r.get(11)?,
                        created_at: r.get(12)?,
                        updated_at: r.get(13)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(id.to_string()))?;
        row.into_capsule(&self.conn)
    }
}

#[derive(Debug, Clone)]
pub struct NewCapsule {
    pub id: CapsuleId,
    pub title: String,
    pub description: String,
    pub acceptance: Acceptance,
    pub scope_prefixes: Vec<CanonicalPath>,
    pub base_ref: String,
    pub depends_on: Vec<CapsuleId>,
}

#[derive(Debug, Clone, Default)]
pub struct ListFilter {
    pub status: Option<Status>,
    /// `planned` capsules whose deps are all `landed` and whose scope does not
    /// overlap any in-flight (`active`/`accepted`) capsule. See DESIGN.md §7.1.1
    /// — the same predicate `claim` would evaluate.
    pub available: bool,
    /// Restrict to capsules whose scope_prefixes overlap this path
    /// (path-component-wise, see `CanonicalPath::overlaps`).
    pub scope_overlaps: Option<CanonicalPath>,
}

#[derive(Debug, Clone)]
pub struct ClaimRequest {
    pub capsule_id: CapsuleId,
    pub owner: String,
    pub session_id: String,
    pub lease_ttl_sec: u64,
    pub base_sha: String,
}

#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub capsule_id: CapsuleId,
    pub session_id: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HeartbeatAck {
    #[serde(with = "time::serde::iso8601")]
    pub lease_expires_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct AttestRequest {
    pub capsule_id: CapsuleId,
    pub session_id: String,
    pub verified_sha: String,
    pub command: String,
    pub exit_code: capsule_core::ExitCode,
    pub duration_ms: u64,
    pub log_ref: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AttestAck {
    pub accepted: bool,
    pub new_status: Status,
}

/// Sweep expired leases (DESIGN.md §3.3, §7.2). Run inside any tx that may
/// observe stale `active`/`accepted` capsules. Skips capsules whose
/// `pending_land_json` is non-null (those are §7.2 reclaim-frozen).
///
/// For every matching attempt: marks `outcome=expired`, sets `closed_at=now`,
/// clears `verification_json`, sets capsule `status=planned`, clears
/// `active_attempt`, and emits an `attempt_expired` event.
fn reclaim_expired_in_tx(tx: &rusqlite::Transaction<'_>, now: OffsetDateTime) -> Result<()> {
    use capsule_core::Lease;

    let now_str = format_iso8601(now)?;

    let mut stmt = tx.prepare(
        "SELECT c.id, c.active_attempt, a.lease_json
         FROM capsule c
         JOIN attempt a
           ON a.capsule_id = c.id AND a.attempt_id = c.active_attempt
         WHERE c.status IN ('active','accepted')
           AND c.active_attempt IS NOT NULL
           AND c.pending_land_json IS NULL",
    )?;
    let candidates: Vec<(String, i64, String)> = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    for (capsule_id, attempt_id, lease_json) in candidates {
        let lease: Lease = json::from_str(&lease_json)?;
        if now <= lease.expires_at {
            continue;
        }

        tx.execute(
            "UPDATE attempt SET outcome='expired', closed_at=?1
             WHERE capsule_id=?2 AND attempt_id=?3",
            params![now_str, capsule_id, attempt_id],
        )?;
        tx.execute(
            "UPDATE capsule
                SET status='planned',
                    active_attempt=NULL,
                    verification_json=NULL,
                    updated_at=?1
              WHERE id=?2",
            params![now_str, capsule_id],
        )?;
        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, ?3, 'system', 'attempt_expired',
                     json_object('lease_expires_at', ?4, 'session_id', ?5))",
            params![
                now_str,
                capsule_id,
                attempt_id,
                format_iso8601(lease.expires_at)?,
                lease.session_id,
            ],
        )?;
    }

    Ok(())
}

fn exit_codes_match(
    expect: &capsule_core::ExpectExit,
    got: &capsule_core::ExitCode,
) -> bool {
    use capsule_core::{ExitCode, ExpectExit};
    match (expect, got) {
        (ExpectExit::Code(a), ExitCode::Code(b)) => a == b,
        (ExpectExit::Sentinel(a), ExitCode::Sentinel(b)) => a == b,
        _ => false,
    }
}

#[derive(serde::Serialize)]
struct CreatedPayload<'a> {
    acceptance: &'a Acceptance,
    scope_prefixes: &'a Vec<CanonicalPath>,
    base_ref: &'a str,
    depends_on: &'a Vec<CapsuleId>,
}

struct RowCapsule {
    id: String,
    title: String,
    description: String,
    acceptance_json: String,
    scope_json: String,
    base_ref: String,
    depends_on_json: String,
    status: String,
    active_attempt: Option<i64>,
    verification_json: Option<String>,
    pending_land_json: Option<String>,
    landing_json: Option<String>,
    created_at: String,
    updated_at: String,
}

impl RowCapsule {
    fn into_capsule(self, conn: &Connection) -> Result<Capsule> {
        let mut attempts = vec![];
        let mut stmt = conn.prepare(
            "SELECT attempt_id, lease_json, branch, witness_branch, base_sha, tip_sha,
                    last_heartbeat, outcome, opened_at, closed_at
             FROM attempt WHERE capsule_id = ?1 ORDER BY attempt_id ASC",
        )?;
        let rows = stmt
            .query_map(params![self.id], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, String>(8)?,
                    r.get::<_, Option<String>>(9)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (id, lease_json, branch, wb, base_sha, tip_sha, hb, outcome, opened, closed) in rows {
            attempts.push(capsule_core::Attempt {
                id: id as u64,
                lease: json::from_str(&lease_json)?,
                branch,
                witness_branch: wb,
                base_sha,
                tip_sha,
                last_heartbeat: parse_iso8601(&hb),
                outcome: parse_outcome(&outcome),
                opened_at: parse_iso8601(&opened),
                closed_at: closed.map(|s| parse_iso8601(&s)),
            });
        }

        Ok(Capsule {
            id: self.id,
            title: self.title,
            description: self.description,
            acceptance: json::from_str(&self.acceptance_json)?,
            scope_prefixes: json::from_str(&self.scope_json)?,
            base_ref: self.base_ref,
            depends_on: json::from_str(&self.depends_on_json)?,
            status: parse_status(&self.status),
            active_attempt: self.active_attempt.map(|i| i as u64),
            attempts,
            verification: self
                .verification_json
                .map(|s| json::from_str(&s))
                .transpose()?,
            pending_land: self
                .pending_land_json
                .map(|s| json::from_str(&s))
                .transpose()?,
            landing: self.landing_json.map(|s| json::from_str(&s)).transpose()?,
            created_at: parse_iso8601(&self.created_at),
            updated_at: parse_iso8601(&self.updated_at),
        })
    }
}

fn status_to_str(s: Status) -> &'static str {
    match s {
        Status::Planned => "planned",
        Status::Active => "active",
        Status::Accepted => "accepted",
        Status::Landed => "landed",
        Status::Abandoned => "abandoned",
    }
}

fn parse_status(s: &str) -> Status {
    match s {
        "planned" => Status::Planned,
        "active" => Status::Active,
        "accepted" => Status::Accepted,
        "landed" => Status::Landed,
        "abandoned" => Status::Abandoned,
        other => panic!("unknown status in DB: {other}"),
    }
}

fn parse_outcome(s: &str) -> capsule_core::AttemptOutcome {
    use capsule_core::AttemptOutcome::*;
    match s {
        "in_flight" => InFlight,
        "released" => Released,
        "expired" => Expired,
        "abandoned" => Abandoned,
        "landed" => Landed,
        other => panic!("unknown attempt outcome in DB: {other}"),
    }
}

fn format_iso8601(t: OffsetDateTime) -> Result<String> {
    Ok(t.format(&time::format_description::well_known::Iso8601::DEFAULT)?)
}

fn parse_iso8601(s: &str) -> OffsetDateTime {
    OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
        .expect("DB stored a non-iso8601 timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let s = Store::open(&path).unwrap();
        std::mem::forget(dir);
        s
    }

    #[test]
    fn open_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let _ = Store::open(&path).unwrap();
        let _ = Store::open(&path).unwrap();
    }

    #[test]
    fn create_and_get() {
        let mut s = tmp_store();
        let c = s
            .create_capsule(NewCapsule {
                id: "abc".into(),
                title: "t".into(),
                description: "d".into(),
                acceptance: Acceptance {
                    run: "true".into(),
                    expect_exit: capsule_core::ExpectExit::Code(0),
                    cwd: None,
                    timeout_sec: None,
                },
                scope_prefixes: vec![CanonicalPath::new("src/api").unwrap()],
                base_ref: "main".into(),
                depends_on: vec![],
            })
            .unwrap();
        assert_eq!(c.status, Status::Planned);
        let got = s.get_capsule("abc").unwrap();
        assert_eq!(got.id, "abc");
        assert_eq!(got.scope_prefixes.len(), 1);
    }

    fn make_capsule(s: &mut Store, id: &str, scope: &str) {
        s.create_capsule(NewCapsule {
            id: id.into(),
            title: "t".into(),
            description: "d".into(),
            acceptance: Acceptance {
                run: "true".into(),
                expect_exit: capsule_core::ExpectExit::Code(0),
                cwd: None,
                timeout_sec: None,
            },
            scope_prefixes: vec![CanonicalPath::new(scope).unwrap()],
            base_ref: "main".into(),
            depends_on: vec![],
        })
        .unwrap();
    }

    fn claim_req(id: &str, sess: &str) -> ClaimRequest {
        ClaimRequest {
            capsule_id: id.into(),
            owner: "o".into(),
            session_id: sess.into(),
            lease_ttl_sec: 300,
            base_sha: "deadbeef".into(),
        }
    }

    #[test]
    fn claim_planned_succeeds() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let a = s.claim(claim_req("x", "sess1")).unwrap();
        assert_eq!(a.id, 1);
        assert_eq!(a.branch, "capsules/x/a1");
        assert_eq!(a.witness_branch, "capsule-witness/x/a1");
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Active);
        assert_eq!(c.active_attempt, Some(1));
        assert_eq!(c.attempts.len(), 1);
    }

    #[test]
    fn claim_active_rejected_when_lease_live() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s.claim(claim_req("x", "sess2")).unwrap_err();
        assert!(matches!(err, StoreError::NotClaimable(_, _)));
    }

    #[test]
    fn claim_scope_conflict() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/api");
        make_capsule(&mut s, "b", "src/api/users.ts");
        s.claim(claim_req("a", "sess1")).unwrap();
        let err = s.claim(claim_req("b", "sess2")).unwrap_err();
        assert!(matches!(err, StoreError::ScopeConflict(_, _)));
    }

    #[test]
    fn heartbeat_advances_lease() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let a1 = s.claim(claim_req("x", "sess1")).unwrap();
        // Brief sleep so the heartbeat-derived expires is strictly later than
        // the claim-derived one (both are now + ttl, ttl is fixed at claim).
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ack = s
            .heartbeat(HeartbeatRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
            })
            .unwrap();
        assert!(ack.lease_expires_at > a1.lease.expires_at);
    }

    #[test]
    fn attest_pass_transitions_to_accepted() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let ack = s
            .attest(AttestRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                verified_sha: "abc".into(),
                command: "true".into(),
                exit_code: capsule_core::ExitCode::Code(0),
                duration_ms: 100,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap();
        assert!(ack.accepted);
        assert_eq!(ack.new_status, Status::Accepted);
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Accepted);
        assert!(c.verification.is_some());
    }

    #[test]
    fn attest_fail_stays_active() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let ack = s
            .attest(AttestRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                verified_sha: "abc".into(),
                command: "false".into(),
                exit_code: capsule_core::ExitCode::Code(1),
                duration_ms: 50,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap();
        assert!(!ack.accepted);
        assert_eq!(ack.new_status, Status::Active);
    }

    #[test]
    fn attest_after_accepted_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let req = AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: "abc".into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 100,
            log_ref: "file:///dev/null".into(),
        };
        s.attest(req.clone()).unwrap();
        let err = s.attest(req).unwrap_err();
        assert!(matches!(err, StoreError::NotClaimable(_, _)));
    }

    #[test]
    fn heartbeat_cross_session_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s
            .heartbeat(HeartbeatRequest {
                capsule_id: "x".into(),
                session_id: "wrong".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::CrossSession));
    }

    fn claim_req_with_ttl(id: &str, sess: &str, ttl_sec: u64) -> ClaimRequest {
        ClaimRequest {
            capsule_id: id.into(),
            owner: "o".into(),
            session_id: sess.into(),
            lease_ttl_sec: ttl_sec,
            base_sha: "deadbeef".into(),
        }
    }

    #[test]
    fn lease_expiry_reverts_to_planned_and_clears_verification() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        // Attest while the lease is still alive — populates verification_json.
        s.attest(AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: "abc".into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        // Sleep past TTL so the next read-path sweep expires this attempt.
        std::thread::sleep(std::time::Duration::from_millis(1200));

        // Any read path that runs reclaim sweeps the expiry.
        let listed = s.list_capsules(ListFilter::default()).unwrap();
        let c = listed.iter().find(|c| c.id == "x").unwrap();
        assert_eq!(c.status, Status::Planned);
        assert!(c.active_attempt.is_none());
        assert!(c.verification.is_none());
        // Attempt itself is closed with outcome=expired.
        assert_eq!(c.attempts.len(), 1);
        assert_eq!(c.attempts[0].outcome, capsule_core::AttemptOutcome::Expired);
    }

    #[test]
    fn reclaim_does_not_touch_unrelated_capsules() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        make_capsule(&mut s, "y", "src/web");
        s.claim(claim_req_with_ttl("x", "sess1", 0)).unwrap();
        s.claim(claim_req_with_ttl("y", "sess2", 3600)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        let listed = s.list_capsules(ListFilter::default()).unwrap();
        let x = listed.iter().find(|c| c.id == "x").unwrap();
        let y = listed.iter().find(|c| c.id == "y").unwrap();
        assert_eq!(x.status, Status::Planned);
        assert_eq!(y.status, Status::Active);
        assert_eq!(y.active_attempt, Some(1));
    }

    #[test]
    fn list_filter_available() {
        let mut s = tmp_store();
        make_capsule(&mut s, "claimed", "src/a");
        make_capsule(&mut s, "free", "src/b");
        make_capsule(&mut s, "conflict", "src/a/sub");
        s.claim(claim_req("claimed", "sess1")).unwrap();

        let avail = s
            .list_capsules(ListFilter {
                available: true,
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = avail.iter().map(|c| c.id.as_str()).collect();
        // `claimed` is active → excluded; `conflict` overlaps `claimed` → excluded.
        assert_eq!(ids, vec!["free"]);
    }

    #[test]
    fn list_filter_available_excludes_unmet_deps() {
        let mut s = tmp_store();
        make_capsule(&mut s, "dep", "src/dep");
        s.create_capsule(NewCapsule {
            id: "child".into(),
            title: "t".into(),
            description: "d".into(),
            acceptance: Acceptance {
                run: "true".into(),
                expect_exit: capsule_core::ExpectExit::Code(0),
                cwd: None,
                timeout_sec: None,
            },
            scope_prefixes: vec![CanonicalPath::new("src/child").unwrap()],
            base_ref: "main".into(),
            depends_on: vec!["dep".into()],
        })
        .unwrap();

        let avail = s
            .list_capsules(ListFilter {
                available: true,
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = avail.iter().map(|c| c.id.as_str()).collect();
        // `dep` is planned with no deps → eligible. `child` deps unmet → excluded.
        assert_eq!(ids, vec!["dep"]);
    }

    #[test]
    fn list_filter_scope_overlaps() {
        let mut s = tmp_store();
        make_capsule(&mut s, "api", "src/api");
        make_capsule(&mut s, "web", "src/web");
        let res = s
            .list_capsules(ListFilter {
                scope_overlaps: Some(CanonicalPath::new("src/api/users.ts").unwrap()),
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = res.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["api"]);
    }

    #[test]
    fn create_rejects_invalid_id() {
        let mut s = tmp_store();
        let err = s
            .create_capsule(NewCapsule {
                id: "bad/id".into(),
                title: "t".into(),
                description: "d".into(),
                acceptance: Acceptance {
                    run: "true".into(),
                    expect_exit: capsule_core::ExpectExit::Code(0),
                    cwd: None,
                    timeout_sec: None,
                },
                scope_prefixes: vec![CanonicalPath::new("a").unwrap()],
                base_ref: "main".into(),
                depends_on: vec![],
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidId(_, _)));
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut s = tmp_store();
        let nc = NewCapsule {
            id: "x".into(),
            title: "t".into(),
            description: "d".into(),
            acceptance: Acceptance {
                run: "true".into(),
                expect_exit: capsule_core::ExpectExit::Code(0),
                cwd: None,
                timeout_sec: None,
            },
            scope_prefixes: vec![CanonicalPath::new("a").unwrap()],
            base_ref: "main".into(),
            depends_on: vec![],
        };
        s.create_capsule(nc.clone()).unwrap();
        let err = s.create_capsule(nc).unwrap_err();
        assert!(matches!(err, StoreError::DuplicateId(_)));
    }
}
