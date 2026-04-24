//! SQLite-backed capsule store. See `DESIGN.md` §4 (data model) and §7.1 (protocols).

pub mod schema;

use std::path::{Path, PathBuf};

use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, Capsule, CapsuleId, Landing, PendingLand, Status, Verification};
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
    #[error("capsule {0} not amendable: status={1} (only planned capsules may be amended)")]
    NotAmendable(CapsuleId, &'static str),
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
    #[error("capsule {0} not landable: missing verification or active_attempt")]
    NotLandable(CapsuleId),
    #[error("git: {0}")]
    Git(#[from] capsule_git::GitError),
    #[error("land push other failure: {0}")]
    LandOtherFailure(String),
    #[error("capsule {0} is terminal: {1}")]
    Terminal(CapsuleId, &'static str),
    #[error("dependency cycle: adding {0} -> {1} would create a cycle")]
    DependencyCycle(CapsuleId, CapsuleId),
    #[error("dependency target {0} does not exist")]
    DepNotFound(CapsuleId),
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

    /// Amend a `Planned` capsule. Rejects any other status with
    /// `StoreError::NotAmendable` — once `claim` has occurred the acceptance
    /// contract is bound to any future `verified_sha` (DESIGN.md §5/§6), so
    /// only pre-claim mutation is safe. `None` fields are unchanged.
    pub fn amend(&mut self, req: AmendRequest) -> Result<Capsule> {
        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;
        let tx = self.conn.transaction()?;

        let status_str: String = tx
            .query_row(
                "SELECT status FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;
        let status = parse_status(&status_str);
        if status != Status::Planned {
            return Err(StoreError::NotAmendable(
                req.capsule_id.clone(),
                status_to_str(status),
            ));
        }

        let mut sets: Vec<String> = Vec::new();
        let mut vals: Vec<rusqlite::types::Value> = Vec::new();
        let mut diff = json::Map::new();

        if let Some(title) = &req.title {
            sets.push(format!("title = ?{}", vals.len() + 1));
            vals.push(title.clone().into());
            diff.insert("title".into(), json::Value::String(title.clone()));
        }
        if let Some(desc) = &req.description {
            sets.push(format!("description = ?{}", vals.len() + 1));
            vals.push(desc.clone().into());
            diff.insert("description".into(), json::Value::String(desc.clone()));
        }
        if let Some(acc) = &req.acceptance {
            let s = json::to_string(acc)?;
            sets.push(format!("acceptance_json = ?{}", vals.len() + 1));
            vals.push(s.clone().into());
            diff.insert("acceptance".into(), json::from_str(&s)?);
        }
        if let Some(scope) = &req.scope_prefixes {
            let s = json::to_string(scope)?;
            sets.push(format!("scope_json = ?{}", vals.len() + 1));
            vals.push(s.clone().into());
            diff.insert("scope_prefixes".into(), json::from_str(&s)?);
        }
        if let Some(base_ref) = &req.base_ref {
            sets.push(format!("base_ref = ?{}", vals.len() + 1));
            vals.push(base_ref.clone().into());
            diff.insert("base_ref".into(), json::Value::String(base_ref.clone()));
        }

        if sets.is_empty() {
            tx.commit()?;
            return self.get_capsule(&req.capsule_id);
        }

        sets.push(format!("updated_at = ?{}", vals.len() + 1));
        vals.push(now_str.clone().into());
        let where_idx = vals.len() + 1;
        vals.push(req.capsule_id.clone().into());
        let sql = format!(
            "UPDATE capsule SET {} WHERE id = ?{}",
            sets.join(", "),
            where_idx
        );
        let params_slice: Vec<&dyn rusqlite::ToSql> =
            vals.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        tx.execute(&sql, params_slice.as_slice())?;

        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, NULL, 'operator', 'capsule_amended', ?3)",
            params![
                now_str,
                req.capsule_id,
                json::Value::Object(diff).to_string()
            ],
        )?;
        tx.commit()?;
        self.get_capsule(&req.capsule_id)
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
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
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
            String,
            Option<i64>,
            Option<String>,
            String,
            String,
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
                        return Err(StoreError::ScopeConflict(req.capsule_id.clone(), other_id));
                    }
                }
            }
        }

        // §7.1.1 step 5: allocate attempt_id.
        let next_id: i64 = tx.query_row(
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
        let new_status = if pass {
            Status::Accepted
        } else {
            Status::Active
        };

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
            params![
                now_str,
                req.capsule_id,
                aid,
                req.session_id,
                verification_json
            ],
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

    /// Land an `accepted` capsule via the git-atomic multi-ref fast-forward push.
    /// See DESIGN.md §7.1.2.
    ///
    /// Three steps interleaved with two DB transactions:
    ///   1. ls_remote `base_ref` to read `prior_base_sha` (no DB).
    ///   2. DB tx: re-verify preconditions; write complete `PendingLand`. Commit.
    ///   3. `git push --atomic --force-with-lease=<witness>: ...` (no DB).
    ///   4. DB tx: based on push outcome, write Landing+clear or clear+abandon /
    ///      clear+stay-accepted; emit events.
    ///
    /// Crash between (3) and (4) leaves `pending_land != null` for the
    /// reconciler to repair (§7.1.2 reconciler decision tree). Until then,
    /// the capsule is reclaim-frozen (§7.2).
    pub fn land(&mut self, req: LandRequest) -> Result<LandAck> {
        use capsule_core::Lease;
        use capsule_git::{land_push, ls_remote_branch, LandOutcome as GitOutcome};

        // ---- Step 1: read remote base_ref tip (outside any DB tx). ----
        // We need this for both the PendingLand record and the eventual
        // Landing.advanced_base_ref computation. If the remote moves between
        // here and step 3, the atomic push will reject as base_ref_moved.
        let (base_ref, witness_branch, verified_sha) = {
            let cap = self.get_capsule(&req.capsule_id)?;
            let v = cap
                .verification
                .as_ref()
                .ok_or_else(|| StoreError::NotLandable(req.capsule_id.clone()))?;
            let aid = cap
                .active_attempt
                .ok_or_else(|| StoreError::NotLandable(req.capsule_id.clone()))?;
            let att = cap
                .attempts
                .iter()
                .find(|a| a.id == aid)
                .ok_or_else(|| StoreError::NotLandable(req.capsule_id.clone()))?;
            (
                cap.base_ref.clone(),
                att.witness_branch.clone(),
                v.verified_sha.clone(),
            )
        };
        let prior_base_sha = ls_remote_branch(&req.remote, &base_ref)?;

        // ---- Step 2: write PendingLand under preconditions. ----
        let attempt_id: i64;
        let pending = {
            let now = OffsetDateTime::now_utc();
            let now_str = format_iso8601(now)?;
            let tx = self.conn.transaction()?;

            let (status_str, active_attempt, pending_land_json, verification_json): (
                String,
                Option<i64>,
                Option<String>,
                Option<String>,
            ) = tx
                .query_row(
                    "SELECT status, active_attempt, pending_land_json, verification_json
                     FROM capsule WHERE id = ?1",
                    params![req.capsule_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .optional()?
                .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;

            if pending_land_json.is_some() {
                return Err(StoreError::PendingLandFrozen);
            }
            let status = parse_status(&status_str);
            if status != Status::Accepted {
                return Err(StoreError::NotClaimable(
                    req.capsule_id.clone(),
                    status_to_str(status),
                ));
            }
            let aid =
                active_attempt.ok_or_else(|| StoreError::NotLandable(req.capsule_id.clone()))?;
            attempt_id = aid;
            let v_json =
                verification_json.ok_or_else(|| StoreError::NotLandable(req.capsule_id.clone()))?;
            let v: Verification = json::from_str(&v_json)?;
            // Re-bind verified_sha from the in-tx read (defense-in-depth: no
            // gap between read and PendingLand write).
            if v.verified_sha != verified_sha {
                return Err(StoreError::NotLandable(req.capsule_id.clone()));
            }

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

            let pending = PendingLand {
                at: now,
                attempt_id: aid as u64,
                verified_sha: verified_sha.clone(),
                prior_base_sha: prior_base_sha.clone(),
                witness_branch: witness_branch.clone(),
                lander: req.lander.clone(),
            };
            let pending_json = json::to_string(&pending)?;

            tx.execute(
                "UPDATE capsule SET pending_land_json=?1, updated_at=?2 WHERE id=?3",
                params![pending_json, now_str, req.capsule_id],
            )?;
            tx.execute(
                "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                 VALUES (?1, ?2, ?3, ?4, 'pending_land_committed', ?5)",
                params![now_str, req.capsule_id, aid, req.lander, pending_json],
            )?;
            tx.commit()?;
            pending
        };

        // ---- Step 3: atomic multi-ref push. No DB. ----
        let push_outcome = land_push(
            &req.repo_dir,
            &req.remote,
            &base_ref,
            &witness_branch,
            &verified_sha,
            &prior_base_sha,
        )?;

        // ---- Step 4: synchronous reconcile from outcome. ----
        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;
        let tx = self.conn.transaction()?;

        let outcome = match push_outcome {
            GitOutcome::Advanced { .. } | GitOutcome::NoOp => {
                let advanced_base_ref = verified_sha != prior_base_sha;
                let landing = Landing {
                    at: now,
                    landed_sha: verified_sha.clone(),
                    prior_base_sha: pending.prior_base_sha.clone(),
                    landed_by: req.lander.clone(),
                    attempt_id: pending.attempt_id,
                    witness_branch: pending.witness_branch.clone(),
                    advanced_base_ref,
                };
                let landing_json = json::to_string(&landing)?;

                tx.execute(
                    "UPDATE capsule
                        SET status='landed',
                            landing_json=?1,
                            pending_land_json=NULL,
                            updated_at=?2
                      WHERE id=?3",
                    params![landing_json, now_str, req.capsule_id],
                )?;
                tx.execute(
                    "UPDATE attempt SET outcome='landed', closed_at=?1
                      WHERE capsule_id=?2 AND attempt_id=?3",
                    params![now_str, req.capsule_id, attempt_id],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'capsule_landed', ?5)",
                    params![
                        now_str,
                        req.capsule_id,
                        attempt_id,
                        req.lander,
                        landing_json
                    ],
                )?;
                LandOutcome::Landed { landing }
            }
            GitOutcome::BaseRefMoved => {
                tx.execute(
                    "UPDATE capsule SET pending_land_json=NULL, updated_at=?1 WHERE id=?2",
                    params![now_str, req.capsule_id],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'pending_land_cleared',
                             json_object('reason', 'base_ref_moved', 'by', ?4))",
                    params![now_str, req.capsule_id, attempt_id, req.lander],
                )?;
                LandOutcome::BaseRefMoved
            }
            GitOutcome::WitnessOidMismatch => {
                tx.execute(
                    "UPDATE capsule
                        SET status='abandoned',
                            pending_land_json=NULL,
                            updated_at=?1
                      WHERE id=?2",
                    params![now_str, req.capsule_id],
                )?;
                tx.execute(
                    "UPDATE attempt SET outcome='abandoned', closed_at=?1
                      WHERE capsule_id=?2 AND attempt_id=?3",
                    params![now_str, req.capsule_id, attempt_id],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'operational_incident',
                             json_object('kind', 'witness_oid_mismatch',
                                         'witness_branch', ?5,
                                         'verified_sha', ?6))",
                    params![
                        now_str,
                        req.capsule_id,
                        attempt_id,
                        req.lander,
                        witness_branch,
                        verified_sha,
                    ],
                )?;
                LandOutcome::WitnessOidMismatch
            }
            GitOutcome::OtherFailure { stderr } => {
                // Per DESIGN: not enumerated in §7.1.2 step 5 (only base_ref_moved
                // and witness_oid_mismatch are). Treat as transient: clear
                // pending_land so the caller can retry without manual unfreeze;
                // bubble the stderr up as an error.
                tx.execute(
                    "UPDATE capsule SET pending_land_json=NULL, updated_at=?1 WHERE id=?2",
                    params![now_str, req.capsule_id],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'pending_land_cleared',
                             json_object('reason', 'other_failure',
                                         'stderr', ?5,
                                         'by', ?4))",
                    params![now_str, req.capsule_id, attempt_id, req.lander, stderr],
                )?;
                tx.commit()?;
                return Err(StoreError::LandOtherFailure(stderr));
            }
        };

        tx.commit()?;
        Ok(LandAck { outcome })
    }

    /// Voluntarily release a capsule. Single DB tx. Refused if pending_land
    /// is set (operator must use force-unfreeze first). See DESIGN.md §6.
    pub fn abandon(&mut self, req: AbandonRequest) -> Result<()> {
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

        if pending.is_some() {
            return Err(StoreError::PendingLandFrozen);
        }
        let status = parse_status(&status_str);
        if matches!(status, Status::Landed | Status::Abandoned) {
            return Err(StoreError::Terminal(
                req.capsule_id.clone(),
                status_to_str(status),
            ));
        }

        // If a lease is held, the abandoning session must own it.
        if let Some(aid) = active_attempt {
            let lease_json: String = tx.query_row(
                "SELECT lease_json FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
                params![req.capsule_id, aid],
                |r| r.get(0),
            )?;
            let lease: Lease = json::from_str(&lease_json)?;
            if lease.session_id != req.session_id {
                return Err(StoreError::CrossSession);
            }
            tx.execute(
                "UPDATE attempt SET outcome='abandoned', closed_at=?1
                  WHERE capsule_id=?2 AND attempt_id=?3",
                params![now_str, req.capsule_id, aid],
            )?;
        }

        tx.execute(
            "UPDATE capsule
                SET status='abandoned',
                    active_attempt=NULL,
                    updated_at=?1
              WHERE id=?2",
            params![now_str, req.capsule_id],
        )?;
        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, ?3, ?4, 'capsule_abandoned',
                     json_object('reason', ?5))",
            params![
                now_str,
                req.capsule_id,
                active_attempt,
                req.session_id,
                req.reason
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Manual reclaim — rarely needed since list/claim/heartbeat already
    /// run an eager sweep (DESIGN.md §6).
    /// Returns `true` if a lease was reclaimed; `false` for no-op.
    pub fn reclaim(&mut self, capsule_id: &str) -> Result<bool> {
        let now = OffsetDateTime::now_utc();

        let pending: Option<String> = self
            .conn
            .query_row(
                "SELECT pending_land_json FROM capsule WHERE id = ?1",
                params![capsule_id],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(capsule_id.into()))?;
        if pending.is_some() {
            return Err(StoreError::PendingLandFrozen);
        }

        let before_status: String = self.conn.query_row(
            "SELECT status FROM capsule WHERE id = ?1",
            params![capsule_id],
            |r| r.get(0),
        )?;
        let tx = self.conn.transaction()?;
        reclaim_expired_in_tx(&tx, now)?;
        let after_status: String = tx.query_row(
            "SELECT status FROM capsule WHERE id = ?1",
            params![capsule_id],
            |r| r.get(0),
        )?;
        tx.commit()?;
        Ok(before_status != after_status)
    }

    /// Add a dependency edge `capsule_id → depends_on`. DB-atomic with cycle
    /// check (DESIGN.md §7.1.3). No-op on terminal capsules. Idempotent if
    /// the edge already exists.
    pub fn add_dep(&mut self, req: DepRequest) -> Result<()> {
        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;
        let tx = self.conn.transaction()?;

        let (status_str, deps_json): (String, String) = tx
            .query_row(
                "SELECT status, depends_on_json FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;

        let status = parse_status(&status_str);
        if matches!(status, Status::Landed | Status::Abandoned) {
            return Ok(()); // §7.1.3 explicit no-op on terminal
        }

        let target_exists: bool = tx
            .query_row(
                "SELECT 1 FROM capsule WHERE id = ?1",
                params![req.depends_on],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if !target_exists {
            return Err(StoreError::DepNotFound(req.depends_on.clone()));
        }

        let mut deps: Vec<String> = json::from_str(&deps_json)?;
        if deps.contains(&req.depends_on) {
            return Ok(());
        }
        if req.depends_on == req.capsule_id {
            return Err(StoreError::DependencyCycle(
                req.capsule_id.clone(),
                req.depends_on,
            ));
        }
        // Cycle check: BFS from `depends_on` over depends_on edges; if we
        // reach `capsule_id`, adding the edge would close a cycle.
        if reachable(&tx, &req.depends_on, &req.capsule_id)? {
            return Err(StoreError::DependencyCycle(
                req.capsule_id.clone(),
                req.depends_on,
            ));
        }
        deps.push(req.depends_on.clone());
        let new_json = json::to_string(&deps)?;

        tx.execute(
            "UPDATE capsule SET depends_on_json=?1, updated_at=?2 WHERE id=?3",
            params![new_json, now_str, req.capsule_id],
        )?;
        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, NULL, 'system', 'dep_added',
                     json_object('depends_on', ?3))",
            params![now_str, req.capsule_id, req.depends_on],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Remove a dependency edge. DB-atomic. No-op on terminal capsules or if
    /// the edge does not exist (DESIGN.md §7.1.3).
    pub fn remove_dep(&mut self, req: DepRequest) -> Result<()> {
        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;
        let tx = self.conn.transaction()?;

        let (status_str, deps_json): (String, String) = tx
            .query_row(
                "SELECT status, depends_on_json FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;

        let status = parse_status(&status_str);
        if matches!(status, Status::Landed | Status::Abandoned) {
            return Ok(());
        }

        let mut deps: Vec<String> = json::from_str(&deps_json)?;
        let before = deps.len();
        deps.retain(|d| d != &req.depends_on);
        if deps.len() == before {
            return Ok(());
        }
        let new_json = json::to_string(&deps)?;

        tx.execute(
            "UPDATE capsule SET depends_on_json=?1, updated_at=?2 WHERE id=?3",
            params![new_json, now_str, req.capsule_id],
        )?;
        tx.execute(
            "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
             VALUES (?1, ?2, NULL, 'system', 'dep_removed',
                     json_object('depends_on', ?3))",
            params![now_str, req.capsule_id, req.depends_on],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Reconciler decision tree (DESIGN.md §7.1.2). No-op if `pending_land`
    /// is null. CAS-conditioned: if `pending_land` changed between the
    /// outside read and the in-tx commit, the in-tx update no-ops (loser
    /// of a reconciler race). Returns the outcome that was applied.
    ///
    /// `repo_dir` is only needed if a future variant invokes git push;
    /// reconcile only ls-remotes, so any cwd is fine — but we accept it
    /// uniformly for symmetry with `land`.
    pub fn reconcile(&mut self, req: ReconcileRequest) -> Result<ReconcileOutcome> {
        self.reconcile_inner(req, /* operator: */ None)
    }

    /// Operator escape hatch (DESIGN.md §7.1.2). Same decision tree as
    /// `reconcile`, but emits a mandatory `operational_incident` event and
    /// requires the operator to assert `lander_confirmed_dead`.
    pub fn force_unfreeze(&mut self, req: ForceUnfreezeRequest) -> Result<ReconcileOutcome> {
        if !req.lander_confirmed_dead {
            return Err(StoreError::LandOtherFailure(
                "force-unfreeze requires --lander-confirmed-dead".into(),
            ));
        }
        let operator = Some(req.operator.clone());
        self.reconcile_inner(
            ReconcileRequest {
                capsule_id: req.capsule_id,
                remote: req.remote,
            },
            operator,
        )
    }

    fn reconcile_inner(
        &mut self,
        req: ReconcileRequest,
        operator: Option<String>,
    ) -> Result<ReconcileOutcome> {
        use capsule_git::ls_remote_branch;

        let now = OffsetDateTime::now_utc();
        let now_str = format_iso8601(now)?;

        // Outside-tx read of pending_land. We snapshot the JSON for CAS.
        let pending_json: Option<String> = self
            .conn
            .query_row(
                "SELECT pending_land_json FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;
        let Some(snapshot_json) = pending_json else {
            return Ok(ReconcileOutcome::NotFrozen);
        };
        let pending: PendingLand = json::from_str(&snapshot_json)?;

        // ls-remote witness branch.
        let witness_sha = ls_remote_branch(&req.remote, &pending.witness_branch)?;
        let witness_state = if witness_sha == capsule_git::ZERO_OID {
            WitnessState::Absent
        } else if witness_sha == pending.verified_sha {
            WitnessState::AtVerifiedSha
        } else {
            WitnessState::Different(witness_sha)
        };

        let actor = operator.clone().unwrap_or_else(|| "reconciler".into());

        let tx = self.conn.transaction()?;
        // CAS: re-read pending_land in tx. If it has been mutated since the
        // outside read, abort with a no-op (another reconciler/lander won).
        let cur_pending: Option<String> = tx
            .query_row(
                "SELECT pending_land_json FROM capsule WHERE id = ?1",
                params![req.capsule_id],
                |r| r.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(req.capsule_id.clone()))?;
        if cur_pending.as_deref() != Some(snapshot_json.as_str()) {
            tx.commit()?;
            return Ok(ReconcileOutcome::CasLost);
        }

        let outcome = match witness_state {
            WitnessState::AtVerifiedSha => {
                // Push ran before crash. Reconstruct Landing from PendingLand.
                let landing = Landing {
                    at: now,
                    landed_sha: pending.verified_sha.clone(),
                    prior_base_sha: pending.prior_base_sha.clone(),
                    landed_by: actor.clone(),
                    attempt_id: pending.attempt_id,
                    witness_branch: pending.witness_branch.clone(),
                    advanced_base_ref: pending.verified_sha != pending.prior_base_sha,
                };
                let landing_json = json::to_string(&landing)?;
                tx.execute(
                    "UPDATE capsule
                        SET status='landed',
                            landing_json=?1,
                            pending_land_json=NULL,
                            updated_at=?2
                      WHERE id=?3",
                    params![landing_json, now_str, req.capsule_id],
                )?;
                tx.execute(
                    "UPDATE attempt SET outcome='landed', closed_at=?1
                      WHERE capsule_id=?2 AND attempt_id=?3",
                    params![now_str, req.capsule_id, pending.attempt_id as i64],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'capsule_landed', ?5)",
                    params![
                        now_str,
                        req.capsule_id,
                        pending.attempt_id as i64,
                        actor,
                        landing_json
                    ],
                )?;
                if operator.is_some() {
                    tx.execute(
                        "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                         VALUES (?1, ?2, ?3, ?4, 'operational_incident',
                                 json_object('kind', 'force_unfreeze',
                                             'resolution', 'reconciled_landed'))",
                        params![now_str, req.capsule_id, pending.attempt_id as i64, actor],
                    )?;
                }
                ReconcileOutcome::Landed
            }
            WitnessState::Different(found_sha) => {
                tx.execute(
                    "UPDATE capsule
                        SET status='abandoned',
                            pending_land_json=NULL,
                            updated_at=?1
                      WHERE id=?2",
                    params![now_str, req.capsule_id],
                )?;
                tx.execute(
                    "UPDATE attempt SET outcome='abandoned', closed_at=?1
                      WHERE capsule_id=?2 AND attempt_id=?3",
                    params![now_str, req.capsule_id, pending.attempt_id as i64],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'operational_incident',
                             json_object('kind', 'witness_oid_mismatch',
                                         'witness_branch', ?5,
                                         'expected_sha', ?6,
                                         'found_sha', ?7,
                                         'by', ?4))",
                    params![
                        now_str,
                        req.capsule_id,
                        pending.attempt_id as i64,
                        actor,
                        pending.witness_branch,
                        pending.verified_sha,
                        found_sha,
                    ],
                )?;
                ReconcileOutcome::Abandoned
            }
            WitnessState::Absent => {
                tx.execute(
                    "UPDATE capsule
                        SET pending_land_json=NULL,
                            updated_at=?1
                      WHERE id=?2",
                    params![now_str, req.capsule_id],
                )?;
                tx.execute(
                    "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                     VALUES (?1, ?2, ?3, ?4, 'pending_land_cleared',
                             json_object('reason', 'witness_absent', 'by', ?4))",
                    params![now_str, req.capsule_id, pending.attempt_id as i64, actor],
                )?;
                if operator.is_some() {
                    tx.execute(
                        "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
                         VALUES (?1, ?2, ?3, ?4, 'operational_incident',
                                 json_object('kind', 'force_unfreeze',
                                             'resolution', 'cleared_witness_absent'))",
                        params![now_str, req.capsule_id, pending.attempt_id as i64, actor],
                    )?;
                }
                ReconcileOutcome::Cleared
            }
        };

        tx.commit()?;
        Ok(outcome)
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

/// Amend a `Planned` capsule. Fields left as `None` are unchanged. `depends_on`
/// has its own `add_dep`/`remove_dep` (§7.1.3) and is intentionally not
/// amendable here. Nothing else binds these fields until `claim`, so
/// pre-claim mutation is spec-compatible (DESIGN.md §5/§6).
#[derive(Debug, Clone, Default)]
pub struct AmendRequest {
    pub capsule_id: CapsuleId,
    pub title: Option<String>,
    pub description: Option<String>,
    pub acceptance: Option<Acceptance>,
    pub scope_prefixes: Option<Vec<CanonicalPath>>,
    pub base_ref: Option<String>,
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

#[derive(Debug, Clone)]
pub struct LandRequest {
    pub capsule_id: CapsuleId,
    pub session_id: String,
    /// Principal id of the lander (recorded in PendingLand/Landing/events).
    pub lander: String,
    /// Git remote name or URL for ls-remote and the atomic push.
    pub remote: String,
    /// Working directory the `git push` is invoked from — must have
    /// `verified_sha` in its object database (typically the lander's clone).
    pub repo_dir: PathBuf,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LandOutcome {
    /// Push succeeded; capsule is `landed`.
    Landed { landing: Landing },
    /// `base_ref` advanced between PendingLand commit and push. Capsule
    /// stays `accepted`; pending_land cleared. Caller rebases + re-attests.
    BaseRefMoved,
    /// Witness ref existed at a different sha. Capsule moved to `abandoned`;
    /// `operational_incident` event emitted.
    WitnessOidMismatch,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LandAck {
    pub outcome: LandOutcome,
}

#[derive(Debug, Clone)]
pub struct AbandonRequest {
    pub capsule_id: CapsuleId,
    pub session_id: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct DepRequest {
    pub capsule_id: CapsuleId,
    pub depends_on: CapsuleId,
}

#[derive(Debug, Clone)]
pub struct ReconcileRequest {
    pub capsule_id: CapsuleId,
    /// Git remote name or URL for ls-remote witness.
    pub remote: String,
}

#[derive(Debug, Clone)]
pub struct ForceUnfreezeRequest {
    pub capsule_id: CapsuleId,
    pub remote: String,
    /// Operator identity, recorded as the actor on emitted events.
    pub operator: String,
    /// Operator must confirm the lander process is dead/unresponsive
    /// before bypassing the reconciler. Without this flag the call is
    /// rejected (DESIGN.md §7.1.2 force-unfreeze precondition).
    pub lander_confirmed_dead: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOutcome {
    /// `pending_land` was null — nothing to do.
    NotFrozen,
    /// `pending_land` mutated between the snapshot and the in-tx CAS check
    /// (another reconciler / lander won the race). No-op.
    CasLost,
    /// Witness exists at verified_sha → reconstructed Landing, status=landed.
    Landed,
    /// Witness exists at a different sha → status=abandoned + incident.
    Abandoned,
    /// Witness absent → cleared pending_land; capsule remains accepted.
    Cleared,
}

enum WitnessState {
    Absent,
    AtVerifiedSha,
    Different(String),
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

/// BFS over the `depends_on` graph from `from` looking for `target`.
/// Used by `add_dep` for cycle rejection (DESIGN.md §7.1.3).
fn reachable(tx: &rusqlite::Transaction<'_>, from: &str, target: &str) -> Result<bool> {
    use std::collections::{HashSet, VecDeque};
    let mut seen: HashSet<String> = HashSet::new();
    let mut q: VecDeque<String> = VecDeque::new();
    q.push_back(from.to_string());
    while let Some(node) = q.pop_front() {
        if !seen.insert(node.clone()) {
            continue;
        }
        if node == target {
            return Ok(true);
        }
        let deps_json: Option<String> = tx
            .query_row(
                "SELECT depends_on_json FROM capsule WHERE id = ?1",
                params![node],
                |r| r.get(0),
            )
            .optional()?;
        let Some(deps_json) = deps_json else {
            continue;
        };
        let deps: Vec<String> = json::from_str(&deps_json)?;
        for d in deps {
            if !seen.contains(&d) {
                q.push_back(d);
            }
        }
    }
    Ok(false)
}

fn exit_codes_match(expect: &capsule_core::ExpectExit, got: &capsule_core::ExitCode) -> bool {
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

    // ---- abandon / reclaim / dep tests ----

    #[test]
    fn abandon_releases_mutex_and_marks_terminal() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            reason: "user request".into(),
        })
        .unwrap();
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Abandoned);
        assert!(c.active_attempt.is_none());
        assert_eq!(
            c.attempts[0].outcome,
            capsule_core::AttemptOutcome::Abandoned
        );

        // Overlapping capsule can now claim.
        make_capsule(&mut s, "y", "src/api/users.ts");
        s.claim(claim_req("y", "sess2")).unwrap();
    }

    #[test]
    fn abandon_cross_session_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s
            .abandon(AbandonRequest {
                capsule_id: "x".into(),
                session_id: "wrong".into(),
                reason: "r".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::CrossSession));
    }

    #[test]
    fn abandon_already_terminal_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            reason: "r".into(),
        })
        .unwrap();
        let err = s
            .abandon(AbandonRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                reason: "r".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::Terminal(_, "abandoned")));
    }

    #[test]
    fn reclaim_noop_when_lease_live() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let reclaimed = s.reclaim("x").unwrap();
        assert!(!reclaimed);
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Active);
    }

    #[test]
    fn reclaim_reclaims_when_lease_expired() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 0)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let reclaimed = s.reclaim("x").unwrap();
        assert!(reclaimed);
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Planned);
    }

    #[test]
    fn amend_planned_changes_all_fields() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let new_acc = Acceptance {
            run: "pnpm test".into(),
            expect_exit: capsule_core::ExpectExit::Code(0),
            cwd: Some("webapp".into()),
            timeout_sec: Some(600),
        };
        s.amend(AmendRequest {
            capsule_id: "x".into(),
            title: Some("new title".into()),
            description: Some("new desc".into()),
            acceptance: Some(new_acc.clone()),
            scope_prefixes: Some(vec![CanonicalPath::new("webapp/src").unwrap()]),
            base_ref: Some("develop".into()),
        })
        .unwrap();
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Planned);
        assert_eq!(c.title, "new title");
        assert_eq!(c.description, "new desc");
        assert_eq!(c.acceptance.run, "pnpm test");
        assert_eq!(c.acceptance.cwd.as_deref(), Some("webapp"));
        assert_eq!(c.scope_prefixes.len(), 1);
        assert_eq!(c.scope_prefixes[0].as_str(), "webapp/src");
        assert_eq!(c.base_ref, "develop");
    }

    #[test]
    fn amend_partial_leaves_others_untouched() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let before = s.get_capsule("x").unwrap();
        s.amend(AmendRequest {
            capsule_id: "x".into(),
            title: Some("only title".into()),
            ..Default::default()
        })
        .unwrap();
        let after = s.get_capsule("x").unwrap();
        assert_eq!(after.title, "only title");
        assert_eq!(after.description, before.description);
        assert_eq!(after.acceptance.run, before.acceptance.run);
        assert_eq!(after.base_ref, before.base_ref);
        assert_eq!(after.scope_prefixes, before.scope_prefixes);
    }

    #[test]
    fn amend_emits_event() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.amend(AmendRequest {
            capsule_id: "x".into(),
            title: Some("t2".into()),
            ..Default::default()
        })
        .unwrap();
        let kind: String = s
            .conn
            .query_row(
                "SELECT kind FROM event WHERE capsule_id = ?1 AND kind = 'capsule_amended'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kind, "capsule_amended");
    }

    #[test]
    fn amend_on_active_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s
            .amend(AmendRequest {
                capsule_id: "x".into(),
                title: Some("nope".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::NotAmendable(_, "active")),
            "got {err:?}"
        );
    }

    #[test]
    fn amend_noop_when_all_none() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let before = s.get_capsule("x").unwrap();
        let after = s
            .amend(AmendRequest {
                capsule_id: "x".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(before.title, after.title);
        assert_eq!(before.updated_at, after.updated_at);
    }

    #[test]
    fn amend_not_found() {
        let mut s = tmp_store();
        let err = s
            .amend(AmendRequest {
                capsule_id: "nope".into(),
                title: Some("x".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn add_dep_records_edge() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        let c = s.get_capsule("a").unwrap();
        assert_eq!(c.depends_on, vec!["b".to_string()]);
    }

    #[test]
    fn add_dep_idempotent() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        s.add_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        let c = s.get_capsule("a").unwrap();
        assert_eq!(c.depends_on, vec!["b".to_string()]);
    }

    #[test]
    fn add_dep_self_loop_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        let err = s
            .add_dep(DepRequest {
                capsule_id: "a".into(),
                depends_on: "a".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::DependencyCycle(_, _)));
    }

    #[test]
    fn add_dep_cycle_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        make_capsule(&mut s, "c", "src/c");
        s.add_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        s.add_dep(DepRequest {
            capsule_id: "b".into(),
            depends_on: "c".into(),
        })
        .unwrap();
        // a → b → c, now try c → a (would close cycle).
        let err = s
            .add_dep(DepRequest {
                capsule_id: "c".into(),
                depends_on: "a".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::DependencyCycle(_, _)));
    }

    #[test]
    fn add_dep_target_not_found() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        let err = s
            .add_dep(DepRequest {
                capsule_id: "a".into(),
                depends_on: "ghost".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::DepNotFound(_)));
    }

    #[test]
    fn remove_dep_removes_edge() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        s.remove_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        let c = s.get_capsule("a").unwrap();
        assert!(c.depends_on.is_empty());
    }

    #[test]
    fn dep_ops_noop_on_terminal_capsules() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.claim(claim_req("a", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "a".into(),
            session_id: "sess1".into(),
            reason: "r".into(),
        })
        .unwrap();
        // Both are no-ops on abandoned capsule.
        s.add_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        s.remove_dep(DepRequest {
            capsule_id: "a".into(),
            depends_on: "b".into(),
        })
        .unwrap();
        let c = s.get_capsule("a").unwrap();
        assert!(c.depends_on.is_empty());
    }

    // ---- land() integration tests against a real local bare repo. ----

    fn git(cwd: &std::path::Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("git invocation failed");
        if !out.status.success() {
            panic!(
                "git {args:?} in {cwd:?} failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
        }
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    /// Build a bare repo with `main` at one commit, plus a worker clone with
    /// a second commit pushed under `capsules/<id>/a1` so the bare repo has
    /// the verified_sha object available for the land push.
    /// Returns `(tempdir, bare_repo_path, work_dir_path, verified_sha)`.
    /// The `work_dir_path` doubles as the lander's `repo_dir` for `land()`.
    fn setup_bare_with_attempt(
        capsule_id: &str,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        String,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let bare = dir.path().join("bare.git");
        std::fs::create_dir(&bare).unwrap();
        git(&bare, &["init", "--bare", "--initial-branch=main"]);

        let work = dir.path().join("work");
        std::fs::create_dir(&work).unwrap();
        git(&work, &["init", "--initial-branch=main"]);
        git(&work, &["config", "user.email", "t@t"]);
        git(&work, &["config", "user.name", "t"]);
        std::fs::write(work.join("README"), "init\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "init"]);
        git(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
        git(&work, &["push", "origin", "main"]);

        // Worker creates a new commit; this is the verified_sha.
        std::fs::write(work.join("feature.txt"), "feature\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "feature"]);
        let verified_sha = git(&work, &["rev-parse", "HEAD"]);
        let attempt_branch = format!("capsules/{capsule_id}/a1");
        // Push to the per-attempt branch so the bare repo has the object.
        git(
            &work,
            &[
                "push",
                "origin",
                &format!("HEAD:refs/heads/{attempt_branch}"),
            ],
        );

        (dir, bare, work, verified_sha)
    }

    fn land_setup_capsule(s: &mut Store, id: &str) {
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
            scope_prefixes: vec![CanonicalPath::new("feature.txt").unwrap()],
            base_ref: "main".into(),
            depends_on: vec![],
        })
        .unwrap();
    }

    #[test]
    fn land_happy_path_advances_base_ref_and_writes_landing() {
        let id = "land1";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        land_setup_capsule(&mut s, id);
        s.claim(claim_req(id, "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.clone(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();

        let ack = s
            .land(LandRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                lander: "test-lander".into(),
                remote: bare.to_str().unwrap().into(),
                repo_dir: work.clone(),
            })
            .unwrap();

        match ack.outcome {
            LandOutcome::Landed { ref landing } => {
                assert_eq!(landing.landed_sha, verified_sha);
                assert!(landing.advanced_base_ref);
                assert_eq!(landing.witness_branch, format!("capsule-witness/{id}/a1"));
            }
            other => panic!("expected Landed, got {other:?}"),
        }

        // Bare repo should now have main at verified_sha + the witness branch.
        let bare_main = git(&bare, &["rev-parse", "main"]);
        assert_eq!(bare_main, verified_sha);
        let witness = git(&bare, &["rev-parse", &format!("capsule-witness/{id}/a1")]);
        assert_eq!(witness, verified_sha);

        // DB: status=landed, landing populated, pending_land cleared, attempt closed.
        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Landed);
        assert!(c.landing.is_some());
        assert!(c.pending_land.is_none());
        let att = c.attempts.iter().find(|a| a.id == 1).unwrap();
        assert_eq!(att.outcome, capsule_core::AttemptOutcome::Landed);
    }

    #[test]
    fn land_idempotent_re_run_is_no_op_on_witness() {
        // Second land call with the same verified_sha against a bare that already
        // has main + witness at that sha is the §7.1.2 crash-retry case.
        // We simulate it by running land() twice; the second one finds main
        // already at verified_sha (NoOp on base_ref) and witness already
        // at verified_sha (same-OID lease accepted as no-op).
        let id = "land2";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        land_setup_capsule(&mut s, id);
        s.claim(claim_req(id, "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.clone(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        s.land(LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir: work.clone(),
        })
        .unwrap();

        // Second land — capsule is now `landed`, so we expect NotClaimable.
        let err = s
            .land(LandRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                lander: "test-lander".into(),
                remote: bare.to_str().unwrap().into(),
                repo_dir: work.clone(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::NotClaimable(_, "landed")));
    }

    // ---- reconciler / force-unfreeze tests ----

    /// Simulate a crash between `land`'s git push (step 3) and DB commit
    /// (step 4) by writing PendingLand directly + executing the push out
    /// of band, leaving status=accepted with pending_land set.
    #[allow(clippy::too_many_arguments)]
    fn simulate_land_crash(
        s: &mut Store,
        id: &str,
        verified_sha: &str,
        prior_base_sha: &str,
        bare: &std::path::Path,
        work: &std::path::Path,
        do_push: bool,
        push_witness_at: Option<&str>,
    ) {
        // Force PendingLand into the DB without going through Store::land()
        // (which would also commit Landing on success).
        let pending = PendingLand {
            at: OffsetDateTime::now_utc(),
            attempt_id: 1,
            verified_sha: verified_sha.into(),
            prior_base_sha: prior_base_sha.into(),
            witness_branch: format!("capsule-witness/{id}/a1"),
            lander: "test-lander".into(),
        };
        let pending_json = json::to_string(&pending).unwrap();
        let now_str = format_iso8601(OffsetDateTime::now_utc()).unwrap();
        s.conn
            .execute(
                "UPDATE capsule SET pending_land_json=?1, updated_at=?2 WHERE id=?3",
                params![pending_json, now_str, id],
            )
            .unwrap();

        if do_push {
            // Real atomic land push (this is what Store::land's step 3 does).
            let outcome = capsule_git::land_push(
                work,
                bare.to_str().unwrap(),
                "main",
                &pending.witness_branch,
                verified_sha,
                prior_base_sha,
            )
            .unwrap();
            assert!(matches!(
                outcome,
                capsule_git::LandOutcome::Advanced { .. } | capsule_git::LandOutcome::NoOp
            ));
        } else if let Some(other_sha) = push_witness_at {
            // Manually create the witness ref at a *different* sha to simulate
            // protection leak / corruption (decision-tree branch 3).
            git(
                work,
                &[
                    "push",
                    bare.to_str().unwrap(),
                    &format!("{other_sha}:refs/heads/{}", pending.witness_branch),
                ],
            );
        }
    }

    #[test]
    fn reconcile_noop_when_pending_land_null() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let outcome = s
            .reconcile(ReconcileRequest {
                capsule_id: "x".into(),
                remote: "/dev/null".into(),
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::NotFrozen);
    }

    #[test]
    fn reconcile_landed_when_witness_at_verified_sha() {
        // Push happened, DB commit didn't.
        let id = "rec1";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        land_setup_capsule(&mut s, id);
        s.claim(claim_req(id, "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.clone(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&mut s, id, &verified_sha, &prior, &bare, &work, true, None);

        let outcome = s
            .reconcile(ReconcileRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Landed);
        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Landed);
        assert!(c.landing.is_some());
        assert!(c.pending_land.is_none());
        assert_eq!(c.landing.as_ref().unwrap().landed_by, "reconciler");
    }

    #[test]
    fn reconcile_cleared_when_witness_absent() {
        // Crash before push. Witness absent → clear, capsule stays accepted.
        let id = "rec2";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        land_setup_capsule(&mut s, id);
        s.claim(claim_req(id, "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.clone(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&mut s, id, &verified_sha, &prior, &bare, &work, false, None);

        let outcome = s
            .reconcile(ReconcileRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Cleared);
        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Accepted);
        assert!(c.pending_land.is_none());
    }

    #[test]
    fn reconcile_abandoned_when_witness_at_different_sha() {
        // Witness exists at some other sha — protection leak / corruption.
        let id = "rec3";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        land_setup_capsule(&mut s, id);
        s.claim(claim_req(id, "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.clone(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        // Push a *different* commit at the witness ref.
        std::fs::write(work.join("noise.txt"), "noise\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "noise"]);
        let other_sha = git(&work, &["rev-parse", "HEAD"]);
        simulate_land_crash(
            &mut s,
            id,
            &verified_sha,
            &prior,
            &bare,
            &work,
            false,
            Some(&other_sha),
        );

        let outcome = s
            .reconcile(ReconcileRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Abandoned);
        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Abandoned);
        assert!(c.pending_land.is_none());
    }

    #[test]
    fn force_unfreeze_requires_lander_confirmed_dead() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let err = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: "x".into(),
                remote: "/dev/null".into(),
                operator: "op".into(),
                lander_confirmed_dead: false,
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::LandOtherFailure(_)));
    }

    #[test]
    fn force_unfreeze_lands_when_witness_at_verified_sha() {
        let id = "force1";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        land_setup_capsule(&mut s, id);
        s.claim(claim_req(id, "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.clone(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&mut s, id, &verified_sha, &prior, &bare, &work, true, None);

        let outcome = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
                operator: "operator-jane".into(),
                lander_confirmed_dead: true,
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Landed);
        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Landed);
        assert_eq!(c.landing.as_ref().unwrap().landed_by, "operator-jane");
    }
}
