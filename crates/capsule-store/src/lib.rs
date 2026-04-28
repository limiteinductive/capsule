//! SQLite-backed capsule store. See `DESIGN.md` §4 (data model) and §7.1 (protocols).

pub mod schema;

use std::path::{Path, PathBuf};

use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, Capsule, CapsuleId, Landing, PendingLand, Status};
use rusqlite::{params, Connection};
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
    /// A capsule operation was rejected because the capsule's current status
    /// does not allow that transition. Struct variant (not tuple) so the two
    /// `&'static str` fields can't be accidentally swapped at construction.
    /// `op` is one of `claim` / `attest` / `heartbeat` / `land` (sourced
    /// from the private `StoreOp::as_wire_str`); `current_status` is one of
    /// the wire strings for `Status` (capsule-core).
    #[error("capsule {capsule_id}: cannot {op} when status={current_status}")]
    WrongStatus {
        capsule_id: CapsuleId,
        op: &'static str,
        current_status: &'static str,
    },
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
    #[error("capsule {0} has pending_land — operation refused; reconcile or force-unfreeze first")]
    PendingLandFrozen(CapsuleId),
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
    #[error(
        "capsule {0} land race: pending_land was finalized by reconcile/force-unfreeze \
         between push and finalize — call get_capsule to read current state"
    )]
    LandRaceLost(CapsuleId),
    #[error("invalid verified_sha: {0}")]
    InvalidSha(#[from] capsule_core::sha::ShaError),
    #[error(
        "lease_ttl_sec {0} cannot be represented as a valid lease expiration; \
         choose a smaller TTL (a day is 86400)."
    )]
    InvalidLeaseTtl(u64),
    #[error(
        "force-unfreeze requires the operator to assert --lander-confirmed-dead \
         (DESIGN.md §7.1.2 — operator escape hatch demands explicit confirmation)"
    )]
    ForceUnfreezeNotConfirmed,
    #[error(
        "deploy verify gate: no recorded pass — run `capsule deploy verify --hermetic` \
         (or `--remote <url>`) before landing, or pass --skip-deploy-verify-gate \
         (DESIGN.md §8.2)"
    )]
    DeployVerifyMissing,
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// State-transition operation names that surface in `StoreError::WrongStatus`
/// error messages. Closed enum so the four hand-written op literals at the
/// `wrong_status` callsites become typed — a typo (`"atest"`) is now a
/// compile error rather than a silently-shipped error string. Mirrors
/// `EventKind::as_wire_str` (iter 114).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreOp {
    Claim,
    Attest,
    Heartbeat,
    Land,
}

impl StoreOp {
    const fn as_wire_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Attest => "attest",
            Self::Heartbeat => "heartbeat",
            Self::Land => "land",
        }
    }
}

impl StoreError {
    /// Build a `WrongStatus` from a `Status` value, snapping its wire form
    /// once. Used by every state-transition guard (`claim`, `attest`,
    /// `heartbeat`, `land`) that rejects based on the current status — the
    /// open-coded struct literal repeated four times let the
    /// `current.as_wire_str()` call drift out of step with the type's
    /// definition (forgetting it would compile, since `&'static str` is
    /// already what the field wants).
    fn wrong_status(capsule_id: CapsuleId, op: StoreOp, current: Status) -> Self {
        Self::WrongStatus {
            capsule_id,
            op: op.as_wire_str(),
            current_status: current.as_wire_str(),
        }
    }

    /// Build a `Terminal` from a typed `Status`, snapping its wire form once.
    /// Same rationale as `wrong_status`: the variant's `&'static str` field is
    /// the public wire format, but every internal callsite should hand off a
    /// typed `Status` so a future caller can't silently pass an unrelated
    /// literal (`"frozen"`, `"done"`) that would compile but ship a bogus
    /// error message.
    fn terminal(capsule_id: CapsuleId, status: Status) -> Self {
        Self::Terminal(capsule_id, status.as_wire_str())
    }

    /// Build a `NotAmendable` from a typed `Status`. See `terminal` for the
    /// rationale; same discipline applied to amend's status guard.
    fn not_amendable(capsule_id: CapsuleId, status: Status) -> Self {
        Self::NotAmendable(capsule_id, status.as_wire_str())
    }
}

/// Map `rusqlite::Error::QueryReturnedNoRows` to `StoreError::NotFound(id)`,
/// preserving any other rusqlite error via `From`. Centralizes the
/// `query_row(...).optional()?.ok_or_else(|| NotFound(...))` boilerplate
/// repeated at every CAS-style read in this file.
trait NotFoundExt<T> {
    fn or_not_found(self, capsule_id: &str) -> Result<T>;
}

impl<T> NotFoundExt<T> for std::result::Result<T, rusqlite::Error> {
    fn or_not_found(self, capsule_id: &str) -> Result<T> {
        match self {
            Ok(v) => Ok(v),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(StoreError::NotFound(capsule_id.to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open or create the store at `db_path`. Idempotent — applies any
    /// pending schema migrations on open. Used by both `init` and the
    /// per-command open path.
    ///
    /// Bumps rusqlite's prepared-statement cache from its default 16 to 32:
    /// `Store` already caches ~14 distinct prepares and the count grows as
    /// SQL-projection helpers proliferate, so the headroom prevents LRU
    /// eviction from thrashing the hot path.
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        conn.set_prepared_statement_cache_capacity(32);
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        schema::ensure(&conn)?;
        Ok(Self { conn })
    }

    /// Create a new capsule. Caller supplies the id (typically a uuid). All
    /// fields validated; status starts at `planned`.
    pub fn create_capsule(&mut self, c: NewCapsule) -> Result<Capsule> {
        if let Err(e) = capsule_core::id::validate(&c.id) {
            return Err(StoreError::InvalidId(c.id, e.to_string()));
        }
        let (now, now_str) = now_pair()?;
        let capsule = Capsule {
            id: c.id,
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

        if capsule_exists(&tx, &capsule.id)? {
            return Err(StoreError::DuplicateId(capsule.id));
        }

        tx.prepare_cached(
            "INSERT INTO capsule (
                id, title, description, acceptance_json, scope_json, base_ref,
                depends_on_json, status, active_attempt, verification_json,
                pending_land_json, landing_json, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'planned', NULL, NULL, NULL, NULL, ?8, ?8)",
        )?
        .execute(params![
            capsule.id,
            capsule.title,
            capsule.description,
            json::to_string(&capsule.acceptance)?,
            json::to_string(&capsule.scope_prefixes)?,
            capsule.base_ref,
            json::to_string(&capsule.depends_on)?,
            now_str,
        ])?;

        insert_event(
            &tx,
            &now_str,
            &capsule.id,
            None,
            actor::SYSTEM,
            EventKind::CapsuleCreated,
            &json::to_value(CreatedPayload {
                acceptance: &capsule.acceptance,
                scope_prefixes: &capsule.scope_prefixes,
                base_ref: &capsule.base_ref,
                depends_on: &capsule.depends_on,
            })?,
        )?;

        tx.commit()?;
        Ok(capsule)
    }

    /// Amend a `Planned` capsule. Rejects any other status with
    /// `StoreError::NotAmendable` — once `claim` has occurred the acceptance
    /// contract is bound to any future `verified_sha` (DESIGN.md §5/§6), so
    /// only pre-claim mutation is safe. `None` fields are unchanged.
    pub fn amend(&mut self, req: AmendRequest) -> Result<Capsule> {
        let AmendRequest {
            capsule_id,
            title,
            description,
            acceptance,
            scope_prefixes,
            base_ref,
        } = req;

        let (_, now_str) = now_pair()?;
        let tx = self.conn.transaction()?;

        let status_str: String = tx
            .prepare_cached("SELECT status FROM capsule WHERE id = ?1")?
            .query_row(params![&capsule_id], |r| r.get(0))
            .or_not_found(&capsule_id)?;
        let status = parse_status(&status_str);
        if status != Status::Planned {
            return Err(StoreError::not_amendable(capsule_id, status));
        }

        let mut update = AmendUpdate::new();
        if let Some(title) = title {
            update.set_string("title", "title", title);
        }
        if let Some(desc) = description {
            update.set_string("description", "description", desc);
        }
        if let Some(acc) = acceptance {
            update.set_json("acceptance_json", "acceptance", &acc)?;
        }
        if let Some(scope) = scope_prefixes {
            update.set_json("scope_json", "scope_prefixes", &scope)?;
        }
        if let Some(base_ref) = base_ref {
            update.set_string("base_ref", "base_ref", base_ref);
        }

        if update.is_empty() {
            tx.commit()?;
            return self.get_capsule(&capsule_id);
        }

        let updated_at_idx = update.vals.len() + 1;
        let where_idx = updated_at_idx + 1;
        update.sets.push(format!("updated_at = ?{updated_at_idx}"));
        update.vals.push(now_str.clone().into());
        update.vals.push(capsule_id.clone().into());
        let sql = format!(
            "UPDATE capsule SET {} WHERE id = ?{where_idx}",
            update.sets.join(", "),
        );
        tx.execute(&sql, rusqlite::params_from_iter(&update.vals))?;

        insert_event(
            &tx,
            &now_str,
            &capsule_id,
            None,
            actor::OPERATOR,
            EventKind::CapsuleAmended,
            &json::Value::Object(update.diff),
        )?;
        tx.commit()?;
        self.get_capsule(&capsule_id)
    }

    pub fn list_capsules(&mut self, filter: ListFilter) -> Result<Vec<Capsule>> {
        let now = OffsetDateTime::now_utc();
        let tx = self.conn.transaction()?;
        reclaim_expired_in_tx(&tx, now)?;

        let (q, status_param): (&'static str, Option<&'static str>) = match filter.status {
            Some(s) => (RowCapsule::SELECT_BY_STATUS_ORDERED, Some(s.as_wire_str())),
            None => (RowCapsule::SELECT_ALL_ORDERED, None),
        };
        let mut stmt = tx.prepare_cached(q)?;
        let rows: Vec<RowCapsule> = stmt
            .query_map(rusqlite::params_from_iter(status_param), RowCapsule::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);

        let mut capsules: Vec<Capsule> = rows
            .into_iter()
            .map(|r| r.into_capsule(&tx))
            .collect::<Result<Vec<_>>>()?;

        if filter.available {
            capsules = retain_available(&tx, capsules)?;
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

        capsule_core::sha::validate(&req.base_sha)?;

        let (now, now_str) = now_pair()?;
        let expires = checked_lease_expiry(now, req.lease_ttl_sec)?;

        let tx = self.conn.transaction()?;

        reclaim_expired_in_tx(&tx, now)?;

        let (status_str, _active_attempt, frozen, depends_on_json, scope_json): (
            String,
            Option<i64>,
            bool,
            String,
            String,
        ) = tx
            .prepare_cached(
                "SELECT status, active_attempt, pending_land_json IS NOT NULL,
                        depends_on_json, scope_json
                 FROM capsule WHERE id = ?1",
            )?
            .query_row(params![req.capsule_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .or_not_found(&req.capsule_id)?;

        if frozen {
            return Err(StoreError::PendingLandFrozen(req.capsule_id));
        }

        let status = parse_status(&status_str);
        if status != Status::Planned {
            return Err(StoreError::wrong_status(req.capsule_id, StoreOp::Claim, status));
        }

        let unmet = find_unmet_deps(&tx, &depends_on_json)?;
        if !unmet.is_empty() {
            return Err(StoreError::UnmetDeps(req.capsule_id, unmet));
        }

        if let Some(other_id) = find_scope_conflict(&tx, &req.capsule_id, &scope_json)? {
            return Err(StoreError::ScopeConflict(req.capsule_id, other_id));
        }

        let next_id = next_attempt_id(&tx, &req.capsule_id)?;

        let branch = format!("capsules/{}/a{}", req.capsule_id, next_id);
        let witness_branch = format!("capsule-witness/{}/a{}", req.capsule_id, next_id);
        let lease = Lease {
            owner: req.owner,
            session_id: req.session_id,
            acquired_at: now,
            expires_at: expires,
            ttl_sec: req.lease_ttl_sec,
        };
        let lease_json = json::to_string(&lease)?;

        tx.prepare_cached(
            "INSERT INTO attempt (
                capsule_id, attempt_id, lease_json, branch, witness_branch,
                base_sha, tip_sha, last_heartbeat, outcome, opened_at, closed_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, 'in_flight', ?7, NULL)",
        )?
        .execute(params![
            req.capsule_id,
            next_id,
            lease_json,
            branch,
            witness_branch,
            req.base_sha,
            now_str,
        ])?;

        tx.prepare_cached(
            "UPDATE capsule SET status='active', active_attempt=?1, updated_at=?2 WHERE id=?3",
        )?
        .execute(params![next_id, now_str, req.capsule_id])?;

        let claimed_payload = json::to_value(ClaimedPayload {
            attempt_id: next_id,
            session_id: &lease.session_id,
            base_sha: &req.base_sha,
            lease: &lease,
        })?;
        insert_event(
            &tx,
            &now_str,
            &req.capsule_id,
            Some(next_id),
            &lease.session_id,
            EventKind::AttemptClaimed,
            &claimed_payload,
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
        use capsule_core::Verification;

        capsule_core::sha::validate(&req.verified_sha)?;

        let (now, now_str) = now_pair()?;

        let tx = self.conn.transaction()?;

        let (status_str, active_attempt, acceptance_json): (String, Option<i64>, String) = tx
            .prepare_cached(
                "SELECT status, active_attempt, acceptance_json FROM capsule WHERE id = ?1",
            )?
            .query_row(params![req.capsule_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .or_not_found(&req.capsule_id)?;

        let status = parse_status(&status_str);
        if status != Status::Active {
            return Err(StoreError::wrong_status(req.capsule_id, StoreOp::Attest, status));
        }
        let aid = active_attempt.expect("active ⇒ active_attempt set");

        assert_live_lease_for_session(&tx, &req.capsule_id, aid, &req.session_id, now)?;

        let acceptance: Acceptance = json::from_str(&acceptance_json)?;
        let verification = Verification {
            at: now,
            attestor: req.session_id,
            attempt_id: aid as u64,
            verified_sha: req.verified_sha,
            command: req.command,
            exit_code: req.exit_code,
            duration_ms: req.duration_ms,
            log_ref: req.log_ref,
        };
        let verification_json = json::to_string(&verification)?;

        let pass = exit_codes_match(&acceptance.expect_exit, &verification.exit_code);
        let new_status = if pass {
            Status::Accepted
        } else {
            Status::Active
        };

        tx.prepare_cached(
            "UPDATE capsule SET verification_json=?1, status=?2, updated_at=?3 WHERE id=?4",
        )?
        .execute(params![
            verification_json,
            new_status.as_wire_str(),
            now_str,
            req.capsule_id,
        ])?;
        tx.prepare_cached("UPDATE attempt SET tip_sha=?1 WHERE capsule_id=?2 AND attempt_id=?3")?
            .execute(params![verification.verified_sha, req.capsule_id, aid])?;
        let event_payload = json::to_value(AttestedPayload {
            verified_sha: &verification.verified_sha,
            exit_code: &verification.exit_code,
            command: &verification.command,
            log_ref: &verification.log_ref,
            duration_ms: verification.duration_ms,
        })?;
        insert_event(
            &tx,
            &now_str,
            &req.capsule_id,
            Some(aid),
            &verification.attestor,
            EventKind::AttemptAttested,
            &event_payload,
        )?;

        tx.commit()?;
        Ok(AttestAck {
            accepted: pass,
            new_status,
        })
    }

    /// Heartbeat: refresh `lease.expires_at = now + lease.ttl_sec`. See DESIGN.md §3.3.
    /// TTL is fixed at claim time; heartbeat does not let the worker change it.
    ///
    /// `pending_land_json` is not read: §7.2 says heartbeats are not required
    /// once a lander has frozen the capsule, and aren't rejected either — the
    /// effective lease won't expire, so this becomes a benign no-op.
    pub fn heartbeat(&mut self, capsule_id: &str, session_id: &str) -> Result<HeartbeatAck> {
        let (now, now_str) = now_pair()?;

        let tx = self.conn.transaction()?;

        let (status_str, active_attempt): (String, Option<i64>) = tx
            .prepare_cached("SELECT status, active_attempt FROM capsule WHERE id = ?1")?
            .query_row(params![capsule_id], |r| Ok((r.get(0)?, r.get(1)?)))
            .or_not_found(capsule_id)?;

        let status = parse_status(&status_str);
        if !status.holds_lease() {
            return Err(StoreError::wrong_status(
                capsule_id.to_string(),
                StoreOp::Heartbeat,
                status,
            ));
        }
        let aid = active_attempt.expect("holds_lease ⇒ active_attempt set");

        let ttl_sec = project_live_lease_for_renewal(&tx, capsule_id, aid, session_id, now)?;
        let new_expires = checked_lease_expiry(now, ttl_sec)?;
        let new_expires_str = format_iso8601(new_expires)?;

        tx.prepare_cached(
            "UPDATE attempt
             SET lease_json = json_set(lease_json, '$.expires_at', ?1),
                 last_heartbeat = ?2
             WHERE capsule_id = ?3 AND attempt_id = ?4",
        )?
        .execute(params![new_expires_str, now_str, capsule_id, aid])?;
        tx.prepare_cached("UPDATE capsule SET updated_at=?1 WHERE id=?2")?
            .execute(params![now_str, capsule_id])?;

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
        use capsule_git::{land_push, ls_remote_branch, LandOutcome as GitOutcome};

        self.enforce_deploy_verify_gate(req.skip_deploy_verify_gate)?;

        let cap = self.get_capsule(&req.capsule_id)?;
        let Some(LandableSnapshot {
            base_ref,
            witness_branch,
            verified_sha,
        }) = LandableSnapshot::extract(cap)
        else {
            return Err(StoreError::NotLandable(req.capsule_id));
        };
        let prior_base_sha = ls_remote_branch(&req.remote, &base_ref)?;

        let attempt_id: i64;
        let pending_json: String;
        let pending = {
            let (now, now_str) = now_pair()?;
            let tx = self.conn.transaction()?;

            let (status_str, active_attempt, frozen, in_tx_verified_sha) =
                load_land_preconditions(&tx, &req.capsule_id)?;

            if frozen {
                return Err(StoreError::PendingLandFrozen(req.capsule_id));
            }
            let status = parse_status(&status_str);
            if status != Status::Accepted {
                return Err(StoreError::wrong_status(req.capsule_id, StoreOp::Land, status));
            }
            let aid = active_attempt.expect("accepted ⇒ active_attempt set");
            attempt_id = aid;
            let in_tx_verified_sha =
                in_tx_verified_sha.expect("accepted ⇒ verification set with verified_sha");
            if in_tx_verified_sha != verified_sha {
                return Err(StoreError::NotLandable(req.capsule_id));
            }

            assert_live_lease_for_session(&tx, &req.capsule_id, aid, &req.session_id, now)?;

            let pending = PendingLand {
                at: now,
                attempt_id: aid as u64,
                verified_sha,
                prior_base_sha,
                witness_branch,
                lander: req.lander,
            };
            let (pending_value, pending_str) = serialize_pending_for_cas_and_audit(&pending)?;
            pending_json = pending_str;

            tx.prepare_cached(
                "UPDATE capsule SET pending_land_json=?1, updated_at=?2 WHERE id=?3",
            )?
            .execute(params![pending_json, now_str, req.capsule_id])?;
            insert_event(
                &tx,
                &now_str,
                &req.capsule_id,
                Some(aid),
                &pending.lander,
                EventKind::PendingLandCommitted,
                &pending_value,
            )?;
            tx.commit()?;
            pending
        };

        let push_outcome = land_push(
            &req.repo_dir,
            &req.remote,
            &base_ref,
            &pending.witness_branch,
            &pending.verified_sha,
        )?;

        let (now, now_str) = now_pair()?;
        let tx = self.conn.transaction()?;

        if !pending_land_snapshot_unchanged(&tx, &req.capsule_id, &pending_json)? {
            tx.commit()?;
            return Err(StoreError::LandRaceLost(req.capsule_id));
        }

        let outcome = match push_outcome {
            GitOutcome::Advanced { .. } | GitOutcome::NoOp => {
                let advanced_base_ref = pending.verified_sha != pending.prior_base_sha;
                let lander = pending.lander.clone();
                let landing = pending.into_landing(now, advanced_base_ref, lander);
                finalize_landed(&tx, &req.capsule_id, &landing, &now_str)?;
                LandOutcome::Landed { landing }
            }
            GitOutcome::BaseRefMoved => {
                clear_pending_land(
                    &tx,
                    &now_str,
                    &req.capsule_id,
                    Some(attempt_id),
                    &pending.lander,
                    PendingLandClearedReason::BaseRefMoved,
                )?;
                LandOutcome::BaseRefMoved
            }
            GitOutcome::WitnessOidMismatch => {
                abandon_on_witness_mismatch(&tx, &req.capsule_id, attempt_id, &now_str)?;
                emit_operational_incident(
                    &tx,
                    &now_str,
                    &req.capsule_id,
                    Some(attempt_id),
                    &pending.lander,
                    OperationalIncidentKind::WitnessOidMismatch,
                    json::json!({
                        "witness_branch": pending.witness_branch,
                        "verified_sha": pending.verified_sha,
                    }),
                )?;
                LandOutcome::WitnessOidMismatch
            }
            GitOutcome::OtherFailure { stderr } => {
                record_transient_land_failure(
                    &tx,
                    &now_str,
                    &req.capsule_id,
                    attempt_id,
                    &pending.lander,
                    &stderr,
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
        let (_, now_str) = now_pair()?;
        let tx = self.conn.transaction()?;

        let (status_str, active_attempt, frozen): (String, Option<i64>, bool) = tx
            .prepare_cached(
                "SELECT status, active_attempt, pending_land_json IS NOT NULL
                 FROM capsule WHERE id = ?1",
            )?
            .query_row(params![req.capsule_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .or_not_found(&req.capsule_id)?;

        if frozen {
            return Err(StoreError::PendingLandFrozen(req.capsule_id));
        }
        let status = parse_status(&status_str);
        if status.is_terminal() {
            return Err(StoreError::terminal(req.capsule_id, status));
        }

        if let Some(aid) = active_attempt {
            assert_session_owns_attempt(&tx, &req.capsule_id, aid, &req.session_id)?;
            close_attempt(
                &tx,
                &req.capsule_id,
                aid,
                capsule_core::AttemptOutcome::Abandoned,
                &now_str,
            )?;
        }

        tx.prepare_cached(
            "UPDATE capsule
                SET status='abandoned',
                    active_attempt=NULL,
                    updated_at=?1
              WHERE id=?2",
        )?
        .execute(params![now_str, req.capsule_id])?;
        let payload = json::json!({ "reason": req.reason });
        insert_event(
            &tx,
            &now_str,
            &req.capsule_id,
            active_attempt,
            &req.session_id,
            EventKind::CapsuleAbandoned,
            &payload,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Manual reclaim — rarely needed since list/claim/heartbeat already
    /// run an eager sweep (DESIGN.md §6).
    /// Returns `true` if a lease was reclaimed; `false` for no-op.
    pub fn reclaim(&mut self, capsule_id: &str) -> Result<bool> {
        let now = OffsetDateTime::now_utc();
        let tx = self.conn.transaction()?;

        assert_not_pending_land_frozen_in_tx(&tx, capsule_id)?;
        let read_status = |id: &str| -> Result<String> {
            tx.prepare_cached("SELECT status FROM capsule WHERE id = ?1")?
                .query_row(params![id], |r| r.get(0))
                .or_not_found(id)
        };
        let before_status = read_status(capsule_id)?;
        reclaim_expired_in_tx(&tx, now)?;
        let after_status = read_status(capsule_id)?;
        tx.commit()?;
        Ok(before_status != after_status)
    }

    /// Add a dependency edge `capsule_id → depends_on`. DB-atomic with cycle
    /// check (DESIGN.md §7.1.3). No-op on terminal capsules. Idempotent if
    /// the edge already exists.
    pub fn add_dep(&mut self, req: DepRequest) -> Result<()> {
        let (_, now_str) = now_pair()?;
        let tx = self.conn.transaction()?;

        let Some(mut deps) = load_deps_for_mutation(&tx, &req.capsule_id)? else {
            return Ok(());
        };

        if !capsule_exists(&tx, &req.depends_on)? {
            return Err(StoreError::DepNotFound(req.depends_on));
        }

        if deps.contains(&req.depends_on) {
            return Ok(());
        }
        if creates_cycle(&tx, &req.capsule_id, &req.depends_on)? {
            return Err(StoreError::DependencyCycle(
                req.capsule_id,
                req.depends_on,
            ));
        }
        deps.push(req.depends_on);
        let new_json = json::to_string(&deps)?;
        let dep_id = deps.last().expect("just pushed");

        persist_dep_change(
            &tx,
            &now_str,
            &req.capsule_id,
            &new_json,
            EventKind::DependencyAdded,
            dep_id,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Remove a dependency edge. DB-atomic. No-op on terminal capsules or if
    /// the edge does not exist (DESIGN.md §7.1.3).
    pub fn remove_dep(&mut self, req: DepRequest) -> Result<()> {
        let (_, now_str) = now_pair()?;
        let tx = self.conn.transaction()?;

        let Some(mut deps) = load_deps_for_mutation(&tx, &req.capsule_id)? else {
            return Ok(());
        };

        let before = deps.len();
        deps.retain(|d| d != &req.depends_on);
        if deps.len() == before {
            return Ok(());
        }
        let new_json = json::to_string(&deps)?;

        persist_dep_change(
            &tx,
            &now_str,
            &req.capsule_id,
            &new_json,
            EventKind::DependencyRemoved,
            &req.depends_on,
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

    /// Record that the deploy-verify ACL suite (DESIGN §8.2) passed for
    /// this store. Single-row table — overwrites any prior pass. The
    /// presence of a row is the gate `Store::land` checks.
    pub fn record_deploy_verify_pass(&mut self, mode: &str, base_ref: &str) -> Result<()> {
        let (_, now_str) = now_pair()?;
        self.conn.execute(
            "INSERT OR REPLACE INTO deploy_verify_pass(id, at, mode, base_ref) \
             VALUES (1, ?1, ?2, ?3)",
            params![now_str, mode, base_ref],
        )?;
        Ok(())
    }

    /// True iff `record_deploy_verify_pass` has been called on this store.
    pub fn check_deploy_verify_pass(&self) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM deploy_verify_pass WHERE id = 1",
            [],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    /// §8.2 step 0: refuse to land in production until the ACL suite
    /// is recorded as passing for this deployment. Bypass requires the
    /// caller to set `skip` explicitly — the CLI surfaces this behind
    /// `--skip-deploy-verify-gate`, which is itself audit-logged.
    fn enforce_deploy_verify_gate(&self, skip: bool) -> Result<()> {
        if !skip && !self.check_deploy_verify_pass()? {
            return Err(StoreError::DeployVerifyMissing);
        }
        Ok(())
    }

    /// Operator escape hatch (DESIGN.md §7.1.2). Same decision tree as
    /// `reconcile`, but emits a mandatory `force_unfreeze_invoked` event
    /// (DESIGN §6) and requires the operator to assert `lander_confirmed_dead`.
    pub fn force_unfreeze(&mut self, req: ForceUnfreezeRequest) -> Result<ReconcileOutcome> {
        if !req.lander_confirmed_dead {
            return Err(StoreError::ForceUnfreezeNotConfirmed);
        }
        self.reconcile_inner(
            ReconcileRequest {
                capsule_id: req.capsule_id,
                remote: req.remote,
            },
            Some((req.operator, req.reason)),
        )
    }

    fn reconcile_inner(
        &mut self,
        req: ReconcileRequest,
        operator: Option<(String, String)>,
    ) -> Result<ReconcileOutcome> {
        use capsule_git::ls_remote_branch;

        let (now, now_str) = now_pair()?;

        let pending_json = load_pending_land_json(&self.conn, &req.capsule_id)?;
        let Some(snapshot_json) = pending_json else {
            if let Some((op, reason)) = operator.as_ref() {
                return self.audit_force_unfreeze_on_unfrozen(&now_str, &req.capsule_id, op, reason);
            }
            return Ok(ReconcileOutcome::NotFrozen);
        };
        let pending: PendingLand = json::from_str(&snapshot_json)?;

        let witness_sha = ls_remote_branch(&req.remote, &pending.witness_branch)?;
        let witness_state = WitnessState::classify(witness_sha, &pending.verified_sha);

        let actor: &str = operator
            .as_ref()
            .map(|(op, _)| op.as_str())
            .unwrap_or(actor::RECONCILER);
        let attempt_id_i64 = pending.attempt_id as i64;
        let witness_state_json = witness_remote_state_json(&witness_state);

        let tx = self.conn.transaction()?;
        if !pending_land_snapshot_unchanged(&tx, &req.capsule_id, &snapshot_json)? {
            emit_reconciler_ran(
                &tx,
                &now_str,
                &req.capsule_id,
                Some(attempt_id_i64),
                actor,
                ReconcileOutcome::CasLost,
                &witness_state_json,
            )?;
            if let Some((op, reason)) = operator.as_ref() {
                emit_force_unfreeze_invoked(
                    &tx,
                    &now_str,
                    &req.capsule_id,
                    op,
                    reason,
                    Some(&pending),
                    ReconcileOutcome::CasLost,
                )?;
            }
            tx.commit()?;
            return Ok(ReconcileOutcome::CasLost);
        }

        let outcome = match witness_state {
            WitnessState::AtVerifiedSha(_) => {
                let advanced_base_ref = pending.verified_sha != pending.prior_base_sha;
                let landing =
                    pending.clone().into_landing(now, advanced_base_ref, actor.to_string());
                finalize_landed(&tx, &req.capsule_id, &landing, &now_str)?;
                ReconcileOutcome::Landed
            }
            WitnessState::Different(found_sha) => {
                abandon_on_witness_mismatch(&tx, &req.capsule_id, attempt_id_i64, &now_str)?;
                emit_operational_incident(
                    &tx,
                    &now_str,
                    &req.capsule_id,
                    Some(attempt_id_i64),
                    actor,
                    OperationalIncidentKind::WitnessOidMismatch,
                    json::json!({
                        "witness_branch": pending.witness_branch,
                        "expected_sha": pending.verified_sha,
                        "found_sha": found_sha,
                    }),
                )?;
                ReconcileOutcome::Abandoned
            }
            WitnessState::Absent => {
                clear_pending_land(
                    &tx,
                    &now_str,
                    &req.capsule_id,
                    Some(attempt_id_i64),
                    actor,
                    PendingLandClearedReason::WitnessAbsent,
                )?;
                ReconcileOutcome::Cleared
            }
        };

        emit_reconciler_ran(
            &tx,
            &now_str,
            &req.capsule_id,
            Some(attempt_id_i64),
            actor,
            outcome,
            &witness_state_json,
        )?;
        if let Some((op, reason)) = operator.as_ref() {
            emit_force_unfreeze_invoked(
                &tx,
                &now_str,
                &req.capsule_id,
                op,
                reason,
                Some(&pending),
                outcome,
            )?;
        }

        tx.commit()?;
        Ok(outcome)
    }

    /// `force_unfreeze` invoked on a capsule with no `pending_land`: the
    /// reconciler itself is a no-op (DESIGN §7.1.2 — reconciler scope is
    /// gated on `pending_land != null`, so no `reconciler_ran` row), but
    /// §6 still requires a `force_unfreeze_invoked` audit row whenever
    /// the operator invokes the escape hatch. Re-check `pending_land`
    /// inside the tx — a concurrent lander may have set it between the
    /// outer-tx read and now; if so the `null` snapshot we'd record is
    /// stale, surface `CasLost` and let the operator retry rather than
    /// emit a misleading `not_frozen` audit row.
    fn audit_force_unfreeze_on_unfrozen(
        &mut self,
        now_str: &str,
        capsule_id: &str,
        op: &str,
        reason: &str,
    ) -> Result<ReconcileOutcome> {
        let tx = self.conn.transaction()?;
        if is_pending_land_set_in_tx(&tx, capsule_id)? {
            tx.commit()?;
            return Ok(ReconcileOutcome::CasLost);
        }
        emit_force_unfreeze_invoked(
            &tx,
            now_str,
            capsule_id,
            op,
            reason,
            None,
            ReconcileOutcome::NotFrozen,
        )?;
        tx.commit()?;
        Ok(ReconcileOutcome::NotFrozen)
    }

    pub fn get_capsule(&self, id: &str) -> Result<Capsule> {
        let tx = self.snapshot_read_tx()?;
        let row: RowCapsule = tx
            .prepare_cached(RowCapsule::SELECT_BY_ID)?
            .query_row(params![id], RowCapsule::from_row)
            .or_not_found(id)?;
        row.into_capsule(&tx)
    }

    /// Open a read-only `Transaction` over `&self.conn` so a multi-statement
    /// read sees one snapshot — without this, separate statements each pick
    /// their own and a concurrent writer (e.g. `claim`) committing between
    /// them yields a hybrid view (pre-write parent + post-write attempts, or
    /// vice versa) that never existed atomically in the store. Brings the
    /// single-capsule read path to parity with `list_capsules`, which already
    /// wraps its reads in a `transaction()`.
    ///
    /// `unchecked_transaction` is the rusqlite escape hatch for `&Connection`
    /// callers — "unchecked" refers to compile-time borrow tracking, not
    /// runtime safety; SQLite still rejects nested transactions on the same
    /// connection. The `Transaction`'s `Drop` rolls back on scope exit, the
    /// right semantics for a read-only tx — no `commit()` needed.
    fn snapshot_read_tx(&self) -> Result<rusqlite::Transaction<'_>> {
        Ok(self.conn.unchecked_transaction()?)
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
    /// Bypass the DESIGN §8.2 deploy-verify gate. Tests and development
    /// flows pass `true`; production landers must pre-record a deploy-verify
    /// pass via `capsule deploy verify`.
    pub skip_deploy_verify_gate: bool,
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
    /// Free-text justification — DESIGN §6 force_unfreeze_invoked payload
    /// requires it for the audit trail.
    pub reason: String,
    /// Operator must confirm the lander process is dead/unresponsive
    /// before bypassing the reconciler. Without this flag the call is
    /// rejected (DESIGN.md §7.1.2 force-unfreeze precondition).
    pub lander_confirmed_dead: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

impl ReconcileOutcome {
    /// Stable wire string for event payloads. Mirrors `Status::as_wire_str` /
    /// `AttemptOutcome::as_wire_str`: the explicit `match` ensures any new
    /// variant forces a compile-time review of the wire format, where a
    /// `serde(rename_all = "snake_case")` derive would silently coin a name.
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            Self::NotFrozen => "not_frozen",
            Self::CasLost => "cas_lost",
            Self::Landed => "landed",
            Self::Abandoned => "abandoned",
            Self::Cleared => "cleared",
        }
    }
}

enum WitnessState {
    Absent,
    /// Carries the observed sha (== `pending.verified_sha` by construction)
    /// so audit rows can record it without re-joining to `PendingLand`.
    AtVerifiedSha(String),
    Different(String),
}

impl WitnessState {
    /// Classify a freshly ls-remote'd witness sha against the lander's claimed
    /// `verified_sha`: zero-OID ⇒ Absent, equal ⇒ AtVerifiedSha (the witness
    /// truly carries the lander's commit), otherwise ⇒ Different.
    fn classify(observed: String, verified: &str) -> Self {
        if observed == capsule_git::ZERO_OID {
            Self::Absent
        } else if observed == verified {
            Self::AtVerifiedSha(observed)
        } else {
            Self::Different(observed)
        }
    }

    /// Stable wire string for the `state` discriminant in the
    /// `witness_remote_state` payload (DESIGN §6 `reconciler_ran`). Mirrors
    /// `ReconcileOutcome::as_wire_str` — explicit `match` so a new variant
    /// forces compile-time review of the wire format.
    const fn state_wire_str(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::AtVerifiedSha(_) => "at_verified_sha",
            Self::Different(_) => "different",
        }
    }
}

/// Stable JSON shape for `witness_remote_state` in the `reconciler_ran`
/// event payload (DESIGN §6). Tagged so consumers can switch on `state`.
fn witness_remote_state_json(s: &WitnessState) -> json::Value {
    let state = s.state_wire_str();
    match s {
        WitnessState::Absent => json::json!({ "state": state }),
        WitnessState::AtVerifiedSha(sha) | WitnessState::Different(sha) => {
            json::json!({ "state": state, "sha": sha })
        }
    }
}

/// Emit `force_unfreeze_invoked` (DESIGN §6 line 172) with the four spec
/// keys: `{operator, reason, snapshot, post_action_outcome}`. `snapshot`
/// is `None` when the operator invoked force-unfreeze on a capsule with
/// no `pending_land` (NotFrozen path) — the audit row records the
/// attempt without a frozen state to point at.
fn emit_force_unfreeze_invoked(
    tx: &rusqlite::Transaction<'_>,
    now_str: &str,
    capsule_id: &str,
    operator: &str,
    reason: &str,
    snapshot: Option<&PendingLand>,
    outcome: ReconcileOutcome,
) -> Result<()> {
    let payload = json::json!({
        "operator": operator,
        "reason": reason,
        "snapshot": snapshot,
        "post_action_outcome": outcome.as_wire_str(),
    });
    insert_event(
        tx,
        now_str,
        capsule_id,
        snapshot.map(|s| s.attempt_id as i64),
        operator,
        EventKind::ForceUnfreezeInvoked,
        &payload,
    )
}

/// Emit a `reconciler_ran` event with the canonical
/// `{decision, witness_remote_state}` payload (DESIGN.md §6). The contract:
/// every reconciler invocation that successfully completed the witness
/// `ls-remote` and classified a witness state emits exactly one of these —
/// including CAS-lost no-ops, so audit/observability consumers can see the
/// reconciler tried even when another writer beat it. Both call sites in
/// `reconcile_inner` (CAS-lost no-op and final outcome) route through this
/// helper so the payload shape stays structurally identical and a future
/// payload-key tweak lands in one place.
fn emit_reconciler_ran(
    tx: &rusqlite::Transaction<'_>,
    now_str: &str,
    capsule_id: &str,
    attempt_id: Option<i64>,
    actor: &str,
    decision: ReconcileOutcome,
    witness_remote_state: &json::Value,
) -> Result<()> {
    let payload = json::json!({
        "decision": decision.as_wire_str(),
        "witness_remote_state": witness_remote_state,
    });
    insert_event(
        tx,
        now_str,
        capsule_id,
        attempt_id,
        actor,
        EventKind::ReconcilerRan,
        &payload,
    )
}

/// Canonical fixed `event.actor` role values. The actor column
/// is heterogeneous — it carries variable identifiers too (a worker's
/// `session_id`, the lander's id, an operator's id, the original `landed_by`
/// for reconciler-reconstructed events) — so it stays a `&str` parameter to
/// `insert_event`. These three constants cover every callsite where the
/// actor is a fixed protocol-role name, so a typo (`"systme"`,
/// `"reconcillr"`) becomes a compile error rather than a silently-shipped
/// audit-log discrepancy.
mod actor {
    pub const SYSTEM: &str = "system";
    pub const OPERATOR: &str = "operator";
    pub const RECONCILER: &str = "reconciler";
}

/// Wire-string vocabulary for `event.kind` (DESIGN.md §6). Closed enum so the
/// canonical event kinds live in one place — a typo at a callsite (e.g.
/// `"pendng_land_committed"`) would silently emit a kind no consumer expects.
/// Mirrors `OperationalIncidentKind::as_wire_str` / `Status::as_wire_str`.
///
/// Covers every event kind currently emitted by the store. DESIGN §6 also
/// forward-declares `attempt_heartbeat` (optional, high-volume) and
/// `attempt_released` (worker-initiated release); neither is emitted today,
/// so neither is a variant. Add the variant when the emit site lands.
#[derive(Clone, Copy)]
enum EventKind {
    CapsuleCreated,
    CapsuleAmended,
    AttemptClaimed,
    AttemptAttested,
    AttemptExpired,
    PendingLandCommitted,
    PendingLandCleared,
    CapsuleLanded,
    CapsuleAbandoned,
    DependencyAdded,
    DependencyRemoved,
    ForceUnfreezeInvoked,
    ReconcilerRan,
    OperationalIncident,
}

impl EventKind {
    const fn as_wire_str(self) -> &'static str {
        match self {
            Self::CapsuleCreated => "capsule_created",
            Self::CapsuleAmended => "capsule_amended",
            Self::AttemptClaimed => "attempt_claimed",
            Self::AttemptAttested => "attempt_attested",
            Self::AttemptExpired => "attempt_expired",
            Self::PendingLandCommitted => "pending_land_committed",
            Self::PendingLandCleared => "pending_land_cleared",
            Self::CapsuleLanded => "capsule_landed",
            Self::CapsuleAbandoned => "capsule_abandoned",
            Self::DependencyAdded => "dependency_added",
            Self::DependencyRemoved => "dependency_removed",
            Self::ForceUnfreezeInvoked => "force_unfreeze_invoked",
            Self::ReconcilerRan => "reconciler_ran",
            Self::OperationalIncident => "operational_incident",
        }
    }
}

/// Wire-string vocabulary for `operational_incident.kind` (DESIGN.md §6).
/// Closed enum (not `&str`) so the canonical kind names are discoverable in
/// one place and a typo can't silently coexist alongside the real ones.
#[derive(Clone, Copy)]
enum OperationalIncidentKind {
    WitnessOidMismatch,
    LandOtherFailure,
}

impl OperationalIncidentKind {
    const fn as_wire_str(self) -> &'static str {
        match self {
            Self::WitnessOidMismatch => "witness_oid_mismatch",
            Self::LandOtherFailure => "land_other_failure",
        }
    }
}

/// Emit an `operational_incident` event with the canonical `{kind, detail}`
/// payload (DESIGN.md §6). Centralizes the wrapper shape so call sites can
/// pass just the per-incident `detail` object — pre-extraction, three sites
/// (land's WitnessOidMismatch + OtherFailure, reconcile's WitnessState::Different)
/// each open-coded the wrapper, and a regression once flattened the keys
/// (the pin test in `reconcile_witness_different_records_design_compliant_payload`
/// caught it).
fn emit_operational_incident(
    tx: &rusqlite::Transaction<'_>,
    now_str: &str,
    capsule_id: &str,
    attempt_id: Option<i64>,
    actor: &str,
    kind: OperationalIncidentKind,
    detail: json::Value,
) -> Result<()> {
    let payload = json::json!({
        "kind": kind.as_wire_str(),
        "detail": detail,
    });
    insert_event(
        tx,
        now_str,
        capsule_id,
        attempt_id,
        actor,
        EventKind::OperationalIncident,
        &payload,
    )
}

/// Sweep expired leases (DESIGN.md §3.3, §7.2). Run inside any tx that may
/// observe stale `active`/`accepted` capsules. Skips capsules whose
/// `pending_land_json` is non-null (those are §7.2 reclaim-frozen).
///
/// For every matching attempt: marks `outcome=expired`, sets `closed_at=now`,
/// clears `verification_json`, sets capsule `status=planned`, clears
/// `active_attempt`, and emits an `attempt_expired` event with the DESIGN §6
/// payload `{at, prior_lease_expires_at}` (the prior expiry is pass-through
/// from `json_extract`, same canonical form `format_iso8601` would produce).
///
/// The candidate query projects only `$.expires_at` from `lease_json` — full
/// lease blob stays inside SQLite. The expiry compare happens in Rust via
/// `parse_iso8601`, not as a SQL lex-compare on the stored string: the
/// json_extract'd string is `time::serde::iso8601`'s `+0YYYYY` form while
/// `now_str` is `format_iso8601`'s 4-digit form, and `+` (0x2B) lex-sorts
/// before any digit, so a live future-expiry lease would lex-compare older
/// than `now_str` and be reclaimed prematurely.
fn reclaim_expired_in_tx(tx: &rusqlite::Transaction<'_>, now: OffsetDateTime) -> Result<()> {
    let now_str = format_iso8601(now)?;

    let mut stmt = tx.prepare_cached(concat!(
        "SELECT c.id, c.active_attempt,
                json_extract(a.lease_json, '$.expires_at')
         FROM capsule c
         JOIN attempt a
           ON a.capsule_id = c.id AND a.attempt_id = c.active_attempt
         WHERE c.status IN (",
        capsule_core::holds_lease_sql_in_list!(),
        ") AND c.active_attempt IS NOT NULL
           AND c.pending_land_json IS NULL",
    ))?;
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

    for (capsule_id, attempt_id, expires_at_str) in candidates {
        let expires_at = parse_iso8601(&expires_at_str);
        if now <= expires_at {
            continue;
        }

        close_attempt(
            tx,
            &capsule_id,
            attempt_id,
            capsule_core::AttemptOutcome::Expired,
            &now_str,
        )?;
        tx.prepare_cached(
            "UPDATE capsule
                SET status='planned',
                    active_attempt=NULL,
                    verification_json=NULL,
                    updated_at=?1
              WHERE id=?2",
        )?
        .execute(params![now_str, capsule_id])?;
        let payload = json::json!({
            "at": now_str,
            "prior_lease_expires_at": expires_at_str,
        });
        insert_event(
            tx,
            &now_str,
            &capsule_id,
            Some(attempt_id),
            actor::SYSTEM,
            EventKind::AttemptExpired,
            &payload,
        )?;
    }

    Ok(())
}

/// DESIGN.md §7.1.1 step 3: return any dep ids in `depends_on_json` that are
/// missing or not yet `landed`. `json_each` keeps everything to one bind (no
/// SQLite variable-limit ceiling) and `ORDER BY j.key` preserves the input
/// order so `UnmetDeps` reports stable positions.
fn find_unmet_deps(
    tx: &rusqlite::Transaction<'_>,
    depends_on_json: &str,
) -> Result<Vec<String>> {
    let mut stmt = tx.prepare_cached(
        "SELECT j.value FROM json_each(?1) j
         LEFT JOIN capsule c ON c.id = j.value AND c.status = 'landed'
         WHERE c.id IS NULL
         ORDER BY j.key",
    )?;
    let unmet = stmt
        .query_map(params![depends_on_json], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(unmet)
}

/// DESIGN.md §7.1.1 step 4: return the first other in-flight (lease-holding)
/// capsule whose scope component-wise overlaps `our_scope_json`, or `None`.
/// Rows are streamed so the first overlap short-circuits the cursor without
/// materializing every in-flight row into an intermediate `Vec`.
fn find_scope_conflict(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    our_scope_json: &str,
) -> Result<Option<CapsuleId>> {
    let our_scope: Vec<CanonicalPath> = json::from_str(our_scope_json)?;
    let mut stmt = tx.prepare_cached(concat!(
        "SELECT id, scope_json FROM capsule
         WHERE status IN (",
        capsule_core::holds_lease_sql_in_list!(),
        ") AND id != ?1",
    ))?;
    let mut rows = stmt.query(params![capsule_id])?;
    while let Some(row) = rows.next()? {
        let other_scope_json: String = row.get(1)?;
        let other: Vec<CanonicalPath> = json::from_str(&other_scope_json)?;
        if CanonicalPath::any_overlap(&our_scope, &other) {
            return Ok(Some(row.get(0)?));
        }
    }
    Ok(None)
}

/// DESIGN.md §7.1.1 step 5: allocate the next `attempt_id` for `capsule_id`.
/// Monotonically increasing per capsule; gaps are allowed (an aborted claim
/// leaves a row that future claims do not reuse).
fn next_attempt_id(tx: &rusqlite::Transaction<'_>, capsule_id: &str) -> Result<i64> {
    let next = tx
        .prepare_cached(
            "SELECT COALESCE(MAX(attempt_id), 0) + 1 FROM attempt WHERE capsule_id = ?1",
        )?
        .query_row(params![capsule_id], |r| r.get(0))?;
    Ok(next)
}

/// `list --available` filter: keep only `Planned` capsules whose deps are all
/// landed and whose scope does not overlap any in-flight (lease-holding)
/// capsule. Mirrors the precondition set of `Store::claim` (DESIGN.md §7.1.1)
/// — a capsule is "available" iff a fresh `claim` against it would succeed.
fn retain_available(
    tx: &rusqlite::Transaction<'_>,
    capsules: Vec<Capsule>,
) -> Result<Vec<Capsule>> {
    let landed_ids: std::collections::HashSet<String> = tx
        .prepare_cached("SELECT id FROM capsule WHERE status = 'landed'")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;
    let in_flight_scopes: Vec<(String, Vec<CanonicalPath>)> = tx
        .prepare_cached(concat!(
            "SELECT id, scope_json FROM capsule WHERE status IN (",
            capsule_core::holds_lease_sql_in_list!(),
            ")",
        ))?
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .map(|row| {
            let (id, j) = row?;
            Ok((id, json::from_str(&j)?))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(capsules
        .into_iter()
        .filter(|c| {
            c.status == Status::Planned
                && c.depends_on.iter().all(|d| landed_ids.contains(d))
                && !in_flight_scopes.iter().any(|(other_id, other_scope)| {
                    other_id != &c.id
                        && CanonicalPath::any_overlap(&c.scope_prefixes, other_scope)
                })
        })
        .collect())
}

/// Lease ownership check for `abandon`: assert the calling session owns the
/// attempt's lease. Expiry is not checked, so a worker can self-abandon after
/// losing its heartbeat grip (DESIGN.md §3.3). Projects `lease_json ->
/// session_id` via `json_extract` so the rest of the lease blob stays inside
/// SQLite — only the small session_id string crosses the rusqlite boundary,
/// and the JSON parse on the Rust side is elided entirely.
fn assert_session_owns_attempt(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    attempt_id: i64,
    session_id: &str,
) -> Result<()> {
    let owner: String = tx
        .prepare_cached(
            "SELECT json_extract(lease_json, '$.session_id')
             FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
        )?
        .query_row(params![capsule_id, attempt_id], |r| r.get(0))?;
    if owner != session_id {
        return Err(StoreError::CrossSession);
    }
    Ok(())
}

/// Single-row read for `Store::land` step 2: `(status, active_attempt,
/// frozen, verified_sha)`. Projects `pending_land_json IS NOT NULL` for the
/// freeze flag and `json_extract($.verified_sha)` from `verification_json`
/// so neither full blob crosses into Rust — saves one
/// `serde_json::from_str::<Verification>` parse per land step 2.
fn load_land_preconditions(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
) -> Result<(String, Option<i64>, bool, Option<String>)> {
    tx.prepare_cached(
        "SELECT status, active_attempt, pending_land_json IS NOT NULL,
                json_extract(verification_json, '$.verified_sha')
         FROM capsule WHERE id = ?1",
    )?
    .query_row(params![capsule_id], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    })
    .or_not_found(capsule_id)
}

/// Live-lease assertion for callers that don't consume the lease body
/// (`attest`, `land` step 2). Projects only `session_id` + `expires_at` via
/// `json_extract`, so the full lease blob stays inside SQLite — no String
/// alloc for the blob, no `serde_json::from_str::<Lease>` parse. Precedence
/// (CrossSession before LeaseExpired) matches `Store::heartbeat`'s open-coded
/// projection; both pinned by `cross_session_outranks_expired_lease`
/// (heartbeat) and `attest_cross_session_outranks_expired_lease` (this
/// helper).
fn assert_live_lease_for_session(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    attempt_id: i64,
    session_id: &str,
    now: OffsetDateTime,
) -> Result<()> {
    let (owner, expires_at_str): (String, String) = tx
        .prepare_cached(
            "SELECT json_extract(lease_json, '$.session_id'),
                    json_extract(lease_json, '$.expires_at')
             FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
        )?
        .query_row(params![capsule_id, attempt_id], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
    if owner != session_id {
        return Err(StoreError::CrossSession);
    }
    let expires_at = parse_iso8601(&expires_at_str);
    if now > expires_at {
        return Err(StoreError::LeaseExpired(expires_at_str));
    }
    Ok(())
}

/// Sister to `assert_live_lease_for_session` for `heartbeat`'s renewal path:
/// runs the same CrossSession-before-LeaseExpired precedence (pinned by
/// `cross_session_outranks_expired_lease`) but also projects `ttl_sec` for
/// the `checked_lease_expiry` recompute the predicate-only helper doesn't
/// need. Returns the ttl as `u64` after asserting non-negativity — `claim`
/// gates `ttl_sec` through `checked_lease_expiry` (which validates
/// `i64::try_from`), so a stored ttl is always in `[0, i64::MAX]`; the
/// `expect` is defense-in-depth against a corrupted row, not a real
/// runtime branch. Like the sister, the lease blob stays inside SQLite —
/// `Store::heartbeat` patches `expires_at` in place via `json_set`, so no
/// `serde_json::from_str::<Lease>` parse + re-serialize per heartbeat.
fn project_live_lease_for_renewal(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    attempt_id: i64,
    session_id: &str,
    now: OffsetDateTime,
) -> Result<u64> {
    let (owner, expires_at_str, ttl_sec): (String, String, i64) = tx
        .prepare_cached(
            "SELECT json_extract(lease_json, '$.session_id'),
                    json_extract(lease_json, '$.expires_at'),
                    json_extract(lease_json, '$.ttl_sec')
             FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
        )?
        .query_row(params![capsule_id, attempt_id], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    if owner != session_id {
        return Err(StoreError::CrossSession);
    }
    let expires_at = parse_iso8601(&expires_at_str);
    if now > expires_at {
        return Err(StoreError::LeaseExpired(expires_at_str));
    }
    Ok(u64::try_from(ttl_sec).expect("claim stores ttl_sec as a non-negative i64"))
}

/// Read `pending_land_json` for a capsule. Returns `Ok(None)` when the column
/// is NULL, `Err(NotFound)` if the capsule does not exist. Used when the
/// caller actually needs the JSON body — `reconcile_inner` deserializes it
/// into `PendingLand` to drive the witness comparison (DESIGN.md §7.1.2).
/// Callers that only need snapshot equality should use
/// `pending_land_snapshot_unchanged`, which keeps the blob inside SQLite.
/// Accepts `&Connection` so that `&Transaction` (which derefs to
/// `&Connection`) callers work without per-shape duplication.
fn load_pending_land_json(
    conn: &rusqlite::Connection,
    capsule_id: &str,
) -> Result<Option<String>> {
    conn.prepare_cached("SELECT pending_land_json FROM capsule WHERE id = ?1")?
        .query_row(params![capsule_id], |r| r.get(0))
        .or_not_found(capsule_id)
}

/// CAS check on `pending_land_json`: true iff the in-tx column still equals
/// the bytes the caller snapshotted earlier. False ⇒ a concurrent reconcile
/// / force_unfreeze / lander finalized this same pending; the observable git
/// state has already been recorded by the winner. Both `Store::land` step 4
/// and `reconcile_inner` gate their finalize work on this (§7.1.2 / §7.2).
///
/// SQL-side byte compare keeps the JSON blob inside SQLite — the
/// `IS NOT NULL AND = ?2` projection lands directly as a 0/1 boolean. The
/// explicit `IS NOT NULL` guard normalizes NULL to `false`: under SQLite's
/// three-valued logic `NULL = ?2` is itself NULL (not 0) and would surface
/// to Rust as a row-decode error instead of "freeze cleared, snapshot does
/// not match".
fn pending_land_snapshot_unchanged(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    snapshot_json: &str,
) -> Result<bool> {
    tx.prepare_cached(
        "SELECT pending_land_json IS NOT NULL AND pending_land_json = ?2
         FROM capsule WHERE id = ?1",
    )?
    .query_row(params![capsule_id, snapshot_json], |r| r.get(0))
    .or_not_found(capsule_id)
}

/// Serialize `PendingLand` once and hand back both the typed `Value`
/// (for the `pending_land_committed` event payload) and its `String`
/// form (the `pending_land_json` column bytes). Step-4's CAS via
/// `pending_land_snapshot_unchanged` byte-compares the column against
/// the snapshot the lander captured here, so a single serialization
/// guarantees the audit row and the CAS subject are byte-identical by
/// construction — diverging serializations would let the CAS pass
/// while the audit row references a different snapshot.
fn serialize_pending_for_cas_and_audit(p: &PendingLand) -> Result<(json::Value, String)> {
    let v = json::to_value(p)?;
    let s = v.to_string();
    Ok((v, s))
}

/// SQL-side existence check for the §7.2 reclaim/abandon freeze flag,
/// projecting `pending_land_json IS NOT NULL` so the JSON payload never
/// crosses into Rust when only the boolean matters. Standalone in-tx
/// boolean checks share this; wider precondition reads project the same
/// expression inline because they already fetch adjacent columns.
fn is_pending_land_set_in_tx(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
) -> Result<bool> {
    tx.prepare_cached("SELECT pending_land_json IS NOT NULL FROM capsule WHERE id = ?1")?
        .query_row(params![capsule_id], |r| r.get(0))
        .or_not_found(capsule_id)
}

/// Reject any state change that would race with an in-flight land. Read +
/// check share one tx snapshot so a concurrent `land` between the two reads
/// can't produce `Ok(false)` here when `PendingLandFrozen` is the right
/// answer (§7.2). Used by `reclaim`; `claim`/`abandon`/`land` open-code the
/// same check because they read the freeze flag in a wider column-set query.
fn assert_not_pending_land_frozen_in_tx(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
) -> Result<()> {
    if is_pending_land_set_in_tx(tx, capsule_id)? {
        return Err(StoreError::PendingLandFrozen(capsule_id.into()));
    }
    Ok(())
}

/// Shared preamble for `add_dep`/`remove_dep`: load `depends_on_json` and
/// short-circuit on terminal capsules. Returns:
/// - `Err(NotFound)` if the capsule does not exist;
/// - `Ok(None)` if the capsule is in a terminal state (§7.1.3 no-op signal);
/// - `Ok(Some(deps))` otherwise — caller mutates and writes back.
fn load_deps_for_mutation(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
) -> Result<Option<Vec<String>>> {
    let (status_str, deps_json): (String, String) = tx
        .prepare_cached("SELECT status, depends_on_json FROM capsule WHERE id = ?1")?
        .query_row(params![capsule_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .or_not_found(capsule_id)?;
    if parse_status(&status_str).is_terminal() {
        return Ok(None);
    }
    Ok(Some(json::from_str(&deps_json)?))
}

/// True iff adding `capsule_id -> new_dep` to the depends_on graph would
/// close a cycle (DESIGN.md §7.1.3). Equivalent to: is `capsule_id`
/// reachable from `new_dep`? Self-dep falls out reflexively from
/// `reachable(tx, x, x) == true`.
fn creates_cycle(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    new_dep: &str,
) -> Result<bool> {
    reachable(tx, new_dep, capsule_id)
}

/// BFS over the `depends_on` graph from `from` looking for `target`.
/// Reachability is reflexive: `reachable(tx, x, x)` returns true (the
/// graph-theory definition — `creates_cycle` relies on this so the
/// self-dep case is not a separate branch). Preserve this when refactoring.
///
/// `seen` is populated at push-time, not pop-time. Each reachable capsule is
/// enqueued at most once, dropping the pop-time `seen.contains` check and
/// avoiding duplicate enqueues for diamond-shaped subgraphs.
///
/// Per-node neighbor expansion uses `json_each` against the row's
/// `depends_on_json`, so the deps array stays inside SQLite — no
/// `serde_json::from_str` per pop. A missing row (capsule not in DB) yields
/// zero join rows, matching the prior `.optional()` skip path.
///
/// The explicit `drop(stmt)` after the per-pop `query_map` releases the
/// `prepare_cached` borrow on `tx` before the next pop's `prepare_cached`
/// call. A trailing-expression block scope hit `clippy::let_and_return`
/// vs E0597 (the `Rows` iterator borrows `stmt`); explicit drop is the
/// cleanest form.
fn reachable(tx: &rusqlite::Transaction<'_>, from: &str, target: &str) -> Result<bool> {
    use std::collections::{HashSet, VecDeque};
    let mut seen: HashSet<String> = HashSet::new();
    let mut q: VecDeque<String> = VecDeque::new();
    seen.insert(from.to_string());
    q.push_back(from.to_string());
    while let Some(node) = q.pop_front() {
        if node == target {
            return Ok(true);
        }
        let mut stmt = tx.prepare_cached(
            "SELECT j.value FROM capsule c, json_each(c.depends_on_json) j
             WHERE c.id = ?1",
        )?;
        let deps = stmt
            .query_map(params![node], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        drop(stmt);
        for d in deps {
            if seen.insert(d.clone()) {
                q.push_back(d);
            }
        }
    }
    Ok(false)
}

/// Same-shape equality for the verification gate. Cross-shape mismatches are
/// enumerated explicitly (not `_ => false`) so that adding a variant to
/// either `ExpectExit` or `ExitCode` forces compile-time review here — same
/// exhaustive-match discipline as `Status::is_terminal`.
fn exit_codes_match(expect: &capsule_core::ExpectExit, got: &capsule_core::ExitCode) -> bool {
    use capsule_core::{ExitCode, ExpectExit};
    match (expect, got) {
        (ExpectExit::Code(a), ExitCode::Code(b)) => a == b,
        (ExpectExit::Sentinel(a), ExitCode::Sentinel(b)) => a == b,
        (ExpectExit::Code(_), ExitCode::Sentinel(_))
        | (ExpectExit::Sentinel(_), ExitCode::Code(_)) => false,
    }
}

/// The three fields `Store::land` step 1 needs from the in-memory capsule:
/// `base_ref` to ls-remote, `witness_branch` for the atomic push, `verified_sha`
/// for both the push target and the in-tx re-bind. `extract` returns `Some`
/// only when verification + active_attempt are both present; any missing
/// piece collapses to `NotLandable` at the call site.
struct LandableSnapshot {
    base_ref: String,
    witness_branch: String,
    verified_sha: String,
}

impl LandableSnapshot {
    /// Consumes the capsule so land step 1 can move out the three needed
    /// String fields instead of cloning them.
    fn extract(mut cap: capsule_core::Capsule) -> Option<Self> {
        let aid = cap.active_attempt?;
        let v = cap.verification.take()?;
        let pos = cap.attempts.iter().position(|a| a.id == aid)?;
        let att = cap.attempts.swap_remove(pos);
        Some(Self {
            base_ref: cap.base_ref,
            witness_branch: att.witness_branch,
            verified_sha: v.verified_sha,
        })
    }
}

#[derive(serde::Serialize)]
struct CreatedPayload<'a> {
    acceptance: &'a Acceptance,
    scope_prefixes: &'a [CanonicalPath],
    base_ref: &'a str,
    depends_on: &'a [CapsuleId],
}

/// `attempt_claimed` event payload (DESIGN.md §6). The event row's
/// `attempt_id` column duplicates `attempt_id` here so the audit log can be
/// filtered without parsing JSON.
#[derive(serde::Serialize)]
struct ClaimedPayload<'a> {
    attempt_id: i64,
    session_id: &'a str,
    base_sha: &'a str,
    lease: &'a capsule_core::Lease,
}

/// `attempt_attested` event payload (DESIGN.md §6) — the run inputs and
/// outputs. The event row's `at` / `actor` / `attempt_id` columns carry what
/// `Verification` (§5) calls `at` / `attestor` / `attempt_id`, so they are
/// not duplicated in this struct.
#[derive(serde::Serialize)]
struct AttestedPayload<'a> {
    verified_sha: &'a str,
    exit_code: &'a capsule_core::ExitCode,
    command: &'a str,
    log_ref: &'a str,
    duration_ms: u64,
}

/// Builder for `Store::amend`'s parallel writes: each field set goes into
/// the SQL UPDATE statement (`sets` + `vals`) and the audit-event diff
/// (`diff`) in lockstep, so a partial update can't drift between SQL and
/// event log.
#[derive(Default)]
struct AmendUpdate {
    sets: Vec<String>,
    vals: Vec<rusqlite::types::Value>,
    diff: json::Map<String, json::Value>,
}

impl AmendUpdate {
    fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    fn next_placeholder(&self) -> usize {
        self.vals.len() + 1
    }

    fn bind_sql_only(&mut self, col: &str, val: rusqlite::types::Value) {
        self.sets.push(format!("{col} = ?{}", self.next_placeholder()));
        self.vals.push(val);
    }

    fn set_string(&mut self, col: &str, diff_key: &str, value: String) {
        self.bind_sql_only(col, value.clone().into());
        self.diff.insert(diff_key.into(), json::Value::String(value));
    }

    /// Serialize once into a `Value`, then bind both the audit diff (which
    /// keeps the typed `Value`) and the SQL column (which derives compact
    /// JSON text from it). Output is semantically equivalent to a direct
    /// `to_string`, not byte-identical (the `Value` path emits object keys
    /// in sorted order — fine since reads round-trip via `from_str`).
    fn set_json<T: serde::Serialize>(
        &mut self,
        col: &str,
        diff_key: &str,
        value: &T,
    ) -> Result<()> {
        let v = json::to_value(value)?;
        self.bind_sql_only(col, v.to_string().into());
        self.diff.insert(diff_key.into(), v);
        Ok(())
    }
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

/// Single source of truth for the capsule SELECT clause — used by
/// `list_capsules` (no WHERE / `WHERE status`) and `get_capsule` (`WHERE id`).
/// `RowCapsule::from_row` reads columns by name (not position), so a future
/// SELECT reorder or insertion cannot silently shift any caller. Wrapped in
/// a `macro_rules!` so the trailing-clause variants below can be `concat!`-ed
/// at compile time, dropping a `format!` per read-path call.
macro_rules! select_capsule_sql {
    () => {
        "SELECT id, title, description, acceptance_json, scope_json, base_ref,
                depends_on_json, status, active_attempt, verification_json,
                pending_land_json, landing_json, created_at, updated_at
         FROM capsule"
    };
}

impl RowCapsule {
    const SELECT_BY_ID: &'static str = concat!(select_capsule_sql!(), " WHERE id = ?1");
    const SELECT_ALL_ORDERED: &'static str =
        concat!(select_capsule_sql!(), " ORDER BY created_at ASC");
    const SELECT_BY_STATUS_ORDERED: &'static str = concat!(
        select_capsule_sql!(),
        " WHERE status = ?1 ORDER BY created_at ASC"
    );

    fn from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get("id")?,
            title: r.get("title")?,
            description: r.get("description")?,
            acceptance_json: r.get("acceptance_json")?,
            scope_json: r.get("scope_json")?,
            base_ref: r.get("base_ref")?,
            depends_on_json: r.get("depends_on_json")?,
            status: r.get("status")?,
            active_attempt: r.get("active_attempt")?,
            verification_json: r.get("verification_json")?,
            pending_land_json: r.get("pending_land_json")?,
            landing_json: r.get("landing_json")?,
            created_at: r.get("created_at")?,
            updated_at: r.get("updated_at")?,
        })
    }

    fn into_capsule(self, conn: &Connection) -> Result<Capsule> {
        let attempts = load_attempts_for_capsule(conn, &self.id)?;
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
            verification: decode_opt_json(self.verification_json)?,
            pending_land: decode_opt_json(self.pending_land_json)?,
            landing: decode_opt_json(self.landing_json)?,
            created_at: parse_iso8601(&self.created_at),
            updated_at: parse_iso8601(&self.updated_at),
        })
    }
}

/// Load all attempts for one capsule, ordered by `attempt_id ASC`. Single
/// source of truth for the SELECT column list against `attempt`; drift between
/// the column list and the `Attempt` field assignments would silently corrupt
/// rehydration. Sibling of `select_capsule_sql!` / `RowCapsule::from_row`.
///
/// Streamed rather than `query_map`-collected: `query_map`'s closure must
/// return `rusqlite::Result`, but `into_attempt` raises `StoreError` on
/// `lease_json` decode, so a one-pass `while let Some(row) = rows.next()?`
/// lets both error kinds short-circuit the loop without an intermediate Vec.
fn load_attempts_for_capsule(
    conn: &Connection,
    capsule_id: &str,
) -> Result<Vec<capsule_core::Attempt>> {
    let mut stmt = conn.prepare_cached(RowAttempt::SELECT)?;
    let mut rows = stmt.query(params![capsule_id])?;
    let mut attempts = Vec::new();
    while let Some(row) = rows.next()? {
        let raw = RowAttempt::from_row(row)?;
        attempts.push(raw.into_attempt()?);
    }
    Ok(attempts)
}

struct RowAttempt {
    attempt_id: i64,
    lease_json: String,
    branch: String,
    witness_branch: String,
    base_sha: String,
    tip_sha: Option<String>,
    last_heartbeat: String,
    outcome: String,
    opened_at: String,
    closed_at: Option<String>,
}

impl RowAttempt {
    /// Sibling of `select_capsule_sql!`. Read by name in `from_row` so a
    /// future column reorder is safe.
    const SELECT: &'static str = "SELECT attempt_id, lease_json, branch, witness_branch, base_sha,
                tip_sha, last_heartbeat, outcome, opened_at, closed_at
         FROM attempt WHERE capsule_id = ?1 ORDER BY attempt_id ASC";

    fn from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            attempt_id: r.get("attempt_id")?,
            lease_json: r.get("lease_json")?,
            branch: r.get("branch")?,
            witness_branch: r.get("witness_branch")?,
            base_sha: r.get("base_sha")?,
            tip_sha: r.get("tip_sha")?,
            last_heartbeat: r.get("last_heartbeat")?,
            outcome: r.get("outcome")?,
            opened_at: r.get("opened_at")?,
            closed_at: r.get("closed_at")?,
        })
    }

    fn into_attempt(self) -> Result<capsule_core::Attempt> {
        Ok(capsule_core::Attempt {
            id: self.attempt_id as u64,
            lease: json::from_str(&self.lease_json)?,
            branch: self.branch,
            witness_branch: self.witness_branch,
            base_sha: self.base_sha,
            tip_sha: self.tip_sha,
            last_heartbeat: parse_iso8601(&self.last_heartbeat),
            outcome: parse_outcome(&self.outcome),
            opened_at: parse_iso8601(&self.opened_at),
            closed_at: self.closed_at.as_deref().map(parse_iso8601),
        })
    }
}

/// Read-side wire-string parser shared by `parse_status` and `parse_outcome`.
///
/// Wire round-trips live on the enums in `capsule_core::model`; the SQL CHECK
/// constraints on `capsule.status` and `attempt.outcome` enforce membership, so
/// a `None` here ⇒ DB corruption (or migration mismatch). Panics loudly with
/// the offending value rather than letting callers paper over it with their
/// own `.unwrap_or_else(...)` at every read site.
fn parse_wire<T>(kind: &str, value: &str, parse: impl FnOnce(&str) -> Option<T>) -> T {
    parse(value).unwrap_or_else(|| panic!("unknown {kind} in DB: {value}"))
}

fn parse_status(s: &str) -> Status {
    parse_wire("status", s, Status::from_wire)
}

fn parse_outcome(s: &str) -> capsule_core::AttemptOutcome {
    parse_wire("attempt outcome", s, capsule_core::AttemptOutcome::from_wire)
}

fn format_iso8601(t: OffsetDateTime) -> Result<String> {
    Ok(t.format(&time::format_description::well_known::Iso8601::DEFAULT)?)
}

/// One `now_utc()` reading and its ISO-8601 form for DB writes. Use when a
/// mutation needs both the in-memory `OffsetDateTime` (e.g. for `Lease`
/// fields, in-tx lease checks) and the string bound into `updated_at` / event
/// rows, so both values come from the same clock read. Sites that need only
/// the string discard with `let (_, now_str) = now_pair()?;`. Sites that need
/// only the instant should call `OffsetDateTime::now_utc()` directly — the
/// helper's centralized format pass is wasted there.
fn now_pair() -> Result<(OffsetDateTime, String)> {
    let now = OffsetDateTime::now_utc();
    let now_str = format_iso8601(now)?;
    Ok((now, now_str))
}

/// `now + ttl_sec`, rejecting both u64→i64 wrap and OffsetDateTime overflow as
/// `InvalidLeaseTtl`. Pre-fix `claim` and `heartbeat` cast u64→i64 unchecked,
/// then added without `checked_add` — pathological TTLs either wrapped to a
/// negative Duration (lease born expired) or panicked in the time crate.
fn checked_lease_expiry(now: OffsetDateTime, ttl_sec: u64) -> Result<OffsetDateTime> {
    let ttl_i64 = i64::try_from(ttl_sec).map_err(|_| StoreError::InvalidLeaseTtl(ttl_sec))?;
    now.checked_add(time::Duration::seconds(ttl_i64))
        .ok_or(StoreError::InvalidLeaseTtl(ttl_sec))
}

fn parse_iso8601(s: &str) -> OffsetDateTime {
    OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
        .expect("DB stored a non-iso8601 timestamp")
}

/// Decode a nullable JSON column. The three nullable JSON-backed fields on
/// `Capsule` (verification, pending_land, landing) all share the same
/// `Option<String>` → `Option<T>` shape; centralizing keeps the
/// `.map(...).transpose()?` ritual off the call sites and limits the
/// `serde_json::Error → StoreError` conversion to one place.
fn decode_opt_json<T: serde::de::DeserializeOwned>(s: Option<String>) -> Result<Option<T>> {
    s.map(|s| json::from_str::<T>(&s))
        .transpose()
        .map_err(Into::into)
}

/// True iff a row with this id exists in `capsule`. `id` is the primary key,
/// so this is an indexed lookup. (Distinct from `capsule_core::id::validate`,
/// which is a syntactic check on the id string.)
/// `EXISTS(SELECT 1 ...)` always yields exactly one row holding 0/1, so the
/// `.optional()? .unwrap_or(false)` indirection the callsite-once row-presence
/// shape needed disappears — one `query_row` returning `bool` suffices.
fn capsule_exists(tx: &rusqlite::Transaction<'_>, id: &str) -> Result<bool> {
    Ok(tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM capsule WHERE id = ?1)")?
        .query_row(params![id], |r| r.get(0))?)
}

/// Persist a dependency-edge change: write the new `depends_on_json` and
/// emit a `dependency_added`/`dependency_removed` event with payload
/// `{dep_id}` (DESIGN.md §6 event taxonomy). Both `add_dep` and `remove_dep`
/// share this tail (DESIGN.md §7.1.3); the head — load, mutate, early-return
/// on no-op — stays per-method since the predicates differ.
fn persist_dep_change(
    tx: &rusqlite::Transaction<'_>,
    now_str: &str,
    capsule_id: &str,
    deps_json: &str,
    event_kind: EventKind,
    dep_id: &str,
) -> Result<()> {
    tx.prepare_cached("UPDATE capsule SET depends_on_json=?1, updated_at=?2 WHERE id=?3")?
        .execute(params![deps_json, now_str, capsule_id])?;
    let payload = json::json!({ "dep_id": dep_id });
    insert_event(tx, now_str, capsule_id, None, actor::SYSTEM, event_kind, &payload)?;
    Ok(())
}

/// Apply the DESIGN §7.1.2 landed-state transition atomically: persist
/// `landing` and clear `pending_land_json`, close the active attempt with
/// `Landed`, and emit the canonical `capsule_landed` event with the landing
/// JSON. Used by `land` step 4 (`GitOutcome::Advanced` / `NoOp` arms) and
/// `reconcile_inner` (`WitnessState::AtVerifiedSha`); both branches construct
/// a `Landing` from slightly different sources (live push outcome vs. crash
/// recovery from `PendingLand`) but the persistence shape is identical, and
/// the event payload IS the landing JSON.
fn finalize_landed(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    landing: &Landing,
    now_str: &str,
) -> Result<()> {
    let landing_value = json::to_value(landing)?;
    let landing_json = landing_value.to_string();
    let attempt_id = landing.attempt_id as i64;
    tx.prepare_cached(
        "UPDATE capsule
            SET status='landed',
                landing_json=?1,
                pending_land_json=NULL,
                updated_at=?2
          WHERE id=?3",
    )?
    .execute(params![landing_json, now_str, capsule_id])?;
    close_attempt(
        tx,
        capsule_id,
        attempt_id,
        capsule_core::AttemptOutcome::Landed,
        now_str,
    )?;
    insert_event(
        tx,
        now_str,
        capsule_id,
        Some(attempt_id),
        &landing.landed_by,
        EventKind::CapsuleLanded,
        &landing_value,
    )
}

/// Abandon a capsule whose witness ref disagrees with the snapshot — used by
/// `land` step 4 (`WitnessOidMismatch`) and `reconcile_inner`
/// (`WitnessState::Different`). Both paths must atomically: flip status to
/// `abandoned`, clear `pending_land_json` (no longer frozen), and close the
/// active attempt with `Abandoned`. Distinct from `abandon`, which clears
/// `active_attempt` instead — that path has no pending_land.
fn abandon_on_witness_mismatch(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    attempt_id: i64,
    now_str: &str,
) -> Result<()> {
    tx.prepare_cached(
        "UPDATE capsule
            SET status='abandoned',
                pending_land_json=NULL,
                updated_at=?1
          WHERE id=?2",
    )?
    .execute(params![now_str, capsule_id])?;
    close_attempt(
        tx,
        capsule_id,
        attempt_id,
        capsule_core::AttemptOutcome::Abandoned,
        now_str,
    )
}

/// Mark an attempt terminal by setting `outcome` and `closed_at`. Caller
/// must pass a terminal `AttemptOutcome` (`Landed`/`Abandoned`/`Expired`);
/// `closed_at` is meaningless for `InFlight`/`Released`. Cached: shared by
/// four callers and runs inside `reclaim_expired_in_tx`'s per-attempt loop.
fn close_attempt(
    tx: &rusqlite::Transaction<'_>,
    capsule_id: &str,
    attempt_id: i64,
    outcome: capsule_core::AttemptOutcome,
    now_str: &str,
) -> Result<()> {
    debug_assert!(
        outcome.is_terminal(),
        "close_attempt called with non-terminal outcome {outcome:?}",
    );
    tx.prepare_cached(
        "UPDATE attempt SET outcome=?1, closed_at=?2
         WHERE capsule_id=?3 AND attempt_id=?4",
    )?
    .execute(params![outcome.as_wire_str(), now_str, capsule_id, attempt_id])?;
    Ok(())
}

/// Wire-string vocabulary for `pending_land_cleared.reason` (DESIGN.md §6).
/// Closed enum (not `&str`) so the canonical reasons are discoverable in one
/// place and a typo can't silently coexist with the wire-pinned vocabulary.
/// Mirrors the `OperationalIncidentKind` precedent in this file.
#[derive(Clone, Copy)]
enum PendingLandClearedReason {
    BaseRefMoved,
    OtherFailure,
    WitnessAbsent,
}

impl PendingLandClearedReason {
    const fn as_wire_str(self) -> &'static str {
        match self {
            Self::BaseRefMoved => "base_ref_moved",
            Self::OtherFailure => "other_failure",
            Self::WitnessAbsent => "witness_absent",
        }
    }
}

/// Clear `pending_land_json` and emit the matching `pending_land_cleared`
/// audit event with the canonical `{reason, by}` payload (DESIGN.md §6) in
/// one step. Used by every site that drops a pending entry without
/// finalizing a Landing — `land`'s BaseRefMoved and OtherFailure arms
/// (DESIGN.md §7.1.2) and `reconcile_inner`'s WitnessAbsent arm. The three
/// sites previously open-coded the (UPDATE + emit) pair, which risked one
/// site clearing without auditing on a future edit. Audit is the invariant:
/// there is no caller that clears without emitting. `by` stays in the JSON
/// payload for the on-wire shape DESIGN §6 pins, even though it duplicates
/// the event row's `actor` column.
fn clear_pending_land(
    tx: &rusqlite::Transaction<'_>,
    now_str: &str,
    capsule_id: &str,
    attempt_id: Option<i64>,
    by: &str,
    reason: PendingLandClearedReason,
) -> Result<()> {
    tx.prepare_cached("UPDATE capsule SET pending_land_json=NULL, updated_at=?1 WHERE id=?2")?
        .execute(params![now_str, capsule_id])?;
    let payload = json::json!({
        "reason": reason.as_wire_str(),
        "by": by,
    });
    insert_event(
        tx,
        now_str,
        capsule_id,
        attempt_id,
        by,
        EventKind::PendingLandCleared,
        &payload,
    )
}

/// `Store::land` step 4: handle a push failure that DESIGN §7.1.2 step 5 does
/// not enumerate (i.e. neither base_ref_moved nor witness_oid_mismatch).
/// Treat as transient — clear `pending_land` so the caller can retry without
/// manual unfreeze, and emit a paired `operational_incident` carrying the
/// stderr. The pair-emit exists because §6 pins `pending_land_cleared` to a
/// slim `{reason, by}` shape, so the diagnostic rides a separate event row.
fn record_transient_land_failure(
    tx: &rusqlite::Transaction<'_>,
    now_str: &str,
    capsule_id: &str,
    attempt_id: i64,
    by: &str,
    stderr: &str,
) -> Result<()> {
    clear_pending_land(
        tx,
        now_str,
        capsule_id,
        Some(attempt_id),
        by,
        PendingLandClearedReason::OtherFailure,
    )?;
    emit_operational_incident(
        tx,
        now_str,
        capsule_id,
        Some(attempt_id),
        by,
        OperationalIncidentKind::LandOtherFailure,
        json::json!({ "stderr": stderr }),
    )
}

/// Serialize the event payload and append it to the `event` table.
///
/// Takes a `&json::Value` (not `&str`, not `&impl Serialize`) so that an
/// already-stringified JSON String cannot be passed in by mistake — that would
/// double-encode the audit row (`"\"...\""` instead of `{...}`). The type
/// system enforces shape, not provenance: don't `from_str` already-rendered
/// JSON just to satisfy the type — convert from the typed source via
/// `json::to_value(&t)?` so the conversion is visible.
///
/// Note on bytes: payloads built from typed structs via `to_value` round-trip
/// through `Value::Object` (BTreeMap-sorted keys with default `serde_json`),
/// so the on-wire key order is alphabetical, not struct declaration order.
/// DESIGN.md §6 pins payload shape, not byte order; consumers parse via
/// `from_str` and are order-agnostic.
///
/// Uses `prepare_cached`: every state-changing op emits ≥1 event, so the
/// prepared form amortizes across the whole Store lifetime; SQL is invariant.
fn insert_event(
    tx: &rusqlite::Transaction<'_>,
    at: &str,
    capsule_id: &str,
    attempt_id: Option<i64>,
    actor: &str,
    kind: EventKind,
    payload: &json::Value,
) -> Result<()> {
    let payload_json = json::to_string(payload)?;
    tx.prepare_cached(
        "INSERT INTO event (at, capsule_id, attempt_id, actor, kind, payload_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?
    .execute(params![
        at,
        capsule_id,
        attempt_id,
        actor,
        kind.as_wire_str(),
        payload_json
    ])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic 40-hex sha for tests that don't push to a real remote.
    /// Real tests against a bare repo build their own via git rev-parse.
    const FAKE_SHA: &str = "1111111111111111111111111111111111111111";

    fn tmp_store() -> Store {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let s = Store::open(&path).unwrap();
        std::mem::forget(dir);
        s
    }

    /// Read the unique `event.payload_json` row matching `(capsule_id, kind)`,
    /// parsed as `json::Value`. Asserts exactly one match — pre-extraction the
    /// open-coded sites used `query_row`, which silently returns the first row
    /// when duplicates exist; that would mask a regression where the same event
    /// fires twice. The DESIGN §6 pin tests all expect a single emission, so
    /// the cardinality assert encodes that intent. Tests with legitimate
    /// multi-emission scenarios should query directly so order is explicit.
    fn read_event_payload(s: &Store, capsule_id: &str, kind: &str) -> json::Value {
        let payloads: Vec<String> = s
            .conn
            .prepare(
                "SELECT payload_json FROM event
                 WHERE capsule_id = ?1 AND kind = ?2
                 ORDER BY rowid
                 LIMIT 2",
            )
            .unwrap()
            .query_map(params![capsule_id, kind], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            payloads.len(),
            1,
            "expected exactly one {kind} event for capsule {capsule_id}",
        );
        json::from_str(&payloads[0]).unwrap()
    }

    #[test]
    fn open_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let _ = Store::open(&path).unwrap();
        let _ = Store::open(&path).unwrap();
    }

    /// `actor` module constants ship verbatim to the `event.actor` column
    /// (DESIGN §6). Same drift concern as the wire-string enums below: a
    /// typo (`"sytsem"`, `"recon"`) would silently rewrite the audit-log
    /// vocabulary downstream consumers grep on. (No closed-set test on
    /// `event.actor` itself — the column is heterogeneous by design;
    /// principal IDs like worker `session_id` and `landing.landed_by` are
    /// also written through `insert_event`'s `actor: &str`.)
    #[test]
    fn actor_wire_strings_pinned() {
        assert_eq!(actor::SYSTEM, "system");
        assert_eq!(actor::OPERATOR, "operator");
        assert_eq!(actor::RECONCILER, "reconciler");
    }

    /// Wire-format pin for the enums whose `as_wire_str` strings ship to
    /// external consumers (audit-event payloads, --json CLI output, error
    /// messages). A typo or rename in production call sites would propagate
    /// silently; the assertions below freeze the (variant, wire) table so
    /// such drift is a test failure.
    #[test]
    fn store_op_wire_table_pinned() {
        let cases = [
            (StoreOp::Claim, "claim"),
            (StoreOp::Attest, "attest"),
            (StoreOp::Heartbeat, "heartbeat"),
            (StoreOp::Land, "land"),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
        }
    }

    /// `EventKind` is the subset of DESIGN §6 currently emitted by the
    /// store; `attempt_heartbeat` / `attempt_released` are spec'd but not
    /// yet wired up.
    #[test]
    fn event_kind_wire_table_pinned() {
        let cases = [
            (EventKind::CapsuleCreated, "capsule_created"),
            (EventKind::CapsuleAmended, "capsule_amended"),
            (EventKind::AttemptClaimed, "attempt_claimed"),
            (EventKind::AttemptAttested, "attempt_attested"),
            (EventKind::AttemptExpired, "attempt_expired"),
            (EventKind::PendingLandCommitted, "pending_land_committed"),
            (EventKind::PendingLandCleared, "pending_land_cleared"),
            (EventKind::CapsuleLanded, "capsule_landed"),
            (EventKind::CapsuleAbandoned, "capsule_abandoned"),
            (EventKind::DependencyAdded, "dependency_added"),
            (EventKind::DependencyRemoved, "dependency_removed"),
            (EventKind::ForceUnfreezeInvoked, "force_unfreeze_invoked"),
            (EventKind::ReconcilerRan, "reconciler_ran"),
            (EventKind::OperationalIncident, "operational_incident"),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
        }
    }

    #[test]
    fn reconcile_outcome_wire_table_pinned() {
        let cases = [
            (ReconcileOutcome::NotFrozen, "not_frozen"),
            (ReconcileOutcome::CasLost, "cas_lost"),
            (ReconcileOutcome::Landed, "landed"),
            (ReconcileOutcome::Abandoned, "abandoned"),
            (ReconcileOutcome::Cleared, "cleared"),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
        }
    }

    #[test]
    fn operational_incident_kind_wire_table_pinned() {
        let cases = [
            (
                OperationalIncidentKind::WitnessOidMismatch,
                "witness_oid_mismatch",
            ),
            (
                OperationalIncidentKind::LandOtherFailure,
                "land_other_failure",
            ),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
        }
    }

    #[test]
    fn pending_land_cleared_reason_wire_table_pinned() {
        let cases = [
            (PendingLandClearedReason::BaseRefMoved, "base_ref_moved"),
            (PendingLandClearedReason::OtherFailure, "other_failure"),
            (PendingLandClearedReason::WitnessAbsent, "witness_absent"),
        ];
        for (v, wire) in cases {
            assert_eq!(v.as_wire_str(), wire);
        }
    }

    /// Pin `(variant, wire)` for `WitnessState::state_wire_str`. The strings
    /// surface as the `state` field of `witness_remote_state` in the
    /// `reconciler_ran` event payload (DESIGN §6) — operator dashboards and
    /// audit consumers key on these. Also pins payload shape: the `sha` field
    /// is present iff the state variant carries one. Mirrors the
    /// `reconcile_outcome_wire_table_pinned` pattern.
    #[test]
    fn witness_state_wire_table_pinned() {
        let cases = [
            (WitnessState::Absent, "absent"),
            (WitnessState::AtVerifiedSha("dead".into()), "at_verified_sha"),
            (WitnessState::Different("beef".into()), "different"),
        ];
        for (v, wire) in &cases {
            assert_eq!(v.state_wire_str(), *wire);
        }
        assert_eq!(
            witness_remote_state_json(&WitnessState::Absent),
            json::json!({ "state": "absent" })
        );
        assert_eq!(
            witness_remote_state_json(&WitnessState::AtVerifiedSha("aa".into())),
            json::json!({ "state": "at_verified_sha", "sha": "aa" })
        );
        assert_eq!(
            witness_remote_state_json(&WitnessState::Different("bb".into())),
            json::json!({ "state": "different", "sha": "bb" })
        );
    }

    /// Direct truth-table for `WitnessState::classify`. Today it's only hit
    /// through `Store::reconcile`, so a regression in the precedence (e.g.
    /// reordering the if/else so observed == verified is checked before
    /// observed == ZERO_OID) would slip past unit tests until an integration
    /// test happened to exercise it. Pin all three branches + the
    /// load-bearing Absent-wins precedence: ZERO_OID observed always
    /// classifies as Absent, never as AtVerifiedSha, even in the degenerate
    /// case where `verified` itself is ZERO_OID.
    #[test]
    fn witness_state_classify_truth_table() {
        let verified = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        assert!(matches!(
            WitnessState::classify(capsule_git::ZERO_OID.to_string(), verified),
            WitnessState::Absent
        ));
        match WitnessState::classify(verified.to_string(), verified) {
            WitnessState::AtVerifiedSha(s) => assert_eq!(s, verified),
            WitnessState::Absent | WitnessState::Different(_) => panic!("expected AtVerifiedSha"),
        }
        let other_sha = "cafef00dcafef00dcafef00dcafef00dcafef00d";
        match WitnessState::classify(other_sha.to_string(), verified) {
            WitnessState::Different(s) => assert_eq!(s, other_sha),
            WitnessState::Absent | WitnessState::AtVerifiedSha(_) => panic!("expected Different"),
        }
        assert!(matches!(
            WitnessState::classify(capsule_git::ZERO_OID.to_string(), capsule_git::ZERO_OID),
            WitnessState::Absent
        ));
    }

    #[test]
    fn create_and_get() {
        let mut s = tmp_store();
        let c = make_capsule(&mut s, "abc", "src/api");
        assert_eq!(c.status, Status::Planned);
        let got = s.get_capsule("abc").unwrap();
        assert_eq!(got.id, "abc");
        assert_eq!(got.scope_prefixes.len(), 1);
    }

    /// DESIGN §6 `attempt_claimed` payload: `{attempt_id, session_id,
    /// base_sha, lease}`. Pin the keys so a future refactor can't silently
    /// drop back to the pre-fix `lease_expires_at` flat shape.
    #[test]
    fn attempt_claimed_event_payload_matches_design_spec() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let v = read_event_payload(&s, "x", "attempt_claimed");
        assert!(v.get("attempt_id").is_some(), "missing attempt_id");
        assert_eq!(v["session_id"], "sess1");
        assert!(v.get("base_sha").is_some(), "missing base_sha");
        let lease = v.get("lease").expect("missing lease object");
        assert!(lease.is_object(), "lease must be a JSON object, not a scalar");
        assert!(lease.get("expires_at").is_some());
        assert!(lease.get("ttl_sec").is_some());
        assert!(v.get("lease_expires_at").is_none(), "old key must not return");
    }

    /// Pin attempt_claimed audit-row attribution. The event row must be
    /// attributed to the claiming session and linked to the newly allocated
    /// attempt; the payload-shape test does not protect those columns.
    #[test]
    fn attempt_claimed_event_row_attributes_session_and_attempt() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let ack = s.claim(claim_req("x", "sess1")).unwrap();
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'attempt_claimed'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one attempt_claimed row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'attempt_claimed'",
                params!["x"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "sess1", "claim attributes to the calling session");
        assert_eq!(
            attempt_id,
            Some(ack.id as i64),
            "attempt_id must point at the just-allocated attempt"
        );
    }

    /// `attempt_attested` event payload (DESIGN §6): `{verified_sha, exit_code,
    /// command, log_ref, duration_ms}`. The full `Verification` struct also
    /// carries `at/attestor/attempt_id`, but those duplicate the event row's
    /// own `at/actor/attempt_id` columns, so the payload omits them.
    ///
    /// `ExitCode` is `#[serde(untagged)]`, so the wire shape varies by variant:
    /// `Code(0)` → JSON number, `Sentinel("timeout")` → JSON string. External
    /// readers key on these. The companion test
    /// `attempt_attested_event_payload_serializes_sentinel_exit_code` pins the
    /// sentinel arm; this one pins the integer arm and the full key set.
    #[test]
    fn attempt_attested_event_payload_matches_design_spec() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 7,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let v = read_event_payload(&s, "x", "attempt_attested");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["command", "duration_ms", "exit_code", "log_ref", "verified_sha"]
        );
        assert_eq!(v["verified_sha"], FAKE_SHA);
        assert_eq!(v["duration_ms"], 7);
        assert_eq!(v["command"], "true");
        assert_eq!(v["exit_code"], 0);
    }

    /// Pin attempt_attested audit-row attribution. The payload intentionally
    /// omits `attestor` and `attempt_id` because they duplicate the event
    /// row's `actor` and `attempt_id`; those columns are load-bearing.
    #[test]
    fn attempt_attested_event_row_attributes_session_and_attempt() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let ack = s.claim(claim_req("x", "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'attempt_attested'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one attempt_attested row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'attempt_attested'",
                params!["x"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "sess1", "attest attributes to the session that called it");
        assert_eq!(
            attempt_id,
            Some(ack.id as i64),
            "attempt_id must point at the attempt being attested"
        );
    }

    #[test]
    fn attempt_attested_event_payload_serializes_sentinel_exit_code() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "sleep 999".into(),
            exit_code: capsule_core::ExitCode::Sentinel("timeout".into()),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let v = read_event_payload(&s, "x", "attempt_attested");
        assert_eq!(v["exit_code"], "timeout");
    }

    /// DESIGN §6: `capsule_created` payload is only the audit-relevant tail:
    /// `{acceptance, scope_prefixes, base_ref, depends_on}`. `id` lives on
    /// the event row; `title`/`description` are intentionally omitted.
    #[test]
    fn capsule_created_event_payload_matches_design_spec() {
        let mut s = tmp_store();
        let mut nc = new_capsule_args("x", "src/api");
        nc.depends_on = vec!["dep1".into()];
        s.create_capsule(nc).unwrap();
        let v = read_event_payload(&s, "x", "capsule_created");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["acceptance", "base_ref", "depends_on", "scope_prefixes"]
        );
        assert_eq!(v["base_ref"], "main");
        assert_eq!(v["depends_on"], json::json!(["dep1"]));
        assert_eq!(v["scope_prefixes"], json::json!(["src/api"]));
        assert_eq!(v["acceptance"]["run"], "true");
    }

    /// Pin capsule_created audit attribution. Creation is system-owned and
    /// predates attempts, unlike capsule_amended (operator) and principal
    /// events (session/lander with attempt_id).
    #[test]
    fn capsule_created_event_row_is_system_attributed_with_null_attempt_id() {
        let mut s = tmp_store();
        s.create_capsule(new_capsule_args("x", "src/api")).unwrap();
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_created'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one capsule_created row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_created'",
                params!["x"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "system", "creation is system-driven, not operator");
        assert_eq!(
            attempt_id, None,
            "creation predates any attempt; attempt_id must be NULL"
        );
    }

    /// DESIGN §6: dependency event payloads carry only `{dep_id}`;
    /// the mutated capsule id lives on the event row.
    #[test]
    fn dependency_event_payload_matches_design_spec() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(dep_req("a", "b")).unwrap();
        s.remove_dep(dep_req("a", "b")).unwrap();
        for kind in ["dependency_added", "dependency_removed"] {
            let v = read_event_payload(&s, "a", kind);
            let obj = v.as_object().expect("payload must be a JSON object");
            let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
            keys.sort();
            assert_eq!(keys, vec!["dep_id"], "{kind}");
            assert_eq!(v["dep_id"], "b", "{kind}");
        }
    }

    /// Dependency mutations are unauthenticated, so audit rows must stay
    /// `actor=system` with no attempt. Pin this separately from payload shape.
    #[test]
    fn dependency_event_row_attribution_is_system_no_attempt() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(dep_req("a", "b")).unwrap();
        s.remove_dep(dep_req("a", "b")).unwrap();
        for kind in ["dependency_added", "dependency_removed"] {
            let (actor, attempt_id): (String, Option<i64>) = s
                .conn
                .query_row(
                    "SELECT actor, attempt_id FROM event
                     WHERE capsule_id = ?1 AND kind = ?2",
                    params!["a", kind],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(actor, "system", "{kind}");
            assert!(attempt_id.is_none(), "{kind} attempt_id must be NULL");
        }
    }

    /// DESIGN §6: `capsule_abandoned` payload carries only `{reason}`.
    /// `session_id` and `attempt_id` live on the event row (`actor` /
    /// `attempt_id`), not in kind-specific payload.
    #[test]
    fn capsule_abandoned_event_payload_matches_design_spec() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            reason: "user request".into(),
        })
        .unwrap();
        let v = read_event_payload(&s, "x", "capsule_abandoned");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["reason"]);
        assert_eq!(v["reason"], "user request");
    }

    /// Pin abandon's audit coordinates: `actor=session_id` and `attempt_id`
    /// points at the just-closed attempt. Audit views depend on both for
    /// principal attribution and attempt-history joins.
    #[test]
    fn capsule_abandoned_event_row_attributes_session_and_attempt() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let ack = s.claim(claim_req("x", "sess1")).unwrap();
        let attempt_id = ack.id as i64;
        s.abandon(AbandonRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            reason: "user request".into(),
        })
        .unwrap();
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_abandoned'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one capsule_abandoned row");
        let (actor, row_attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_abandoned'",
                params!["x"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "sess1", "abandon attributes to the session that called it");
        assert_eq!(
            row_attempt_id,
            Some(attempt_id),
            "abandon event_row.attempt_id must point at the just-closed attempt"
        );
    }

    /// Build a `NewCapsule` with sensible test defaults: title="t", description="d",
    /// acceptance=`true` (exit 0), base_ref="main", no deps. Caller mutates fields
    /// post-build for non-default scenarios (e.g. setting `depends_on`).
    fn new_capsule_args(id: &str, scope: &str) -> NewCapsule {
        NewCapsule {
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
        }
    }

    fn make_capsule(s: &mut Store, id: &str, scope: &str) -> Capsule {
        s.create_capsule(new_capsule_args(id, scope)).unwrap()
    }

    fn claim_req(id: &str, sess: &str) -> ClaimRequest {
        claim_req_with_ttl(id, sess, 300)
    }

    fn dep_req(from: &str, to: &str) -> DepRequest {
        DepRequest {
            capsule_id: from.into(),
            depends_on: to.into(),
        }
    }

    /// Pin the `exit_codes_match` 4-cell truth table directly. The cross-shape
    /// cells were once absorbed by `_ => false`, then spelled out so a future
    /// variant on either enum forces compile-time review. Existing attest
    /// tests touch some cells transitively but only assert event serialization
    /// — these tests pin the pass/fail policy itself.
    ///
    /// Cross-shape real-world example: capsule expects exit 0, run hits a
    /// sentinel like `"timeout"` — DESIGN §5 says that's a genuine fail.
    #[test]
    fn exit_codes_match_same_shape_equal_payload() {
        use capsule_core::{ExitCode, ExpectExit};
        assert!(exit_codes_match(&ExpectExit::Code(0), &ExitCode::Code(0)));
        assert!(exit_codes_match(
            &ExpectExit::Sentinel("timeout".into()),
            &ExitCode::Sentinel("timeout".into()),
        ));
    }

    #[test]
    fn exit_codes_match_same_shape_different_payload_fails() {
        use capsule_core::{ExitCode, ExpectExit};
        assert!(!exit_codes_match(&ExpectExit::Code(0), &ExitCode::Code(1)));
        assert!(!exit_codes_match(
            &ExpectExit::Sentinel("timeout".into()),
            &ExitCode::Sentinel("killed".into()),
        ));
    }

    #[test]
    fn exit_codes_match_cross_shape_fails() {
        use capsule_core::{ExitCode, ExpectExit};
        assert!(!exit_codes_match(
            &ExpectExit::Code(0),
            &ExitCode::Sentinel("timeout".into()),
        ));
        assert!(!exit_codes_match(
            &ExpectExit::Sentinel("timeout".into()),
            &ExitCode::Code(0),
        ));
    }

    fn claim_req_with_ttl(id: &str, sess: &str, ttl_sec: u64) -> ClaimRequest {
        ClaimRequest {
            capsule_id: id.into(),
            owner: "o".into(),
            session_id: sess.into(),
            lease_ttl_sec: ttl_sec,
            base_sha: FAKE_SHA.into(),
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
        assert!(matches!(err, StoreError::WrongStatus { op: "claim", .. }));
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

    /// Pins `ScopeConflict(failing_claim, existing_in_flight)`.
    /// Both fields are `CapsuleId`, so swapping them still matches the variant
    /// but makes the Display message point operators at the wrong capsule.
    #[test]
    fn claim_scope_conflict_error_arg_order_and_message() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/api");
        make_capsule(&mut s, "b", "src/api/users.ts");
        s.claim(claim_req("a", "sess1")).unwrap();
        let err = s.claim(claim_req("b", "sess2")).unwrap_err();
        let StoreError::ScopeConflict(claimed, conflict) = &err else {
            panic!("expected ScopeConflict, got {err:?}");
        };
        assert_eq!(claimed, "b", "first arg is the failing claim's capsule");
        assert_eq!(conflict, "a", "second arg is the existing in-flight capsule");
        assert_eq!(
            err.to_string(),
            "capsule b scope overlaps in-flight capsule a"
        );
    }

    /// Accepted capsules still hold scope: this pins the post-attest,
    /// pre-land state where §7.0 still forbids overlapping claims.
    #[test]
    fn claim_scope_conflict_blocks_when_holder_is_accepted() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/api");
        s.claim(claim_req("a", "sess1")).unwrap();
        s.attest(AttestRequest {
            capsule_id: "a".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        assert_eq!(
            s.get_capsule("a").unwrap().status,
            Status::Accepted,
            "guard: holder must be Accepted, not Active"
        );

        make_capsule(&mut s, "b", "src/api/users.ts");
        let err = s.claim(claim_req("b", "sess2")).unwrap_err();
        assert!(
            matches!(err, StoreError::ScopeConflict(ref claimed, ref conflict)
                if claimed == "b" && conflict == "a"),
            "got {err:?}"
        );
    }

    /// Landed capsules release scope. Pins the terminal-success case
    /// separately from Abandoned so future claims are not blocked forever
    /// by previously landed prefixes.
    #[test]
    fn claim_scope_conflict_ignores_landed() {
        let id = "landed_a";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "src/api");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        s.land(LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir: work,
            skip_deploy_verify_gate: true,
        })
        .unwrap();
        assert_eq!(s.get_capsule(id).unwrap().status, Status::Landed);

        make_capsule(&mut s, "next", "src/api");
        s.claim(claim_req("next", "sess2")).unwrap();
        assert_eq!(s.get_capsule("next").unwrap().status, Status::Active);
    }

    /// Abandoned capsules do not lock their scope. `find_scope_conflict` must
    /// only consider lease-holding capsules, otherwise abandoning would leave
    /// the old `scope_json` blocking future claims forever.
    #[test]
    fn claim_scope_conflict_ignores_abandoned() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/api");
        s.claim(claim_req("a", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "a".into(),
            session_id: "sess1".into(),
            reason: "r".into(),
        })
        .unwrap();
        assert_eq!(s.get_capsule("a").unwrap().status, Status::Abandoned);
        make_capsule(&mut s, "b", "src/api");
        s.claim(claim_req("b", "sess2")).unwrap();
        assert_eq!(s.get_capsule("b").unwrap().status, Status::Active);
    }

    /// Pins error precedence: unmet deps are reported before scope conflicts,
    /// even when the capsule also overlaps an in-flight sibling.
    #[test]
    fn claim_unmet_deps_outranks_scope_conflict() {
        let mut s = tmp_store();
        make_capsule(&mut s, "sibling", "src/api");
        s.claim(claim_req("sibling", "sess1")).unwrap();

        let mut child = new_capsule_args("child", "src/api/sub");
        child.depends_on = vec!["ghost".into()];
        s.create_capsule(child).unwrap();

        let err = s.claim(claim_req("child", "sess2")).unwrap_err();
        assert!(
            matches!(err, StoreError::UnmetDeps(_, ref deps) if deps == &vec!["ghost".to_string()]),
            "got {err:?}"
        );
    }

    /// Pins error precedence: `WrongStatus` outranks `UnmetDeps` for claim.
    #[test]
    fn claim_wrong_status_outranks_unmet_deps() {
        let mut s = tmp_store();
        make_capsule(&mut s, "child", "src/api");
        s.claim(claim_req("child", "sess1")).unwrap();
        make_capsule(&mut s, "dep", "src/dep");
        s.add_dep(dep_req("child", "dep")).unwrap();
        let err = s.claim(claim_req("child", "sess2")).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::WrongStatus { op: "claim", current_status: "active", .. }
            ),
            "got {err:?}"
        );
    }

    /// Only Landed deps are met. Accepted deps are not on `base_ref` yet
    /// (DESIGN §7.1.3), so claim must still report them as unmet.
    #[test]
    fn claim_unmet_deps_includes_accepted_dep() {
        let mut s = tmp_store();
        make_capsule(&mut s, "dep", "src/dep");
        s.claim(claim_req("dep", "sess1")).unwrap();
        attest_pass(&mut s, "dep", FAKE_SHA);

        let mut child = new_capsule_args("child", "src/child");
        child.depends_on = vec!["dep".into()];
        s.create_capsule(child).unwrap();

        let err = s.claim(claim_req("child", "sess2")).unwrap_err();
        assert!(
            matches!(err, StoreError::UnmetDeps(_, ref deps) if deps.as_slice() == ["dep"]),
            "got {err:?}"
        );
    }

    /// `heartbeat` extends the lease by re-stamping `now + ttl_sec` with the
    /// fixed-at-claim TTL. The 10ms sleep is required for the strict-`>`
    /// assertion: claim and heartbeat both compute `now + ttl`, so without a
    /// gap they could land on the same instant.
    #[test]
    fn heartbeat_advances_lease() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let a1 = s.claim(claim_req("x", "sess1")).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let ack = s.heartbeat("x", "sess1").unwrap();
        assert!(ack.lease_expires_at > a1.lease.expires_at);
    }

    /// DESIGN §3.3: heartbeat sets `expires_at = now + ttl_sec`.
    /// Pins full-TTL renewal, catching constant or fractional extension bugs.
    #[test]
    fn heartbeat_extends_lease_by_full_ttl_sec() {
        const TTL: i64 = 3600;
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", TTL as u64)).unwrap();
        let before = OffsetDateTime::now_utc();
        let ack = s.heartbeat("x", "sess1").unwrap();
        let after = OffsetDateTime::now_utc();
        let upper = (after - before).whole_seconds() + TTL;

        let ack_ext = (ack.lease_expires_at - before).whole_seconds();
        assert!(
            (TTL..=upper).contains(&ack_ext),
            "ack_ext={ack_ext}s outside [{TTL}, {upper}]"
        );

        let stored_str: String = s
            .conn
            .query_row(
                "SELECT json_extract(lease_json, '$.expires_at')
                 FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", 1i64],
                |r| r.get(0),
            )
            .unwrap();
        let stored_ext = (parse_iso8601(&stored_str) - before).whole_seconds();
        assert!(
            (TTL..=upper).contains(&stored_ext),
            "stored_ext={stored_ext}s outside [{TTL}, {upper}]"
        );
    }

    /// `last_heartbeat` is the operator-visible liveness signal.
    /// Heartbeat must advance it, not only `lease_json.expires_at`.
    #[test]
    fn heartbeat_advances_attempt_last_heartbeat() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let a1 = s.claim(claim_req("x", "sess1")).unwrap();
        let aid = a1.id as i64;
        let claim_lh: String = s
            .conn
            .query_row(
                "SELECT last_heartbeat FROM attempt
                 WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", aid],
                |r| r.get(0),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        s.heartbeat("x", "sess1").unwrap();
        let after_lh: String = s
            .conn
            .query_row(
                "SELECT last_heartbeat FROM attempt
                 WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", aid],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            parse_iso8601(&after_lh) > parse_iso8601(&claim_lh),
            "heartbeat must advance last_heartbeat: claim={claim_lh} after={after_lh}"
        );
    }

    /// DESIGN §3.3: `ttl_sec` is set at claim and immutable.
    /// Heartbeat may extend `expires_at`, but must not rewrite `ttl_sec`.
    #[test]
    fn heartbeat_preserves_immutable_ttl_sec() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 300)).unwrap();
        s.heartbeat("x", "sess1").unwrap();
        s.heartbeat("x", "sess1").unwrap();
        let ttl_sec: i64 = s
            .conn
            .query_row(
                "SELECT json_extract(lease_json, '$.ttl_sec')
                 FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", 1i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ttl_sec, 300);
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
                verified_sha: FAKE_SHA.into(),
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

    /// Attestation stores the verified SHA on the active attempt, not only on the capsule.
    #[test]
    fn attest_records_tip_sha_on_active_attempt() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let pre = s.get_capsule("x").unwrap();
        assert!(pre.attempts[0].tip_sha.is_none(), "claim leaves tip_sha NULL");
        s.attest(AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        let post = s.get_capsule("x").unwrap();
        assert_eq!(post.attempts[0].tip_sha.as_deref(), Some(FAKE_SHA));
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
                verified_sha: FAKE_SHA.into(),
                command: "false".into(),
                exit_code: capsule_core::ExitCode::Code(1),
                duration_ms: 50,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap();
        assert!(!ack.accepted);
        assert_eq!(ack.new_status, Status::Active);
    }

    /// Failed attestations are retryable while the attempt remains Active.
    /// A later attest in the same live lease overwrites `capsule.verification`;
    /// per-attempt history lives in events, not the capsule row.
    #[test]
    fn attest_retryable_after_fail_overwrites_verification() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let req = |sha: &str, code: i32| AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: sha.into(),
            command: "cmd".into(),
            exit_code: capsule_core::ExitCode::Code(code),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        };
        let other_sha = "fedcba9876543210fedcba9876543210fedcba98";
        let ack1 = s.attest(req(FAKE_SHA, 1)).unwrap();
        assert!(!ack1.accepted);
        assert_eq!(ack1.new_status, Status::Active);

        let ack2 = s.attest(req(other_sha, 0)).unwrap();
        assert!(ack2.accepted);
        assert_eq!(ack2.new_status, Status::Accepted);

        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.status, Status::Accepted);
        let v = c.verification.expect("verification must persist after pass");
        assert_eq!(v.verified_sha, other_sha);
        assert!(matches!(v.exit_code, capsule_core::ExitCode::Code(0)));
    }

    /// Retry attest must overwrite both `capsule.verification` and
    /// `attempt.tip_sha` from the same `verified_sha`. Pins against a
    /// refactor making `tip_sha` sticky after the first attest.
    #[test]
    fn attest_retry_overwrites_attempt_tip_sha() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let req = |sha: &str, code: i32| AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: sha.into(),
            command: "cmd".into(),
            exit_code: capsule_core::ExitCode::Code(code),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        };
        let other_sha = "fedcba9876543210fedcba9876543210fedcba98";
        s.attest(req(FAKE_SHA, 1)).unwrap();
        s.attest(req(other_sha, 0)).unwrap();
        let post = s.get_capsule("x").unwrap();
        assert_eq!(
            post.attempts[0].tip_sha.as_deref(),
            Some(other_sha),
            "retry must overwrite tip_sha so attempt row tracks capsule.verification"
        );
        assert_eq!(
            post.verification.unwrap().verified_sha,
            post.attempts[0].tip_sha.clone().unwrap(),
            "tip_sha and verification.verified_sha must agree after retry",
        );
    }

    #[test]
    fn attest_after_accepted_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let req = AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 100,
            log_ref: "file:///dev/null".into(),
        };
        s.attest(req.clone()).unwrap();
        let err = s.attest(req).unwrap_err();
        assert!(matches!(err, StoreError::WrongStatus { op: "attest", .. }));
    }

    /// Accepted outranks cross-session for attest retries.
    /// Caller recovery depends on seeing WrongStatus(Accepted), not CrossSession.
    #[test]
    fn attest_wrong_status_outranks_cross_session() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let req = |sid: &str| AttestRequest {
            capsule_id: "x".into(),
            session_id: sid.into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 100,
            log_ref: "file:///dev/null".into(),
        };
        s.attest(req("sess1")).unwrap();
        let err = s.attest(req("sess2")).unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::WrongStatus {
                    ref capsule_id,
                    op: "attest",
                    current_status: "accepted",
                } if capsule_id == "x"
            ),
            "got {err:?}"
        );
    }

    /// Garbage `verified_sha` should fail at the protocol boundary (here),
    /// not later as an opaque `git push <garbage>:refs/heads/...` failure.
    #[test]
    fn attest_rejects_malformed_verified_sha() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s
            .attest(AttestRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                verified_sha: "abc".into(),
                command: "true".into(),
                exit_code: capsule_core::ExitCode::Code(0),
                duration_ms: 1,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidSha(_)),
            "expected InvalidSha, got: {err:?}"
        );
    }

    /// `attest` validates `verified_sha` before capsule lookup, so malformed
    /// input returns `InvalidSha` even when the capsule does not exist.
    #[test]
    fn attest_invalid_sha_outranks_not_found() {
        let mut s = tmp_store();
        let err = s
            .attest(AttestRequest {
                capsule_id: "ghost".into(),
                session_id: "sess1".into(),
                verified_sha: "abc".into(),
                command: "true".into(),
                exit_code: capsule_core::ExitCode::Code(0),
                duration_ms: 1,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidSha(_)), "got {err:?}");
    }

    /// Malformed `verified_sha` is reported before capsule status.
    /// A Planned capsule with a bad sha must surface `InvalidSha`, not
    /// `WrongStatus` — pins input validation ahead of the status gate.
    #[test]
    fn attest_invalid_sha_outranks_wrong_status() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        assert_eq!(s.get_capsule("x").unwrap().status, Status::Planned);
        let err = s
            .attest(AttestRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                verified_sha: "abc".into(),
                command: "true".into(),
                exit_code: capsule_core::ExitCode::Code(0),
                duration_ms: 1,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidSha(_)), "got {err:?}");
    }

    /// Symmetric with attest: base_sha flows into `git worktree add ... <sha>`
    /// (capsule-cli isolation) and into LandPush prior-base computation.
    /// Reject at the protocol boundary.
    #[test]
    fn claim_rejects_malformed_base_sha() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let err = s
            .claim(ClaimRequest {
                capsule_id: "x".into(),
                owner: "o".into(),
                session_id: "sess1".into(),
                lease_ttl_sec: 300,
                base_sha: "deadbeef".into(),
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidSha(_)),
            "expected InvalidSha, got: {err:?}"
        );
    }

    /// Symmetric with `attest_invalid_sha_outranks_not_found`: `claim`
    /// validates `base_sha` before capsule lookup, so malformed input
    /// returns `InvalidSha` even when the capsule does not exist.
    #[test]
    fn claim_invalid_sha_outranks_not_found() {
        let mut s = tmp_store();
        let err = s
            .claim(ClaimRequest {
                capsule_id: "ghost".into(),
                owner: "o".into(),
                session_id: "sess1".into(),
                lease_ttl_sec: 300,
                base_sha: "deadbeef".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidSha(_)), "got {err:?}");
    }

    /// `claim` reports invalid lease TTL before capsule lookup, symmetric to
    /// `claim_invalid_sha_outranks_not_found`.
    #[test]
    fn claim_invalid_lease_ttl_outranks_not_found() {
        let mut s = tmp_store();
        let mut req = claim_req("ghost", "sess1");
        req.lease_ttl_sec = u64::MAX;
        let err = s.claim(req).unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidLeaseTtl(t) if t == u64::MAX),
            "got {err:?}"
        );
    }

    /// When multiple claim inputs are invalid, `base_sha` is reported first.
    /// Pins validation order against refactors that group or reorder
    /// input checks.
    #[test]
    fn claim_invalid_sha_outranks_invalid_lease_ttl() {
        let mut s = tmp_store();
        let err = s
            .claim(ClaimRequest {
                capsule_id: "ghost".into(),
                owner: "o".into(),
                session_id: "sess1".into(),
                lease_ttl_sec: u64::MAX,
                base_sha: "bogus".into(),
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidSha(_)), "got {err:?}");
    }

    #[test]
    fn heartbeat_cross_session_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s.heartbeat("x", "wrong").unwrap_err();
        assert!(matches!(err, StoreError::CrossSession));
    }

    /// Heartbeat on a never-claimed capsule must report `WrongStatus`.
    /// Pins the pre-lease status guard before session/attempt checks.
    #[test]
    fn heartbeat_wrong_status_for_planned() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let err = s.heartbeat("x", "sess1").unwrap_err();
        assert!(
            matches!(
                err,
                StoreError::WrongStatus { op: "heartbeat", current_status: "planned", .. }
            ),
            "got {err:?}"
        );
    }

    /// Same-session heartbeat after expiry returns `LeaseExpired` with the
    /// lease's prior `expires_at`; heartbeat does not reclaim before renewal.
    #[test]
    fn heartbeat_same_session_lease_expired_carries_prior_expires_at() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let claimed = s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        let prior_expires = claimed.lease.expires_at;
        let stored_str: String = s
            .conn
            .query_row(
                "SELECT json_extract(lease_json, '$.expires_at')
                 FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", 1i64],
                |r| r.get(0),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));
        let err = s.heartbeat("x", "sess1").unwrap_err();
        let StoreError::LeaseExpired(at_str) = err else {
            panic!("expected LeaseExpired, got {err:?}");
        };
        assert_eq!(parse_iso8601(&at_str), prior_expires);
        assert_eq!(at_str, stored_str, "payload must pass through json_extract bytes");
    }

    /// Pin precedence: a wrong-session caller whose lease has ALSO expired
    /// must see CrossSession, not LeaseExpired. They don't own the lease,
    /// period — the expiry is irrelevant context, and leaking it would let
    /// a foreign session probe lease state. Heartbeat is the cleanest
    /// probe: it does not run reclaim before loading the lease, so status
    /// stays `active` and heartbeat's open-coded projection (session_id +
    /// expires_at + ttl_sec via `json_extract`) is exercised end-to-end.
    #[test]
    fn cross_session_outranks_expired_lease() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));
        let err = s.heartbeat("x", "wrong").unwrap_err();
        assert!(
            matches!(err, StoreError::CrossSession),
            "expected CrossSession, got {err:?}",
        );
    }

    /// Pins `assert_live_lease_for_session`'s `LeaseExpired` payload.
    /// Heartbeat has a separate emit site, so it cannot cover attest drift.
    #[test]
    fn attest_same_session_lease_expired_carries_prior_expires_at() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        let stored_str: String = s
            .conn
            .query_row(
                "SELECT json_extract(lease_json, '$.expires_at')
                 FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", 1i64],
                |r| r.get(0),
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));
        let err = s
            .attest(AttestRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                verified_sha: FAKE_SHA.into(),
                command: "true".into(),
                exit_code: capsule_core::ExitCode::Code(0),
                duration_ms: 1,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap_err();
        let StoreError::LeaseExpired(at_str) = err else {
            panic!("expected LeaseExpired, got {err:?}");
        };
        assert_eq!(at_str, stored_str, "payload must pass through json_extract bytes");
    }

    /// Parallel pin to `cross_session_outranks_expired_lease` for the
    /// `assert_live_lease_for_session` helper. `attest` does not run
    /// reclaim before lease load, so status stays `active` and the
    /// CrossSession-before-LeaseExpired precedence is observable here too.
    /// The two helpers are independently evolvable; without this pin a
    /// future tweak could drift one without the other failing.
    #[test]
    fn attest_cross_session_outranks_expired_lease() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));
        let err = s
            .attest(AttestRequest {
                capsule_id: "x".into(),
                session_id: "wrong".into(),
                verified_sha: FAKE_SHA.into(),
                command: "true".into(),
                exit_code: capsule_core::ExitCode::Code(0),
                duration_ms: 1,
                log_ref: "file:///dev/null".into(),
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::CrossSession),
            "expected CrossSession, got {err:?}",
        );
    }

    fn claim_with_ttl(s: &mut Store, capsule_id: &str, ttl_sec: u64) -> Result<()> {
        let mut req = claim_req(capsule_id, "sess1");
        req.lease_ttl_sec = ttl_sec;
        s.claim(req).map(|_| ())
    }

    /// Pre-fix, `req.lease_ttl_sec as i64` wrapped for ttl > i64::MAX,
    /// producing a negative `time::Duration` → expires_at < now → lease
    /// born already-expired. Must surface as a clean error.
    #[test]
    fn claim_rejects_u64_max_lease_ttl() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        match claim_with_ttl(&mut s, "x", u64::MAX) {
            Err(StoreError::InvalidLeaseTtl(t)) => assert_eq!(t, u64::MAX),
            other => panic!("expected InvalidLeaseTtl, got {other:?}"),
        }
    }

    #[test]
    fn claim_rejects_lease_ttl_just_past_i64_max() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        assert!(matches!(
            claim_with_ttl(&mut s, "x", (i64::MAX as u64) + 1),
            Err(StoreError::InvalidLeaseTtl(_))
        ));
    }

    /// `i64::MAX` seconds fits in `time::Duration` but overflows
    /// `now + duration` — surfaces as `InvalidLeaseTtl`, not a panic.
    #[test]
    fn claim_rejects_i64_max_lease_ttl() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        assert!(matches!(
            claim_with_ttl(&mut s, "x", i64::MAX as u64),
            Err(StoreError::InvalidLeaseTtl(_))
        ));
    }

    #[test]
    fn claim_accepts_sane_lease_ttl() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        assert!(claim_with_ttl(&mut s, "x", 3600).is_ok());
    }

    /// Lease expires past TTL → next read-path sweep (here: `list_capsules`)
    /// reverts the capsule to `Planned`, clears `verification`, and closes the
    /// attempt with outcome=`Expired`. Also pins the `attempt_expired` event
    /// payload to exactly DESIGN §6's keys (`at`, `prior_lease_expires_at`) —
    /// the old `lease_expires_at` name and code-only `session_id` must not leak.
    #[test]
    fn lease_expiry_reverts_to_planned_and_clears_verification() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        s.attest(AttestRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            verified_sha: FAKE_SHA.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));

        let listed = s.list_capsules(ListFilter::default()).unwrap();
        let c = listed.iter().find(|c| c.id == "x").unwrap();
        assert_eq!(c.status, Status::Planned);
        assert!(c.active_attempt.is_none());
        assert!(c.verification.is_none());
        assert_eq!(c.attempts.len(), 1);
        assert_eq!(c.attempts[0].outcome, capsule_core::AttemptOutcome::Expired);

        let v = read_event_payload(&s, "x", "attempt_expired");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["at", "prior_lease_expires_at"]);
    }

    /// Pin byte-for-byte: `prior_lease_expires_at` in the emitted
    /// `attempt_expired` event equals `$.expires_at` as stored in
    /// `lease_json` (iter 172 dropped the parse + re-format and now
    /// passes the `json_extract` string straight through to the payload).
    /// If a future change drifts the stored ISO-8601 form away from what
    /// `format_iso8601` produces, this test fails.
    #[test]
    fn reclaim_event_payload_matches_stored_expires_at_byte_for_byte() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        let expires_at_before_reclaim: String = s
            .conn
            .query_row(
                "SELECT json_extract(lease_json, '$.expires_at')
                 FROM attempt WHERE capsule_id = ?1 AND attempt_id = ?2",
                params!["x", 1i64],
                |r| r.get(0),
            )
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1200));
        let _ = s.list_capsules(ListFilter::default()).unwrap();

        let v = read_event_payload(&s, "x", "attempt_expired");
        let payload_str = v
            .get("prior_lease_expires_at")
            .and_then(|v| v.as_str())
            .expect("prior_lease_expires_at must be a string");
        assert_eq!(payload_str, expires_at_before_reclaim);
    }

    /// Pin attempt_expired audit attribution: reclaim is system-driven, while
    /// attempt_id still points at the expired attempt for history joins.
    #[test]
    fn attempt_expired_event_row_is_system_attributed_with_expired_attempt_id() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let ack = s.claim(claim_req_with_ttl("x", "sess1", 1)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1200));
        // Listing drives lazy auto-reclaim for expired leases.
        let _ = s.list_capsules(ListFilter::default()).unwrap();

        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event WHERE capsule_id = ?1 AND kind = 'attempt_expired'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one attempt_expired row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'attempt_expired'",
                params!["x"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "system", "auto-reclaim is system-driven, not the prior session");
        assert_eq!(
            attempt_id,
            Some(ack.id as i64),
            "attempt_id must link to the expired attempt",
        );
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

    /// `claimed` is active → excluded; `conflict` overlaps `claimed` →
    /// excluded; only `free` survives.
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
        assert_eq!(ids, vec!["free"]);
    }

    /// Pin the contract: `UnmetDeps`'s Vec follows the capsule's
    /// `depends_on` order, even when SQLite is free to return rows in
    /// any order. Regression-guards the `json_each + ORDER BY j.key`
    /// refactor. Input order is deliberately non-alphabetical to
    /// detect any sorting drift.
    #[test]
    fn claim_unmet_deps_preserve_input_order() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        make_capsule(&mut s, "c", "src/c");
        let mut child = new_capsule_args("child", "src/child");
        child.depends_on = vec!["c".into(), "a".into(), "b".into()];
        s.create_capsule(child).unwrap();

        let err = s.claim(claim_req("child", "sess1")).unwrap_err();
        match err {
            StoreError::UnmetDeps(_, deps) => assert_eq!(deps, vec!["c", "a", "b"]),
            other => panic!("expected UnmetDeps, got {other:?}"),
        }
    }

    /// A dep id that does not resolve to any capsule must be reported as
    /// unmet (matches the prior `Option<String> ⇒ None` branch).
    #[test]
    fn claim_unmet_deps_includes_missing_ids() {
        let mut s = tmp_store();
        let mut child = new_capsule_args("child", "src/child");
        child.depends_on = vec!["ghost".into()];
        s.create_capsule(child).unwrap();
        let err = s.claim(claim_req("child", "sess1")).unwrap_err();
        match err {
            StoreError::UnmetDeps(_, deps) => assert_eq!(deps, vec!["ghost"]),
            other => panic!("expected UnmetDeps, got {other:?}"),
        }
    }

    /// `available` ignores overlap between Planned capsules. Both are
    /// listed; claiming one makes the other fail with `ScopeConflict`
    /// (pinned by `claim_scope_conflict`). Refactoring the filter to
    /// prune Planned-vs-Planned overlap would silently shift scheduling
    /// from race-then-conflict to filter-then-pick.
    #[test]
    fn list_filter_available_planned_overlap_both_listed() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/api");
        make_capsule(&mut s, "b", "src/api/sub");
        let avail = s
            .list_capsules(ListFilter {
                available: true,
                ..Default::default()
            })
            .unwrap();
        let mut ids: Vec<String> = avail.into_iter().map(|c| c.id).collect();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }

    /// `dep` is planned with no deps → eligible. `child`'s deps are unmet
    /// (its dep `dep` is planned, not landed) → excluded.
    #[test]
    fn list_filter_available_excludes_unmet_deps() {
        let mut s = tmp_store();
        make_capsule(&mut s, "dep", "src/dep");
        let mut child = new_capsule_args("child", "src/child");
        child.depends_on = vec!["dep".into()];
        s.create_capsule(child).unwrap();

        let avail = s
            .list_capsules(ListFilter {
                available: true,
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = avail.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["dep"]);
    }

    /// Pin the `WHERE status = ?1` SQL arm of `list_capsules`. The other
    /// `list_filter_*` tests use `..Default::default()` which leaves
    /// `status` as `None` and exercises only the unbound arm. This test
    /// moves p1 to abandoned via claim+abandon while p2 stays planned,
    /// then asserts each status filter returns only its bucket.
    #[test]
    fn list_filter_status_pins_bound_arm() {
        let mut s = tmp_store();
        make_capsule(&mut s, "p1", "src/p1");
        make_capsule(&mut s, "p2", "src/p2");
        s.claim(claim_req("p1", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "p1".into(),
            session_id: "sess1".into(),
            reason: "test".into(),
        })
        .unwrap();

        let planned = s
            .list_capsules(ListFilter {
                status: Some(Status::Planned),
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = planned.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["p2"]);

        let abandoned = s
            .list_capsules(ListFilter {
                status: Some(Status::Abandoned),
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = abandoned.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["p1"]);
    }

    /// Pins oldest-first ordering for operator-visible `list` output.
    /// Both SQL constants (`SELECT_ALL_ORDERED`, `SELECT_BY_STATUS_ORDERED`)
    /// can drift independently — assert both arms.
    ///
    /// Setup uses non-alphabetical insertion order (`z_first`, `a_second`,
    /// `m_third`) so an `ORDER BY id` refactor would diverge from the
    /// expected sequence and fail visibly.
    #[test]
    fn list_capsules_orders_by_created_at_asc() {
        let mut s = tmp_store();
        for id in &["z_first", "a_second", "m_third"] {
            make_capsule(&mut s, id, &format!("src/{id}"));
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let listed = s.list_capsules(ListFilter::default()).unwrap();
        let ids: Vec<&str> = listed.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["z_first", "a_second", "m_third"], "unbound arm");

        let filtered = s
            .list_capsules(ListFilter {
                status: Some(Status::Planned),
                ..Default::default()
            })
            .unwrap();
        let ids: Vec<&str> = filtered.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, vec!["z_first", "a_second", "m_third"], "status-filtered arm");
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

    /// Pins `scope_overlaps` to component-wise prefix matching (DESIGN §7.0).
    /// `src/apifoo` must not overlap a capsule scoped to `src/api`; a raw
    /// string-prefix implementation would incorrectly match it.
    #[test]
    fn list_filter_scope_overlaps_is_component_wise() {
        let mut s = tmp_store();
        make_capsule(&mut s, "api", "src/api");
        let res = s
            .list_capsules(ListFilter {
                scope_overlaps: Some(CanonicalPath::new("src/apifoo").unwrap()),
                ..Default::default()
            })
            .unwrap();
        assert!(res.is_empty(), "got {:?}", res.iter().map(|c| &c.id).collect::<Vec<_>>());
    }

    #[test]
    fn create_rejects_invalid_id() {
        let mut s = tmp_store();
        let err = s
            .create_capsule(new_capsule_args("bad/id", "a"))
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidId(_, _)));
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut s = tmp_store();
        let nc = new_capsule_args("x", "a");
        s.create_capsule(nc.clone()).unwrap();
        let err = s.create_capsule(nc).unwrap_err();
        assert!(matches!(err, StoreError::DuplicateId(_)));
    }

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

        make_capsule(&mut s, "y", "src/api/users.ts");
        s.claim(claim_req("y", "sess2")).unwrap();
    }

    /// Terminal attempts must record `closed_at`, not just `outcome`.
    /// This pins `close_attempt`; otherwise duration projections stay NULL.
    #[test]
    fn abandon_records_attempt_closed_at() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let pre = s.get_capsule("x").unwrap();
        assert!(
            pre.attempts[0].closed_at.is_none(),
            "in-flight attempt must have no closed_at"
        );
        let before = OffsetDateTime::now_utc();
        s.abandon(AbandonRequest {
            capsule_id: "x".into(),
            session_id: "sess1".into(),
            reason: "test".into(),
        })
        .unwrap();
        let post = s.get_capsule("x").unwrap();
        assert_eq!(
            post.attempts[0].outcome,
            capsule_core::AttemptOutcome::Abandoned,
            "guard: pin must inspect a terminal attempt"
        );
        let closed = post.attempts[0]
            .closed_at
            .expect("abandoned attempt must record closed_at");
        assert!(
            closed >= before,
            "closed_at {closed} earlier than abandon start {before}"
        );
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

    /// Explicit reclaim rejects frozen capsules, unlike auto-reclaim paths
    /// that silently skip frozen rows.
    #[test]
    fn reclaim_frozen_capsule_returns_pending_land_frozen() {
        let id = "rclfrz";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);
        let err = s.reclaim(id).unwrap_err();
        assert!(
            matches!(err, StoreError::PendingLandFrozen(ref cid) if cid == id),
            "got {err:?}"
        );
    }

    /// Expired attempts remain audit history, so reclaim must not reuse
    /// their id. Reuse would collide with git refs derived from
    /// `(capsule_id, attempt_id)`.
    #[test]
    fn claim_after_reclaim_allocates_new_attempt_id() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req_with_ttl("x", "sess1", 0)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        s.reclaim("x").unwrap();
        let a = s.claim(claim_req("x", "sess2")).unwrap();
        assert_eq!(a.id, 2);
        let c = s.get_capsule("x").unwrap();
        assert_eq!(c.attempts.len(), 2);
        assert_eq!(c.attempts[0].id, 1);
        assert_eq!(
            c.attempts[0].outcome,
            capsule_core::AttemptOutcome::Expired
        );
        assert_eq!(c.attempts[1].id, 2);
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
            acceptance: Some(new_acc),
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

    /// `capsule_amended` payload is a diff, not a snapshot: only fields
    /// explicitly changed by the request may appear in the event payload.
    #[test]
    fn amend_event_payload_records_only_changed_fields() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.amend(AmendRequest {
            capsule_id: "x".into(),
            title: Some("t2".into()),
            ..Default::default()
        })
        .unwrap();
        let payload_json: String = s
            .conn
            .query_row(
                "SELECT payload_json FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_amended'
                 ORDER BY rowid DESC LIMIT 1",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        let v: json::Value = json::from_str(&payload_json).unwrap();
        let obj = v.as_object().expect("payload object");
        assert_eq!(obj.len(), 1, "extra keys in diff payload: {obj:?}");
        assert_eq!(obj.get("title"), Some(&json::Value::String("t2".into())));
    }

    /// DESIGN §6: amends are human config changes and must remain
    /// operator-attributed for operator-action audit views.
    #[test]
    fn amend_event_attributes_actor_to_operator() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.amend(AmendRequest {
            capsule_id: "x".into(),
            title: Some("t2".into()),
            ..Default::default()
        })
        .unwrap();
        let (actor, count): (String, i64) = s
            .conn
            .query_row(
                "SELECT actor, COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_amended'",
                params!["x"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one capsule_amended row");
        assert_eq!(actor, "operator", "capsule_amended must stay operator-attributed");
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

    /// Empty amend (all fields `None`) is a full no-op: scalar fields and
    /// `updated_at` are unchanged, AND no `capsule_amended` event is emitted.
    /// The audit-trail invariant matters for downstream consumers (event
    /// streams, reviewer dashboards) that count amend events; a refactor
    /// hoisting `insert_event` above the `update.is_empty()` short-circuit
    /// would silently emit zero-diff audit rows.
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
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event WHERE capsule_id = ?1 AND kind = 'capsule_amended'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "empty amend must not emit a zero-diff event");
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

    /// Empty amend still validates capsule state before no-oping.
    /// Pins `NotAmendable` precedence for non-Planned capsules.
    #[test]
    fn amend_noop_on_non_planned_returns_not_amendable() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        let err = s
            .amend(AmendRequest {
                capsule_id: "x".into(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::NotAmendable(ref id, "active") if id == "x"),
            "got {err:?}"
        );
    }

    /// Symmetric pin: empty amend on an unknown capsule still surfaces
    /// `NotFound`, never silent success.
    #[test]
    fn amend_noop_unknown_capsule_returns_not_found() {
        let mut s = tmp_store();
        let err = s
            .amend(AmendRequest {
                capsule_id: "ghost".into(),
                ..Default::default()
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::NotFound(ref id) if id == "ghost"),
            "got {err:?}"
        );
    }

    #[test]
    fn add_dep_records_edge() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(dep_req("a", "b")).unwrap();
        let c = s.get_capsule("a").unwrap();
        assert_eq!(c.depends_on, vec!["b".to_string()]);
    }

    #[test]
    fn add_dep_idempotent() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(dep_req("a", "b")).unwrap();
        s.add_dep(dep_req("a", "b")).unwrap();
        let c = s.get_capsule("a").unwrap();
        assert_eq!(c.depends_on, vec!["b".to_string()]);
    }

    /// Idempotent add_dep must not emit duplicate `dependency_added` events.
    /// Pins audit-log behavior for retry paths where the deps array is unchanged.
    #[test]
    fn add_dep_idempotent_emits_no_event() {
        let count = |s: &Store| -> i64 {
            s.conn
                .query_row(
                    "SELECT COUNT(*) FROM event
                     WHERE capsule_id = ?1 AND kind = 'dependency_added'",
                    params!["a"],
                    |r| r.get(0),
                )
                .unwrap()
        };
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(dep_req("a", "b")).unwrap();
        assert_eq!(count(&s), 1, "first add must emit");
        s.add_dep(dep_req("a", "b")).unwrap();
        assert_eq!(count(&s), 1, "duplicate add must not re-emit");
    }

    #[test]
    fn add_dep_self_loop_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        let err = s.add_dep(dep_req("a", "a")).unwrap_err();
        assert!(matches!(err, StoreError::DependencyCycle(_, _)));
    }

    /// Reflexivity pin: `reachable(tx, x, x)` returns `true` even when `x`
    /// has no row in `capsule`. The reflexive case must short-circuit before
    /// the DB read — `creates_cycle(self, self)` relies on this so the
    /// self-dep path doesn't need a separate branch in `add_dep`.
    #[test]
    fn reachable_reflexive_short_circuits_before_db_read() {
        let mut s = tmp_store();
        let tx = s.conn.transaction().unwrap();
        assert!(reachable(&tx, "ghost", "ghost").unwrap());
    }

    /// Missing capsule rows are leaves, not errors. Pins the
    /// `json_each` join behavior that makes stale ids return clean
    /// `false` instead of panic/Err — a plausible "fail-fast on stale
    /// id" refactor would silently break this contract.
    #[test]
    fn reachable_missing_row_yields_no_neighbors() {
        let mut s = tmp_store();
        let tx = s.conn.transaction().unwrap();
        assert!(!reachable(&tx, "ghost", "target").unwrap());
    }

    #[test]
    fn add_dep_cycle_rejected() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        make_capsule(&mut s, "c", "src/c");
        s.add_dep(dep_req("a", "b")).unwrap();
        s.add_dep(dep_req("b", "c")).unwrap();
        let err = s.add_dep(dep_req("c", "a")).unwrap_err();
        assert!(matches!(err, StoreError::DependencyCycle(_, _)));
    }

    /// Pins `DependencyCycle(adder, target)`. Both fields are `CapsuleId`,
    /// so a swap would still match the variant while reversing the rejected
    /// edge in the operator-facing message.
    #[test]
    fn add_dep_cycle_error_arg_order_and_message() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        make_capsule(&mut s, "c", "src/c");
        s.add_dep(dep_req("a", "b")).unwrap();
        s.add_dep(dep_req("b", "c")).unwrap();
        let err = s.add_dep(dep_req("c", "a")).unwrap_err();
        let StoreError::DependencyCycle(adder, target) = &err else {
            panic!("expected DependencyCycle, got {err:?}");
        };
        assert_eq!(adder, "c", "first arg is the capsule that tried to add the dep");
        assert_eq!(target, "a", "second arg is the dep target");
        assert_eq!(
            err.to_string(),
            "dependency cycle: adding c -> a would create a cycle"
        );
    }

    #[test]
    fn add_dep_target_not_found() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        let err = s.add_dep(dep_req("a", "ghost")).unwrap_err();
        assert!(matches!(err, StoreError::DepNotFound(_)));
    }

    /// Existing deps from `create_capsule` may still point at missing
    /// capsules. Re-adding such a dep must surface `DepNotFound`, not
    /// succeed as an idempotent no-op.
    #[test]
    fn add_dep_dep_not_found_outranks_idempotent_noop() {
        let mut s = tmp_store();
        let mut a = new_capsule_args("a", "src/a");
        a.depends_on = vec!["ghost".into()];
        s.create_capsule(a).unwrap();
        let err = s.add_dep(dep_req("a", "ghost")).unwrap_err();
        assert!(
            matches!(err, StoreError::DepNotFound(ref id) if id == "ghost"),
            "got {err:?}"
        );
    }

    #[test]
    fn remove_dep_removes_edge() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        make_capsule(&mut s, "b", "src/b");
        s.add_dep(dep_req("a", "b")).unwrap();
        s.remove_dep(dep_req("a", "b")).unwrap();
        let c = s.get_capsule("a").unwrap();
        assert!(c.depends_on.is_empty());
    }

    /// `remove_dep` is intentionally lenient: unlike `add_dep`, it does not
    /// validate the target exists. Removing an absent edge is a no-op and
    /// must not emit a `DependencyRemoved` event.
    #[test]
    fn remove_dep_missing_target_noop_no_event() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        s.remove_dep(dep_req("a", "ghost")).unwrap();
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event WHERE capsule_id = ?1 AND kind = 'dependency_removed'",
                params!["a"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "absent-edge remove must not emit a dep_removed event");
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
        s.add_dep(dep_req("a", "b")).unwrap();
        s.remove_dep(dep_req("a", "b")).unwrap();
        let c = s.get_capsule("a").unwrap();
        assert!(c.depends_on.is_empty());
    }

    /// Terminal capsules short-circuit before dep-target validation. An
    /// `add_dep` from an abandoned capsule to a missing target is Ok, not
    /// DepNotFound — the terminal check in `load_deps_for_mutation` runs
    /// first.
    #[test]
    fn add_dep_terminal_outranks_dep_not_found() {
        let mut s = tmp_store();
        make_capsule(&mut s, "a", "src/a");
        s.claim(claim_req("a", "sess1")).unwrap();
        s.abandon(AbandonRequest {
            capsule_id: "a".into(),
            session_id: "sess1".into(),
            reason: "r".into(),
        })
        .unwrap();
        let res = s.add_dep(dep_req("a", "ghost"));
        assert!(
            res.is_ok(),
            "terminal add_dep should no-op before target validation, got {res:?}"
        );
        let c = s.get_capsule("a").unwrap();
        assert!(c.depends_on.is_empty());
    }

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

    /// `git push --force-with-lease=ref:` compares against the local
    /// origin-tracking ref, not the remote. Tests that stage a conflicting
    /// witness directly on the bare must refresh the worker's tracking
    /// refs afterwards — otherwise the lander's lease compares against a
    /// stale snapshot and the rejection path under test never fires.
    fn refresh_worker_origin_tracking(work: &std::path::Path) {
        git(work, &["fetch", "origin"]);
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

        std::fs::write(work.join("feature.txt"), "feature\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "feature"]);
        let verified_sha = git(&work, &["rev-parse", "HEAD"]);
        let attempt_branch = format!("capsules/{capsule_id}/a1");
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

    /// Standard "exit 0" attest used by every land/reconcile/force test.
    /// Returns the ack so callers can assert on it; current callers ignore it.
    fn attest_pass(s: &mut Store, id: &str, verified_sha: &str) -> AttestAck {
        s.attest(AttestRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            verified_sha: verified_sha.into(),
            command: "true".into(),
            exit_code: capsule_core::ExitCode::Code(0),
            duration_ms: 1,
            log_ref: "file:///dev/null".into(),
        })
        .unwrap()
    }

    /// Happy-path land: §7.1.2 step 3's atomic push lands `main` at
    /// `verified_sha` and writes the witness ref at the same sha; step
    /// 4's reconcile flips status → `landed`, populates `landing`,
    /// clears `pending_land`, and closes the attempt with
    /// `AttemptOutcome::Landed`. Asserts span both sides of the
    /// boundary — git rev-parse on the bare repo proves the push, then
    /// `get_capsule` proves the DB committed.
    #[test]
    fn land_happy_path_advances_base_ref_and_writes_landing() {
        let id = "land1";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);

        let ack = s
            .land(LandRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                lander: "test-lander".into(),
                remote: bare.to_str().unwrap().into(),
                repo_dir: work,
                skip_deploy_verify_gate: true,
            })
            .unwrap();

        match ack.outcome {
            LandOutcome::Landed { ref landing } => {
                assert_eq!(landing.landed_sha, verified_sha);
                assert!(landing.advanced_base_ref);
                assert_eq!(landing.witness_branch, format!("capsule-witness/{id}/a1"));
            }
            other @ (LandOutcome::BaseRefMoved | LandOutcome::WitnessOidMismatch) => {
                panic!("expected Landed, got {other:?}")
            }
        }

        let bare_main = git(&bare, &["rev-parse", "main"]);
        assert_eq!(bare_main, verified_sha);
        let witness = git(&bare, &["rev-parse", &format!("capsule-witness/{id}/a1")]);
        assert_eq!(witness, verified_sha);

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Landed);
        assert!(c.landing.is_some());
        assert!(c.pending_land.is_none());
        let att = c.attempts.iter().find(|a| a.id == 1).unwrap();
        assert_eq!(att.outcome, capsule_core::AttemptOutcome::Landed);
    }

    /// Pins land-time witness mismatch handling: abandon the capsule and emit
    /// `{witness_branch, verified_sha}` without `found_sha`.
    #[test]
    fn land_witness_oid_mismatch_abandons_and_emits_incident() {
        let id = "land_mismatch";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let other_sha = git(&bare, &["rev-parse", "main"]);
        assert_ne!(other_sha, verified_sha);
        git(
            &work,
            &[
                "push",
                bare.to_str().unwrap(),
                &format!("{other_sha}:refs/heads/capsule-witness/{id}/a1"),
            ],
        );
        refresh_worker_origin_tracking(&work);

        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);

        let ack = s
            .land(LandRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                lander: "test-lander".into(),
                remote: bare.to_str().unwrap().into(),
                repo_dir: work,
                skip_deploy_verify_gate: true,
            })
            .unwrap();
        assert!(
            matches!(ack.outcome, LandOutcome::WitnessOidMismatch),
            "expected WitnessOidMismatch, got {:?}",
            ack.outcome
        );

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Abandoned);
        assert!(c.pending_land.is_none());
        let att = c.attempts.iter().find(|a| a.id == 1).unwrap();
        assert_eq!(att.outcome, capsule_core::AttemptOutcome::Abandoned);

        let v = read_event_payload(&s, id, "operational_incident");
        assert_eq!(v["kind"], "witness_oid_mismatch");
        let detail = v.get("detail").expect("missing detail wrapper");
        assert!(detail.is_object(), "detail must be a JSON object");
        assert_eq!(detail["witness_branch"], format!("capsule-witness/{id}/a1"));
        assert_eq!(detail["verified_sha"], verified_sha);
        assert!(
            detail.get("found_sha").is_none(),
            "land-time payload has no found_sha"
        );
    }

    /// Pins land-time `BaseRefMoved`: a non-FF base push clears
    /// `pending_land`, emits `pending_land_cleared`, and leaves the
    /// capsule `Accepted` with the active attempt still `InFlight`.
    ///
    /// Setup forces bare:main to a sha that is not an ancestor of
    /// `verified_sha`, so the atomic push of `verified_sha → main` is
    /// rejected non-FF and exercises the `BaseRefMoved` arm.
    #[test]
    fn land_base_ref_moved_clears_pending_and_stays_accepted() {
        let id = "land_baseref";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);

        let init_sha = git(&work, &["rev-parse", "HEAD~"]);
        git(&work, &["checkout", "-b", "diverge", &init_sha]);
        git(&work, &["commit", "--allow-empty", "-m", "diverge"]);
        let diverge_sha = git(&work, &["rev-parse", "HEAD"]);
        assert_ne!(diverge_sha, verified_sha);
        git(
            &work,
            &[
                "push",
                "--force",
                bare.to_str().unwrap(),
                &format!("{diverge_sha}:refs/heads/main"),
            ],
        );

        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);

        let ack = s
            .land(LandRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                lander: "test-lander".into(),
                remote: bare.to_str().unwrap().into(),
                repo_dir: work,
                skip_deploy_verify_gate: true,
            })
            .unwrap();
        assert!(
            matches!(ack.outcome, LandOutcome::BaseRefMoved),
            "expected BaseRefMoved, got {:?}",
            ack.outcome
        );

        let bare_main = git(&bare, &["rev-parse", "main"]);
        assert_eq!(bare_main, diverge_sha, "remote base ref untouched");

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Accepted);
        assert!(c.pending_land.is_none());
        assert!(c.landing.is_none());
        let att = c.attempts.iter().find(|a| a.id == 1).unwrap();
        assert_eq!(att.outcome, capsule_core::AttemptOutcome::InFlight);

        let cleared: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'pending_land_cleared'",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cleared, 1, "exactly one pending_land_cleared event");
        let landed: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_landed'",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(landed, 0, "BaseRefMoved must not emit capsule_landed");

        let (payload, actor): (String, String) = s
            .conn
            .query_row(
                "SELECT payload_json, actor FROM event
                 WHERE capsule_id = ?1 AND kind = 'pending_land_cleared'",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let v: json::Value = json::from_str(&payload).unwrap();
        assert_eq!(v["reason"], "base_ref_moved");
        assert_eq!(v["by"], "test-lander");
        assert_eq!(actor, "test-lander");
    }

    /// Pin land's transient OtherFailure path: unclassified push failures
    /// must clear pending_land and emit the paired operational incident
    /// (kind=land_other_failure) with stderr in detail.
    #[cfg(unix)]
    #[test]
    fn land_other_failure_clears_pending_and_emits_paired_incident() {
        let id = "land_other";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);

        let hook = bare.join("hooks").join("pre-receive");
        std::fs::write(
            &hook,
            "#!/bin/sh\necho 'capsule-test: policy rejected push' >&2\nexit 1\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);

        let err = s
            .land(LandRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                lander: "test-lander".into(),
                remote: bare.to_str().unwrap().into(),
                repo_dir: work,
                skip_deploy_verify_gate: true,
            })
            .unwrap_err();
        let stderr = match err {
            StoreError::LandOtherFailure(s) => s,
            other => panic!("expected LandOtherFailure, got {other:?}"),
        };
        assert!(
            stderr.contains("policy rejected push"),
            "stderr must carry hook message, got: {stderr}"
        );

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Accepted);
        assert!(c.pending_land.is_none());
        let att = c.attempts.iter().find(|a| a.id == 1).unwrap();
        assert_eq!(att.outcome, capsule_core::AttemptOutcome::InFlight);

        let cleared = read_event_payload(&s, id, "pending_land_cleared");
        assert_eq!(cleared["reason"], "other_failure");
        assert_eq!(cleared["by"], "test-lander");

        let incident = read_event_payload(&s, id, "operational_incident");
        assert_eq!(incident["kind"], "land_other_failure");
        let detail = incident.get("detail").expect("missing detail wrapper");
        assert!(
            detail["stderr"]
                .as_str()
                .unwrap()
                .contains("policy rejected push"),
            "incident detail.stderr must carry hook message, got: {detail}"
        );
    }

    /// DESIGN §6/§7.1.2: `pending_land_committed` must use the exact
    /// `PendingLand` JSON shape shared with `pending_land_json`.
    #[test]
    fn pending_land_committed_event_payload_matches_design_spec() {
        let id = "pending_payload";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        s.land(LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir: work,
            skip_deploy_verify_gate: true,
        })
        .unwrap();

        let v = read_event_payload(&s, id, "pending_land_committed");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "at",
                "attempt_id",
                "lander",
                "prior_base_sha",
                "verified_sha",
                "witness_branch",
            ]
        );
        let expected_branch = format!("capsule-witness/{id}/a1");
        assert_eq!(v["verified_sha"].as_str(), Some(verified_sha.as_str()));
        assert_eq!(v["lander"].as_str(), Some("test-lander"));
        assert_eq!(v["attempt_id"].as_u64(), Some(1));
        assert_eq!(v["witness_branch"].as_str(), Some(expected_branch.as_str()));
        assert!(v["prior_base_sha"].as_str().is_some());
    }

    /// Pin pending_land_committed audit-row attribution: actor is the
    /// lander principal, not the claiming session, and attempt_id is the
    /// active attempt. The payload pin only covers JSON fields.
    #[test]
    fn pending_land_committed_event_row_attributes_to_lander() {
        let id = "pending_attr";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        let ack = s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        s.land(LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir: work,
            skip_deploy_verify_gate: true,
        })
        .unwrap();
        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'pending_land_committed'",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one pending_land_committed row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'pending_land_committed'",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "test-lander", "actor must be the lander, not the session_id");
        assert_eq!(
            attempt_id,
            Some(ack.id as i64),
            "attempt_id must point at the active land attempt"
        );
    }

    /// DESIGN §6: `capsule_landed` payload is exactly `Landing` JSON.
    /// Pin replay-visible keys and equality to the persisted `landing_json`
    /// column — `id`/row metadata do not replace payload fields.
    #[test]
    fn capsule_landed_event_payload_matches_design_spec() {
        let id = "land_payload";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        s.land(LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir: work,
            skip_deploy_verify_gate: true,
        })
        .unwrap();

        let v = read_event_payload(&s, id, "capsule_landed");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "advanced_base_ref",
                "at",
                "attempt_id",
                "landed_by",
                "landed_sha",
                "prior_base_sha",
                "witness_branch",
            ]
        );
        assert_eq!(v["landed_sha"].as_str(), Some(verified_sha.as_str()));
        assert_eq!(v["landed_by"].as_str(), Some("test-lander"));
        assert_eq!(v["attempt_id"].as_i64(), Some(1));
        assert_eq!(v["advanced_base_ref"].as_bool(), Some(true));

        let landing = s.get_capsule(id).unwrap().landing.expect("landed");
        assert_eq!(
            json::to_value(&landing).unwrap(),
            v,
            "event payload must mirror persisted landing_json byte-for-byte"
        );
    }

    /// Pin capsule_landed audit-row attribution. Row actor must be the
    /// lander principal, not session_id, and attempt_id must link to the
    /// just-closed attempt. Payload tests cover only the JSON shape.
    #[test]
    fn capsule_landed_event_row_attributes_to_lander() {
        let id = "land_attr";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        let ack = s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        s.land(LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir: work,
            skip_deploy_verify_gate: true,
        })
        .unwrap();

        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_landed'",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one capsule_landed row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_landed'",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(actor, "test-lander", "actor must be the lander, not the session_id");
        assert_eq!(
            attempt_id,
            Some(ack.id as i64),
            "attempt_id must point at the just-closed land attempt"
        );
    }

    /// §7.1.2 crash-retry: a second `land()` against a bare that already has
    /// main + witness at `verified_sha` finds main already at `verified_sha`
    /// (NoOp on base_ref) and witness already at `verified_sha` (same-OID
    /// lease accepted as no-op). After the first call lands the capsule,
    /// the second call sees status=landed and surfaces `WrongStatus` rather
    /// than re-running the push.
    #[test]
    fn land_idempotent_re_run_is_no_op_on_witness() {
        let id = "land2";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let land_req = |repo_dir| LandRequest {
            capsule_id: id.into(),
            session_id: "sess1".into(),
            lander: "test-lander".into(),
            remote: bare.to_str().unwrap().into(),
            repo_dir,
            skip_deploy_verify_gate: true,
        };
        s.land(land_req(work.clone())).unwrap();
        let err = s.land(land_req(work)).unwrap_err();
        assert!(matches!(
            err,
            StoreError::WrongStatus {
                op: "land",
                current_status: "landed",
                ..
            }
        ));
    }

    /// Step 0 deploy-verify gate runs before capsule lookup. Without a recorded
    /// pass, `land()` must report `DeployVerifyMissing` even for an unknown
    /// capsule.
    #[test]
    fn land_deploy_verify_gate_outranks_not_found() {
        let mut s = tmp_store();
        let err = s
            .land(LandRequest {
                capsule_id: "ghost".into(),
                session_id: "sess1".into(),
                lander: "test".into(),
                remote: "unused".into(),
                repo_dir: std::env::temp_dir(),
                skip_deploy_verify_gate: false,
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::DeployVerifyMissing),
            "got {err:?}"
        );
    }

    /// Symmetric to `land_deploy_verify_gate_outranks_not_found`: a recorded
    /// pass clears step 0, so `NotFound` resurfaces for an unknown capsule —
    /// proves the gate is not unconditionally hard-failing `land`.
    #[test]
    fn land_deploy_verify_gate_clears_after_recorded_pass() {
        let mut s = tmp_store();
        s.record_deploy_verify_pass("hermetic", "main").unwrap();
        let err = s
            .land(LandRequest {
                capsule_id: "ghost".into(),
                session_id: "sess1".into(),
                lander: "test".into(),
                remote: "unused".into(),
                repo_dir: std::env::temp_dir(),
                skip_deploy_verify_gate: false,
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::NotFound(ref id) if id == "ghost"),
            "got {err:?}"
        );
    }

    /// Pins deploy-verify as the first land gate for existing capsules too.
    /// An Active+unattested capsule would be `NotLandable` after fetch/extract,
    /// but missing deploy verification must still surface first.
    #[test]
    fn land_deploy_verify_gate_outranks_not_landable() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        assert_eq!(s.get_capsule("x").unwrap().status, Status::Active);
        let err = s
            .land(LandRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                lander: "test".into(),
                remote: "unused".into(),
                repo_dir: std::env::temp_dir(),
                skip_deploy_verify_gate: false,
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::DeployVerifyMissing),
            "got {err:?}"
        );
    }

    /// Active but unattested capsules fail pre-tx with `NotLandable`,
    /// preserving the caller hint to attest before landing.
    #[test]
    fn land_not_landable_for_active_unattested_capsule() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        s.claim(claim_req("x", "sess1")).unwrap();
        assert_eq!(s.get_capsule("x").unwrap().status, Status::Active);
        let err = s
            .land(LandRequest {
                capsule_id: "x".into(),
                session_id: "sess1".into(),
                lander: "test".into(),
                remote: "unused".into(),
                repo_dir: std::env::temp_dir(),
                skip_deploy_verify_gate: true,
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::NotLandable(ref id) if id == "x"),
            "got {err:?}"
        );
    }

    /// Drive the §7.1.2 land-crash decision tree from a test by writing
    /// `PendingLand` straight into `capsule.pending_land_json` (skipping
    /// `Store::land`, which would also commit `Landing` on success), then
    /// either running the real atomic land push (`do_push = true`, what
    /// `Store::land`'s step 3 does) or fabricating a divergent witness ref
    /// at `push_witness_at` to simulate protection leak / corruption
    /// (decision-tree branch 3). Leaves `status=accepted` with
    /// `pending_land` set so reconcile sees the crash window.
    #[allow(clippy::too_many_arguments)]
    fn simulate_land_crash(
        s: &Store,
        id: &str,
        verified_sha: &str,
        prior_base_sha: &str,
        bare: &std::path::Path,
        work: &std::path::Path,
        do_push: bool,
        push_witness_at: Option<&str>,
    ) {
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
            let outcome = capsule_git::land_push(
                work,
                bare.to_str().unwrap(),
                "main",
                &pending.witness_branch,
                verified_sha,
            )
            .unwrap();
            assert!(matches!(
                outcome,
                capsule_git::LandOutcome::Advanced { .. } | capsule_git::LandOutcome::NoOp
            ));
        } else if let Some(other_sha) = push_witness_at {
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

    /// Crash window: §7.1.2 step 3 (push) succeeded, step 4 (DB commit)
    /// didn't. Reconcile must promote `accepted+pending_land` →
    /// `landed`, attribute the landing to `reconciler`, and emit the
    /// §6 `reconciler_ran` audit row with the canonical
    /// `{decision, witness_remote_state}` shape — sha embedded in
    /// `witness_remote_state` so audit rows stay self-contained
    /// without re-joining `PendingLand`.
    #[test]
    fn reconcile_landed_when_witness_at_verified_sha() {
        let id = "rec1";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, true, None);

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

        let v = read_event_payload(&s, id, "reconciler_ran");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["decision", "witness_remote_state"]);
        assert_eq!(v["decision"], "landed");
        assert_eq!(v["witness_remote_state"]["state"], "at_verified_sha");
        assert_eq!(v["witness_remote_state"]["sha"], verified_sha);
    }

    /// Recovery counterpart to `capsule_landed_event_row_attributes_to_lander`.
    /// Autonomous recovery must attribute the landing event to the reconciler,
    /// not the original lander from the crashed attempt.
    #[test]
    fn reconcile_recovered_capsule_landed_event_row_attributes_to_reconciler() {
        let id = "rec_attr";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        let ack = s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, true, None);

        let outcome = s
            .reconcile(ReconcileRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Landed);

        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_landed'",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "guard: exactly one capsule_landed row");
        let (actor, attempt_id): (String, Option<i64>) = s
            .conn
            .query_row(
                "SELECT actor, attempt_id FROM event
                 WHERE capsule_id = ?1 AND kind = 'capsule_landed'",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            actor, "reconciler",
            "recovery-path landings attribute to reconciler, not the original lander"
        );
        assert_eq!(
            attempt_id,
            Some(ack.id as i64),
            "attempt_id must point at the attempt the reconciler closed"
        );
    }

    /// Frozen capsules reject `abandon` before session ownership is checked.
    /// Preserves the §7.2 pending-land invariant for both wrong-session and
    /// legitimate-session callers; recovery / `force_unfreeze` must resolve.
    #[test]
    fn abandon_frozen_outranks_cross_session() {
        let id = "abf";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);

        let err = s
            .abandon(AbandonRequest {
                capsule_id: id.into(),
                session_id: "wrong".into(),
                reason: "r".into(),
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::PendingLandFrozen(ref cid) if cid == id),
            "got {err:?}"
        );

        let err = s
            .abandon(AbandonRequest {
                capsule_id: id.into(),
                session_id: "sess1".into(),
                reason: "r".into(),
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::PendingLandFrozen(ref cid) if cid == id),
            "got {err:?}"
        );
    }

    /// `claim` rejects frozen capsules with `PendingLandFrozen`, not
    /// `WrongStatus(Accepted)`. Frozen capsules are mid-land; callers use
    /// this error to trigger reconciliation instead of normal status handling.
    #[test]
    fn claim_frozen_outranks_wrong_status() {
        let id = "frzcl";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        assert_eq!(
            s.get_capsule(id).unwrap().status,
            Status::Accepted,
            "precondition: WrongStatus(Accepted) would otherwise fire"
        );
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);

        let err = s.claim(claim_req(id, "sess2")).unwrap_err();
        assert!(
            matches!(err, StoreError::PendingLandFrozen(ref cid) if cid == id),
            "got {err:?}"
        );
    }

    /// Auto-reclaim must skip frozen capsules even after lease expiry.
    /// The land step-4 reconciler is the only path allowed to resolve
    /// `pending_land_json`; this pins the SQL freeze guard.
    #[test]
    fn reclaim_skips_frozen_capsule_with_expired_lease() {
        let id = "frzexp";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req_with_ttl(id, "sess1", 1)).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);

        std::thread::sleep(std::time::Duration::from_millis(1200));

        let _ = s
            .list_capsules(ListFilter::default())
            .expect("list_capsules drives reclaim_expired_in_tx");

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Accepted, "frozen capsule must not be reclaimed");
        assert_eq!(c.active_attempt, Some(1));
        assert!(c.verification.is_some(), "verification must persist");
        assert!(c.pending_land.is_some(), "pending_land must persist");

        let expired_events: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event WHERE capsule_id = ?1 AND kind = 'attempt_expired'",
                params![id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(expired_events, 0, "frozen sweep must not record expiry");
    }

    /// Crash before push. Witness absent → clear, capsule stays accepted.
    /// Pin the `pending_land_cleared` payload (DESIGN.md §6: `{reason, by}`)
    /// so non-spec extras like `stderr` cannot creep back in, and pin
    /// `payload.by == event.actor` so the `by` audit duplicate stays
    /// consistent with its row column.
    #[test]
    fn reconcile_cleared_when_witness_absent() {
        let id = "rec2";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);

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

        let (payload, actor): (String, String) = s
            .conn
            .query_row(
                "SELECT payload_json, actor FROM event
                 WHERE capsule_id = ?1 AND kind = 'pending_land_cleared'",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let v: json::Value = json::from_str(&payload).unwrap();
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["by", "reason"]);
        assert_eq!(v["reason"], "witness_absent");
        assert_eq!(v["by"], actor);
    }

    /// Pins the Cleared/Absent `reconciler_ran` payload end-to-end.
    /// Absent witness state must omit `sha`; the direct helper pin does
    /// not prove this shape is preserved through reconciler event emission.
    #[test]
    fn reconcile_absent_reconciler_ran_omits_sha() {
        let id = "rec_absent_run";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);

        s.reconcile(ReconcileRequest {
            capsule_id: id.into(),
            remote: bare.to_str().unwrap().into(),
        })
        .unwrap();

        let v = read_event_payload(&s, id, "reconciler_ran");
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["decision", "witness_remote_state"]);
        assert_eq!(v["decision"], "cleared");
        let state = v["witness_remote_state"]
            .as_object()
            .expect("witness_remote_state must be a JSON object");
        let mut state_keys: Vec<&str> = state.keys().map(String::as_str).collect();
        state_keys.sort();
        assert_eq!(state_keys, vec!["state"], "Absent variant must not carry sha");
        assert_eq!(state["state"], "absent");
    }

    /// Witness exists at some other sha — protection leak / corruption.
    /// The setup writes a `noise.txt` commit and pushes its sha as the
    /// witness ref, simulating divergence between PendingLand's
    /// `verified_sha` and the actual witness tip. Pins the §6
    /// `operational_incident` payload as the wrapper shape
    /// `{kind, detail: {witness_branch, expected_sha, found_sha, by}}` —
    /// pre-fix code emitted those keys flat at the top level, which
    /// would shadow `kind`/`detail` consumers.
    #[test]
    fn reconcile_abandoned_when_witness_at_different_sha() {
        let id = "rec3";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        std::fs::write(work.join("noise.txt"), "noise\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "noise"]);
        let other_sha = git(&work, &["rev-parse", "HEAD"]);
        simulate_land_crash(
            &s,
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

        let v = read_event_payload(&s, id, "operational_incident");
        assert_eq!(v["kind"], "witness_oid_mismatch");
        let detail = v.get("detail").expect("missing detail wrapper");
        assert!(detail.is_object(), "detail must be a JSON object");
        assert!(detail.get("witness_branch").is_some());
        assert!(detail.get("expected_sha").is_some());
        assert_eq!(detail["found_sha"], other_sha);
        assert!(
            v.get("witness_branch").is_none(),
            "old flat key must not regress to top level"
        );
        assert!(
            v.get("found_sha").is_none(),
            "old flat key must not regress to top level"
        );
    }

    /// Operator invokes force-unfreeze on a capsule with no pending_land.
    /// Returns `NotFrozen` but still emits `force_unfreeze_invoked` so the
    /// audit log captures the attempt; `snapshot=null` in the payload, and
    /// no `reconciler_ran` row is emitted because the reconciler scope is
    /// gated on `pending_land != null` (DESIGN §7.1.2).
    #[test]
    fn force_unfreeze_on_non_frozen_capsule_audits_operator_action() {
        let mut s = tmp_store();
        make_capsule(&mut s, "x", "src/api");
        let outcome = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: "x".into(),
                remote: "/dev/null".into(),
                operator: "operator-jane".into(),
                reason: "thought it was stuck".into(),
                lander_confirmed_dead: true,
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::NotFrozen);

        let v = read_event_payload(&s, "x", "force_unfreeze_invoked");
        assert_eq!(v["operator"], "operator-jane");
        assert_eq!(v["reason"], "thought it was stuck");
        assert_eq!(v["post_action_outcome"], "not_frozen");
        assert!(v["snapshot"].is_null(), "snapshot must be null when nothing was frozen");

        let count: i64 = s
            .conn
            .query_row(
                "SELECT COUNT(*) FROM event
                 WHERE capsule_id = ?1 AND kind = 'reconciler_ran'",
                params!["x"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
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
                reason: "test".into(),
                lander_confirmed_dead: false,
            })
            .unwrap_err();
        assert!(matches!(err, StoreError::ForceUnfreezeNotConfirmed));
    }

    /// Unconfirmed force-unfreeze requests must fail before capsule lookup,
    /// preserving a stable error surface for unauthorized callers even when
    /// the target capsule does not exist.
    #[test]
    fn force_unfreeze_confirm_outranks_not_found() {
        let mut s = tmp_store();
        let err = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: "ghost".into(),
                remote: "unused".into(),
                operator: "op".into(),
                reason: "test".into(),
                lander_confirmed_dead: false,
            })
            .unwrap_err();
        assert!(
            matches!(err, StoreError::ForceUnfreezeNotConfirmed),
            "got {err:?}"
        );
    }

    /// Operator force-unfreezes a frozen capsule whose witness already
    /// landed (push succeeded, DB commit didn't): outcome is `Landed`,
    /// `landed_by` carries the operator id. Pins the §6
    /// `force_unfreeze_invoked` payload as
    /// `{operator, post_action_outcome, reason, snapshot}` with snapshot
    /// being the parsed `PendingLand` JSON — `witness_branch` is the
    /// key an operator actually references when investigating, so the
    /// assert hits that field rather than just `is_object()`.
    #[test]
    fn force_unfreeze_lands_when_witness_at_verified_sha() {
        let id = "force1";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, true, None);

        let outcome = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
                operator: "operator-jane".into(),
                reason: "lander pid 12345 unresponsive >30m".into(),
                lander_confirmed_dead: true,
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Landed);
        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Landed);
        assert_eq!(c.landing.as_ref().unwrap().landed_by, "operator-jane");

        let (payload, actor): (String, String) = s
            .conn
            .query_row(
                "SELECT payload_json, actor FROM event
                 WHERE capsule_id = ?1 AND kind = 'force_unfreeze_invoked'",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let v: json::Value = json::from_str(&payload).unwrap();
        let obj = v.as_object().expect("payload must be a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(
            keys,
            vec!["operator", "post_action_outcome", "reason", "snapshot"]
        );
        assert_eq!(v["operator"], "operator-jane");
        assert_eq!(
            v["operator"], actor,
            "actor row column must duplicate payload.operator"
        );
        assert_eq!(v["reason"], "lander pid 12345 unresponsive >30m");
        assert_eq!(v["post_action_outcome"], "landed");
        assert!(v["snapshot"]["witness_branch"].is_string());
    }

    /// Force-unfreeze on a witness mismatch abandons and attributes the
    /// abandon audit rows to the operator, not the autonomous reconciler.
    #[test]
    fn force_unfreeze_abandons_when_witness_at_different_sha() {
        let id = "force_diff";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        std::fs::write(work.join("noise.txt"), "noise\n").unwrap();
        git(&work, &["add", "."]);
        git(&work, &["commit", "-m", "noise"]);
        let other_sha = git(&work, &["rev-parse", "HEAD"]);
        simulate_land_crash(
            &s,
            id,
            &verified_sha,
            &prior,
            &bare,
            &work,
            false,
            Some(&other_sha),
        );

        let outcome = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
                operator: "operator-jane".into(),
                reason: "lander dead, witness leaked".into(),
                lander_confirmed_dead: true,
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Abandoned);

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Abandoned);
        assert!(c.pending_land.is_none());

        let incident = read_event_payload(&s, id, "operational_incident");
        assert_eq!(incident["kind"], "witness_oid_mismatch");
        assert_eq!(
            incident["detail"]["found_sha"], other_sha,
            "incident row points at the witness-mismatch event for this capsule"
        );
        for (kind, expected) in [
            ("operational_incident", "operator-jane"),
            ("reconciler_ran", "operator-jane"),
            ("force_unfreeze_invoked", "operator-jane"),
        ] {
            let actor: String = s
                .conn
                .query_row(
                    "SELECT actor FROM event WHERE capsule_id = ?1 AND kind = ?2",
                    params![id, kind],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(actor, expected, "{kind} actor");
        }

        let v = read_event_payload(&s, id, "force_unfreeze_invoked");
        assert_eq!(v["operator"], "operator-jane");
        assert_eq!(v["post_action_outcome"], "abandoned");
        assert!(v["snapshot"].is_object(), "snapshot must carry the pending_land");
    }

    /// Force-unfreeze on witness absence clears `pending_land` without
    /// completing the capsule or attempt. All emitted audit rows are
    /// attributed to the operator, not `reconciler`.
    #[test]
    fn force_unfreeze_clears_when_witness_absent() {
        let id = "force_absent";
        let (_dir, bare, work, verified_sha) = setup_bare_with_attempt(id);
        let mut s = tmp_store();
        make_capsule(&mut s, id, "feature.txt");
        s.claim(claim_req(id, "sess1")).unwrap();
        attest_pass(&mut s, id, &verified_sha);
        let prior = capsule_git::ls_remote_branch(bare.to_str().unwrap(), "main").unwrap();
        simulate_land_crash(&s, id, &verified_sha, &prior, &bare, &work, false, None);

        let outcome = s
            .force_unfreeze(ForceUnfreezeRequest {
                capsule_id: id.into(),
                remote: bare.to_str().unwrap().into(),
                operator: "operator-jane".into(),
                reason: "lander dead, no witness pushed".into(),
                lander_confirmed_dead: true,
            })
            .unwrap();
        assert_eq!(outcome, ReconcileOutcome::Cleared);

        let c = s.get_capsule(id).unwrap();
        assert_eq!(c.status, Status::Accepted, "Cleared keeps status=Accepted");
        assert!(c.pending_land.is_none(), "pending_land cleared");
        assert_eq!(c.attempts.len(), 1, "guard: attempts[0] must be the land attempt");
        assert_eq!(
            c.attempts[0].outcome,
            capsule_core::AttemptOutcome::InFlight,
            "Cleared keeps the attempt in-flight (operator can re-land)"
        );

        for (kind, expected) in [
            ("pending_land_cleared", "operator-jane"),
            ("reconciler_ran", "operator-jane"),
            ("force_unfreeze_invoked", "operator-jane"),
        ] {
            let actor: String = s
                .conn
                .query_row(
                    "SELECT actor FROM event WHERE capsule_id = ?1 AND kind = ?2",
                    params![id, kind],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(actor, expected, "{kind} actor");
        }

        let cleared = read_event_payload(&s, id, "pending_land_cleared");
        assert_eq!(cleared["reason"], "witness_absent");
        assert_eq!(cleared["by"], "operator-jane");

        let v = read_event_payload(&s, id, "force_unfreeze_invoked");
        assert_eq!(v["operator"], "operator-jane");
        assert_eq!(v["post_action_outcome"], "cleared");
        assert!(v["snapshot"].is_object(), "snapshot must carry the pending_land");
    }

    /// `format_iso8601` and `parse_iso8601` are a tied pair: every
    /// timestamp column written via `format_iso8601` is read back via
    /// `parse_iso8601`, so they jointly own the DB-row timestamp
    /// serialization boundary. Pin parse compatibility on diverse
    /// timestamps so a future change that breaks the inverse trips here
    /// before producing rows the other side parses as a different
    /// `OffsetDateTime`. Includes a `.120000000` case so a unilateral
    /// switch to `Rfc3339` (which trims trailing fractional zeros)
    /// would round-trip semantically but is pinned canonically by
    /// [`iso8601_emits_canonical_nine_digit_fractional_seconds`].
    #[test]
    fn iso8601_round_trips_diverse_timestamps() {
        let cases = [
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::now_utc(),
            time::macros::datetime!(2099-12-31 23:59:59.123456789 UTC),
            time::macros::datetime!(0001-01-01 00:00:00 UTC),
            time::macros::datetime!(2024-06-15 12:00:00.120000000 UTC),
        ];
        for t in cases {
            let s = format_iso8601(t).expect("format_iso8601");
            let parsed = parse_iso8601(&s);
            assert_eq!(parsed, t, "round-trip drift via {s}");
        }
    }

    /// Pin the canonical emitted form: `Iso8601::DEFAULT` keeps fixed
    /// nine-digit fractional seconds where `Rfc3339` would trim
    /// trailing zeros. External consumers (audit log readers,
    /// `attempt_expired` event payload passthrough) depend on the
    /// shape, so a unilateral switch must trip a test even though the
    /// semantic round-trip would still pass.
    #[test]
    fn iso8601_emits_canonical_nine_digit_fractional_seconds() {
        assert_eq!(
            format_iso8601(OffsetDateTime::UNIX_EPOCH).unwrap(),
            "1970-01-01T00:00:00.000000000Z",
        );
        assert_eq!(
            format_iso8601(time::macros::datetime!(2024-06-15 12:00:00.120000000 UTC)).unwrap(),
            "2024-06-15T12:00:00.120000000Z",
        );
    }

    /// `parse_iso8601` accepts two repo-local forms: 4-digit year from
    /// `format_iso8601` (DB column writes — created_at, last_heartbeat, etc.)
    /// and 6-digit padded year from `time::serde::iso8601` (timestamps inside
    /// JSON columns). The 6-digit form is load-bearing: `reclaim_expired_in_tx`
    /// and the live-lease checks read `json_extract(lease_json, '$.expires_at')`
    /// and feed the result here, so swapping to `Rfc3339` (which rejects
    /// `+0YYYYY` years) would silently break lease expiry sweeps.
    #[test]
    fn parse_iso8601_accepts_both_4digit_and_6digit_year_forms() {
        let four_digit = "1970-01-01T00:00:00.000000000Z";
        let six_digit = "+001970-01-01T00:00:00.000000000Z";
        assert_eq!(parse_iso8601(four_digit), OffsetDateTime::UNIX_EPOCH);
        assert_eq!(parse_iso8601(six_digit), OffsetDateTime::UNIX_EPOCH);
    }

    /// Pin why `reclaim_expired_in_tx` must compare parsed timestamps, not
    /// strings. `format_iso8601` emits 4-digit years, while
    /// `time::serde::iso8601` emits `+0YYYYY` years; `+` sorts before digits,
    /// so a live future lease can lex-compare as older than `now_str`.
    #[test]
    fn parse_iso8601_year_padding_inverts_lex_compare() {
        let now_4digit = "2024-12-31T00:00:00.000000000Z";
        let future_6digit = "+002025-01-01T00:00:00.000000000Z";
        assert!(
            future_6digit < now_4digit,
            "serde future timestamp sorts before 4-digit now"
        );
        assert!(
            parse_iso8601(future_6digit) > parse_iso8601(now_4digit),
            "parsed future timestamp is after now"
        );
    }

    /// Pin the `parse_wire` panic message: kind label is per-call-site
    /// (distinct between `parse_status` and `parse_outcome`) and the
    /// offending value reaches the panic. A refactor that drops either
    /// halves the diagnostic the operator sees on a CHECK-violating row.
    #[test]
    #[should_panic(expected = "unknown status in DB: not-a-real-status")]
    fn parse_status_panics_with_kind_and_value_on_unknown_input() {
        let _ = parse_status("not-a-real-status");
    }

    #[test]
    #[should_panic(expected = "unknown attempt outcome in DB: not-a-real-outcome")]
    fn parse_outcome_panics_with_kind_and_value_on_unknown_input() {
        let _ = parse_outcome("not-a-real-outcome");
    }
}
