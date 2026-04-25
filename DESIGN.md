# Capsule — a pure design (v12)

**Status:** draft for review, revision 12
**Audience:** someone who has never heard of `stack`, goals, dispatch, or orchestrate.
**Changes from v11 (self-critic pass — doc-completeness for fresh reader):**
- **§6 CLI surface fully enumerated.** v11 said "Unchanged from v7 except the land sub-procedure" but v7 was not in this doc. v12 lists every operation a caller invokes (`create`, `claim`, `heartbeat`, `attest`, `abandon`, `reclaim`, `add-dep`, `remove-dep`, `list`, `land`, `force-unfreeze`) with arguments, preconditions, and side effects.
- **§7.1.1 claim protocol spelled out.** Was "Unchanged from v5/v6/v7." v12 inlines the DB transaction.
- **New §7.1.0 attest** defines verification as a first-class step (was scattered across the document).
- **Session, heartbeat, lease semantics defined explicitly** in §3.3. Was implicit; fresh reader could not tell what a "session" is or how the lease is kept alive.
- **§4 Event taxonomy enumerated** (replaced "Event { ... as before }").
- **Reconciler-sweep interval clarification.** v11's force-unfreeze post-action verification waits "one reconciler-sweep interval"; if a lander is between `PendingLand` write and push, that interval is `max(60s, lease_ttl/4)`. v12 specifies that the post-action wait is `2 × sweep interval` to allow for clock skew + push retry latency.
- **Heartbeat cadence: `lease_ttl / 3`** with miss-three semantics, explicit in §3.3.
- **Agent-facing surface formalized** (§9.1): OSS release ships a `/capsule` skill as the primary agent API; CLI is the protocol. Skill encodes the discipline + recovery matrix + setup walkthrough. Optional `PreToolUse` hook (§9.2) hardens discipline into enforcement.

**Changes from v10 (self-critic pass; codex unavailable for v10 review):**
- **PendingLand has no placeholder state.** v10 wrote `PendingLand` in step 1 with a `prior_base_sha` placeholder, then patched it in step 2. A crash between step 1 and step 2 left the reconciler with a placeholder — `advanced_base_ref` would be miscomputed. v11 reorders: remote `base_ref` read happens *first*, then a single DB transaction writes the complete `PendingLand`. Eliminates the partial-state class entirely.
- **Reconciler concurrency: CAS semantics.** Two reconciler instances racing on the same `pending_land` could both write `Landing`. v11 mandates compare-and-set on `pending_land` (read-then-write conditioned on `pending_land == read_value`). Loser's commit no-ops; winner's takes effect. Documented in §7.2.
- **force-unfreeze hardened.** v10 said the hatch "runs the reconciler decision tree." But if a lander is mid-push (between DB write of `PendingLand` and the git push), force-unfreeze can clear `pending_land` while the lander is about to push successfully — leaving git advanced but DB unaware. v11 adds a precondition: operator MUST confirm the lander process is dead/unresponsive (the CLI requires `--lander-confirmed-dead` flag with audit trail). v11 also adds a post-action verification loop: after force-unfreeze clears, re-query remote witness for one reconciler interval and reapply if witness appears.
- **Acceptance timeout, verification updates, log_ref binding.** §6 / §7 now specify: acceptance timeout → verification fails (records `exit_code = "timeout"`); verification can be replaced while `active`, locked once `accepted`; `log_ref` MUST be write-once or content-addressed at the deployment level (no in-place overwrites).
- **Empty repo / first commit handled.** If `base_ref` does not exist on the remote, `prior_base_sha = ZERO_OID`; descendant-or-equal becomes "any sha is FF from null" (git's plain push to a non-existent branch creates it). Documented in §7.1.2.
- **Cycles in `depends_on` rejected.** `add-dep` runs a cycle check in the same DB transaction that mutates `depends_on`. Documented in §7.1.3 reference.
- **abandoned releases the write-set mutex.** Previously implicit. Now stated in §3 and §7.2.
- **PendingLand implies lease held.** While `pending_land != null`, lease cannot expire (covered by reclaim-freeze) AND heartbeats are not required. Stated in §7.2.
- **Attempt-branch trust model.** Attempt branches (`capsules/<id>/a<N>`) are NOT git-protected per-attempt — only the namespace is push-allowed for workers. Trust comes from the DB lease (only the leased session can `attest`/`land`). A malicious worker can push noise to a different capsule's attempt-branch namespace, but cannot induce a false land. Stated in §3.2 / §7.4.
- **One-sentence summary corrected** (held over from v10): replaced stale "creates (never updates)" with accurate null-OID-lease wording.

---

## 1. One sentence

A **capsule** is a crash-safe, git-backed critical section over a declared set of path prefixes: one attempt at a time may modify those paths; a machine-executed acceptance command records an exit code bound to a specific commit sha; landing is a fast-forward of that exact sha onto the configured base ref via an atomic multi-ref git push that, in the same transaction, writes a per-attempt witness branch (with a null-OID `--force-with-lease` guarding against any different-OID pre-existing state at the witness name).

## 2. The problem it solves

When multiple autonomous agents modify a shared codebase, four things go wrong that existing primitives don't cover together:

1. **Concurrency over paths.** Two agents editing the same file produce a merge mess. File locks don't survive process death. Task queues serialize *task rows*, not *write-sets*.
2. **Crash recovery over git.** A dead worker leaves the coordinator guessing. Task queues record "claimed/not claimed"; they don't record "pushed commits abc..def to branch foo on shared remote."
3. **Acceptance evidence bound to content.** "Done" is fuzzy. Without a machine-executed verification result bound to a specific commit sha, a worker can assert completion with work that doesn't match the attested content.
4. **Hard ordering.** Some work must happen after other work lands. Issue-tracker dependencies are unenforced.

Existing tools cover subsets. Celery/SQS: (1) for task rows only, not paths; (2) only as "visibility timeout"; no (3). Temporal/Airflow: (4) and partially (2); no (1) over paths; no (3). Git alone: none. LangChain/AutoGen: none.

A capsule covers all four with a small, composable surface.

## 3. Core concept

A capsule is a **promise, a reservation, and a claim**.

- **The promise** is declarative: write-set (path prefixes), machine-executable acceptance command, dependencies. Deps are mutable while non-terminal (§6); write-set and acceptance are fixed at create time.
- **The reservation** is a mutex over the write-set. At most one capsule holding each prefix may be *in-flight* (status ∈ {`active`, `accepted`}); overlapping capsules cannot claim until the first lands or is abandoned.
- **The claim** is an imperative attempt: a time-bounded lease plus a monotonic attempt id. Lease is held across `active` and `accepted`; expires without heartbeats; reclaims bump the attempt id.

Work is recorded as commits on a per-attempt git branch. Git is authoritative for *content and ref state*. The capsule store is authoritative for *intent, ownership, attempt identity, verification evidence, and landing records*.

### 3.1 Publication contract

A deployment requires:

- A **shared git remote** reachable by every host that might claim a capsule. Must support git wire protocol `atomic` (git ≥ 2.4 — every current hosted forge) and `--force-with-lease` (git ≥ 2.13 client-side, universal server support).
- A **lander principal** with push authority for two branch namespaces: `refs/heads/<base_ref>` and `refs/heads/capsule-witness/**`. The store invokes the lander for `land` (§7.1.2).
- **Worker principals** with push authority for their attempt branches (`refs/heads/capsules/<id>/a<N>`).
- **Branch-protection rules** on the shared remote satisfying two *separately enumerated* properties for `capsule-witness/**`: (a) **creation-restricted** to the lander (not merely update-restricted), and (b) **no more-permissive rule matches the same pattern**. Plus the same for `<base_ref>`. See §8.1 for the forge matrix and §8.2 for the ACL test suite every deployment must pass before going live.
- **Reconciler liveness (new in v10).** At least one of: (a) the reconciler runs on every capsule access (default, cheap), (b) a periodic sweep at `max(60s, lease_ttl/4)` is live, or (c) operators are authorized to invoke `capsule force-unfreeze` out-of-band. Without any of these, a crash between the `land` push and the DB commit can leave a capsule indefinitely frozen (§7.2).

`land` is the only operation that advances `base_ref`. The `land` push uses `--force-with-lease=refs/heads/<witness_branch>:` (empty expect = null OID). Empirically (git 2.39.5) this **rejects** a pre-existing witness at a *different* OID — atomic-failing the whole push so `base_ref` is never advanced in that case — and is a **no-op** for a pre-existing witness at the *same* OID (`Everything up-to-date` exit 0). That same-OID behavior is what we want for crash-retry idempotency (§7.1.2). The lease is therefore a runtime integrity check (different-OID = protection leak or externally rewritten state → fail loud), *not* a create-only primitive. Per-attempt witness *name* uniqueness — which is what actually prevents cross-attempt collision — comes from the monotonic `a<N>` suffix and creation-restriction branch protection.

### 3.2 Threat model

Crash-safe and stale-actor-safe, not Byzantine.

- **Protected against:** worker crashes, network partitions, delayed messages, stale leases, reclaimed attempts whose prior worker keeps pushing (inert — pushes go to the stale per-attempt branch, not to the active attempt's ref), **stale landers delayed between `PendingLand` commit and push** (`pending_land != null` freezes reclaim; see §7.2).
- **Not protected against:**
  - A currently-leased worker that heartbeats but refuses to land — blocks overlapping capsules until lease expiry. External lease-ceiling policy is the mitigation.
  - Lander principal compromise — arbitrary `base_ref` and `capsule-witness/**` mutation within the lander's push authority. Protect as a trusted service principal.
  - External writes to `base_ref` or to `refs/heads/capsule-witness/**` outside the capsule protocol. Deployments that cannot enforce §3.1's branch-protection requirements must not use the witness branch as proof-of-land (see §7.1.2 reconciler).
  - **Cross-attempt-branch noise (new in v11 — explicit).** The git layer protects only the namespaces (`capsules/**` allowed for workers, `capsule-witness/**` lander-only, `<base_ref>` lander-only). Within `capsules/**`, any worker with namespace push authority can push to *any* capsule's attempt branch. Per-attempt integrity comes from the **DB lease** (only the leased session can call `attest` / `land`), not from git. A malicious worker can push noise commits to another capsule's attempt branch but cannot induce a false land — the DB lease + verified_sha binding prevent this. Cleanup is operational: §8.4 prune.

### 3.3 Session, lease, heartbeat (defined in v12)

- **Session.** A `session_id` is an opaque, caller-minted identifier for a single worker process's claim on a single capsule attempt. Sessions are 1:1 with attempts: a session is created when `claim` succeeds and is consumed when the attempt terminates (`landed`, `abandoned`, `expired`, or `released`). A new attempt requires a new session.
- **Lease.** `Lease{owner, session_id, acquired_at, expires_at}`. `expires_at = acquired_at + lease_ttl`. `lease_ttl` is a deployment parameter (recommended: 5 minutes). Recommended bounds: ≥ 60s (avoid heartbeat thrash), ≤ 1h (bound stale-actor blast radius).
- **Heartbeat.** `capsule heartbeat <id> --session <session_id>` advances `Attempt.last_heartbeat` to `now` and `lease.expires_at` to `now + lease_ttl`, in one DB transaction. Workers SHOULD heartbeat every `lease_ttl / 3` (miss-three semantics: a worker missing two consecutive heartbeats is suspect; missing three is treated as crashed once `expires_at` passes).
- **Lease expiry.** When `now > lease.expires_at`, the next reclaim path observing the capsule transitions it back to `planned` (bumping the attempt id; the prior session is dead-lettered) — *iff* `pending_land == null` (§7.2 reclaim freeze).
- **Lease retention through `accepted`.** The lease is held continuously from claim through `land`. `accepted` is a sub-state, not a hand-off. The same session that claimed must call `attest` and `land`. Cross-session land is rejected.

## 4. Data model

```
Capsule {
  id:                  string           # stable, opaque
  title:               string
  description:         string
  acceptance:          Acceptance
  scope_prefixes:      [CanonicalPath]  # canonicalized at create (§7.0)
  base_ref:            string           # required; e.g. "main"
  depends_on:          [capsule_id]
  status:              Status           # {planned, active, accepted, landed, abandoned}
  active_attempt:      AttemptId?
  attempts:            [Attempt]
  verification:        Verification?
  pending_land:        PendingLand?     # NEW v8 (§7.1.2)
  landing:             Landing?
  created_at:          timestamp
  updated_at:          timestamp
}

Acceptance { run, expect_exit, cwd?, timeout_sec? }

Attempt {
  id:              AttemptId
  lease:           Lease                # retained through accepted
  branch:          string               # "capsules/<id>/a<N>"
  witness_branch:  string               # "capsule-witness/<id>/a<N>"
  base_sha:        string               # immutable; captured at claim
  tip_sha:         string?
  last_heartbeat:  timestamp
  outcome:         "in_flight" | "released" | "expired" | "abandoned" | "landed"
  opened_at:       timestamp
  closed_at:       timestamp?
}

Lease { owner, session_id, acquired_at, expires_at }

Verification {
  at, attestor, attempt_id,
  verified_sha, command, exit_code, duration_ms, log_ref
}

# NEW in v8. Committed to the DB BEFORE the land push.
# Makes Landing reconstructable after a crash between push and DB commit.
PendingLand {
  at:              timestamp
  attempt_id:      AttemptId
  verified_sha:    string               # sha being pushed to base_ref + witness
  prior_base_sha:  string               # remote base_ref tip as observed pre-push
  witness_branch:  string               # "capsule-witness/<id>/a<N>"
  lander:          string               # principal id
}

Landing {
  at, landed_sha, prior_base_sha, landed_by, attempt_id,
  witness_branch,           # "capsule-witness/<id>/a<N>" — the durable proof
  advanced_base_ref: bool   # true if the push moved base_ref; false if it was a
                            # no-op (base_ref already equalled verified_sha)
}

Event {
  at:           timestamp
  capsule_id:   string
  attempt_id:   AttemptId?
  actor:        string             # session_id, lander principal, "reconciler", or operator id
  kind:         EventKind          # see below
  payload:      json               # kind-specific
}

# v12: enumerated. Append-only audit log; never mutated.
EventKind ∈ {
  capsule_created,                 # payload: {acceptance, scope_prefixes, base_ref, depends_on}
  dependency_added,                # payload: {dep_id}
  dependency_removed,              # payload: {dep_id}
  attempt_claimed,                 # payload: {attempt_id, session_id, base_sha, lease}
  attempt_heartbeat,               # payload: {lease_expires_at}            (optional; high-volume)
  attempt_attested,                # payload: {verified_sha, exit_code, command, log_ref, duration_ms}
  attempt_released,                # payload: {reason}                      (worker-initiated)
  attempt_expired,                 # payload: {at, prior_lease_expires_at}
  pending_land_committed,          # payload: PendingLand                   (DB tx of §7.1.2 step 2)
  pending_land_cleared,            # payload: {reason, by}                  (success / failure / reconciler / force-unfreeze)
  capsule_landed,                  # payload: Landing
  capsule_abandoned,               # payload: {reason}
  reconciler_ran,                  # payload: {decision, witness_remote_state}
  force_unfreeze_invoked,          # payload: {operator, reason, snapshot, post_action_outcome}
  operational_incident,            # payload: {kind, detail}                (§7.1.2 step 5 witness_oid_mismatch, §6 force-unfreeze undone, etc.)
}
```

## 5. State machine

Unchanged from v7. States: `planned`, `active`, `accepted`, `landed`, `abandoned`.

- `planned → active` via atomic claim (§7.1.1).
- `active → accepted` on verification pass (strict equality of `verified_sha` to pushed attempt-tip sha).
- `accepted → landed` via `land` (atomic multi-ref fast-forward push; §7.1.2). Strict `landed_sha == verified_sha`.
- `{active, accepted} → planned` on lease expiry (reclaim bumps attempt id), **iff `pending_land == null`** (§7.2 invariant). A capsule with `pending_land` is reclaim-frozen until the reconciler resolves it.
- `* → abandoned` terminal, explicit.

## 6. Operations (CLI surface)

The full CLI is enumerated in v12 (was "unchanged from v7" in prior revisions, which was opaque to a fresh reader). Every operation that mutates state is a single DB transaction (or a single git transaction, in the case of `land` step 3). All commands accept `--json` for structured output. Errors are exit-coded; preconditions are documented per command.

```
capsule create
  --title <string>
  --description <string>
  --acceptance-cmd <shell>           # acceptance.run
  --acceptance-expect-exit <int>     # acceptance.expect_exit (default 0)
  --acceptance-cwd <path>            # optional
  --acceptance-timeout-sec <int>     # optional; see §7.2 acceptance timeout
  --scope <prefix>                   # repeatable; canonicalized at create (§7.0)
  --base-ref <branch>                # required, e.g. "main"
  --depends-on <capsule_id>          # repeatable; cycle-checked vs current graph (§7.1.3)
  # Effects: inserts Capsule{status=planned, attempts=[], …}; emits capsule_created.
  # No git effect.

capsule list
  [--available]                      # capsules whose deps are all landed AND whose
                                     # scope_prefixes do not overlap any in-flight capsule
  [--status planned|active|accepted|landed|abandoned]
  [--scope-overlaps <prefix>]
  # Read-only. On a capsule with pending_land != null, list triggers a
  # reconciler decision-tree pass (§7.1.2) for that capsule before returning.

capsule claim <id>
  --owner <principal>
  --session <session_id>             # caller-minted opaque (§3.3)
  --lease-ttl-sec <int>              # default deployment-configured (recommended 300)
  --base-sha <sha>                   # remote base_ref tip as observed by the
                                     # worker pre-claim. Informational; not
                                     # load-bearing for land (§7.1.1 step 5).
                                     # Deployments where the store has direct
                                     # git read access may omit this and let
                                     # the store observe directly.
  # See §7.1.1 for the atomic DB transaction. Returns Attempt with the new
  # branch and witness_branch names. Worker is then expected to push commits
  # on the attempt branch and heartbeat. Emits attempt_claimed.

capsule heartbeat <id>
  --session <session_id>
  # Single DB tx: verifies session matches active_attempt.lease, lease still
  # live; advances last_heartbeat = now and lease.expires_at = now + lease_ttl.
  # Cadence: lease_ttl/3 (§3.3). No git effect.

capsule attest <id>
  --session <session_id>
  --verified-sha <sha>               # tip of the attempt branch as pushed
  --command <shell>                  # what was run (recorded; should equal acceptance.run)
  --exit-code <int|"timeout">        # if "timeout", capsule stays active (§7.2 acceptance timeout)
  --duration-ms <int>
  --log-ref <uri>                    # write-once or content-addressed (§7.2 log_ref integrity)
  # See §7.1.0. Writes Verification; if exit_code matches acceptance.expect_exit,
  # transitions active → accepted (locking Verification — §7.2). Otherwise leaves
  # status=active and worker may retry. Cross-session attest rejected.

capsule abandon <id>
  --session <session_id>
  --reason <string>
  # Terminal. Single DB tx: status=abandoned, attempt outcome=abandoned, releases
  # write-set mutex (§7.2). Lease is voided. Refused if pending_land != null
  # (operator must use force-unfreeze first, or wait for reconciler).

capsule reclaim <id>
  # Manual reclaim (rarely needed; the periodic reclaim path runs on every list
  # / claim / heartbeat). Refused if pending_land != null (§7.2 reclaim freeze).
  # If lease has expired: status → planned, attempt outcome=expired, attempt id
  # bumps on next claim. If lease is still live: no-op.

capsule add-dep <id> --depends-on <other_id>
capsule remove-dep <id> --depends-on <other_id>
  # See §7.1.3. add-dep runs in-tx cycle check; remove-dep cannot create cycles.
  # No-op if capsule is in a terminal state (landed/abandoned).

capsule land <id>
  --session <id>
  # Lander does:
  #   1. Read remote base_ref tip -> prior_base_sha. (If base_ref does not
  #      exist on the remote: prior_base_sha = ZERO_OID; the eventual push
  #      will create base_ref. See §7.1.2.)
  #   2. DB transaction (single write, no placeholder; reordered v11):
  #      verify capsule.status == accepted, session matches lease, lease live,
  #      pending_land == null. AND write the COMPLETE PendingLand{verified_sha,
  #      prior_base_sha, witness_branch, lander, attempt_id, at=now}. Commit.
  #      The DB-write freezes reclaim for this capsule (§7.2 invariant).
  #      A race with another lander whose base_ref read interleaves with ours
  #      is resolved by the atomic push in step 3 (base_ref_moved error if
  #      our prior_base_sha is stale).
  #   3. Execute, as a single --atomic push:
  #        git push --atomic
  #          --force-with-lease=refs/heads/<witness_branch>:
  #          <remote>
  #          <verified_sha>:refs/heads/<base_ref>
  #          <verified_sha>:refs/heads/<witness_branch>
  #      No --force. Remote accepts iff:
  #        - verified_sha is descendant-or-equal to current remote base_ref
  #          (plain fast-forward; equality is the no-op case), AND
  #        - the witness branch is absent OR already at verified_sha
  #          (null-OID lease: different-OID -> atomic reject; same-OID ->
  #          Everything up-to-date, treated as idempotent success).
  #      Both conditions enforced in one remote transaction.
  #   4. On success: DB transaction -> status=landed, attempt outcome=landed,
  #      populate Landing (from PendingLand; advanced_base_ref =
  #      (verified_sha != prior_base_sha)); clear pending_land.
  #   5. On failure: synchronously clear pending_land in one DB transaction:
  #        - base_ref_moved: status stays accepted; caller rebases + re-attests.
  #        - witness_oid_mismatch: status -> abandoned; log operational
  #          incident (branch-protection leak or external corruption).
  #      The reconciler is NOT used for ordinary failure paths; it handles
  #      ONLY the crash-between-push-and-DB-commit case (see §7.1.2).

capsule force-unfreeze <id>
  --operator <auditable-identity>
  --reason <free-text>
  --lander-confirmed-dead     # required (v11): operator attests the
                              # PendingLand.lander process is dead. This is
                              # an operational best-effort claim, not a
                              # checked precondition; the post-action loop
                              # below catches false attestations.
  # Operator-only escape hatch. Required when the reconciler service is
  # down AND no access path will fire AND the lander is confirmed dead.
  # Process:
  #   1. Refuses if pending_land == null (nothing to unfreeze).
  #   2. Snapshots PendingLand to the audit log.
  #   3. Runs the reconciler decision tree synchronously (§7.1.2) under
  #      the operator identity, with CAS on pending_land (§7.2).
  #   4. Post-action verification: re-queries remote witness state after
  #      2 × reconciler-sweep interval (sweep interval is max(60s, lease_ttl/4);
  #      the 2x allows for clock skew + retry latency in a slow lander push).
  #      If a witness branch matching the
  #      snapshotted PendingLand has appeared (proving the lander was not
  #      actually dead), emits a "force-unfreeze undone" incident
  #      requiring operator review. Does NOT auto-restore Landing — by
  #      this point the capsule may have been re-claimed onto a new
  #      attempt, and silent restoration would conflict.
  # Emits a mandatory incident event with {operator, reason, pre-state,
  # post-state, snapshot}.
```

## 7. Invariants and guarantees

### 7.0 Path canonicalization

Unchanged from v3-v7: POSIX, case-sensitive NFC, path-component-wise prefix overlap. `src/foo` overlaps `src/foo/bar.rs` but not `src/foobar`.

### 7.1 Protocols

#### 7.1.0 Attest (single DB transaction; defined explicitly in v12)

Worker has pushed `verified_sha` to the attempt branch on the remote and run the acceptance command locally (or via CI; the deployment chooses). Worker calls `attest` with `--verified-sha`, `--exit-code`, `--log-ref`, etc. (§6).

In one DB transaction:
1. Verify capsule status ∈ {`active`}; reject if `accepted`/`landed`/`abandoned` (verification is locked once `accepted` per §7.2).
2. Verify session matches `active_attempt.lease.session_id`; reject `cross-session` otherwise.
3. Verify lease is live (`now < lease.expires_at`); reject `lease_expired` otherwise.
4. Write `Verification{at=now, attestor=session_id, attempt_id, verified_sha, command, exit_code, duration_ms, log_ref}` (replacing any prior `active`-state Verification — §7.2 verification update semantics).
5. If `exit_code == acceptance.expect_exit`: transition status `active → accepted`. Verification is now locked.
6. Otherwise: status stays `active`; worker may retry. (`exit_code = "timeout"` is a regular failure here unless `acceptance.expect_exit == "timeout"`.)
7. Emit `attempt_attested`.

No git effect. The `verified_sha` is recorded as a string; this protocol does not pull or verify the commit's existence on the remote — the eventual `land` push will atomic-fail if `verified_sha` is unreachable.

#### 7.1.1 Claim (atomic DB transaction; spelled out in v12)

Worker calls `capsule claim <id> --owner P --session S --lease-ttl-sec T` (§6). In one DB transaction:

1. Verify capsule status == `planned` (or `accepted` reverting to `planned` via expired lease — see step 2).
2. If status ∈ {`active`, `accepted`} but `now > active_attempt.lease.expires_at` AND `pending_land == null` (§7.2 reclaim freeze): close the prior attempt as `outcome=expired`, transition status → `planned`, then proceed.
3. Verify all `depends_on` capsules are in `landed`; reject `unmet_deps` otherwise.
4. Verify no other in-flight (`status ∈ {active, accepted}`) capsule has any `scope_prefixes` overlapping this one (§7.0); reject `scope_conflict` otherwise.
5. Allocate `attempt_id = max(prior attempts) + 1` (or `1` for first claim). The new `Attempt` records `branch = "capsules/<id>/a<N>"`, `witness_branch = "capsule-witness/<id>/a<N>"`, `base_sha` = remote `base_ref` tip *as observed by the worker pre-claim and supplied as a parameter* (deployments where the store has direct git read access may observe directly; the protocol does not require it — `base_sha` is informational and not load-bearing for `land`).
6. Insert `Attempt{id=attempt_id, lease=Lease{owner=P, session_id=S, acquired_at=now, expires_at=now+T}, branch, witness_branch, base_sha, last_heartbeat=now, outcome=in_flight, opened_at=now}`. Set `active_attempt = attempt_id`, status → `active`.
7. Emit `attempt_claimed`.

Returns the `Attempt` record. Worker now creates the local branch off `base_sha`, pushes work to `branch`, heartbeats, and eventually calls `attest`.

#### 7.1.2 Land (git-atomic multi-ref fast-forward)

Git is the serialization authority; the DB commit follows, but a DB-persisted `PendingLand` records pre-push state for crash recovery *and* freezes reclaim for the duration.

1. **Read remote `base_ref` tip → `prior_base_sha`.** If `base_ref` does not exist on the remote (fresh repo, first ever capsule for this base_ref), `prior_base_sha = ZERO_OID` and the eventual push creates the ref.
2. **DB transaction (single write, fence + pre-commit; reordered in v11).** In one transaction: verify (capsule `accepted`, session matches `active_attempt` lease, lease live, `pending_land == null`); write the **complete** `PendingLand{verified_sha, prior_base_sha, witness_branch, lander, attempt_id, at=now}`. Commit. §7.2 guarantees that once `pending_land != null`, no reclaim path can run on this capsule. If any condition fails, `land` returns without touching the remote. (If the remote `base_ref` moved between step 1's read and the eventual push, step 3's atomic push catches it as `base_ref_moved`. The persisted `prior_base_sha` may be stale relative to the time of push; that is *only* used to compute `Landing.advanced_base_ref` and is correct as "the base_ref tip the lander observed at decision time," which is what we want to record.)
3. Execute:
   ```
   git push --atomic \
     --force-with-lease=refs/heads/<witness_branch>: \
     <remote> \
     <verified_sha>:refs/heads/<base_ref> \
     <verified_sha>:refs/heads/<witness_branch>
   ```
   No `--force`. Empirically verified (git 2.39.5), push outcome decomposes along two axes that the remote evaluates independently under `--atomic`:

   **Base_ref axis:**
   - If `base_ref` does not exist on remote (`prior_base_sha == ZERO_OID`): the push *creates* `base_ref` at `verified_sha` (accepted; treated as advance for `Landing.advanced_base_ref`).
   - If `verified_sha == base_ref_tip`: no-op advance (accepted).
   - If `verified_sha` is a strict descendant of `base_ref_tip`: fast-forward advance (accepted).
   - Otherwise: non-fast-forward (rejected).

   **Witness axis (null-OID `--force-with-lease`):**
   - If witness ref is absent: create at `verified_sha` (accepted).
   - If witness ref exists at `verified_sha`: no-op (accepted).
   - If witness ref exists at any other OID: stale-info reject.

   `--atomic` requires *both* axes to be accepted. On any reject, the remote rolls back both refs. Empirically observed concrete cases:
   - Absent witness + base_ref behind → base_ref advances, witness created. `Landing.advanced_base_ref = true`.
   - Absent witness + base_ref == `verified_sha` → `Everything up-to-date` on base_ref, witness created. `advanced_base_ref = false`.
   - Same-OID witness + base_ref behind → base_ref advances, witness no-op. `advanced_base_ref = true`. Crash-retry case.
   - Same-OID witness + base_ref == `verified_sha` → `Everything up-to-date` on both. Full idempotent re-run.
   - Different-OID witness + any base_ref → atomic reject; neither ref changes. `witness_oid_mismatch` error.
   - Non-FF base_ref + any witness → atomic reject; neither ref changes. `base_ref_moved` error.

   **Descendant-or-equal rationale.** Git's plain push allows updating a ref to its current value as a no-op. This occurs if two capsules attest against the same `verified_sha` and `base_ref` already matches. The first lander advances `base_ref`; the second lander sees `prior_base_sha == verified_sha` and its push is a no-op on `base_ref` but creates *its own* fresh witness branch (witnesses are per-attempt, named `capsule-witness/<id>/a<N>` with globally unique capsule id + monotonic attempt id). `Landing.advanced_base_ref = false` records this.

   **Witness semantics (corrected in v9).** Witness *name* uniqueness — the property that prevents cross-attempt witness collision — comes from the monotonic `a<N>` suffix in the claim protocol (§7.1.1), not from git semantics. Branch-protection creation-restriction (§3.1, §8.2) prevents non-lander actors from pre-creating a witness at some attacker-chosen sha. The null-OID lease on `land` is a runtime integrity check: if someone (or some corruption) has placed a different-OID ref at the witness name we're about to create, the push atomic-fails and `base_ref` is never advanced — a loud failure, not silent corruption. Same-OID re-push is intentional idempotency for crash retry.
4. On push success: DB transaction writes `Landing` (copying fields from `PendingLand`, setting `advanced_base_ref = (verified_sha != prior_base_sha)`), clears `pending_land`, advances status to `landed`.
5. On push failure (synchronous, single DB transaction; reconciler is NOT used for these):
   - `base_ref_moved`: in one DB transaction, clear `pending_land`; capsule stays `accepted`. Caller rebases attempt branch onto new `base_ref`, pushes, re-`attest`s (new `verified_sha`), retries `land` (which will write a fresh `PendingLand`).
   - `witness_oid_mismatch`: in one DB transaction, clear `pending_land`, set capsule → `abandoned`, emit an operational-incident event. Branch-protection leak or external corruption has tainted this witness name; operator investigation required before reuse.

**Reconciler — narrow scope (v10).** The reconciler runs *only* when a crash occurred between the `land` push (step 3) and the DB commit (step 4 or step 5). Triggers: any access to a capsule with `pending_land != null`, plus a periodic sweep at `max(60s, lease_ttl/4)`. Reconciler decision tree:

1. Query the remote: does `refs/heads/<witness_branch>` exist, and if so, at what sha?
2. If it exists at `pending_land.verified_sha`: the push ran before the crash. In one DB transaction: populate `Landing` from `PendingLand` (`at = now`, `landed_by = "reconciled"`, `advanced_base_ref` from stored `prior_base_sha` vs. `verified_sha`), advance status to `landed`, clear `pending_land`.
3. If it exists at a *different* sha: atomic-reject would have fired, so this state implies external corruption or protection leak. In one DB transaction: mark capsule `abandoned`, log incident, clear `pending_land`.
4. If it does not exist: push did not run. In one DB transaction: clear `pending_land`; capsule remains `accepted`.

**Operator escape hatch (new in v10).** `capsule force-unfreeze <id>` is the only mechanism to clear `pending_land` when both the reconciler and all access paths are down. It runs the reconciler decision tree synchronously under an audited operator identity and emits a mandatory incident event. This bounds freeze liveness even if the reconciler service is offline.

The witness branch is the *current-tip* per-attempt proof. After pruning (§8.4), `Landing` — which records `landed_sha`, `witness_branch` name, and `prior_base_sha` — is the durable proof. With creation-restriction branch protection in place (§3.1, §8), prior witness existence at `verified_sha` ⇔ the lander's `land` push ran.

#### 7.1.3 Add-dep / remove-dep

DB-atomic mutation of `depends_on` while the capsule is non-terminal. **Cycle rejection (explicit in v11):** `add-dep A → B` runs a depth-first traversal of the dependency graph in the same DB transaction; if adding the edge would close a cycle, the transaction aborts with `dependency_cycle`. `remove-dep` cannot create cycles. Both are no-ops if the capsule is in a terminal state (`landed` / `abandoned`).

### 7.2 Store-enforced invariants

- `pending_land != null ⇒ status == accepted`.
- **Reclaim freeze.** `pending_land != null ⇒ the capsule is reclaim-frozen`: no lease-expiry path, no manual reclaim, no claim-overlap resolution may transition the capsule or its `active_attempt` while `pending_land` is set. Only `land`'s success path (§7.1.2 step 4), `land`'s synchronous failure cleanup (§7.1.2 step 5), the reconciler (§7.1.2), or the operator escape hatch `capsule force-unfreeze` (§7.1.2) may clear `pending_land`.
- **Fence scope (new in v10).** `PendingLand` fences *attempt reclaim* — it guarantees that while it is set, no other session can claim or advance this capsule's attempt. It does **not** fence remote `base_ref` freshness; that is resolved by the `--atomic` push in step 3, which atomic-rejects if `base_ref` has moved. Once `PendingLand` is committed, session liveness is no longer the trust root — the `PendingLand` record is. A session whose token expired after writing `PendingLand` can still land, provided the lander's own push identity remains valid (§3.1).
- `landing != null ⇒ pending_land == null` and `landing.landed_sha == verification.verified_sha` and `landing.witness_branch == attempt.witness_branch` (for the landed attempt).
- `accepted → landed` requires a successful multi-ref atomic fast-forward push OR reconciliation from `pending_land` via confirmed witness-branch existence at `verified_sha`.
- **Bounded freeze (refined in v10).** Freeze is bounded by whichever of these fires first: (a) any access to the capsule (every capsule read triggers reconciliation on `pending_land != null`), (b) the periodic reconciler sweep at `max(60s, lease_ttl/4)`, or (c) operator invocation of `capsule force-unfreeze`. **Deployment assumption (new in v10):** at least one of (a) or (b) is live; (c) is the escape hatch when both are down. Without any of (a)–(c), the capsule is indefinitely frozen — this is an operational failure, not a protocol flaw.
- **Reconciler concurrency (CAS, new in v11).** The reconciler's `pending_land`-clearing write is a compare-and-set: the transaction is conditioned on `pending_land == read_value`. Two reconciler instances that race on the same capsule are safe — only one CAS succeeds; the other's transaction no-ops (the work was done). This applies to `capsule force-unfreeze` as well.
- **PendingLand implies effective lease hold (new in v11).** While `pending_land != null`: lease cannot expire (covered by reclaim-freeze above), heartbeats are not required, and `lease.expires_at` is logically extended to `min(lease.expires_at, reconciler-clears-pending-land-time)`. The session that wrote `PendingLand` need not stay alive — the lander principal completes the operation.
- **Abandoned releases the write-set mutex (explicit in v11).** Capsules in `abandoned` are not in-flight; their `scope_prefixes` are released and overlapping capsules can claim. Same for `landed`. Mutex is held only while status ∈ {`active`, `accepted`}.
- **`log_ref` integrity (new in v11).** Deployments must use a write-once or content-addressed URI scheme for `Verification.log_ref` (e.g., `s3://bucket/<sha256-of-log>`, `gs://bucket/...?generation=<n>`, `file:///immutable/...`). In-place overwriting violates the "verification bound to content" property.
- **Acceptance timeout (new in v11).** If `Acceptance.timeout_sec` is set and the acceptance command exceeds it, the worker records `Verification.exit_code = "timeout"`, capsule stays `active`, lease still valid; worker may retry `attest`. If `expect_exit != "timeout"`, the verification record marks failure.
- **Verification update semantics (new in v11).** While `active`, `attest` may be called repeatedly; the latest call replaces `Verification`. On `active → accepted` transition (verification matches `expect_exit`), `verification` is locked and immutable for the remainder of the attempt's lifetime. A subsequent `attest` after `accepted` is rejected.

### 7.3 Guarantees

- **Per-attempt fencing.** Each reclaim → new attempt id, new attempt branch, new witness branch.
- **Content-bound landing.** `landed_sha == verified_sha` always; post-land `base_ref` equals `verified_sha` (either advanced or already-equal).
- **Durable completion witness.** Until pruned, `refs/heads/capsule-witness/<id>/a<N>` at `verified_sha` is a live proof: it exists iff the `land` push ran (under deployment-enforced creation-restriction branch protection). After pruning (§8.4), the `Landing` record — `landed_sha`, `witness_branch` name, `prior_base_sha`, `at`, `landed_by` — is the durable proof. The null-OID lease additionally ensures that if something *else* places a different-OID ref at the witness name during a `land` call, the push atomic-fails rather than silently corrupting state.
- **No history rewrites.** Plain fast-forward on `base_ref`; `base_ref` can only advance or stay put. The `--force-with-lease` is targeted *only* at the witness namespace and uses a null-OID lease, which cannot rewrite history — it can only cause create or reject.
- **Crash-safe landing.** `PendingLand` is DB-durable before the push; reconciler reconstructs `Landing` from it regardless of which step crashed.
- **Stale-lander fence.** While `pending_land != null`, no reclaim can run. A lander delayed between the DB fence-write and the atomic push cannot have its attempt stolen out from under it.
- **No crash-induced starvation.** `pending_land == null` lease expiry returns capsule to `planned`. `pending_land != null` is bounded by reconciler sweep interval.
- **Atomicity scope.** DB transactions are the only DB-level consistency points (§7.1.1, §7.1.2 steps 1, 2, 4, 5, §7.1.3). Git's `--atomic` server-side transaction is the only cross-system consistency point (§7.1.2 step 3). `PendingLand` bridges the two without requiring distributed transactions.

### 7.4 Non-guarantees

- No scope-path enforcement on attempt-branch commits (lint only).
- No PR management, no review, no merge execution beyond the `land` fast-forward push.
- No fairness.
- No Byzantine resistance (§3.2).
- No bounded progress under sustained `base_ref` churn (rebase/re-attest loop can livelock).
- No first-class human acceptance.
- **No defense against a deployment with misconfigured branch protection.** If the lander is not the sole creator of `refs/heads/capsule-witness/**`, a non-lander can pre-create a witness at an arbitrary sha. If that sha happens to equal `verified_sha` for a future attempt, the null-OID lease silently accepts it (same-OID idempotency) and the reconciler can mis-attribute a land. If it differs, the attempt atomic-fails (capsule → `abandoned` per §7.1.2 step 5 `witness_oid_mismatch`). The ACL test suite in §8.2 — in particular test 4b (different-OID rejection) and tests 1/2/6 (non-lander creation) — exists to catch this before go-live.

## 8. Deployment

### 8.1 Forge matrix (narrowed from v7)

Prerequisites per row: `--atomic` push, `--force-with-lease` (both universal git wire features), wildcard branch protection with **creation** restriction (not just update restriction), and no more-permissive rule matching the same pattern.

| Forge | Supported config | Prerequisites / caveats |
|---|---|---|
| Bare-repo over SSH + pre-receive hook | **Full** | Ship reference hook alongside CLI. Hook enforces lander identity on `refs/heads/capsule-witness/**` create + `refs/heads/<base_ref>` update. This is the reference deployment. |
| Gitolite | **Full** | `refex` rules support both create and update restriction with full wildcard. Configure `capsule-witness/*` with `RW` only for the lander. |
| GitLab self-managed | **Full, with hook** | Protected-branches API restricts push (update) but GitLab's rule-precedence can be defeated by a broader wildcard. Install server-side `pre-receive` to enforce lander-identity on create. Admin review of all wildcard rules that overlap `capsule-witness/*` required before go-live. |
| GitLab.com SaaS | **Conditional** | Protected branches on SaaS restrict *push*, not *creation*, for patterns where no branch yet exists; GitLab applies the *most permissive matching rule*, so a broader `*` wildcard owned by another maintainer nullifies the restriction. Usable only if (a) the project has no broader `*` protected-branch rule, (b) a `capsule-witness/*` rule with "Allowed to push and merge: none except <lander>" is in place, and (c) §8.2 ACL tests pass for *creation*. Where feasible, prefer a self-managed GitLab instance with the pre-receive hook. |
| GitHub Enterprise Server | **Full, with org ruleset** | Use repository or org-level rulesets targeting `refs/heads/capsule-witness/**` with "Restrict creations" + "Restrict updates" + a bypass list containing only the lander **GitHub App**. Requires org-owned repo. PATs are **not** first-class ruleset bypass actors — use an App. |
| GitHub.com (org repo) | **Conditional** | Same ruleset config as GHES. Requires (a) the repo is owned by an organization, (b) the lander is installed as a GitHub App with `contents:write` scoped to the repo and listed as the sole bypass actor on the ruleset, and (c) rulesets configured to "Restrict creations" (commonly missed). Rulesets are available on public repos with GitHub Free (v9 correction); the org-ownership is what gates bypass-actor support, not the billing tier. |
| GitHub.com (personal repo) | **Conditional / unverified** | Personal accounts can install GitHub Apps and create repository rulesets; an App-backed lander with bypass on a `capsule-witness/**` ruleset is structurally possible. We have not run §8.2 ACL tests against a personal-account deployment to confirm "Restrict creations" semantics match org behavior. Deploy only after running `capsule deploy verify`. Org-owned repos remain the recommended target. |
| Gitea / Forgejo | **Conditional** | Branch protection restricts push but creation restriction depends on version. Require Gitea ≥ 1.21 / Forgejo ≥ 7 with "restrict pushes that create matching branches" enabled. Otherwise deploy with a server-side hook. |
| Bitbucket / Azure DevOps | **Unverified** | Not targeted for v0. Users wanting these must run §8.2 ACL tests and file results. |

### 8.2 ACL test suite (run before go-live)

A deployment is valid only if *all* tests pass against the configured remote, using three identities: `lander`, `worker` (claim authority on `refs/heads/capsules/**`), and `outsider` (push authority on some normal branch but no special capsule rights).

1. `outsider` attempts `git push <remote> <any_sha>:refs/heads/capsule-witness/probe/a1` — must be rejected.
2. `worker` attempts the same — must be rejected.
3. `lander` executes a real land push (step 4 of §7.1.2) against a test capsule — must succeed.
4. **Witness OID fence (tightened in v10).** Split into two sub-tests:
   - 4a. `lander` re-runs the exact same land push from test 3 — must return `Everything up-to-date` exit 0 (idempotent same-OID re-push; confirms the push path supports crash retry without spurious failure).
   - 4b. Precondition: after test 3, advance `base_ref` to a known sha `B`. Choose `Y` such that `Y` is a **fast-forward from `B`** (so a rejection cannot be masquerading as a base_ref non-FF). Record `base_ref_before = B`. `lander` attempts `git push --atomic --force-with-lease=refs/heads/<witness_branch>: <remote> Y:refs/heads/<base_ref> Y:refs/heads/<witness_branch>` — the push must be rejected with `stale info` on the witness ref. Read `base_ref_after` and assert `base_ref_after == base_ref_before` (proves `--atomic` rolled back `base_ref` on witness rejection). Also assert the witness ref still points at `verified_sha` from test 3.
5. `outsider` attempts `git push <remote> +<any_sha>:refs/heads/<base_ref>` — must be rejected.
6. `outsider` attempts `git push <remote> <any_sha>:refs/heads/capsule-witness/**` via wildcard-style refspec — must be rejected (catches GitLab's most-permissive-rule failure mode).
7. Prune path: `lander` deletes a landed witness branch — must succeed (required for §8.4 pruning to work).
8. `outsider` deletes a witness branch — must be rejected.

Ship these as `capsule deploy verify` in the reference CLI. Refuse to run `land` in production until the suite is recorded as passing for the deployment.

### 8.3 Lander principal realization

- **Self-hosted SSH + hook:** a dedicated unix user + ssh key; hook enforces identity.
- **Gitolite:** a dedicated gitolite user with `RW` on the two namespaces.
- **GitLab self-managed / SaaS:** a service user (token) with Maintainer on the repo, listed as sole "allowed to push/merge" for the two rules; hook-enforced creation on self-managed.
- **GitHub.com org / GHES:** GitHub App installed on the repo, scoped to `contents:write`, listed as sole bypass identity on both rulesets. **PATs are not supported** — GitHub rulesets' bypass-actor list accepts roles, teams, Apps, and (via REST) deploy keys, not arbitrary PATs. Use an App.
- **Gitea/Forgejo:** dedicated service user with repository write and push-creation bypass.

Operate the lander as a single-writer service. Concurrent landers on the same capsule are safe (git's atomic push serializes them) but unnecessary and complicate audit.

### 8.4 Garbage collection

`capsule prune-branches` deletes attempt branches and witness branches for `landed` / `abandoned` attempts older than a configurable threshold. Witness branches for attempts with unresolved `pending_land` are preserved unconditionally.

**Caveats:**
- On GitHub.com, protected branches cannot be deleted via git push by default; the lander deletes via API call from its bypass-identity App. `capsule deploy verify` step 7 checks this.
- On GitLab, protected branches cannot be deleted by `git push` at all; the lander must call the protected-branches API to unprotect, then delete, then (optionally) re-apply protection — or simply leave witnesses in place and accept ref-count growth.
- Witness branches grow ref count linearly with landed attempts. At 10k+ landed attempts, `git ls-remote` latency and UI branch-list pagination degrade noticeably; plan for periodic pruning or archival to a separate refspace.

### 8.5 Observability side-effects

Witness-branch creation events appear as branch-created signals in downstream automation:

- **GitHub Actions** `push` triggers fire on witness refs unless `branches-ignore: ['capsule-witness/**']` is set in every workflow.
- **GitLab system hooks / pipelines** fire likewise; pipelines for `capsule-witness/*` should be disabled via `workflow: rules`.
- **PR/MR UIs** will surface witness branches in compare-ref selectors and branch autocomplete. Not a bug; a reason to keep `capsule-witness/` prefixed and well-known.

Every deployment should ship recommended CI config exclusions alongside the protection rules.

## 9. What a caller builds on top

- Scheduler picks from `capsule list --available`.
- Worker runtime claims, checks out `base_sha`, creates `branch`, pushes, heartbeats, calls `attest`, calls `land`; handles `base_ref_moved` by rebase + re-attest.
- Merge/squash policy: caller creates the merge/squash commit on the attempt branch pre-`attest`. v0 supports fast-forward land only.
- Intent source: humans, planners, reactive rules.
- Lease-ceiling / starvation / fairness policy: external to this primitive.

### 9.1 Agent-facing surface: the `capsule` skill (new in v12)

The OSS release ships a Claude Code **skill** (`/capsule`) alongside the binary. The skill is the agent-facing API; the CLI is the protocol.

Why a skill rather than CLAUDE.md prose:
- Loads on demand. Sessions in repos without `.capsule/` pay zero context cost.
- Single entry point for discovery. `/capsule` in a fresh session is how most users will first encounter the primitive.
- Carries the recovery matrix (every error code → action) so an agent can self-correct without operator intervention.

Skill responsibilities:
- Encode **the discipline** (claim → heartbeat → attest → land) as procedural instructions an agent follows turn-to-turn.
- Map every CLI error code to a recovery action (`scope_conflict`, `base_ref_moved`, `lease_expired`, `witness_oid_mismatch`, `pending_land`, etc.).
- Walk the user through `capsule init` + `capsule deploy verify` on first use.
- State explicitly when *not* to invoke (solo dev, no `.capsule/`).

Skill explicitly does NOT:
- Replace the design doc (the skill links to it for protocol-level questions).
- Make policy decisions the design defers to deployments (lease TTL, lease-ceiling, scope granularity heuristics beyond "narrowest covering prefix").

A reference implementation of the skill ships in the OSS repo at `skills/capsule/SKILL.md`.

### 9.2 Editor / hook integration (recommended)

Beyond the skill (which guides agent behavior), deployments may install a `PreToolUse` hook that refuses `Edit`/`Write` on paths not covered by an active capsule held by the current session. This converts "discipline" (skill-level) into "enforcement" (hook-level). The skill should mention the hook as an opt-in hardening step, not a prerequisite.

## 10. Workflow exclusions and open questions

### 10.1 Known-incompatible workflows (v0)

- **Hosted merge buttons** (GitHub "Merge pull request", GitLab "Merge"). Server synthesizes the final commit; capsule acceptance never ran on it. Workaround: capsules land to an integration branch; a separate process (PR + button) merges integration → main.
- **Merge queues.** Same reason — final commit synthesized at integration time. Same workaround.
- **Protected-branch PR-only flows** where direct pushes to `main` are forbidden. The lander cannot advance `base_ref`. Workaround: capsule-managed integration ref (e.g. `capsule-staging`) with external replication to `main`.
- **Remotes that don't advertise `atomic` protocol capability.** Only very old git servers (< 2.4) or some proxy setups. All current hosted forges support `atomic`.
- **Remotes where §8.2 ACL tests cannot all pass.** The witness trust collapses without create-restriction. Use a different deployment target or the bare-repo + hook reference deployment.
- **Shared wildcard-protected branches on GitLab SaaS.** If the project already has a broader protected-branch wildcard owned by other maintainers that matches `capsule-witness/*`, GitLab's most-permissive-rule precedence makes the stricter rule non-binding; redesign your protection scheme or move to self-managed.

### 10.2 Open

1. **Log ref URI schemes** (v0): `file://`, `s3://`, `gs://`, `http(s)://`. `null://` disallowed.
2. **First-class human acceptance** — v1 feature.
3. **Reference pre-receive hook** for bare-repo / self-hosted deployments, shipped alongside the reference implementation.
4. **Reconciler triggers** — currently: on every access of a capsule with `pending_land != null`, plus periodic sweep at `max(60s, lease_ttl/4)` (codified in §7.2). Open: whether to additionally reconcile on `list` of capsules whose most recent event was > N seconds ago, to bound freeze latency without access.
5. **Attempt retention** — unbounded until ~10k/capsule; see §8.4 pruning caveats.
6. **Multi-base-ref capsules.** Out of scope. A capsule binds to one `base_ref`.

## 11. Changes from prior revisions

- **Bounded freeze + operator escape hatch (v9→v10).** v9's freeze-under-`pending_land` was unbounded if the reconciler service was down and no access path fired. v10 adds explicit reconciler-liveness deployment requirement (§3.1) and `capsule force-unfreeze` operator-only escape hatch (§6, §7.1.2) — synchronously runs reconciler decision tree, emits audited incident event.
- **Single-valued land-failure cleanup (v9→v10).** v9's §6 pseudocode ("both failures leave `pending_land` for reconciler") contradicted §7.1.2 (synchronous clear). v10 unifies: all land-step failures clear `pending_land` synchronously in one DB transaction. Reconciler handles **only** crash-between-push-and-DB-commit.
- **Witness push outcome decomposed (v9→v10).** v9's "three witness cases" conflated witness-ref and base_ref outcomes. v10 splits push outcome into two independent axes (base_ref: advance/no-op/reject; witness: create/no-op/reject) with an empirical outcome matrix. `Everything up-to-date` is only the double-no-op subcase.
- **ACL test 4b tightened (v9→v10).** Precondition: `Y` is a fast-forward from current `base_ref` so rejection can only be witness-lease. Assert `base_ref` before and after.
- **PendingLand fence scope stated (v9→v10).** `PendingLand` fences attempt reclaim; it does **not** fence remote `base_ref` freshness (atomic push does). After `PendingLand` commit, session liveness is not the trust root.
- **GitHub personal-repo softened (v9→v10).** Moved from "Not supported" to "Conditional / unverified" — Apps + rulesets are structurally possible on personal accounts; requires §8.2 ACL-test confirmation.
- **Durable-proof lifecycle clarified (v9→v10).** Pre-prune: witness branch *and* `Landing`. Post-prune: `Landing` alone.
- **Honest witness semantics (v8→v9).** Empirically tested on git 2.39.5: `--force-with-lease=<ref>:` rejects different-OID pre-existing refs (atomic-fails the whole push; `base_ref` untouched) but silently accepts same-OID via `Everything up-to-date`. v8's "true create-only" claim was wrong. v9 reframes: name uniqueness via monotonic `a<N>` + creation-restriction branch protection; null-OID lease as runtime integrity check against different-OID corruption; same-OID idempotency for crash retry.
- **Stale-lander fence (v8→v9).** `pending_land != null` freezes reclaim — no lease-expiry-to-`planned` transition while set. The DB fence-write in `land` step 1 is both a precondition check and the reclaim freeze. Closes v8's race where a delayed lander could push after its attempt was reclaimed.
- **ACL test 4 rewritten (v8→v9).** Split into 4a (same-OID idempotency confirmation) and 4b (different-OID rejection + `base_ref`-rollback confirmation). The old test 4 was invalid — empirical testing showed it never fails.
- **GitHub narrowing (v8→v9).** PAT path removed from supported lander configurations on GitHub (rulesets' bypass-actor mechanism is Apps/roles/teams/deploy-keys; PATs are not first-class). GitHub Free public-repo over-pessimism corrected: rulesets are available; org-ownership gates bypass-actor support, not the billing tier.
- **Durable pre-push state (v7→v8).** `PendingLand` record, DB-committed before the git push. Reconciler populates `Landing` from `PendingLand` + remote witness query, without in-memory dependency.
- **Honest forge matrix (v7→v8):** §8.1 narrowed with per-forge prerequisites; §8.2 adds `capsule deploy verify` ACL test suite; §8.4 documents protected-branch deletion caveats; §8.5 documents CI side-effects.
- **Witness placement (v6→v7):** `refs/heads/capsule-witness/*` (protectable) replaces `refs/capsules/*` (unprotectable on SaaS).
- **Fast-forward rule (v6→v7):** "strict descendant" → "descendant or equal" (git's actual semantics). `Landing.advanced_base_ref` records no-op advances.
- **Earlier:** atomic multi-ref push with witness (v6), strict sha-equality landing (v5), lease retention through accepted (v4), add-dep atomicity (v4), verified_sha binding (v3), path-component canonicalization (v3), per-attempt fencing (v2), claim-time scope overlap (v2).

## 12. Reference: why this is not X

- **Not a task queue.** Serializes on write-set, not task id. Records per-attempt git identity, verification bound to sha, landing atomic with a git multi-ref update.
- **Not a workflow engine.** No DAG, no compensation.
- **Not a ticket tracker.** Events are audit log, not conversation.
- **Not a git host.** One remote operation beyond reads: the atomic multi-ref fast-forward + null-OID-leased-witness push at `land`.
- **Not an agent framework.** Doesn't run LLMs.
- **Not married to `git worktree`.** The protocol is an abstraction over *git refs + coordination DB*, not over working trees. (a) The cross-capsule mutex is over *path prefixes* (§7.0, §7.1.1 step 4), enforced in the store — `git worktree` doesn't enforce write-sets. (b) The lander is ref-only (§7.1.2); workers materialize working state, but making that native would force asymmetric protocol coupling. (c) Authority separation (§3.1/§3.2) keeps git authoritative for refs and the store authoritative for intent/ownership/verification; worktrees are neither distributed nor transactionally coordinated across hosts, so putting them in the protocol would force `reconcile` / `force-unfreeze` to reason about local fs state (orphan worktrees). (d) The §3.2 threat model is crash-safe and stale-actor-safe, not scope-drift-safe; intra-capsule scope containment (preventing a worker from committing outside its declared prefixes) is currently advisory post-`claim` — handle it via attest-time diff validation (§7.1.0 extension) or skill-layer discipline (sparse-checkout, per-capsule worktree), orthogonal to the worktree-at-protocol-layer question. (e) A CLI-layer opt-in (e.g. `capsule work --isolate=worktree`) is not foreclosed by this section — it composes `Store` + `git worktree add` without touching the protocol crates.

A concurrency primitive for code-change work: write-set mutex, per-attempt git fencing with a creation-protected branch-namespace witness, verification bound to a commit sha, landing atomic under git's native multi-ref push with a null-OID integrity lease. One primitive. Well-defined edges.
