# Capsule — Positioning Proposal v0.2

Status: implemented. Companion to `DESIGN.md` v12. **No protocol changes proposed.** All three items are CLI-layer or implementation work; protocol invariants (§3, §7, §8.2) are untouched. See §8 (Implementation status) at the end of this doc.

## 1. Why this doc

Capsule shipped a verified-atomic-land protocol on the bet that the right primitive for parallel coding agents is *upfront scope coordination*, not post-hoc conflict resolution. The market made the opposite bet. This doc surveys (within a bounded set; see §2.1) where other tools landed, names capsule's actual differentiation, and proposes three changes that sharpen it without touching the protocol.

## 2. Landscape (April 2026, surveyed set)

### 2.1 The dominant pattern in the surveyed set: worktree isolation + sequential merge

In the surveyed harnesses (Cursor 2.0, Claude Code agent teams, Augment, Composio agent-orchestrator, oh-my-codex, Hermes), the same shape appears:

- One agent → one git worktree → one branch.
- Agents do not coordinate during work. Conflicts are deferred to merge time and surfaced through standard git tooling.
- Orchestration is hierarchical: planner / worker / judge. Workers do not talk to each other.

Tools in this lane: agentree, git-worktree-runner (CodeRabbit), worktree-cli (with MCP for Claude Code), Cursor Worktrees feature, Composio agent-orchestrator. Survey is non-exhaustive; tools outside this set may diverge.

### 2.2 The abandoned pattern: equal-status agents with locks

Cursor's published post-mortem ([Scaling long-running autonomous coding](https://cursor.com/blog/scaling-agents), retrieved April 2026) describes the failure mode capsule was built to fix. Paraphrasing the post: equal-status agents with locks suffered throughput collapse — Cursor reports order-of-magnitude effective-throughput loss at high agent counts due to lock-holding, lock leaks, and brittle coordination-file races. Cursor tried optimistic concurrency next and then pivoted to hierarchical orchestration plus worktrees. (Specific numbers in the post are paraphrased here; consult the source for exact figures.)

### 2.3 Adjacent / partial analogs

- **Multi-Agent Coordination MCP for Cursor IDE** — closest by intent. Lighter: no verified-land protocol, no atomic git-wire push, no crash-recoverable lease.
- **Git LFS file locking** — same shape (advisory exclusive lock on paths). Wrong domain (binary assets, human users, no atomic-land semantics).
- **agentic-primitives** — tool-scope as a primitive. Orthogonal to path-scope.
- **Beads (gastownhall/beads)** — task-graph memory. Disjoint problem (what to do, not how to merge).

### 2.4 Where capsule sits in the surveyed set

Three properties appear unique to capsule within the surveyed tools (not "the industry"):

1. **Component-wise path-prefix scope lock** (`src/foo` overlaps `src/foo/bar.rs`, not `src/foobar`; DESIGN.md §7.0).
2. **Verified atomic land via `git push --atomic --force-with-lease` against a witness ref** — race-free without a central coordinator and without trusting the lock holder's local state (DESIGN.md §7.1.2).
3. **Crash-recoverable lease/session model** with reconcile from witness-ref state and `force_unfreeze` audit trail (DESIGN.md §3.3, §7.2).

The surveyed tools chose isolation-and-defer because it is simpler. Capsule's bet is that the verified-land protocol pays back its complexity at scope-overlap density worktree fleets cannot handle (lockfiles, generated code, schema migrations, cross-cutting refactors).

## 3. Proposals

Three changes. None modifies the protocol crates (`capsule-core`, `capsule-store`, `capsule-git`); all live in `capsule-cli` or are pure implementation against existing spec.

### 3.1 Thin worktree CLI shim (`capsule work --isolate=worktree`)

**Status: implemented** (`crates/capsule-cli/src/worktree.rs`).

**Proposal.** Add a CLI-only convenience wrapper, exactly as DESIGN.md §12(e) permits:

> A CLI-layer opt-in (e.g. `capsule work --isolate=worktree`) is not foreclosed by this section — it composes `Store` + `git worktree add` without touching the protocol crates.

`capsule work --isolate=worktree <id> --session <s>` calls `Store::claim` (which returns the new `Attempt` record including `branch = "capsules/<id>/a<N>"` and `base_sha`), then `git worktree add -b capsules/<id>/a<N> <path> <base_sha>` to create the attempt branch off the recorded base sha. The witness ref `capsule-witness/<id>/a<N>` remains lander-only per §3.1/§3.2 trust model. Default `<path>` is `<capsule_dir>/worktrees/<id>-a<N>` — `a<N>`-suffixed so a re-claim allocates a fresh path rather than colliding with a stale worktree from a prior attempt. The shim does not auto-push the empty branch — first push happens when the worker pushes work per §7.1.1 follow-up.

**Collision recovery.** Setup is serialized by `<capsule_dir>/locks/worktree-setup-<id>.lock`; runtime is guarded by `<capsule_dir>/locks/worktree-run-<id>-a<N>.lock`. The setup logic enumerates the four `(branch_exists, worktree_registered, dir_exists)` quadrants and either reuses or fails with a remediation hint — including the case where a prior partial run left an unregistered directory or a registered path now missing on disk.

**Heartbeats stay a worker-process responsibility** per DESIGN.md §3.3 (`lease_ttl/3` time-driven cadence, miss-three semantics). No git hook gimmicks — `post-commit` is event-driven and would let the lease expire during long edits or acceptance runs. The skill (`skills/capsule/SKILL.md`) already encodes this discipline.

**Why.** Most users in the surveyed set already think in worktrees. A one-command shim makes capsule's claim/attest/land discipline trivially compatible with that mental model, without coupling the protocol to local fs state (which §12 explicitly warns against — orphan worktrees, reconcile/force-unfreeze reasoning about local state).

**Cost.** Small. Pure CLI shell-out. No core/store/git crate changes.

**Risk.** Low. The shim is opt-in; existing workflows unaffected. Worktree
cleanup is explicit via `capsule cleanup-worktrees`; it only targets Capsule's
default worktree paths for terminal attempts and lets `git worktree remove`
refuse dirty worktrees unless the caller passes `--force`.

**Non-goal.** Not "reframing capsule on top of worktrees." The protocol remains a coordination primitive over refs + DB; the worktree shim is convenience only.

### 3.2 Default-scope config for serialized files

**Status: implemented** (`crates/capsule-cli/src/config.rs` and `crates/capsule-cli/src/serialize_lint.rs`; lint wired into `Cmd::Attest` at `main.rs:run_serialize_lint`).

**Proposal.** Ship a `.capsule/config.toml` default (overridable per repo) that lists ecosystem files which any capsule touching them MUST include in `--scope`:

```toml
[serialize_paths]
# If a capsule's diff touches these paths, --scope must cover them.
# Existing §7.0 component-wise overlap then serializes such capsules.
required = [
  "Cargo.lock",
  "package-lock.json",
  "pnpm-lock.yaml",
  "yarn.lock",
  "go.sum",
  "uv.lock",
]
```

Enforcement is **CLI-layer only**, not a `Store::attest` precondition. DESIGN.md §7.1.0 specifies attest as a single DB transaction with no git effect; adding diff inspection there would be a §7.1.0 extension (per §12(c)) and a `capsule-git` capability change. Two compatible enforcement points instead:

- **CLI lint at attest time.** `capsule attest` (the CLI command, not `Store::attest`) computes the diff `attempt.base_sha..verified_sha` via local `git diff --name-only -z` from `--repo-dir` (default cwd), compares against `serialize_paths.required`, and refuses to invoke `Store::attest` if any listed path is touched and uncovered by `scope_prefixes`. Prints `serialize_path_uncovered` and exits non-zero. Fails closed if `verified_sha` is not in the local object DB (the lint cannot make a sound decision against an unknown commit). Bypassable for tests/break-glass via `--skip-serialize-lint`. No store error, no protocol change.
- **PreToolUse hook extension (DESIGN.md §9.2).** The hook today refuses `Edit`/`Write` on out-of-scope paths; extending it to also flag any single touched path matching `serialize_paths.required` not covered by the active capsule's scope is straightforward (the predicate lives in the same lint module). Implementation deferred — agent-runtime-specific.

Both are bypassable by direct `Store::attest` calls — that's intentional; the protocol's correctness still rests on path-prefix overlap, and any two capsules that both declare `--scope Cargo.lock` are still serialized correctly via §7.1.1 step 4. The default config + lint just makes the right discipline the default.

**Entry semantics.** Every `serialize_paths.required` entry is parsed as a `CanonicalPath` (DESIGN §7.0): POSIX, case-sensitive, NFC-normalized, no `..`, no absolute paths. Trailing slashes are stripped (`db/migrations/` ≡ `db/migrations`). Overlap is component-wise prefix — `Cargo.lock` matches exactly `Cargo.lock`; `db/migrations` matches any path under `db/migrations/`. No glob, no basename, no regex. Duplicate canonical entries dedup automatically.

**Critically: no new scope kind, no DESIGN.md change, no protocol-crate change.** Existing `CanonicalPath` + §7.1.1 step 4 component-wise overlap already serializes any two capsules that both declare `--scope Cargo.lock`. The only thing missing is enforcement that capsules touching these files actually declare them — discipline, not protocol.

**Why.** Surveyed sources flagged lockfile / migration merges as the parallel-work killer ([Augment: Multi-Agent Coding Workflow](https://www.augmentcode.com/guides/git-worktrees-parallel-ai-agent-execution), retrieved April 2026 — paraphrased: parallel agents adding different versions of the same dep require re-running the lockfile generator on merge, no manual resolution). Worktree-only fleets solve this by funneling deps through a single coordinator worker (manual discipline) or freezing deps for the parallel batch (loses agility). Capsule's existing scope mechanism solves it correctly — but only if users remember to declare the scope. Default config + attest-time enforcement closes that gap.

**Cost.** Small. Default config + CLI-layer diff lint + optional PreToolUse hook extension. No store error variant, no schema migration, no `Scope` sum type, no `scope_json` shape change, no `capsule-git` API addition.

**Risk.** Low. Backwards-compatible. Repos may extend the list (e.g., add `db/migrations/` for stricter ordering) or empty it. Migrations directories are a separate question — most teams want ordering, not exclusion; if they want exclusion they declare `--scope db/migrations/`.

### 3.3 Implement DESIGN.md §8.2 (`capsule deploy-verify`)

**Status: implemented** (`crates/capsule-cli/src/deploy_verify.rs`; gate enforced in `Store::land` via `enforce_deploy_verify_gate`).

DESIGN.md §8.2 fully specifies an 8-test ACL/branch-protection suite (tests 1, 2, 3, 4a, 4b, 5, 6, 7, 8) using three identities (lander, worker, outsider) against the configured remote. CLAUDE.md confirms this was the only spec'd item that previously `bail!`d. The work was wiring the harness to `capsule deploy-verify` and shipping it as the deployment go-live gate per §8.2 mandate ("Refuse to run `land` in production until the suite is recorded as passing").

**Two modes.**

- **`--hermetic` (default).** Spins up an in-process tempdir bare repo with the reference `pre-receive.sh` hook (`skills/capsule/pre-receive.sh`) and three sibling clones configured with `push.pushOption=identity=<role>`. Useful for development self-test of the *reference hook*. Does NOT exercise real-forge ACL behavior — test 6 (wildcard refspec) is `skip`ped because the reference hook denies on ref *pattern*, not refspec *form*; the wildcard semantics only matter at the forge ACL layer (e.g. GitLab's most-permissive-rule failure mode).
- **`--remote <name> --lander-url --worker-url --outsider-url --remote-allow-mutations`.** Production gate. Runs the same suite against three real per-identity URLs on a forge. Destructive: tests 3, 4b, 7, 8 mutate refs. The witness ref name is uuid-suffixed per run (`capsule-witness/deploy-verify-<uuid>/a{1,2}`) so concurrent runs and real in-flight capsules cannot collide; test 8's recreated witness is best-effort cleaned up after the suite. The `--remote-allow-mutations` flag is required to acknowledge that this mutates the configured remote — run against a deploy-validation environment, not production base_ref.

**Test 4b OID fence.** §8.2 requires asserting BOTH `base_ref_after == base_ref_before` AND that the witness ref still points at `verified_sha` from test 3. The implementation captures the witness OID after test 3 passes and re-checks it after the rejected push, so a regression that drops the OID fence cannot pass the suite.

**Land gate.** A successful run records a single row in the `deploy_verify_pass` table (schema v2, `deploy_verify_pass(id PK CHECK(id=1), at, mode, base_ref)` — single-row by construction). `Store::land` enforces presence of this row unless `LandRequest::skip_deploy_verify_gate` is set; bypassed via `capsule land --skip-deploy-verify-gate` for tests / break-glass / development.

**Important scope distinction.** §8.2 is an *ACL/branch-protection* suite — every test exercises who can push/create/delete which refs against a real remote. It is not a protocol fault-injection suite. Adversarial schedules (concurrent landers, base_ref races, witness OID mismatches under load, crashes between push and DB commit) are valuable but belong in `cargo test -p capsule-store` against tempdir bare repos, not in `capsule deploy-verify` (which assumes a configured remote with three identities). These are separate work items; this proposal covers only §8.2.

**Why.** §8.2 is the deployment gate by design; until it ships, the README cannot honestly claim "verified atomic land is provably race-free in your deployment." Cheapest unique-defense move available.

**Cost.** Medium. Three test identities to provision against the test remote; all 8 tests' preconditions/assertions are already written in §8.2.

**Risk.** Low engineering risk. Some risk that test 4b (different-OID witness rejection with base_ref-rollback assertion) or tests 1/2/6 (non-lander creation) uncover a real misconfiguration in a target deployment — which is the point.

**Optional follow-up (separate proposal).** A protocol fault-injection harness as `cargo test -p capsule-store --test fault_injection`, exercising concurrent landers, base_ref races during step 2→step 3, crashes between steps. Not part of the deploy gate. Out of scope here.

## 4. Order

1. **3.3 first.** Implementation against an existing spec; ships the deploy gate.
2. **3.2 second.** Default config + attest-time check; small surface, addresses a named industry pain.
3. **3.1 third.** Pure ergonomic shim; worth doing only after 3.2/3.3 land so the discipline it wraps is fully shipped.

## 5. Non-goals

- Not changing the four-step land happy path or the five-step land operation including failure cleanup (DESIGN.md §6, §7.1.2).
- Not adding orchestration / planning / judging — that's beads/agent-orchestrator territory.
- Not chasing libgit2 — `git` CLI shell-out is load-bearing for `--atomic` + `--force-with-lease` semantics (CLAUDE.md, capsule-git crate doc).
- Not loosening `id::validate` or `CanonicalPath` invariants.
- Not introducing a new `Scope` sum type or modifying `scope_json` shape.
- Not coupling the protocol to worktree state (per DESIGN.md §12).

## 6. Open questions

- ~~Default `serialize_paths.required` list: ship empty or preloaded?~~ **Resolved: preloaded.** `DEFAULT_REQUIRED` ships `Cargo.lock`, `package-lock.json`, `pnpm-lock.yaml`, `yarn.lock`, `go.sum`, `uv.lock`. Repos that disagree set `[serialize_paths] required = []` (or a custom list) in the capsule store config (`.capsule/config.toml` by default).
- Worktree pruning hook: `capsule cleanup-worktrees` now covers explicit
  cleanup of default terminal worktrees. Automatic cleanup in `abandon` or
  successful `land` remains intentionally avoided because it can surprise users
  with uncommitted work in the worktree dir; callers can opt into
  `cleanup-worktrees --force` when they have made that decision.

## 7. Decided in DESIGN.md (not open)

The following questions appeared in earlier proposal drafts but are settled by DESIGN.md and removed from §6:

- **Worktree branch identity.** §3.2 trust model: workers push `capsules/<id>/a<N>`, lander creates `capsule-witness/<id>/a<N>`. Merging the two would require workers to have push authority on the witness namespace, breaking the trust separation.
- **`capsule deploy-verify` entry point.** §8.2 specifies it as a CLI command that gates `land`, exercising real-remote ACLs with three identities. A `cargo test` entry point cannot replace it — tempdir bare repos cannot test remote branch-protection ACLs (tests 1, 2, 5, 6, 8). Different harnesses, different scopes.

## 8. Implementation status

| Item | Status | Locus |
|---|---|---|
| §3.3 deploy verify (hermetic) | ✓ shipped | `capsule-cli/src/deploy_verify.rs::Bootstrap::hermetic` |
| §3.3 deploy verify (remote) | ✓ shipped | `capsule-cli/src/deploy_verify.rs::Bootstrap::remote` |
| §3.3 land gate enforcement | ✓ shipped | `capsule-store/src/lib.rs::enforce_deploy_verify_gate` (schema v2 `deploy_verify_pass`) |
| §3.2 default config + attest lint | ✓ shipped | `capsule-cli/src/config.rs`, `capsule-cli/src/serialize_lint.rs`, wired in `main.rs::run_serialize_lint` |
| §3.2 PreToolUse hook extension | deferred | predicate (`lint_paths`) ready; integration is agent-runtime-specific |
| §3.1 worktree shim | ✓ shipped | `capsule-cli/src/worktree.rs` (default path `<capsule_dir>/worktrees/<id>-a<N>`) |
| §3.1 worktree cleanup | ✓ shipped | explicit `capsule cleanup-worktrees`; auto-prune on land/abandon intentionally not default |
| §3.1 worktree auto-push on first commit | not planned | discipline: skill (`skills/capsule/SKILL.md`) instructs the worker to push before `capsule attest`; `Cmd::Attest` lint fails closed if `verified_sha` is not in the local object DB |

### Test coverage

- `cargo test -p capsule-cli --test deploy_verify` — `hermetic_acl_suite_passes` (8/9 tests pass, test 6 skip), `land_gate_blocks_without_recorded_pass`.
- `cargo test -p capsule-cli serialize_lint` — config load (defaults / user override / disable / invalid path / dedup), lint predicate (covered, uncovered, directory prefix, adjacent-file false-positive, unrelated path).
- `cargo test -p capsule-cli worktree` — porcelain parser regressions, override traversal canonicalization.
- `cargo test -p capsule-store` — full state-machine + `record_deploy_verify_pass` / `enforce_deploy_verify_gate` coverage.
