# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Capsule is a coordination primitive for parallel agents on a shared git repo: a path-prefix lock + verified atomic land. `DESIGN.md` (v12) is the authoritative spec — it is unusually load-bearing. Before changing protocol behavior, data model, or git-wire semantics, read the relevant §. Code comments already cite § numbers; follow them.

Status: **spec complete, implementation in progress.** The reference CLI, embedded SQLite store, and git-wire integration exist; `capsule deploy verify` (ACL test suite, DESIGN §8.2) is unimplemented and currently `bail!`s.

## Workspace layout

Rust workspace, four crates with strict layering — do not add upward deps.

- `capsule-core` — pure types + protocol logic. **No I/O, no DB, no git.** Owns `Capsule`, `Attempt`, `Lease`, `Verification`, `PendingLand`, `Landing`, status/outcome enums; `CanonicalPath` (NFC, component-wise prefix overlap, §7.0); `id::validate` (ref-safe capsule ids).
- `capsule-git` — git wire. Shells out to the `git` CLI (not libgit2) so that `--atomic` + `--force-with-lease` semantics match the canonical client exactly. Public surface: `ls_remote_branch`, `land_push`, `LandOutcome`.
- `capsule-store` — SQLite-backed aggregate root for capsule state. Depends on `capsule-core` and `capsule-git`. `Store` owns all state transitions: `create_capsule`, `claim`, `heartbeat`, `attest`, `land`, `abandon`, `reclaim`, `add_dep`/`remove_dep`, `list_capsules`, `reconcile`, `force_unfreeze`, `get_capsule`. Test suite lives inline in `lib.rs` with real bare repos under `tempfile`.
- `capsule-cli` — the `capsule` binary. Thin clap wrapper over `Store`; commands translate 1:1 to `Store` methods. `--json` for machine output; default store dir is `.capsule/` (override with `--dir` / `CAPSULE_DIR`).

`skills/capsule/SKILL.md` is the agent-facing surface (Claude Code skill) — the CLI is the protocol, the skill is the discipline wrapper.

## Commands

```bash
cargo build                              # build workspace
cargo test                               # all tests (store tests spin up bare git repos via tempfile)
cargo test -p capsule-store              # just store tests (the bulk of behavior coverage)
cargo test -p capsule-store reconcile_   # a single test or name-prefix
cargo clippy --all-targets -- -D warnings
cargo fmt --all

# Run the CLI against a scratch store:
cargo run -p capsule-cli -- --dir /tmp/cap init
cargo run -p capsule-cli -- --dir /tmp/cap list --json
```

Store tests shell out to real `git`, so `git` must be on PATH. They're hermetic (tempdirs) but not parallel-safe against a shared cwd — use `cargo test` normally, don't force single-threaded unless debugging.

## Architecture — the load-bearing parts

### Store is the single writer, SQLite is the log
`capsule-store::Store` is the only thing that mutates capsule state. Every state transition is one DB transaction; crash-safety comes from the transaction boundary. The schema (v1, `crates/capsule-store/src/schema.rs`) denormalizes the aggregate: `capsule` row carries `acceptance_json`, `scope_json`, `verification_json`, `pending_land_json`, `landing_json` inline; `attempt` rows are normalized; `event` is the append-only audit log. Migrations are linearly numbered — append a new entry to `MIGRATIONS`; `SCHEMA_VERSION` is derived from `MIGRATIONS.len()` and tracks automatically.

WAL + `foreign_keys=ON` + `synchronous=NORMAL` are set in `Store::open`.

### The land protocol is the crown jewel (DESIGN §7.1.2)
`Store::land` is a four-step dance — do not reorder, do not fold steps together:

1. **Read remote `base_ref` tip** outside any DB tx (via `ls_remote_branch`).
2. **Write `PendingLand`** in one DB tx, under preconditions (status=accepted, lease live & session matches, no existing pending_land, verified_sha re-bound in-tx).
3. **Atomic multi-ref push**, no DB. Uses `git push --atomic --force-with-lease=refs/heads/<witness>:` with empty lease expect (= null OID). Same-OID is a no-op (idempotent retry); different-OID is atomic-rejected (protection leak).
4. **Reconcile from push outcome** in one DB tx → `Landed` / `BaseRefMoved` / `WitnessOidMismatch` / other.

Between step 2 and step 4, `pending_land != null` freezes reclaim (§7.2). A crash anywhere is recoverable by `Store::reconcile` (re-reads the witness ref and replays the decision tree) or, for a stuck lander, by `force_unfreeze` (requires `--lander-confirmed-dead`, audit-trailed).

### Lease / session rules (DESIGN §3.3)
- `session_id` is 1:1 with an `Attempt`. A new attempt requires a new session.
- Lease is held from claim through land. Same session must `attest` and `land`. Cross-session ops return `StoreError::CrossSession`.
- `lease_ttl_sec` is set at claim and immutable; heartbeat extends `expires_at = now + ttl_sec` but cannot change the TTL.
- Expired leases are reclaimed lazily on the next read path (`reclaim_expired_in_tx` runs in `list_capsules`, `claim`, etc.) — but **skipped** if `pending_land != null`.

### Canonical paths & scope conflict (DESIGN §7.0)
`CanonicalPath::new` enforces POSIX, case-sensitive, NFC-normalized, no `..`, no absolute paths. `overlaps` is **component-wise** prefix — `src/foo` overlaps `src/foo/bar.rs` but not `src/foobar`. Scope conflict check at `claim` iterates in-flight capsules (status ∈ {active, accepted}) and returns `StoreError::ScopeConflict` on any overlap.

### Capsule ids flow into git refs
`id::validate` is conservative (ASCII alnum + `-_.`, no edge dots, no `..`, ≤128 bytes) because ids end up in `refs/heads/capsules/<id>/a<N>` and `refs/heads/capsule-witness/<id>/a<N>`. Don't loosen without updating the ref-protection model in DESIGN §3.1.

## Conventions

- Errors go through `thiserror`; the public `StoreError` enum is exhaustive — add a variant rather than stuffing context into `anyhow`.
- Time is `time::OffsetDateTime` everywhere, serialized as ISO-8601 via `time::serde::iso8601`. Use the crate's own formatters, don't hand-format.
- JSON columns in SQLite store `serde_json`-encoded values of the core types; round-trip is load-bearing. If you add a field to a core type, check the migration story.
- `DESIGN.md` § cross-references in code comments are intentional. When adding non-trivial logic, cite the §.

## Skill & hook surface (DESIGN §9)

The OSS release ships `skills/capsule/SKILL.md` as the primary agent API — it encodes the claim/heartbeat/attest/land discipline and the recovery matrix. A future `PreToolUse` hook is planned to hard-enforce the discipline.

## Response style

Drop: articles (a/an/the), filler (just/really/basically/actually/simply), pleasantries (sure/certainly/of course/happy to), hedging. Fragments OK. Short synonyms (big not extensive, fix not "implement a solution for"). Technical terms exact. Code blocks unchanged. Errors quoted exact.

Abbreviate (DB/auth/config/req/res/fn/impl), strip conjunctions, arrows for causality (X → Y), one word when one word enough.

Pattern: [thing] [action] [reason]. [next step].

Not: "Sure! I'd be happy to help you with that. The issue you're experiencing is likely caused by..." Yes: "Bug in auth middleware. Token expiry check use < not <=. Fix:"
