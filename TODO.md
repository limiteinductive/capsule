# TODO — what's left for capsule to be "done"

Authoritative status of the implementation against `DESIGN.md` (v12). Items
are grouped by what blocks the OSS v0 release vs. what's deferred to v1+.

## v0 release blockers

### 1. `capsule deploy-verify --remote` (DESIGN §8.2)

Hermetic mode passes the eight-test ACL suite end-to-end. Remote mode is
parsed and validated by clap but `run` returns `bail!("deploy verify --remote
is not yet implemented")` (the bail string predates the clap subcommand
rename to `deploy-verify`) (`crates/capsule-cli/src/deploy_verify.rs:76`).

What's needed:

- Wire `Mode::Remote { remote, lander_url, worker_url, outsider_url }` through
  to the eight test functions. Today they take a `&Bootstrap` whose three
  sibling clones are tempdir-local; remote mode needs an analogous bootstrap
  that clones from `remote` three times and configures each clone's
  `remote.origin.pushurl` to the per-identity URL.
- Reuse the test bodies unchanged. They already shell out via `git push -o
  identity=<role>`; under remote mode `pushurl` carries the identity instead
  of the push-option. Decide which axis is authoritative on remote forges
  (probably URL; push-option is an artifact of the hermetic hook).
- Tests 3, 4b, 7, 8 mutate real refs (test 8 first runs a lander create of
  `capsule-witness/probe/a2`, then verifies an outsider delete is rejected,
  leaving the witness ref behind). Document the destructive behavior in
  the CLI `--help` and `skills/capsule/SKILL.md`, and decide whether
  remote mode should auto-clean the leftover witness ref or require
  operator cleanup.
- Add a smoke test against at least one real forge before tagging v0. GitHub
  with rulesets + bypass actors is the canonical target (DESIGN §8.1).

### 2. CI

No `.github/` exists. Minimum to ship:

- Workflow that runs `cargo build --workspace`, `cargo test --workspace`,
  `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all -- --check`
  on push + PR. Pin to `ubuntu-latest` and `macos-latest`; `git` is on the
  PATH on both.
- `capsule deploy-verify --hermetic --base-ref main` as a separate job. The store tests
  already spin up bare repos under `tempfile`; deploy-verify is a heavier
  end-to-end check and should run on every PR.
- Cache `~/.cargo` and `target/` keyed on `Cargo.lock`.

### 3. Release artifacts

`Cargo.toml` is at `0.0.1`. To call it shipped:

- Bump to `0.1.0` once §1 and §2 land.
- `cargo-dist` (or hand-rolled `release.yml`) producing prebuilt
  `capsule` binaries for `x86_64-unknown-linux-gnu`,
  `aarch64-apple-darwin`, `x86_64-apple-darwin`. Windows is out of scope
  for v0 (the deployment story assumes POSIX shells in the pre-receive
  hook).
- `cargo publish` in dependency order: `capsule-core` → `capsule-git`
  → `capsule-store` → `capsule-cli` (`capsule-store` depends on both
  `capsule-core` and `capsule-git`; `capsule-cli` depends on all three). Add `description`, `keywords`,
  `categories`, `readme` to each crate's `Cargo.toml` first.
- Tag releases as `v0.1.0` etc. Attach the prebuilt binaries.

### 4a. MSRV vs lockfile drift

`Cargo.toml` declares `rust-version = "1.75"`, but the current
`Cargo.lock` resolves to `clap 4.6.1` and `uuid 1.23.1`, both of which
require Rust 1.85+. Pre-release tasks: either pin `clap` / `uuid` to
1.75-compatible versions, or bump the workspace `rust-version` to
match reality. CI from §2 should run on the declared MSRV so this
drift gets caught next time.

### 4. README hygiene

Done in this pass: status block now reflects reality, install + quickstart
added. Remaining:

- `LICENSE` is MIT, called out in the header. OK.
- Add a "Try it" section once binaries exist (currently
  `cargo install --path crates/capsule-cli` is the only path).
- Link to `DESIGN.md` and `skills/capsule/SKILL.md` from the top fold.
  Done.

### 5. `CHANGELOG.md`

Empty repo. Start one before tagging v0; `DESIGN.md` §11 is a usable seed
for the design-side history.

### 6. `CONTRIBUTING.md`

Repo is currently single-author. Even a 20-line file describing
"`cargo fmt`, `cargo clippy -D warnings`, store tests must pass, cite
`DESIGN.md` § in non-trivial commits" is enough to set the bar.

## Spec-side open questions (DESIGN §10.2)

These do NOT block v0 if scoped out explicitly in the README. They DO
block v1.

### 7. Log ref URI schemes (DESIGN §10.2.1)

v0 currently has no log-ref export. Decision needed:

- Which of `file://`, `s3://`, `gs://`, `http(s)://` ship in v0?
- Implementation lives where? `capsule-store` has the audit log
  (`event` table, append-only). Export is a new crate or a CLI
  subcommand (`capsule log export --to <uri>`).
- `null://` is explicitly disallowed per §10.2.1.

### 8. First-class human acceptance (DESIGN §10.2.2)

Marked v1 in the design. Today the `acceptance` is a single `command`
the worker runs. Human acceptance means a different verification
shape (signature? approval record? both bound to `verified_sha`?).
Out of scope for v0.

### 9. Reconciler trigger sweep (DESIGN §10.2.4)

v0 reconciler liveness is weaker than DESIGN §7.2 prescribes. Today:

- The witness-ref reconciler (`Store::reconcile`, recovers
  `pending_land != null` from a crash between push and DB commit) is
  **only** triggered by an explicit `capsule reconcile <id>` invocation.
- `list_capsules` runs `reclaim_expired_in_tx` for lease expiry but does
  **not** call the witness-ref reconciler. `get_capsule` (the path
  behind `capsule status <id>`) opens only a `snapshot_read_tx` and
  runs neither — so `status` can return a stale active/accepted lease
  that should already have been reclaimed. A frozen capsule whose
  lander died is stuck until someone runs `reconcile` (or
  `force-unfreeze`).
- The periodic sweep at `max(60s, lease_ttl/4)` from §7.2 is **not
  implemented**. There is no daemon, no in-process timer, nothing.

What's needed for v0 (or stated as a known gap if deferred):

- An access-path trigger: when `get_capsule` / `list_capsules` observe
  `pending_land != null`, call `reconcile_inner`. Cheap, bounds freeze
  latency to the next access. Same patch should add
  `reclaim_expired_in_tx` to `get_capsule` so `status` agrees with
  `list`.
- A periodic sweep loop, either as a `capsule reconcile-sweep --daemon`
  subcommand or an in-process tokio task spawned by long-running
  consumers.
- Pick one of {access-trigger only, daemon only, both}. Document the
  liveness guarantee that ships.

### 10. Attempt retention (DESIGN §10.2.5)

Unbounded until ~10k attempts/capsule per §8.4. No pruning logic
exists. Add `capsule prune --capsule <id> [--keep N]` before users hit
the wall.

## Skill / hook surface (DESIGN §9)

### 11. `PreToolUse` hook

`CLAUDE.md` calls this out as planned. The skill (`skills/capsule/SKILL.md`)
encodes the discipline; a `PreToolUse` hook would hard-enforce it by
rejecting Edit/Write tool calls that touch paths outside any active
attempt's scope.

What's needed:

- Hook script that reads `.capsule/state.db` (read-only), resolves the
  current `CAPSULE_SESSION`, looks up the active attempt's scope, and
  rejects writes outside it.
- Decide: shipped in `skills/capsule/` next to the skill, or in a
  separate `hooks/` dir? The README and `init` flow should mention it.
- `capsule init` could optionally drop a `settings.local.json` snippet
  wiring the hook into Claude Code.

### 12. Skill smoke test

Today `skills/capsule/SKILL.md` is prose. No mechanical check that the
commands it documents match what the CLI actually accepts. A small test
that parses fenced bash blocks from the skill and runs `--help` for each
command would catch drift.

## Nice-to-have, not blocking

- `capsule status --json` cross-check tests (the JSON output is
  exercised only inline; no separate integration test asserts the
  schema).
- Coloured human-readable `list` output. Today `--json` is the rich
  surface and the default human output is terse.
- A `capsule doctor` command that runs `deploy-verify --hermetic`,
  checks `.capsule/state.db` integrity (`PRAGMA integrity_check`), and
  reports the schema version vs. binary's `SCHEMA_VERSION`.
- `metrics` / OpenTelemetry export from `Store`. Not on the critical
  path but useful once capsule is running in real fleets.
