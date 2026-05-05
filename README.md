# capsule

A path-prefix lock + verified atomic land for parallel agents on a shared git repo.

**Status:** spec complete (`DESIGN.md` v12), reference implementation
working end-to-end, OSS release in progress. See [`TODO.md`](TODO.md) for
the remaining work.

## What it is

When multiple autonomous agents (Claude Code sessions, CI workers, scripts) modify a shared codebase, they stomp each other. A capsule is a crash-safe critical section over declared path prefixes: one attempt at a time may modify those paths, an acceptance command produces a verification bound to a specific commit sha, and landing is an atomic multi-ref git push that — in the same transaction — writes a witness branch proving the land happened.

Pair-symmetric with a git commit:

- **Commit** = receipt: "here is what I did."
- **Capsule** = reservation: "I'm taking these paths, here is the pass criterion."

## Why

Existing tools cover subsets and miss the whole:

| Tool | Concurrency on paths | Crash recovery on git | Verification bound to sha | Hard ordering |
|------|---|---|---|---|
| Task queues (Celery, SQS) | task rows only | visibility timeout only | — | — |
| Workflow engines (Temporal, Airflow) | — | partial | — | yes |
| Git alone | — | — | — | — |
| **capsule** | yes | yes | yes | yes |

## Read the spec

- [`DESIGN.md`](DESIGN.md) — the full pure-design spec (data model, protocols, forge matrix, ACL test suite, threat model)
- [`skills/capsule/SKILL.md`](skills/capsule/SKILL.md) — agent-facing skill (Claude Code)
- [`TODO.md`](TODO.md) — what's left for v0

## Install

No prebuilt binaries yet (see [`TODO.md`](TODO.md) §3). Build from source:

```bash
git clone https://github.com/limiteinductive/capsule
cd capsule
cargo install --path crates/capsule-cli
```

Requires Rust 1.85+ and `git` on `PATH`. The workspace declares
`rust-version = "1.75"` (`Cargo.toml`), but the current `Cargo.lock`
resolves dependencies (e.g. `clap 4.6.1`, `uuid 1.23.1`) whose own
package metadata requires Rust 1.85. Either install on 1.85+ or pin
older dep versions in `Cargo.lock` before building.

## Quickstart

```bash
# Initialize a capsule store at the repo root.
capsule init

# Plan a capsule covering the paths you intend to touch.
capsule create \
  --title "rewrite users API" \
  --scope src/api/users \
  --acceptance-cmd "cargo test -p api" \
  --base-ref main

# List planned capsules and pick one.
capsule list --json

# Claim it for your session.
SID=$(uuidgen)
capsule claim <id> --owner alice --session "$SID" --base-sha "$(git rev-parse main)"
export CAPSULE_SESSION="$SID"

# Run the worker under heartbeat. `--isolate=worktree` materializes a fresh
# git worktree on the attempt branch (`capsules/<id>/a<N>`) returned by
# `claim` and chdirs the child into it. Without `--isolate`, you'd have to
# `git checkout -b capsules/<id>/a1 <base_sha>` yourself. The trailing
# command runs your edits + the acceptance check; heartbeats keep the lease
# alive throughout.
capsule work <id> --isolate=worktree -- bash -c '
  edit-edit-edit
  git commit -am "rewrite users"
  cargo test -p api
'

# Push the attempt branch so `verified_sha` is reachable from the remote
# before `land`. Skip only if the lander invokes from this same checkout —
# `land` only requires the sha in its local object DB.
git push origin "capsules/<id>/a1"

# Attest verification against the commit sha that passed the acceptance
# command. `--exit-code` accepts an integer or a sentinel ("timeout",
# "killed:SIGKILL"); `--log-ref` is a write-once / content-addressed URI
# (DESIGN §10.2.1). `--command` MUST match what you actually ran above.
capsule attest <id> \
  --verified-sha "$(git rev-parse refs/heads/capsules/<id>/a1)" \
  --command "cargo test -p api" \
  --exit-code 0 \
  --duration-ms 1234 \
  --log-ref "file:///tmp/capsule-logs/<id>.txt"

# Verify the deployment ACL contract (DESIGN §8.2). Records a pass that the
# `land` gate consumes. Hermetic mode validates the reference pre-receive
# hook, not your actual forge — for production, run `--remote <url>` once it
# ships (TODO.md §1) or pass `--skip-deploy-verify-gate` to `land` for
# local/demo flows.
capsule deploy-verify --hermetic --base-ref main

# Land — atomic multi-ref push that fast-forwards base_ref AND writes the
# witness branch in one transaction.
capsule land <id> --lander alice --remote origin
```

If something crashes between push and DB commit, run `capsule reconcile
<id> --remote origin`. For a stuck lander, `capsule force-unfreeze <id>
--remote origin --operator <you> --reason "<why>" --lander-confirmed-dead`
is the audited escape hatch.

## Verify a deployment

Before trusting a forge to enforce the capsule publication contract, run the
ACL test suite (DESIGN §8.2):

```bash
# Hermetic: spins up a tempdir bare repo with the reference pre-receive hook.
capsule deploy-verify --hermetic --base-ref main

# Remote: against a real forge with three pre-provisioned principals.
# Not yet implemented — see TODO.md §1.
```

## Status

- [x] Design (v12, `DESIGN.md`)
- [x] Agent-facing skill (`skills/capsule/SKILL.md`)
- [x] Reference CLI (`init`, `create`, `amend`, `claim`, `work`, `heartbeat`, `attest`, `land`, `abandon`, `reclaim`, `add-dep`, `remove-dep`, `list`, `status`, `reconcile`, `force-unfreeze`)
- [x] Embedded SQLite store (WAL, foreign keys, append-only event log, schema v2)
- [x] Git wire integration (atomic multi-ref push, null-OID `--force-with-lease`)
- [x] Reference pre-receive hook (`skills/capsule/pre-receive.sh`)
- [x] `capsule deploy-verify --hermetic` (eight-test ACL suite)
- [ ] `capsule deploy-verify --remote` ([`TODO.md`](TODO.md) §1)
- [ ] CI ([`TODO.md`](TODO.md) §2)
- [ ] OSS release: prebuilt binaries + `cargo publish` ([`TODO.md`](TODO.md) §3)
- [ ] `PreToolUse` hook for skill enforcement ([`TODO.md`](TODO.md) §11)

## License

MIT
