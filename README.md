# capsule

[![CI](https://github.com/limiteinductive/capsule/actions/workflows/ci.yml/badge.svg)](https://github.com/limiteinductive/capsule/actions/workflows/ci.yml)

Capsule is a path-prefix lock and verified atomic land protocol for parallel
coding agents working in the same git repository.

It gives each agent a declared write scope, a lease, an acceptance command, and
an atomic git landing path. The goal is simple: many agents can work at once
without relying on hope, chat coordination, or last-minute merge conflict
cleanup.

## Why Capsule Exists

Worktree-based agent fleets are easy to start, but they defer coordination until
merge time. That is often fine for independent files. It breaks down around
lockfiles, migrations, generated code, shared APIs, and cross-cutting refactors.

Capsule coordinates before the edit lands:

| Capability | Git alone | Worktrees alone | Capsule |
|---|---:|---:|---:|
| Path-prefix write claims | No | No | Yes |
| Lease and crash recovery | No | No | Yes |
| Verification bound to commit SHA | No | No | Yes |
| Atomic land with witness ref | No | No | Yes |
| Serialized lockfile discipline | Manual | Manual | Built in |

Pair-symmetric with a git commit:

- Commit: "here is what I did."
- Capsule: "I am taking these paths, and here is the pass criterion."

## Current Status

Capsule is a working reference implementation, not only a design note.

- Embedded SQLite store
- Path-prefix scope conflict detection
- `capsule doctor` setup diagnostics
- Claim, heartbeat, attest, land, abandon, reclaim, deps, reconcile, force-unfreeze
- `capsule events` audit-log inspection
- Git atomic multi-ref push with witness refs
- `capsule work --isolate=worktree`
- `capsule cleanup-worktrees` for stale managed worktrees
- `capsule deploy-verify` hermetic ACL suite
- Remote deploy-verify mode for provisioned lander, worker, and outsider principals
- Attest-time serialize-path lint for lockfiles such as `Cargo.lock`
- 300+ Rust tests plus clippy and format checks

The full protocol design is in [DESIGN.md](DESIGN.md). The product positioning
and implementation proposal is in [PROPOSAL.md](PROPOSAL.md).

## Install

From this checkout:

```sh
cargo build --release
./target/release/capsule --help
```

During active development:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

Generate shell completions from the current CLI schema:

```sh
capsule completions bash > ~/.local/share/bash-completion/completions/capsule
capsule completions zsh > "${fpath[1]}/_capsule"
capsule completions fish > ~/.config/fish/completions/capsule.fish
```

## Releases

Release tags use the `vX.Y.Z` format. Pushing a version tag runs the release
workflow, verifies format, clippy, and tests, then builds native `capsule`
binaries for Linux, macOS, and Windows:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The workflow attaches each binary and a `.sha256` checksum file to the GitHub
release for the tag.

## Quickstart

Initialize a store in the current repo:

```sh
capsule init
capsule doctor
```

Create a capsule for a bounded change:

```sh
capsule create \
  --id api-timeout \
  --title "Tighten API timeout handling" \
  --description "Keep this change inside the API client and its tests" \
  --acceptance-cmd "cargo test -p capsule-cli" \
  --scope crates/capsule-cli \
  --base-ref main
```

Inspect the queue and find claimable work:

```sh
capsule queue
capsule queue --scope-overlaps crates/capsule-cli --limit 5
```

Claim it for a session:

```sh
export CAPSULE_SESSION="$(uuidgen)"
capsule claim api-timeout \
  --owner "$USER" \
  --session "$CAPSULE_SESSION" \
  --base-sha "$(git rev-parse main)"
```

Run the acceptance command under a heartbeat:

```sh
capsule work api-timeout -- cargo test -p capsule-cli
```

Push the attempt branch printed by `claim`, then attest the verified commit:

```sh
git push origin HEAD:capsules/api-timeout/a1

capsule attest api-timeout \
  --session "$CAPSULE_SESSION" \
  --verified-sha "$(git rev-parse HEAD)" \
  --command "cargo test -p capsule-cli" \
  --exit-code 0 \
  --duration-ms 1000 \
  --log-ref "local://cargo-test"
```

Run the deployment ACL gate once per deployment environment:

```sh
capsule deploy-verify --hermetic
```

Land atomically:

```sh
capsule land api-timeout \
  --session "$CAPSULE_SESSION" \
  --lander "$USER" \
  --remote origin
```

For production deploy verification against a real forge, use distinct
per-principal push URLs and a validation remote:

```sh
capsule deploy-verify \
  --remote validation \
  --lander-url "$LANDER_URL" \
  --worker-url "$WORKER_URL" \
  --outsider-url "$OUTSIDER_URL" \
  --remote-allow-mutations
```

Remote deploy verification mutates refs by design. Run it against a
deploy-validation environment, not an important production branch.

## Core Model

A capsule moves through these states:

```text
planned -> active -> accepted -> landed
              |          |
              v          v
          abandoned   pending_land -> reconciled
```

Important refs:

- Worker attempt branch: `capsules/<id>/a<N>`
- Lander witness branch: `capsule-witness/<id>/a<N>`
- Base branch: usually `main`

The land operation uses `git push --atomic --force-with-lease` so base ref
movement and witness ref publication succeed or fail together.

## Audit Events

Every state transition appends an audit event. Inspect recent events:

```sh
capsule events
capsule events api-timeout --kind attempt_attested
capsule --json events api-timeout
```

## Worktree Cleanup

`capsule work --isolate=worktree` creates default attempt worktrees under
`.capsule/worktrees/<id>-a<N>`. Once a capsule is `landed` or `abandoned`, clean
those managed worktrees explicitly:

```sh
capsule cleanup-worktrees --dry-run
capsule cleanup-worktrees
```

The cleanup command only removes Capsule's default worktree paths for terminal
attempts. It leaves active attempts and custom `--worktree-dir` registrations
alone, and relies on `git worktree remove` to refuse dirty worktrees unless
`--force` is passed.

## Serialized Paths

Capsule ships a default serialize-path lint for files whose merge semantics are
usually global:

- `Cargo.lock`
- `package-lock.json`
- `pnpm-lock.yaml`
- `yarn.lock`
- `go.sum`
- `uv.lock`

If an accepted diff touches one of these paths, the capsule scope must cover it.
Override the list in `.capsule/config.toml`:

```toml
[serialize_paths]
required = ["Cargo.lock", "db/migrations/"]
```

Use `required = []` to disable the lint for a repo.

## Repository Map

- `crates/capsule-core`: protocol types and validation logic
- `crates/capsule-store`: SQLite-backed state machine
- `crates/capsule-git`: git wire integration
- `crates/capsule-cli`: reference CLI
- `skills/capsule`: agent-facing operating discipline and reference hook
- `DESIGN.md`: protocol design
- `PROPOSAL.md`: positioning and implementation plan
- `CONTRIBUTING.md`: contribution workflow and design rules
- `SECURITY.md`: vulnerability reporting and security-sensitive areas

## Development

Run the same checks as CI:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

The repo currently keeps `Cargo.lock` checked in because the CLI is a binary.
The workspace declares Rust 1.85 as its MSRV; CI checks both stable and MSRV.

See [CONTRIBUTING.md](CONTRIBUTING.md) for design boundaries, test
expectations, and PR guidance.

## Non-Goals

- Capsule is not a task queue.
- Capsule is not a planner or multi-agent orchestrator.
- Capsule is not a replacement for code review.
- Capsule is not a replacement for git.

It is the coordination and verified landing layer underneath those systems.

## License

MIT
