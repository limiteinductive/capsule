# Contributing to Capsule

Capsule is a coordination primitive for parallel coding agents. Changes should
preserve the core safety property: a capsule claim, verification, and land
sequence must be explainable from durable store state plus git refs.

## Development Setup

Required tools:

- Rust stable for development
- Rust 1.85 compatibility for MSRV-sensitive dependency changes
- Git

Recommended first check:

```sh
cargo build
cargo test
```

If you are touching CLI behavior, run:

```sh
cargo run -- --help
cargo run -- doctor
```

## Required Checks

Capsule declares Rust 1.85 as its minimum supported Rust version (MSRV).
Keep dependency updates compatible with that MSRV unless the workspace
`rust-version` is deliberately raised.

Before opening a PR, run the same checks as CI:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

For focused iteration, run the narrowest relevant test first, then the full
workspace test before you commit.

## Design Rules

- Keep protocol types and pure validation in `capsule-core`.
- Keep durable state transitions in `capsule-store`.
- Keep git wire behavior in `capsule-git`.
- Keep shelling out, UX, and operator conveniences in `capsule-cli`.
- Do not move git effects into `Store::attest`; attest is intentionally a DB
  transaction with no git side effect.
- Do not weaken `CanonicalPath`, capsule id, SHA, lease, or witness-ref
  validation.
- Do not add local filesystem state to the protocol. Worktrees are a CLI
  convenience, not part of the durable safety model.

## Tests

Use tests to pin invariants, not just happy paths. Important categories:

- Wire strings and JSON shapes
- State-transition precedence
- Lease boundary behavior
- Crash/reconcile behavior
- Git push classification
- CLI behavior for diagnostics and deploy verification

Timing-sensitive tests should compare against the timestamp as stored in SQLite
when the behavior under test depends on `json_extract` payload bytes.

## Commit Style

Use concise conventional-style subjects where possible:

```text
feat(cli): add doctor diagnostics
fix(store): preserve lease expiry payload
docs: refresh quickstart
test(git): pin witness stale classification
```

Prefer small, coherent commits, but do not split one safety invariant across
multiple commits just to make the diff look smaller.

## Pull Requests

Good PRs include:

- The problem being solved
- The safety invariant affected, if any
- The tests run
- Any deployment or migration implications

If a change affects the protocol, update `DESIGN.md` or explicitly explain why
the design stays unchanged.
