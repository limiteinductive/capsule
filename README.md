# capsule

A path-prefix lock + verified atomic land for parallel agents on a shared git repo.

**Status:** spec complete (`DESIGN.md`), implementation in progress.

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

## Quick read

- `DESIGN.md` — the full pure-design spec (data model, protocols, forge matrix, ACL test suite, threat model)
- `skills/capsule/SKILL.md` — agent-facing skill (Claude Code)

## Status

- [x] Design (v12)
- [x] Agent-facing skill draft
- [ ] Reference CLI (`capsule init`, `claim`, `attest`, `land`, ...)
- [ ] Embedded SQLite store
- [ ] Git wire integration (atomic multi-ref push, `--force-with-lease`)
- [ ] Reference pre-receive hook (bare-SSH deployment)
- [ ] `capsule deploy verify` ACL test suite
- [ ] OSS release: binary + skill + reference hook

## License

MIT
