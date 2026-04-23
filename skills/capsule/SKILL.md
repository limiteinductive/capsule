---
name: capsule
description: Coordinate parallel code-change work via the capsule primitive — claim a path-prefix lock, work, attest verification, atomic land. Invoke when the user wants to start work that may overlap with other agents/sessions, or when an Edit/Write hook reports a scope conflict.
---

# capsule

Capsule is a path-prefix lock + verified atomic land for parallel agents on a shared repo. Use it when more than one session might touch the same code.

## When to invoke

- User asks you to start non-trivial work in a multi-agent repo (`capsule init` exists in `.capsule/`).
- A `pre-edit` hook reports `scope_conflict` or `no_capsule_covers_path`.
- User says "claim", "land", "what capsules are in flight", or similar.
- User says "I want to run multiple Claude sessions on this" — recommend `capsule init`.

Do NOT invoke for: solo-dev one-session work in a repo without `.capsule/` (capsule isn't initialized).

## The discipline (every session)

1. **Pick or create a capsule** before editing.
   ```
   capsule list --available --json
   ```
   If a suitable one exists, claim it. Otherwise:
   ```
   capsule create \
     --title "<terse>" --description "<why>" \
     --scope <prefix> [--scope <prefix>...] \
     --acceptance-cmd "<test command>" \
     --base-ref main
   ```
   **Scope rule:** narrowest prefix that covers your edits. `src/api/users.ts` not `src/`. Over-broad scope blocks other sessions for no reason.

2. **Claim it.**
   ```
   SESSION=$(uuidgen)
   capsule claim <id> --owner claude --session "$SESSION" --base-sha "$(git rev-parse main)"
   ```
   On `scope_conflict`: see Recovery below. On `unmet_deps`: pick a different task.

3. **Heartbeat while working.** Every `lease_ttl/3` seconds (default: 100s). A background loop:
   ```
   while ...; do capsule heartbeat <id> --session "$SESSION"; sleep 100; done
   ```
   Three missed heartbeats → lease expires → capsule reclaimable. If you anticipate a long pause (waiting on a build), heartbeat first.

4. **Push commits to the attempt branch.**
   ```
   git push origin HEAD:capsules/<id>/a<N>
   ```
   The attempt branch name is in the `claim` response.

5. **Attest after acceptance command passes.**
   ```
   capsule attest <id> --session "$SESSION" \
     --verified-sha "$(git rev-parse HEAD)" \
     --command "<exact command run>" \
     --exit-code 0 \
     --duration-ms <n> \
     --log-ref s3://...           # write-once URI
   ```
   Status: `active → accepted`.

6. **Land atomically.**
   ```
   capsule land <id> --session "$SESSION"
   ```
   On `base_ref_moved`: rebase attempt branch onto new `main`, push, re-attest, retry land. On `witness_oid_mismatch`: escalate (operational incident — do not retry).

## Recovery

| Error | What it means | Action |
|---|---|---|
| `scope_conflict` on claim | Another in-flight capsule covers an overlapping prefix | Run `capsule list --scope-overlaps <prefix>` to find holder. Either narrow your scope, pick different work, or wait. Do not "force." |
| `unmet_deps` on claim | A `--depends-on` capsule isn't `landed` yet | Wait, or work the dep first |
| `lease_expired` on heartbeat/attest | Crashed or paused too long | The capsule is reclaimable; do not retry. Your work on the local branch is intact — start a fresh `claim` (new attempt id), push commits to the new attempt branch, re-attest |
| `cross_session` on attest/land | Wrong session_id | You're using a stale session — match the one from `claim` |
| `base_ref_moved` on land | Someone landed onto `main` between your attest and your land | Rebase, push, re-attest with new sha, retry land |
| `witness_oid_mismatch` on land | Branch-protection leak or external corruption at the witness branch | Escalate. Capsule auto-abandons. Do not retry |
| `pending_land != null` on any op | Prior land crashed mid-flight; reconciler will resolve | Wait one sweep interval (default 60s), retry. If still stuck, operator runs `capsule force-unfreeze` |

## Setup (first-time)

```
capsule init                       # creates .capsule/state.db (sqlite, embedded)
capsule deploy verify              # runs ACL test suite against your remote
```

`init` is idempotent. `deploy verify` is required before first `land` in a new deployment — refuses to land otherwise.

## What capsule is NOT

- Not a task queue (serializes on write-set, not task id)
- Not a workflow engine (no DAG; deps are ordering only)
- Not a PR/review tool (lands directly via fast-forward push)
- Not a replacement for git (sits above it; uses it as the authoritative content store)

## Reference

Full design: `docs/capsule-design.md` (or wherever the deployment placed it). Design defines the data model, protocols, forge matrix, ACL test suite, and threat model.
