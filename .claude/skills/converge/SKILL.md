---
name: converge
description: Adversarial convergence loop using critic + codex. Modes — review (specs, plans, docs), diagnose (bugs, root cause), implement (code a spec step-by-step with review at each step), write (blog posts, essays, copywriting — phased pipeline with AI-ism detection). Trigger on "converge", "/converge".
---

# Converge — Constructive Adversarial Convergence

Two independent reviewers from different model families review the same target in parallel, then cross-examine each other's findings. 2-3 rounds of independent-then-compare review is optimal.

- **Critic (Claude)** — logical gaps, hidden assumptions, structural issues
- **Codex (GPT-5.5)** — implementation bugs, code mismatches, edge cases
- **Write mode** — role-asymmetric: Claude evaluates narrative/structure, Codex detects AI-isms and clichés

## Arguments

```
/converge <target-or-description> [--mode review|diagnose|implement|write] [--rounds N] [--min-rounds N] [--focus "question"] [--severity high|medium|low] [--pre-scan [angle]]
```

- `<target-or-description>` — file path, bug description, or free-text context
- `--mode` — auto-detected from context if not specified (see below)
- `--rounds N` — max rounds before stopping (default: 3). If set below `--min-rounds`, warn the user: "Max rounds (N) is below minimum rounds (M). Running M rounds."
- `--min-rounds N` — minimum rounds before convergence is allowed (default: 2). Cannot be set below 2.
- `--focus "question"` — optional focus area to guide reviewers
- `--severity high|medium|low` — convergence threshold (default: medium). Converge only when no new findings at or above this severity remain. Low-severity nits do not block convergence.
- `--pre-scan [angle]` — run a focused preliminary scan before round 1 (see "Pre-scan" below). Angle is optional — if omitted, inferred from target type. Example angles: "security", "performance", "user-facing behavior", "edge cases".

### Mode auto-detection

If no `--mode` is given, infer from context:
- **File path to a doc** (spec, plan, design, `.md`) → `review` (unless the file content describes a bug — then `diagnose`)
- **Bug description, error message, "why does X happen"** → `diagnose`
- **"implement", "build", "code", references to a spec + impl plan** → `implement`
- **Source code file** (`.ts`, `.py`, etc.) → `review`
- **"review this blog post", "improve the writing", "check for AI-isms", prose-focused context** → `write`
- **Ambiguous** → ask the user. After clarification, proceed to input validation with the chosen mode. Mode detection runs before input validation and pre-scan, so no prior work is discarded.

### Input validation

Before launching any review:
- **File target:** verify the file exists. If not, report the error and stop.
- **Bug description without reproduction steps:** ask the user for steps if current evidence does not support a defensible root-cause hypothesis. If the user states they cannot provide steps, proceed with available information and note in the report: "No reproduction steps provided — root cause analysis is lower confidence."
- **Empty or missing target:** report the error and stop.

### Pre-scan

When `--pre-scan "angle"` is provided, run a single focused Codex pass **before** round 1 begins. The pre-scan reviews the target through a specific lens (security, performance, user-facing behavior, etc.) and produces a list of angle-specific findings.

**Why pre-scan instead of a third reviewer:** A third reviewer from the same family as Codex (another GPT-5.5) would have highly correlated failure modes with the first — prompt diversity is weaker than model diversity for catching different bugs. Adding compute from the same family gains little coverage. Pre-scan is cheaper (one extra call, not 50% more per round) and its findings get properly cross-examined by two different-family reviewers.

**How it works:**
1. Launch one Codex call using the isolated invocation pattern (see "Codex invocation pattern" section) with the angle prompt: "Review this target specifically for [angle]. Return findings with evidence."
2. Collect pre-scan findings and label them `PS1`, `PS2`, etc. (temporary intake labels). When they enter the round 2 synthesis table, assign `F*` IDs — **unless** a PS finding duplicates an existing round-1 F finding (same claim + same evidence), in which case reuse the existing F ID and append "confirmed by pre-scan" to its note. This keeps cycling detection and finding-identity rules consistent.
3. Feed pre-scan findings to both reviewers in **round 2** (not round 1) as additional items to verify: "A preliminary scan flagged these items. Verify, dispute, or confirm each as part of your cross-critique." This preserves the blind independence of round 1 — injecting shared findings before independent review would anchor both reviewers on the same hypotheses.
4. Both real reviewers can cross-examine pre-scan findings in the normal 2-reviewer flow — no orphaned or unverified findings.

The pre-scan does NOT count as a round. It is a context-enrichment step.

**Angle selection:** If `--pre-scan` is provided without a specific angle, infer a sensible default from the target type:
- Source code → "security and error handling"
- API spec / schema → "breaking changes and backwards compatibility"
- Design doc / plan → "feasibility and missing requirements"
- General / unclear → default to "correctness and edge cases" (do not ask — avoid double-question flow when mode is also ambiguous)

---

## Round structure (review, diagnose, implement)

All modes except write use the same round structure. Write mode uses a phased pipeline instead (see "Mode: Write"). Mode-specific differences in fix application are noted below.

**Round 1 — Independent review (blind):**
1. Launch both reviewers in parallel. Neither sees the other's output. Each reviews independently. (Pre-scan findings, if any, are held until round 2 to preserve blind independence.)
2. Collect findings — each returns claims with evidence, classified by severity (High/Medium/Low).
3. Synthesize into convergence table, then apply fixes per the **fix application policy** below.
4. Output progress line and check user input (see "User controls").

**Fix application policy (used by Round 1 step 3 and Round 2 step 3):**
- **Review / implement modes**: apply *mechanical* fixes directly — typos, broken internal references, formatting errors, and contradictions where the correction is uniquely determined by adjacent text. Treat *intent-changing* fixes (new guardrails, changed scope or requirements, factual corrections that require domain judgment, API behavior, architectural direction, product decisions) as pending-approval: surface a one-line summary + proposed diff to the user and apply only after approval. If in doubt, treat as intent-changing. Pending-approval findings remain active in the convergence table with status `pending-approval`; cross-critique rounds continue to include them as active items (not as fixed) until the user decides.
- **Diagnose mode**: record all proposed fixes for user approval as before.
- **Implement mode commit ordering**: do not commit a converged step while any pending-approval fix for that step is outstanding. Commit only after all mechanical and approved intent-changing fixes have landed.

**Round 2 — Cross-critique (always runs):**
1. Send each reviewer the OTHER's **active (unfixed)** round 1 findings with the debiasing prompt: "Assume the other review contains at least one error. Identify it with evidence, or explain with evidence why each finding is correct." Fixed findings are excluded from the active set per "Finding identity and tracking" and summarized per "Context management" (one-line neutral status only — no stale quotes or line numbers). This generalizes to every cross-critique round (Round 2+): send only active findings; fixed findings appear only as context summaries.
2. Each reviewer: confirms, disputes with counter-evidence, or adds findings they missed in round 1.
3. Synthesize. Apply fixes per the fix application policy above. Flag disagreements.
4. Output the progress line and check user input (see "User controls"). If no input, check convergence criteria. If not converged AND max rounds > 2, continue to round 3.

**Round 3+ — Focused resolution (only if unresolved High/Medium findings remain):**
1. Send unresolved findings plus the **minimal target excerpts** needed to verify each (quoted passages, line-numbered snippets, or changed hunks with surrounding context). Do NOT send the full target. Scope the prompt: "These N findings remain in dispute. For each, provide your final position with evidence."
2. Check for cycling before synthesizing (see below).
3. If converged or max rounds reached, stop and produce report.

### Convergence criteria (review, diagnose, implement)

Write mode uses per-phase criteria instead (see "Mode: Write").

Stop when ALL of:
- At least `--min-rounds` rounds completed (minimum 2, always)
- No new findings at or above `--severity` threshold (default: Medium) in the latest round, AND each reviewer explicitly states "no new findings at threshold" with a brief note of what they checked (per Rule 6). Degraded single-reviewer mode uses its own exit — see Error handling.

OR: Max rounds reached → **stopped** (report remaining disagreements)

OR: **Cycling detected** → **stopped** (see below)

**Additional mode-specific requirements:**
- **Diagnose:** both must agree on root cause. Disagreement on cause = not converged, even if no "new" findings.
- **Implement:** per-step convergence. Each step must satisfy the criteria independently. Final verification round on full changeset does not count toward step rounds.

### Cycling detection

Cycling = round N re-argues the same claims as round N-2 with no new evidence. To detect:
1. Compare the finding IDs and evidence cited in round N vs round N-2.
2. If >80% of findings are the same claims with the same evidence (just re-stated), declare cycling.
3. This is a judgment call, not exact string matching. The key question: "Did this round produce any NEW evidence or NEW claims?" If no → cycling.

Stop immediately and report: "Convergence stopped — reviewers are repeating arguments without new evidence. Remaining disagreements require human judgment."

Note: "Assume the other review has at least one error" (the debiasing prompt) does NOT mean reviewers must invent disagreements. If a reviewer checks and finds no errors, they should say so with evidence of what they checked. The debiasing prompt prevents rubber-stamping, not genuine agreement.

---

## Mode: Review

Converge on the quality of a document (spec, plan, design doc, code file).

Reviewers receive: the target document + any `--focus` context. **Context budget:** for targets over 500 lines, send a summary + the sections most relevant to `--focus` (or the full doc if no focus is specified and it fits). Target: reviewer prompt should not exceed ~30K tokens including boilerplate and accumulated findings.

Fixes are applied between rounds per the **fix application policy** in the "Round structure" section — mechanical fixes land directly; intent-changing fixes require user approval first. Both reviewers see the updated document in subsequent rounds. **For source code targets:** run typecheck/lint/tests after applying fixes, same as implement mode. If they fail, revert the fix and flag it as disputed.

---

## Mode: Diagnose

Converge on the root cause of a bug or unexpected behavior.

### Workflow

1. **Gather context** — read error messages, logs, relevant code. Include only code paths relevant to the hypothesis (keep reviewer prompts under ~30K tokens). Ask the user for reproduction steps if unclear (per input validation rules — proceed at lower confidence if unavailable).
2. **Form hypothesis** — state the suspected root cause with evidence.
3. **Run rounds 1-N** on the hypothesis — reviewers stress-test and verify.
4. **Propose fix to user** — present the fix with evidence once converged. The user decides whether to apply it. Do NOT apply fixes to production code without user confirmation in diagnose mode.
5. **Verification round** — after the user approves and the fix is applied, run one more reviewer pass: "Does this fix address the root cause? Any regressions?" This is a bonus round outside the convergence loop — it does not count toward `--rounds`.

---

## Mode: Implement

Converge on a full implementation of a spec or plan. You (Claude) write the code; reviewers verify each step against the spec.

### Workflow

1. **Read the spec and impl plan** — identify the ordered list of steps/stories.
2. **For each step:**
   a. **Implement** — write the code changes for this step.
   b. **Self-check** — run typecheck, lint, tests. Fix any failures.
   c. **Launch both reviewers** — send them the **diff for this step only** (not full files, not cumulative diffs). For files over 100 lines, send only changed hunks with **30 lines** of surrounding context (reviewers need enough context to spot aliasing, view relationships, and state set up earlier in the function).
   d. **Run rounds 1-N** per the shared round structure above.
   e. **Step converged** → move to next step.
3. **Final verification** — after all steps, run both reviewers on the full changeset vs. the spec: "Is the spec fully implemented? Any gaps?" This is a single pass, not a convergence loop. If final verification finds High/Medium issues, create a follow-up implementation step to address them, then rerun final verification. Repeat until clean or user stops.
4. **Report.**

### Key rules for implement mode

- **You write the code, reviewers verify.** Don't delegate implementation to subagents.
- **Typecheck/lint/test between steps.** Don't accumulate broken code.
- **NEVER skip reviewer rounds.** Every step must be reviewed before committing. Do not commit steps while "waiting for reviewers on a previous step." Skipping rounds to move fast is false economy — bugs that slip through cost more time than the review.
- **Commit after each converged step** (if the user wants — ask on the first step, then follow that preference). Stage only files modified in the current step by name (not `git add -A`). Use message format: `converge: step N — [step name]`. If not in a git repo, skip commits. If a pre-commit hook fails, fix the issue and create a new commit.
- **If a reviewer finding requires changing the spec or plan**, flag it to the user before proceeding. Don't silently deviate from the spec.
- **If stuck on a step** (reviewers keep finding new issues after max rounds), pause and ask the user.

---

## Mode: Write

Converge on the quality of a prose artifact (blog post, essay, announcement, documentation narrative) through **phased review**. Write mode replaces the standard round structure with a sequential pipeline where each phase has its own reviewer prompts, evidence standard, and convergence criteria.

**Write mode overrides for shared infrastructure:**
- The global `--min-rounds ≥ 2` rule does NOT apply in write mode. Each phase has its own round semantics. `--rounds` caps apply per phase: Phase 1 max 2, Phase 3 max 1 unless `more rounds` extends. Values exceeding caps are silently capped. Phases 2 and 4 are orchestrator-only, always single-pass. The overall pipeline always runs all 4 phases.
- Use the **write mode severity table** below instead of the standard severity definitions in reviewer prompts. Do not send both. The global `--severity` threshold is ignored in write mode; convergence is per-phase (see each phase's criteria). The accumulated-Low rewrite trigger (8+) is treated as an effective Medium finding for convergence purposes.
- In Phase 3, **strip Rule 8 (cross-critique)** from the reviewer prompt — Phase 3 is role-asymmetric with no cross-examination.
- **User controls in write mode:** "skip" advances to the next phase. "stop" halts the entire pipeline. "more rounds" adds rounds to Phase 1 or Phase 3 only.

### Write mode severity

| Severity | Definition | Example |
|----------|-----------|---------|
| High | Factual error, logical contradiction, credibility-destroying claim, AI-ism that signals "generated" | "GPT-4 was released in 2022"; "fundamentally transforming the landscape" |
| Medium | Structural gap, audience mismatch, section that doesn't earn its length, unclear argument | A section that doesn't connect to the thesis |
| Low | Word choice, minor phrasing, rhythm issue, optional improvement | "roughly" where a precise number exists |

**Accumulated Low findings matter in writing.** A piece with 15 Low-severity AI-isms is worse than one with a single Medium structural gap. Treat accumulated Lows as a signal for a full style rewrite, not individual nits. **Threshold:** trigger a full style rewrite when 8+ Low findings accumulate, or the same pattern appears in 3+ sections.

### Write mode evidence format

Do NOT use file paths or line numbers. Use direct quotes:

```
QUOTE: [exact text from the piece]
PROBLEM: [specific diagnosis — not "this is awkward" but "passive construction obscures the actor"]
FIX: [concrete rewrite, or "delete"]
```

### Pre-step: Thesis and Arc (required, orchestrator-only)

Before launching any reviewer:

1. **Write a 2-sentence TLDR.** What is the one thing this piece must communicate, and why does it matter? If the text lacks a clear thesis, flag this to the user as a blocking issue.
2. **Write a narrative arc** — 4-6 bullets showing the logical progression. Format: `[section] → [what it establishes] → [why the reader needs this before the next section]`.
3. **Confirm with the user.** The thesis and arc are the evaluation anchor for all phases. Revise if the user disagrees.

This step replaces the implicit "spec" that code has. Without it, reviewers evaluate against personal taste, producing preference disagreements instead of findings.

### Phase 1: Accuracy (2 reviewers, max 2 rounds)

**Goal:** Every factual claim is correct.

**Context budget (critical):** Reviewers receive ONLY the text and any reference documents the orchestrator provides **inline**. Do NOT tell reviewers to "verify against codebase" or give them tool access — this causes context exhaustion as they spend all tokens exploring files instead of reviewing. If a claim can't be verified from inline references, the reviewer flags it as `[UNVERIFIABLE]` with what reference would resolve it.

**How to provide references:** Before launching, the orchestrator reads the relevant docs/source files and includes key excerpts inline in the reviewer prompt. Keep to <2000 tokens of reference material per reviewer. **Scaling for large docs:** if the text has more claims than can be verified against a 2000-token reference budget, split the text into sections, verify each batch separately with its own relevant references, then merge results.

**Reviewer prompt (same for both):**
```
Verify every factual claim in this text against the reference material provided.

For each claim:
QUOTE: [the claim]
REFERENCE: [which reference confirms or contradicts, with exact quote]
STATUS: correct | incorrect | unverifiable
FIX: [corrected text if incorrect]

Do NOT search for files, read code, or use tools. Work only from the text and
references provided. If you cannot verify a claim, mark it [UNVERIFIABLE].
```

**Convergence:** Both agree on all claims, or disputes are flagged for the user. Max 2 rounds.

### Phase 2: Structure and Narrative (orchestrator-only, single pass)

**Goal:** The piece follows the agreed arc. Each section earns the next.

Compare the text against the thesis/arc and flag:
- Sections that don't serve the thesis
- Missing transitions between sections
- Sections in the wrong order
- Where reader attention likely dies (dense paragraphs, repeated ideas, list fatigue)
- Whether the opening hooks and the closing lands

Apply structural edits. If any factual claims were moved, reworded, or removed, re-verify affected claims against references before proceeding to Phase 3. Show the user the updated arc if it changed significantly.

### Phase 3: Style and Taste (2 reviewers, 1 round, role-asymmetric)

**Goal:** The writing sounds like a specific person wrote it with care.

Launch both reviewers in parallel, but with **different roles**:

**Critic (Claude) — narrative and audience:**
```
Review for narrative quality and audience fit.

AUDIENCE: [from user or inferred]
THESIS: [from pre-step]

Find:
- Paragraphs that tell instead of show (claiming a conclusion without story or evidence)
- Hedging that weakens claims without adding nuance
- Sections where energy drops — the reader would skim or stop
- Transitions that are mechanical
- Whether the opening hooks and the closing lands

For each finding:
QUOTE: [exact text]
PROBLEM: [specific diagnosis]
FIX: [suggested rewrite or "delete"]
```

**Codex (GPT via `codex exec`) — AI-ism and cliché detection:**

Invoke using the isolated Codex pattern from the "Codex invocation pattern" section — build the prompt as a temp file and pipe via stdin to `codex exec -` from `/tmp`. Do NOT run from the project directory or embed content as a shell argument.

```
Find every phrase that sounds AI-generated, formulaic, or like startup content marketing.

Patterns to catch:
- Formulaic constructions: "Not X, but Y"; "This isn't just A — it's B"
- Hollow intensifiers: "truly", "incredibly", "fundamentally", "genuinely"
- Performed humility: "rough edges and all", "we don't have all the answers"
- Anthropomorphized software: "the system reasons", "it knows", "it understands"
- Pitch-deck cadence: "One X. One Y. One Z." and short slogan fragments
- Thesis-restating: concluding by paraphrasing the introduction
- Meta-commentary: "here's where most posts stop", "let's dive in"
- Sweeping predictions: "every team will", "the future of", "the next generation of"
- LinkedIn-ready aphorisms: "small wins compound", "that's stubbornly human"

Also flag dead metaphors, clichés, and sentences that sound correct but say nothing.

For each finding:
QUOTE: [exact text]
PATTERN: [which pattern this matches]
FIX: [rewrite or "delete"]
```

**No cross-critique in phase 3.** The two reviewers evaluate different things (narrative vs tics), so cross-examination adds no value. Synthesize both sets of findings and apply.

### Phase 4: Final sweep (orchestrator-only)

Single read-through for rhythm, word-level polish, and overall feel:
- Sentences too long or short relative to neighbors
- Repeated words within 2-3 sentences
- Paragraph openings that all use the same structure
- Anything that "sounds off"

Apply micro-edits. Self-check against Phase 3 AI-ism findings to ensure no flagged patterns were reintroduced. If a full style rewrite was triggered in Phase 3, re-verify any factual claims in rewritten sections against references before applying final polish. Show the user the final version.

### Write mode progress signal

```
--- Phase N/4: [Accuracy|Structure|Style|Sweep] | Findings: X (H:a M:b L:c) | Status: [done/continuing] ---
```

### Write mode: what the orchestrator does vs reviewers

**Orchestrator (you, Claude) writes.** Reviewers critique. The orchestrator does the actual rewriting between phases. Reviewers never produce drafts — they produce findings with suggested fixes. This is the same as implement mode: you write, they verify.

---

## Reviewer prompting

Both reviewers get these instructions verbatim every round. This is the most important section of the skill — LLM reviewers are highly sensitive to emotional framing and will mirror whatever tone they receive. The goal is **constructive peer review**: rigorous and evidence-based, like two senior engineers reviewing each other's PRs. Direct but respectful. Evidence over opinion. Suggestions over complaints.

**Write mode adjustment:** Write mode overrides rules 2 and 8 — see "Write mode overrides for shared infrastructure" in Mode: Write. When constructing a write-mode reviewer prompt, edit the RULES block before sending: replace rule 2's severity bullets with the write-mode severity table, and do not leave the standard severity table in the prompt. Rule 8 (cross-critique) is stripped only in Phase 3 (role-asymmetric, no cross-examination). Phase 1 sends the full edited rules block including rule 8 across its 2 rounds. Phases 2 and 4 are orchestrator-only and never use the reviewer-prompt block. **Phase 1 exception:** Phase 1's Accuracy prompt uses its own `STATUS: correct | incorrect | unverifiable` format — severity labels apply only when a reviewer reports a finding beyond claim verification or when synthesizing progress counts.

```
RULES FOR THIS REVIEW:

You are one of two independent reviewers. Your goal is to make the work better 
through rigorous, evidence-based analysis. This is peer review, not a debate to 
win. Be direct but constructive.

1. EVIDENCE FIRST. Every claim must cite: file path + line number, or a direct 
   quote from the target. A claim without evidence is not a finding — it is 
   speculation. State what the code/doc does, what the spec/intent requires, and 
   the gap between them.

2. SEVERITY. Classify each finding:
   - High: correctness bug, security issue, spec violation, data loss risk
   - Medium: logic gap, missing edge case, unclear behavior, performance issue
   - Low: style, naming, minor readability, non-blocking suggestion

3. NO EMOTIONAL LANGUAGE. Banned words: "clearly", "obviously", "unfortunately",
   "importantly", "crucial". Banned patterns: "I think maybe", "it seems like", 
   "this is wrong". Instead: "Line 42 calls foo(). foo is not defined in this 
   scope." Full stop. LLM reviewers escalate when prompted emotionally — keep 
   all language clinical and neutral.

4. CONSTRUCTIVE. Every finding must include a concrete fix suggestion or a 
   specific question. "Line 42 calls foo() which is undefined — either import 
   it from utils.ts or replace with bar()" is useful. "This is broken" is not.

5. NO DEFENSIVENESS. If the other reviewer contradicted a prior finding:
   - Counter-evidence: "Line 42 shows X, which contradicts reviewer's claim Y"
   - Correction: "Corrected — [prior finding] was wrong because [reason]"
   Never: "I still believe..." or "as I mentioned before..."

6. NO RUBBER-STAMPING. "No issues found" requires MORE rigor than a finding:
   describe what you checked, how you checked it, and why it is correct. If you 
   checked nothing new, say so explicitly. This standard prevents premature 
   convergence.

7. UNCERTAINTY IS OK. Label uncertain findings "[UNCERTAIN]" and state what 
   information would resolve them. Do not suppress findings because you're 
   unsure — flag the uncertainty.

8. CROSS-CRITIQUE (rounds 2+). Assume the other review may contain errors. 
   Independently verify each of the other reviewer's findings. If you agree, 
   explain WHY with evidence — not just "I concur." If you find no errors after
   genuine checking, say so. Do not manufacture disagreements.

9. PROPORTIONAL DEPTH. High findings get full analysis and a fix. Medium get a 
   paragraph. Low get one line. Do not write a paragraph about a naming nit.

10. ALIASING AND VIEWS. When reviewing code that operates on tensors, buffers,
    or arrays: trace each variable back to where it was created. If a variable
    is a view/slice of another (e.g., `y = x[:n]`), flag any operation that
    mutates the underlying buffer while the view is still in use. This class
    of bug is invisible in diffs — it requires reading surrounding context.

11. INITIALIZATION ORDERING. When reviewing code that initializes multiple
    subsystems sequentially: check what mutable state each step leaves behind.
    Flag cases where step N leaves shared state (flags, buffers, descriptors)
    that corrupts step N+1.
```

### Symmetric vs asymmetric prompts

- **Round 1 (blind):** Give both reviewers the **same prompt** — identical task, identical rules. Let model diversity (Claude vs GPT) do the work. Same-task independent review produces the cleanest comparison signal.
- **Round 2+ (cross-critique):** Add **light role guidance** to leverage each model's strengths — "pay particular attention to logical coherence and structural issues" for Critic, "pay particular attention to implementation correctness and code-level details" for Codex. They're now engaging with specific findings, not doing independent assessment, so targeted guidance helps.

### Debiasing

**Never** send both reviewers' findings in the same undifferentiated block. Always label whose findings are whose. The round structure (blind → cross-critique → focused) is the primary anchoring mitigation.

---

## Orchestration

### Launching reviewers

Launch BOTH reviewers in the SAME message with `run_in_background: true`:

- **Critic:** Use `Agent(prompt: "...", run_in_background: true)`. Embed the review target inline in the prompt (same as Codex — the Agent has no guaranteed file access). For targets over 500 lines, send only the relevant sections with 10 lines of surrounding context. If the Agent tool is unavailable, fall back to **degraded single-reviewer mode** (Codex only — see Error handling). Do NOT launch a second Codex instance as a substitute: same-family reviewers have correlated failure modes and will not reproduce the model-diversity property the skill depends on.
- **Codex:** Use `Bash(command: "...", run_in_background: true, timeout: 300000)` with the invocation pattern below

**Do NOT launch one blocking and one background** — that serializes execution.

### Codex invocation pattern (critical — prevents timeout)

Codex CLI is an autonomous agent. When run inside a git repo, it will explore the filesystem to "gather context" before reviewing — burning its token budget and timing out before producing findings. This is the most common failure mode of `/converge`.

**Prevention: isolate Codex from the repo and pipe the prompt via stdin.**

The orchestrator must build the prompt in two steps — write it to a temp file, then pipe it to `codex exec` via stdin. Do NOT embed content as a shell argument via `$(cat ...)` — markdown files contain backticks, dollar signs, and quotes that break shell expansion, causing Codex to echo a mangled prompt and produce no findings.

```bash
# Step 0: Write the review target to a temp file (the Codex prompt appends from here).
# Use the Write tool or `cat <<EOF > /tmp/converge-review-target.md ...` — do this ONCE before
# launching reviewers. The target file is shared across rounds; rebuild it if the target changes.

# Step 1: Build the full prompt file (instructions + target content)
cat > /tmp/codex-prompt.txt << 'ENDOFPROMPT'
IMPORTANT: Do NOT use any tools. Do NOT read files, run commands, or explore
the filesystem. Your ENTIRE review target is provided below. Work ONLY from
this text. Produce your findings and stop.

<review instructions here>

=== REVIEW TARGET ===
ENDOFPROMPT
cat /tmp/converge-review-target.md >> /tmp/codex-prompt.txt
echo '=== END TARGET ===' >> /tmp/codex-prompt.txt

# Step 2: Pipe to codex via stdin (the `-` arg reads prompt from stdin)
# Pin the model explicitly — do not rely on the CLI default, which may drift between codex-cli
# versions. If `-m gpt-5.5` is not recognized by the installed CLI, log the limitation and
# proceed with the default (note in the report: "model pin failed, review labeled model-unknown").
cat /tmp/codex-prompt.txt | codex exec \
  -m gpt-5.5 \
  --skip-git-repo-check \
  -C /tmp \
  --ephemeral \
  -s read-only \
  -
```

**Why this approach:**
- **Stdin piping (`-`)** — avoids shell expansion entirely. No backticks, dollar signs, or quotes in the target can break the command. This is the #1 reliability fix.
- **Heredoc with single-quoted delimiter (`'ENDOFPROMPT'`)** — prevents shell expansion in the instruction portion
- **`-m gpt-5.5`** — explicit model pin; guarantees the model-diversity property the skill depends on (Critic = Claude, Codex = GPT-5.5)
- `--skip-git-repo-check` — allows running outside a git repo
- `-C /tmp` — no repo to explore
- `--ephemeral` — don't save session files
- `-s read-only` — sandbox guardrail (flag name may vary by CLI version — see prerequisite)

**Timeout:** Always set `timeout: 300000` (5 minutes) on the Bash call.

**Prerequisite:** Before first Codex invocation in a session, verify `codex` is installed and the required flags work (`codex exec --help` — confirm `-m`, `--skip-git-repo-check`, `--ephemeral`, and the sandbox flag all appear). If any required flag is unsupported, drop that flag, note the limitation in the report, and proceed if possible. If `codex exec` itself is unavailable, fall back to degraded single-reviewer mode (Critic only).

**Do NOT:**
- Embed content as a shell argument via `$(cat file)` — backticks, dollar signs, and quotes in the target will break shell expansion, causing Codex to echo a mangled prompt with no findings
- Run Codex from the project directory — it WILL explore the filesystem
- Omit the "Do NOT use any tools" instruction — without it, Codex defaults to agent behavior

### Waiting for completion

After launching both reviewers, output: "Both reviewers launched. Waiting for results..." You will be automatically notified when each completes. Do not poll or sleep. When the first reviewer completes, note it but do not proceed until both complete (or one times out per error handling). When both have returned results, proceed to synthesis.

### Error handling

- **Agent timeout or failure:** If one reviewer fails to return results, proceed with the other reviewer's findings only. Note in the report: "Round N: [reviewer] timed out. Findings from [other reviewer] only — treat as unverified." **Degraded convergence:** single-reviewer rounds cannot satisfy the "both reviewers" convergence criterion. In degraded mode, run one additional round with the surviving reviewer, then stop and report as "Stopped (degraded — single reviewer)." Do not attempt full convergence with one reviewer. **Fallback is symmetric:** whichever reviewer is unavailable is dropped; the other runs alone. Never substitute a same-family reviewer for a missing one (no double-Codex, no double-Critic) — that breaks the model-diversity property the skill depends on.
- **Malformed output:** If a reviewer returns findings without evidence (violating the prompting rules), discard those findings and note: "Round N: [X] findings from [reviewer] discarded — no evidence provided."
- **Both fail:** Report the failure and stop. Do not retry automatically — ask the user.

### Progress signals

After each round, output:

```
--- Round N/M | Findings: X new (H:a M:b L:c) | Cumulative fixed: Y | Status: continuing/converged/stopped ---
```

For implement mode, prefix with the step:
```
--- Step 2/5 | Round N/M | Findings: X new (H:a M:b L:c) | Status: continuing ---
```

### User controls

After each round's progress line, the user may respond. If they do:
- **"skip"** or **"good enough"** → accept current state, move to next step (implement) or stop (review/diagnose)
- **"stop"** → halt convergence entirely, produce report with current findings
- **"override [finding]"** → mark a disagreement as resolved in the user's favor
- **"more rounds"** → increase max rounds by 2 for the current step/target

If the user is silent (no message), continue the convergence loop automatically. If the user sends an unrelated message, pause convergence and handle the new request unless the user explicitly says to continue — unrelated input is likely a context-shift, not consent to keep running.

### Finding identity and tracking

Assign each finding a stable ID when it first appears: `F1`, `F2`, etc. Use these IDs throughout the convergence — in synthesis tables, cross-critique prompts, and the final report. This prevents:
- **Duplicate detection failure:** paraphrased findings being treated as "new"
- **Cycling false negatives:** re-argued findings not being recognized as the same claim
- **Tracking confusion:** "the finding about foo()" is ambiguous; "F3" is not

When a finding is fixed, note it as "F3: fixed in round N" and do not send it to reviewers again.

### Stale references after edits

When fixes are applied between rounds, line numbers in prior findings may shift. Before sending prior findings to reviewers in the next round:
- Update line numbers if the edit location is known (e.g., you applied the fix, so you know the delta)
- If line numbers can't be reliably updated, replace them with a quote from the relevant code: "the block containing `foo(bar)`" instead of "line 42"
- Never send stale line numbers to reviewers — this causes false confirmations or false disputes

### Context management

Multi-round convergence accumulates context. To prevent prompt bloat:
- **Fixed findings:** One line: "F3: fixed in round N — [description]". Don't include full text or diff.
- **Active findings:** Include full text only for findings still under discussion.
- **Prior-round reviewer output:** Summarize. "Critic round 2: 2 new findings (F5 High, F6 Medium), agreed on F3, disputed F4."
- **Implement mode:** Only the current step's diff. Never cumulative diffs.

---

## Convergence table

After each round, synthesize findings from both reviewers:

```markdown
| ID | Finding | Sev | Critic | Codex | Status |
|----|---------|-----|--------|-------|--------|
| F1 | <claim with evidence> | H/M/L | <position> | <position> | Agree / Disagree / New |
| F2 | ... | ... | ... | ... | Fixed (round N) |
```

Use stable IDs (F1, F2...) throughout. Mark fixed findings so they don't re-enter circulation.

---

## Report format

The orchestrator (you, Claude) generates the report after convergence or stopping.

```markdown
## Convergence Report: <target>

**Mode:** review | diagnose | implement | write
**Rounds:** N (min: M) | **Severity threshold:** Medium
**Status:** Converged / Stopped (max rounds) / Stopped (cycling) / Stopped (user) / Stopped (degraded — single reviewer)

### Accepted findings
1. [H] <finding> — fixed in round N
2. [M] <finding> — fixed in round N

### Remaining disagreements (if any)
1. <topic>: Critic says X (evidence: ...). Codex says Y (evidence: ...).

### Below-threshold findings (not blocking)
1. [L] <finding> — noted, not fixed

### Changes applied
- Round 1: <list>
- Round 2: <list>
```

For implement mode, also include:
```markdown
### Steps completed
1. <step name> — converged in N rounds, M findings fixed
2. ...

### Final verification
- <result>
```
