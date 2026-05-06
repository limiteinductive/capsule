use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, Capsule, ExpectExit, Status};
use capsule_store::{
    AbandonRequest, AmendRequest, AttestRequest, ClaimRequest, DepRequest, ForceUnfreezeRequest,
    LandRequest, ListFilter, NewCapsule, ReconcileRequest, Store,
};
use clap::{CommandFactory, Parser, Subcommand};
use time::format_description::well_known::Rfc3339;

mod config;
mod deploy_verify;
mod init;
mod serialize_lint;
mod worktree;

#[derive(Parser)]
#[command(
    name = "capsule",
    version,
    about = "Path-prefix lock + verified atomic land for parallel agents."
)]
struct Cli {
    /// Path to the store dir (default: `.capsule/` in cwd).
    #[arg(long, env = "CAPSULE_DIR", global = true)]
    dir: Option<PathBuf>,

    /// Emit JSON on stdout where applicable.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Initialize a capsule store at `<dir>/state.db`.
    Init(InitArgs),
    /// Diagnose local capsule setup without mutating state.
    Doctor,
    /// Generate shell completion scripts.
    Completions(CompletionsArgs),
    /// Remove Capsule-managed worktrees for terminal attempts.
    CleanupWorktrees(CleanupWorktreesArgs),
    /// Run the deployment ACL test suite (DESIGN §8.2). Hermetic mode spins
    /// up a tempdir bare repo with the reference pre-receive hook; remote
    /// mode runs against a real forge with three pre-provisioned principals.
    DeployVerify(DeployVerifyArgs),
    /// Create a new capsule.
    Create(CreateArgs),
    /// Amend a planned capsule (pre-claim). Use before `claim` to fix a
    /// too-broad scope or wrong acceptance without abandoning the capsule.
    Amend(AmendArgs),
    /// Show full state of a single capsule.
    Status(StatusArgs),
    /// Claim a planned capsule for a session.
    Claim(ClaimArgs),
    /// Run a command under the active attempt, heartbeating until it exits.
    Work(WorkArgs),
    /// Heartbeat the active attempt.
    Heartbeat(HeartbeatArgs),
    /// Record verification for the active attempt.
    Attest(AttestArgs),
    /// Land an accepted capsule via atomic multi-ref push.
    Land(LandArgs),
    /// Abandon a capsule.
    Abandon(AbandonArgs),
    /// Reclaim an expired capsule (manual).
    Reclaim(ReclaimArgs),
    /// Add a dependency edge.
    AddDep(DepArgs),
    /// Remove a dependency edge.
    RemoveDep(DepArgs),
    /// List capsules.
    List(ListArgs),
    /// Run the reconciler on a frozen capsule (pending_land set).
    Reconcile(ReconcileArgs),
    /// Operator escape hatch: force-clear a stuck pending_land.
    ForceUnfreeze(ForceUnfreezeArgs),
}

#[derive(clap::Args)]
struct InitArgs {
    /// Don't touch `.gitignore`. Default: append a rule so `state.db` isn't committed.
    #[arg(long = "no-gitignore")]
    no_gitignore: bool,
}

#[derive(clap::Args)]
struct CompletionsArgs {
    /// Shell to generate completions for.
    #[arg(value_enum)]
    shell: clap_complete::Shell,
}

#[derive(clap::Args)]
struct CleanupWorktreesArgs {
    /// Show what would be removed without deleting worktrees.
    #[arg(long)]
    dry_run: bool,
    /// Pass `--force` to `git worktree remove` for dirty or locked worktrees.
    #[arg(long)]
    force: bool,
}

#[derive(clap::Args)]
struct CreateArgs {
    /// Stable id (default: random uuid).
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    title: String,
    #[arg(long, default_value = "")]
    description: String,
    /// Acceptance command, run by the worker. e.g. `pnpm test`.
    #[arg(long = "acceptance-cmd")]
    acceptance_cmd: String,
    /// Expected exit code on success.
    #[arg(long = "acceptance-expect-exit", default_value_t = 0)]
    acceptance_expect_exit: i32,
    #[arg(long = "acceptance-cwd")]
    acceptance_cwd: Option<String>,
    #[arg(long = "acceptance-timeout-sec")]
    acceptance_timeout_sec: Option<u64>,
    /// Path prefix(es). Repeatable. Canonicalized at create.
    #[arg(long = "scope", required = true)]
    scope: Vec<String>,
    /// Base ref to land onto, e.g. "main".
    #[arg(long = "base-ref")]
    base_ref: String,
    /// Capsule id this depends on. Repeatable.
    #[arg(long = "depends-on")]
    depends_on: Vec<String>,
}

#[derive(clap::Args)]
struct AmendArgs {
    capsule_id: String,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    description: Option<String>,
    #[arg(long = "acceptance-cmd")]
    acceptance_cmd: Option<String>,
    #[arg(long = "acceptance-expect-exit")]
    acceptance_expect_exit: Option<i32>,
    #[arg(long = "acceptance-cwd")]
    acceptance_cwd: Option<String>,
    #[arg(long = "acceptance-timeout-sec")]
    acceptance_timeout_sec: Option<u64>,
    /// Replace scope prefixes wholesale. Repeatable. Empty = leave unchanged.
    #[arg(long = "scope")]
    scope: Vec<String>,
    #[arg(long = "base-ref")]
    base_ref: Option<String>,
}

#[derive(clap::Args)]
struct StatusArgs {
    capsule_id: String,
}

#[derive(clap::Args)]
struct WorkArgs {
    capsule_id: String,
    #[arg(long, env = "CAPSULE_SESSION")]
    session: String,
    /// Working-tree isolation mode. `worktree` materializes a per-attempt git
    /// worktree on the attempt branch and chdirs the child into it. After
    /// `--isolate=worktree` starts the worktree, run `git sparse-checkout set
    /// <prefixes>` inside it to minimize the on-disk read scope.
    #[arg(long, value_enum, default_value_t = IsolateMode::None)]
    isolate: IsolateMode,
    /// Override worktree path. Default: `<capsule_dir>/worktrees/<id>-a<N>`.
    /// Only meaningful with `--isolate=worktree`.
    #[arg(long = "worktree-dir")]
    worktree_dir: Option<PathBuf>,
    /// Command + args after `--`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
    cmd: Vec<String>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
enum IsolateMode {
    None,
    Worktree,
}

#[derive(clap::Args)]
struct ClaimArgs {
    capsule_id: String,
    #[arg(long)]
    owner: String,
    #[arg(long, env = "CAPSULE_SESSION")]
    session: String,
    #[arg(long = "lease-ttl-sec", default_value_t = 300)]
    lease_ttl_sec: u64,
    #[arg(long = "base-sha")]
    base_sha: String,
}

#[derive(clap::Args)]
struct HeartbeatArgs {
    capsule_id: String,
    #[arg(long, env = "CAPSULE_SESSION")]
    session: String,
}

#[derive(clap::Args)]
struct AttestArgs {
    capsule_id: String,
    #[arg(long, env = "CAPSULE_SESSION")]
    session: String,
    #[arg(long = "verified-sha")]
    verified_sha: String,
    #[arg(long)]
    command: String,
    /// Either an integer exit code or a sentinel string (e.g. "timeout",
    /// "killed:SIGKILL"). Anything that does not parse as `i32` is treated
    /// as a sentinel verbatim (DESIGN §5).
    #[arg(long = "exit-code")]
    exit_code: String,
    #[arg(long = "duration-ms")]
    duration_ms: u64,
    /// Write-once or content-addressed URI for the verification log.
    #[arg(long = "log-ref")]
    log_ref: String,
    /// Repo to read `git diff --name-only <base_sha>..<verified_sha>` from for
    /// the PROPOSAL §3.2 serialize-paths lint. Defaults to cwd.
    #[arg(long = "repo-dir")]
    repo_dir: Option<PathBuf>,
    /// Bypass the PROPOSAL §3.2 serialize-paths lint. Audit-logged on stderr.
    #[arg(long = "skip-serialize-lint")]
    skip_serialize_lint: bool,
}

#[derive(clap::Args)]
struct LandArgs {
    capsule_id: String,
    #[arg(long, env = "CAPSULE_SESSION")]
    session: String,
    /// Lander principal id. Recorded in PendingLand / Landing / events.
    #[arg(long)]
    lander: String,
    /// Git remote name or URL (e.g. "origin" or a path to a bare repo).
    #[arg(long)]
    remote: String,
    /// Working directory the lander invokes `git push` from. Must have
    /// `verified_sha` in its object database. Defaults to cwd.
    #[arg(long = "repo-dir")]
    repo_dir: Option<PathBuf>,
    /// Bypass the DESIGN §8.2 deploy-verify gate. For tests, development,
    /// and break-glass use only — production should run `capsule deploy
    /// verify` first to record a pass.
    #[arg(long = "skip-deploy-verify-gate")]
    skip_deploy_verify_gate: bool,
}

#[derive(clap::Args)]
struct DeployVerifyArgs {
    /// Run the §8.2 ACL suite against an in-process tempdir bare repo with
    /// the reference `pre-receive.sh` hook. Default mode.
    #[arg(long, conflicts_with = "remote")]
    hermetic: bool,
    /// Run the §8.2 ACL suite against the named git remote. Requires three
    /// per-identity push URLs. Destructive: tests 3, 4b, 7 mutate real refs.
    #[arg(long)]
    remote: Option<String>,
    /// Per-identity push URL: lander principal. Required with `--remote`.
    #[arg(long = "lander-url", requires = "remote")]
    lander_url: Option<String>,
    /// Per-identity push URL: worker principal. Required with `--remote`.
    #[arg(long = "worker-url", requires = "remote")]
    worker_url: Option<String>,
    /// Per-identity push URL: outsider principal. Required with `--remote`.
    #[arg(long = "outsider-url", requires = "remote")]
    outsider_url: Option<String>,
    /// Protected base ref. Default `main`.
    #[arg(long = "base-ref", default_value = "main")]
    base_ref: String,
    /// Confirm `--remote` mutates the configured remote (tests 3 and 4b
    /// advance base_ref; tests 4b/7/8 mutate witness refs). Required for
    /// `--remote`. Use a deploy-validation environment, not production.
    #[arg(long = "remote-allow-mutations", requires = "remote")]
    remote_allow_mutations: bool,
}

#[derive(clap::Args)]
struct ReconcileArgs {
    capsule_id: String,
    #[arg(long)]
    remote: String,
}

#[derive(clap::Args)]
struct ForceUnfreezeArgs {
    capsule_id: String,
    #[arg(long)]
    remote: String,
    /// Operator identity, audited on every emitted incident event.
    #[arg(long)]
    operator: String,
    /// Free-text justification — recorded in `force_unfreeze_invoked`.
    #[arg(long)]
    reason: String,
    /// Operator MUST confirm the lander is dead/unresponsive.
    #[arg(long = "lander-confirmed-dead")]
    lander_confirmed_dead: bool,
}

#[derive(clap::Args)]
struct AbandonArgs {
    capsule_id: String,
    #[arg(long, env = "CAPSULE_SESSION")]
    session: String,
    #[arg(long)]
    reason: String,
}

#[derive(clap::Args)]
struct ReclaimArgs {
    capsule_id: String,
}

#[derive(clap::Args)]
struct DepArgs {
    capsule_id: String,
    #[arg(long = "depends-on")]
    depends_on: String,
}

#[derive(clap::Args)]
struct ListArgs {
    #[arg(long, value_parser = parse_status_arg)]
    status: Option<Status>,
    /// Only capsules claimable right now.
    #[arg(long)]
    available: bool,
    /// Only capsules whose scope overlaps this path.
    #[arg(long = "scope-overlaps")]
    scope_overlaps: Option<String>,
    /// Emit full `Capsule` records in `--json` (default: summary rows).
    #[arg(long)]
    full: bool,
}

fn parse_status_arg(s: &str) -> std::result::Result<Status, String> {
    Status::from_wire(s).ok_or_else(|| format!("unknown status: {s}"))
}

/// Canonicalize a CLI path arg, formatting errors uniformly across flags.
fn parse_canonical_path(flag: &str, s: &str) -> Result<CanonicalPath> {
    CanonicalPath::new(s).with_context(|| format!("invalid --{flag} {s:?}"))
}

/// Canonicalize repeated `--scope` args (used by `create` and `amend`).
fn canonicalize_scope_args(scope: &[String]) -> Result<Vec<CanonicalPath>> {
    scope
        .iter()
        .map(|s| parse_canonical_path("scope", s))
        .collect()
}

fn store_dir(arg: Option<PathBuf>) -> PathBuf {
    arg.unwrap_or_else(|| PathBuf::from(".capsule"))
}

fn open_store(dir: &Path) -> Result<Store> {
    let db = dir.join("state.db");
    Store::open(&db).with_context(|| format!("opening store at {}", db.display()))
}

/// Stdout for fire-and-forget mutations that succeed silently — `--json` emits
/// `{"ok": true}` (the agent-facing ack), bare invocation prints `label`.
/// Used by abandon/add-dep/remove-dep where the only useful signal is "no
/// error"; the actual state change is observable via `status` / `list`.
fn print_ok(json: bool, label: &str) {
    if json {
        println!("{}", serde_json::json!({"ok": true}));
    } else {
        println!("{label}");
    }
}

/// Pretty-print a serde value to stdout. Used for `--json` arms where the
/// payload is a typed core/store struct (capsules, attempts, outcomes, init
/// reports); the inline-`json!` arms (`print_ok`, reclaim's `{reclaimed}`)
/// stay literal because their shape is already trivial. Centralizes the
/// `to_string_pretty` call so a future switch to a stable formatter (e.g.
/// canonical key ordering for diffability) is one edit.
fn print_json<T: serde::Serialize>(v: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

#[derive(serde::Serialize)]
struct CleanupWorktreesReport {
    removed: usize,
    would_remove: usize,
    skipped: usize,
    entries: Vec<worktree::CleanupWorktreeEntry>,
}

impl CleanupWorktreesReport {
    fn new(entries: Vec<worktree::CleanupWorktreeEntry>) -> Self {
        Self {
            removed: count_cleanup_action(&entries, "removed"),
            would_remove: count_cleanup_action(&entries, "would_remove"),
            skipped: count_cleanup_action(&entries, "skipped"),
            entries,
        }
    }
}

fn count_cleanup_action(entries: &[worktree::CleanupWorktreeEntry], action: &str) -> usize {
    entries
        .iter()
        .filter(|entry| entry.action == action)
        .count()
}

fn cleanup_worktrees(
    dir: &Path,
    capsules: &[Capsule],
    force: bool,
    dry_run: bool,
) -> Result<CleanupWorktreesReport> {
    let mut entries = Vec::new();
    for capsule in capsules {
        if !matches!(capsule.status, Status::Landed | Status::Abandoned) {
            continue;
        }
        for attempt in &capsule.attempts {
            if let Some(entry) = worktree::cleanup_default_worktree(
                dir,
                &capsule.id,
                attempt.id,
                &attempt.branch,
                force,
                dry_run,
            )? {
                entries.push(entry);
            }
        }
    }
    Ok(CleanupWorktreesReport::new(entries))
}

fn print_cleanup_worktrees(report: &CleanupWorktreesReport) {
    println!(
        "cleanup-worktrees\tremoved={}\twould_remove={}\tskipped={}",
        report.removed, report.would_remove, report.skipped
    );
    for entry in &report.entries {
        println!(
            "{}\t{}\tattempt={}\tbranch={}\tpath={}\t{}",
            entry.action,
            entry.capsule_id,
            entry.attempt,
            entry.branch,
            entry.path.display(),
            entry.detail
        );
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let cli = Cli::parse();
    let dir = store_dir(cli.dir);

    match cli.cmd {
        Cmd::Init(args) => {
            let report = init::run(init::InitOpts {
                dir,
                no_gitignore: args.no_gitignore,
            })?;
            if cli.json {
                print_json(&report)?;
            } else {
                println!("initialized capsule store at {}", report.dir.display());
                if let Some(p) = &report.gitignore_updated {
                    println!("gitignore: appended rule to {}", p.display());
                } else if let Some(reason) = &report.gitignore_skipped {
                    println!("gitignore: skipped ({reason})");
                }
                for w in &report.warnings {
                    eprintln!("warning: {w}");
                }
                if !report.next_steps.is_empty() {
                    println!("next:");
                    for s in &report.next_steps {
                        println!("  - {s}");
                    }
                }
            }
        }
        Cmd::Doctor => {
            let report = run_doctor(&dir);
            if cli.json {
                print_json(&report)?;
            } else {
                print_doctor(&report);
            }
            if !report.ok {
                std::process::exit(1);
            }
        }
        Cmd::Completions(args) => {
            let mut cmd = Cli::command();
            let bin_name = cmd.get_name().to_string();
            clap_complete::generate(args.shell, &mut cmd, bin_name, &mut std::io::stdout());
        }
        Cmd::CleanupWorktrees(args) => {
            let mut store = open_store(&dir)?;
            let capsules = store.list_capsules(ListFilter {
                status: None,
                available: false,
                scope_overlaps: None,
            })?;
            let report = cleanup_worktrees(&dir, &capsules, args.force, args.dry_run)?;
            if cli.json {
                print_json(&report)?;
            } else {
                print_cleanup_worktrees(&report);
            }
        }
        Cmd::Create(args) => {
            let mut store = open_store(&dir)?;
            let scope_prefixes = canonicalize_scope_args(&args.scope)?;

            let id = args.id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            let capsule = store.create_capsule(NewCapsule {
                id,
                title: args.title,
                description: args.description,
                acceptance: Acceptance {
                    run: args.acceptance_cmd,
                    expect_exit: ExpectExit::Code(args.acceptance_expect_exit),
                    cwd: args.acceptance_cwd,
                    timeout_sec: args.acceptance_timeout_sec,
                },
                scope_prefixes,
                base_ref: args.base_ref,
                depends_on: args.depends_on,
            })?;

            if cli.json {
                print_json(&capsule)?;
            } else {
                println!(
                    "{}\t{}\t{}",
                    capsule.id,
                    capsule.status.as_wire_str(),
                    capsule.title
                );
            }
        }
        Cmd::List(args) => {
            let mut store = open_store(&dir)?;
            let scope_overlaps = args
                .scope_overlaps
                .as_deref()
                .map(|s| parse_canonical_path("scope-overlaps", s))
                .transpose()?;
            let capsules = store.list_capsules(ListFilter {
                status: args.status,
                available: args.available,
                scope_overlaps,
            })?;
            if cli.json {
                if args.full {
                    print_json(&capsules)?;
                } else {
                    print_json(&CapsuleSummaries(&capsules))?;
                }
            } else {
                for c in &capsules {
                    print_capsule_summary_line(c);
                }
            }
        }
        Cmd::Amend(args) => {
            let mut store = open_store(&dir)?;
            let has_acceptance_options = args.acceptance_expect_exit.is_some()
                || args.acceptance_cwd.is_some()
                || args.acceptance_timeout_sec.is_some();
            if args.acceptance_cmd.is_none() && has_acceptance_options {
                anyhow::bail!(
                    "--acceptance-expect-exit/--acceptance-cwd/--acceptance-timeout-sec \
                     require --acceptance-cmd"
                );
            }
            let acceptance = args.acceptance_cmd.map(|run| Acceptance {
                run,
                expect_exit: ExpectExit::Code(args.acceptance_expect_exit.unwrap_or(0)),
                cwd: args.acceptance_cwd,
                timeout_sec: args.acceptance_timeout_sec,
            });
            let scope_prefixes = (!args.scope.is_empty())
                .then(|| canonicalize_scope_args(&args.scope))
                .transpose()?;
            let capsule = store.amend(AmendRequest {
                capsule_id: args.capsule_id,
                title: args.title,
                description: args.description,
                acceptance,
                scope_prefixes,
                base_ref: args.base_ref,
            })?;
            if cli.json {
                print_json(&capsule)?;
            } else {
                println!("amended\t{}\t{}", capsule.id, capsule.title);
            }
        }
        Cmd::Status(args) => {
            let store = open_store(&dir)?;
            let capsule = store.get_capsule(&args.capsule_id)?;
            if cli.json {
                print_json(&capsule)?;
            } else {
                print_status(&capsule);
            }
        }
        Cmd::Claim(args) => {
            let mut store = open_store(&dir)?;
            let attempt = store.claim(ClaimRequest {
                capsule_id: args.capsule_id,
                owner: args.owner,
                session_id: args.session,
                lease_ttl_sec: args.lease_ttl_sec,
                base_sha: args.base_sha,
            })?;
            if cli.json {
                print_json(&attempt)?;
            } else {
                println!(
                    "claimed\tsession={}\tattempt={}\tbranch={}\twitness={}\tlease_expires={}",
                    attempt.lease.session_id,
                    attempt.id,
                    attempt.branch,
                    attempt.witness_branch,
                    fmt_ts(attempt.lease.expires_at)
                );
                println!(
                    "hint: export CAPSULE_SESSION={} to omit --session on later calls",
                    attempt.lease.session_id
                );
            }
        }
        Cmd::Work(args) => {
            let code = run_work(&dir, args)?;
            std::process::exit(code);
        }
        Cmd::Heartbeat(args) => {
            let mut store = open_store(&dir)?;
            let ack = store.heartbeat(&args.capsule_id, &args.session)?;
            if cli.json {
                print_json(&ack)?;
            } else {
                println!("lease_expires={}", fmt_ts(ack.lease_expires_at));
            }
        }
        Cmd::Attest(args) => {
            let mut store = open_store(&dir)?;
            if args.skip_serialize_lint {
                eprintln!(
                    "warning: --skip-serialize-lint set; PROPOSAL §3.2 lockfile lint bypassed"
                );
            } else {
                run_serialize_lint(
                    &dir,
                    &store,
                    &args.capsule_id,
                    &args.session,
                    &args.verified_sha,
                    args.repo_dir.as_deref(),
                )?;
            }
            let ack = store.attest(AttestRequest {
                capsule_id: args.capsule_id,
                session_id: args.session,
                verified_sha: args.verified_sha,
                command: args.command,
                exit_code: args.exit_code.into(),
                duration_ms: args.duration_ms,
                log_ref: args.log_ref,
            })?;
            if cli.json {
                print_json(&ack)?;
            } else {
                println!(
                    "attested\taccepted={}\tstatus={}",
                    ack.accepted,
                    ack.new_status.as_wire_str()
                );
            }
        }
        Cmd::Land(args) => {
            let mut store = open_store(&dir)?;
            let repo_dir = args
                .repo_dir
                .map_or_else(std::env::current_dir, Ok)
                .context("resolving --repo-dir / cwd")?;
            let ack = store.land(LandRequest {
                capsule_id: args.capsule_id,
                session_id: args.session,
                lander: args.lander,
                remote: args.remote,
                repo_dir,
                skip_deploy_verify_gate: args.skip_deploy_verify_gate,
            })?;
            if cli.json {
                print_json(&ack)?;
            } else {
                match &ack.outcome {
                    capsule_store::LandOutcome::Landed { landing } => println!(
                        "landed\tsha={}\tprior={}\tadvanced={}",
                        landing.landed_sha, landing.prior_base_sha, landing.advanced_base_ref
                    ),
                    capsule_store::LandOutcome::BaseRefMoved => {
                        println!("base_ref_moved\tcapsule stays accepted; rebase + re-attest");
                    }
                    capsule_store::LandOutcome::WitnessOidMismatch => {
                        println!("witness_oid_mismatch\tcapsule abandoned; investigate");
                    }
                }
            }
        }
        Cmd::Abandon(args) => {
            let mut store = open_store(&dir)?;
            store.abandon(AbandonRequest {
                capsule_id: args.capsule_id,
                session_id: args.session,
                reason: args.reason,
            })?;
            print_ok(cli.json, "abandoned");
        }
        Cmd::Reclaim(args) => {
            let mut store = open_store(&dir)?;
            let reclaimed = store.reclaim(&args.capsule_id)?;
            if cli.json {
                println!("{}", serde_json::json!({"reclaimed": reclaimed}));
            } else {
                println!("{}", if reclaimed { "reclaimed" } else { "no-op" });
            }
        }
        Cmd::AddDep(args) => {
            let mut store = open_store(&dir)?;
            store.add_dep(DepRequest {
                capsule_id: args.capsule_id,
                depends_on: args.depends_on,
            })?;
            print_ok(cli.json, "dep-added");
        }
        Cmd::RemoveDep(args) => {
            let mut store = open_store(&dir)?;
            store.remove_dep(DepRequest {
                capsule_id: args.capsule_id,
                depends_on: args.depends_on,
            })?;
            print_ok(cli.json, "dep-removed");
        }
        Cmd::Reconcile(args) => {
            let mut store = open_store(&dir)?;
            let outcome = store.reconcile(ReconcileRequest {
                capsule_id: args.capsule_id,
                remote: args.remote,
            })?;
            let wire = outcome.as_wire_str();
            if cli.json {
                print_json(&wire)?;
            } else {
                println!("reconcile\toutcome={wire}");
            }
        }
        Cmd::ForceUnfreeze(args) => {
            let mut store = open_store(&dir)?;
            let outcome = store.force_unfreeze(ForceUnfreezeRequest {
                capsule_id: args.capsule_id,
                remote: args.remote,
                operator: args.operator,
                reason: args.reason,
                lander_confirmed_dead: args.lander_confirmed_dead,
            })?;
            let wire = outcome.as_wire_str();
            if cli.json {
                print_json(&wire)?;
            } else {
                println!("force-unfreeze\toutcome={wire}");
            }
        }
        Cmd::DeployVerify(args) => {
            let mode = match (
                args.remote,
                args.lander_url,
                args.worker_url,
                args.outsider_url,
            ) {
                (Some(remote), Some(lander_url), Some(worker_url), Some(outsider_url)) => {
                    if !args.remote_allow_mutations {
                        anyhow::bail!(
                            "--remote mutates the configured remote — pass \
                             --remote-allow-mutations to acknowledge"
                        );
                    }
                    deploy_verify::Mode::Remote {
                        remote,
                        lander_url,
                        worker_url,
                        outsider_url,
                    }
                }
                (Some(_), _, _, _) => {
                    anyhow::bail!("--remote requires --lander-url, --worker-url, --outsider-url")
                }
                _ => deploy_verify::Mode::Hermetic,
            };
            // Materialize the store *before* running the suite so we fail fast
            // if the store dir is missing — and so we can record the pass row
            // without re-opening.
            let mut store = open_store(&dir)?;
            let report = deploy_verify::run(deploy_verify::Opts {
                mode,
                base_ref: args.base_ref,
                json: cli.json,
            })?;
            if report.all_passed {
                store
                    .record_deploy_verify_pass(&report.mode_label, &report.base_ref)
                    .context("recording deploy verify pass")?;
            } else {
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct DoctorReport {
    ok: bool,
    checks: Vec<DoctorCheck>,
}

#[derive(serde::Serialize)]
struct DoctorCheck {
    name: &'static str,
    status: &'static str,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

impl DoctorReport {
    fn new(checks: Vec<DoctorCheck>) -> Self {
        Self {
            ok: !checks.iter().any(|c| c.status == "fail"),
            checks,
        }
    }
}

fn doctor_check(
    name: &'static str,
    status: &'static str,
    detail: impl Into<String>,
    hint: Option<impl Into<String>>,
) -> DoctorCheck {
    DoctorCheck {
        name,
        status,
        detail: detail.into(),
        hint: hint.map(Into::into),
    }
}

fn run_doctor(dir: &Path) -> DoctorReport {
    let mut checks = Vec::new();

    match std::process::Command::new("git").arg("--version").output() {
        Ok(out) if out.status.success() => checks.push(doctor_check(
            "git",
            "ok",
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
            None::<String>,
        )),
        Ok(out) => checks.push(doctor_check(
            "git",
            "fail",
            format!(
                "git --version exited {}: {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ),
            Some("install git and ensure it is on PATH"),
        )),
        Err(e) => checks.push(doctor_check(
            "git",
            "fail",
            format!("could not run git --version: {e}"),
            Some("install git and ensure it is on PATH"),
        )),
    }

    match std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
    {
        Ok(out) if out.status.success() => checks.push(doctor_check(
            "worktree",
            "ok",
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
            None::<String>,
        )),
        _ => checks.push(doctor_check(
            "worktree",
            "warn",
            "not running inside a git worktree",
            Some("run capsule from a git repository for claim/attest/land workflows"),
        )),
    }

    match config::load(dir).and_then(|cfg| config::canonicalize_required(&cfg).map(|r| r.len())) {
        Ok(count) => checks.push(doctor_check(
            "config",
            "ok",
            format!("{count} serialize_paths.required entries"),
            None::<String>,
        )),
        Err(e) => checks.push(doctor_check(
            "config",
            "fail",
            e.to_string(),
            Some("fix config.toml or remove it to use defaults"),
        )),
    }

    let db = dir.join("state.db");
    let store = if db.exists() {
        match Store::open(&db) {
            Ok(store) => {
                checks.push(doctor_check(
                    "store",
                    "ok",
                    db.display().to_string(),
                    None::<String>,
                ));
                Some(store)
            }
            Err(e) => {
                checks.push(doctor_check(
                    "store",
                    "fail",
                    format!("{}: {e}", db.display()),
                    Some("restore the store from backup or re-run capsule init"),
                ));
                None
            }
        }
    } else {
        checks.push(doctor_check(
            "store",
            "fail",
            format!("{} does not exist", db.display()),
            Some("run capsule init"),
        ));
        None
    };

    if let Some(store) = store {
        match store.check_deploy_verify_pass() {
            Ok(true) => checks.push(doctor_check(
                "deploy_verify",
                "ok",
                "recorded pass row present",
                None::<String>,
            )),
            Ok(false) => checks.push(doctor_check(
                "deploy_verify",
                "warn",
                "no recorded pass row",
                Some("run capsule deploy-verify --hermetic or remote deploy verification"),
            )),
            Err(e) => checks.push(doctor_check(
                "deploy_verify",
                "fail",
                e.to_string(),
                Some("inspect the capsule store"),
            )),
        }
    }

    DoctorReport::new(checks)
}

fn print_doctor(report: &DoctorReport) {
    println!("doctor\t{}", if report.ok { "ok" } else { "fail" });
    for check in &report.checks {
        println!("{}\t{}\t{}", check.status, check.name, check.detail);
        if let Some(hint) = &check.hint {
            println!("hint\t{}\t{}", check.name, hint);
        }
    }
}

struct SessionAttempt {
    /// Raw lease TTL; heartbeat cadence is floored in `spawn_heartbeat_thread`.
    ttl: u64,
    branch: String,
    base_sha: String,
    num: u64,
}

/// Pre-flight for `capsule work`: confirm there is an active attempt for the
/// given session and surface the fields the work loop needs (ttl, branch,
/// base sha, attempt num). Distinguishes "no active attempt" (claimable, hint
/// the user to `capsule claim`) from "`active_attempt` points at a row that
/// does not exist" (state-shape violation, unreachable in well-formed state
/// but `claim` can't repair it — so "run claim" would be actively misleading).
fn load_session_attempt(dir: &Path, capsule_id: &str, session: &str) -> Result<SessionAttempt> {
    let store = open_store(dir)?;
    let capsule = store.get_capsule(capsule_id)?;
    let active_attempt_id = capsule.active_attempt;
    let attempt = capsule
        .into_active_attempt()
        .ok_or_else(|| match active_attempt_id {
            Some(aid) => {
                anyhow::anyhow!("active_attempt {aid} not found in attempts (corrupt state)")
            }
            None => anyhow::anyhow!("capsule has no active attempt; run `capsule claim`"),
        })?;
    if attempt.lease.session_id != session {
        anyhow::bail!(
            "session mismatch: active attempt session is {}",
            attempt.lease.session_id
        );
    }
    Ok(SessionAttempt {
        ttl: attempt.lease.ttl_sec,
        branch: attempt.branch,
        base_sha: attempt.base_sha,
        num: attempt.id,
    })
}

/// One synchronous heartbeat before the child spawns: fails fast if the lease
/// already expired or drifted to another session (worktree setup may have
/// consumed TTL), and refreshes expiry so the heartbeat thread's first tick
/// has full headroom.
fn pre_spawn_heartbeat(dir: &Path, capsule_id: &str, session: &str) -> Result<()> {
    let mut store = open_store(dir)?;
    store
        .heartbeat(capsule_id, session)
        .context("pre-spawn heartbeat (lease lost before child started)")?;
    Ok(())
}

/// Handle for the spawned heartbeat thread. Call `shutdown` to wake the thread
/// (drops `stop`, the sender side of its `recv_timeout` channel), join it, and
/// read whether the lease was lost during the run.
struct HeartbeatThread {
    handle: std::thread::JoinHandle<Result<()>>,
    stop: std::sync::mpsc::Sender<()>,
    lease_lost: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl HeartbeatThread {
    fn shutdown(self) -> bool {
        let Self {
            handle,
            stop,
            lease_lost,
        } = self;
        drop(stop);
        let _ = handle.join();
        lease_lost.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Spawn the heartbeat-thread that pings the lease at `ttl/3` cadence on a
/// fresh SQLite connection (WAL makes same-process dual connections safe).
///
/// - **Cadence floor.** Clamps `ttl/3` at 1s. Tests use tiny TTLs (e.g. ttl=1)
///   and agents fronting `capsule work` may pass anything; without the floor
///   `recv_timeout(0)` returns immediately and heartbeat hammers the DB.
///   Defend at the consumer, not by rejecting at claim.
/// - **Shutdown.** The parent drops `stop`; the thread's `recv_timeout` then
///   returns `Disconnected` and exits cleanly. Sleep-first cadence avoids
///   double-heartbeating immediately after the pre-spawn ping. No site sends
///   on the channel, so `Ok(())` from `recv_timeout` is unreachable by
///   construction — the `unreachable!` surfaces a contract break if a future
///   contributor adds `stop.send(())`.
/// - **Lease loss.** `CrossSession` / `LeaseExpired` set `lease_lost` and
///   exit; the child is **not** killed. The parent reads the flag after
///   `join` and forces a non-zero exit. `AtomicBool` (not a second channel)
///   because the parent reads after `join`.
fn spawn_heartbeat_thread(
    dir: &Path,
    capsule_id: String,
    session: String,
    ttl: u64,
) -> HeartbeatThread {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    let (stop, stop_rx) = mpsc::channel::<()>();
    let lease_lost = Arc::new(AtomicBool::new(false));
    let dir = dir.to_path_buf();
    let lease_lost_hb = Arc::clone(&lease_lost);

    let interval = Duration::from_secs((ttl / 3).max(1));
    let handle = thread::spawn(move || -> Result<()> {
        let mut store = open_store(&dir)?;
        loop {
            match stop_rx.recv_timeout(interval) {
                Ok(()) => unreachable!(
                    "heartbeat shutdown is signaled by dropping the sender, not sending"
                ),
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
                Err(RecvTimeoutError::Timeout) => {}
            }
            match store.heartbeat(&capsule_id, &session) {
                Ok(_) => {}
                Err(
                    e @ (capsule_store::StoreError::CrossSession
                    | capsule_store::StoreError::LeaseExpired(_)),
                ) => {
                    eprintln!(
                        "capsule work: lease lost ({e}); attest will fail. Finish or cancel \
                         the child and re-claim."
                    );
                    lease_lost_hb.store(true, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("capsule work: heartbeat failed: {e}");
                    return Ok(());
                }
            }
        }
    });

    HeartbeatThread {
        handle,
        stop,
        lease_lost,
    }
}

/// `capsule work`: spawn child, heartbeat in a thread at `ttl/3` cadence on a
/// second SQLite connection (WAL makes same-process dual connections safe), and
/// forward the child's exit code. No custom signal handlers — terminal signals
/// reach the child through process-group propagation; the heartbeat thread
/// shuts down via `HeartbeatThread::shutdown` (its `recv_timeout` returns
/// `Disconnected` once the sender is dropped).
fn run_work(dir: &Path, args: WorkArgs) -> Result<i32> {
    let SessionAttempt {
        ttl,
        branch: attempt_branch,
        base_sha: attempt_base_sha,
        num: attempt_num,
    } = load_session_attempt(dir, &args.capsule_id, &args.session)?;

    let isolate_state = if args.isolate == IsolateMode::Worktree {
        Some(worktree::setup(
            dir,
            &args.capsule_id,
            &attempt_branch,
            &attempt_base_sha,
            attempt_num,
            args.worktree_dir.as_deref(),
        )?)
    } else {
        None
    };

    pre_spawn_heartbeat(dir, &args.capsule_id, &args.session)?;

    let hb = spawn_heartbeat_thread(dir, args.capsule_id.clone(), args.session.clone(), ttl);

    let (first, rest) = args.cmd.split_first().expect("clap required >= 1 arg");
    let mut command = std::process::Command::new(first);
    command.args(rest);
    if let Some(s) = &isolate_state {
        command
            .current_dir(&s.worktree_path)
            .env("CAPSULE_DIR", &s.canonical_capsule_dir)
            .env("CAPSULE_ID", &args.capsule_id)
            .env("CAPSULE_SESSION", &args.session);
    }
    let status = command.status();

    let lease_lost = hb.shutdown();
    drop(isolate_state);

    let s = status.with_context(|| format!("spawning {first}"))?;
    let code = s.code().unwrap_or_else(|| {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            s.signal().map_or(1, |sig| 128 + sig)
        }
        #[cfg(not(unix))]
        {
            1
        }
    });
    Ok(if lease_lost && code == 0 { 1 } else { code })
}

/// PROPOSAL §3.2: pre-attest gate that rejects when a touched lockfile is not
/// covered by the capsule's declared scope. Lives CLI-side because DESIGN
/// §7.1.0 keeps `Store::attest` git-free; the lint shells out to
/// `git diff --name-only`. Findings exit 2 unless the user passes
/// `--skip-serialize-lint`.
fn run_serialize_lint(
    dir: &Path,
    store: &Store,
    capsule_id: &str,
    session: &str,
    verified_sha: &str,
    repo_dir_arg: Option<&Path>,
) -> Result<()> {
    let capsule = store.get_capsule(capsule_id)?;
    let scope_prefixes = capsule.scope_prefixes.clone();
    let active_attempt_id = capsule.active_attempt;
    let attempt = capsule
        .into_active_attempt()
        .ok_or_else(|| match active_attempt_id {
            Some(aid) => {
                anyhow::anyhow!("active_attempt {aid} not found in attempts (corrupt state)")
            }
            None => anyhow::anyhow!("capsule has no active attempt; run `capsule claim`"),
        })?;
    if attempt.lease.session_id != session {
        anyhow::bail!(
            "session mismatch: active attempt session is {}",
            attempt.lease.session_id
        );
    }
    let base_sha = attempt.base_sha;

    let cfg = config::load(dir)?;
    let required = config::canonicalize_required(&cfg)?;
    if required.is_empty() {
        return Ok(());
    }
    let repo_dir = match repo_dir_arg {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("resolving --repo-dir / cwd")?,
    };
    let exists = std::process::Command::new("git")
        .args(["cat-file", "-e", verified_sha])
        .current_dir(&repo_dir)
        .status()
        .with_context(|| format!("invoking git in {}", repo_dir.display()))?;
    if !exists.success() {
        anyhow::bail!(
            "verified_sha {verified_sha} not found in {}; ensure the worker has pushed and \
             this repo has fetched it before attest, or pass --skip-serialize-lint",
            repo_dir.display()
        );
    }
    let findings = serialize_lint::check_attest_diff(
        &repo_dir,
        &base_sha,
        verified_sha,
        &scope_prefixes,
        &required,
    )?;
    if findings.is_empty() {
        return Ok(());
    }
    for f in &findings {
        eprintln!(
            "serialize_path_uncovered: {} (matched required {})",
            f.path, f.matched_required
        );
    }
    eprintln!(
        "hint: amend the capsule with the listed paths in --scope, or pass \
         --skip-serialize-lint to bypass."
    );
    std::process::exit(2);
}

/// `Display` adapter for the comma-joined scope-list rendering used inside
/// `print_capsule_summary_line`'s `[...]` cell. Formats straight into the
/// `println!` buffer — no intermediate `String` per row.
struct ScopeList<'a>(&'a [CanonicalPath]);

impl std::fmt::Display for ScopeList<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, p) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            f.write_str(p.as_str())?;
        }
        Ok(())
    }
}

/// One-line tab-separated summary used by both `list` (one row per capsule)
/// and `status` (header for the per-capsule detail block). Textual sibling
/// of `CapsuleSummary` (the `--json` shape); the two formats serve different
/// consumers and are not lockstep, but the field set deliberately matches
/// — keep them in sync when adding columns.
fn print_capsule_summary_line(c: &Capsule) {
    println!(
        "{}\t{}\t{}\t[{}]\t{}",
        c.id,
        c.status.as_wire_str(),
        c.base_ref,
        ScopeList(&c.scope_prefixes),
        c.title
    );
}

fn print_status(c: &Capsule) {
    print_capsule_summary_line(c);
    if !c.depends_on.is_empty() {
        println!("  depends_on: {}", c.depends_on.join(", "));
    }
    for a in &c.attempts {
        println!(
            "  attempt {}: {}\tsession={}\tbranch={}\tlease_expires={}",
            a.id,
            a.outcome.as_wire_str(),
            a.lease.session_id,
            a.branch,
            fmt_ts(a.lease.expires_at)
        );
    }
    if let Some(v) = &c.verification {
        println!(
            "  verification: exit={}\tverified_sha={}\tdur={}ms",
            v.exit_code, v.verified_sha, v.duration_ms
        );
    }
    if let Some(p) = &c.pending_land {
        println!(
            "  pending_land: lander={}\tat={}\tverified_sha={}",
            p.lander,
            fmt_ts(p.at),
            p.verified_sha
        );
    }
    if let Some(l) = &c.landing {
        println!(
            "  landing: landed_sha={}\tby={}\tat={}\tadvanced={}",
            l.landed_sha,
            l.landed_by,
            fmt_ts(l.at),
            l.advanced_base_ref
        );
    }
}

fn fmt_ts(t: time::OffsetDateTime) -> String {
    t.format(&Rfc3339).unwrap_or_else(|_| t.to_string())
}

#[derive(serde::Serialize)]
struct CapsuleSummary<'a> {
    id: &'a str,
    status: Status,
    base_ref: &'a str,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    scope_prefixes: &'a [CanonicalPath],
    title: &'a str,
}

impl<'a> From<&'a Capsule> for CapsuleSummary<'a> {
    fn from(c: &'a Capsule) -> Self {
        Self {
            id: &c.id,
            status: c.status,
            base_ref: &c.base_ref,
            scope_prefixes: &c.scope_prefixes,
            title: &c.title,
        }
    }
}

/// `Serialize` adapter for the `--json` array shape of `capsule list` (default,
/// non-`--full`). Builds each `CapsuleSummary` on the fly during serialization
/// so the slice doesn't need to be re-collected into a `Vec<CapsuleSummary<'_>>`
/// per call. Output is byte-identical to a `Vec`-backed serialization.
struct CapsuleSummaries<'a>(&'a [Capsule]);

impl serde::Serialize for CapsuleSummaries<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for c in self.0 {
            seq.serialize_element(&CapsuleSummary::from(c))?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ScopeList` is interpolated between literal `[` and `]` in
    /// `print_capsule_summary_line`. Pin separator (multi), no-comma at
    /// length 1, and `[]` (not `[,]`) at length 0.
    #[test]
    fn scope_list_display_comma_joined_no_trailing() {
        let p1 = CanonicalPath::new("src/foo").unwrap();
        let p2 = CanonicalPath::new("docs").unwrap();
        assert_eq!(format!("{}", ScopeList(&[])), "");
        assert_eq!(
            format!("{}", ScopeList(std::slice::from_ref(&p1))),
            "src/foo"
        );
        assert_eq!(format!("{}", ScopeList(&[p1, p2])), "src/foo,docs");
    }
}
