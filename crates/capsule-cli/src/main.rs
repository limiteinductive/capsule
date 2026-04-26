use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, Capsule, ExpectExit, Status};
use capsule_store::{
    self, AbandonRequest, AmendRequest, AttestRequest, ClaimRequest, DepRequest,
    ForceUnfreezeRequest, LandRequest, ListFilter, NewCapsule, ReconcileRequest,
    Store,
};
use clap::{Parser, Subcommand};
use time::format_description::well_known::Rfc3339;

mod init;
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
    /// Run the deployment ACL test suite. [unimplemented]
    DeployVerify,
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
    CanonicalPath::new(s).map_err(|e| anyhow::anyhow!("invalid --{flag} {s:?}: {e}"))
}

/// Canonicalize repeated `--scope` args (used by `create` and `amend`).
fn canonicalize_scope_args(scope: &[String]) -> Result<Vec<CanonicalPath>> {
    scope.iter().map(|s| parse_canonical_path("scope", s)).collect()
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
                    let summaries: Vec<CapsuleSummary<'_>> =
                        capsules.iter().map(CapsuleSummary::from).collect();
                    print_json(&summaries)?;
                }
            } else {
                for c in &capsules {
                    print_capsule_summary_line(c);
                }
            }
        }
        Cmd::Amend(args) => {
            let mut store = open_store(&dir)?;
            let acceptance = if let Some(run) = args.acceptance_cmd {
                Some(Acceptance {
                    run,
                    expect_exit: ExpectExit::Code(args.acceptance_expect_exit.unwrap_or(0)),
                    cwd: args.acceptance_cwd,
                    timeout_sec: args.acceptance_timeout_sec,
                })
            } else {
                if args.acceptance_expect_exit.is_some()
                    || args.acceptance_cwd.is_some()
                    || args.acceptance_timeout_sec.is_some()
                {
                    anyhow::bail!(
                        "--acceptance-expect-exit/--acceptance-cwd/--acceptance-timeout-sec \
                         require --acceptance-cmd"
                    );
                }
                None
            };
            let scope_prefixes = if args.scope.is_empty() {
                None
            } else {
                Some(canonicalize_scope_args(&args.scope)?)
            };
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
            } else if reclaimed {
                println!("reclaimed");
            } else {
                println!("no-op");
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
            if cli.json {
                print_json(&outcome.as_wire_str())?;
            } else {
                println!("reconcile\toutcome={}", outcome.as_wire_str());
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
            if cli.json {
                print_json(&outcome.as_wire_str())?;
            } else {
                println!("force-unfreeze\toutcome={}", outcome.as_wire_str());
            }
        }
        Cmd::DeployVerify => {
            anyhow::bail!("not yet implemented")
        }
    }
    Ok(())
}

/// `capsule work`: spawn child, heartbeat in a thread at `ttl/3` cadence on a
/// second SQLite connection (WAL makes same-process dual connections safe), and
/// forward the child's exit code. No custom signal handlers — terminal signals
/// reach the child through process-group propagation; the heartbeat thread
/// shuts down when the parent drops `stop_tx` (the `recv_timeout` returns
/// `Disconnected` immediately).
fn run_work(dir: &Path, args: WorkArgs) -> Result<i32> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // Pre-flight: confirm active attempt exists for this session, read ttl.
    let pre = open_store(dir)?;
    let capsule = pre.get_capsule(&args.capsule_id)?;
    let active_attempt_id = capsule.active_attempt;
    let attempt = capsule.into_active_attempt().ok_or_else(|| {
        // Distinguish "no active attempt" (claimable) from "active_attempt
        // points at a row that does not exist" (corrupt state). The latter
        // is unreachable in well-formed state, but `claim` cannot repair it,
        // so a "run claim" hint would be actively misleading.
        if let Some(aid) = active_attempt_id {
            anyhow::anyhow!("active_attempt {aid} not found in attempts (corrupt state)")
        } else {
            anyhow::anyhow!("capsule has no active attempt; run `capsule claim`")
        }
    })?;
    if attempt.lease.session_id != args.session {
        anyhow::bail!(
            "session mismatch: active attempt session is {}",
            attempt.lease.session_id
        );
    }
    let ttl = attempt.lease.ttl_sec.max(3);
    let attempt_branch = attempt.branch;
    let attempt_base_sha = attempt.base_sha;
    let attempt_num = attempt.id;
    drop(pre);

    // Materialize worktree if requested. Held for the child's lifetime via
    // `_isolate` (drops the runtime flock on scope exit).
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

    // Heartbeat once before spawning the child: fails fast on already-expired
    // or cross-session leases (worktree setup may have consumed TTL), and
    // refreshes expiry so the first thread tick has full headroom.
    {
        let mut hb = open_store(dir)?;
        hb.heartbeat(&args.capsule_id, &args.session)
            .context("pre-spawn heartbeat (lease lost before child started)")?;
    }

    // Shutdown signaled by dropping `stop_tx` in the parent: the heartbeat
    // thread's `recv_timeout` returns `Disconnected` immediately, eliminating
    // the prior 200ms polling tick (and shaving up to that much off shutdown
    // latency on child exit).
    let (stop_tx, stop_rx) = mpsc::channel::<()>();
    // F8: lease lost mid-run → flag set, child not killed, parent exits non-zero.
    // Stays an `AtomicBool` because the parent reads it after `join`; the channel
    // only carries the shutdown edge.
    let lease_lost = Arc::new(AtomicBool::new(false));
    let dir_hb = dir.to_path_buf();
    let capsule_id_hb = args.capsule_id.clone();
    let session_hb = args.session.clone();
    let lease_lost_hb = Arc::clone(&lease_lost);

    let hb_thread = thread::spawn(move || -> Result<()> {
        let mut store = open_store(&dir_hb)?;
        // Clamp at 1s. With ttl < 3, naive `ttl/3` is 0 → the recv_timeout
        // returns immediately → heartbeat fires in a tight loop hammering the
        // DB. Tests use tiny TTLs (e.g. ttl=1) and an agent fronting `capsule
        // work` may pass any value; defend at the consumer rather than
        // rejecting at claim.
        let interval = Duration::from_secs((ttl / 3).max(1));
        loop {
            // Sleep first so we don't double-heartbeat immediately after claim.
            // Drop of the sender is the canonical shutdown signal; no site
            // sends, so `Ok(())` is unreachable by construction. If a future
            // contributor adds `stop_tx.send(())`, the `unreachable!` will
            // surface that the control contract changed.
            match stop_rx.recv_timeout(interval) {
                Ok(()) => unreachable!(
                    "heartbeat shutdown is signaled by dropping the sender, not sending"
                ),
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
                Err(RecvTimeoutError::Timeout) => {}
            }
            match store.heartbeat(&capsule_id_hb, &session_hb) {
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

    drop(stop_tx); // signals heartbeat thread to wake immediately
    let _ = hb_thread.join();
    drop(isolate_state); // release runtime flock

    match status {
        Ok(s) => {
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
            // F8: if lease was lost mid-run, force non-zero even on child success.
            if lease_lost.load(Ordering::SeqCst) && code == 0 {
                Ok(1)
            } else {
                Ok(code)
            }
        }
        Err(e) => Err(anyhow::anyhow!("spawning {first}: {e}")),
    }
}

fn join_scope(prefixes: &[CanonicalPath]) -> String {
    prefixes
        .iter()
        .map(CanonicalPath::as_str)
        .collect::<Vec<_>>()
        .join(",")
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
        join_scope(&c.scope_prefixes),
        c.title
    );
}

fn print_status(c: &Capsule) {
    print_capsule_summary_line(c);
    if !c.depends_on.is_empty() {
        println!("  depends_on: {}", c.depends_on.join(", "));
    }
    for (i, a) in c.attempts.iter().enumerate() {
        println!(
            "  attempt {}: {}\tsession={}\tbranch={}\tlease_expires={}",
            i + 1,
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    scope_prefixes: Vec<&'a str>,
    title: &'a str,
}

impl<'a> From<&'a Capsule> for CapsuleSummary<'a> {
    fn from(c: &'a Capsule) -> Self {
        Self {
            id: &c.id,
            status: c.status,
            base_ref: &c.base_ref,
            scope_prefixes: c.scope_prefixes.iter().map(|p| p.as_str()).collect(),
            title: &c.title,
        }
    }
}
