use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, Capsule, ExpectExit, Status};
use capsule_store::{
    AbandonRequest, AmendRequest, AttestRequest, ClaimRequest, DepRequest, ForceUnfreezeRequest,
    HeartbeatRequest, LandRequest, ListFilter, NewCapsule, ReconcileRequest, Store,
};
use clap::{Parser, Subcommand};
use time::format_description::well_known::Rfc3339;

mod init;

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
    /// Command + args after `--`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
    cmd: Vec<String>,
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
    /// Either an integer or the literal string "timeout".
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
    match s {
        "planned" => Ok(Status::Planned),
        "active" => Ok(Status::Active),
        "accepted" => Ok(Status::Accepted),
        "landed" => Ok(Status::Landed),
        "abandoned" => Ok(Status::Abandoned),
        other => Err(format!("unknown status: {other}")),
    }
}

fn store_dir(arg: Option<PathBuf>) -> PathBuf {
    arg.unwrap_or_else(|| PathBuf::from(".capsule"))
}

fn open_store(dir: &Path) -> Result<Store> {
    let db = dir.join("state.db");
    Store::open(&db).with_context(|| format!("opening store at {}", db.display()))
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
                dir: dir.clone(),
                no_gitignore: args.no_gitignore,
            })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
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
            let scope_prefixes = args
                .scope
                .iter()
                .map(|s| {
                    CanonicalPath::new(s).map_err(|e| anyhow::anyhow!("invalid --scope {s:?}: {e}"))
                })
                .collect::<Result<Vec<_>>>()?;

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
                println!("{}", serde_json::to_string_pretty(&capsule)?);
            } else {
                println!(
                    "{}\t{}\t{}",
                    capsule.id,
                    status_str(capsule.status),
                    capsule.title
                );
            }
        }
        Cmd::List(args) => {
            let mut store = open_store(&dir)?;
            let scope_overlaps = args
                .scope_overlaps
                .as_deref()
                .map(|s| {
                    CanonicalPath::new(s)
                        .map_err(|e| anyhow::anyhow!("invalid --scope-overlaps {s:?}: {e}"))
                })
                .transpose()?;
            let capsules = store.list_capsules(ListFilter {
                status: args.status,
                available: args.available,
                scope_overlaps,
            })?;
            if cli.json {
                if args.full {
                    println!("{}", serde_json::to_string_pretty(&capsules)?);
                } else {
                    let summaries: Vec<CapsuleSummary<'_>> =
                        capsules.iter().map(CapsuleSummary::from).collect();
                    println!("{}", serde_json::to_string_pretty(&summaries)?);
                }
            } else {
                for c in capsules {
                    let scope = c
                        .scope_prefixes
                        .iter()
                        .map(|p| p.as_str())
                        .collect::<Vec<_>>()
                        .join(",");
                    println!(
                        "{}\t{}\t{}\t[{}]\t{}",
                        c.id,
                        status_str(c.status),
                        c.base_ref,
                        scope,
                        c.title
                    );
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
                let v = args
                    .scope
                    .iter()
                    .map(|s| {
                        CanonicalPath::new(s)
                            .map_err(|e| anyhow::anyhow!("invalid --scope {s:?}: {e}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Some(v)
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
                println!("{}", serde_json::to_string_pretty(&capsule)?);
            } else {
                println!("amended\t{}\t{}", capsule.id, capsule.title);
            }
        }
        Cmd::Status(args) => {
            let store = open_store(&dir)?;
            let capsule = store.get_capsule(&args.capsule_id)?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&capsule)?);
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
                println!("{}", serde_json::to_string_pretty(&attempt)?);
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
            let ack = store.heartbeat(HeartbeatRequest {
                capsule_id: args.capsule_id,
                session_id: args.session,
            })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&ack)?);
            } else {
                println!("lease_expires={}", fmt_ts(ack.lease_expires_at));
            }
        }
        Cmd::Attest(args) => {
            let mut store = open_store(&dir)?;
            let exit_code = match args.exit_code.parse::<i32>() {
                Ok(n) => capsule_core::ExitCode::Code(n),
                Err(_) => capsule_core::ExitCode::Sentinel(args.exit_code),
            };
            let ack = store.attest(AttestRequest {
                capsule_id: args.capsule_id,
                session_id: args.session,
                verified_sha: args.verified_sha,
                command: args.command,
                exit_code,
                duration_ms: args.duration_ms,
                log_ref: args.log_ref,
            })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&ack)?);
            } else {
                println!(
                    "attested\taccepted={}\tstatus={}",
                    ack.accepted,
                    status_str(ack.new_status)
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
                println!("{}", serde_json::to_string_pretty(&ack)?);
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
            if cli.json {
                println!("{}", serde_json::json!({"ok": true}));
            } else {
                println!("abandoned");
            }
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
            if cli.json {
                println!("{}", serde_json::json!({"ok": true}));
            } else {
                println!("dep-added");
            }
        }
        Cmd::RemoveDep(args) => {
            let mut store = open_store(&dir)?;
            store.remove_dep(DepRequest {
                capsule_id: args.capsule_id,
                depends_on: args.depends_on,
            })?;
            if cli.json {
                println!("{}", serde_json::json!({"ok": true}));
            } else {
                println!("dep-removed");
            }
        }
        Cmd::Reconcile(args) => {
            let mut store = open_store(&dir)?;
            let outcome = store.reconcile(ReconcileRequest {
                capsule_id: args.capsule_id,
                remote: args.remote,
            })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&outcome)?);
            } else {
                println!("reconcile\toutcome={outcome:?}");
            }
        }
        Cmd::ForceUnfreeze(args) => {
            let mut store = open_store(&dir)?;
            let outcome = store.force_unfreeze(ForceUnfreezeRequest {
                capsule_id: args.capsule_id,
                remote: args.remote,
                operator: args.operator,
                lander_confirmed_dead: args.lander_confirmed_dead,
            })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&outcome)?);
            } else {
                println!("force-unfreeze\toutcome={outcome:?}");
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
/// stops on the `AtomicBool` after child exit.
fn run_work(dir: &Path, args: WorkArgs) -> Result<i32> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // Pre-flight: confirm active attempt exists for this session, read ttl.
    let pre = open_store(dir)?;
    let capsule = pre.get_capsule(&args.capsule_id)?;
    let active_id = capsule
        .active_attempt
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("capsule has no active attempt; run `capsule claim`"))?;
    let attempt = capsule
        .attempts
        .iter()
        .find(|a| &a.id == active_id)
        .ok_or_else(|| anyhow::anyhow!("active_attempt not found in attempts"))?;
    if attempt.lease.session_id != args.session {
        anyhow::bail!(
            "session mismatch: active attempt session is {}",
            attempt.lease.session_id
        );
    }
    let ttl = attempt.lease.ttl_sec.max(3);
    drop(pre);

    let stop = Arc::new(AtomicBool::new(false));
    let dir_hb = dir.to_path_buf();
    let capsule_id_hb = args.capsule_id.clone();
    let session_hb = args.session.clone();
    let stop_hb = Arc::clone(&stop);

    let hb_thread = thread::spawn(move || -> Result<()> {
        let mut store = open_store(&dir_hb)?;
        let interval = Duration::from_secs(ttl / 3);
        while !stop_hb.load(Ordering::SeqCst) {
            // Sleep first so we don't double-heartbeat immediately after claim.
            let mut slept = Duration::ZERO;
            let tick = Duration::from_millis(200);
            while slept < interval && !stop_hb.load(Ordering::SeqCst) {
                thread::sleep(tick);
                slept += tick;
            }
            if stop_hb.load(Ordering::SeqCst) {
                break;
            }
            if let Err(e) = store.heartbeat(HeartbeatRequest {
                capsule_id: capsule_id_hb.clone(),
                session_id: session_hb.clone(),
            }) {
                eprintln!("capsule work: heartbeat failed: {e}");
                break;
            }
        }
        Ok(())
    });

    let (first, rest) = args.cmd.split_first().expect("clap required >= 1 arg");
    let status = std::process::Command::new(first).args(rest).status();

    stop.store(true, Ordering::SeqCst);
    let _ = hb_thread.join();

    match status {
        Ok(s) => Ok(s.code().unwrap_or_else(|| {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                s.signal().map(|sig| 128 + sig).unwrap_or(1)
            }
            #[cfg(not(unix))]
            {
                1
            }
        })),
        Err(e) => Err(anyhow::anyhow!("spawning {first}: {e}")),
    }
}

fn print_status(c: &Capsule) {
    let scope = c
        .scope_prefixes
        .iter()
        .map(|p| p.as_str())
        .collect::<Vec<_>>()
        .join(",");
    println!(
        "{}\t{}\t{}\t[{}]\t{}",
        c.id,
        status_str(c.status),
        c.base_ref,
        scope,
        c.title
    );
    if !c.depends_on.is_empty() {
        println!("  depends_on: {}", c.depends_on.join(", "));
    }
    for (i, a) in c.attempts.iter().enumerate() {
        let outcome = match a.outcome {
            capsule_core::AttemptOutcome::InFlight => "in_flight",
            capsule_core::AttemptOutcome::Released => "released",
            capsule_core::AttemptOutcome::Expired => "expired",
            capsule_core::AttemptOutcome::Abandoned => "abandoned",
            capsule_core::AttemptOutcome::Landed => "landed",
        };
        println!(
            "  attempt {}: {}\tsession={}\tbranch={}\tlease_expires={}",
            i + 1,
            outcome,
            a.lease.session_id,
            a.branch,
            fmt_ts(a.lease.expires_at)
        );
    }
    if let Some(v) = &c.verification {
        let exit = match &v.exit_code {
            capsule_core::ExitCode::Code(n) => n.to_string(),
            capsule_core::ExitCode::Sentinel(s) => s.clone(),
        };
        println!(
            "  verification: exit={}\tverified_sha={}\tdur={}ms",
            exit, v.verified_sha, v.duration_ms
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

fn status_str(s: Status) -> &'static str {
    match s {
        Status::Planned => "planned",
        Status::Active => "active",
        Status::Accepted => "accepted",
        Status::Landed => "landed",
        Status::Abandoned => "abandoned",
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
