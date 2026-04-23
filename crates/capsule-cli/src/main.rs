use std::path::PathBuf;

use anyhow::{Context, Result};
use capsule_core::path::CanonicalPath;
use capsule_core::{Acceptance, ExpectExit, Status};
use capsule_store::{
    AttestRequest, ClaimRequest, HeartbeatRequest, ListFilter, NewCapsule, Store,
};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "capsule", version, about = "Path-prefix lock + verified atomic land for parallel agents.")]
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
    Init,
    /// Run the deployment ACL test suite (DESIGN.md §8.2). [unimplemented]
    DeployVerify,
    /// Create a new capsule.
    Create(CreateArgs),
    /// Claim a planned capsule for a session.
    Claim(ClaimArgs),
    /// Heartbeat the active attempt.
    Heartbeat(HeartbeatArgs),
    /// Record verification for the active attempt.
    Attest(AttestArgs),
    /// Land an accepted capsule via atomic multi-ref push. [unimplemented]
    Land,
    /// Abandon a capsule. [unimplemented]
    Abandon,
    /// Reclaim an expired capsule (manual). [unimplemented]
    Reclaim,
    /// Add a dependency edge. [unimplemented]
    AddDep,
    /// Remove a dependency edge. [unimplemented]
    RemoveDep,
    /// List capsules.
    List(ListArgs),
    /// Operator escape hatch: force-clear a stuck pending_land. [unimplemented]
    ForceUnfreeze,
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
struct ClaimArgs {
    capsule_id: String,
    #[arg(long)]
    owner: String,
    #[arg(long)]
    session: String,
    #[arg(long = "lease-ttl-sec", default_value_t = 300)]
    lease_ttl_sec: u64,
    #[arg(long = "base-sha")]
    base_sha: String,
}

#[derive(clap::Args)]
struct HeartbeatArgs {
    capsule_id: String,
    #[arg(long)]
    session: String,
    #[arg(long = "lease-ttl-sec", default_value_t = 300)]
    lease_ttl_sec: u64,
}

#[derive(clap::Args)]
struct AttestArgs {
    capsule_id: String,
    #[arg(long)]
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
    /// Write-once or content-addressed URI (DESIGN.md §7.2 log_ref integrity).
    #[arg(long = "log-ref")]
    log_ref: String,
}

#[derive(clap::Args)]
struct ListArgs {
    #[arg(long, value_parser = parse_status_arg)]
    status: Option<Status>,
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

fn open_store(dir: &PathBuf) -> Result<Store> {
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
        Cmd::Init => {
            let _ = open_store(&dir)?;
            if cli.json {
                println!("{}", serde_json::json!({"ok": true, "dir": dir}));
            } else {
                println!("initialized capsule store at {}", dir.display());
            }
        }
        Cmd::Create(args) => {
            let mut store = open_store(&dir)?;
            let scope_prefixes = args
                .scope
                .iter()
                .map(|s| {
                    CanonicalPath::new(s)
                        .map_err(|e| anyhow::anyhow!("invalid --scope {s:?}: {e}"))
                })
                .collect::<Result<Vec<_>>>()?;

            let id = args
                .id
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

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
                println!("{}\t{}\t{}", capsule.id, status_str(capsule.status), capsule.title);
            }
        }
        Cmd::List(args) => {
            let store = open_store(&dir)?;
            let capsules = store.list_capsules(ListFilter { status: args.status })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&capsules)?);
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
                    "claimed\tattempt={}\tbranch={}\twitness={}\tlease_expires={}",
                    attempt.id,
                    attempt.branch,
                    attempt.witness_branch,
                    attempt.lease.expires_at
                );
            }
        }
        Cmd::Heartbeat(args) => {
            let mut store = open_store(&dir)?;
            let ack = store.heartbeat(HeartbeatRequest {
                capsule_id: args.capsule_id,
                session_id: args.session,
                lease_ttl_sec: args.lease_ttl_sec,
            })?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&ack)?);
            } else {
                println!("heartbeat\tlease_expires={}", ack.lease_expires_at);
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
        Cmd::DeployVerify | Cmd::Land
        | Cmd::Abandon
        | Cmd::Reclaim
        | Cmd::AddDep
        | Cmd::RemoveDep
        | Cmd::ForceUnfreeze => {
            anyhow::bail!("not yet implemented")
        }
    }
    Ok(())
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
