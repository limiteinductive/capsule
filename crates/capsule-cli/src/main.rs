use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "capsule", version, about = "Path-prefix lock + verified atomic land for parallel agents.")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Initialize a capsule store in `.capsule/state.db`.
    Init,
    /// Run the deployment ACL test suite (DESIGN.md §8.2).
    DeployVerify,
    /// Create a new capsule.
    Create,
    /// Claim a planned capsule for a session.
    Claim,
    /// Heartbeat the active attempt.
    Heartbeat,
    /// Record verification for the active attempt.
    Attest,
    /// Land an accepted capsule via atomic multi-ref push.
    Land,
    /// Abandon a capsule.
    Abandon,
    /// Reclaim an expired capsule (manual).
    Reclaim,
    /// Add a dependency edge.
    AddDep,
    /// Remove a dependency edge.
    RemoveDep,
    /// List capsules.
    List,
    /// Operator escape hatch: force-clear a stuck pending_land.
    ForceUnfreeze,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init => todo!("capsule init"),
        Cmd::DeployVerify => todo!("capsule deploy verify"),
        Cmd::Create => todo!("capsule create"),
        Cmd::Claim => todo!("capsule claim"),
        Cmd::Heartbeat => todo!("capsule heartbeat"),
        Cmd::Attest => todo!("capsule attest"),
        Cmd::Land => todo!("capsule land"),
        Cmd::Abandon => todo!("capsule abandon"),
        Cmd::Reclaim => todo!("capsule reclaim"),
        Cmd::AddDep => todo!("capsule add-dep"),
        Cmd::RemoveDep => todo!("capsule remove-dep"),
        Cmd::List => todo!("capsule list"),
        Cmd::ForceUnfreeze => todo!("capsule force-unfreeze"),
    }
}
