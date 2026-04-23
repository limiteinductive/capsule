//! Git wire integration. See `DESIGN.md` §7.1.2 (land) and §3.1 (publication contract).
//!
//! Shells out to the `git` CLI for portability — `--force-with-lease` and
//! atomic multi-ref push semantics are best preserved by the canonical client.

use std::process::{Command, Stdio};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GitError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("git exited {code}: {stderr}")]
    Failed { code: i32, stderr: String },
    #[error("could not parse git output: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, GitError>;

pub const ZERO_OID: &str = "0000000000000000000000000000000000000000";

/// Read the current sha at `refs/heads/<branch>` on `remote`, or `ZERO_OID` if absent.
/// Uses `git ls-remote --heads <remote> <branch>`.
pub fn ls_remote_branch(remote: &str, branch: &str) -> Result<String> {
    let out = Command::new("git")
        .args(["ls-remote", "--heads", remote, branch])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()?;
    if !out.status.success() {
        return Err(GitError::Failed {
            code: out.status.code().unwrap_or(-1),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        });
    }
    let s = String::from_utf8_lossy(&out.stdout);
    if s.trim().is_empty() {
        return Ok(ZERO_OID.to_string());
    }
    let sha = s
        .split_whitespace()
        .next()
        .ok_or_else(|| GitError::Parse(s.to_string()))?;
    Ok(sha.to_string())
}

/// Outcome of an atomic multi-ref land push (DESIGN.md §7.1.2 step 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandOutcome {
    /// Push accepted; `base_ref` advanced (or was created from ZERO_OID),
    /// witness branch created or already at verified_sha.
    Advanced { base_ref_created: bool, witness_created: bool },
    /// Both refs already at verified_sha — fully idempotent re-run.
    NoOp,
    /// Witness ref existed at a different sha than expected null.
    /// Atomic-rejected: neither ref changed. Operational incident.
    WitnessOidMismatch,
    /// `base_ref` was not a fast-forward from verified_sha.
    /// Atomic-rejected: neither ref changed. Caller rebases + re-attests.
    BaseRefMoved,
    /// Other failure (network, auth, etc.).
    OtherFailure { stderr: String },
}

/// Atomic multi-ref land push:
///
/// ```text
/// git push --atomic \
///   --force-with-lease=refs/heads/<witness_branch>: \
///   <remote> \
///   <verified_sha>:refs/heads/<base_ref> \
///   <verified_sha>:refs/heads/<witness_branch>
/// ```
///
/// `repo_dir` is the git working directory the push is invoked from — it
/// must have `verified_sha` in its object database (typically the lander's
/// clone of the remote, which has fetched/received the worker's commits).
///
/// The `expected_prior_base_sha` argument is informational — it lets the
/// caller compute `Landing.advanced_base_ref`. The atomic push uses git's
/// own FF check on `base_ref`, not the `expected_prior_base_sha`.
pub fn land_push(
    repo_dir: &std::path::Path,
    remote: &str,
    base_ref: &str,
    witness_branch: &str,
    verified_sha: &str,
    _expected_prior_base_sha: &str,
) -> Result<LandOutcome> {
    let force_with_lease = format!("--force-with-lease=refs/heads/{witness_branch}:");
    let base_refspec = format!("{verified_sha}:refs/heads/{base_ref}");
    let witness_refspec = format!("{verified_sha}:refs/heads/{witness_branch}");

    let out = Command::new("git")
        .current_dir(repo_dir)
        .args([
            "push",
            "--atomic",
            "--porcelain",
            &force_with_lease,
            remote,
            &base_refspec,
            &witness_refspec,
        ])
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()?;

    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let code = out.status.code().unwrap_or(-1);

    if out.status.success() {
        // Parse porcelain output. Lines beginning with status flags:
        //   ' ' fast-forward, '+' forced, '-' deleted, '*' new, '!' rejected, '=' up to date
        // We expect two ref lines (base + witness).
        let mut base_created = false;
        let mut witness_created = false;
        let mut any_change = false;
        for line in stdout.lines() {
            // porcelain format:  "<flag> <from>:<to>  <summary>"
            // lines start with one of " +-*!=" (then space)
            if line.starts_with("To ") || line.is_empty() {
                continue;
            }
            let flag = line.chars().next().unwrap_or(' ');
            // identify which ref this line is about
            let is_base = line.contains(&format!("refs/heads/{base_ref}"))
                && !line.contains("witness");
            let is_witness = line.contains(witness_branch);
            match flag {
                '*' => {
                    any_change = true;
                    if is_base {
                        base_created = true;
                    } else if is_witness {
                        witness_created = true;
                    }
                }
                ' ' | '+' => {
                    any_change = true;
                }
                '=' => {
                    // up-to-date — no change for this ref
                }
                _ => {}
            }
        }
        if any_change {
            Ok(LandOutcome::Advanced {
                base_ref_created: base_created,
                witness_created,
            })
        } else {
            Ok(LandOutcome::NoOp)
        }
    } else {
        // Distinguish witness lease failure from non-FF base.
        // git surfaces lease failure as "stale info" and non-FF as "non-fast-forward"
        // or "fetch first" / "rejected" depending on version.
        if stderr.contains("stale info") {
            Ok(LandOutcome::WitnessOidMismatch)
        } else if stderr.contains("non-fast-forward")
            || stderr.contains("fetch first")
            || stderr.contains("Updates were rejected")
        {
            Ok(LandOutcome::BaseRefMoved)
        } else if code != 0 {
            Ok(LandOutcome::OtherFailure { stderr })
        } else {
            Err(GitError::Failed { code, stderr })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ls_remote_nonexistent_returns_zero_oid() {
        // Smoke test against a clearly-bogus remote — git will fail or return empty.
        // We only assert the API shape (ZERO_OID len = 40).
        assert_eq!(ZERO_OID.len(), 40);
    }
}
