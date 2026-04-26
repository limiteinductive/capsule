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

// Compile-time pin: ZERO_OID conforms to `sha::validate` (40 lowercase hex).
// The runtime test elsewhere only checked `len == 40`; this catches a typo
// (e.g. an `O` in place of `0`) at build time instead.
const _: () = match capsule_core::sha::validate(ZERO_OID) {
    Ok(()) => (),
    Err(_) => panic!("ZERO_OID does not satisfy sha::validate"),
};

/// Substrings of git's user-facing error messages that classify_push matches
/// against to map a push failure to a `LandOutcome`. These are NOT git's own
/// stable API — they're the canonical client's CLI output. Centralizing them
/// here so the porcelain stdout branch and the stderr fallback don't drift
/// independently when adapting to a new git version.
mod git_reject {
    /// Reported when `--force-with-lease=ref:` finds the ref non-empty
    /// (the witness leak case — DESIGN §7.1.2 step 3 / §3.1).
    pub const STALE_INFO: &str = "stale info";
    /// Modern git: base_ref is not a fast-forward from verified_sha.
    pub const FETCH_FIRST: &str = "fetch first";
    /// Older git: same as FETCH_FIRST. Hyphenated form on stdout porcelain.
    pub const NON_FAST_FORWARD: &str = "non-fast-forward";
    /// Older git: same as FETCH_FIRST. Space form (`man git-push`).
    /// Stdout-only: porcelain emits this; stderr aggregates with
    /// `UPDATES_REJECTED` instead.
    pub const NON_FAST_FORWARD_SPACE: &str = "non-fast forward";
    /// Aggregate stderr line from `git push` when any ref was rejected;
    /// stderr-only because porcelain emits per-ref reasons on stdout.
    pub const UPDATES_REJECTED: &str = "Updates were rejected";
}

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
    parse_ls_remote_stdout(&String::from_utf8_lossy(&out.stdout))
}

/// Pure parser/validator for `git ls-remote --heads` stdout. Empty stdout →
/// `ZERO_OID`; otherwise the first whitespace-delimited token must be a valid
/// 40-char lowercase hex sha. Validated at the wire boundary so garbage here
/// (e.g. a corrupt remote response) cannot flow into Store::land's
/// `pre_push_base_sha` and silently corrupt the §7.1.2 dance.
fn parse_ls_remote_stdout(stdout: &str) -> Result<String> {
    match stdout.split_whitespace().next() {
        None => Ok(ZERO_OID.to_string()),
        Some(sha) => {
            capsule_core::sha::validate(sha).map_err(|e| {
                GitError::Parse(format!("ls-remote returned non-sha token {sha:?}: {e}"))
            })?;
            Ok(sha.to_string())
        }
    }
}

/// Outcome of an atomic multi-ref land push (DESIGN.md §7.1.2 step 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LandOutcome {
    /// Push accepted; `base_ref` advanced (or was created from ZERO_OID),
    /// witness branch created or already at verified_sha.
    Advanced {
        base_ref_created: bool,
        witness_created: bool,
    },
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
/// Note: the atomic push uses git's own FF check on `base_ref`, so the
/// `prior_base_sha` is not needed here — the caller already records it in
/// `PendingLand` and uses it later to compute `Landing.advanced_base_ref`.
pub fn land_push(
    repo_dir: &std::path::Path,
    remote: &str,
    base_ref: &str,
    witness_branch: &str,
    verified_sha: &str,
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

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let code = out.status.code().unwrap_or(-1);

    classify_push(
        &stdout,
        &stderr,
        out.status.success(),
        code,
        base_ref,
        witness_branch,
    )
}

/// Classify a `git push --atomic --porcelain --force-with-lease` invocation
/// from its stdout/stderr/status. Pure function; testable.
///
/// Porcelain ref-line format: `<flag>\t<src>:<dst>\t<summary>` with flag ∈
/// `[ +-*!=]`. CRITICAL: with `--porcelain`, git emits `[rejected] (stale
/// info)` and `[rejected] (fetch first)` on **stdout**, not stderr — stderr
/// only carries the bare "failed to push some refs" line plus user hints. So
/// failure classification must read stdout. Witness `(stale info)` always
/// outranks base non-FF, since a witness OID mismatch is a §3.1 protection
/// leak (operational incident) while a non-FF base is benign caller-rebase.
///
/// Destination matching: `land_push` emits full `refs/heads/<branch>`
/// destinations; this function strips that prefix and compares bare names so
/// `base_ref` / `witness_branch` callers don't pay two `format!` allocs per
/// call.
fn classify_push(
    stdout: &str,
    stderr: &str,
    success: bool,
    code: i32,
    base_ref: &str,
    witness_branch: &str,
) -> Result<LandOutcome> {
    let mut witness: Option<RefLine<'_>> = None;
    let mut base: Option<RefLine<'_>> = None;
    for line in stdout.lines().filter_map(parse_ref_line) {
        let Some(name) = line.dst.strip_prefix("refs/heads/") else {
            continue;
        };
        if name == witness_branch {
            witness = Some(line);
        } else if name == base_ref {
            base = Some(line);
        }
    }
    let witness = witness.as_ref();
    let base = base.as_ref();

    if success {
        let (base_created, base_changed) = ref_change(base);
        let (witness_created, witness_changed) = ref_change(witness);
        if base_changed || witness_changed {
            Ok(LandOutcome::Advanced {
                base_ref_created: base_created,
                witness_created,
            })
        } else {
            Ok(LandOutcome::NoOp)
        }
    } else if witness_protection_leak(witness) {
        Ok(LandOutcome::WitnessOidMismatch)
    } else if base_non_fast_forward(base) {
        Ok(LandOutcome::BaseRefMoved)
    } else {
        classify_failure_from_stderr(stderr, code)
    }
}

fn witness_protection_leak(witness: Option<&RefLine<'_>>) -> bool {
    rejected_with(witness, git_reject::STALE_INFO)
}

/// Inlined (not three `rejected_with` calls) so `flag == '!'` is checked
/// once across the three equivalent rejection reasons git emits for non-FF.
fn base_non_fast_forward(base: Option<&RefLine<'_>>) -> bool {
    base.is_some_and(|l| {
        l.flag == '!'
            && [
                git_reject::FETCH_FIRST,
                git_reject::NON_FAST_FORWARD,
                git_reject::NON_FAST_FORWARD_SPACE,
            ]
            .iter()
            .any(|n| l.summary.contains(n))
    })
}

/// Stderr-only fallback when porcelain stdout did not carry per-ref reasons
/// (older git, unrecognized output). Same precedence as the stdout path:
/// witness leak outranks base non-FF.
fn classify_failure_from_stderr(stderr: &str, code: i32) -> Result<LandOutcome> {
    if stderr.contains(git_reject::STALE_INFO) {
        Ok(LandOutcome::WitnessOidMismatch)
    } else if stderr.contains(git_reject::NON_FAST_FORWARD)
        || stderr.contains(git_reject::FETCH_FIRST)
        || stderr.contains(git_reject::UPDATES_REJECTED)
    {
        Ok(LandOutcome::BaseRefMoved)
    } else if code != 0 {
        Ok(LandOutcome::OtherFailure {
            stderr: stderr.to_string(),
        })
    } else {
        Err(GitError::Failed {
            code,
            stderr: stderr.to_string(),
        })
    }
}

#[derive(Debug)]
struct RefLine<'a> {
    flag: char,
    dst: &'a str,
    summary: &'a str,
}

/// Parse one porcelain line: `<flag>\t<src>:<dst>\t<summary>`. Header lines
/// (`To <remote>`, `Done`) and blanks return None.
fn parse_ref_line(line: &str) -> Option<RefLine<'_>> {
    if line.is_empty() || line.starts_with("To ") || line.starts_with("Done") {
        return None;
    }
    let mut parts = line.splitn(3, '\t');
    let flag_field = parts.next()?;
    let refspec = parts.next()?;
    let summary = parts.next().unwrap_or("");
    let flag = flag_field.chars().next()?;
    let dst = refspec.split_once(':').map_or(refspec, |(_, d)| d);
    Some(RefLine { flag, dst, summary })
}

/// (created, changed). `*` = new ref, `' '` = FF, `'+'` = forced update.
fn ref_change(line: Option<&RefLine<'_>>) -> (bool, bool) {
    match line.map(|l| l.flag) {
        Some('*') => (true, true),
        Some(' ' | '+') => (false, true),
        _ => (false, false),
    }
}

fn rejected_with(line: Option<&RefLine<'_>>, needle: &str) -> bool {
    line.is_some_and(|l| l.flag == '!' && l.summary.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mirror the build-time `const _: ()` pin in `cargo test` output too,
    /// so a typo in `ZERO_OID` surfaces in test runs and not just `cargo
    /// build`.
    #[test]
    fn zero_oid_is_valid_sha() {
        capsule_core::sha::validate(ZERO_OID).unwrap();
    }

    #[test]
    fn parse_ls_remote_empty_returns_zero_oid() {
        assert_eq!(parse_ls_remote_stdout("").unwrap(), ZERO_OID);
        assert_eq!(parse_ls_remote_stdout("   \n\t").unwrap(), ZERO_OID);
    }

    #[test]
    fn parse_ls_remote_valid_sha() {
        let stdout = "0123456789abcdef0123456789abcdef01234567\trefs/heads/main\n";
        assert_eq!(
            parse_ls_remote_stdout(stdout).unwrap(),
            "0123456789abcdef0123456789abcdef01234567"
        );
    }

    /// Realistic failure: a misconfigured proxy injects an HTML error page,
    /// or git emits a warning line before the sha. Garbage on the wire must
    /// surface as `GitError::Parse`, never flow on as a sha.
    #[test]
    fn parse_ls_remote_rejects_non_sha_token() {
        let stdout = "not-a-sha\trefs/heads/main\n";
        match parse_ls_remote_stdout(stdout) {
            Err(GitError::Parse(msg)) => assert!(msg.contains("not-a-sha"), "msg: {msg}"),
            other @ (Ok(_) | Err(GitError::Io(_)) | Err(GitError::Failed { .. })) => {
                panic!("expected Parse error, got {other:?}")
            }
        }
    }

    #[test]
    fn parse_ls_remote_rejects_uppercase_sha() {
        let stdout = "0123456789ABCDEF0123456789abcdef01234567\trefs/heads/main\n";
        assert!(matches!(
            parse_ls_remote_stdout(stdout),
            Err(GitError::Parse(_))
        ));
    }

    /// Empirical transcripts captured from `git push --porcelain` against a
    /// real remote (git 2.x). Tab-separated, as porcelain emits.
    const STALE_INFO_STDOUT: &str = "To /tmp/remote.git\n!\trefs/heads/witness:refs/heads/witness\t[rejected] (stale info)\nDone\n";
    const STALE_INFO_STDERR: &str = "error: failed to push some refs to '/tmp/remote.git'\n";
    const FETCH_FIRST_STDOUT: &str =
        "To /tmp/remote.git\n!\tHEAD:refs/heads/main\t[rejected] (fetch first)\nDone\n";
    const FETCH_FIRST_STDERR: &str = "error: failed to push some refs to '/tmp/remote.git'\nhint: Updates were rejected ...\n";
    const ADVANCED_STDOUT: &str =
        "To /tmp/remote.git\n \trefs/heads/x:refs/heads/main\t<sha>..<sha>\n*\trefs/heads/x:refs/heads/capsule-witness/foo/a1\t[new branch]\nDone\n";
    const NOOP_STDOUT: &str = "To /tmp/remote.git\n=\trefs/heads/x:refs/heads/main\t[up to date]\n=\trefs/heads/x:refs/heads/capsule-witness/foo/a1\t[up to date]\nDone\n";

    #[test]
    fn classifies_witness_stale_info_from_stdout() {
        let r = classify_push(
            STALE_INFO_STDOUT,
            STALE_INFO_STDERR,
            false,
            1,
            "main",
            "witness",
        )
        .unwrap();
        assert_eq!(r, LandOutcome::WitnessOidMismatch);
    }

    #[test]
    fn classifies_base_non_ff_from_stdout() {
        let r = classify_push(
            FETCH_FIRST_STDOUT,
            FETCH_FIRST_STDERR,
            false,
            1,
            "main",
            "capsule-witness/foo/a1",
        )
        .unwrap();
        assert_eq!(r, LandOutcome::BaseRefMoved);
    }

    /// Synthesized: with `--atomic`, a witness stale-info would still appear
    /// even when base also failed. Witness wins (protection leak outranks
    /// caller-rebase).
    #[test]
    fn witness_stale_outranks_base_non_ff() {
        let stdout = "To /tmp/remote.git\n\
            !\tHEAD:refs/heads/main\t[rejected] (fetch first)\n\
            !\trefs/heads/w:refs/heads/capsule-witness/foo/a1\t[rejected] (stale info)\n\
            Done\n";
        let r = classify_push(stdout, "", false, 1, "main", "capsule-witness/foo/a1").unwrap();
        assert_eq!(r, LandOutcome::WitnessOidMismatch);
    }

    #[test]
    fn classifies_advanced() {
        let r = classify_push(
            ADVANCED_STDOUT,
            "",
            true,
            0,
            "main",
            "capsule-witness/foo/a1",
        )
        .unwrap();
        assert_eq!(
            r,
            LandOutcome::Advanced {
                base_ref_created: false,
                witness_created: true,
            }
        );
    }

    #[test]
    fn classifies_noop() {
        let r = classify_push(NOOP_STDOUT, "", true, 0, "main", "capsule-witness/foo/a1").unwrap();
        assert_eq!(r, LandOutcome::NoOp);
    }

    #[test]
    fn other_failure_when_unrecognized() {
        let r = classify_push(
            "To /tmp/remote.git\nDone\n",
            "fatal: unable to access 'https://...': could not resolve host\n",
            false,
            128,
            "main",
            "w",
        )
        .unwrap();
        match r {
            LandOutcome::OtherFailure { stderr } => assert!(stderr.contains("resolve host")),
            other @ (LandOutcome::Advanced { .. }
            | LandOutcome::NoOp
            | LandOutcome::WitnessOidMismatch
            | LandOutcome::BaseRefMoved) => panic!("expected OtherFailure, got {other:?}"),
        }
    }
}
