//! `capsule work --isolate=worktree` materialization.
//!
//! Plan v3.1: protocol's path-prefix mutex lives in the store; the worktree is
//! a CLI-layer aid. Authoritative registry is `git worktree list --porcelain`,
//! not a state file. Setup serialized by `<capsule_dir>/locks/worktree-setup-<id>.lock`;
//! runtime guarded by `<capsule_dir>/locks/worktree-run-<id>-a<N>.lock`.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use fs2::FileExt;

const SETUP_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

pub struct IsolateState {
    pub worktree_path: PathBuf,
    pub canonical_capsule_dir: PathBuf,
    /// Held for the child's lifetime. Dropped (released) when this struct does.
    _runtime_lock: File,
}

/// Materialize (or reuse) a worktree for `attempt_branch` and lock it for the
/// child's lifetime.
///
/// The bare-repo check precedes `git rev-parse --show-toplevel` because the
/// latter errors out in bare repos with a less actionable message.
///
/// Lock-handoff order matters: the runtime lock is acquired *before* the
/// setup lock is released, so a second `capsule work` invocation can never
/// observe the worktree unlocked between the two.
///
/// Reuse semantics: when an existing registration matches `worktree_path`,
/// the branch tip is **not** re-validated against `attempt_base_sha` —
/// legitimate commits may have advanced it since the original add. When the
/// registration exists but its directory is gone, fail fast with a remediation
/// hint rather than letting the child's `chdir` surface as ENOENT.
pub fn setup(
    capsule_dir: &Path,
    capsule_id: &str,
    attempt_branch: &str,
    attempt_base_sha: &str,
    attempt_num: u64,
    worktree_dir_override: Option<&Path>,
) -> Result<IsolateState> {
    let canonical_capsule_dir = fs::canonicalize(capsule_dir)
        .with_context(|| format!("canonicalize capsule dir {}", capsule_dir.display()))?;

    if git_is_bare()? {
        bail!(
            "--isolate=worktree requires a working repository; this is a bare repo. \
             Re-run with --isolate=none."
        );
    }
    let main_worktree_root = git_show_toplevel()?;

    let locks_dir = canonical_capsule_dir.join("locks");
    fs::create_dir_all(&locks_dir).with_context(|| format!("create {}", locks_dir.display()))?;
    let setup_lock_path = locks_dir.join(format!("worktree-setup-{capsule_id}.lock"));
    let setup_lock = acquire_setup_lock(&setup_lock_path)?;

    let worktree_path = match worktree_dir_override {
        None => canonical_capsule_dir.join(format!("worktrees/{capsule_id}-a{attempt_num}")),
        Some(p) => validate_worktree_dir_override(p, &main_worktree_root, &canonical_capsule_dir)?,
    };

    let registered = git_worktree_list_for_branch(attempt_branch)?;
    let branch_exists = git_branch_exists(attempt_branch)?;
    let dir_exists = worktree_path.exists();

    match (branch_exists, registered.as_deref(), dir_exists) {
        (false, None, false) => {
            git_worktree_add_new_branch(&worktree_path, attempt_branch, attempt_base_sha)
                .context("git worktree add -b")?;
        }
        (true, None, false) => {
            let tip = git_rev_parse(&format!("refs/heads/{attempt_branch}"))?;
            if tip != attempt_base_sha {
                bail!(
                    "branch {attempt_branch} tip ({tip}) does not match recorded base_sha \
                     ({attempt_base_sha}); refusing to reuse. Run `capsule abandon {capsule_id}` \
                     and re-claim. Do NOT `git update-ref` — capsule refs are protected and \
                     rewriting silently discards any unattested local commits."
                );
            }
            git_worktree_add_existing_branch(&worktree_path, attempt_branch)
                .context("git worktree add (existing branch)")?;
        }
        (_, Some(registered_path), true) if Path::new(registered_path) == worktree_path => {}
        (_, Some(registered_path), false) if Path::new(registered_path) == worktree_path => {
            bail!(
                "worktree for branch {attempt_branch} is registered at {registered_path} but \
                 the directory is missing. Remediation: `git worktree prune` (or `git worktree \
                 remove --force {registered_path}`), then re-run."
            );
        }
        (_, Some(registered_path), _) => {
            bail!(
                "worktree for branch {attempt_branch} is registered at {registered_path}, not \
                 the requested path {}. Remediation: `git worktree remove {registered_path}` \
                 first, then re-run.",
                worktree_path.display()
            );
        }
        (_, None, true) => {
            bail!(
                "directory {} exists but is not a registered git worktree. Remediation: \
                 `git worktree remove --force {}` (verify nothing valuable is in it first), \
                 then re-run.",
                worktree_path.display(),
                worktree_path.display()
            );
        }
    }

    let runtime_lock_path =
        locks_dir.join(format!("worktree-run-{capsule_id}-a{attempt_num}.lock"));
    let runtime_lock = acquire_runtime_lock(&runtime_lock_path, &worktree_path)?;
    drop(setup_lock);

    Ok(IsolateState {
        worktree_path,
        canonical_capsule_dir,
        _runtime_lock: runtime_lock,
    })
}

/// Validate a `--worktree-dir` override. Rejects relative paths up-front:
/// `fs::canonicalize` would resolve them against the process cwd, which is
/// unpredictable when `capsule work` is invoked by an agent from a variable
/// working directory — silently landing the worktree in a surprising location.
/// Force the caller to be explicit.
fn validate_worktree_dir_override(
    p: &Path,
    main_root: &Path,
    capsule_dir: &Path,
) -> Result<PathBuf> {
    if !p.is_absolute() {
        bail!(
            "--worktree-dir must be an absolute path (got {}); relative paths would resolve \
             against the current working directory and silently land elsewhere",
            p.display()
        );
    }

    let canonical = canonicalize_via_parent_if_missing(p)?;

    if canonical == main_root {
        bail!(
            "--worktree-dir cannot equal the main worktree {}",
            main_root.display()
        );
    }
    let git_dir = main_root.join(".git");
    if path_within(&canonical, &git_dir) {
        bail!("--worktree-dir cannot be inside .git");
    }
    if path_within(&canonical, capsule_dir) {
        let allowed_root = capsule_dir.join("worktrees");
        if !path_within(&canonical, &allowed_root) {
            bail!(
                "--worktree-dir is inside the capsule store but outside {}",
                allowed_root.display()
            );
        }
    } else {
        eprintln!(
            "warning: --worktree-dir {} is outside the capsule store ({})",
            canonical.display(),
            capsule_dir.display()
        );
    }
    Ok(canonical)
}

fn path_within(child: &Path, parent: &Path) -> bool {
    child.starts_with(parent)
}

/// Canonicalize `p` if it exists; otherwise canonicalize its parent and rejoin
/// the leaf. `fs::canonicalize` requires the path to exist, but `--worktree-dir`
/// can name a not-yet-created location whose parent does exist (typical when
/// the worktree dir will be `git worktree add`-ed).
fn canonicalize_via_parent_if_missing(p: &Path) -> Result<PathBuf> {
    if p.exists() {
        return fs::canonicalize(p).with_context(|| format!("canonicalize {}", p.display()));
    }
    let parent = p
        .parent()
        .ok_or_else(|| anyhow!("--worktree-dir has no parent: {}", p.display()))?;
    let parent_canon = fs::canonicalize(parent).with_context(|| {
        format!(
            "canonicalize parent of {} ({})",
            p.display(),
            parent.display()
        )
    })?;
    let leaf = p
        .file_name()
        .ok_or_else(|| anyhow!("--worktree-dir has no file name: {}", p.display()))?;
    Ok(parent_canon.join(leaf))
}

fn open_lock_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("open {}", path.display()))
}

fn acquire_setup_lock(path: &Path) -> Result<File> {
    let f = open_lock_file(path)?;
    let deadline = Instant::now() + SETUP_LOCK_TIMEOUT;
    loop {
        match f.try_lock_exclusive() {
            Ok(()) => return Ok(f),
            Err(_) if Instant::now() >= deadline => {
                bail!(
                    "another `capsule work --isolate=worktree` may be running (setup lock at {} \
                     held > 30s)",
                    path.display()
                );
            }
            Err(_) => std::thread::sleep(Duration::from_millis(200)),
        }
    }
}

fn acquire_runtime_lock(path: &Path, worktree_path: &Path) -> Result<File> {
    let f = open_lock_file(path)?;
    f.try_lock_exclusive().map_err(|_| {
        anyhow!(
            "another `capsule work --isolate=worktree` is running in {}; finish or kill that \
             process first (runtime lock {})",
            worktree_path.display(),
            path.display()
        )
    })?;
    Ok(f)
}

fn git_show_toplevel() -> Result<PathBuf> {
    let out = run_git_capture(&["rev-parse", "--show-toplevel"])
        .context("git rev-parse --show-toplevel")?;
    Ok(PathBuf::from(out.trim()))
}

fn git_is_bare() -> Result<bool> {
    let out = run_git_capture(&["rev-parse", "--is-bare-repository"])
        .context("git rev-parse --is-bare-repository")?;
    Ok(out.trim() == "true")
}

fn git_rev_parse(rev: &str) -> Result<String> {
    let mut out = run_git_capture(&["rev-parse", "--verify", rev])
        .with_context(|| format!("git rev-parse {rev}"))?;
    out.truncate(out.trim_end().len());
    Ok(out)
}

fn git_branch_exists(branch: &str) -> Result<bool> {
    let status = Command::new("git")
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ])
        .status()
        .context("spawn git show-ref")?;
    Ok(status.success())
}

/// `git worktree list --porcelain` — return the worktree path registered for
/// `branch`, if any.
fn git_worktree_list_for_branch(branch: &str) -> Result<Option<String>> {
    let out = run_git_capture(&["worktree", "list", "--porcelain"])
        .context("git worktree list --porcelain")?;
    Ok(parse_worktree_list_for_branch(&out, branch).map(str::to_string))
}

/// Pure parser for `git worktree list --porcelain` output.
///
/// Tracks `current_path` as `&str` borrowed from `porcelain` so only the
/// matched entry pays a `String` allocation in the caller. The prior shape
/// pushed an owned `String` for every `worktree ` line, even though every
/// record except at most one is discarded on the way to the match.
///
/// Porcelain record format: each record begins with `worktree <path>` and
/// ends with a blank line; the `branch refs/heads/<name>` line, when
/// present, follows `worktree`. Detached HEADs emit `detached` instead of
/// `branch`. The empty-line arm clears `current_path` to avoid carrying
/// a stale path into the next record (matters when a malformed `branch`
/// line appears outside any record).
fn parse_worktree_list_for_branch<'a>(porcelain: &'a str, branch: &str) -> Option<&'a str> {
    let mut current_path: Option<&str> = None;
    for line in porcelain.lines() {
        if let Some(rest) = line.strip_prefix("worktree ") {
            current_path = Some(rest);
        } else if let Some(name) = line.strip_prefix("branch refs/heads/") {
            if name == branch {
                return current_path;
            }
        } else if line.is_empty() {
            current_path = None;
        }
    }
    None
}

fn git_worktree_add_new_branch(path: &Path, branch: &str, base_sha: &str) -> Result<()> {
    let status = Command::new("git")
        .args(["worktree", "add", "-b", branch])
        .arg(path)
        .arg(base_sha)
        .status()
        .context("spawn git worktree add -b")?;
    if !status.success() {
        bail!(
            "git worktree add -b {branch} {} {base_sha} failed",
            path.display()
        );
    }
    Ok(())
}

fn git_worktree_add_existing_branch(path: &Path, branch: &str) -> Result<()> {
    let status = Command::new("git")
        .args(["worktree", "add"])
        .arg(path)
        .arg(branch)
        .status()
        .context("spawn git worktree add")?;
    if !status.success() {
        bail!("git worktree add {} {branch} failed", path.display());
    }
    Ok(())
}

fn run_git_capture(args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .args(args)
        .output()
        .context("spawn git")?;
    if !out.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8(out.stdout)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    /// Pin both branches of `canonicalize_via_parent_if_missing` against
    /// a real tempdir: when the path exists, full `fs::canonicalize`
    /// applies; when it does NOT, only the parent is canonicalized and
    /// the leaf is rejoined verbatim. Both branches must yield the same
    /// canonical-parent prefix so `validate_worktree_dir_override`'s
    /// `path_within` checks behave consistently regardless of whether
    /// the override directory has been created yet (the typical
    /// `git worktree add`-into-new-dir path).
    #[test]
    fn canonicalize_via_parent_if_missing_both_branches() {
        let td = tempdir().unwrap();
        let parent = fs::canonicalize(td.path()).unwrap();

        let existing = parent.join("here");
        fs::create_dir(&existing).unwrap();
        let got_existing = canonicalize_via_parent_if_missing(&existing).unwrap();
        assert_eq!(got_existing, fs::canonicalize(&existing).unwrap());

        let missing = parent.join("not-yet");
        assert!(!missing.exists());
        let got_missing = canonicalize_via_parent_if_missing(&missing).unwrap();
        assert_eq!(got_missing, parent.join("not-yet"));
    }

    /// Last assertion is the sibling-prefix regression guard: `/a/bad` must
    /// NOT be considered within `/a/b`. `Path::starts_with` is component-aware,
    /// so this guards against a future regression to string-prefix matching.
    #[test]
    fn path_within_basic() {
        assert!(path_within(Path::new("/a/b/c"), Path::new("/a/b")));
        assert!(path_within(Path::new("/a/b"), Path::new("/a/b")));
        assert!(!path_within(Path::new("/a/b"), Path::new("/a/b/c")));
        assert!(!path_within(Path::new("/x"), Path::new("/y")));
        assert!(!path_within(Path::new("/a/bad"), Path::new("/a/b")));
    }

    fn make_main_root(td: &tempfile::TempDir) -> std::path::PathBuf {
        let root = td.path().join("repo");
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::canonicalize(&root).unwrap()
    }

    #[test]
    fn override_rejects_equal_to_main() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        let err = validate_worktree_dir_override(&main, &main, &cap).unwrap_err();
        assert!(err.to_string().contains("cannot equal the main worktree"));
    }

    #[test]
    fn override_rejects_inside_git() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        let inside = main.join(".git/sub");
        fs::create_dir_all(&inside).unwrap();
        let err = validate_worktree_dir_override(&inside, &main, &cap).unwrap_err();
        assert!(err.to_string().contains("inside .git"));
    }

    /// Two contracts pinned at once:
    /// 1. `allowed` does not exist — exercises the parent-canonicalize branch
    ///    so that the validator works for paths the caller plans to create.
    /// 2. The returned path is full-path equal (not just `file_name` equal):
    ///    a regression in parent canonicalization would still pass a
    ///    `file_name`-only check.
    #[test]
    fn override_allows_default_subtree() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        let allowed = cap.join("worktrees/foo-a1");
        fs::create_dir_all(&cap).unwrap();
        fs::create_dir_all(allowed.parent().unwrap()).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        assert!(!allowed.exists());
        let got = validate_worktree_dir_override(&allowed, &main, &cap).unwrap();
        assert_eq!(got, cap.join("worktrees/foo-a1"));
    }

    #[test]
    fn override_rejects_inside_store_outside_worktrees() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        let bad = cap.join("foreign/x");
        fs::create_dir_all(bad.parent().unwrap()).unwrap();
        let err = validate_worktree_dir_override(&bad, &main, &cap).unwrap_err();
        assert!(err.to_string().contains("inside the capsule store"));
    }

    #[test]
    fn override_rejects_relative_path() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        let rel = Path::new("foo/bar");
        let err = validate_worktree_dir_override(rel, &main, &cap).unwrap_err();
        assert!(
            err.to_string().contains("must be an absolute path"),
            "got: {err}"
        );
    }

    /// Outside-the-store paths are accepted (with a stderr warning, not
    /// bail) — the policy is advisory once the override lands fully
    /// outside the `.capsule` subtree. Pin the acceptance so a future
    /// tightening to "must live under <cap>/worktrees" is a deliberate
    /// change rather than a silent rejection of valid out-of-tree
    /// overrides (e.g. `--worktree-dir=/tmp/scratch`).
    #[test]
    fn override_outside_store_accepted_with_warning() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        let outside = td.path().join("scratch/wt");
        fs::create_dir_all(outside.parent().unwrap()).unwrap();
        let got = validate_worktree_dir_override(&outside, &main, &cap).unwrap();
        let expected_parent = fs::canonicalize(outside.parent().unwrap()).unwrap();
        assert_eq!(got, expected_parent.join("wt"));
    }

    /// `cap/worktrees/../foreign/x` with `foreign` existing — verifies that
    /// `fs::canonicalize` collapses `..` so the validation sees the real
    /// target (inside the store, outside worktrees) and rejects it.
    #[test]
    fn override_traversal_via_dotdot_is_canonicalized() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        let foreign = cap.join("foreign");
        fs::create_dir_all(&foreign).unwrap();
        let traversal = cap.join("worktrees/../foreign/x");
        fs::create_dir_all(cap.join("worktrees")).unwrap();
        let err = validate_worktree_dir_override(&traversal, &main, &cap).unwrap_err();
        assert!(err.to_string().contains("inside the capsule store"));
    }

    /// Empirical fixture: three records (main + one branch worktree + one
    /// detached). Pinned shape: blank-line separated, `worktree` line first,
    /// `branch refs/heads/<name>` line within the record. The detached
    /// record emits `detached` instead of `branch`.
    const PORCELAIN: &str = "\
worktree /repo/main
HEAD aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
branch refs/heads/main

worktree /repo/wt-feature
HEAD bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
branch refs/heads/feature

worktree /repo/wt-detached
HEAD cccccccccccccccccccccccccccccccccccccccc
detached

";

    #[test]
    fn parse_worktree_list_returns_path_for_match() {
        assert_eq!(
            parse_worktree_list_for_branch(PORCELAIN, "feature"),
            Some("/repo/wt-feature")
        );
    }

    #[test]
    fn parse_worktree_list_returns_path_for_first_record() {
        assert_eq!(
            parse_worktree_list_for_branch(PORCELAIN, "main"),
            Some("/repo/main")
        );
    }

    #[test]
    fn parse_worktree_list_unknown_branch_is_none() {
        assert_eq!(
            parse_worktree_list_for_branch(PORCELAIN, "nope"),
            None
        );
    }

    /// Detached records have no `branch` line, so they must never match,
    /// regardless of how the caller spells the query.
    #[test]
    fn parse_worktree_list_detached_record_never_matches() {
        assert_eq!(
            parse_worktree_list_for_branch(PORCELAIN, "detached"),
            None
        );
    }

    /// Stray `branch` line outside any record (regression guard for the
    /// empty-line reset of `current_path`): without the reset, the orphan
    /// branch would inherit the previous record's path and the function
    /// would return `Some("/repo/main")` — the caller would then treat
    /// `/repo/main` as the registered worktree for `orphan`, which is a
    /// real correctness bug. The reset zeros `current_path` at the blank
    /// line, so the orphan resolves to `None` instead.
    #[test]
    fn parse_worktree_list_orphan_branch_line_yields_none_path() {
        let stray = "\
worktree /repo/main
branch refs/heads/main

branch refs/heads/orphan
";
        assert_eq!(parse_worktree_list_for_branch(stray, "orphan"), None);
    }

    /// Records may carry extra porcelain keys (`locked`, `prunable`, `bare`)
    /// alongside `worktree` / `branch`. The parser ignores unknown lines
    /// other than the empty-line record separator, so extra keys must not
    /// disrupt the match. Pinned so a future shape change to the matcher
    /// (e.g. switching to a strict line-by-line state machine) doesn't
    /// silently regress on real-world porcelain.
    #[test]
    fn parse_worktree_list_extra_keys_in_record_ignored() {
        let porcelain = "\
worktree /repo/wt-locked
HEAD aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
branch refs/heads/feature
locked
prunable

";
        assert_eq!(
            parse_worktree_list_for_branch(porcelain, "feature"),
            Some("/repo/wt-locked")
        );
    }

    /// Empty input → `None` (consistent with `lines()` over an empty string
    /// yielding zero items). Pinned because `git worktree list --porcelain`
    /// can in principle emit an empty body.
    #[test]
    fn parse_worktree_list_empty_input_is_none() {
        assert_eq!(parse_worktree_list_for_branch("", "main"), None);
    }

    /// Branch-name match is `==`, not a prefix: `capsules/<id>/a1` must
    /// not match `branch refs/heads/capsules/<id>/a10`.
    #[test]
    fn parse_worktree_list_branch_match_does_not_prefix_match() {
        let porcelain = "\
worktree /repo/wt-a10
branch refs/heads/capsules/abc/a10

";
        assert_eq!(
            parse_worktree_list_for_branch(porcelain, "capsules/abc/a10"),
            Some("/repo/wt-a10")
        );
        assert_eq!(
            parse_worktree_list_for_branch(porcelain, "capsules/abc/a1"),
            None
        );
    }
}
