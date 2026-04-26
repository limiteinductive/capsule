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

pub fn setup(
    capsule_dir: &Path,
    capsule_id: &str,
    attempt_branch: &str,
    attempt_base_sha: &str,
    attempt_num: u64,
    worktree_dir_override: Option<&Path>,
) -> Result<IsolateState> {
    // Step 1: canonicalize capsule_dir to absolute (F26).
    let canonical_capsule_dir = fs::canonicalize(capsule_dir)
        .with_context(|| format!("canonicalize capsule dir {}", capsule_dir.display()))?;

    // Step 2: bare-repo check (F9). Check bare BEFORE --show-toplevel, since
    // the latter errors out in bare repos with a less actionable message.
    if git_is_bare()? {
        bail!(
            "--isolate=worktree requires a working repository; this is a bare repo. \
             Re-run with --isolate=none."
        );
    }
    let main_worktree_root = git_show_toplevel()?;

    // Step 3: setup lock (F4, F23). Steps 4-8 happen inside it.
    let locks_dir = canonical_capsule_dir.join("locks");
    fs::create_dir_all(&locks_dir).with_context(|| format!("create {}", locks_dir.display()))?;
    let setup_lock_path = locks_dir.join(format!("worktree-setup-{capsule_id}.lock"));
    let setup_lock = acquire_setup_lock(&setup_lock_path)?;

    // Step 5: resolve worktree path (default or override) and validate (F7).
    let default_path = canonical_capsule_dir.join(format!("worktrees/{capsule_id}-a{attempt_num}"));
    let worktree_path = match worktree_dir_override {
        None => default_path,
        Some(p) => validate_worktree_dir_override(p, &main_worktree_root, &canonical_capsule_dir)?,
    };

    // Step 5: probe state.
    let registered = git_worktree_list_for_branch(attempt_branch)?;
    let branch_exists = git_branch_exists(attempt_branch)?;
    let dir_exists = worktree_path.exists();

    // Step 6: decision tree.
    match (branch_exists, registered.as_deref(), dir_exists) {
        // (a) branch absent + dir absent + nothing registered.
        (false, None, false) => {
            git_worktree_add_new_branch(&worktree_path, attempt_branch, attempt_base_sha)
                .context("git worktree add -b")?;
        }
        // (b) branch present + no registration + dir absent — verify tip (F24).
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
        // (c) registered at expected path, dir present — reuse.
        // Tip not validated on (c); legitimate commits may exist.
        (_, Some(registered_path), true) if Path::new(registered_path) == worktree_path => {}
        // (c′) registered at expected path but dir is missing (stale registration).
        // Spawning the child here would fail with ENOENT once we chdir; bail
        // explicitly with a remediation hint instead.
        (_, Some(registered_path), false) if Path::new(registered_path) == worktree_path => {
            bail!(
                "worktree for branch {attempt_branch} is registered at {registered_path} but \
                 the directory is missing. Remediation: `git worktree prune` (or `git worktree \
                 remove --force {registered_path}`), then re-run."
            );
        }
        // (d) registered at different path.
        (_, Some(registered_path), _) => {
            bail!(
                "worktree for branch {attempt_branch} is registered at {registered_path}, not \
                 the requested path {}. Remediation: `git worktree remove {registered_path}` \
                 first, then re-run.",
                worktree_path.display()
            );
        }
        // (e) dir present but not registered (stale from aborted add).
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

    // (f) registered but dir unreadable — handled implicitly: if registration says
    // it's there but `dir_exists` was false, we'd have hit arm (d) or fallen through.
    // git worktree repair is left for v1; the dir-absent + registered case fails
    // above with a clear message.

    // Step 8: runtime lock (F22). Acquired BEFORE setup lock release.
    let runtime_lock_path =
        locks_dir.join(format!("worktree-run-{capsule_id}-a{attempt_num}.lock"));
    let runtime_lock = acquire_runtime_lock(&runtime_lock_path, &worktree_path)?;

    // Step 9: setup lock released here as `setup_lock` falls out of scope.
    drop(setup_lock);

    Ok(IsolateState {
        worktree_path,
        canonical_capsule_dir,
        _runtime_lock: runtime_lock,
    })
}

fn validate_worktree_dir_override(
    p: &Path,
    main_root: &Path,
    capsule_dir: &Path,
) -> Result<PathBuf> {
    // Reject relative paths up-front. `fs::canonicalize` would resolve them
    // against the process cwd, which is unpredictable when `capsule work` is
    // invoked by an agent from a variable working directory — silently landing
    // the worktree in a surprising location. Force the caller to be explicit.
    if !p.is_absolute() {
        bail!(
            "--worktree-dir must be an absolute path (got {}); relative paths would resolve \
             against the current working directory and silently land elsewhere",
            p.display()
        );
    }

    // Canonicalize via parent if the path doesn't exist yet.
    let canonical = if p.exists() {
        fs::canonicalize(p).with_context(|| format!("canonicalize {}", p.display()))?
    } else {
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
        parent_canon.join(leaf)
    };

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
        // Allow the default subdir under capsule_dir, but not inside the store itself.
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
    child == parent || child.starts_with(parent)
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
    let out = run_git_capture(&["rev-parse", "--verify", rev])
        .with_context(|| format!("git rev-parse {rev}"))?;
    Ok(out.trim().to_string())
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
///
/// Tracks `current_path` as `&str` borrowed from `out` so only the matched
/// entry pays a `String` allocation. The prior shape pushed an owned `String`
/// for every `worktree ` line, even though every record except at most one is
/// discarded on the way to the match.
fn git_worktree_list_for_branch(branch: &str) -> Result<Option<String>> {
    let out = run_git_capture(&["worktree", "list", "--porcelain"])
        .context("git worktree list --porcelain")?;
    let mut current_path: Option<&str> = None;
    let want = format!("refs/heads/{branch}");
    for line in out.lines() {
        if let Some(rest) = line.strip_prefix("worktree ") {
            current_path = Some(rest);
        } else if let Some(rest) = line.strip_prefix("branch ") {
            if rest == want {
                return Ok(current_path.map(str::to_string));
            }
        } else if line.is_empty() {
            // Blank lines separate porcelain records; don't carry a path into the next one.
            current_path = None;
        }
    }
    Ok(None)
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

    #[test]
    fn path_within_basic() {
        assert!(path_within(Path::new("/a/b/c"), Path::new("/a/b")));
        assert!(path_within(Path::new("/a/b"), Path::new("/a/b")));
        assert!(!path_within(Path::new("/a/b"), Path::new("/a/b/c")));
        assert!(!path_within(Path::new("/x"), Path::new("/y")));
        // Sibling-prefix regression: `/a/bad` must NOT be considered within
        // `/a/b`. `Path::starts_with` is component-aware, so this guards
        // against a future regression to string-prefix matching.
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

    #[test]
    fn override_allows_default_subtree() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        let allowed = cap.join("worktrees/foo-a1");
        fs::create_dir_all(&cap).unwrap();
        fs::create_dir_all(allowed.parent().unwrap()).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        // `allowed` itself does not exist — exercises the parent-canonicalize branch.
        assert!(!allowed.exists());
        let got = validate_worktree_dir_override(&allowed, &main, &cap).unwrap();
        // Full-path equality (not just file_name): a regression in parent
        // canonicalization would still pass a file_name-only check.
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

    #[test]
    fn override_traversal_via_dotdot_is_canonicalized() {
        let td = tempdir().unwrap();
        let main = make_main_root(&td);
        let cap = main.join(".capsule");
        fs::create_dir_all(&cap).unwrap();
        let cap = fs::canonicalize(&cap).unwrap();
        // `cap/worktrees/../foreign/x` with `foreign` existing — verifies
        // that fs::canonicalize collapses `..` so the validation sees the
        // real target (inside the store, outside worktrees) and rejects.
        let foreign = cap.join("foreign");
        fs::create_dir_all(&foreign).unwrap();
        let traversal = cap.join("worktrees/../foreign/x");
        fs::create_dir_all(cap.join("worktrees")).unwrap();
        let err = validate_worktree_dir_override(&traversal, &main, &cap).unwrap_err();
        assert!(err.to_string().contains("inside the capsule store"));
    }
}
