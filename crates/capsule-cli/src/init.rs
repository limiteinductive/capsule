//! `capsule init` — store bootstrap + first-time UX.
//!
//! Beyond creating `<dir>/state.db`, this:
//! - appends an ignore rule for the store dir to the worktree-root `.gitignore`
//!   (skippable with `--no-gitignore`) — committing `state.db` would be actively bad;
//! - warns (non-fatal) if cwd isn't inside a git worktree;
//! - warns if the `git` CLI is missing or < 2.13 (DESIGN §3.1 requires
//!   `--force-with-lease`, client-side git ≥ 2.13).
//!
//! The hard check for git belongs in `capsule deploy verify` (DESIGN §8.2); `init`
//! only warns so the user can create a store offline / ahead of tooling.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde::Serialize;

/// Structured result of `capsule init`. Serialized under `--json`; formatted for
/// humans otherwise.
#[derive(Debug, Serialize)]
pub struct InitReport {
    pub dir: PathBuf,
    /// Absolute path of the `.gitignore` we wrote to, if any.
    pub gitignore_updated: Option<PathBuf>,
    /// Human-readable reason we skipped the `.gitignore` append.
    pub gitignore_skipped: Option<String>,
    /// Non-fatal worktree / git warnings.
    pub warnings: Vec<String>,
    pub next_steps: Vec<String>,
}

pub struct InitOpts {
    pub dir: PathBuf,
    pub no_gitignore: bool,
}

pub fn run(opts: InitOpts) -> Result<InitReport> {
    // Store bootstrap — opening the Store creates `<dir>/state.db` and applies migrations.
    let db = opts.dir.join("state.db");
    let _ = capsule_store::Store::open(&db)
        .with_context(|| format!("opening store at {}", db.display()))?;

    let mut warnings: Vec<String> = Vec::new();

    let worktree = detect_worktree();
    if worktree.is_none() {
        warnings.push(
            "cwd is not inside a git worktree; capsule needs a git repo with a remote before \
             `capsule land` can run"
                .to_string(),
        );
    }

    if let Some(msg) = check_git() {
        warnings.push(msg);
    }

    let (gitignore_updated, gitignore_skipped) =
        maybe_update_gitignore(&opts.dir, worktree.as_deref(), opts.no_gitignore)?;

    let next_steps = vec!["capsule list --available".to_string()];

    Ok(InitReport {
        dir: opts.dir,
        gitignore_updated,
        gitignore_skipped,
        warnings,
        next_steps,
    })
}

/// `Some(worktree_root)` if cwd is inside a git worktree; `None` otherwise.
fn detect_worktree() -> Option<PathBuf> {
    let out = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

/// Returns a warning string if git is missing or < 2.13.
fn check_git() -> Option<String> {
    let out = match Command::new("git").arg("--version").output() {
        Ok(o) if o.status.success() => o,
        _ => {
            return Some(
                "`git` not found on PATH; capsule shells out to git for land/reconcile".to_string(),
            )
        }
    };
    // Keep the lossy output as Cow so valid UTF-8 avoids an owned copy.
    let text = String::from_utf8_lossy(&out.stdout);
    match parse_git_version(&text) {
        Some((major, minor)) if (major, minor) < (2, 13) => Some(format!(
            "git {major}.{minor} detected; capsule requires >= 2.13 for `--force-with-lease`"
        )),
        Some(_) => None,
        None => Some(format!("could not parse git version from: {}", text.trim())),
    }
}

/// Extract `(major, minor)` from e.g. `git version 2.43.0` / `git version 2.39.5 (Apple Git-154)`.
fn parse_git_version(s: &str) -> Option<(u32, u32)> {
    let tail = s.trim().strip_prefix("git version ")?;
    let ver = tail.split_whitespace().next()?;
    let mut parts = ver.split('.');
    let major: u32 = parts.next()?.parse().ok()?;
    let minor: u32 = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Append a rule for the store dir to the worktree-root `.gitignore`, idempotently.
///
/// Returns `(Some(path), None)` on write, `(None, Some(reason))` on skip.
fn maybe_update_gitignore(
    store_dir: &Path,
    worktree: Option<&Path>,
    no_gitignore: bool,
) -> Result<(Option<PathBuf>, Option<String>)> {
    if no_gitignore {
        return Ok((None, Some("--no-gitignore".to_string())));
    }
    let Some(worktree) = worktree else {
        return Ok((None, Some("not inside a git worktree".to_string())));
    };

    // Canonicalize both sides so symlink-y tmpdirs (e.g. `/var/...` → `/private/var/...` on
    // macOS) compare equal. Store dir exists by now — `Store::open` created it.
    let abs_store_dir = fs::canonicalize(store_dir)
        .with_context(|| format!("canonicalizing {}", store_dir.display()))?;
    let abs_worktree = fs::canonicalize(worktree)
        .with_context(|| format!("canonicalizing worktree {}", worktree.display()))?;
    let rel = match abs_store_dir.strip_prefix(&abs_worktree) {
        Ok(r) => r.to_path_buf(),
        Err(_) => {
            return Ok((
                None,
                Some(format!(
                    "store dir {} is outside the worktree {}",
                    abs_store_dir.display(),
                    abs_worktree.display()
                )),
            ));
        }
    };

    // Gitignore is POSIX-sep + trailing slash to mark a dir.
    let mut line = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/");
    if line.is_empty() {
        return Ok((
            None,
            Some("store dir equals worktree root — refusing to ignore the repo".to_string()),
        ));
    }
    line.push('/');

    let gi_path = abs_worktree.join(".gitignore");
    let current = fs::read_to_string(&gi_path).unwrap_or_default();
    if current.lines().any(|l| l.trim() == line) {
        return Ok((None, Some("already present".to_string())));
    }

    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&gi_path)
        .with_context(|| format!("opening {} for append", gi_path.display()))?;
    if !current.is_empty() && !current.ends_with('\n') {
        f.write_all(b"\n")?;
    }
    writeln!(f, "{line}")?;
    Ok((Some(gi_path), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // `detect_worktree` reads process cwd; these tests mutate it. Serialize so
    // `cargo test` with the default multi-thread runner doesn't race.
    static CWD_LOCK: Mutex<()> = Mutex::new(());

    fn git_init(dir: &Path) {
        let status = Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir)
            .status()
            .expect("git init");
        assert!(status.success());
    }

    fn run_at(cwd: &Path, dir: PathBuf, no_gitignore: bool) -> Result<InitReport> {
        let _g = CWD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::current_dir().unwrap();
        std::env::set_current_dir(cwd).unwrap();
        let res = run(InitOpts { dir, no_gitignore });
        std::env::set_current_dir(prev).unwrap();
        res
    }

    #[test]
    fn init_appends_gitignore_in_worktree() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let store = tmp.path().join(".capsule");

        let r = run_at(tmp.path(), store.clone(), false).unwrap();

        let gi = fs::canonicalize(tmp.path()).unwrap().join(".gitignore");
        assert_eq!(r.gitignore_updated.as_deref(), Some(gi.as_path()));
        assert!(r.gitignore_skipped.is_none());
        let contents = fs::read_to_string(&gi).unwrap();
        assert!(
            contents.lines().any(|l| l == ".capsule/"),
            "got: {contents:?}"
        );
        assert!(store.join("state.db").exists());
    }

    #[test]
    fn init_gitignore_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let store = tmp.path().join(".capsule");

        let _ = run_at(tmp.path(), store.clone(), false).unwrap();
        let r2 = run_at(tmp.path(), store, false).unwrap();

        assert!(r2.gitignore_updated.is_none());
        assert_eq!(r2.gitignore_skipped.as_deref(), Some("already present"));
        let contents = fs::read_to_string(tmp.path().join(".gitignore")).unwrap();
        let count = contents.lines().filter(|l| *l == ".capsule/").count();
        assert_eq!(count, 1);
    }

    #[test]
    fn init_no_gitignore_flag_skips() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let store = tmp.path().join(".capsule");

        let r = run_at(tmp.path(), store, true).unwrap();

        assert!(r.gitignore_updated.is_none());
        assert_eq!(r.gitignore_skipped.as_deref(), Some("--no-gitignore"));
        assert!(!tmp.path().join(".gitignore").exists());
    }

    #[test]
    fn init_outside_worktree_skips_gitignore_and_warns() {
        let tmp = TempDir::new().unwrap();
        // no git init
        let store = tmp.path().join(".capsule");

        let r = run_at(tmp.path(), store, false).unwrap();

        assert!(r.gitignore_updated.is_none());
        assert_eq!(
            r.gitignore_skipped.as_deref(),
            Some("not inside a git worktree")
        );
        assert!(
            r.warnings
                .iter()
                .any(|w| w.contains("not inside a git worktree")),
            "warnings: {:?}",
            r.warnings
        );
    }

    #[test]
    fn init_custom_dir_inside_worktree_uses_that_path() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let store = tmp.path().join("work/my-capsule");

        let r = run_at(tmp.path(), store, false).unwrap();

        assert!(r.gitignore_updated.is_some());
        let contents = fs::read_to_string(tmp.path().join(".gitignore")).unwrap();
        assert!(
            contents.lines().any(|l| l == "work/my-capsule/"),
            "got: {contents:?}"
        );
    }

    #[test]
    fn parse_git_version_samples() {
        assert_eq!(parse_git_version("git version 2.43.0\n"), Some((2, 43)));
        assert_eq!(
            parse_git_version("git version 2.39.5 (Apple Git-154)\n"),
            Some((2, 39))
        );
        assert_eq!(parse_git_version("nope"), None);
    }
}
