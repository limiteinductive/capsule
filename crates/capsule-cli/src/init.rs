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

/// Render a path as a gitignore directory pattern: POSIX `/` separators
/// regardless of host OS, with a trailing `/` so git treats it as a dir.
/// Returns `None` for the empty path (worktree root), where ignoring the
/// pattern would shadow the entire repo.
fn format_gitignore_dir_pattern(rel: &Path) -> Option<String> {
    let mut out = String::new();
    for c in rel.components() {
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(&c.as_os_str().to_string_lossy());
    }
    if out.is_empty() {
        return None;
    }
    out.push('/');
    Some(out)
}

/// Append a rule for the store dir to the worktree-root `.gitignore`, idempotently.
///
/// Returns `(Some(path), None)` on write, `(None, Some(reason))` on skip.
///
/// Both sides are canonicalized before comparison so symlink-y tmpdirs
/// (e.g. macOS `/var/...` → `/private/var/...`) compare equal. The store
/// dir is guaranteed to exist by now — `Store::open` creates it.
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

    let Some(line) = format_gitignore_dir_pattern(&rel) else {
        return Ok((
            None,
            Some("store dir equals worktree root — refusing to ignore the repo".to_string()),
        ));
    };

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

    /// `detect_worktree` reads process cwd; these tests mutate it. Serialize
    /// so `cargo test` with the default multi-thread runner doesn't race.
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

    /// Pin three shapes:
    /// - Empty path = worktree root → `None` (refuse; would shadow whole repo).
    /// - Single component → trailing `/` only.
    /// - Nested → `/`-separated, single trailing `/`.
    #[test]
    fn format_gitignore_dir_pattern_shape() {
        assert_eq!(format_gitignore_dir_pattern(Path::new("")), None);
        assert_eq!(
            format_gitignore_dir_pattern(Path::new(".capsule")).as_deref(),
            Some(".capsule/"),
        );
        assert_eq!(
            format_gitignore_dir_pattern(Path::new("var/cap")).as_deref(),
            Some("var/cap/"),
        );
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

    /// Tempdir without `git init` — capsule must still run, skip the
    /// gitignore append, and emit the not-in-worktree warning.
    #[test]
    fn init_outside_worktree_skips_gitignore_and_warns() {
        let tmp = TempDir::new().unwrap();
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

    /// `maybe_update_gitignore` prepends a `\n` before writing its rule when
    /// the existing file has content but no trailing newline — without this
    /// guard the new entry would concatenate with the last line, producing a
    /// malformed `existing_rule.capsule/` token that git would silently
    /// ignore. Pin both branches:
    /// 1. Existing content ends without `\n` → result has the existing line
    ///    intact AND `.capsule/` on its own line (i.e. the prepended `\n`
    ///    landed).
    /// 2. The total line count goes up by exactly one (no extra blank).
    #[test]
    fn init_gitignore_without_trailing_newline_keeps_existing_intact() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let gi = tmp.path().join(".gitignore");
        fs::write(&gi, "node_modules").unwrap();
        assert!(!fs::read_to_string(&gi).unwrap().ends_with('\n'));

        let store = tmp.path().join(".capsule");
        let r = run_at(tmp.path(), store, false).unwrap();
        assert!(r.gitignore_updated.is_some());

        let contents = fs::read_to_string(&gi).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines,
            vec!["node_modules", ".capsule/"],
            "unexpected .gitignore contents: {contents:?}"
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

    /// Git for Windows emits extra dotted suffixes like
    /// `git version 2.43.0.windows.1`; only major/minor matter here.
    #[test]
    fn parse_git_version_accepts_windows_suffix() {
        assert_eq!(
            parse_git_version("git version 2.43.0.windows.1\n"),
            Some((2, 43))
        );
    }

    /// Malformed-version-string corners: prefix matches but the value is
    /// missing or non-numeric. Each must yield `None` (caller treats it as
    /// "git version unknown") rather than a partial `Some((major, 0))` that
    /// would silently bypass version-floor checks.
    #[test]
    fn parse_git_version_rejects_malformed() {
        assert_eq!(parse_git_version("git version "), None);
        assert_eq!(parse_git_version("git version 2"), None);
        assert_eq!(parse_git_version("git version a.b.c"), None);
        assert_eq!(parse_git_version("git version 2.x"), None);
    }

    /// Pin the two `maybe_update_gitignore` safety guards beyond the
    /// `--no-gitignore` and not-in-worktree skips already covered: store
    /// dir resolves outside the worktree, and store dir equals the
    /// worktree root (would shadow the entire repo). Each test pre-seeds
    /// `.gitignore` with a sentinel and asserts the file is byte-identical
    /// afterwards, so the guard is proven not just to skip a *create* but
    /// also to refuse to *append* into an existing `.gitignore`.
    #[test]
    fn init_gitignore_refuses_store_dir_outside_worktree() {
        let tmp_wt = TempDir::new().unwrap();
        git_init(tmp_wt.path());
        let gi = tmp_wt.path().join(".gitignore");
        fs::write(&gi, "node_modules\n").unwrap();
        let tmp_outside = TempDir::new().unwrap();
        let store = tmp_outside.path().join(".capsule");

        let r = run_at(tmp_wt.path(), store, false).unwrap();

        assert!(r.gitignore_updated.is_none());
        assert!(
            r.gitignore_skipped
                .as_deref()
                .is_some_and(|s| s.contains("outside the worktree")),
            "skip reason: {:?}",
            r.gitignore_skipped
        );
        assert_eq!(fs::read_to_string(&gi).unwrap(), "node_modules\n");
    }

    /// Store dir == worktree root: stripping the worktree prefix would yield
    /// an empty pattern, which `format_gitignore_dir_pattern` rejects rather
    /// than emit a `/` rule that would ignore the entire repo.
    #[test]
    fn init_gitignore_refuses_store_dir_equals_worktree_root() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let gi = tmp.path().join(".gitignore");
        fs::write(&gi, "node_modules\n").unwrap();
        let store = tmp.path().to_path_buf();

        let r = run_at(tmp.path(), store, false).unwrap();

        assert!(r.gitignore_updated.is_none());
        assert!(
            r.gitignore_skipped
                .as_deref()
                .is_some_and(|s| s.contains("refusing to ignore the repo")),
            "skip reason: {:?}",
            r.gitignore_skipped
        );
        assert_eq!(fs::read_to_string(&gi).unwrap(), "node_modules\n");
    }

    /// Sibling to `init_gitignore_without_trailing_newline_keeps_existing_intact`:
    /// when `.gitignore` already ends with `\n`, appending `.capsule/` must
    /// NOT insert an extra blank line. Together the two tests pin the
    /// `current.ends_with('\n')` branch of the prepend-newline guard from
    /// both sides — this one fixes false-positives, the sibling fixes
    /// false-negatives.
    #[test]
    fn init_gitignore_with_trailing_newline_does_not_double_blank() {
        let tmp = TempDir::new().unwrap();
        git_init(tmp.path());
        let gi = tmp.path().join(".gitignore");
        fs::write(&gi, "node_modules\n").unwrap();

        let store = tmp.path().join(".capsule");
        let r = run_at(tmp.path(), store, false).unwrap();
        assert!(r.gitignore_updated.is_some());

        assert_eq!(
            fs::read_to_string(&gi).unwrap(),
            "node_modules\n.capsule/\n",
        );
    }
}
