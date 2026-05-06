//! Attest-time lint for "serialized" paths (PROPOSAL §3.2).
//!
//! Reads `git diff --name-only <base_sha>..<verified_sha>` from `repo_dir`
//! and rejects the attest if any touched path is in `required` but not
//! covered by the capsule's `scope_prefixes`. Component-wise prefix overlap
//! follows DESIGN §7.0 / `CanonicalPath::overlaps`.
//!
//! This is a CLI-layer guard, not a `Store::attest` precondition. DESIGN
//! §7.1.0 mandates attest is a single DB transaction with no git effect;
//! the lint runs *before* the store call (and can be skipped with
//! `--skip-serialize-lint`). It does not change protocol semantics.

use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use capsule_core::path::CanonicalPath;

/// Returned for each diff entry that fails the lint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uncovered {
    pub path: String,
    pub matched_required: String,
}

/// Run the lint. Returns `Ok(Vec::new())` when every required-touching path
/// is covered by some scope prefix.
pub fn check_attest_diff(
    repo_dir: &Path,
    base_sha: &str,
    verified_sha: &str,
    scope_prefixes: &[CanonicalPath],
    required: &[CanonicalPath],
) -> Result<Vec<Uncovered>> {
    if required.is_empty() {
        return Ok(Vec::new());
    }
    let touched = git_diff_name_only(repo_dir, base_sha, verified_sha)?;
    let touched = touched
        .into_iter()
        .map(|raw| {
            let path = CanonicalPath::new(&raw)
                .with_context(|| format!("canonicalizing diff path {raw:?}"))?;
            Ok((raw, path))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(lint_paths(&touched, scope_prefixes, required))
}

/// A required entry "matches" a touched path iff the touched path is at or
/// under the required path. `CanonicalPath::overlaps` already implements
/// component-wise prefix overlap symmetrically, so reuse it.
fn paths_match(required: &CanonicalPath, touched: &CanonicalPath) -> bool {
    required.overlaps(touched)
}

fn lint_paths(
    touched: &[(String, CanonicalPath)],
    scope_prefixes: &[CanonicalPath],
    required: &[CanonicalPath],
) -> Vec<Uncovered> {
    let mut findings = Vec::new();
    for (raw, touched_path) in touched {
        let Some(req_match) = required.iter().find(|r| paths_match(r, touched_path)) else {
            continue;
        };
        if !scope_prefixes.iter().any(|s| s.overlaps(touched_path)) {
            findings.push(Uncovered {
                path: raw.clone(),
                matched_required: req_match.as_str().to_string(),
            });
        }
    }
    findings
}

fn git_diff_name_only(repo_dir: &Path, base_sha: &str, verified_sha: &str) -> Result<Vec<String>> {
    let out = Command::new("git")
        .args([
            "diff",
            "--name-only",
            "-z",
            &format!("{base_sha}..{verified_sha}"),
        ])
        .current_dir(repo_dir)
        .output()?;
    if !out.status.success() {
        return Err(anyhow!(
            "git diff --name-only {}..{} failed in {}: {}",
            base_sha,
            verified_sha,
            repo_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim(),
        ));
    }
    out.stdout
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|bytes| {
            let s = std::str::from_utf8(bytes)
                .with_context(|| format!("non-utf8 path in diff: {bytes:?}"))?;
            Ok(s.to_string())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(s: &str) -> CanonicalPath {
        CanonicalPath::new(s).unwrap()
    }

    #[test]
    fn paths_match_treats_required_as_prefix() {
        // `Cargo.lock` matches itself exactly; not `Cargo.lock.bak`
        // (component-wise — DESIGN §7.0).
        assert!(paths_match(&cp("Cargo.lock"), &cp("Cargo.lock")));
        assert!(!paths_match(&cp("Cargo.lock"), &cp("Cargo.lock.bak")));
        // A directory required entry covers its descendants.
        assert!(paths_match(&cp("vendor"), &cp("vendor/foo.rs")));
    }

    #[test]
    fn empty_required_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let out = check_attest_diff(dir.path(), "HEAD", "HEAD", &[cp("src")], &[]).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn lint_passes_when_scope_covers_touched_lockfile() {
        let touched = vec![
            ("Cargo.lock".to_string(), cp("Cargo.lock")),
            ("src/main.rs".to_string(), cp("src/main.rs")),
        ];
        let scope = vec![cp("Cargo.lock"), cp("src")];
        let required = vec![cp("Cargo.lock")];
        assert_eq!(lint_paths(&touched, &scope, &required), vec![]);
    }

    #[test]
    fn lint_flags_uncovered_lockfile() {
        let touched = vec![
            ("Cargo.lock".to_string(), cp("Cargo.lock")),
            ("src/main.rs".to_string(), cp("src/main.rs")),
        ];
        let scope = vec![cp("src")];
        let required = vec![cp("Cargo.lock")];

        let v = lint_paths(&touched, &scope, &required);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].path, "Cargo.lock");
        assert_eq!(v[0].matched_required, "Cargo.lock");
    }

    #[test]
    fn lint_handles_directory_prefix_required_entry() {
        let touched = vec![(
            "db/migrations/2024_01_01_init.sql".to_string(),
            cp("db/migrations/2024_01_01_init.sql"),
        )];
        let scope = vec![cp("src")];
        let required = vec![cp("db/migrations/")];

        let v = lint_paths(&touched, &scope, &required);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].matched_required, "db/migrations");
    }

    #[test]
    fn lint_does_not_flag_adjacent_files() {
        let touched = vec![("Cargo.toml".to_string(), cp("Cargo.toml"))];
        let scope = vec![cp("src")];
        let required = vec![cp("Cargo.lock")];
        assert_eq!(lint_paths(&touched, &scope, &required), vec![]);
    }

    #[test]
    fn lint_ignores_paths_not_in_required_list() {
        let touched = vec![("README.md".to_string(), cp("README.md"))];
        let scope = vec![cp("src")];
        let required = vec![cp("Cargo.lock")];
        assert_eq!(lint_paths(&touched, &scope, &required), vec![]);
    }
}
