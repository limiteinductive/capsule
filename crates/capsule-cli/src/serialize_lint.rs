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

use anyhow::{anyhow, Result};
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
    let mut findings = Vec::new();
    for raw in touched {
        let Ok(touched_path) = CanonicalPath::new(&raw) else {
            // Untrackable filenames (NFC-violating, etc.) skip the lint —
            // the store side will reject the attest later if it matters.
            continue;
        };
        let Some(req_match) = required.iter().find(|r| paths_match(r, &touched_path)) else {
            continue;
        };
        if !scope_prefixes.iter().any(|s| s.overlaps(&touched_path)) {
            findings.push(Uncovered {
                path: raw,
                matched_required: req_match.as_str().to_string(),
            });
        }
    }
    Ok(findings)
}

/// A required entry "matches" a touched path iff the touched path is at or
/// under the required path. `CanonicalPath::overlaps` already implements
/// component-wise prefix overlap symmetrically, so reuse it.
fn paths_match(required: &CanonicalPath, touched: &CanonicalPath) -> bool {
    required.overlaps(touched)
}

fn git_diff_name_only(repo_dir: &Path, base_sha: &str, verified_sha: &str) -> Result<Vec<String>> {
    let out = Command::new("git")
        .args([
            "diff",
            "--name-only",
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
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| !l.is_empty())
        .map(|s| s.to_string())
        .collect())
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
}
