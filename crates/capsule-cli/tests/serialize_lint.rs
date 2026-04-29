//! Integration test for the PROPOSAL §3.2 attest-time serialize-paths lint.
//!
//! Spins up a tempdir git repo with two commits that touch `Cargo.lock`,
//! then runs `capsule attest` twice: once with a scope that excludes the
//! lockfile (expect exit 2 + `serialize_path_uncovered` on stderr), once
//! with a scope that covers it (expect attest to succeed).

use std::path::Path;
use std::process::Command;

use assert_cmd::cargo::CommandCargoExt;

fn git(repo: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap_or_else(|e| panic!("git {args:?}: {e}"));
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn capsule(store_dir: &Path, repo_dir: &Path, args: &[&str]) -> std::process::Output {
    Command::cargo_bin("capsule")
        .unwrap()
        .args(["--dir", store_dir.to_str().unwrap()])
        .args(args)
        .current_dir(repo_dir)
        .output()
        .expect("capsule")
}

struct Fixture {
    _tmp: tempfile::TempDir,
    repo: std::path::PathBuf,
    store: std::path::PathBuf,
    base_sha: String,
    verified_sha: String,
}

fn setup() -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("work");
    let store = tmp.path().join("cap");
    std::fs::create_dir_all(&repo).unwrap();

    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["config", "user.email", "a@b"]);
    git(&repo, &["config", "user.name", "T"]);
    std::fs::write(repo.join("Cargo.lock"), "v1\n").unwrap();
    git(&repo, &["add", "Cargo.lock"]);
    git(&repo, &["commit", "-q", "-m", "base"]);
    let base_sha = git(&repo, &["rev-parse", "HEAD"]);

    std::fs::write(repo.join("Cargo.lock"), "v2\n").unwrap();
    git(&repo, &["add", "Cargo.lock"]);
    git(&repo, &["commit", "-q", "-m", "bump lock"]);
    let verified_sha = git(&repo, &["rev-parse", "HEAD"]);

    let init = capsule(&store, &repo, &["init"]);
    assert!(init.status.success(), "init: {init:?}");

    Fixture {
        _tmp: tmp,
        repo,
        store,
        base_sha,
        verified_sha,
    }
}

fn create_and_claim(fx: &Fixture, capsule_id: &str, scope: &[&str]) {
    let mut create = vec![
        "create",
        "--id",
        capsule_id,
        "--title",
        "t",
        "--description",
        "d",
        "--acceptance-cmd",
        "true",
        "--base-ref",
        "main",
    ];
    for s in scope {
        create.push("--scope");
        create.push(s);
    }
    let out = capsule(&fx.store, &fx.repo, &create);
    assert!(out.status.success(), "create: {out:?}");

    let claim = capsule(
        &fx.store,
        &fx.repo,
        &[
            "claim",
            capsule_id,
            "--owner",
            "me",
            "--session",
            "s",
            "--base-sha",
            &fx.base_sha,
        ],
    );
    assert!(claim.status.success(), "claim: {claim:?}");
}

#[test]
fn attest_rejects_uncovered_lockfile_touch() {
    let fx = setup();
    create_and_claim(&fx, "probe", &["src"]);

    let out = capsule(
        &fx.store,
        &fx.repo,
        &[
            "attest",
            "probe",
            "--session",
            "s",
            "--verified-sha",
            &fx.verified_sha,
            "--command",
            "true",
            "--exit-code",
            "0",
            "--duration-ms",
            "1",
            "--log-ref",
            "file:///dev/null",
        ],
    );
    assert_eq!(out.status.code(), Some(2), "expected exit 2, got {out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("serialize_path_uncovered: Cargo.lock"),
        "stderr did not flag Cargo.lock: {stderr}"
    );
}

#[test]
fn attest_passes_when_scope_covers_lockfile() {
    let fx = setup();
    create_and_claim(&fx, "probe", &["Cargo.lock"]);

    let out = capsule(
        &fx.store,
        &fx.repo,
        &[
            "attest",
            "probe",
            "--session",
            "s",
            "--verified-sha",
            &fx.verified_sha,
            "--command",
            "true",
            "--exit-code",
            "0",
            "--duration-ms",
            "1",
            "--log-ref",
            "file:///dev/null",
        ],
    );
    assert!(
        out.status.success(),
        "attest with covering scope failed: {out:?}"
    );
}

#[test]
fn skip_serialize_lint_bypasses_with_warning() {
    let fx = setup();
    create_and_claim(&fx, "probe", &["src"]);

    let out = capsule(
        &fx.store,
        &fx.repo,
        &[
            "attest",
            "probe",
            "--session",
            "s",
            "--verified-sha",
            &fx.verified_sha,
            "--command",
            "true",
            "--exit-code",
            "0",
            "--duration-ms",
            "1",
            "--log-ref",
            "file:///dev/null",
            "--skip-serialize-lint",
        ],
    );
    assert!(out.status.success(), "skip-bypass attest failed: {out:?}");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--skip-serialize-lint"),
        "expected bypass warning on stderr: {stderr}"
    );
}
