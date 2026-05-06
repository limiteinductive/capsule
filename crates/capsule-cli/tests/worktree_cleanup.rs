//! Integration tests for `capsule cleanup-worktrees`.

use std::path::{Path, PathBuf};
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

fn capsule(cwd: &Path, store_dir: &Path, args: &[&str]) -> std::process::Output {
    Command::cargo_bin("capsule")
        .unwrap()
        .args(["--dir", store_dir.to_str().unwrap()])
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("capsule")
}

struct Fixture {
    _tmp: tempfile::TempDir,
    repo: PathBuf,
    store: PathBuf,
    base_sha: String,
}

fn setup() -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    let store = repo.join(".capsule");
    std::fs::create_dir_all(repo.join("src")).unwrap();

    git(&repo, &["init", "-q", "-b", "main"]);
    git(&repo, &["config", "user.email", "a@b"]);
    git(&repo, &["config", "user.name", "T"]);
    std::fs::write(repo.join("src/lib.rs"), "pub fn f() {}\n").unwrap();
    git(&repo, &["add", "src/lib.rs"]);
    git(&repo, &["commit", "-q", "-m", "base"]);
    let base_sha = git(&repo, &["rev-parse", "HEAD"]);

    let init = capsule(&repo, &store, &["init"]);
    assert!(init.status.success(), "init failed: {init:?}");

    Fixture {
        _tmp: tmp,
        repo,
        store,
        base_sha,
    }
}

fn create_claim_and_materialize(fx: &Fixture) -> PathBuf {
    let create = capsule(
        &fx.repo,
        &fx.store,
        &[
            "create",
            "--id",
            "cleanup",
            "--title",
            "cleanup",
            "--description",
            "cleanup",
            "--acceptance-cmd",
            "git status --short",
            "--base-ref",
            "main",
            "--scope",
            "src",
        ],
    );
    assert!(create.status.success(), "create failed: {create:?}");

    let claim = capsule(
        &fx.repo,
        &fx.store,
        &[
            "claim",
            "cleanup",
            "--owner",
            "me",
            "--session",
            "s",
            "--base-sha",
            &fx.base_sha,
        ],
    );
    assert!(claim.status.success(), "claim failed: {claim:?}");

    let work = capsule(
        &fx.repo,
        &fx.store,
        &[
            "work",
            "cleanup",
            "--session",
            "s",
            "--isolate",
            "worktree",
            "--",
            "git",
            "status",
            "--short",
        ],
    );
    assert!(work.status.success(), "work failed: {work:?}");

    let worktree = fx.store.join("worktrees/cleanup-a1");
    assert!(worktree.exists(), "worktree should exist");
    worktree
}

#[test]
fn cleanup_worktrees_dry_runs_then_removes_terminal_default_worktree() {
    let fx = setup();
    let worktree = create_claim_and_materialize(&fx);

    let active_cleanup = capsule(&fx.repo, &fx.store, &["cleanup-worktrees", "--dry-run"]);
    assert!(
        active_cleanup.status.success(),
        "active dry-run failed: {active_cleanup:?}"
    );
    let stdout = String::from_utf8_lossy(&active_cleanup.stdout);
    assert!(stdout.contains("would_remove=0"), "{stdout}");
    assert!(worktree.exists(), "active worktree must not be removed");

    let abandon = capsule(
        &fx.repo,
        &fx.store,
        &["abandon", "cleanup", "--session", "s", "--reason", "done"],
    );
    assert!(abandon.status.success(), "abandon failed: {abandon:?}");

    let dry_run = capsule(&fx.repo, &fx.store, &["cleanup-worktrees", "--dry-run"]);
    assert!(dry_run.status.success(), "dry-run failed: {dry_run:?}");
    let stdout = String::from_utf8_lossy(&dry_run.stdout);
    assert!(stdout.contains("would_remove=1"), "{stdout}");
    assert!(stdout.contains("cleanup-a1"), "{stdout}");
    assert!(worktree.exists(), "dry-run must not remove the worktree");

    let cleanup = capsule(&fx.repo, &fx.store, &["cleanup-worktrees"]);
    assert!(cleanup.status.success(), "cleanup failed: {cleanup:?}");
    let stdout = String::from_utf8_lossy(&cleanup.stdout);
    assert!(stdout.contains("removed=1"), "{stdout}");
    assert!(!worktree.exists(), "cleanup should remove the worktree");

    let listed = git(&fx.repo, &["worktree", "list", "--porcelain"]);
    assert!(
        !listed.contains("branch refs/heads/capsules/cleanup/a1"),
        "{listed}"
    );
}
