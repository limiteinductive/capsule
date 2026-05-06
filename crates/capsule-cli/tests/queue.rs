//! Integration tests for `capsule queue`.

use std::path::Path;
use std::process::Command;

use assert_cmd::cargo::CommandCargoExt;
use serde::Deserialize;

fn capsule(cwd: &Path, store_dir: &Path, args: &[&str]) -> std::process::Output {
    Command::cargo_bin("capsule")
        .unwrap()
        .args(["--dir", store_dir.to_str().unwrap()])
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("capsule")
}

fn create_capsule(cwd: &Path, store: &Path, id: &str, scope: &str, depends_on: &[&str]) {
    let mut args = vec![
        "create",
        "--id",
        id,
        "--title",
        id,
        "--description",
        id,
        "--acceptance-cmd",
        "true",
        "--base-ref",
        "main",
        "--scope",
        scope,
    ];
    for dep in depends_on {
        args.push("--depends-on");
        args.push(dep);
    }

    let out = capsule(cwd, store, &args);
    assert!(out.status.success(), "create {id} failed: {out:?}");
}

#[derive(Deserialize)]
struct QueueReport {
    counts: QueueCounts,
    deploy_verify_passed: bool,
    available: Vec<CapsuleSummary>,
    active: Vec<CapsuleSummary>,
    accepted: Vec<CapsuleSummary>,
}

#[derive(Deserialize)]
struct QueueCounts {
    planned: usize,
    active: usize,
    accepted: usize,
}

#[derive(Deserialize)]
struct CapsuleSummary {
    id: String,
    status: String,
}

#[test]
fn queue_reports_counts_available_work_and_json_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("cap");

    let init = capsule(tmp.path(), &store, &["init"]);
    assert!(init.status.success(), "init failed: {init:?}");

    create_capsule(tmp.path(), &store, "dep", "deps", &[]);
    create_capsule(tmp.path(), &store, "blocked", "src/api", &["dep"]);
    create_capsule(tmp.path(), &store, "ready", "src/api", &[]);

    let claim = capsule(
        tmp.path(),
        &store,
        &[
            "claim",
            "dep",
            "--owner",
            "agent",
            "--session",
            "sess",
            "--base-sha",
            "0000000000000000000000000000000000000000",
        ],
    );
    assert!(claim.status.success(), "claim failed: {claim:?}");

    let text = capsule(
        tmp.path(),
        &store,
        &["queue", "--scope-overlaps", "src", "--limit", "5"],
    );
    assert!(text.status.success(), "queue text failed: {text:?}");
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(
        stdout.contains("queue\tplanned=2\tactive=1\taccepted=0"),
        "{stdout}"
    );
    assert!(
        stdout.contains("\tavailable=1\tdeploy_verify=missing"),
        "{stdout}"
    );
    assert!(
        stdout.contains("available:\nready\tplanned\tmain\t[src/api]\tready"),
        "{stdout}"
    );
    assert!(
        stdout.contains("active:\ndep\tactive\tmain\t[deps]\tdep"),
        "{stdout}"
    );
    assert!(stdout.contains("accepted: none"), "{stdout}");
    assert!(!stdout.contains("blocked\tplanned"), "{stdout}");

    let json = capsule(
        tmp.path(),
        &store,
        &["--json", "queue", "--scope-overlaps", "src", "--limit", "5"],
    );
    assert!(json.status.success(), "queue json failed: {json:?}");
    let stdout = String::from_utf8(json.stdout).unwrap();
    let report: QueueReport = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("parse json: {e}\nstdout:\n{stdout}"));
    assert!(!report.deploy_verify_passed);
    assert_eq!(report.counts.planned, 2);
    assert_eq!(report.counts.active, 1);
    assert_eq!(report.counts.accepted, 0);
    assert_eq!(report.available.len(), 1);
    assert_eq!(report.available[0].id, "ready");
    assert_eq!(report.available[0].status, "planned");
    assert_eq!(report.active.len(), 1);
    assert_eq!(report.active[0].id, "dep");
    assert!(report.accepted.is_empty());
}
