//! Integration tests for `capsule doctor`.

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

#[test]
fn doctor_reports_missing_store_without_creating_it() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("cap");

    let out = capsule(tmp.path(), &store, &["doctor"]);

    assert!(!out.status.success(), "doctor should fail without a store");
    assert!(
        !store.join("state.db").exists(),
        "doctor must not create a missing store"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("doctor\tfail"), "{stdout}");
    assert!(stdout.contains("fail\tstore"), "{stdout}");
    assert!(stdout.contains("hint\tstore\trun capsule init"), "{stdout}");
}

#[test]
fn doctor_succeeds_after_init_and_warns_before_deploy_verify() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("cap");

    let init = capsule(tmp.path(), &store, &["init"]);
    assert!(init.status.success(), "init failed: {init:?}");

    let out = capsule(tmp.path(), &store, &["doctor"]);

    assert!(
        out.status.success(),
        "doctor should tolerate warnings: {out:?}"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("doctor\tok"), "{stdout}");
    assert!(stdout.contains("ok\tstore"), "{stdout}");
    assert!(stdout.contains("warn\tdeploy_verify"), "{stdout}");
}

#[derive(Deserialize)]
struct DoctorReport {
    ok: bool,
    checks: Vec<DoctorCheck>,
}

#[derive(Deserialize)]
struct DoctorCheck {
    name: String,
    status: String,
}

#[test]
fn doctor_json_reports_checks() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("cap");

    let init = capsule(tmp.path(), &store, &["init"]);
    assert!(init.status.success(), "init failed: {init:?}");

    let out = capsule(tmp.path(), &store, &["--json", "doctor"]);
    assert!(out.status.success(), "doctor json failed: {out:?}");

    let stdout = String::from_utf8(out.stdout).unwrap();
    let report: DoctorReport = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("parse json: {e}\nstdout:\n{stdout}"));
    assert!(report.ok, "warnings should not make doctor fail");
    assert!(report
        .checks
        .iter()
        .any(|c| c.name == "store" && c.status == "ok"));
    assert!(report
        .checks
        .iter()
        .any(|c| c.name == "deploy_verify" && c.status == "warn"));
}
