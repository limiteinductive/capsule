//! Integration test for `capsule deploy verify --hermetic` (DESIGN §8.2).
//!
//! Spawns the CLI as a subprocess against a tempdir store, parses the JSON
//! report, and asserts every test case in the ACL suite passes against the
//! reference pre-receive hook.

use std::process::Command;

use assert_cmd::cargo::CommandCargoExt;
use serde::Deserialize;

#[derive(Deserialize)]
struct Report {
    mode_label: String,
    base_ref: String,
    all_passed: bool,
    passed: usize,
    failed: usize,
    tests: Vec<TestEntry>,
}

#[derive(Deserialize)]
struct TestEntry {
    name: String,
    status: String,
    evidence: String,
}

#[test]
fn hermetic_acl_suite_passes() {
    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join("cap");

    let init = Command::cargo_bin("capsule")
        .unwrap()
        .args(["--dir", store_dir.to_str().unwrap(), "init"])
        .output()
        .expect("capsule init");
    assert!(init.status.success(), "init failed: {init:?}");

    let out = Command::cargo_bin("capsule")
        .unwrap()
        .args([
            "--dir",
            store_dir.to_str().unwrap(),
            "--json",
            "deploy-verify",
            "--hermetic",
        ])
        .output()
        .expect("capsule deploy-verify");
    assert!(out.status.success(), "deploy-verify exit nonzero: {out:?}");

    let stdout = String::from_utf8(out.stdout).unwrap();
    let report: Report = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("parse json: {e}\nstdout:\n{stdout}"));

    assert_eq!(report.mode_label, "hermetic");
    assert_eq!(report.base_ref, "main");
    assert!(report.all_passed, "tests: {:?}", failed_names(&report));
    assert_eq!(report.failed, 0);
    assert_eq!(report.passed, report.tests.len());
    // Pin the case set so a future spec change forces a deliberate test edit.
    let names: Vec<&str> = report.tests.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "outsider_witness_create",
            "worker_witness_create",
            "lander_witness_create",
            "lander_idempotent_replay",
            "witness_oid_mismatch_atomic",
            "outsider_force_push_base_ref",
            "outsider_wildcard_witness",
            "lander_witness_delete",
            "outsider_witness_delete",
        ],
    );
}

#[test]
fn deploy_verify_records_pass_row() {
    // A fresh store has no recorded deploy-verify pass; running the suite
    // installs exactly one row in `deploy_verify_pass`. `Store::land`'s
    // gate is exercised via this row's presence (DESIGN §8.2).
    let dir = tempfile::tempdir().unwrap();
    let store_dir = dir.path().join("cap");

    let init = Command::cargo_bin("capsule")
        .unwrap()
        .args(["--dir", store_dir.to_str().unwrap(), "init"])
        .output()
        .expect("capsule init");
    assert!(init.status.success());

    let db = store_dir.join("state.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM deploy_verify_pass", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "fresh store should have no recorded pass");
    drop(conn);

    let dv = Command::cargo_bin("capsule")
        .unwrap()
        .args([
            "--dir",
            store_dir.to_str().unwrap(),
            "deploy-verify",
            "--hermetic",
        ])
        .output()
        .expect("deploy-verify");
    assert!(dv.status.success());

    let conn = rusqlite::Connection::open(&db).unwrap();
    let (count, mode, base_ref): (i64, String, String) = conn
        .query_row(
            "SELECT COUNT(*), COALESCE(MAX(mode), ''), COALESCE(MAX(base_ref), '') \
             FROM deploy_verify_pass",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(mode, "hermetic");
    assert_eq!(base_ref, "main");
}

fn failed_names(report: &Report) -> Vec<(String, String)> {
    report
        .tests
        .iter()
        .filter(|t| t.status != "pass")
        .map(|t| (t.name.clone(), t.evidence.clone()))
        .collect()
}
