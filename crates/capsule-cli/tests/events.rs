//! Integration tests for `capsule events`.

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

fn setup(cwd: &Path, store: &Path) {
    let init = capsule(cwd, store, &["init"]);
    assert!(init.status.success(), "init failed: {init:?}");

    let create = capsule(
        cwd,
        store,
        &[
            "create",
            "--id",
            "events",
            "--title",
            "events",
            "--description",
            "events",
            "--acceptance-cmd",
            "true",
            "--base-ref",
            "main",
            "--scope",
            "src",
        ],
    );
    assert!(create.status.success(), "create failed: {create:?}");

    let amend = capsule(cwd, store, &["amend", "events", "--title", "new title"]);
    assert!(amend.status.success(), "amend failed: {amend:?}");
}

#[derive(Deserialize)]
struct Event {
    capsule_id: String,
    attempt_id: Option<u64>,
    actor: String,
    kind: String,
    payload: serde_json::Value,
}

#[test]
fn events_prints_text_and_json_audit_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let store = tmp.path().join("cap");
    setup(tmp.path(), &store);

    let text = capsule(tmp.path(), &store, &["events", "events", "--limit", "10"]);
    assert!(text.status.success(), "events text failed: {text:?}");
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(stdout.contains("\tsystem\tcapsule_created\t"), "{stdout}");
    assert!(stdout.contains("\toperator\tcapsule_amended\t"), "{stdout}");

    let json = capsule(
        tmp.path(),
        &store,
        &["--json", "events", "events", "--kind", "capsule_created"],
    );
    assert!(json.status.success(), "events json failed: {json:?}");
    let stdout = String::from_utf8(json.stdout).unwrap();
    let events: Vec<Event> = serde_json::from_str(&stdout)
        .unwrap_or_else(|e| panic!("parse json: {e}\nstdout:\n{stdout}"));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].capsule_id, "events");
    assert_eq!(events[0].attempt_id, None);
    assert_eq!(events[0].actor, "system");
    assert_eq!(events[0].kind, "capsule_created");
    assert_eq!(events[0].payload["base_ref"], "main");
}
