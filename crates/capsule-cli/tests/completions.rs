//! Integration tests for generated shell completions.

use std::process::Command;

use assert_cmd::cargo::CommandCargoExt;

#[test]
fn bash_completions_include_current_subcommands() {
    let out = Command::cargo_bin("capsule")
        .unwrap()
        .args(["completions", "bash"])
        .output()
        .expect("capsule completions");

    assert!(out.status.success(), "completions failed: {out:?}");
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.contains("_capsule"), "{stdout}");
    assert!(stdout.contains("deploy-verify"), "{stdout}");
    assert!(stdout.contains("force-unfreeze"), "{stdout}");
    assert!(stdout.contains("completions"), "{stdout}");
}
