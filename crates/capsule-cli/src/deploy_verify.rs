//! Deployment ACL test suite — DESIGN §8.2.
//!
//! Verifies that a deployment correctly enforces the publication contract
//! (DESIGN §3.1): only the lander principal may write `capsule-witness/**`
//! refs or update the base branch. The eight test cases below exercise the
//! three identities (lander / worker / outsider) against the protected refs.
//!
//! **Hermetic mode** spins up a tempdir bare repo with three sibling clones,
//! installs the reference pre-receive hook (`skills/capsule/pre-receive.sh`),
//! and uses `git push -o identity=<role>` (configured via
//! `push.pushOption` per clone) to assert identity. The hook denies pushes
//! to protected refs unless `identity=lander`.
//!
//! **Remote mode** assumes the operator has provisioned three distinct
//! principals on a real forge (lander/worker/outsider), each with the
//! appropriate ACL. The same eight tests run end-to-end against the live
//! forge — destructive: tests 3, 4b, 7 mutate real refs.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;

#[derive(Clone, Debug)]
pub enum Mode {
    Hermetic,
    Remote {
        remote: String,
        // Per-identity push URLs are accepted by the CLI but not yet wired
        // through — `Mode::Remote` returns an error from `run`. See plan.
        #[allow(dead_code)]
        lander_url: String,
        #[allow(dead_code)]
        worker_url: String,
        #[allow(dead_code)]
        outsider_url: String,
    },
}

pub struct Opts {
    pub mode: Mode,
    pub base_ref: String,
    pub json: bool,
}

#[derive(Serialize)]
pub struct Report {
    pub mode_label: String,
    pub base_ref: String,
    pub all_passed: bool,
    pub passed: usize,
    pub failed: usize,
    pub tests: Vec<TestResult>,
}

#[derive(Clone, Serialize)]
pub struct TestResult {
    pub name: &'static str,
    pub status: &'static str, // "pass" or "fail"
    pub evidence: String,
}

const PRE_RECEIVE_HOOK: &str = include_str!("../../../skills/capsule/pre-receive.sh");

const WITNESS_BRANCH: &str = "capsule-witness/probe/a1";

pub fn run(opts: Opts) -> Result<Report> {
    let mode_label = match &opts.mode {
        Mode::Hermetic => "hermetic".to_string(),
        Mode::Remote { remote, .. } => format!("remote:{remote}"),
    };

    let bootstrap = match &opts.mode {
        Mode::Hermetic => Bootstrap::hermetic(&opts.base_ref)?,
        Mode::Remote { .. } => bail!("deploy verify --remote is not yet implemented"),
    };

    let results = vec![
        test_1_outsider_witness_create_rejected(&bootstrap),
        test_2_worker_witness_create_rejected(&bootstrap),
        test_3_lander_witness_create_accepted(&bootstrap),
        test_4a_lander_witness_idempotent_replay(&bootstrap),
        test_4b_witness_oid_mismatch_atomic_rollback(&bootstrap),
        test_5_outsider_force_push_base_ref_rejected(&bootstrap),
        test_6_outsider_wildcard_witness_rejected(&bootstrap),
        test_7_lander_witness_delete_accepted(&bootstrap),
        test_8_outsider_witness_delete_rejected(&bootstrap),
    ];

    let passed = results.iter().filter(|r| r.status == "pass").count();
    let failed = results.len() - passed;
    let all_passed = failed == 0;

    if opts.json {
        let report = Report {
            mode_label: mode_label.clone(),
            base_ref: opts.base_ref.clone(),
            all_passed,
            passed,
            failed,
            tests: results.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        for (i, r) in results.iter().enumerate() {
            println!(
                "[{}/{}] {:32} {}  {}",
                i + 1,
                results.len(),
                r.name,
                r.status.to_uppercase(),
                r.evidence,
            );
        }
        println!(
            "deploy verify: {}/{} passed ({})",
            passed,
            results.len(),
            mode_label
        );
    }

    Ok(Report {
        mode_label,
        base_ref: opts.base_ref,
        all_passed,
        passed,
        failed,
        tests: results,
    })
}

/// Bootstrap holds the on-disk state shared across the eight tests. The
/// `_tmp` field anchors the tempdir lifetime — drop after `run` returns.
struct Bootstrap {
    _tmp: Option<tempfile::TempDir>,
    bare: PathBuf,
    lander: PathBuf,
    worker: PathBuf,
    outsider: PathBuf,
    base_ref: String,
}

impl Bootstrap {
    fn hermetic(base_ref: &str) -> Result<Self> {
        let tmp = tempfile::tempdir().context("creating tempdir for hermetic deploy verify")?;
        let bare = tmp.path().join("bare.git");
        std::fs::create_dir(&bare)?;
        git_run(
            &bare,
            &["init", "--bare", &format!("--initial-branch={base_ref}")],
        )?;
        // §3.1: forge accepts push options so the hook can read identity.
        git_run(&bare, &["config", "receive.advertisePushOptions", "true"])?;

        // Install the reference pre-receive hook. If base_ref is non-default
        // (i.e. not "main"), prefix with `export CAPSULE_BASE_REF=...` so the
        // hook protects the right branch — receive-pack does not inherit the
        // parent shell's environment any more than a real forge would.
        let hook_path = bare.join("hooks").join("pre-receive");
        let hook_body = if base_ref == "main" {
            PRE_RECEIVE_HOOK.to_string()
        } else {
            format!(
                "#!/bin/sh\nexport CAPSULE_BASE_REF={}\n{}",
                shell_quote(base_ref),
                PRE_RECEIVE_HOOK
                    .strip_prefix("#!/bin/sh\n")
                    .unwrap_or(PRE_RECEIVE_HOOK),
            )
        };
        std::fs::write(&hook_path, hook_body)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&hook_path)?.permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&hook_path, perms)?;
        }

        // Three clones — one per identity. Each clone configures
        // `push.pushOption` so every push from it includes `-o identity=<role>`.
        let lander = tmp.path().join("lander");
        let worker = tmp.path().join("worker");
        let outsider = tmp.path().join("outsider");
        for (path, role) in [
            (&lander, "lander"),
            (&worker, "worker"),
            (&outsider, "outsider"),
        ] {
            std::fs::create_dir(path)?;
            git_run(path, &["init", &format!("--initial-branch={base_ref}")])?;
            git_run(path, &["config", "user.email", &format!("{role}@deploy")])?;
            git_run(path, &["config", "user.name", role])?;
            git_run(
                path,
                &["config", "push.pushOption", &format!("identity={role}")],
            )?;
            git_run(path, &["remote", "add", "origin", bare.to_str().unwrap()])?;
        }

        // Lander seeds the bare with the initial base_ref commit.
        std::fs::write(lander.join("README"), "deploy-verify\n")?;
        git_run(&lander, &["add", "."])?;
        git_run(&lander, &["commit", "-m", "deploy-verify init"])?;
        git_run(&lander, &["push", "origin", base_ref])?;

        // Worker and outsider fetch so they have the base_ref locally.
        for path in [&worker, &outsider] {
            git_run(path, &["fetch", "origin", base_ref])?;
            git_run(
                path,
                &[
                    "update-ref",
                    &format!("refs/heads/{base_ref}"),
                    &format!("refs/remotes/origin/{base_ref}"),
                ],
            )?;
            git_run(path, &["checkout", base_ref])?;
        }

        Ok(Self {
            _tmp: Some(tmp),
            bare,
            lander,
            worker,
            outsider,
            base_ref: base_ref.to_string(),
        })
    }

    fn remote(&self) -> &str {
        self.bare.to_str().unwrap()
    }
}

fn shell_quote(s: &str) -> String {
    let mut out = String::from("'");
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

fn make_commit(repo: &Path, file: &str, content: &str, msg: &str) -> Result<String> {
    std::fs::write(repo.join(file), content)?;
    git_run(repo, &["add", file])?;
    git_run(repo, &["commit", "-m", msg])?;
    git_capture(repo, &["rev-parse", "HEAD"])
}

fn pass(name: &'static str, evidence: impl Into<String>) -> TestResult {
    TestResult {
        name,
        status: "pass",
        evidence: evidence.into(),
    }
}

fn fail(name: &'static str, evidence: impl Into<String>) -> TestResult {
    TestResult {
        name,
        status: "fail",
        evidence: evidence.into(),
    }
}

/// Test 1: outsider creates `capsule-witness/<id>/a1` → reject.
fn test_1_outsider_witness_create_rejected(b: &Bootstrap) -> TestResult {
    const NAME: &str = "outsider_witness_create";
    let sha = match make_commit(&b.outsider, "x.txt", "outsider\n", "x") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    match git_run(
        &b.outsider,
        &[
            "push",
            "origin",
            &format!("{sha}:refs/heads/{WITNESS_BRANCH}"),
        ],
    ) {
        Ok(_) => fail(NAME, "push unexpectedly accepted"),
        Err(e) => denial_check(NAME, &e),
    }
}

/// Test 2: worker creates `capsule-witness/<id>/a1` → reject.
fn test_2_worker_witness_create_rejected(b: &Bootstrap) -> TestResult {
    const NAME: &str = "worker_witness_create";
    let sha = match make_commit(&b.worker, "y.txt", "worker\n", "y") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    match git_run(
        &b.worker,
        &[
            "push",
            "origin",
            &format!("{sha}:refs/heads/{WITNESS_BRANCH}"),
        ],
    ) {
        Ok(_) => fail(NAME, "push unexpectedly accepted"),
        Err(e) => denial_check(NAME, &e),
    }
}

/// Test 3: lander runs the real land push (atomic multi-ref with
/// `--force-with-lease`). Replicates `capsule_git::land_push`.
fn test_3_lander_witness_create_accepted(b: &Bootstrap) -> TestResult {
    const NAME: &str = "lander_witness_create";
    let verified_sha = match make_commit(&b.lander, "feat.txt", "feat\n", "feat") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    match capsule_git::land_push(
        &b.lander,
        b.remote(),
        &b.base_ref,
        WITNESS_BRANCH,
        &verified_sha,
    ) {
        Ok(capsule_git::LandOutcome::Advanced { .. }) => pass(
            NAME,
            format!("advanced base_ref + witness to {verified_sha}"),
        ),
        Ok(other) => fail(NAME, format!("expected Advanced, got {other:?}")),
        Err(e) => fail(NAME, format!("push failed: {e}")),
    }
}

/// Test 4a: idempotent replay of test 3 → NoOp.
fn test_4a_lander_witness_idempotent_replay(b: &Bootstrap) -> TestResult {
    const NAME: &str = "lander_idempotent_replay";
    let head = match git_capture(&b.lander, &["rev-parse", "HEAD"]) {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("rev-parse: {e}")),
    };
    match capsule_git::land_push(&b.lander, b.remote(), &b.base_ref, WITNESS_BRANCH, &head) {
        Ok(capsule_git::LandOutcome::NoOp) => pass(NAME, "no-op replay accepted"),
        Ok(other) => fail(NAME, format!("expected NoOp, got {other:?}")),
        Err(e) => fail(NAME, format!("push failed: {e}")),
    }
}

/// Test 4b: base_ref advanced after test 3, then a stale verified_sha for
/// the same witness → WitnessOidMismatch + atomic rollback (base_ref tip
/// unchanged from the advance).
fn test_4b_witness_oid_mismatch_atomic_rollback(b: &Bootstrap) -> TestResult {
    const NAME: &str = "witness_oid_mismatch_atomic";
    let b_sha = match make_commit(&b.lander, "second.txt", "B\n", "B") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("advance setup: {e}")),
    };
    if let Err(e) = git_run(&b.lander, &["push", "origin", &b.base_ref]) {
        return fail(NAME, format!("advance push: {e}"));
    }
    let y_sha = match make_commit(&b.lander, "third.txt", "Y\n", "Y") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("Y setup: {e}")),
    };
    let outcome =
        capsule_git::land_push(&b.lander, b.remote(), &b.base_ref, WITNESS_BRANCH, &y_sha);
    match outcome {
        Ok(capsule_git::LandOutcome::WitnessOidMismatch) => {
            match capsule_git::ls_remote_branch(b.remote(), &b.base_ref) {
                Ok(tip) if tip == b_sha => pass(
                    NAME,
                    format!("atomic rollback: base_ref still at B={b_sha}"),
                ),
                Ok(tip) => fail(
                    NAME,
                    format!("base_ref drifted to {tip}, expected B={b_sha}"),
                ),
                Err(e) => fail(NAME, format!("ls-remote: {e}")),
            }
        }
        Ok(other) => fail(NAME, format!("expected WitnessOidMismatch, got {other:?}")),
        Err(e) => fail(NAME, format!("push errored: {e}")),
    }
}

/// Test 5: outsider force-pushes base_ref → reject.
fn test_5_outsider_force_push_base_ref_rejected(b: &Bootstrap) -> TestResult {
    const NAME: &str = "outsider_force_push_base_ref";
    let sha = match make_commit(&b.outsider, "rogue.txt", "rogue\n", "rogue") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    match git_run(
        &b.outsider,
        &[
            "push",
            "--force",
            "origin",
            &format!("{sha}:refs/heads/{}", b.base_ref),
        ],
    ) {
        Ok(_) => fail(NAME, "force-push unexpectedly accepted"),
        Err(e) => denial_check(NAME, &e),
    }
}

/// Test 6: outsider pushes via wildcard refspec to a never-claimed witness.
/// Hermetically identical to test 1 (the hook denies on ref pattern, not
/// refspec form); kept as a separate test to mirror the §8.2 spec ordering
/// and to let real-forge mode catch GitLab's most-permissive-rule failure.
fn test_6_outsider_wildcard_witness_rejected(b: &Bootstrap) -> TestResult {
    const NAME: &str = "outsider_wildcard_witness";
    let sha = match make_commit(&b.outsider, "wild.txt", "wild\n", "wild") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    match git_run(
        &b.outsider,
        &[
            "push",
            "origin",
            &format!("{sha}:refs/heads/capsule-witness/foo/a99"),
        ],
    ) {
        Ok(_) => fail(NAME, "wildcard push unexpectedly accepted"),
        Err(e) => denial_check(NAME, &e),
    }
}

/// Test 7: lander deletes the test-3 witness branch → succeed.
fn test_7_lander_witness_delete_accepted(b: &Bootstrap) -> TestResult {
    const NAME: &str = "lander_witness_delete";
    match git_run(
        &b.lander,
        &["push", "origin", &format!(":refs/heads/{WITNESS_BRANCH}")],
    ) {
        Ok(_) => pass(NAME, "delete accepted"),
        Err(e) => fail(NAME, format!("delete failed: {e}")),
    }
}

/// Test 8: outsider tries to delete a witness branch → reject. Re-creates
/// a witness branch as the lander first (test 7 already deleted the
/// original), then has the outsider attempt to delete it.
fn test_8_outsider_witness_delete_rejected(b: &Bootstrap) -> TestResult {
    const NAME: &str = "outsider_witness_delete";
    let head = match git_capture(&b.lander, &["rev-parse", "HEAD"]) {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("rev-parse: {e}")),
    };
    let target = "capsule-witness/probe/a2";
    if let Err(e) = git_run(
        &b.lander,
        &["push", "origin", &format!("{head}:refs/heads/{target}")],
    ) {
        return fail(NAME, format!("recreate setup: {e}"));
    }
    match git_run(
        &b.outsider,
        &["push", "origin", &format!(":refs/heads/{target}")],
    ) {
        Ok(_) => fail(NAME, "delete unexpectedly accepted"),
        Err(e) => denial_check(NAME, &e),
    }
}

fn denial_check(name: &'static str, e: &anyhow::Error) -> TestResult {
    let m = e.to_string();
    if let Some(idx) = m.find("deny:") {
        let tail = &m[idx..];
        let stop = tail.find(['\n', '\r']).unwrap_or(tail.len());
        pass(name, format!("rejected: {}", tail[..stop].trim()))
    } else if m.contains("only lander") {
        pass(name, "rejected: only lander allowed")
    } else {
        fail(name, format!("rejected but unexpected message: {m}"))
    }
}

// ---- git shell helpers ----

fn git_run(cwd: &Path, args: &[&str]) -> Result<()> {
    let out = Command::new("git").args(args).current_dir(cwd).output()?;
    if !out.status.success() {
        return Err(anyhow!(
            "git {:?} failed in {:?}: stdout={} stderr={}",
            args,
            cwd,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        ));
    }
    Ok(())
}

fn git_capture(cwd: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git").args(args).current_dir(cwd).output()?;
    if !out.status.success() {
        return Err(anyhow!(
            "git {:?} failed in {:?}: {}",
            args,
            cwd,
            String::from_utf8_lossy(&out.stderr),
        ));
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}
