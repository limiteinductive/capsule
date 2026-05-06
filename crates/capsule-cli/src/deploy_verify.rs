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
//! forge — destructive: tests 3 and 4b advance the base ref, and tests
//! 4b/7/8 mutate witness refs.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
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
    pub status: &'static str, // "pass", "fail", or "skip"
    pub evidence: String,
}

const PRE_RECEIVE_HOOK: &str = include_str!("../../../skills/capsule/pre-receive.sh");

const HERMETIC_WITNESS_PREFIX: &str = "capsule-witness/probe";

pub fn run(opts: Opts) -> Result<Report> {
    let mode_label = match &opts.mode {
        Mode::Hermetic => "hermetic".to_string(),
        Mode::Remote { remote, .. } => format!("remote:{remote}"),
    };

    let bootstrap = match &opts.mode {
        Mode::Hermetic => Bootstrap::hermetic(&opts.base_ref)?,
        Mode::Remote {
            lander_url,
            worker_url,
            outsider_url,
            ..
        } => Bootstrap::remote(&opts.base_ref, lander_url, worker_url, outsider_url)?,
    };

    let hermetic = matches!(opts.mode, Mode::Hermetic);
    let mut results = Vec::with_capacity(9);
    results.push(test_1_outsider_witness_create_rejected(&bootstrap));
    results.push(test_2_worker_witness_create_rejected(&bootstrap));
    let test3 = test_3_lander_witness_create_accepted(&bootstrap);
    let witness_after_test3 = if test3.status == "pass" {
        capsule_git::ls_remote_branch(bootstrap.lander_remote(), &bootstrap.witness_a1()).ok()
    } else {
        None
    };
    results.push(test3);
    results.push(test_4a_lander_witness_idempotent_replay(&bootstrap));
    results.push(test_4b_witness_oid_mismatch_atomic_rollback(
        &bootstrap,
        witness_after_test3.as_deref(),
    ));
    results.push(test_5_outsider_force_push_base_ref_rejected(&bootstrap));
    results.push(test_6_outsider_wildcard_witness_rejected(
        &bootstrap, hermetic,
    ));
    results.push(test_7_lander_witness_delete_accepted(&bootstrap));
    results.push(test_8_outsider_witness_delete_rejected(&bootstrap));

    if !hermetic {
        // Remote mode: best-effort cleanup of the witness ref test 8 created.
        let _ = git_run(
            &bootstrap.lander,
            &[
                "push",
                "origin",
                &format!(":refs/heads/{}", bootstrap.witness_a2()),
            ],
        );
    }

    let passed = results.iter().filter(|r| r.status == "pass").count();
    let failed = results.iter().filter(|r| r.status == "fail").count();
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
/// Each clone path has its `origin` configured to that identity's URL
/// (in hermetic mode all three point at the same bare path; in remote
/// mode each URL is the deployment-validation forge URL for that role).
/// `witness_prefix` is the per-run witness ref prefix — fixed in hermetic
/// mode, uuid-suffixed in remote mode so concurrent runs do not collide.
struct Bootstrap {
    _tmp: Option<tempfile::TempDir>,
    lander: PathBuf,
    worker: PathBuf,
    outsider: PathBuf,
    base_ref: String,
    lander_url: String,
    witness_prefix: String,
}

impl Bootstrap {
    fn witness_a1(&self) -> String {
        format!("{}/a1", self.witness_prefix)
    }

    fn witness_a2(&self) -> String {
        format!("{}/a2", self.witness_prefix)
    }

    fn lander_remote(&self) -> &str {
        &self.lander_url
    }

    fn remote(
        base_ref: &str,
        lander_url: &str,
        worker_url: &str,
        outsider_url: &str,
    ) -> Result<Self> {
        let tmp = tempfile::tempdir().context("creating tempdir for remote deploy verify")?;
        let lander = tmp.path().join("lander");
        let worker = tmp.path().join("worker");
        let outsider = tmp.path().join("outsider");

        for (path, role, url) in [
            (&lander, "lander", lander_url),
            (&worker, "worker", worker_url),
            (&outsider, "outsider", outsider_url),
        ] {
            std::fs::create_dir(path)?;
            git_run(path, &["init", &format!("--initial-branch={base_ref}")])?;
            git_run(path, &["config", "user.email", &format!("{role}@deploy")])?;
            git_run(path, &["config", "user.name", role])?;
            git_run(path, &["remote", "add", "origin", url])?;
            git_run(path, &["fetch", "origin", base_ref])
                .with_context(|| format!("fetching {base_ref} from {role} URL"))?;
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

        // Per-run unique witness prefix — avoids collisions with concurrent
        // suite runs and with real in-flight capsules on the same forge.
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let witness_prefix = format!("capsule-witness/deploy-verify-{suffix}");

        Ok(Self {
            _tmp: Some(tmp),
            lander,
            worker,
            outsider,
            base_ref: base_ref.to_string(),
            lander_url: lander_url.to_string(),
            witness_prefix,
        })
    }

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

        // Install the reference pre-receive hook.
        let hook_path = bare.join("hooks").join("pre-receive");
        std::fs::write(&hook_path, PRE_RECEIVE_HOOK)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&hook_path)?.permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&hook_path, perms)?;
        }
        if !base_ref.is_empty() && base_ref != "main" {
            // Hook reads CAPSULE_BASE_REF for the protected branch name.
            let env_path = bare.join("hooks").join("pre-receive.env");
            std::fs::write(&env_path, format!("CAPSULE_BASE_REF={base_ref}\n"))?;
            // Re-write the hook to source the env file so `git receive-pack`
            // picks up CAPSULE_BASE_REF (it does not inherit the parent
            // shell's environment in a forge deployment either — bake into
            // the hook).
            let wrapped = format!(
                "#!/bin/sh\nexport CAPSULE_BASE_REF={}\n{}",
                shell_quote(base_ref),
                PRE_RECEIVE_HOOK
                    .strip_prefix("#!/bin/sh\n")
                    .unwrap_or(PRE_RECEIVE_HOOK),
            );
            std::fs::write(&hook_path, wrapped)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&hook_path)?.permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&hook_path, perms)?;
            }
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

        let lander_url = bare.to_str().unwrap().to_string();
        Ok(Self {
            _tmp: Some(tmp),
            lander,
            worker,
            outsider,
            base_ref: base_ref.to_string(),
            lander_url,
            witness_prefix: HERMETIC_WITNESS_PREFIX.to_string(),
        })
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
            &format!("{sha}:refs/heads/{}", b.witness_a1()),
        ],
    ) {
        Ok(_) => fail(NAME, "push unexpectedly accepted"),
        Err(e) => {
            let m = e.to_string();
            if m.contains("deny") || m.contains("only lander") {
                pass(NAME, "rejected: deny: only lander may write witness")
            } else {
                fail(NAME, format!("rejected but unexpected message: {m}"))
            }
        }
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
            &format!("{sha}:refs/heads/{}", b.witness_a1()),
        ],
    ) {
        Ok(_) => fail(NAME, "push unexpectedly accepted"),
        Err(e) => {
            let m = e.to_string();
            if m.contains("deny") || m.contains("only lander") {
                pass(NAME, "rejected: deny: only lander may write witness")
            } else {
                fail(NAME, format!("rejected but unexpected message: {m}"))
            }
        }
    }
}

/// Test 3: lander runs the real land push (atomic multi-ref with
/// `--force-with-lease`). Replicates `capsule_git::land_push`.
fn test_3_lander_witness_create_accepted(b: &Bootstrap) -> TestResult {
    const NAME: &str = "lander_witness_create";
    // Lander needs the worker's commit content visible; for the hermetic
    // suite, lander makes its own attempt commit on top of base_ref.
    let verified_sha = match make_commit(&b.lander, "feat.txt", "feat\n", "feat") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    match capsule_git::land_push(
        &b.lander,
        b.lander_remote(),
        &b.base_ref,
        &b.witness_a1(),
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
    match capsule_git::land_push(
        &b.lander,
        b.lander_remote(),
        &b.base_ref,
        &b.witness_a1(),
        &head,
    ) {
        Ok(capsule_git::LandOutcome::NoOp) => pass(NAME, "no-op replay accepted"),
        Ok(other) => fail(NAME, format!("expected NoOp, got {other:?}")),
        Err(e) => fail(NAME, format!("push failed: {e}")),
    }
}

/// Test 4b: base_ref advanced after test 3, then a stale verified_sha for
/// the same witness → WitnessOidMismatch + atomic rollback. DESIGN §8.2
/// requires asserting BOTH `base_ref_after == base_ref_before` AND the
/// witness ref still points at the test-3 verified_sha (the OID fence).
fn test_4b_witness_oid_mismatch_atomic_rollback(
    b: &Bootstrap,
    witness_before: Option<&str>,
) -> TestResult {
    const NAME: &str = "witness_oid_mismatch_atomic";
    let witness_before = match witness_before {
        Some(s) => s.to_string(),
        None => return fail(NAME, "test 3 witness OID unavailable (test 3 did not pass)"),
    };
    // Lander advances base_ref with a new commit B.
    let b_sha = match make_commit(&b.lander, "second.txt", "B\n", "B") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("advance setup: {e}")),
    };
    if let Err(e) = git_run(&b.lander, &["push", "origin", &b.base_ref]) {
        return fail(NAME, format!("advance push: {e}"));
    }
    // Now create commit Y descended from B.
    let y_sha = match make_commit(&b.lander, "third.txt", "Y\n", "Y") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("Y setup: {e}")),
    };
    // Try a land push with witness still-existing at the test-3 sha.
    // force-with-lease=witness: (empty) means "expected null" — but witness
    // already exists, so the push is rejected atomically.
    let outcome = capsule_git::land_push(
        &b.lander,
        b.lander_remote(),
        &b.base_ref,
        &b.witness_a1(),
        &y_sha,
    );
    match outcome {
        Ok(capsule_git::LandOutcome::WitnessOidMismatch) => {
            let base_after = match capsule_git::ls_remote_branch(b.lander_remote(), &b.base_ref) {
                Ok(tip) => tip,
                Err(e) => return fail(NAME, format!("ls-remote base_ref: {e}")),
            };
            if base_after != b_sha {
                return fail(
                    NAME,
                    format!("base_ref drifted to {base_after}, expected B={b_sha}"),
                );
            }
            // OID fence: witness must still equal the test-3 sha.
            let witness_after =
                match capsule_git::ls_remote_branch(b.lander_remote(), &b.witness_a1()) {
                    Ok(tip) => tip,
                    Err(e) => return fail(NAME, format!("ls-remote witness: {e}")),
                };
            if witness_after != witness_before {
                return fail(
                    NAME,
                    format!("witness drifted: before={witness_before}, after={witness_after}"),
                );
            }
            pass(
                NAME,
                format!(
                    "atomic rollback: base_ref still at B={b_sha}, witness still at {witness_before}"
                ),
            )
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
        Err(e) => {
            let m = e.to_string();
            if m.contains("deny") || m.contains("only lander") {
                pass(NAME, "rejected: deny: only lander may update base_ref")
            } else {
                fail(NAME, format!("rejected but unexpected message: {m}"))
            }
        }
    }
}

/// Test 6: outsider pushes via wildcard refspec to a never-claimed witness.
/// DESIGN §8.2 frames this as catching a forge's most-permissive-rule
/// failure mode (e.g. GitLab branch-protection wildcard semantics).
/// In hermetic mode the reference hook denies on ref *pattern*, not on
/// the refspec *form*, so a wildcard push gives no information beyond
/// test 1 — we skip and surface that explicitly. Real-forge `--remote`
/// mode is where this test earns its keep.
fn test_6_outsider_wildcard_witness_rejected(b: &Bootstrap, hermetic: bool) -> TestResult {
    const NAME: &str = "outsider_wildcard_witness";
    if hermetic {
        return TestResult {
            name: NAME,
            status: "skip",
            evidence: "hermetic mode: hook denies by ref pattern, not refspec form — \
                       redundant with test 1; only meaningful against a real forge"
                .to_string(),
        };
    }
    // Configure a wildcard push refspec on the outsider remote, then push
    // without an explicit refspec to exercise the wildcard expansion path.
    let sha = match make_commit(&b.outsider, "wild.txt", "wild\n", "wild") {
        Ok(s) => s,
        Err(e) => return fail(NAME, format!("setup: {e}")),
    };
    if let Err(e) = git_run(
        &b.outsider,
        &["branch", "-f", "capsule-witness/foo/a99", &sha],
    ) {
        return fail(NAME, format!("local branch: {e}"));
    }
    match git_run(
        &b.outsider,
        &[
            "push",
            "origin",
            "refs/heads/capsule-witness/*:refs/heads/capsule-witness/*",
        ],
    ) {
        Ok(_) => fail(NAME, "wildcard push unexpectedly accepted"),
        Err(e) => {
            let m = e.to_string();
            if m.contains("deny") || m.contains("only lander") {
                pass(NAME, "rejected: deny: only lander may write witness")
            } else {
                fail(NAME, format!("rejected but unexpected message: {m}"))
            }
        }
    }
}

/// Test 7: lander deletes the test-3 witness branch → succeed.
fn test_7_lander_witness_delete_accepted(b: &Bootstrap) -> TestResult {
    const NAME: &str = "lander_witness_delete";
    match git_run(
        &b.lander,
        &["push", "origin", &format!(":refs/heads/{}", b.witness_a1())],
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
    let target = b.witness_a2();
    let target = target.as_str();
    if let Err(e) = git_run(
        &b.lander,
        &["push", "origin", &format!("{head}:refs/heads/{target}")],
    ) {
        return fail(NAME, format!("recreate setup: {e}"));
    }
    // Outsider needs the ref to attempt deletion locally (or push :ref
    // directly — outsider can name any remote ref by string).
    match git_run(
        &b.outsider,
        &["push", "origin", &format!(":refs/heads/{target}")],
    ) {
        Ok(_) => fail(NAME, "delete unexpectedly accepted"),
        Err(e) => {
            let m = e.to_string();
            if m.contains("deny") || m.contains("only lander") {
                pass(NAME, "rejected: deny: only lander may write witness")
            } else {
                fail(NAME, format!("rejected but unexpected message: {m}"))
            }
        }
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
