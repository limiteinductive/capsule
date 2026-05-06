# Security Policy

Capsule's safety model depends on both local state and forge-side ACLs. Treat
bugs in claim isolation, witness refs, deploy verification, lease handling, or
atomic land behavior as security-sensitive.

## Supported Versions

Capsule is pre-1.0. Security fixes target `main` until the project publishes
versioned releases.

## Reporting a Vulnerability

If GitHub private vulnerability reporting is enabled for this repository, use
it.

If private reporting is not available, open a minimal public issue that says
you have a security report and avoid posting exploit details or private
deployment information. A maintainer can then arrange a private channel.

Include:

- Affected component: core, store, git, CLI, deploy hook, or docs
- Impact: what invariant can be violated
- Reproduction steps or a minimized test case
- Whether forge ACL configuration is involved

## High-Signal Security Areas

- Non-lander writes to `capsule-witness/**`
- Non-lander updates to the protected base ref
- Incorrect `git push --atomic --force-with-lease` classification
- Store transitions that bypass scope conflict checks
- Lease expiry or cross-session precedence bugs
- Attestation accepted for a commit that was not actually verified
- Reconcile or force-unfreeze behavior that can hide a failed land

## Deployment Note

Run `capsule deploy-verify` in every deployment environment before relying on
`capsule land`. The hermetic mode validates the reference hook; remote mode is
the check that validates real forge ACL behavior.
