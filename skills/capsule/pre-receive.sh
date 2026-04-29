#!/bin/sh
# Reference pre-receive hook for capsule deployments. See DESIGN.md §8.1.
#
# Enforces the publication contract from DESIGN.md §3.1:
#   - capsule-witness/** : create or update only by `lander`
#   - <base_ref> (e.g. main): non-fast-forward update only by `lander`
#   - capsules/** and other refs: open
#
# In real deployments these rules live in the forge's branch-protection ACL
# (GitHub rulesets, GitLab protected branches, Gitea, etc.). This hook is the
# canonical artifact for hermetic verification — `capsule deploy verify
# --hermetic` runs the §8.2 ACL test suite against a tempdir bare repo with
# this hook installed.
#
# Identity is read from `git push -o identity=<role>`. Roles: `lander`,
# `worker`, `outsider`. A real forge derives identity from the authenticated
# principal; here it is push-option-asserted because the hermetic harness has
# no auth layer.
#
# CAPSULE_BASE_REF (env, default `main`): the protected base branch.

set -eu

BASE_REF="${CAPSULE_BASE_REF:-main}"

identity=""
i=0
while :; do
    var="GIT_PUSH_OPTION_$i"
    eval "val=\${$var-}"
    [ -z "${val:-}" ] && break
    case "$val" in
        identity=*) identity="${val#identity=}" ;;
    esac
    i=$((i + 1))
done

deny() {
    echo "deny: $1" >&2
    exit 1
}

while read -r _old new ref; do
    case "$ref" in
        refs/heads/capsule-witness/*)
            # Both create (new commit, _old=zero) and delete (new=zero) are
            # lander-only. DESIGN §3.1: "only the lander process may create
            # capsule-witness/**".
            [ "$identity" = "lander" ] || \
                deny "only lander may write $ref (identity=$identity)"
            ;;
        "refs/heads/$BASE_REF")
            [ "$identity" = "lander" ] || \
                deny "only lander may update $ref (identity=$identity)"
            ;;
    esac
    # `capsules/**` and any other refs are accepted.
done

exit 0
