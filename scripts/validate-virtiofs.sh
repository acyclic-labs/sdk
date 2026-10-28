#!/usr/bin/env bash
# One Linux job: compile gates, unchanged filesystem tests, public admission,
# existing host tests, and actual standard QEMU/cloud-hypervisor frontends.
set -u

run() {
    local step=$1
    shift
    if "$@"; then
        return 0
    else
        local code=$?
        printf 'step=%s; exit=%s; recovery:' "$step" "$code" >&2
        printf ' %q' "$@" >&2
        printf '\n' >&2
        return "$code"
    fi
}

features=filesystem-local,native-execution,test-support
status=0
run 'compile filesystem tests' cargo test --locked -p acyclic-fs --lib --no-run || status=$?
run 'compile local owner and daemon tests' cargo test --locked -p acyclic-harness --features "$features" --lib --bin acyclic-virtiofsd --test virtiofs --no-run || status=$?
run 'compile original Bound tests' cargo test --locked -p acyclic-harness-core --features "$features" --test agent-model --no-run || status=$?
if (( status != 0 )); then
    printf 'step=virtiofs compile gates; recovery: bash scripts/validate-virtiofs.sh\n' >&2
    exit "$status"
fi
run 'prepare real guest artifacts' python3 scripts/prepare-virtiofs-guest.py --output /tmp/acyclic-guest || exit "$?"
export ACYCLIC_FS_GUEST_ARTIFACTS=/tmp/acyclic-guest

status=0
run 'filesystem suite including real kernel packets and both VMMs' cargo test --locked -p acyclic-fs --lib -- --include-ignored --nocapture || status=$?
run 'existing local host library suite' cargo test --locked -p acyclic-harness --features "$features" --lib || status=$?
run 'original Bound agent suite' cargo test --locked -p acyclic-harness-core --features "$features" --test agent-model || status=$?
run 'kernel-authenticated daemon host peer' cargo test --locked -p acyclic-harness --features "$features" --bin acyclic-virtiofsd || status=$?
run 'public Bound and standalone daemon with both VMMs' cargo test --locked -p acyclic-harness --features "$features" --test virtiofs -- --include-ignored --nocapture || status=$?
if (( status != 0 )); then
    printf 'step=virtiofs validation; recovery: bash scripts/validate-virtiofs.sh\n' >&2
fi
exit "$status"
