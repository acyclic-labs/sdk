#!/usr/bin/env bash
set -euo pipefail
if [ "${1-}" != --container ]; then
    source=${E2E_DOCKER_SOURCE:-$(cd -- "$(dirname -- "$0")/../../.." && pwd)}
    # Stock Docker's minimal /dev has no FUSE device. No privileged flags,
    # device forwarding or namespace mount permissions are needed.
    exec docker run --rm \
        --mount "type=bind,src=$source,dst=/source,readonly" \
        --mount "type=bind,src=${ACYCLIC_TEST_BINARY:?},dst=/artifacts/acyclic,readonly" \
        --mount "type=bind,src=${E2E_GRAPHCODER:?},dst=/artifacts/graphcoder,readonly" \
        --mount "type=bind,src=${ACYCLIC_HEADLESS_TEST_BINARY:?},dst=/artifacts/headless-acyclic,readonly" \
        --mount "type=bind,src=${E2E_NOFUSE_CONTROL_GRAPHCODER:?},dst=/artifacts/control-graphcoder,readonly" \
        --env ACYCLIC_TEST_BINARY=/artifacts/acyclic \
        --env E2E_GRAPHCODER=/artifacts/graphcoder \
        --env ACYCLIC_HEADLESS_TEST_BINARY=/artifacts/headless-acyclic \
        --env E2E_NOFUSE_CONTROL_GRAPHCODER=/artifacts/control-graphcoder \
        --env E2E_NOFUSE_CONTROL_SHA="${E2E_NOFUSE_CONTROL_SHA:?}" \
        --env QUALIFICATION_SHA="${QUALIFICATION_SHA:?}" \
        --workdir /source debian:trixie-slim bash /source/plugin/tests/hosts/workspace-nofuse.sh --container
fi
test ! -e /dev/fuse
printf 'NOFUSE_ENVIRONMENT sha=%s filesystem=%s fuse=absent\n' "$QUALIFICATION_SHA" "$(stat -f -c %T /tmp)"
apt-get update -qq
# The shared process-tree controller is a test-harness dependency.
apt-get install -y -qq --no-install-recommends nodejs git ca-certificates python3
node --test --test-concurrency=1 "$(dirname -- "$0")/workspace-nofuse.mjs"
