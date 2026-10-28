#!/usr/bin/env bash
# Linux host only; the local Dex account and client are disposable public test inputs.
set -euo pipefail
trap 'rc=$?; echo "STEP_FAILED be-edge-oidc line=$LINENO rc=$rc; recovery: bash scripts/be-edge-oidc.sh" >&2' ERR
root=${BE_EDGE_ROOT:-${TMPDIR:-/tmp}/be-edge}/oidc
mkdir -p "$root/captures"
exec 9>"$root/emulator.lock"
flock -n 9
container=
cleanup() {
    local result=$?
    if [ -n "$container" ]; then
        if ! docker stop "$container" >/dev/null; then
            echo "STEP_FAILED dex-stop; recovery: docker stop $container" >&2
            result=1
        fi
        if ! docker rm "$container" >/dev/null; then
            echo "STEP_FAILED dex-remove; recovery: docker rm $container" >&2
            result=1
        fi
    fi
    exit "$result"
}
trap cleanup EXIT
image=ghcr.io/dexidp/dex:v2.44.0
if ! docker image inspect "$image" >/dev/null 2>&1; then timeout 180 docker pull "$image"; fi
cat > "$root/config.yaml" <<'DEX'
issuer: http://127.0.0.1:15482/dex
storage:
  type: memory
web:
  http: 127.0.0.1:15482
telemetry:
  http: 127.0.0.1:15483
oauth2:
  passwordConnector: local
  grantTypes: [password]
staticClients:
- id: conformance-turns
  name: Conformance
  secret: conformance-public-test-secret
  redirectURIs: [http://127.0.0.1:15482/unused]
enablePasswordDB: true
staticPasswords:
- email: admin@example.com
  hash: "$2a$10$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4W"
  username: conformance-user
  userID: 08a8684b-db88-4b73-90a9-3cd1661f5466
expiry:
  idTokens: 5m
DEX
chmod 644 "$root/config.yaml" # Public test inputs; Dex runs as uid 1001.
container=$(docker run --detach --network host -v "$root/config.yaml:/etc/dex/config.yaml:ro" "$image" dex serve /etc/dex/config.yaml)
for attempt in $(seq 1 45); do
    if curl --silent --fail http://127.0.0.1:15482/dex/.well-known/openid-configuration >/dev/null; then break; fi
    if [ "$(docker inspect --format '{{.State.Running}}' "$container")" != true ]; then break; fi
    sleep 1
done
if ! curl --silent --fail http://127.0.0.1:15482/dex/.well-known/openid-configuration >/dev/null; then
    docker logs "$container" >&2 || echo "STEP_FAILED dex-logs; recovery: docker logs $container" >&2
    echo "STEP_FAILED dex-readiness; recovery: bash scripts/be-edge-oidc.sh" >&2
    exit 1
fi
docker inspect --format '{{.Image}}' "$container" > "$root/captures/image.txt"
python3 scripts/be-edge-oidc-capture.py "$root/captures"
mkdir -p rust/crates/harness/fixtures/conformance/identity/dex
cp "$root/captures/"* rust/crates/harness/fixtures/conformance/identity/dex/
cargo run -p acyclic-agent-host-conformance --features local --bin oidc-qualify -- "$root/captures"
