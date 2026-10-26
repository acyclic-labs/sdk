#!/usr/bin/env bash
# Run only on bb2, through Q:/lanes/bs.ps1. No account credentials required.
set -euo pipefail
trap 'rc=$?; echo "STEP_FAILED be-edge-local line=$LINENO rc=$rc; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/be-edge-local.sh" >&2' ERR
root=/mnt/bb/lane-scratch/be-edge
mkdir -p "$root/models" "$root/captures"
exec 9>"$root/emulators.lock"
flock -n 9
container=
limiter=
cleanup() {
    local result=$? id
    for id in "$limiter" "$container"; do
        if [ -n "$id" ]; then
            if ! docker stop "$id" >/dev/null; then
                echo "STEP_FAILED emulator-stop; recovery: docker stop $id" >&2
                result=1
            fi
        fi
    done
    exit "$result"
}
trap cleanup EXIT
port=15480
rateport=15481
image=ghcr.io/ggml-org/llama.cpp:server
if ! docker image inspect "$image" >/dev/null 2>&1; then
    timeout 180 docker pull "$image"
fi
model="$root/models/Qwen3-0.6B-Q4_0.gguf"
if [ ! -s "$model" ]; then
    curl --fail --location --retry 1 --max-time 180 \
        https://huggingface.co/ggml-org/Qwen3-0.6B-GGUF/resolve/main/Qwen3-0.6B-Q4_0.gguf \
        --output "$model.part"
    mv "$model.part" "$model"
fi
# An isolated container lives only for this invocation; the public models cache persists.
container=$(docker run --detach --rm --network host \
    -v "$root/models:/models:ro" "$image" \
    -m /models/Qwen3-0.6B-Q4_0.gguf --host 127.0.0.1 --port "$port" \
    --alias conformance-tiny -c 2048 -t 2 --jinja --reasoning-budget 0 --api-key conformance-public-test-key)
endpoint="http://127.0.0.1:$port"
for attempt in $(seq 1 45); do
    if curl --silent --fail "$endpoint/health" >/dev/null; then break; fi
    sleep 1
done
curl --fail --silent "$endpoint/health"
if ! docker image inspect nginx:stable-alpine >/dev/null 2>&1; then timeout 180 docker pull nginx:stable-alpine; fi
cat > "$root/nginx.conf" <<'NGINX'
events {}
http {
    limit_req_zone $binary_remote_addr zone=inference:1m rate=1r/m;
    server {
        listen 127.0.0.1:15481;
        location / {
            limit_req zone=inference;
            limit_req_status 429;
            proxy_pass http://127.0.0.1:15480/health;
        }
    }
}
NGINX
limiter=$(docker run --detach --rm --network host -v "$root/nginx.conf:/etc/nginx/nginx.conf:ro" nginx:stable-alpine)
for attempt in $(seq 1 20); do
    if curl --silent --output /dev/null "http://127.0.0.1:$rateport/"; then break; fi
    sleep 1
done
docker inspect --format '{{.Image}}' "$container" > "$root/captures/image.txt"
sha256sum "$model" > "$root/captures/model.sha256"
python3 scripts/be-edge-capture.py "$endpoint" "$root/captures" "http://127.0.0.1:$rateport/"
mkdir -p rust/crates/conformance/fixtures/inference/llamacpp
cp "$root/captures/"* rust/crates/conformance/fixtures/inference/llamacpp/
if [ "${BE_EDGE_RUN_SUITE:-0}" = 1 ]; then
    cargo run -p acyclic-conformance --features inference --bin inference-qualify -- \
        "$root/captures/manifest.json"
    curl --silent --output /dev/null "http://127.0.0.1:$rateport/"
    cargo run -p acyclic-conformance --features inference --bin inference-qualify -- \
        --local-live "$root/captures/manifest.json" "$endpoint/v1" "http://127.0.0.1:$rateport/v1"
fi
