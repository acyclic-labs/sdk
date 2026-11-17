#!/usr/bin/env bash
# Run only through the shell on Linux. Every provider is a disposable local emulator.
# Caller input admission declares no comparison receipts or borrowed artifacts;
# the uploaded source creates fresh fixtures against ordinary emulator images.
set -euo pipefail
step=setup
root=
pg=
minio=
cleanup() {
    rc=$?
    if (( rc != 0 )); then
        echo "step=$step failed (exit $rc); recovery: bash scripts/backends/storage-conformance.sh" >&2
    fi
    for container in "$pg" "$minio"; do
        if [[ -n "$container" ]]; then
            docker rm -f "$container" >/dev/null || echo "step=cleanup failed; recovery: docker rm -f $container" >&2
        fi
    done
    echo "Emulator data retained for diagnosis: $root"
    exit "$rc"
}
trap cleanup EXIT
# Blocks until the stream a command prints yields a line matching the pattern,
# then stops the stream. A stream that ends first (its container died) fails.
await_line() {
    local pattern=$1 line fd pid
    shift
    exec {fd}< <(exec "$@" 2>&1)
    pid=$!
    while IFS= read -r line <&"$fd"; do
        if [[ $line =~ $pattern ]]; then
            kill "$pid" 2>/dev/null || true
            exec {fd}<&-
            return 0
        fi
    done
    exec {fd}<&-
    echo "step=$step: stream ended before '$pattern'; recovery: docker ps -a" >&2
    return 1
}
scratch=${TMPDIR:-/tmp}
root=$(mktemp -d "$scratch/acyclic-storage.XXXXXXXX")
case "$root" in "$scratch"/acyclic-storage.*) ;; *) echo "step=setup: unexpected data root $root; recovery: ls -ld $scratch" >&2; exit 2 ;; esac

step=images
minio_image=be-storage-minio:RELEASE.2025-10-15T17-29-55Z
for image in postgres:17-alpine; do
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        docker pull "$image"
    fi
done
if ! docker image inspect "$minio_image" >/dev/null 2>&1; then
    step=minio-source
    # Official release archive, captured on 2026-10-26. The upstream public
    # container and binary distribution returned unauthorized/410 on that date.
    curl --fail --location --output "$root/minio.tar.gz" \
        https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-10-15T17-29-55Z
    echo "be6d0bd3696c3a13a35f02d3a0280b64319c67918b4501c5c3d87f96d000085c  $root/minio.tar.gz" | sha256sum --check
    mkdir "$root/minio-source"
    tar -xzf "$root/minio.tar.gz" --strip-components=1 -C "$root/minio-source"
    step=minio-build
    docker build -f scripts/backends/minio.Dockerfile -t "$minio_image" "$root/minio-source"
fi
step=postgres-start
mkdir -p "$root/postgres" "$root/minio"
# Docker refuses an occupied loopback port; the EXIT trap reports recovery and
# removes either container.
started=$(date +%s)
# The image's temporary bootstrap server accepts Unix sockets only, so a TCP
# probe cannot report health before the final server listens.
pg=$(docker run -d --rm --health-cmd "pg_isready -h 127.0.0.1 -U postgres -d storage" --health-interval 1s --health-start-period 60s --health-start-interval 100ms -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=storage -v "$root/postgres:/var/lib/postgresql/data" -p 127.0.0.1:15400:5432 postgres:17-alpine)
step=minio-start
# Public, disposable emulator values only. Never read host/cloud credentials.
minio=$(docker run -d --rm -e MINIO_ROOT_USER=storage-local-access -e MINIO_ROOT_PASSWORD=storage-local-secret -v "$root/minio:/data" -p 127.0.0.1:15401:9000 "$minio_image" server /data)
pg_port=$(docker port "$pg" 5432/tcp)
minio_port=$(docker port "$minio" 9000/tcp)
export STORAGE_POSTGRES_URL="postgresql://postgres@127.0.0.1:${pg_port##*:}/storage"
export STORAGE_S3_ENDPOINT="http://127.0.0.1:${minio_port##*:}"
export STORAGE_S3_BUCKET=storage
export STORAGE_S3_REGION=us-east-1
export STORAGE_S3_ACCESS_KEY=storage-local-access
export STORAGE_S3_SECRET_KEY=storage-local-secret
export STORAGE_S3_ALLOW_HTTP=1

step=postgres-ready
# Docker's health event for the container, or its death, ends the wait.
await_line '^(health_status: healthy|health_status: unhealthy|die)$' \
    docker events --since "$started" --filter "container=$pg" \
    --filter event=health_status --filter event=die --format '{{.Action}}'
[[ $(docker inspect -f '{{.State.Health.Status}}' "$pg") == healthy ]]
step=postgres-isolation-fixture
# Exercise a nondefault server setting: publication must choose its own fresh
# statement snapshots after waiting for the namespace advisory lock.
docker exec "$pg" psql -U postgres -d storage -v ON_ERROR_STOP=1 -c "ALTER DATABASE storage SET default_transaction_isolation TO 'repeatable read'"
docker exec "$pg" psql -U postgres -d storage -Atc 'SHOW default_transaction_isolation'
step=minio-ready
# MinIO announces its API endpoint once its listener accepts; the image has
# no shell for a health command. A stopped container ends the log stream.
await_line 'API: http' docker logs --follow "$minio"
curl -fsS "$STORAGE_S3_ENDPOINT/minio/health/ready"
step=physical-bucket
curl --fail --silent --show-error --aws-sigv4 aws:amz:us-east-1:s3 --user storage-local-access:storage-local-secret -X PUT "$STORAGE_S3_ENDPOINT/storage"

step=compile
bazel build //rust/crates/conformance:storage-conformance //rust/crates/conformance/agent-host:main //rust/crates/stream:provider-conformance_example //rust/crates/stream:acyclic_stream_test //rust/crates/objects:acyclic_objects_test
step=canonical-suites
bazel run //rust/crates/conformance:storage-conformance
step=agent-host-baseline
bazel test //rust/crates/conformance/agent-host:main
step=shared-stream-regressions
bazel test //rust/crates/stream:acyclic_stream_test
step=postgres-cross-session
bazel run //rust/crates/stream:provider-conformance_example -- --postgres
step=postgres-scale
bazel run //rust/crates/stream:provider-conformance_example -- --postgres --scale
step=shared-objects-regressions
bazel test //rust/crates/objects:acyclic_objects_test
step=complete
