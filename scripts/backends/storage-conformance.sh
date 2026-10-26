#!/usr/bin/env bash
# Run only through bs.ps1 on bb2. Every provider is a disposable local emulator.
set -euo pipefail
step=setup
root=
pg=
minio=
cleanup() {
    rc=$?
    if (( rc != 0 )); then
        echo "step=$step failed (exit $rc); recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/backends/storage-conformance.sh" >&2
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
root=$(mktemp -d /mnt/bb/be-storage.XXXXXXXX)
case "$root" in /mnt/bb/be-storage.*) ;; *) echo "step=setup: unexpected data root $root; recovery: ls -ld /mnt/bb" >&2; exit 2 ;; esac

step=images
minio_image=be-storage-minio:RELEASE.2025-10-15T17-29-55Z
for image in postgres:17-alpine; do
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        timeout 120 docker pull "$image"
    fi
done
if ! docker image inspect "$minio_image" >/dev/null 2>&1; then
    step=minio-source
    # Official release archive, captured on 2026-10-26. The upstream public
    # container and binary distribution returned unauthorized/410 on that date.
    curl --fail --location --max-time 60 --retry 1 --output "$root/minio.tar.gz" \
        https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-10-15T17-29-55Z
    echo "be6d0bd3696c3a13a35f02d3a0280b64319c67918b4501c5c3d87f96d000085c  $root/minio.tar.gz" | sha256sum --check
    mkdir "$root/minio-source"
    tar -xzf "$root/minio.tar.gz" --strip-components=1 -C "$root/minio-source"
    step=minio-build
    docker build -f scripts/backends/minio.Dockerfile -t "$minio_image" "$root/minio-source"
fi
step=postgres-start
mkdir -p "$root/postgres" "$root/minio"
# Reserved lane ports, outside the Cloud local-cell range. Docker refuses an
# occupied port; the EXIT trap reports recovery and removes either container.
pg=$(docker run -d --rm -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=storage -v "$root/postgres:/var/lib/postgresql/data" -p 127.0.0.1:15400:5432 postgres:17-alpine)
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
# The image's temporary bootstrap server accepts Unix sockets only. Probe TCP
# so readiness cannot pass before that server shuts down and the final one starts.
for _ in {1..40}; do
    if docker exec "$pg" pg_isready -h 127.0.0.1 -U postgres -d storage >/dev/null; then break; fi
    sleep 0.25
done
docker exec "$pg" pg_isready -h 127.0.0.1 -U postgres -d storage
step=postgres-isolation-fixture
# Exercise a nondefault server setting: publication must choose its own fresh
# statement snapshots after waiting for the namespace advisory lock.
docker exec "$pg" psql -U postgres -d storage -v ON_ERROR_STOP=1 -c "ALTER DATABASE storage SET default_transaction_isolation TO 'repeatable read'"
docker exec "$pg" psql -U postgres -d storage -Atc 'SHOW default_transaction_isolation'
step=minio-ready
for _ in {1..40}; do
    if curl -fsS "$STORAGE_S3_ENDPOINT/minio/health/ready" >/dev/null 2>&1; then break; fi
    sleep 0.25
done
curl -fsS "$STORAGE_S3_ENDPOINT/minio/health/ready"
step=physical-bucket
curl --fail --silent --show-error --aws-sigv4 aws:amz:us-east-1:s3 --user storage-local-access:storage-local-secret -X PUT "$STORAGE_S3_ENDPOINT/storage"

step=compile
cargo build --locked -p acyclic-conformance --features storage-backends --bin storage-conformance
step=canonical-suites
timeout 60 cargo run --locked -p acyclic-conformance --features storage-backends --bin storage-conformance
step=compile-postgres-checks
cargo build --locked -p acyclic-stream --features postgres --example postgres-conformance
step=postgres-cross-session
timeout 60 cargo run --locked -p acyclic-stream --features postgres --example postgres-conformance
step=postgres-scale
timeout 60 cargo run --locked -p acyclic-stream --features postgres --example postgres-conformance -- --scale
step=compile-shared-objects-regressions
cargo test --locked -p acyclic-objects --features local,s3 --lib --no-run
step=shared-objects-regressions
timeout 60 cargo test --locked -p acyclic-objects --features local,s3 --lib
step=complete
