#!/usr/bin/env bash
set -euo pipefail

lane="${1:?qualification lane is required}"
mkdir -p "${AGENT_TEMPDIRECTORY:?}" "${BUILD_ARTIFACTSTAGINGDIRECTORY:?}" "${TOOLS_DIR:?}"

case "$lane" in
  linux)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    bun install --frozen-lockfile
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    cargo test --workspace --all-features --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- --ignored --test-threads=1
    cargo build -p acyclic-fs-napi --locked
    bun scripts/check-filesystem-napi.mjs "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/native"
    bash scripts/check-inference-package.sh "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/inference"
    cargo run --locked -p acyclic-cli
    bun run test
    bash scripts/check-filesystem-package.sh "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/filesystem"
    bash scripts/check-harness-package.sh "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/harness"
    bun scripts/run-harness-conformance.mjs \
      "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/harness" \
      "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/harness/runner-report.json" \
      "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/harness/qualification-receipt.json"
    bun run licenses
    bun x buf format -d --exit-code
    bun x buf lint
    bun run check:generated
    bun scripts/check-boundaries.mjs
    bun scripts/check-metadata.mjs
    base="${SYSTEM_PULLREQUEST_TARGETBRANCH:-}"
    if [[ -n "$base" ]]; then
      base=${base#refs/heads/}
      bun x buf breaking --against ".git#ref=origin/$base" \
        --exclude-path proto/inference/v1/inference.proto \
        --exclude-path proto/filesystem/v1 \
        --exclude-path proto/filesystem/daemon/v1
    fi
    ;;
  policy)
    bash scripts/test-ensure-rust-target.sh
    head="${SYSTEM_PULLREQUEST_SOURCECOMMITID:-$BUILD_SOURCEVERSION}"
    if [[ -n "${SYSTEM_PULLREQUEST_TARGETBRANCH:-}" ]]; then
      branch="${SYSTEM_PULLREQUEST_TARGETBRANCH#refs/heads/}"
      git fetch --no-tags origin "$branch"
      base="$(git merge-base "$head" "origin/$branch")"
    else
      base="${head}^"
    fi
    while read -r commit; do
      git show --quiet --format=%B "$commit" | grep --quiet --ignore-case '^Signed-off-by: .\+ <.\+>$' || {
        echo "Commit $commit lacks a Signed-off-by trailer." >&2
        exit 1
      }
    done < <(git rev-list --reverse "$base..$head")

    archive="$TOOLS_DIR/cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz"
    mkdir -p "$TOOLS_DIR"
    if [[ ! -f "$archive" ]]; then
      curl --fail --silent --show-error --location --max-time 30 \
        https://github.com/EmbarkStudios/cargo-deny/releases/download/0.19.0/cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz \
        --output "$archive"
    fi
    echo "0e8c2aa59128612c90d9e09c02204e912f29a5b8d9a64671b94608cbe09e064f  $archive" | sha256sum --check
    deny_root="$(mktemp -d "${AGENT_TEMPDIRECTORY:?}/cargo-deny.XXXXXXXX")"
    trap 'rm -rf -- "$deny_root"' EXIT
    tar -xzf "$archive" -C "$deny_root" --strip-components=1 \
      cargo-deny-0.19.0-x86_64-unknown-linux-musl/cargo-deny
    "$deny_root/cargo-deny" check licenses

    archive="$TOOLS_DIR/gitleaks_8.30.1_linux_x64.tar.gz"
    mkdir -p "$TOOLS_DIR/gitleaks-8.30.1"
    if [[ ! -x "$TOOLS_DIR/gitleaks-8.30.1/gitleaks" ]]; then
      curl --fail --silent --show-error --location --max-time 30 \
        https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/gitleaks_8.30.1_linux_x64.tar.gz \
        --output "$archive"
      echo "551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb  $archive" | sha256sum --check
      tar --extract --gzip --file "$archive" --directory "$TOOLS_DIR/gitleaks-8.30.1" gitleaks
    fi
    "$TOOLS_DIR/gitleaks-8.30.1/gitleaks" detect --source . --no-banner --redact
    python3 scripts/check-secret-policy.py "$TOOLS_DIR/gitleaks-8.30.1/gitleaks"
    ;;
  web)
    bash scripts/ensure-rust-target.sh wasm32-unknown-unknown
    if ! command -v wasm-bindgen-test-runner >/dev/null; then
      cargo install --locked wasm-bindgen-cli --version 0.2.117
    fi
    export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
    CHROMEDRIVER="$(command -v chromedriver)" cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    GECKODRIVER="$(command -v geckodriver)" cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    ;;
  linux-arm64)
    if ! command -v cc >/dev/null || ! command -v unzip >/dev/null || \
      ! command -v pkg-config >/dev/null || ! pkg-config --exists fuse3; then
      sudo apt-get update -qq
      sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential pkg-config libfuse3-dev unzip
    fi
    source scripts/ensure-bun.sh
    cargo test --workspace --all-features --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- --ignored --test-threads=1
    cargo build -p acyclic-fs-napi --locked
    bun scripts/check-filesystem-napi.mjs "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/native"
    ;;
  macos)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    cargo test --workspace --all-features --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- --ignored --test-threads=1
    cargo build -p acyclic-fs-napi --locked
    bun scripts/check-filesystem-napi.mjs "$BUILD_ARTIFACTSTAGINGDIRECTORY/packages/native"
    bash scripts/ensure-rust-target.sh x86_64-apple-darwin
    cargo check -p acyclic-fs -p acyclic-fs-napi --all-features --target x86_64-apple-darwin --locked
    ;;
  *)
    echo "unknown qualification lane: $lane" >&2
    exit 2
    ;;
esac
