#!/usr/bin/env bash
set -euo pipefail

lane="${1:?qualification lane is required}"
: "${SDK_TEMP_DIR:?}"
: "${SDK_ARTIFACT_DIR:?}"
: "${TOOLS_DIR:?}"
mkdir -p "$SDK_TEMP_DIR" "$SDK_ARTIFACT_DIR" "$TOOLS_DIR"
export PATH="$TOOLS_DIR/cargo/bin:$PATH"

case "$lane" in
  gate)
    if ! rustup component list --installed | grep -Eq '^llvm-tools-'; then
      component_log="$(mktemp "${SDK_TEMP_DIR}/rustup-component.XXXXXXXX")"
      trap 'rm -f -- "${component_log:-}"' EXIT
      if ! rustup component add llvm-tools-preview 2>&1 | tee "$component_log"; then
        grep -Eqi 'detected conflict|could not rename|File exists|already exists' "$component_log" || exit 1
        toolchain="$(rustup show active-toolchain | awk 'NR == 1 { print $1 }')"
        [[ "$toolchain" =~ ^[0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9_.-]+$ ]] || {
          echo 'refusing to repair an unexpected Rust toolchain' >&2
          exit 1
        }
        rustup toolchain uninstall "$toolchain"
        rustup toolchain install "$toolchain" --profile minimal \
          --component clippy --component rustfmt --component llvm-tools-preview
      fi
      rm -f -- "$component_log"
      trap - EXIT
    fi
    if ! command -v cargo-llvm-cov >/dev/null; then
      cargo install cargo-llvm-cov --version 0.9.1 --locked --root "$TOOLS_DIR/cargo"
    fi
    mkdir -p "$SDK_ARTIFACT_DIR/coverage"
    cargo llvm-cov --workspace --all-features --locked --fail-under-lines 70 \
      --lcov --output-path "$SDK_ARTIFACT_DIR/coverage/lcov.info"
    cargo llvm-cov report --summary-only
    ;;
  linux)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    bun install --frozen-lockfile
    # Package validation runs with --offline; populate every locked crate even
    # when the Blacksmith dependency cache is cold.
    cargo fetch --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- \
      --ignored --test-threads=1
    cargo build -p acyclic-fs-napi --locked
    bun scripts/check-filesystem-napi.mjs "$SDK_ARTIFACT_DIR/packages/native"
    bash scripts/check-inference-package.sh "$SDK_ARTIFACT_DIR/packages/inference"
    bash scripts/check-machines-package.sh "$SDK_ARTIFACT_DIR/packages/machines"
    node scripts/build-product.mjs
    node plugin/scripts/package.mjs \
      --binary "${CARGO_TARGET_DIR:-target}/release/acyclic" \
      --out "$SDK_ARTIFACT_DIR/acyclic-plugin"
    node plugin/scripts/validate-package.mjs \
      "$SDK_ARTIFACT_DIR/acyclic-plugin"
    bun run test
    bash scripts/check-filesystem-package.sh "$SDK_ARTIFACT_DIR/packages/filesystem"
    bash scripts/check-harness-package.sh "$SDK_ARTIFACT_DIR/packages/harness"
    bun scripts/run-harness-conformance.mjs \
      "$SDK_ARTIFACT_DIR/packages/harness" \
      "$SDK_ARTIFACT_DIR/packages/harness/runner-report.json" \
      "$SDK_ARTIFACT_DIR/packages/harness/qualification-receipt.json"
    bun run licenses
    bun x buf format -d --exit-code
    bun x buf lint
    bun run check:generated
    bun scripts/check-boundaries.mjs
    bun scripts/check-metadata.mjs
    base="${CI_TARGET_BRANCH:-}"
    if [[ -n "$base" ]]; then
      bun x buf breaking --against ".git#ref=origin/$base" \
        --exclude-path proto/inference/v1/inference.proto \
        --exclude-path proto/filesystem/v1 \
        --exclude-path proto/filesystem/daemon/v2
    fi
    bash -n scripts/check-typescript-packages.sh
    ;;
  policy)
    bash scripts/test-ensure-rust-target.sh
    bash scripts/test-qualify-gate-rustup.sh
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    cargo test -p acyclic-labs-plugin --locked
    cargo clippy -p acyclic-labs-plugin --all-targets --all-features --locked -- -D warnings
    head="${CI_HEAD_SHA:-$(git rev-parse HEAD)}"
    if [[ -n "${CI_TARGET_BRANCH:-}" ]]; then
      branch="$CI_TARGET_BRANCH"
      git fetch --no-tags origin "$branch"
      base="$(git merge-base "$head" "origin/$branch")"
    else
      base="${head}^"
    fi
    while read -r commit; do
      git show --quiet --format=%B "$commit" | \
        grep --quiet --ignore-case '^Signed-off-by: .\+ <.\+>$' || {
          echo "Commit $commit lacks a Signed-off-by trailer." >&2
          exit 1
        }
    done < <(git rev-list --reverse "$base..$head")

    archive="$TOOLS_DIR/cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz"
    if [[ ! -f "$archive" ]]; then
      curl --fail --silent --show-error --location --max-time 30 \
        https://github.com/EmbarkStudios/cargo-deny/releases/download/0.19.0/cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz \
        --output "$archive"
    fi
    echo "0e8c2aa59128612c90d9e09c02204e912f29a5b8d9a64671b94608cbe09e064f  $archive" | sha256sum --check
    deny_root="$(mktemp -d "$SDK_TEMP_DIR/cargo-deny.XXXXXXXX")"
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
      tar --extract --gzip --file "$archive" --directory \
        "$TOOLS_DIR/gitleaks-8.30.1" gitleaks
    fi
    "$TOOLS_DIR/gitleaks-8.30.1/gitleaks" detect --source . --no-banner --redact \
      --log-opts "$base..$head"
    ;;
  web)
    bash scripts/ensure-rust-target.sh wasm32-unknown-unknown
    if ! command -v wasm-bindgen-test-runner >/dev/null; then
      cargo install --locked wasm-bindgen-cli --version 0.2.117 --root "$TOOLS_DIR/cargo"
    fi
    export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
    CHROMEDRIVER="$(command -v chromedriver)" \
      cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    GECKODRIVER="$(command -v geckodriver)" \
      cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    ;;
  linux-arm64)
    if ! command -v cc >/dev/null || ! command -v unzip >/dev/null || \
      ! command -v pkg-config >/dev/null || ! pkg-config --exists fuse3; then
      sudo apt-get update -qq
      sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends build-essential pkg-config libfuse3-dev unzip
    fi
    source scripts/ensure-bun.sh
    cargo test --workspace --all-features --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- \
      --ignored --test-threads=1
    cargo build -p acyclic-fs-napi --locked
    bun scripts/check-filesystem-napi.mjs "$SDK_ARTIFACT_DIR/packages/native"
    ;;
  macos)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    cargo test --workspace --all-features --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- \
      --ignored --test-threads=1
    cargo build -p acyclic-fs-napi --locked
    bun scripts/check-filesystem-napi.mjs "$SDK_ARTIFACT_DIR/packages/native"
    node scripts/build-product.mjs
    node plugin/scripts/package.mjs \
      --binary "${CARGO_TARGET_DIR:-target}/release/acyclic" \
      --out "$SDK_ARTIFACT_DIR/acyclic-plugin"
    node plugin/scripts/validate-package.mjs \
      "$SDK_ARTIFACT_DIR/acyclic-plugin"
    bash scripts/ensure-rust-target.sh x86_64-apple-darwin
    cargo check -p acyclic-fs -p acyclic-fs-napi --all-features \
      --target x86_64-apple-darwin --locked
    ;;
  *)
    echo "unknown qualification lane: $lane" >&2
    exit 2
    ;;
esac
