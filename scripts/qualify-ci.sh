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
    bash scripts/check-harness-package.sh "$SDK_ARTIFACT_DIR/packages/harness"
    bash scripts/check-filesystem-package.sh "$SDK_ARTIFACT_DIR/packages/filesystem"
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
    if [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" ]]; then
      base="$(jq -er '.pull_request.base.sha' "$GITHUB_EVENT_PATH")"
      git cat-file -e "$base^{commit}" 2>/dev/null || git fetch --no-tags origin "$base"
      bun x buf breaking --against ".git#ref=$base" \
        --exclude-path proto/inference/v1/inference.proto \
        --exclude-path proto/filesystem/v1 \
        --exclude-path proto/filesystem/daemon/v2
    fi
    bash -n scripts/check-typescript-packages.sh
    ;;
  policy)
    bash scripts/test-ensure-rust-target.sh
    bash scripts/test-qualify-gate-rustup.sh
    node scripts/check-workflow-runners.mjs
    actionlint_archive="$TOOLS_DIR/actionlint_1.7.7_linux_amd64.tar.gz"
    actionlint_checksum="023070a287cd8cccd71515fedc843f1985bf96c436b7effaecce67290e7e0757"
    if [[ ! -f "$actionlint_archive" ]] ||
      ! echo "$actionlint_checksum  $actionlint_archive" | sha256sum --check --status; then
      temporary_archive="$(mktemp "${actionlint_archive}.XXXXXXXX")"
      if ! curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
        --max-time 30 \
        https://github.com/rhysd/actionlint/releases/download/v1.7.7/actionlint_1.7.7_linux_amd64.tar.gz \
        --output "$temporary_archive" ||
        ! echo "$actionlint_checksum  $temporary_archive" | sha256sum --check --status; then
        rm -f -- "$temporary_archive"
        exit 1
      fi
      mv -- "$temporary_archive" "$actionlint_archive"
    fi
    actionlint_root="$TOOLS_DIR/actionlint-1.7.7"
    mkdir -p "$actionlint_root"
    tar --extract --gzip --file "$actionlint_archive" --directory "$actionlint_root" actionlint
    "$actionlint_root/actionlint" .github/workflows/*.yml
    node scripts/publish-cargo-crates.mjs check
    node --test scripts/test-publish-cargo-crates.mjs
    node scripts/test-verify-release-binary.mjs
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
    cargo test -p acyclic-labs-plugin --locked
    cargo clippy -p acyclic-labs-plugin --all-targets --all-features --locked -- -D warnings
    head="$(git rev-parse HEAD)"
    allow_webflow=false
    if [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" ]]; then
      event_head="$(jq -er '.pull_request.head.sha' "$GITHUB_EVENT_PATH")"
      [[ "$head" == "$event_head" ]] || {
        echo 'checked-out source does not match the pull request head' >&2
        exit 1
      }
      event_base="$(jq -er '.pull_request.base.sha' "$GITHUB_EVENT_PATH")"
      git cat-file -e "$event_base^{commit}" 2>/dev/null ||
        git fetch --no-tags origin "$event_base"
      base="$(git merge-base "$head" "$event_base")"
      range="$base..$head"
    elif [[ "${GITHUB_EVENT_NAME:-}" == "push" &&
            "${GITHUB_REF:-}" == "refs/heads/main" ]]; then
      event_head="$(jq -er '.after' "$GITHUB_EVENT_PATH")"
      [[ "$head" == "$event_head" ]] || {
        echo 'checked-out source does not match the main push head' >&2
        exit 1
      }
      before="$(jq -er '.before' "$GITHUB_EVENT_PATH")"
      [[ "$before" =~ ^[0-9a-f]{40}$ ]] || {
        echo 'main push must include its previous commit for signature verification' >&2
        exit 1
      }
      if [[ "$before" == "0000000000000000000000000000000000000000" ]]; then
        range="$head"
      else
        git merge-base --is-ancestor "$before" "$head" || {
          echo 'main push previous commit is not an ancestor of its head' >&2
          exit 1
        }
        range="$before..$head"
      fi
      allow_webflow=true
    else
      base="${head}^"
      range="$base..$head"
    fi
    webflow_home=""
    while read -r commit; do
      verification=$(git \
        -c gpg.format=ssh \
        -c "gpg.ssh.allowedSignersFile=$(pwd)/.github/allowed_signers" \
        show --quiet --format='%G?' "$commit")
      if [[ "$verification" == "G" ]]; then
        continue
      fi
      # GitHub signs squash merges with its web-flow OpenPGP key. Only main
      # accepts that pinned key; PR commits must still use allowed SSH signers.
      if [[ "$allow_webflow" == true ]]; then
        if [[ -z "$webflow_home" ]]; then
          webflow_home="$(mktemp -d "$SDK_TEMP_DIR/web-flow.XXXXXXXX")"
          trap 'rm -rf -- "$webflow_home"' EXIT
          curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
            --max-time 30 https://github.com/web-flow.gpg \
            --output "$webflow_home/web-flow.gpg"
          echo "6e8af687f60cf3f403151c8fb1b26e95e6f9e424ca60cc8f3787bd4466a3ef84  $webflow_home/web-flow.gpg" |
            sha256sum --check --status
          GNUPGHOME="$webflow_home" gpg --batch --quiet --import "$webflow_home/web-flow.gpg"
        fi
        signature=$(GNUPGHOME="$webflow_home" git -c gpg.format=openpgp \
          show --quiet --format='%G? %GF' "$commit")
        if [[ "$signature" == "G 968479A1AFF927E37D1A566BB5690EEEBB952194" ||
              "$signature" == "U 968479A1AFF927E37D1A566BB5690EEEBB952194" ]]; then
          continue
        fi
      fi
      echo "Commit $commit lacks an authorized cryptographic signature." >&2
      exit 1
    done < <(git rev-list --reverse "$range")
    if [[ -n "$webflow_home" ]]; then
      rm -rf -- "$webflow_home"
      trap - EXIT
    fi

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
      --log-opts "$range"
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
