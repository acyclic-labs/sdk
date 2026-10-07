#!/usr/bin/env bash
set -euo pipefail

lane="${1:?qualification lane is required}"
: "${SDK_TEMP_DIR:?}"
: "${SDK_ARTIFACT_DIR:?}"
: "${TOOLS_DIR:?}"
mkdir -p "$SDK_TEMP_DIR" "$SDK_ARTIFACT_DIR" "$TOOLS_DIR"
export PATH="$TOOLS_DIR/cargo/bin:$PATH"
target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
# Test timings and each top-level command's start time feed the job summary
# (scripts/ci-summary.mjs, run by the qualification-lane action).
observability="$SDK_TEMP_DIR/observability"
mkdir -p "$observability"
timing_start=$(($(date +%s) - SECONDS))
trap 'printf "%s\t%s\0" "${EPOCHREALTIME:-$((timing_start + SECONDS))}" "$BASH_COMMAND" \
  >>"$observability/steps.tsv"' DEBUG

full_qualification="${FORCE:-false}"
case "${GITHUB_EVENT_NAME:-}" in
  release|workflow_dispatch|schedule) full_qualification=true ;;
esac
# macOS ships shasum rather than sha256sum.
sha256_matches() {
  local observed
  if command -v sha256sum >/dev/null; then
    observed="$(sha256sum "$2")" || return 1
  else
    observed="$(shasum -a 256 "$2")" || return 1
  fi
  [[ "${observed%% *}" == "$1" ]]
}
# Fetches a pinned release archive into $TOOLS_DIR once, verifies its SHA-256
# on every use, and extracts MEMBER into DESTINATION.
install_tool() {
  local archive="$TOOLS_DIR/$1" url="$2" checksum="$3" destination="$4" member="$5"
  local strip="${6:-0}" temporary
  if [[ ! -f "$archive" ]] || ! sha256_matches "$checksum" "$archive"; then
    temporary="$(mktemp "${archive}.XXXXXXXX")"
    if ! curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
      --max-time 30 "$url" --output "$temporary" ||
      ! sha256_matches "$checksum" "$temporary"; then
      rm -f -- "$temporary"
      echo "could not fetch a verified $url" >&2
      return 1
    fi
    mv -- "$temporary" "$archive"
  fi
  mkdir -p "$destination"
  tar --extract --gzip --file "$archive" --directory "$destination" \
    --strip-components="$strip" "$member"
}

# Runs Rust tests with the pinned cargo-nextest: one process per test, test
# binaries in parallel, slow tests flagged, and per-test timings kept as JUnit
# (.config/nextest.toml). Doctests and the serial live-mount suites stay on
# cargo test.
nextest() {
  local target checksum status=0
  if [[ "$(cargo-nextest nextest --version 2>/dev/null)" != "cargo-nextest 0.9.146 "* ]]; then
    case "$(uname -s):$(uname -m)" in
      Linux:x86_64)
        target=x86_64-unknown-linux-gnu
        checksum=682c21b777c333e96fd532e114d3a5a894e0729ab88d94c0a9f20f8419695428 ;;
      Linux:aarch64)
        target=aarch64-unknown-linux-gnu
        checksum=b2e33d7c72de7ade0ff7b3a948ac37516b24f8a836b7a8870c1f634a94be9de9 ;;
      Darwin:*)
        target=universal-apple-darwin
        checksum=39785160b3c2f6ed9a765049cf4fa79f3b39aa02eb7598a5a0e2a1a0b9ffb9a8 ;;
    esac
    install_tool "cargo-nextest-0.9.146-$target.tar.gz" \
      "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-0.9.146/cargo-nextest-0.9.146-$target.tar.gz" \
      "$checksum" "$TOOLS_DIR/cargo/bin" cargo-nextest
  fi
  cargo nextest run --profile ci "$@" || status=$?
  mv -f -- "$target_dir/nextest/ci/junit.xml" "$observability/rust.xml" 2>/dev/null || true
  return "$status"
}

# Independent builds run beside the main test build in their own target
# directories so Cargo's build lock never serializes them; the shared compiler
# cache still deduplicates identical crates across them.
background() {
  local name="$1"
  shift
  ("$@") >"$SDK_TEMP_DIR/background-$name.log" 2>&1 &
  echo "$!" >"$SDK_TEMP_DIR/background-$name.pid"
}
finish() {
  local name status=0
  for name in "$@"; do
    status=0
    wait "$(cat "$SDK_TEMP_DIR/background-$name.pid")" || status=$?
    echo "::group::$name"
    cat "$SDK_TEMP_DIR/background-$name.log"
    echo "::endgroup::"
    if ((status != 0)); then
      echo "::error::$name failed with exit status $status"
      tail -n 80 "$SDK_TEMP_DIR/background-$name.log" >&2
      exit "$status"
    fi
  done
}
release_plugin() {
  CARGO_TARGET_DIR="$target_dir-release" node scripts/build-product.mjs
  node plugin/scripts/package.mjs \
    --binary "$target_dir-release/dist/acyclic" \
    --out "$SDK_ARTIFACT_DIR/acyclic-plugin"
  node plugin/scripts/validate-package.mjs "$SDK_ARTIFACT_DIR/acyclic-plugin"
}
native_binding() {
  CARGO_TARGET_DIR="$target_dir-napi" cargo build -p acyclic-fs-napi --locked
  CARGO_TARGET_DIR="$target_dir-napi" \
    bun scripts/check-filesystem-napi.mjs "$SDK_ARTIFACT_DIR/packages/native"
}
# The live native-mount tests are the only ignored acyclic-fs library tests.
# Selecting them from the all-feature workspace build reuses its test binaries
# instead of rebuilding acyclic-fs under a narrower feature resolution.
native_mount_tests() {
  cargo test --workspace --all-features --locked --lib -- \
    --ignored --test-threads=1 native_mount::
}

# Black-box fork/join conformance through hooks, the CLI and live mounts. The
# shared test support module carries ignored tests of its own, skipped here.
fork_join_conformance() {
  cargo test -p acyclic-plugin --locked --test fork_join -- \
    --ignored --test-threads=1 --skip support::
}

case "$lane" in
  gate)
    # These crates deliberately have independent workspaces; --workspace and
    # llvm-cov cannot cover them, even during full qualification.
    cargo test --manifest-path rust/crates/sdk-docs/Cargo.toml --locked
    if [[ "$full_qualification" != true ]]; then
      # The same suite the full native lanes run, without coverage; the
      # ignored live-mount and fork/join suites stay there.
      nextest --workspace --all-features --locked
      mkdir -p "$SDK_ARTIFACT_DIR/coverage"
      printf '%s\n' '{"scope":"rust-contract-tests","coverage_instrumented":false}' >"$SDK_ARTIFACT_DIR/coverage/core-check.json"
      exit 0
    fi
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
    if [[ "$(cargo-llvm-cov llvm-cov --version 2>/dev/null)" != *" 0.9.1" ]]; then
      # The pinned release archive avoids compiling cargo-llvm-cov on a cold cache.
      install_tool cargo-llvm-cov-0.9.1-x86_64-unknown-linux-gnu.tar.gz \
        https://github.com/taiki-e/cargo-llvm-cov/releases/download/v0.9.1/cargo-llvm-cov-x86_64-unknown-linux-gnu.tar.gz \
        b3f68e625481fed9b16444174f3fa5ebcdbde4a1878803a35eabe2dcefcdc41a \
        "$TOOLS_DIR/cargo/bin" cargo-llvm-cov
      [[ "$(cargo-llvm-cov llvm-cov --version)" == *" 0.9.1" ]]
    fi
    mkdir -p "$SDK_ARTIFACT_DIR/coverage"
    cargo llvm-cov --workspace --all-features --locked --fail-under-lines 70 \
      --lcov --output-path "$SDK_ARTIFACT_DIR/coverage/lcov.info"
    cargo llvm-cov report --summary-only
    ;;
  preflight)
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
    elif [[ "${GITHUB_EVENT_NAME:-}" == "release" ]]; then
      release_tag="$(jq -er '.release.tag_name' "$GITHUB_EVENT_PATH")"
      [[ "${GITHUB_REF:-}" == "refs/tags/$release_tag" ]] || {
        echo 'release ref does not match the event tag' >&2
        exit 1
      }
      [[ "$(git rev-parse "$GITHUB_REF^{commit}")" == "$head" ]] || {
        echo 'checked-out source does not match the release tag' >&2
        exit 1
      }
      git fetch --no-tags origin main
      git merge-base --is-ancestor "$head" FETCH_HEAD || {
        echo 'release source is not part of main' >&2
        exit 1
      }
      range="${head}^..$head"
      allow_webflow=true
    else
      base="${head}^"
      range="$base..$head"
      # Scheduled and dispatched runs of main check its latest squash merge,
      # which GitHub signs with the same pinned web-flow key as main pushes.
      if [[ "${GITHUB_REF:-}" == "refs/heads/main" ]]; then
        allow_webflow=true
      fi
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
      # GitHub signs squash merges with its web-flow OpenPGP key. Main and
      # release tags verified as part of main accept it; PR commits use SSH.
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

    install_tool gitleaks_8.30.1_linux_x64.tar.gz \
      https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/gitleaks_8.30.1_linux_x64.tar.gz \
      551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb \
      "$TOOLS_DIR/gitleaks-8.30.1" gitleaks
    "$TOOLS_DIR/gitleaks-8.30.1/gitleaks" detect --source . --no-banner --redact \
      --log-opts "$range"
    ;;
  linux)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    wasm_bindgen_bin="$(bash scripts/ensure-wasm-bindgen.sh)"
    export PATH="$(dirname "$wasm_bindgen_bin"):$PATH"
    bun install --frozen-lockfile
    # Package validation runs with --offline; populate every locked crate even
    # when the Blacksmith dependency cache is cold.
    cargo fetch --locked
    background release release_plugin
    background napi native_binding
    cargo test -p acyclic-fs --features native-mount --locked --lib -- \
      --ignored --test-threads=1
    fork_join_conformance
    bash scripts/check-inference-package.sh "$SDK_ARTIFACT_DIR/packages/inference"
    bash scripts/check-machines-package.sh "$SDK_ARTIFACT_DIR/packages/machines"
    finish napi release
    bun run test
    bun scripts/check-typescript-tarballs.mjs
    # WASM builds are path-independent but not host-independent: panic
    # locations keep the host's path separators and private wasm-bindgen
    # closure names carry host-derived crate hashes. Verify the fresh WASM
    # against the committed package API and runtime, then stage the committed
    # release bytes.
    git restore --worktree -- \
      typescript/packages/filesystem/generated/wasm \
      typescript/packages/stream/generated/wasm
    bun run check:generated
    bash scripts/check-harness-package.sh "$SDK_ARTIFACT_DIR/packages/harness"
    bash scripts/check-filesystem-package.sh "$SDK_ARTIFACT_DIR/packages/filesystem"
    bun scripts/run-harness-conformance.mjs \
      "$SDK_ARTIFACT_DIR/packages/harness" \
      "$SDK_ARTIFACT_DIR/packages/harness/runner-report.json" \
      "$SDK_ARTIFACT_DIR/packages/harness/qualification-receipt.json"
    bun run licenses
    bun x buf format -d --exit-code
    bun x buf lint
    bun scripts/check-boundaries.mjs
    bun scripts/check-metadata.mjs
    # No wire or JSON compatibility gate: each family's protocol identity
    # binds peers and stored records to one exact descriptor digest, so a
    # schema change is always a new, fail-closed version (no upgrade path).
    bash -n scripts/check-typescript-packages.sh
    ;;
  linux-musl|linux-arm64-musl)
    if [[ "$lane" == linux-musl ]]; then
      target=x86_64-unknown-linux-musl
      release_target=linux-x64-musl
    else
      target=aarch64-unknown-linux-musl
      release_target=linux-arm64-musl
    fi
    sudo apt-get update -qq
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y musl-tools
    rustup target add "$target"
    export CC_aarch64_unknown_linux_musl=musl-gcc
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc
    background release env CARGO_TARGET_DIR="$target_dir-release" \
      CARGO_BUILD_TARGET="$target" node scripts/build-product.mjs
    # Run capture admission and publication in the shipped libc ABI. Path
    # statx and held-descriptor fstat can expose different optional metadata.
    cargo test -p acyclic-fs --features native-mount --locked \
      --target "$target" --lib native_capture::
    finish release
    binary="$target_dir-release/$target/dist/acyclic"
    node scripts/verify-release-binary.mjs "$release_target" "$binary"
    expected="$(cargo metadata --locked --no-deps --format-version 1 |
      jq -r '.packages[] | select(.name == "acyclic-plugin") | "acyclic \(.version)"')"
    test "$("$binary" --version)" = "$expected"
    ;;
  policy)
    bash scripts/test-qualify-ci-preflight.sh
    if [[ "$full_qualification" != true ]]; then
      cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
      node --test scripts/test-plan-qualification.mjs
      cargo fmt --all -- --check
      exit 0
    fi
    bash scripts/test-ensure-rust-target.sh
    bash scripts/test-qualify-gate-rustup.sh
    node scripts/check-workflow-runners.mjs
    node --test scripts/harness-provider-evidence.test.mjs
    install_tool actionlint_1.7.7_linux_amd64.tar.gz \
      https://github.com/rhysd/actionlint/releases/download/v1.7.7/actionlint_1.7.7_linux_amd64.tar.gz \
      023070a287cd8cccd71515fedc843f1985bf96c436b7effaecce67290e7e0757 \
      "$TOOLS_DIR/actionlint-1.7.7" actionlint
    "$TOOLS_DIR/actionlint-1.7.7/actionlint" .github/workflows/*.yml
    node scripts/publish-cargo-crates.mjs check
    node --test scripts/test-publish-cargo-crates.mjs
    node --test scripts/test-publish-npm-packages.mjs
    node --test scripts/test-typescript-qualification.mjs
    node --test scripts/test-plan-qualification.mjs
    node scripts/test-verify-release-binary.mjs
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    node scripts/clippy-feature-sets.mjs 2
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
    install_tool cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz \
      https://github.com/EmbarkStudios/cargo-deny/releases/download/0.19.0/cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz \
      0e8c2aa59128612c90d9e09c02204e912f29a5b8d9a64671b94608cbe09e064f \
      "$TOOLS_DIR/cargo-deny-0.19.0" cargo-deny-0.19.0-x86_64-unknown-linux-musl/cargo-deny 1
    "$TOOLS_DIR/cargo-deny-0.19.0/cargo-deny" check
    ;;
  web)
    bash scripts/ensure-rust-target.sh wasm32-unknown-unknown
    # The host clippy run cannot see code that only the browser build
    # compiles or omits, so lint each browser build as it ships.
    cargo clippy -p acyclic-fs-wasm --target wasm32-unknown-unknown \
      --all-targets --all-features --locked -- -D warnings
    cargo clippy -p acyclic-harness --features wasm \
      --target wasm32-unknown-unknown --locked -- -D warnings
    cargo clippy -p acyclic-machines-wasm -p acyclic-inference-wasm -p acyclic-objects-wasm \
      --target wasm32-unknown-unknown --all-targets --all-features --locked -- -D warnings
    if [[ "$(wasm-bindgen-test-runner --version 2>/dev/null)" != "wasm-bindgen-test-runner 0.2.117" ]]; then
      # The pinned release archive avoids compiling wasm-bindgen-cli on a cold cache.
      install_tool wasm-bindgen-0.2.117-x86_64-unknown-linux-musl.tar.gz \
        https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.117/wasm-bindgen-0.2.117-x86_64-unknown-linux-musl.tar.gz \
        97f527f7c7956f69a88a4bdb5176142ebc4e255c2dbe3805ec4f373421028240 \
        "$TOOLS_DIR/cargo/bin" wasm-bindgen-0.2.117-x86_64-unknown-linux-musl/wasm-bindgen-test-runner 1
      [[ "$(wasm-bindgen-test-runner --version)" == "wasm-bindgen-test-runner 0.2.117" ]]
    fi
    export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner
    CHROMEDRIVER="$(command -v chromedriver)" \
      cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    GECKODRIVER="$(command -v geckodriver)" \
      cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    # The shipped browser package end to end in headless Chrome: one tab, then
    # concurrent tabs sharing one database.
    source scripts/ensure-bun.sh
    wasm_bindgen_bin="$(bash scripts/ensure-wasm-bindgen.sh)"
    export PATH="$(dirname "$wasm_bindgen_bin"):$PATH"
    bun install --frozen-lockfile
    bun run --filter '@acyclic-labs/fs' build
    # The combined browser pages consume the public Stream and Harness builds;
    # Harness also needs its Objects package declaration dependency.
    bun run --filter '@acyclic-labs/objects' build
    bun run --filter '@acyclic-labs/stream' build
    bun run --filter '@acyclic-labs/harness' build
    CHROME="$(command -v google-chrome || command -v chromium)" \
      bun run --filter '@acyclic-labs/fs' test:browser
    ;;
  linux-arm64)
    if ! command -v cc >/dev/null || ! command -v unzip >/dev/null || \
      ! command -v pkg-config >/dev/null || ! pkg-config --exists fuse3; then
      sudo apt-get update -qq
      sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends build-essential pkg-config libfuse3-dev unzip
    fi
    source scripts/ensure-bun.sh
    cargo fetch --locked
    background napi native_binding
    nextest --workspace --all-features --locked
    cargo test --workspace --all-features --locked --doc
    finish napi
    native_mount_tests
    ;;
  macos)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    bash scripts/ensure-rust-target.sh x86_64-apple-darwin
    cargo fetch --locked
    background release release_plugin
    background napi native_binding
    background x86_64 cargo check -p acyclic-fs -p acyclic-fs-napi --all-features \
      --target x86_64-apple-darwin --locked --target-dir "$target_dir-x86_64"
    nextest --workspace --all-features --locked
    cargo test --workspace --all-features --locked --doc
    finish napi release x86_64
    native_mount_tests
    fork_join_conformance
    ;;
  *)
    echo "unknown qualification lane: $lane" >&2
    exit 2
    ;;
esac
