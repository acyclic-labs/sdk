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
  # Rust drift tests consume ignored TypeScript authoring outputs. Generate them
  # with the existing Rust producer; these lanes do not require Bun or protoc JS.
  cargo run --locked -p acyclic-actors --example actors-http-routes -- \
    typescript/packages/actors/src/generated/actors-service.ts
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
  local name status=0 result=0 log_status=0
  for name in "$@"; do
    status=0
    wait "$(cat "$SDK_TEMP_DIR/background-$name.pid")" || status=$?
    log_status=0
    command node scripts/ci-background-log.mjs "$SDK_TEMP_DIR/background-$name.log" "$name" "$status" || log_status=$?
    if ((status != 0)); then
      ((result != 0)) || result=$status
    fi
    if ((log_status != 0)); then
      ((result != 0)) || result=$log_status
    fi
  done
  return "$result"
}
native_binding() {
  CARGO_TARGET_DIR="$target_dir-napi" cargo build -p acyclic-fs-napi --locked
  CARGO_TARGET_DIR="$target_dir-napi" \
    bun scripts/check-filesystem-napi.mjs "$SDK_ARTIFACT_DIR/packages/native"
}
stream_binding() {
  local target directory="$SDK_ARTIFACT_DIR/packages/stream-native"
  target="$(rustc --version --verbose | sed -n 's/^host: //p')"
  node scripts/build-stream-native.mjs build --target "$target" \
    --output "$directory/bundle" --target-dir "$target_dir-stream-native"
  cp "$target_dir-stream-native/stream-native-build-inputs.receipt.json" "$directory/producer-receipt.json"
}
client_bindings() {
  local target family bundle
  target="$(rustc --version --verbose | sed -n 's/^host: //p')"
  for family in actors; do
    bundle="$SDK_ARTIFACT_DIR/packages/$family-native/bundle"
    node "scripts/build-$family-native.mjs" build --target "$target" \
      --output "$bundle" --target-dir "$target_dir-$family-native"
    cp "$target_dir-$family-native/$family-native-build-inputs.receipt.json" "$(dirname "$bundle")/producer-receipt.json"
    node "scripts/build-$family-native.mjs" stage --bundle "$bundle"
  done
  # The live TLS fixture invokes this helper inside Bun's short test deadline.
  cargo build --offline --locked -p acyclic-actors --example conformance-certificate
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

# Exercise the installed native host against its real local providers, not a
# test-only coordinator. The script records binary, suite and scenario evidence.
installed_graphcoder() {
  local install_root="$SDK_TEMP_DIR/graphcoder-installed"
  cargo install --path rust/crates/harness/src/bin/graphcoder --locked --debug \
    --root "$install_root" --target-dir "$target_dir"
  bun install --cwd scripts/graphcoder-pty --frozen-lockfile --ignore-scripts
  node --test scripts/graphcoder-pty/screen.test.mjs
  node scripts/test-graphcoder.mjs "$install_root/bin/graphcoder"
  node scripts/test-graphcoder-pty.mjs "$install_root/bin/graphcoder"
}

# Keep external-host inputs pinned and exercise the source-built CLI, including
# native lost-ack recovery and cancellation against the real installed Pi host.
pinned_pi() {
  local hosts="$PWD/plugin/tests/hosts" binary="$target_dir/debug/acyclic"
  cargo build -p acyclic-plugin --bin acyclic --locked
  export ACYCLIC_BINARY="$binary"
  export ACYCLIC_PI_TEST_NODE="$(command -v node)"
  export ACYCLIC_PI_TEST_CLI="$hosts/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"
  export ACYCLIC_PI_TEST_PROVIDER_DIR="$hosts"
  (
    cd "$hosts"
    npm ci --ignore-scripts --no-audit --no-fund
    bun run test:pi-context
    bun run test:pi-context-mail
    bun run test:pi-bootstrap
    bun run test:pi-session
    bun run test:pi-installation
    bun run test:pi-service
    bun run test:pi-native-recursive
    bun run test:pi-context-fresh
    bun run test:pi-context-pages
    bun run test:pi-builtin-paths
  )
}

# Every shipped browser feature set, with its unit and integration tests,
# compiles warning-free on wasm32; test-only code is not exempt.
web_warning_closures() {
  local features
  for features in browser wasm browser,test-support wasm,test-support; do
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
      cargo check --locked -p acyclic-harness -p acyclic-harness-core --no-default-features \
        --features "$features" --target wasm32-unknown-unknown --lib --tests
  done
  RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
    cargo check --locked -p acyclic-harness -p acyclic-harness-core --no-default-features \
      --target wasm32-unknown-unknown --lib
}

harness_tests() {
  cargo test --locked "$@" 2>&1 | tee "$observability/harness-tests.log"
  grep -Eq 'test result: ok\. [1-9][0-9]* passed;' "$observability/harness-tests.log"
  ! grep -Eq 'test result: ok\. 0 passed;' "$observability/harness-tests.log"
}

case "$lane" in
  gate)
    node scripts/check-hermetic-clippy.mjs
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
      cargo check --locked -p acyclic-harness-fuzz --bins
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings --cfg acyclic_harness_loom --cfg acyclic_loom" \
      cargo check --locked -p acyclic-harness -p acyclic-harness-core --features model-checking --lib
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings --cfg acyclic_harness_loom --cfg acyclic_loom" \
      cargo check --locked -p acyclic-harness -p acyclic-harness-core --features filesystem-local,model-checking --lib
    # Unit tests compile without optional native features, so a test seam
    # cannot cross a feature gate the all-features suites never close.
    cargo check --locked -p acyclic-harness -p acyclic-harness-core --features filesystem --lib --tests
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings --cfg acyclic_harness_loom --cfg acyclic_loom" \
      harness_tests -p acyclic-harness --features model-checking \
        --test client-mailbox-model --test client_journal_model
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings --cfg acyclic_harness_loom --cfg acyclic_loom" \
      harness_tests -p acyclic-harness-core --features model-checking --test mcp-loom
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings --cfg acyclic_harness_loom --cfg acyclic_loom" \
      harness_tests -p acyclic-harness-core --features model-checking \
        --lib workflow::model_checking::tests::
    node scripts/qualify-mcp-loom.mjs --gate
    nextest --workspace --all-features --locked
    cargo test -p acyclic-harness --all-features --locked --example approved-native-process
    ;;
  mount-exclusion)
    bash scripts/fuzz-mount-exclusion.sh -runs=64 -max_len=4096 -timeout=2
    ;;
  docs)
    # Independent workspaces qualify alongside the root workspace.
    cargo test --manifest-path rust/crates/sdk-docs/Cargo.toml --locked
    ;;
  generation)
    cargo test --manifest-path rust/crates/sdk-generation/Cargo.toml --locked
    node --test rust/crates/sdk-generation/scripts/test-qualify-typescript-snippets.mjs
    ;;
  preflight)
    head="$(git rev-parse HEAD)"
    allow_webflow=false
    base="${head}^"
    range="$base..$head"
    # Dispatched main runs verify the latest squash merge with GitHub's pinned key.
    if [[ "${GITHUB_REF:-}" == "refs/heads/main" ]]; then
      allow_webflow=true
    fi
    commits="$(git rev-list --reverse "$range")"
    [[ -n "$commits" ]] || { echo 'qualification range contains no commits' >&2; exit 1; }
    webflow_home=""
    while read -r commit; do
      verification=$(git \
        -c gpg.format=ssh \
        -c "gpg.ssh.allowedSignersFile=$(pwd)/.github/allowed_signers" \
        show --quiet --format='%G?' "$commit")
      if [[ "$verification" == "G" ]]; then
        continue
      fi
      # GitHub signs squash merges with its web-flow OpenPGP key.
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
    done <<<"$commits"
    if [[ -n "$webflow_home" ]]; then
      rm -rf -- "$webflow_home"
      trap - EXIT
    fi

    install_tool gitleaks_8.30.1_linux_x64.tar.gz \
      https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/gitleaks_8.30.1_linux_x64.tar.gz \
      551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb \
      "$TOOLS_DIR/gitleaks-8.30.1" gitleaks
    # Pinned 8.30.1 can exit zero after Git errors; require nonempty scan evidence.
    # First-parent merge diffs include authored/resolution changes; counts remain evidence, not a proof.
    scan_log="$observability/gitleaks.log"
    "$TOOLS_DIR/gitleaks-8.30.1/gitleaks" detect --source . --no-banner --no-color --redact \
      --log-opts "--diff-merges=first-parent $range" 2>&1 | tee "$scan_log"
    if grep -Eqi '(^|[[:space:]])ERR[[:space:]]|fatal:|error:' "$scan_log" ||
      ! grep -Eq ' INF [1-9][0-9]* commits? scanned\.$' "$scan_log" ||
      ! grep -Eq ' INF scanned ~[1-9][0-9]* bytes ' "$scan_log" ||
      ! grep -Eq ' INF no leaks found$' "$scan_log"; then
      echo 'gitleaks did not establish a successful nonempty source scan' >&2
      exit 1
    fi
    ;;
  linux-runtime)
    cargo fetch --locked
    cargo test -p acyclic-fs --features native-mount --locked --lib -- \
      --ignored --test-threads=1
    fork_join_conformance
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
    node scripts/check-git-proto-build.mjs
    background napi native_binding
    bash scripts/check-inference-package.sh "$SDK_ARTIFACT_DIR/packages/inference"
    bash scripts/check-machines-package.sh "$SDK_ARTIFACT_DIR/packages/machines"
    finish napi
    installed_graphcoder
    pinned_pi
    stream_binding
    client_bindings
    # Warm the three isolated WASM targets concurrently; the complete workspace
    # test still runs its normal build and validates every resulting package.
    node scripts/build-wasm-groups.mjs
    bun run test
    bun scripts/check-typescript-tarballs.mjs
    background harness-package bash scripts/check-harness-package.sh "$SDK_ARTIFACT_DIR/packages/harness"
    generated_status=0
    bun run check:baselines || generated_status=$?
    finish harness-package
    if ((generated_status != 0)); then exit "$generated_status"; fi
    background harness-conformance bun scripts/run-harness-conformance.mjs \
      "$SDK_ARTIFACT_DIR/packages/harness" \
      "$SDK_ARTIFACT_DIR/packages/harness/runner-report.json" \
      "$SDK_ARTIFACT_DIR/packages/harness/qualification-receipt.json"
    filesystem_status=0
    bash scripts/check-filesystem-package.sh "$SDK_ARTIFACT_DIR/packages/filesystem" || filesystem_status=$?
    finish harness-conformance
    if ((filesystem_status != 0)); then exit "$filesystem_status"; fi
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
    else
      target=aarch64-unknown-linux-musl
    fi
    sudo apt-get update -qq
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y musl-tools
    rustup target add "$target"
    export CC_aarch64_unknown_linux_musl=musl-gcc
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc
    # Run capture admission and publication in the shipped libc ABI. Path
    # statx and held-descriptor fstat can expose different optional metadata.
    cargo test -p acyclic-fs --features native-mount --locked \
      --target "$target" --lib native_capture::
    ;;
  policy)
    node scripts/static-ci.mjs
    node scripts/check-compatibility-digests.mjs
    bash scripts/test-qualify-ci-preflight.sh
    node --test scripts/wasm-size-report.test.mjs
    bash scripts/test-ensure-rust-target.sh
    node scripts/check-workflow-runners.mjs
    node --test scripts/harness-provider-evidence.test.mjs
    node --test scripts/check-harness-package.test.mjs scripts/check-machines-package.test.mjs scripts/ci-background-log.test.mjs
    node --test plugin/tests/hosts/login-link.test.mjs
    install_tool actionlint_1.7.7_linux_amd64.tar.gz \
      https://github.com/rhysd/actionlint/releases/download/v1.7.7/actionlint_1.7.7_linux_amd64.tar.gz \
      023070a287cd8cccd71515fedc843f1985bf96c436b7effaecce67290e7e0757 \
      "$TOOLS_DIR/actionlint-1.7.7" actionlint
    node --test scripts/check-workflows.test.mjs
    bash scripts/check-workflows.sh "$TOOLS_DIR/actionlint-1.7.7/actionlint"
    node scripts/publish-cargo-crates.mjs check
    node --test scripts/test-publish-cargo-crates.mjs
    node --test scripts/test-publish-npm-packages.mjs
    node --test scripts/static-policy.test.mjs scripts/static-ci.test.mjs scripts/check-boundaries.test.mjs
    node --test scripts/tiny-ci.test.mjs scripts/cargo-lock-policy.test.mjs scripts/publish-release-assets.test.mjs scripts/release-qualification-artifacts.test.mjs scripts/release-source.test.mjs
    node --test scripts/native-cache-evidence.test.mjs scripts/cache-save-decision.test.mjs scripts/cache-limits.test.mjs scripts/cargo-timings-summary.test.mjs scripts/bench-client-requests.test.mjs
    node --test scripts/test-typescript-qualification.mjs
    node --test scripts/test-plan-qualification.mjs scripts/test-plan-qualification-gate.mjs scripts/test-plan-qualification-windows-rust.mjs scripts/build-wasm-groups.test.mjs scripts/test-qualify-linux-package-overlap.mjs scripts/qualify-mcp-loom.test.mjs scripts/qualify-ci-preflight-fixtures.test.mjs
    node --test scripts/test-contract-artifacts.mjs scripts/test-contract-targets.mjs
    node scripts/test-verify-release-binary.mjs
    source scripts/ensure-bun.sh
    bun test scripts/qualify-rustfmt.test.mjs
    bash scripts/qualify-rustfmt.sh
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
    # Production-only reduced sets must not inherit test/dev features. Keep
    # these with the lane feature-set checks, outside the tiny SDK Static job.
    # Focused bb1 gate: Q:/lanes/bs.ps1 bash scripts/check-storage-feature-sets.sh
    bash scripts/check-storage-feature-sets.sh
    node scripts/clippy-feature-sets.mjs 2
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
    install_tool cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz \
      https://github.com/EmbarkStudios/cargo-deny/releases/download/0.19.0/cargo-deny-0.19.0-x86_64-unknown-linux-musl.tar.gz \
      0e8c2aa59128612c90d9e09c02204e912f29a5b8d9a64671b94608cbe09e064f \
      "$TOOLS_DIR/cargo-deny-0.19.0" cargo-deny-0.19.0-x86_64-unknown-linux-musl/cargo-deny 1
    "$TOOLS_DIR/cargo-deny-0.19.0/cargo-deny" check
    # Exactly one rustls crypto provider (ring): a second compiled-in provider
    # makes provider-less rustls builders panic. Cargo.lock resolves every
    # feature of every member (and the Static job keeps it --locked), so it
    # sees optional features that cargo-deny's default graph does not.
    if grep -Eq '^name = "aws-lc-(rs|sys)"$' Cargo.lock; then
      echo "Cargo.lock contains aws-lc-rs/aws-lc-sys; build rustls with the ring provider only" >&2
      exit 1
    fi
    ;;
  wasm-tests)
    # Compile target-specific test code without a browser or native dev-deps.
    for features in browser,test-support wasm,test-support; do
      cargo check --locked -p acyclic-harness -p acyclic-harness-core --no-default-features \
        --features "$features" --target wasm32-unknown-unknown --lib --tests
    done
    ;;
  web-warnings)
    # The web lane's compile-only closures, runnable without browsers.
    web_warning_closures
    ;;
  web)
    bash scripts/ensure-rust-target.sh wasm32-unknown-unknown
    python3 scripts/test-provider-catalog-targets.py
    # The host clippy run cannot see code that only the browser build
    # compiles or omits, so lint each browser build as it ships.
    cargo clippy -p acyclic-fs-wasm --target wasm32-unknown-unknown \
      --all-targets --all-features --locked -- -D warnings
    cargo clippy -p acyclic-harness -p acyclic-harness-core --features wasm \
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
    web_warning_closures
    for suite in filesystem::default_tools::tests:: memory_store::tests::; do
      CHROMEDRIVER="$(command -v chromedriver)" \
        RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
        harness_tests -p acyclic-harness-core --features browser,test-support --target wasm32-unknown-unknown --lib "$suite"
    done
    CHROMEDRIVER="$(command -v chromedriver)" \
      RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
      harness_tests -p acyclic-harness --features browser,test-support --target wasm32-unknown-unknown \
        --test main client_public_consumer::identical_public_harness_trace
    harness_tests -p acyclic-harness --features filesystem,test-support --test main client_public_consumer::identical_public_harness_trace
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
      harness_tests -p acyclic-harness --features browser,test-support --target wasm32-unknown-unknown --test main client_views_parity::
    RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
    CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="node $PWD/scripts/run-node-wasm-tests.mjs" \
      cargo test -p acyclic-harness-providers --features browser,acyclic-harness/test-support \
      --target wasm32-unknown-unknown --lib --locked
    CHROMEDRIVER="$(command -v chromedriver)" \
      cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    CHROMEDRIVER="$(command -v chromedriver)" \
      RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }-Dwarnings" \
      harness_tests -p acyclic-harness --features browser,test-support --target wasm32-unknown-unknown \
        --test main native_media_boundary::
    GECKODRIVER="$(command -v geckodriver)" \
      cargo test -p acyclic-fs-wasm --target wasm32-unknown-unknown --locked
    # The shipped browser package end to end in headless Chrome: one tab, then
    # concurrent tabs sharing one database.
    source scripts/ensure-bun.sh
    wasm_bindgen_bin="$(bash scripts/ensure-wasm-bindgen.sh)"
    export PATH="$(dirname "$wasm_bindgen_bin"):$PATH"
    # Package builds resolve the whole locked Cargo graph offline (family
    # admission runs `cargo metadata`), including crates that only other
    # targets compile, which the wasm32 builds above never download.
    cargo fetch --locked
    bun install --frozen-lockfile
    bun run --filter '@acyclic-labs/fs' build
    # The combined browser pages consume the public Stream and Harness builds;
    # Harness also needs its Objects package declaration dependency.
    bun run --filter '@acyclic-labs/objects' build
    bun run --filter '@acyclic-labs/stream' build
    bun run --filter '@acyclic-labs/harness' build
    export CHROME="$(command -v google-chrome || command -v chromium)"
    browser_qualification() {
      local name="$1" start="$EPOCHREALTIME" status=0
      shift
      "$@" || status=$?
      printf '%s\t%s\t%s\t%s\0' "$start" "$EPOCHREALTIME" "$status" "$*" >"$observability/browser-$name.tsv"
      return "$status"
    }
    browser_pids=()
    # Each driver owns its server and profile; overlap their browser cleanup.
    browser_qualification fs bun run --filter '@acyclic-labs/fs' test:browser & browser_pids+=("$!")
    browser_qualification compaction bun typescript/packages/filesystem/test/browser-qualify.mjs ../harness/test/browser-compaction.html & browser_pids+=("$!")
    browser_qualification mcp bun run --filter '@acyclic-labs/harness' test:browser:mcp & browser_pids+=("$!")
    browser_qualification client bun run --filter '@acyclic-labs/harness' test:client:browser & browser_pids+=("$!")
    browser_status=0
    for browser_pid in "${browser_pids[@]}"; do
      if wait "$browser_pid"; then :; else browser_status=1; fi
    done
    if (( browser_status != 0 )); then exit "$browser_status"; fi
    ;;
  typescript)
    # Family admission resolves the locked Cargo graph offline before WASM
    # compilation, so a cold runner needs the dependency index and crates first.
    cargo fetch --locked
    # Pull requests and main pushes only; full runs cover this in the linux
    # lane. Build outputs are regenerated before checking compatibility baselines.
    source scripts/ensure-bun.sh
    wasm_bindgen_bin="$(bash scripts/ensure-wasm-bindgen.sh)"
    export PATH="$(dirname "$wasm_bindgen_bin"):$PATH"
    bun install --frozen-lockfile
    client_bindings
    bun run test
    bun run check:baselines
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
    nextest --workspace --all-features --locked
    cargo test --workspace --all-features --locked --doc
    native_mount_tests
    installed_graphcoder
    pinned_pi
    ;;
  linux-arm64-bindings)
    if ! command -v cc >/dev/null || ! command -v unzip >/dev/null || \
      ! command -v pkg-config >/dev/null || ! pkg-config --exists fuse3; then
      sudo apt-get update -qq
      sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        --no-install-recommends build-essential pkg-config libfuse3-dev unzip
    fi
    source scripts/ensure-bun.sh
    cargo fetch --locked
    native_binding
    ;;

  macos)
    bash scripts/test-ensure-rust-target.sh
    source scripts/ensure-bun.sh
    bun install --frozen-lockfile
    bash scripts/ensure-rust-target.sh x86_64-apple-darwin
    cargo fetch --locked
    finish_macos() {
      local status="${1:-0}" background_status=0
      trap - EXIT
      finish napi x86_64 fork-join-build || background_status=$?
      ((status != 0)) || status=$background_status
      return "$status"
    }
    CARGO_BUILD_JOBS=2 background napi native_binding
    CARGO_BUILD_JOBS=1 background x86_64 cargo check -p acyclic-fs -p acyclic-fs-napi --all-features \
      --target x86_64-apple-darwin --locked --target-dir "$target_dir-x86_64"
    CARGO_BUILD_JOBS=1 background fork-join-build cargo test -p acyclic-plugin --locked --test fork_join \
      --no-run --target-dir "$target_dir-fork-join"
    trap 'status=$?; finish_macos "$status" || status=$?; exit "$status"' EXIT
    CARGO_BUILD_JOBS=8 nextest --workspace --all-features --locked
    CARGO_BUILD_JOBS=8 cargo test --workspace --all-features --locked --doc
    finish_macos
    installed_graphcoder
    pinned_pi
    stream_binding
    client_bindings
    native_mount_tests
    CARGO_TARGET_DIR="$target_dir-fork-join" fork_join_conformance
    ;;
  *)
    echo "unknown qualification lane: $lane" >&2
    exit 2
    ;;
esac
