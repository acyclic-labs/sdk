#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/acyclic-qualify-ci-test.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/bin"
# Keep the caller outside the checkout: fixture planning and execution must share
# their own repository scope, including relative CARGO_TARGET_DIR resolution.
cd "$work"

# Exercise the actual verifier with a digest-only sha256sum interface,
# including rejected archive bytes and failed reads.
mkdir -p "$work/portable-sha/bin"
printf 'verified archive\n' >"$work/portable-sha/archive"
printf 'corrupted archive\n' >"$work/portable-sha/corrupt"
export TEST_REAL_SHA256
TEST_REAL_SHA256="$(command -v sha256sum)"
archive_checksum="$(sha256sum "$work/portable-sha/archive" | cut -d ' ' -f 1)"
cat >"$work/portable-sha/bin/sha256sum" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ "$#" -eq 1 && "$1" != -* ]] || exit 2
exec "$TEST_REAL_SHA256" "$@"
EOF
chmod +x "$work/portable-sha/bin/sha256sum"
(
  export PATH="$work/portable-sha/bin:$PATH"
  eval "$(sed -n '/^sha256_matches() {/,/^}/p' "$root/scripts/qualify-ci.sh")"
  sha256_matches "$archive_checksum" "$work/portable-sha/archive"
  if sha256_matches "$archive_checksum" "$work/portable-sha/corrupt"; then exit 1; fi
  if sha256_matches "$archive_checksum" "$work/portable-sha/missing"; then exit 1; fi
)

cat >"$work/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_GIT_LOG"
case "$*" in
  *'rev-parse HEAD'*) printf '%s\n' "${FAKE_HEAD:-release-head}" ;;
  *'rev-list --reverse'*)
    case "${FAKE_GIT_MODE:-valid}" in
      revlist-fail) printf '%s\n' release-commit; exit 1 ;;
      revlist-empty) ;;
      *) printf '%s\n' release-commit ;;
    esac ;;
  *'%G? %GF'*) printf '%s\n' 'G 968479A1AFF927E37D1A566BB5690EEEBB952194' ;;
  *'%G?'*)
    [[ "${FAKE_GIT_MODE:-valid}" == ssh-good ]] && printf 'G\n' || printf 'N\n'
    ;;
  *) ;;
esac
EOF

cat >"$work/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_CURL_LOG"
output="${!#}"
printf 'offline web-flow fixture\n' >"$output"
EOF

cat >"$work/bin/sha256sum" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_SHA256_LOG"
if [[ "$*" != '--check --status' ]]; then
  # Signature fixtures use a stub archive, not downloaded tool bytes.
  printf '%s  %s\n' '551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb' "$1"
else
  # The real check consumes stdin; exiting early races the checksum producer.
  cat >/dev/null
fi
EOF

cat >"$work/bin/gpg" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_GPG_LOG"
EOF

cat >"$work/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
verb="$1"
if [[ " $* " == *' --message-format=json '* ]]; then
  exec "$FAKE_MODEL_FIXTURE_DIR/cargo" "$@"
else
  source "$FAKE_OPERATION_DIR/operation.sh"
  operation "$1"
  if [[ "${FAKE_LANE:-}" == web-warnings || "${FAKE_LANE:-}" == wasm-tests ]]; then
    features="" target="" kind=lib nodefault=false
    while (($#)); do
      case "$1" in
        --features) features="$2"; shift ;;
        --target) target="$2"; shift ;;
        --tests) kind=tests ;;
        --no-default-features) nodefault=true ;;
      esac
      shift
    done
    [[ "$target" == wasm32-unknown-unknown && "$nodefault" == true ]] || exit 65
    if [[ "$FAKE_LANE" == web-warnings ]]; then
      [[ " ${RUSTFLAGS:-} " == *-Dwarnings* ]] || exit 65
    fi
    printf '%s|%s\n' "$features" "$kind" >>"$FAKE_OPERATION_DIR/closures"
  fi
fi
if [[ "$verb" == test ]]; then
  case "${FAKE_MODEL_RESULTS:-complete}" in
    missing) ;;
    mixed) printf 'test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n' ;;
    *) printf 'test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n' ;;
  esac
fi
EOF

chmod +x "$work/bin"/*

make_case() {
  local name="$1"
  local case_dir="$work/$name"
  mkdir -p "$case_dir/temp" "$case_dir/artifacts" "$case_dir/tools" "$case_dir/gitleaks-archive"
  : >"$case_dir/git.log"
  : >"$case_dir/curl.log"
  : >"$case_dir/sha256.log"
  : >"$case_dir/gpg.log"
  : >"$case_dir/gitleaks.log"
  cat >"$case_dir/gitleaks-archive/gitleaks" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_GITLEAKS_LOG"
case "${FAKE_GIT_MODE:-valid}" in
  scan-fatal) echo '12:00AM INF 1 commits scanned.'; echo '12:00AM ERR fatal: not a git repository' ;;
  scan-zero) echo '12:00AM INF 0 commits scanned.' ;;
  scan-empty) echo '12:00AM INF 1 commits scanned.'; echo '12:00AM INF scanned ~0 bytes (0) in 1s'; echo '12:00AM INF no leaks found'; exit 0 ;;
  scan-silent) exit 0 ;;
  scan-fail) exit 1 ;;
  *) echo '12:00AM INF 1 commits scanned.' ;;
esac
echo '12:00AM INF scanned ~100 bytes (100) in 1s'
echo '12:00AM INF no leaks found'
EOF
  chmod +x "$case_dir/gitleaks-archive/gitleaks"
  tar --create --gzip --file "$case_dir/tools/gitleaks_8.30.1_linux_x64.tar.gz" \
    --directory "$case_dir/gitleaks-archive" gitleaks
}

run_preflight() {
  local name="$1" mode="$2" expected="$3"
  local case_dir="$work/$name"
  make_case "$name"
  set +e
  (
    cd "$root"
    SDK_TEMP_DIR="$case_dir/temp" \
      SDK_ARTIFACT_DIR="$case_dir/artifacts" \
      TOOLS_DIR="$case_dir/tools" \
      GITHUB_REF="${4:-refs/heads/main}" \
      FAKE_GIT_MODE="$mode" \
      FAKE_GIT_LOG="$case_dir/git.log" \
      FAKE_CURL_LOG="$case_dir/curl.log" \
      FAKE_SHA256_LOG="$case_dir/sha256.log" \
      FAKE_GPG_LOG="$case_dir/gpg.log" \
      FAKE_GITLEAKS_LOG="$case_dir/gitleaks.log" \
      PATH="$work/bin:$PATH" \
      bash scripts/qualify-ci.sh preflight
  ) >"$case_dir/stdout" 2>"$case_dir/stderr"
  local status=$?
  set -e
  if [[ "$status" -ne "$expected" ]]; then
    cat "$case_dir/stdout" "$case_dir/stderr" >&2
    echo "$name returned $status; expected $expected" >&2
    exit 1
  fi
  if [[ "$expected" -ne 0 && "$mode" != scan-* ]]; then
    [[ ! -s "$case_dir/gitleaks.log" ]]
    [[ ! -s "$case_dir/curl.log" ]]
  fi
}

run_preflight valid webflow 0
grep -q 'web-flow.gpg' "$work/valid/curl.log"
grep -q 'gpg.format=openpgp' "$work/valid/git.log"
grep -q 'detect --source .' "$work/valid/gitleaks.log"
grep -q -- '--log-opts --diff-merges=first-parent release-head\^..release-head' "$work/valid/gitleaks.log"

run_preflight revlist-fail revlist-fail 1
run_preflight revlist-empty revlist-empty 1
for mode in scan-fatal scan-zero scan-empty scan-silent scan-fail; do
  run_preflight "$mode" "$mode" 1
  [[ -s "$work/$mode/gitleaks.log" ]]
done
run_preflight ssh-good ssh-good 0
run_preflight branch-webflow webflow 1 refs/heads/feature

gate="$work/gate"
mkdir -p "$gate/temp" "$gate/artifacts" "$gate/tools/cargo/bin" "$gate/bin"
# The pinned nextest is already installed, so the gate must not download it.
cat >"$gate/bin/cargo-nextest" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' 'cargo-nextest 0.9.146 (fake)'
EOF
chmod +x "$gate/bin/cargo-nextest"
export FAKE_MODEL_FIXTURE="$root/scripts/qualify-ci-preflight-fixtures.mjs"
export FAKE_MODEL_FIXTURE_DIR="$gate"
export FAKE_OPERATION_DIR="$gate"
# Exercise default and checkout-relative target paths from an outside caller.
unset CARGO_TARGET_DIR
offline_lane() (
  cd "$root"
  node "$FAKE_MODEL_FIXTURE" prepare "$gate"
  : >"$gate/closures"
  SDK_TEMP_DIR="$gate/temp" SDK_ARTIFACT_DIR="$gate/artifacts" TOOLS_DIR="$gate/tools" \
    FAKE_LANE="$1" PATH="$gate/bin:$work/bin:$PATH" \
    bash scripts/qualify-ci.sh "$1"
)
model_gate() {
  offline_lane gate >"$gate/model-gate.stdout" 2>&1 || return 1
  node "$FAKE_MODEL_FIXTURE" verify "$gate" 2>"$gate/model-gate.stderr"
}
if ! model_gate; then
  cat "$gate/model-gate.stdout" "$gate/model-gate.stderr" >&2
  exit 1
fi
if ! CARGO_TARGET_DIR=relative-models model_gate; then
  cat "$gate/model-gate.stdout" "$gate/model-gate.stderr" >&2
  exit 1
fi

# Fault every kind of native boundary observed in actual offline execution.
# No expected command list or execution order is mirrored by this fixture.
for lane in gate web-warnings wasm-tests docs generation; do
  offline_lane "$lane" >"$gate/$lane.stdout" 2>&1
  mapfile -t roles < <(sort -u "$gate/operations")
  ((${#roles[@]})) || { echo "$lane executed no native operations" >&2; exit 1; }
  for role in "${roles[@]}"; do
    if FAKE_FAIL_ROLE="$role" offline_lane "$lane" >"$gate/fault.stdout" 2>&1; then
      echo "$lane accepted native failure in $role" >&2
      exit 1
    fi
    [[ -s "$gate/failed" && ! -s "$gate/downstream" ]] || {
      cat "$gate/fault.stdout" >&2
      echo "$lane continued beyond native failure in $role" >&2
      exit 1
    }
  done
done

# Missing evidence or an empty suite cannot qualify alongside a valid suite.
for mode in missing mixed; do
  if FAKE_MODEL_RESULTS="$mode" offline_lane gate >"$gate/$mode.stdout" 2>&1; then
    echo "model gate accepted $mode results" >&2
    exit 1
  fi
done

# These are required compilation capabilities, not Cargo command snapshots.
# Feature closure and warning admission are checked by the offline compiler
# boundary above; neither argument order nor shell source spelling is asserted.
for lane in web-warnings wasm-tests; do
  offline_lane "$lane" >"$gate/$lane.stdout" 2>&1
  mapfile -t closures < <(sort -u "$gate/closures")
  required=('browser,test-support|tests' 'wasm,test-support|tests')
  if [[ "$lane" == web-warnings ]]; then
    required+=('browser|tests' 'wasm|tests' '|lib')
  fi
  for closure in "${required[@]}"; do
    printf '%s\n' "${closures[@]}" | grep -Fxq -- "$closure" || {
      echo "$lane omitted required compilation capability $closure" >&2
      exit 1
    }
  done
done
echo 'qualification preflight tests passed'
