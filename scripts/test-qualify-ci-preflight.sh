#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/acyclic-qualify-ci-test.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/bin"

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
printf '%s\n' "$*" >>"$FAKE_CARGO_LOG"
printf '%s\n' "${RUSTFLAGS:-}" >>"${FAKE_RUSTFLAGS_LOG:-/dev/null}"
if [[ " $* " == *' --message-format=json '* ]]; then
  model=mcp-loom
  [[ " $* " != *' --test timer_loom '* ]] || model=timer_loom
  [[ " $* " != *' --test guest_loom '* ]] || model=guest_loom
  [[ " $* " != *' --test native_binding_model '* ]] || model=native_binding_model
  [[ " $* " != *' --test memory-accounting-model '* ]] || model=memory-accounting-model
  [[ " $* " != *' --lib '* ]] || model=acyclic_harness
  printf '{"reason":"compiler-artifact","target":{"name":"%s"},"profile":{"test":true},"executable":"%s"}\n' "$model" "$FAKE_MCP_LOOM_BINARY"
elif [[ "$1" == test ]]; then
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
: >"$gate/cargo.log"
# The pinned nextest is already installed, so the gate must not download it.
cat >"$gate/bin/cargo-nextest" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s
' 'cargo-nextest 0.9.146 (fake)'
EOF
chmod +x "$gate/bin/cargo-nextest"
cat >"$gate/bin/mcp_loom" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  distributed::*) printf 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out;\n' ;;
  local::compact::checkpoints::model::) printf 'test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out;\n' ;;
  *) printf 'test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n' ;;
esac
EOF
chmod +x "$gate/bin/mcp_loom"
(
  cd "$root"
  SDK_TEMP_DIR="$gate/temp" SDK_ARTIFACT_DIR="$gate/artifacts" TOOLS_DIR="$gate/tools" \
    FAKE_CARGO_LOG="$gate/cargo.log" FAKE_RUSTFLAGS_LOG="$gate/flags.log" FAKE_MCP_LOOM_BINARY="$gate/bin/mcp_loom" PATH="$gate/bin:$work/bin:$PATH" \
    bash scripts/qualify-ci.sh gate
)
mapfile -t cargo_calls <"$gate/cargo.log"
[[ "${#cargo_calls[@]}" -eq 14 ]]
[[ "${cargo_calls[0]}" == 'check --locked -p acyclic-harness-fuzz --bins' ]]
[[ "${cargo_calls[1]}" == 'check --locked -p acyclic-harness --features model-checking --lib' ]]
[[ "${cargo_calls[2]}" == 'check --locked -p acyclic-harness --features filesystem-local,model-checking --lib' ]]
[[ "${cargo_calls[3]}" == 'test -p acyclic-harness --locked --features model-checking --test client-mailbox-model --test client_journal_model --test mcp-loom' ]]
[[ "${cargo_calls[4]}" == 'test -p acyclic-harness --locked --features model-checking --lib workflow::model_checking::tests::' ]]
[[ "${cargo_calls[5]}" == 'rustc --locked --message-format=json -p acyclic-harness --test mcp-loom --all-features --features model-checking' ]]
for index in 6 7; do
  [[ "${cargo_calls[index]}" == 'test --locked --message-format=json -p acyclic-harness --lib --no-run --all-features --features model-checking' ]]
done
timer_target="$(cd "$root" && node -e 'console.log(require("node:path").resolve(process.env.CARGO_TARGET_DIR || "target", "timer-loom"))')"
[[ "${cargo_calls[8]}" == "rustc --locked --message-format=json -p acyclic-actors --test timer_loom --features acyclic-stream/local --target-dir $timer_target" ]]
[[ "${cargo_calls[9]}" == 'rustc --locked --message-format=json -p acyclic-actors --test guest_loom -- --cfg loom' ]]
[[ "${cargo_calls[10]}" == 'rustc --locked --message-format=json -p acyclic-fs --test native_binding_model --all-features -- --cfg acyclic_fs_loom' ]]
memory_target="$(cd "$root" && node -e 'console.log(require("node:path").resolve(process.env.CARGO_TARGET_DIR || "target", "memory-accounting-loom"))')"
[[ "${cargo_calls[11]}" == "rustc --locked --message-format=json -p acyclic-harness --test memory-accounting-model --features filesystem,acyclic-fs/model-checking --target-dir $memory_target -- --cfg acyclic_harness_loom" ]]
[[ "${cargo_calls[12]}" == 'nextest run --profile ci --workspace --all-features --locked' ]]
[[ "${cargo_calls[13]}" == 'test -p acyclic-harness --all-features --locked --example approved-native-process' ]]
mapfile -t flags <"$gate/flags.log"
for index in 0 1 2 3 4 11; do [[ "${flags[index]}" == *-Dwarnings* ]]; done
for index in 1 2 3 4; do
  [[ "${flags[index]}" == *'--cfg acyclic_harness_loom'* ]]
  [[ "${flags[index]}" == *'--cfg acyclic_loom'* ]]
done

# Independent controls for missing results and a zero-test suite beside a valid one.
for mode in missing mixed; do
  if (
    cd "$root"
    SDK_TEMP_DIR="$gate/temp" SDK_ARTIFACT_DIR="$gate/artifacts" TOOLS_DIR="$gate/tools" \
      FAKE_MODEL_RESULTS="$mode" FAKE_CARGO_LOG="$gate/cargo.log" \
      FAKE_MCP_LOOM_BINARY="$gate/bin/mcp_loom" PATH="$gate/bin:$work/bin:$PATH" \
      bash scripts/qualify-ci.sh gate
  ) >"$gate/$mode.stdout" 2>"$gate/$mode.stderr"; then
    echo "model gate accepted $mode results" >&2
    exit 1
  fi
done

for lane in docs generation; do
  : >"$gate/cargo.log"
  (
    cd "$root"
    SDK_TEMP_DIR="$gate/temp" SDK_ARTIFACT_DIR="$gate/artifacts" TOOLS_DIR="$gate/tools" \
      FAKE_CARGO_LOG="$gate/cargo.log" PATH="$gate/bin:$work/bin:$PATH" \
      bash scripts/qualify-ci.sh "$lane"
  )
  mapfile -t cargo_calls <"$gate/cargo.log"
  [[ "${#cargo_calls[@]}" -eq 1 ]]
  case "$lane" in
    docs) [[ "${cargo_calls[0]}" == 'test --manifest-path rust/crates/sdk-docs/Cargo.toml --locked' ]] ;;
    generation) [[ "${cargo_calls[0]}" == 'test --manifest-path rust/crates/sdk-generation/Cargo.toml --locked' ]] ;;
  esac
done
echo 'qualification preflight tests passed'
