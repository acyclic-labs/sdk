#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/acyclic-qualify-ci-test.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/bin"

cat >"$work/bin/jq" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *'.release.tag_name'* ]]; then
  printf '%s\n' "${FAKE_RELEASE_TAG:-v1.2.3}"
else
  exit 2
fi
EOF

cat >"$work/bin/git" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_GIT_LOG"
case "$*" in
  *'rev-parse HEAD'*) printf '%s\n' "${FAKE_HEAD:-release-head}" ;;
  *'rev-parse refs/tags/'*) printf '%s\n' "${FAKE_TAG_HEAD:-release-head}" ;;
  *'merge-base --is-ancestor'*) [[ "${FAKE_GIT_MODE:-valid}" != nonmain ]] ;;
  *'rev-list --reverse'*) printf '%s\n' release-commit ;;
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
EOF

chmod +x "$work/bin"/*

make_case() {
  local name="$1"
  local case_dir="$work/$name"
  mkdir -p "$case_dir/temp" "$case_dir/artifacts" "$case_dir/tools/gitleaks-8.30.1"
  : >"$case_dir/git.log"
  : >"$case_dir/curl.log"
  : >"$case_dir/sha256.log"
  : >"$case_dir/gpg.log"
  : >"$case_dir/gitleaks.log"
  cat >"$case_dir/event.json" <<'EOF'
{"release":{"tag_name":"v1.2.3"}}
EOF
  cat >"$case_dir/tools/gitleaks-8.30.1/gitleaks" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_GITLEAKS_LOG"
EOF
  chmod +x "$case_dir/tools/gitleaks-8.30.1/gitleaks"
}

run_release() {
  local name="$1" mode="$2" expected="$3"
  local case_dir="$work/$name"
  make_case "$name"
  set +e
  (
    cd "$root"
    SDK_TEMP_DIR="$case_dir/temp" \
      SDK_ARTIFACT_DIR="$case_dir/artifacts" \
      TOOLS_DIR="$case_dir/tools" \
      GITHUB_EVENT_NAME=release \
      GITHUB_REF=refs/tags/v1.2.3 \
      GITHUB_EVENT_PATH="$case_dir/event.json" \
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
  if [[ "$expected" -ne 0 ]]; then
    [[ ! -s "$case_dir/gitleaks.log" ]]
    [[ ! -s "$case_dir/curl.log" ]]
  fi
}

run_release valid webflow 0
grep -q 'web-flow.gpg' "$work/valid/curl.log"
grep -q 'gpg.format=openpgp' "$work/valid/git.log"
grep -q 'detect --source .' "$work/valid/gitleaks.log"

make_case wrong-tag
cat >"$work/wrong-tag/event.json" <<'EOF'
{"release":{"tag_name":"wrong-tag"}}
EOF
set +e
(
  cd "$root"
  SDK_TEMP_DIR="$work/wrong-tag/temp" SDK_ARTIFACT_DIR="$work/wrong-tag/artifacts" \
    TOOLS_DIR="$work/wrong-tag/tools" GITHUB_EVENT_NAME=release \
    GITHUB_REF=refs/tags/v1.2.3 GITHUB_EVENT_PATH="$work/wrong-tag/event.json" \
    FAKE_RELEASE_TAG=wrong-tag \
    FAKE_GIT_MODE=ssh-good FAKE_GIT_LOG="$work/wrong-tag/git.log" \
    FAKE_CURL_LOG="$work/wrong-tag/curl.log" FAKE_SHA256_LOG="$work/wrong-tag/sha256.log" \
    FAKE_GPG_LOG="$work/wrong-tag/gpg.log" FAKE_GITLEAKS_LOG="$work/wrong-tag/gitleaks.log" \
    PATH="$work/bin:$PATH" bash scripts/qualify-ci.sh preflight
)
status=$?
set -e
[[ "$status" -ne 0 ]]
[[ ! -s "$work/wrong-tag/gitleaks.log" ]]
[[ ! -s "$work/wrong-tag/curl.log" ]]

make_case wrong-head
set +e
(
  cd "$root"
  SDK_TEMP_DIR="$work/wrong-head/temp" SDK_ARTIFACT_DIR="$work/wrong-head/artifacts" \
    TOOLS_DIR="$work/wrong-head/tools" GITHUB_EVENT_NAME=release \
    GITHUB_REF=refs/tags/v1.2.3 GITHUB_EVENT_PATH="$work/wrong-head/event.json" \
    FAKE_TAG_HEAD=other-head FAKE_GIT_MODE=ssh-good FAKE_GIT_LOG="$work/wrong-head/git.log" \
    FAKE_CURL_LOG="$work/wrong-head/curl.log" FAKE_SHA256_LOG="$work/wrong-head/sha256.log" \
    FAKE_GPG_LOG="$work/wrong-head/gpg.log" FAKE_GITLEAKS_LOG="$work/wrong-head/gitleaks.log" \
    PATH="$work/bin:$PATH" bash scripts/qualify-ci.sh preflight
)
status=$?
set -e
[[ "$status" -ne 0 ]]
[[ ! -s "$work/wrong-head/gitleaks.log" ]]
[[ ! -s "$work/wrong-head/curl.log" ]]

run_release nonmain nonmain 1

gate="$work/gate"
mkdir -p "$gate/temp" "$gate/artifacts" "$gate/tools/cargo/bin" "$gate/bin"
: >"$gate/cargo.log"
cat >"$gate/bin/rustup" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == 'component list --installed' ]]; then
  printf '%s\n' 'llvm-tools-x86_64-unknown-linux-gnu (installed)'
fi
EOF
chmod +x "$gate/bin/rustup"
(
  cd "$root"
  SDK_TEMP_DIR="$gate/temp" SDK_ARTIFACT_DIR="$gate/artifacts" TOOLS_DIR="$gate/tools" \
    FORCE=false GITHUB_EVENT_NAME=pull_request \
    FAKE_CARGO_LOG="$gate/cargo.log" PATH="$gate/bin:$work/bin:$PATH" \
    bash scripts/qualify-ci.sh gate
)
mapfile -t cargo_calls <"$gate/cargo.log"
[[ "${#cargo_calls[@]}" -eq 2 ]]
[[ "${cargo_calls[0]}" == 'test --manifest-path rust/crates/sdk-docs/Cargo.toml --locked' ]]
[[ "${cargo_calls[1]}" == 'test --workspace --locked --lib' ]]

full="$work/full"
mkdir -p "$full/temp" "$full/artifacts" "$full/tools/cargo/bin" "$full/bin"
: >"$full/cargo.log"
cat >"$full/bin/rustup" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == 'component list --installed' ]]; then
  printf '%s\n' 'llvm-tools-x86_64-unknown-linux-gnu (installed)'
fi
EOF
cat >"$full/tools/cargo/bin/cargo-llvm-cov" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == 'llvm-cov --version' ]]; then
  printf '%s\n' 'cargo-llvm-cov 0.9.1'
fi
EOF
chmod +x "$full/bin/rustup" "$full/tools/cargo/bin/cargo-llvm-cov"
(
  cd "$root"
  SDK_TEMP_DIR="$full/temp" SDK_ARTIFACT_DIR="$full/artifacts" TOOLS_DIR="$full/tools" \
    FORCE=true FAKE_CARGO_LOG="$full/cargo.log" PATH="$full/bin:$work/bin:$PATH" \
    bash scripts/qualify-ci.sh gate
)
mapfile -t full_cargo_calls <"$full/cargo.log"
[[ "${#full_cargo_calls[@]}" -eq 3 ]]
[[ "${full_cargo_calls[0]}" == 'test --manifest-path rust/crates/sdk-docs/Cargo.toml --locked' ]]
[[ "${full_cargo_calls[1]}" == 'llvm-cov --workspace --all-features --locked --fail-under-lines 70 --lcov --output-path '*'/coverage/lcov.info' ]]
[[ "${full_cargo_calls[2]}" == 'llvm-cov report --summary-only' ]]

echo 'qualification preflight tests passed'
