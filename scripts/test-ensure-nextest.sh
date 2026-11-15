#!/usr/bin/env bash
set -euo pipefail

# Hermetic checks of scripts/ensure-nextest.sh: which installed releases it
# accepts, and that bytes failing the pinned SHA-256 never reach the tool
# cache. The policy lane installs the real release separately.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
subject="$root/scripts/ensure-nextest.sh"
case "$(uname -s)" in
  Linux|Darwin) ;;
  *) echo "test-ensure-nextest.sh runs on Linux or macOS, not $(uname -s)" >&2; exit 1 ;;
esac
work="$(mktemp -d "${TMPDIR:-/tmp}/acyclic-nextest-test.XXXXXXXX")"
trap 'rm -rf -- "$work"' EXIT
fake_bin="$work/fake-bin"
mkdir -p "$fake_bin"

# Network access is a defect here: the fetcher is configured and terminal.
cat > "$fake_bin/curl" <<'EOF'
#!/usr/bin/env bash
echo curl >> "$FAKE_STATE/curl-calls"
exit 1
EOF
cat > "$work/fetcher" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ $# == 3 && "$1" =~ ^[0-9a-f]{64}$ && "$2" == https://github.com/*/download/cargo-nextest-0.9.146/cargo-nextest-0.9.146-*.tar.gz ]]
echo "$2" >> "$FAKE_STATE/fetches"
[[ "$FAKE_FETCH" == corrupt ]]
printf 'not the pinned archive' > "$3"
EOF
chmod +x "$fake_bin/curl" "$work/fetcher"

prepare_case() {
  FAKE_STATE="$work/$1"
  tools="$FAKE_STATE/tools"
  mkdir -p "$tools/cargo/bin"
  FAKE_FETCH=corrupt
}
installed_release() {
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" %q\n' "$1" > "$tools/cargo/bin/cargo-nextest"
  chmod +x "$tools/cargo/bin/cargo-nextest"
}
invoke() {
  FAKE_STATE="$FAKE_STATE" FAKE_FETCH="$FAKE_FETCH" TOOLS_DIR="$tools" \
    PINNED_ASSET_FETCHER="$work/fetcher" PATH="$fake_bin:$PATH" \
    bash "$subject" 2> "$FAKE_STATE/stderr"
}
expect_refusal() {
  if invoke > "$FAKE_STATE/stdout"; then
    echo "expected ensure-nextest.sh to refuse: $1" >&2
    exit 1
  fi
  grep -q 'cargo-nextest archive refused' "$FAKE_STATE/stderr"
  [[ "$(wc -l < "$FAKE_STATE/fetches" | tr -d ' ')" == 1 ]]
  [[ ! -e "$FAKE_STATE/curl-calls" ]]
  # No temporary download stays in the cache.
  [[ -z "$(find "$tools" -maxdepth 1 -name '*.tar.gz.*' -print)" ]]
}
no_archive() {
  [[ -z "$(find "$tools" -maxdepth 1 -name 'cargo-nextest-*' -print)" ]]
}

case "$(uname -s):$(uname -m)" in
  Darwin:*) target=universal-apple-darwin ;;
  Linux:aarch64|Linux:arm64) target=aarch64-unknown-linux-gnu ;;
  *) target=x86_64-unknown-linux-gnu ;;
esac

accepted_case=0
for accepted in $'cargo-nextest 0.9.146 (8af696ddc 2026-09-21)\nrelease: 0.9.146\nhost: x86_64-unknown-linux-gnu' \
  'cargo-nextest 0.9.146' $'cargo-nextest 0.9.146\r'; do
  accepted_case=$((accepted_case + 1))
  prepare_case "accepted-$accepted_case"
  installed_release "$accepted"
  [[ "$(invoke)" == "$tools/cargo/bin" ]]
  [[ ! -e "$FAKE_STATE/fetches" && ! -e "$FAKE_STATE/curl-calls" ]]
done

for wrong in 0.9.145 0.9.1461 0.9.146-dev; do
  prepare_case "wrong-$wrong"
  installed_release "cargo-nextest $wrong (fixture)"
  expect_refusal "$wrong installed and a corrupt download"
  no_archive
  [[ "$("$tools/cargo/bin/cargo-nextest")" == "cargo-nextest $wrong (fixture)" ]]
done

prepare_case corrupt-cached-archive
printf 'truncated' > "$tools/cargo-nextest-0.9.146-$target.tar.gz"
expect_refusal 'corrupt cached archive'
[[ "$(cat "$tools/cargo-nextest-0.9.146-$target.tar.gz")" == truncated && ! -e "$tools/cargo/bin/cargo-nextest" ]]

prepare_case fetcher-failure
FAKE_FETCH=fail
expect_refusal 'fetcher failure without a network fallback'
no_archive

echo 'ensure-nextest tests passed'
