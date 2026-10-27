#!/usr/bin/env bash
set -eu
step() {
  local name=$1
  shift
  "$@" || {
    local status=$?
    printf 'STEP %s failed (exit %s); recovery: bash scripts/qualify-host-serving.sh\n' "$name" "$status" >&2
    exit "$status"
  }
}
step workspace cd "$(dirname "$0")/.."
# An installed browser may be mounted read-only with non-executable helpers.
# Copy only when the caller explicitly selects such a distribution.
if [ -n "${CHROME_BINARY_DIR:-}" ]; then
  chrome_dir=$(step browser-directory mktemp -d)
  trap 'rm -rf -- "$chrome_dir"' EXIT
  step browser-copy cp -a "$CHROME_BINARY_DIR/." "$chrome_dir"
  step browser-executables chmod u+x "$chrome_dir/chrome" "$chrome_dir/chrome_crashpad_handler" "$chrome_dir/chrome_sandbox"
  export CHROME="$chrome_dir/chrome"
  export WASM_BINDGEN_TEST_WEBDRIVER_JSON="$chrome_dir/webdriver.json"
  step browser-configuration python3 -c 'import json,os; json.dump({"goog:chromeOptions":{"binary":os.environ["CHROME"],"args":["--headless=new","--no-sandbox","--disable-dev-shm-usage"]}},open(os.environ["WASM_BINDGEN_TEST_WEBDRIVER_JSON"],"w"))'
fi
step agent-host bash scripts/qualify-agent-host.sh
step cloudflare bash scripts/qualify-cloudflare.sh
