#!/usr/bin/env bash
# On-demand browser execution for a caller-selected Cargo test command.
set -euo pipefail
step=arguments
temporary=
recovery() {
    printf 'step=%s failed (exit %s); recovery:' "$step" "$1" >&2
    printf ' %q' bash scripts/run-browser-tests.sh "${command[@]}" >&2
    printf '\n' >&2
}
cleanup() {
    local status=$?
    if (( status != 0 )); then recovery "$status"; fi
    if [[ -n "$temporary" ]]; then rm -rf -- "$temporary"; fi
    exit "$status"
}
command=("$@")
trap cleanup EXIT
if (( $# == 0 )); then
    echo 'Expected a test command, for example: cargo test --locked -p acyclic-fs --no-default-features --features browser,memory,conformance --target wasm32-unknown-unknown --test browser_commit public_conformance' >&2
    exit 2
fi
step=temporary-directory
temporary=$(mktemp -d)
step=browser-tools
bindgen=$(bash scripts/ensure-wasm-bindgen.sh)
bindgen=$(command -v "$bindgen")
export CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="$(dirname "$bindgen")/wasm-bindgen-test-runner"
step=browser-driver
driver=$(command -v "${CHROMEDRIVER:-chromedriver}")
cp "$driver" "$temporary/chromedriver"
chmod u+x "$temporary/chromedriver"
export CHROMEDRIVER="$temporary/chromedriver"
# Explicit browser distributions may have read-only/non-executable helpers.
# Retain an already-prepared browser selected by qualify-host-serving.sh.
if [[ -n "${CHROME_BINARY_DIR:-}" && -z "${CHROME:-}" ]]; then
    step=browser-distribution
    mkdir "$temporary/chrome"
    cp -a "$CHROME_BINARY_DIR/." "$temporary/chrome"
    for helper in chrome chrome_crashpad_handler chrome_sandbox; do
        if [[ -f "$temporary/chrome/$helper" ]]; then chmod u+x "$temporary/chrome/$helper"; fi
    done
    export CHROME="$temporary/chrome/chrome"
fi
step=browser-executable
if [[ -z "${CHROME:-}" ]]; then
    CHROME=$(command -v google-chrome || command -v chromium)
    export CHROME
fi
step=browser-configuration
export WASM_BINDGEN_USE_BROWSER=1
export WASM_BINDGEN_TEST_WEBDRIVER_JSON="$temporary/webdriver.json"
python3 -c 'import json,os; json.dump({"goog:chromeOptions":{"binary":os.environ["CHROME"],"args":["--headless=new","--no-sandbox","--disable-dev-shm-usage"]}},open(os.environ["WASM_BINDGEN_TEST_WEBDRIVER_JSON"],"w"))'
step=browser-tests
"${command[@]}"
