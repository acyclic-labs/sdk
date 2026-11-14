#!/usr/bin/env bash
# Run on a build worker after the source revision's shared compile succeeds.
# Each historical case checks out its exact pre-fix revision in place (the
# worker's one build tree and cache), overlays only the seeded test, and
# restores the source revision on exit.
set -euo pipefail

source_root=$(git rev-parse --show-toplevel)
source_sha=$(git rev-parse HEAD)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/actors-history.XXXXXX")
restore() {
    git -C "$source_root" checkout -q -f "$source_sha"
    git -C "$source_root" clean -q -f -- rust/crates/actors/src rust/crates/native-runtime/src
    rm -rf "$scratch"
}
trap restore EXIT
cd "$source_root"

run_case() {
    local test=$1 expected=$2 log=$3 features=${4:---all-features}
    local result=0 revision
    revision=$(git rev-parse HEAD)
    # Compile, then run the one test under the suite's 10 s per-test limit.
    (
        cargo test --locked $features -p acyclic-actors --lib --no-run 2>&1 | tee "$log.build"
        binary=$(grep -oE '\([^()]*/acyclic_actors-[0-9a-f]+\)' "$log.build" | tail -1 | tr -d '()')
        cd rust/crates/actors
        timeout --signal=KILL 10 "$binary" "$test" --nocapture
    ) >"$log" 2>&1 || result=$?
    cat "$log"
    if [[ "$expected" == pass ]]; then
        if [[ "$result" != 0 ]]; then return 1; fi
        grep -qE 'test result: ok\. 1 passed; 0 failed' "$log"
    else
        # Compiler failures, infrastructure exits and zero-test filters prove
        # nothing. Require this specific seeded test to have executed and failed.
        if [[ "$result" == 0 ]]; then return 1; fi
        grep -qE 'test result: FAILED\. 0 passed; 1 failed' "$log"
        grep -qE 'failed at seed [0-9]+; reproduce with DST_SEED=' "$log"
    fi
    printf 'ACTORS_HISTORY revision=%s overlay=%s test=%s expected=%s exit=%s\n' \
        "$revision" "$source_sha" "$test" "$expected" "$result"
}

shard_test=seeded_children_share_one_activation_shard
custody_test=seeded_forks_preserve_custody_ownership
run_case "$shard_test" pass "$scratch/shard-fixed.log"
run_case "$custody_test" pass "$scratch/custody-fixed.log"

# The overlay sources, taken from the source revision before any checkout.
testing=rust/crates/native-runtime/src/testing
runtime=rust/crates/actors/src/local/runtime
model=rust/crates/actors/src/local/model
mkdir "$scratch/overlay"
cp "$testing/sweep.rs" "$scratch/overlay/sweep.rs"
cp "$runtime/tests/simulation/composition.rs" "$scratch/overlay/composition.rs"
cp "$model/simulation/custody.rs" "$scratch/overlay/custody.rs"
cp "$model/composition.rs" "$scratch/overlay/model-composition.rs"

# The drivers report through the shared core's sweep, which postdates both.
before() {
    git checkout -q -f --detach "$1"
    cp "$scratch/overlay/sweep.rs" "$testing/sweep.rs"
    printf '\npub mod sweep;\n' >> "$testing.rs"
}

before 88ea479eabc9f73daaebc248cfdac5026b2a1720
cp "$scratch/overlay/composition.rs" "$runtime/tests/historical_composition.rs"
printf '\nmod historical_composition;\n' >> "$runtime/tests.rs"
run_case "$shard_test" fail "$scratch/shard-before.log"
grep -qE 'composition changed the actor publication' "$scratch/shard-before.log"

before d269a4731a4b932e31d7256640dccb6b8d8d065d
node --input-type=module - "$scratch/overlay/model-composition.rs" "$model" <<'JS'
import fs from 'node:fs';
const [composition, model] = process.argv.slice(2);
const helper = fs.readFileSync(composition, 'utf8').match(/pub\(super\) async fn copied_custody\([\s\S]*?\n}\n/);
if (!helper) throw Error('copied custody test helper missing');
fs.appendFileSync(`${model}/composition.rs`, '\n' + helper[0]);
JS
cp "$scratch/overlay/custody.rs" "$model/historical_custody.rs"
printf '\nmod historical_custody;\n' >> "$model.rs"
# This revision does not build its gRPC stream feature (unlinked `url`); the
# custody path needs only test-support.
run_case "$custody_test" fail "$scratch/custody-before.log" "--features test-support"
printf 'ACTORS_HISTORY_COMPLETE source=%s cases=4\n' "$source_sha"
