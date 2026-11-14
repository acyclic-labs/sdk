#!/usr/bin/env bash
# Run on a build worker after the source revision's shared compile succeeds.
set -euo pipefail

source_root=$(git rev-parse --show-toplevel)
source_sha=$(git rev-parse HEAD)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/actors-history.XXXXXX")
snapshots=()
cleanup() {
    for snapshot in "${snapshots[@]}"; do
        git -C "$source_root" worktree remove --force "$snapshot"
    done
    rmdir "$scratch" 2>/dev/null || true
}
trap cleanup EXIT

run_case() {
    local directory=$1 test=$2 expected=$3 revision=$4 log=$5
    local result=0
    (
        cd "$directory"
        cargo test --locked --all-features -p acyclic-actors --lib "$test" -- --nocapture
    ) >"$log" 2>&1 || result=$?
    cat "$log"
    if [[ "$expected" == pass ]]; then
        if [[ "$result" != 0 ]]; then return 1; fi
        rg -q 'test result: ok\. 1 passed; 0 failed' "$log"
    else
        # Compiler failures, infrastructure exits and zero-test filters prove
        # nothing. Require this specific seeded test to have executed and failed.
        if [[ "$result" == 0 ]]; then return 1; fi
        rg -q 'test result: FAILED\. 0 passed; 1 failed' "$log"
        rg -q 'seed=0' "$log"
    fi
    printf 'ACTORS_HISTORY revision=%s overlay=%s test=%s expected=%s exit=%s\n' \
        "$revision" "$source_sha" "$test" "$expected" "$result"
}

shard_test=seeded_children_share_one_activation_shard
custody_test=seeded_forks_preserve_custody_ownership
run_case "$source_root" "$shard_test" pass "$source_sha" "$scratch/shard-fixed.log"
run_case "$source_root" "$custody_test" pass "$source_sha" "$scratch/custody-fixed.log"

shard_before=88ea479eabc9f73daaebc248cfdac5026b2a1720
custody_before=d269a4731a4b932e31d7256640dccb6b8d8d065d
for before in "$shard_before" "$custody_before"; do
    snapshot="$scratch/$before"
    git -C "$source_root" worktree add --detach "$snapshot" "$before"
    snapshots+=("$snapshot")
done

runtime=rust/crates/actors/src/local/runtime
cp "$source_root/$runtime/tests/simulation/composition.rs" \
    "$scratch/$shard_before/$runtime/tests/historical_composition.rs"
printf '\nmod historical_composition;\n' >> "$scratch/$shard_before/$runtime/tests.rs"
run_case "$scratch/$shard_before" "$shard_test" fail "$shard_before" "$scratch/shard-before.log"
rg -q 'composition changed the actor publication' "$scratch/shard-before.log"

node --input-type=module - "$source_root" "$scratch/$custody_before" <<'JS'
import fs from 'node:fs';
import path from 'node:path';
const [source, before] = process.argv.slice(2);
const model = 'rust/crates/actors/src/local/model';
const composition = fs.readFileSync(path.join(source, model, 'composition.rs'), 'utf8');
const helper = composition.match(/pub\(super\) async fn copied_custody\([\s\S]*?\n}\n/);
if (!helper) throw Error('copied custody test helper missing');
fs.appendFileSync(path.join(before, model, 'composition.rs'), '\n' + helper[0]);
fs.copyFileSync(path.join(source, model, 'simulation/custody.rs'), path.join(before, model, 'historical_custody.rs'));
fs.appendFileSync(path.join(before, model + '.rs'), '\nmod historical_custody;\n');
JS
run_case "$scratch/$custody_before" "$custody_test" fail "$custody_before" "$scratch/custody-before.log"
printf 'ACTORS_HISTORY_COMPLETE source=%s cases=4\n' "$source_sha"
