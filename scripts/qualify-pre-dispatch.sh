#!/usr/bin/env bash
# Run on the build host; compile once and bound each independent proof to nine seconds.
set -euo pipefail
cargo test --locked -p acyclic-harness-core --lib --all-features --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py executor::tests::pre_dispatch:: filesystem::task_runtime::tests::model::
cargo test --locked -p acyclic-harness --lib --all-features --no-run --message-format=json |
  python3 scripts/qualify-graphcoder-artifact.py local::admission::tests::pre_dispatch:: local::tests::pre_dispatch::
cargo build --locked -p graphcoder --message-format=json-render-diagnostics > .bb-sessions-pre-dispatch.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-pre-dispatch.jsonl graphcoder > .bb-sessions-pre-dispatch.env
source .bb-sessions-pre-dispatch.env
binary="$SESSIONS_GRAPHCODER_BINARY"
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch prompt
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
if [ "${1:-}" != --negative ]; then exit 0; fi
proof=$(mktemp -d "$CARGO_TARGET_DIR/pre-dispatch-negative.XXXXXX")
model=rust/crates/harness-core/src/filesystem/task_commands/model.rs
worker=rust/crates/harness-core/src/filesystem/task_runtime/dispatch.rs
cp "$model" "$proof/model.rs"
cp "$worker" "$proof/dispatch.rs"
restore() { cp "$proof/model.rs" "$model"; cp "$proof/dispatch.rs" "$worker"; }
trap 'restore; rm -rf "$proof"' EXIT
python3 - <<'PY'
from pathlib import Path
p=Path('rust/crates/harness-core/src/filesystem/task_commands/model.rs')
s=p.read_text()
old='''ExecutionProgress::PreDispatchFailed(failure) => ready(
                Outcome::<TurnOutput>::Failed {
                    message: failure.into_message(),
                },
                io,
            ),'''
assert s.count(old)==1
s=s.replace(old,'ExecutionProgress::PreDispatchFailed(failure) => Err(Error::Invalid(failure.into_message())),',1)
old='Err(error) if !reconciliation_only && phase.can_fail(&error) => {'
assert s.count(old)==1
p.write_text(s.replace(old,'Err(error) if false && !reconciliation_only && phase.can_fail(&error) => {',1))
# Definitive worker settlement is an independent implementation of the same
# terminal behavior. Restore the old uncertain model-command outcome here too.
p=Path('rust/crates/harness-core/src/filesystem/task_runtime/dispatch.rs')
s=p.read_text();old='Err(error) if error.leaves_outcome_uncertain() => {'
assert s.count(old)==1
p.write_text(s.replace(old,'Err(error) if error.leaves_outcome_uncertain() || command.kind == super::super::MODEL_TASK_COMMAND_KIND => {',1))
PY
cargo build --locked -p graphcoder --message-format=json-render-diagnostics > .bb-sessions-pre-dispatch.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-pre-dispatch.jsonl graphcoder > .bb-sessions-pre-dispatch.env
source .bb-sessions-pre-dispatch.env
for mode in prompt jsonl; do
  if node scripts/qualify-final-answer.mjs "$SESSIONS_GRAPHCODER_BINARY" pre-dispatch "$mode" > "$proof/$mode.log" 2>&1; then
    cat "$proof/$mode.log"; echo 'negative control unexpectedly passed' >&2; exit 1
  fi
  cat "$proof/$mode.log"
  grep -Eq 'AssertionError|AbortError' "$proof/$mode.log"
  echo "NEGATIVE_PASS pre-dispatch $mode"
done
restore
trap 'rm -rf "$proof"' EXIT
cargo build --locked -p graphcoder --message-format=json-render-diagnostics > .bb-sessions-pre-dispatch.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-pre-dispatch.jsonl graphcoder > .bb-sessions-pre-dispatch.env
source .bb-sessions-pre-dispatch.env
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch prompt
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
