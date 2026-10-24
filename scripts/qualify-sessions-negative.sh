#!/usr/bin/env bash
# Run on bb2: one combined mutant removes each independent shipping behavior.
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/sessions-negative.XXXXXX")
export SESSIONS_NEGATIVE_PROOF="$proof"
restore() {
 python3 - <<'PY'
import os
from pathlib import Path
root=Path(os.environ['SESSIONS_NEGATIVE_PROOF'])
for name in (root/'paths').read_text().splitlines():
 Path(name).write_bytes((root/name).read_bytes())
PY
}
trap 'restore; rm -rf "$proof"' EXIT
python3 - <<'PY'
from pathlib import Path
import os
root=Path(os.environ['SESSIONS_NEGATIVE_PROOF'])
paths=[
 'rust/crates/harness-providers/src/stream/mod.rs',
 'rust/crates/harness-core/src/filesystem/task_commands/model.rs',
 'rust/crates/harness/src/bin/graphcoder.rs',
 'rust/crates/harness/src/client/local_identity.rs',
 'rust/crates/harness/src/client/input_files.rs',
 'rust/crates/harness/src/local/commands.rs',
 'rust/crates/harness/src/bin/graphcoder/view.rs',
 'rust/crates/harness-core/src/filesystem/task_runtime/dispatch.rs',
 'rust/crates/harness-core/src/executor/tool_resolution.rs',
 'rust/crates/harness/src/tui/render/task.rs',
 'rust/crates/harness/src/tui/render.rs',
]
(root/'paths').write_text('\n'.join(paths))
for name in paths:
 saved=root/name;saved.parent.mkdir(parents=True,exist_ok=True);saved.write_bytes(Path(name).read_bytes())
def replace(name,old,new):
 p=Path(name);s=p.read_text();assert s.count(old)==1,(name,old,s.count(old));p.write_text(s.replace(old,new))
replace(paths[0],'calls.push(ModelEvent::MessageCompleted { text });','drop(text);')
replace(paths[1],'''ExecutionProgress::PreDispatchFailed(failure) => ready(
                Outcome::<TurnOutput>::Failed {
                    message: failure.into_message(),
                },
                io,
            ),''','ExecutionProgress::PreDispatchFailed(failure) => Err(Error::Invalid(failure.into_message())),')
replace(paths[1],'Err(error) if !reconciliation_only && phase.can_fail(&error) => {',
        'Err(error) if false && !reconciliation_only && phase.can_fail(&error) => {')
# Current main also settles definitive command errors. Remove that overlapping
# terminal path for model commands so this control restores the old uncertain
# outcome rather than silently relying on another implementation of the feature.
replace(paths[7],'Err(error) if error.leaves_outcome_uncertain() => {',
        'Err(error) if error.leaves_outcome_uncertain() || command.kind == super::super::MODEL_TASK_COMMAND_KIND => {')
replace(paths[2],'if let Some(provider) = search.provider()? {','if let Some(provider) = Option::<Arc<dyn acyclic_harness::search::SearchProvider>>::None {')
replace(paths[2],'client::local_identity(operation)','phase(|| client::local_identity(&local, args.prompt.as_deref(), operation)).await?')
replace(paths[2],'phase(|| client::resume_local_identity(&local, operation)).await?','return Err("retained interactive resume is unavailable".into())')
p=Path(paths[3]);s=p.read_text();start=s.index('/// Allocate a fresh submission');end=s.index('/// Keep the selected interactive',start)
# Preserve the exact pre-fix implementation so archive-only OS runners can mutate it.
old=r'''/// Reopen an unfinished launch; completed prompts allocate a fresh request.
/// The private volume supplies the workspace identity. Prompt bytes supply the
/// remaining input identity; argv identities are only explicit overrides.
pub async fn local_identity(
    local: &LocalHarness,
    prompt: Option<&str>,
    explicit: Option<OperationId>,
) -> Result<OperationId> {
    if let Some(operation) = explicit {
        return Ok(operation);
    }
    let name = prompt.map_or_else(
        || "interactive.json".to_owned(),
        |text| format!("prompt-{}.json", blake3::hash(text.as_bytes()).to_hex()),
    );
    let key = FileKey::Ui {
        directory: Component::new("workspace")?,
        name: Component::new(name)?,
    };
    let files = local.task_files();
    loop {
        let snapshot = files.snapshot(&Query::File(key.clone()), 1).await?;
        if let Some(file) = snapshot.files.first() {
            let operation: OperationId = serde_json::from_slice(&file.bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let session = Component::new(operation.to_string())?;
            let completed = if prompt.is_some() {
                let result = FileKey::Session {
                    session,
                    file: TaskFile::ExecutionResult(Component::new(operation.to_string())?),
                };
                !files
                    .snapshot(&Query::File(result), 1)
                    .await?
                    .files
                    .is_empty()
            } else {
                let layout = FileKey::Ui {
                    directory: Component::new(format!("session-{operation}"))?,
                    name: Component::new("layout.json")?,
                };
                files
                    .snapshot(&Query::File(layout), 1)
                    .await?
                    .files
                    .first()
                    .map(|file| serde_json::from_slice::<serde_json::Value>(&file.bytes))
                    .transpose()
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .is_some_and(|layout| layout["exit_requested"] == true)
            };
            if !completed {
                return Ok(operation);
            }
        }
        let operation = OperationId::new();
        let value = FileValue::new(
            key.clone(),
            serde_json::to_vec(&operation).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        match files.write(snapshot.generation, &value).await {
            Ok(_) => return Ok(operation),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}
'''
s=s[:start]+old+'\n'+s[end:];s=s.replace('{FileKey, FileValue, Query}','{Component, FileKey, FileValue, Query, TaskFile}');p.write_text(s)
replace(paths[4],'return files.cancel_active().await;','return Ok(());')
replace(paths[8],'''        let decision = journal.interaction_outcome(approval, self.io)?;
        futures::pin_mut!(decision);
        let outcome = match futures::future::select(journal.model_cancellation(), decision).await {
            futures::future::Either::Left((cancelled, _)) => {
                cancelled?;
                Some(InteractionOutcome::Cancelled {})
            }
            futures::future::Either::Right((outcome, _)) => outcome?,
        };''','''        let outcome = journal.interaction_outcome(approval, self.io)?.await?;''')
replace(paths[5],'} => self.rewind_checkpoint(operation, required(target)?).await,','} => Err(Error::Unsupported("rewind is unavailable".into())),')
replace(paths[6],'    if open {','    if false && open {')
PY
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
binary="$SESSIONS_GRAPHCODER_BINARY"
mock="$SESSIONS_MOCK_BINARY"
expect_failure() {
 local name=$1; shift
 python3 - "$proof/$name.log" "$name" "$@" <<'PY'
import pathlib,re,subprocess,sys,time
started=time.monotonic()
# Each shipping driver owns its deadline and reaps its children before returning.
# A second clock would race that cleanup and suppress the actual failed oracle.
result=subprocess.run(sys.argv[3:],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
elapsed=time.monotonic()-started
pathlib.Path(sys.argv[1]).write_text(result.stdout)
print(result.stdout,end='')
assert result.returncode!=0, f'negative control unexpectedly passed: {sys.argv[2]}'
assert re.search(r'AssertionError|AbortError|"status": "fail"',result.stdout), 'negative failed outside the intended oracle'
assert elapsed<10, f'negative control exceeded ten seconds: {elapsed}'
print('NEGATIVE_PASS',sys.argv[2],'seconds',elapsed,flush=True)
PY
}
expect_failure final-prompt node scripts/qualify-final-answer.mjs "$binary" completed-only prompt
expect_failure final-jsonl node scripts/qualify-final-answer.mjs "$binary" completed-only jsonl
expect_failure pre-prompt node scripts/qualify-final-answer.mjs "$binary" pre-dispatch prompt
expect_failure pre-jsonl node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
expect_failure fresh-retry node rust/crates/harness/tests/scenarios/headless-submissions.mjs "$binary" kill-mid-turn
for case in web-search pty-quit-resume pty-checkpoint-rewind pty-cancel-escape pty-approval-cancel-escape pty-approval-cancel-ctrl-c pty-diff-modified; do
 expect_failure "$case" env E2E_OUTPUT="$proof/$case" node scripts/graphcoder-e2e/run.mjs "$binary" "$mock" "$case"
done
restore
trap 'rm -rf "$proof"' EXIT
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
node scripts/qualify-final-answer.mjs "$binary" completed-only prompt
node scripts/qualify-final-answer.mjs "$binary" pre-dispatch jsonl
# Independently restore each presentation defect after testing the shared wait.
for control in pending-state stale-outcome-notice; do
python3 - "$control" <<'PY'
from pathlib import Path
import sys
if sys.argv[1]=='pending-state':
 p=Path('rust/crates/harness/src/tui/render/task.rs')
 old='Some("active" | "running" | "pending")';new='Some("active" | "running")'
else:
 p=Path('rust/crates/harness/src/tui/render.rs')
 old='''        if !self.busy {
            self.composed_notices
                .extend(self.outcome_notice.iter().cloned());
        }'''
 new='''        self.composed_notices
            .extend(self.outcome_notice.iter().cloned());'''
s=p.read_text()
assert s.count(old)==1
p.write_text(s.replace(old,new))
PY
trap 'restore; rm -rf "$proof"' EXIT
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
cases=(pty-approval-cancel-escape pty-approval-cancel-ctrl-c)
if [ "$control" = stale-outcome-notice ]; then
 cases=(pty-approval-cancel-continue-held)
fi
for case in "${cases[@]}"; do
 expect_failure "$control-$case" env E2E_OUTPUT="$proof/$control-$case" node scripts/graphcoder-e2e/run.mjs "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" "$case"
done
restore
done
trap 'rm -rf "$proof"' EXIT
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
for case in pty-approval-cancel-escape pty-approval-cancel-ctrl-c pty-approval-cancel-continue-held; do
 E2E_OUTPUT="$proof/restored-$case" node scripts/graphcoder-e2e/run.mjs "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" "$case"
done
echo SESSIONS_NEGATIVE_CONTROLS_PASSED
