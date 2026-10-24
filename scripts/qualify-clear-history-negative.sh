#!/usr/bin/env bash
# On bb2: the immutable clear outcome must hide its own archive entry.
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/clear-history-negative.XXXXXX")
export CLEAR_HISTORY_NEGATIVE_PROOF="$proof"
source_file=rust/crates/harness/src/client/commands/session.rs
cp "$source_file" "$proof/session.rs"
restore() { cp "$proof/session.rs" "$source_file"; }
trap restore EXIT
python3 - <<'PY'
from pathlib import Path
p=Path('rust/crates/harness/src/client/commands/session.rs')
s=p.read_text()
old='.max(self.cleared_output)'
assert s.count(old)==1
p.write_text(s.replace(old,''))
PY
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > "$proof/build.jsonl"
python3 scripts/graphcoder-binaries.py "$proof/build.jsonl" > "$proof/binaries.env"
source "$proof/binaries.env"
python3 - <<'PY'
import json,os,subprocess,time
from pathlib import Path
root=Path(os.environ['CLEAR_HISTORY_NEGATIVE_PROOF'])
started=time.monotonic()
result=subprocess.run(['node','scripts/graphcoder-e2e/run.mjs',
 os.environ['SESSIONS_GRAPHCODER_BINARY'],os.environ['SESSIONS_MOCK_BINARY'],
 'pty-clear-history'],env=dict(os.environ,E2E_OUTPUT=str(root/'shipping')),
 stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
seconds=time.monotonic()-started
(root/'shipping.log').write_text(result.stdout)
print(result.stdout,flush=True)
report=json.loads((root/'shipping/matrix.json').read_text())
assert report['selected']==['pty-clear-history'] and len(report['results'])==1
case=report['results'][0]
assert result.returncode!=0 and case['case']=='pty-clear-history' and case['status']=='fail'
assert seconds<10,seconds
captures=case['evidence']['captures']
assert len(captures)==1
capture=captures[0]
assert capture['actions']==3,capture['actions']
assert 'CLEAR_E2E_PRIVATE_REPLY' in capture['visible'],capture['visible']
assert 'Enter yes or no.' not in capture['visible'],capture['visible']
print('CLEAR_HISTORY_SHIPPING_NEGATIVE_PASS seconds',seconds,flush=True)
PY
restore
trap - EXIT
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > "$proof/restored.jsonl"
python3 scripts/graphcoder-binaries.py "$proof/restored.jsonl" > "$proof/restored.env"
source "$proof/restored.env"
E2E_OUTPUT="$proof/restored" node scripts/graphcoder-e2e/run.mjs "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" pty-clear-history
echo "CLEAR_HISTORY_SHIPPING_RESTORED_PASS evidence=$proof"
