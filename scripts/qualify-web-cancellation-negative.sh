#!/usr/bin/env bash
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/sessions-web-cancel.XXXXXX")
source=rust/crates/harness-search/src/web/executor.rs
cp "$source" "$proof/executor.rs"
restore() { cp "$proof/executor.rs" "$source"; }
trap restore EXIT
python3 - "$source" <<'PY'
from pathlib import Path
import sys
p=Path(sys.argv[1]);s=p.read_text()
old='fn cancellable_on_drop(&self) -> bool {\n        true\n    }'
assert s.count(old)==1
p.write_text(s.replace(old,old.replace('true','false')))
PY
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
for name in pty-search-cancel-resume held-fetch; do
 python3 - "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" "$proof" "$name" <<'PY'
import os,pathlib,re,subprocess,sys,time
binary,mock,proof,name=sys.argv[1:];start=time.monotonic()
command=['node','scripts/qualify-fetch-cancellation.mjs',binary] if name=='held-fetch' else ['node','scripts/graphcoder-e2e/run.mjs',binary,mock,name]
r=subprocess.run(command,env=dict(os.environ,E2E_OUTPUT=proof+'/'+name),capture_output=True,text=True)
elapsed=time.monotonic()-start;out=r.stdout+r.stderr
pathlib.Path(proof+'/'+name+'.log').write_text(out)
assert r.returncode!=0,out
if name=='held-fetch':
 import json
 record=json.loads(pathlib.Path(proof+'/'+name+'/fetch-cancellation.json').read_text())
 assert not record['passed'] and record['captures'][0]['actions']==2,record
else: assert 'PTY completed 2/3 actions' in out,out
assert elapsed<10,elapsed
print('SESSIONS_WEB_CANCELLATION_NEGATIVE_PASS',name,elapsed,flush=True)
PY
done
restore
trap - EXIT
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
for name in pty-search-cancel-resume held-fetch; do
 if [ "$name" = held-fetch ]; then
  E2E_OUTPUT="$proof/restored-$name" node scripts/qualify-fetch-cancellation.mjs "$SESSIONS_GRAPHCODER_BINARY"
 else
  E2E_OUTPUT="$proof/restored-$name" node scripts/graphcoder-e2e/run.mjs "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" "$name"
 fi
done
E2E_OUTPUT="$proof/restored-lifecycle" node scripts/qualify-web-lifecycle.mjs "$SESSIONS_GRAPHCODER_BINARY"
echo SESSIONS_WEB_CANCELLATION_CONTROLS_PASS
