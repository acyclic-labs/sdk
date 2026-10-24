#!/usr/bin/env bash
# Linux on bb2: distinguish materialized volumes from unimported native roots,
# and prove root metadata cannot poison shipping GraphCoder's content diffs.
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/capture-negative.XXXXXX")
export CAPTURE_NEGATIVE_PROOF="$proof"
restore() {
 python3 - <<'PY'
import os
from pathlib import Path
root=Path(os.environ['CAPTURE_NEGATIVE_PROOF'])
for name in (root/'paths').read_text().splitlines():
 Path(name).write_bytes((root/name).read_bytes())
PY
}
trap restore EXIT
python3 - <<'PY'
import os
from pathlib import Path
root=Path(os.environ['CAPTURE_NEGATIVE_PROOF'])
mutations={
 'rust/crates/harness/src/local/checkout_tools/capture.rs':
  ('physical.import_source && cursor.initial.is_some()', 'cursor.initial.is_some()'),
 'rust/crates/harness-core/src/filesystem/file_query/diff.rs':
  ('path.is_empty() || crate::conversation::is_internal_path(&path)',
   'crate::conversation::is_internal_path(&path)'),
 'rust/crates/filesystem/src/watch.rs':
  ('if relative.as_os_str().is_empty() && mask & !(libc::IN_ATTRIB | libc::IN_ISDIR) != 0 {',
   'if relative.as_os_str().is_empty() {'),
}
(root/'paths').write_text('\n'.join(mutations))
for name in mutations:
 path=Path(name); saved=root/name; saved.parent.mkdir(parents=True,exist_ok=True)
 saved.write_bytes(path.read_bytes())
for name,(old,new) in mutations.items():
 path=Path(name); text=path.read_text()
 assert text.count(old)==1,(name,text.count(old))
 path.write_text(text.replace(old,new))
PY
cargo test --locked -p acyclic-harness --all-features --test main --no-run --message-format=json-render-diagnostics > "$proof/tests.jsonl"
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
python3 - <<'PY'
import json,os,subprocess,time
from pathlib import Path
root=Path(os.environ['CAPTURE_NEGATIVE_PROOF'])
artifacts=[]
for line in (root/'tests.jsonl').read_text().splitlines():
 row=json.loads(line)
 if row.get('reason')=='compiler-artifact' and row.get('executable') and row.get('profile',{}).get('test') and row.get('target',{}).get('name')=='main' and row.get('manifest_path','').endswith('/harness/Cargo.toml'):
  artifacts.append(row['executable'])
assert len(artifacts)==1,artifacts
cases=[
 ('materialized-volume',['scenarios'],{'E2E_FILTER':'scenarios/default-tools/edit/begin.json'},'workspace generation changed'),
 ('root-metadata',['file_query::diff_tree::root_metadata_does_not_interrupt_file_diffs_or_replay'],{},'metadata.paths.is_empty()'),
]
for name,args,extra,expected in cases:
 start=time.monotonic()
 result=subprocess.run([artifacts[0],*args,'--exact','--test-threads=1','--nocapture'],env=dict(os.environ,**extra),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=9)
 elapsed=time.monotonic()-start; (root/f'{name}.log').write_text(result.stdout)
 print(result.stdout,flush=True)
 assert result.returncode!=0 and expected in result.stdout,(name,result.returncode)
 print('CAPTURE_NEGATIVE_PASS',name,'seconds',elapsed,flush=True)
start=time.monotonic()
result=subprocess.run(['node','scripts/graphcoder-e2e/run.mjs',os.environ['SESSIONS_GRAPHCODER_BINARY'],os.environ['SESSIONS_MOCK_BINARY'],'pty-diff-root-metadata'],env=dict(os.environ,E2E_OUTPUT=str(root/'shipping')),stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
elapsed=time.monotonic()-start; (root/'shipping.log').write_text(result.stdout)
print(result.stdout,flush=True)
assert result.returncode!=0 and '"status": "fail"' in result.stdout
assert elapsed<10,elapsed
requests=json.loads((root/'shipping/pty-diff-root-metadata/requests.json').read_text())
assert any('checkout root was replaced' in item.get('output','') for row in requests
 for item in row.get('body',{}).get('input',[]) if item.get('type')=='function_call_output')
print('CAPTURE_NEGATIVE_PASS shipping-root-metadata seconds',elapsed,flush=True)
PY
restore
trap - EXIT
cargo build --locked -p graphcoder -p harness-mock-model --message-format=json-render-diagnostics > .bb-sessions-shipping.jsonl
python3 scripts/graphcoder-binaries.py .bb-sessions-shipping.jsonl > .bb-sessions-binaries.env
source .bb-sessions-binaries.env
E2E_OUTPUT="$proof/restored" node scripts/graphcoder-e2e/run.mjs "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY" pty-diff-root-metadata
echo "CAPTURE_NEGATIVE_CONTROLS_PASS evidence=$proof"
