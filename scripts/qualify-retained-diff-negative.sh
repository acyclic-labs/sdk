#!/usr/bin/env bash
# Run on a Linux host. Restore both pre-review defects and reject their intended oracles.
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/retained-diff-negative.XXXXXX")
export RETAINED_DIFF_NEGATIVE_PROOF="$proof"
restore() {
 python3 - <<'PY'
import os
from pathlib import Path
root=Path(os.environ['RETAINED_DIFF_NEGATIVE_PROOF'])
for name in (root/'paths').read_text().splitlines():
 Path(name).write_bytes((root/name).read_bytes())
PY
}
trap restore EXIT
python3 - <<'PY'
import os
from pathlib import Path
root=Path(os.environ['RETAINED_DIFF_NEGATIVE_PROOF'])
names=['rust/crates/filesystem/src/workspace/read_core.rs','rust/crates/filesystem/src/workspace/diff/paths.rs']
(root/'paths').write_text('\n'.join(names))
for name in names:
 saved=root/name;saved.parent.mkdir(parents=True,exist_ok=True);saved.write_bytes(Path(name).read_bytes())
p=Path(names[0]);s=p.read_text();start=s.index('pub(super) async fn matches_content<');end=s.index('pub(super) async fn read_link<',start)
part=s[start:end];old='finish(workspace, &work, result, &mut accounting, cancellation).await'
assert part.count(old)==1
p.write_text(s[:start]+part.replace(old,'work.finish(result, &accounting)')+s[end:])
p=Path(names[1]);s=p.read_text();start=s.index('    reverse_grow(');end=s.index('\npub(super) fn sort_and_coalesce(',start)
s=s[:start]+'''    super::merge_changed_record(paths, path, record, before, accounting, cancellation)
}
'''+s[end:]
p.write_text(s)
PY
cargo test --locked -p acyclic-fs --all-features --lib --no-run --message-format=json-render-diagnostics > "$proof/tests.jsonl"
python3 - <<'PY'
import json,os,subprocess,time
from pathlib import Path
root=Path(os.environ['RETAINED_DIFF_NEGATIVE_PROOF'])
rows=[json.loads(line) for line in (root/'tests.jsonl').read_text().splitlines()]
paths=[r['executable'] for r in rows if r.get('reason')=='compiler-artifact' and r.get('executable') and r.get('profile',{}).get('test') and r.get('target',{}).get('name')=='acyclic_fs']
assert len(paths)==1,paths
listed=subprocess.check_output([paths[0],'--list'],text=True)
for suffix,oracle in [
 ('retained_content_comparisons_recheck_current_read_authority','assertion failed'),
 ('large_path_merge_preserves_boundary_copy_budget','bytes_copied')]:
 tests=[line.removesuffix(': test') for line in listed.splitlines() if line.endswith(suffix+': test')]
 assert len(tests)==1,tests
 start=time.monotonic()
 result=subprocess.run([paths[0],tests[0],'--exact','--test-threads=1','--nocapture'],capture_output=True,text=True,timeout=9)
 output=result.stdout+result.stderr;(root/(suffix+'.log')).write_text(output)
 print(output,flush=True)
 assert result.returncode!=0 and oracle in output,(suffix,result.returncode,output)
 print('RETAINED_DIFF_NEGATIVE_PASS',suffix,'seconds',time.monotonic()-start,flush=True)
PY
restore
trap - EXIT
echo "RETAINED_DIFF_SOURCE_RESTORED evidence=$proof"
