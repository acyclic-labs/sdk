#!/usr/bin/env bash
# Run on bb2: prove non-repository context cannot create a Git command.
set -euo pipefail
proof=$(mktemp -d "$CARGO_TARGET_DIR/environment-negative.XXXXXX")
source=rust/crates/harness/src/local/context/environment.rs
cp "$source" "$proof/environment.rs"
restore() { cp "$proof/environment.rs" "$source"; }
trap restore EXIT
cat > "$proof/run.py" <<'PY'
import json, pathlib, subprocess, sys, time
root, mode = pathlib.Path(sys.argv[1]), sys.argv[2]
rows = [json.loads(line) for line in (root / 'tests.jsonl').read_text().splitlines()]
paths = [row['executable'] for row in rows if row.get('reason') == 'compiler-artifact'
         and row.get('executable') and row.get('profile', {}).get('test')
         and row.get('target', {}).get('name') == 'acyclic_harness']
assert len(paths) == 1, paths
name = 'local::context::environment::tests::'
if mode == 'binding':
    name = 'local::tests::environment::'
args = [paths[0], name, '--test-threads=4', '--nocapture']
if mode in ('negative', 'deadline'):
    name += ('git_environment_outside_worktree_never_starts_a_command' if mode == 'negative'
             else 'git_environment_discovery_shares_the_summary_deadline')
    args = [paths[0], name, '--exact', '--test-threads=1', '--nocapture']
start = time.monotonic()
result = subprocess.run(args, capture_output=True, text=True, timeout=9)
elapsed = time.monotonic() - start
output = result.stdout + result.stderr
(root / (mode + '.log')).write_text(output)
print(output, flush=True)
assert elapsed < 10, elapsed
if mode in ('negative', 'deadline'):
    oracle = ('non-repository context started a Git command' if mode == 'negative'
              else 'Git discovery escaped the summary deadline')
    assert result.returncode != 0 and oracle in output
else:
    count = 4 if mode == 'binding' else 5
    assert result.returncode == 0 and f'{count} passed; 0 failed' in output
print('ENVIRONMENT_CONTROL_PASS', mode, 'seconds', elapsed, flush=True)
PY
cargo test --locked -p acyclic-harness --all-features --lib --no-run --message-format=json-render-diagnostics > "$proof/tests.jsonl"
python3 "$proof/run.py" "$proof" positive
python3 "$proof/run.py" "$proof" binding
python3 - "$source" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
source = path.read_text()
start = source.index('        let directory =\n')
end = source.index('        let mut command = command()?;', start)
assert 'WorktreeDirectory::discover(&cwd)' in source[start:end]
path.write_text(source[:start] + '        let directory = WorktreeDirectory(cwd);\n' + source[end:])
PY
cargo test --locked -p acyclic-harness --all-features --lib --no-run --message-format=json-render-diagnostics > "$proof/tests.jsonl"
python3 "$proof/run.py" "$proof" negative
restore
python3 - "$source" <<'PY'
import pathlib, sys
path = pathlib.Path(sys.argv[1])
source = path.read_text()
start = source.index('    tokio::time::timeout(GIT_STATUS_TIMEOUT, async {\n')
body = start + len('    tokio::time::timeout(GIT_STATUS_TIMEOUT, async {\n')
end = source.index('        let mut command = command()?;', body)
discovery = source[body:end]
assert 'WorktreeDirectory::discover(&cwd)' in discovery
path.write_text(source[:start] + discovery + source[start:body] + source[end:])
PY
cargo test --locked -p acyclic-harness --all-features --lib --no-run --message-format=json-render-diagnostics > "$proof/tests.jsonl"
python3 "$proof/run.py" "$proof" deadline
restore
trap - EXIT
echo "ENVIRONMENT_SOURCE_RESTORED evidence=$proof"
