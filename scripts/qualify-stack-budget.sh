#!/usr/bin/env bash
set -euo pipefail
mkdir -p .tools
/usr/bin/time -f 'compile wall=%e s' cargo test --locked --no-run \
  -p graphcoder -p acyclic-plugin -p acyclic-harness -p acyclic-harness-core -p acyclic-fs -p acyclic-test-support -p acyclic-native-runtime \
  --features acyclic-harness/test-support --lib --bin acyclic --test cli --test stack_main \
  --message-format=json > .tools/stack-budget-artifacts.jsonl
python3 - <<'PY'
from pathlib import Path
import subprocess

# The real measurement body forbids unsafe Rust. Restoring the expired-stack
# dereference must fail compilation, even if its bytes happen to remain mapped.
source = Path('rust/crates/test-support/src/stack.rs').read_text()
broken = source.replace('let mut snapshot = vec![0; BYTES];',
    'let _ = unsafe { std::ptr::read_volatile(1usize as *const u8) };\n'
    '    let mut snapshot = vec![0; BYTES];', 1)
assert broken != source
path = Path('.tools/stack-budget-negative.rs')
path.write_text(broken)
result = subprocess.run(['rustc', '--edition=2024', '--crate-type=lib', '--crate-name=stack_budget_negative',
    str(path), '-o', '.tools/stack-budget-negative.rlib'], capture_output=True, text=True)
assert result.returncode != 0 and 'usage of an `unsafe` block' in result.stderr, result.stderr
print('expired-stack dereference negative control: rejected', flush=True)
PY
python3 - <<'PY'
import json, os, signal, subprocess, tempfile, time
artifacts = {}
for line in open('.tools/stack-budget-artifacts.jsonl'):
    item = json.loads(line)
    if item.get('reason') == 'compiler-artifact' and item.get('executable') and item['profile']['test']:
        artifacts[item['target']['name']] = item['executable']
checks = [
    ('acyclic_test_support', ['stack::']),
    ('cli', ['main_thread::']),
    ('acyclic', ['control_plane_requests_fit_the_main_thread_budget']),
    ('acyclic_harness_core', ['tool::erasure_tests']),
    ('acyclic_harness_core', ['native_future::tests::']),
    ('acyclic_harness_core', ['created_access::provider::frame_tests::']),
    ('acyclic_harness_core', ['obs::typed::tests']),
    ('acyclic_native_runtime', ['obs::']),
    ('acyclic_fs', ['materializer::preflight::tests']),
    ('acyclic_fs', ['native_mount::stack_budget']),
    ('acyclic_fs', ['heap_future::']),
    ('acyclic_fs', ['facade::read_accounting::']),
    ('acyclic_fs', ['kernel::allocation::accounting::tests::']),
]
# Each shipping entry is a separate bounded runtime suite.
checks.extend(('stack_main', [f'stack_budget::{case}', '--exact']) for case in [
    'diff', 'commit', 'tools-files', 'tools-history', 'tools-branches', 'tools-recovery', 'tools-rejected', 'merge-continue', 'merge-abort', 'child-merge',
    'spawn', 'send', 'wait', 'archive', 'jsonl', 'tui', 'unboxed',
])
# A renamed or missing suite cannot silently qualify by running zero tests.
listed = {}
for name, args in checks:
    if name not in listed:
        listed[name] = subprocess.check_output([artifacts[name], '--list'], text=True)
    assert args[0] in listed[name], (name, args)
failures = []
for name, args in checks:
    started = time.monotonic()
    try:
        with tempfile.TemporaryDirectory(prefix='acyclic-stack-codex-') as codex_home:
            command = [artifacts[name], *args, '--nocapture']
            process = subprocess.Popen(command, start_new_session=(os.name == 'posix'),
                env=dict(os.environ, CODEX_HOME=codex_home))
            try:
                status = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # The stack runner starts a measured shipping child. Retire
                # the entire suite when its deadline expires, including it.
                if os.name == 'posix':
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait()
                raise
            if status:
                raise subprocess.CalledProcessError(status, command)
        elapsed = time.monotonic()-started
        if elapsed >= 10:
            raise RuntimeError(f'suite exceeded 10 seconds: {elapsed:.3f}')
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, RuntimeError) as error:
        failures.append((name, args, str(error)))
    print(f'{name} {args}: wall={time.monotonic()-started:.3f} s', flush=True)
if failures:
    raise SystemExit(f'stack qualification failures: {failures}')
PY
/usr/bin/time -f 'windows-gnu compile wall=%e s' cargo check --locked \
  -p graphcoder -p acyclic-plugin -p acyclic-harness -p acyclic-harness-core -p acyclic-fs -p acyclic-test-support -p acyclic-native-runtime -p acyclic-conformance \
  --lib --tests --features acyclic-harness/test-support,acyclic-conformance/local-runner --target x86_64-pc-windows-gnu
