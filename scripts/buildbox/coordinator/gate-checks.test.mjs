import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const coordinator = dirname(fileURLToPath(import.meta.url));
const scripts = resolve(coordinator, '../..');
const checker = readFileSync(join(coordinator, 'gate-checks.sh'), 'utf8');
const pr = readFileSync(join(scripts, 'pr-gate-checks.sh'), 'utf8');
function fixture(run) {
  const cwd = mkdtempSync(join(tmpdir(), 'gate-public-windows-'));
  const put = (name, text) => {
    const path = join(cwd, name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, text);
    chmodSync(path, 0o700);
  };
  try {
    put('scripts/buildbox/coordinator/gate-checks.sh', checker);
    put('scripts/pr-gate-checks.sh', pr);
    put('scripts/gate-rust-format.sh', readFileSync(join(scripts, 'gate-rust-format.sh')));
    put('bin/bun', '#!/bin/bash\nexit 0\n');
    put('scripts/gate-test-runner.sh', readFileSync(join(scripts, 'gate-test-runner.sh')));
    put('scripts/main-gate-checks.sh', `#!/bin/bash
echo main >> phases
if [ -n "$FORMAT_RC" ]; then
 echo 'Diff in fixture.rs:1:'
 echo "GATE_RUSTFMT rc=$FORMAT_RC seconds=0 scope="
 if [ "$COMPLETION" != missing ]; then echo "GATE_DONE rc=$FORMAT_RC"; fi
 if [ "$COMPLETION" = duplicate ]; then echo "GATE_DONE rc=$FORMAT_RC"; fi
 exit "$FORMAT_RC"
fi
echo "GATE_WINDOWS_FEATURES rc=$FEATURE_RC"
if [ "$COMPLETION" != missing ]; then echo "GATE_DONE rc=$MAIN_RC"; fi
if [ "$COMPLETION" = duplicate ]; then echo "GATE_DONE rc=$MAIN_RC"; fi
exit "$MAIN_RC"
`);
    put('scripts/gate-windows-features.sh', '#!/bin/bash\necho features >> phases\necho "GATE_WINDOWS_FEATURES rc=$FEATURE_RC"\nexit "$FEATURE_RC"\n');
    put('bin/cargo', `#!/bin/bash
if [ "$1" = clippy ] && [[ " $* " == *" --target "* ]]; then
 python3 -c 'import json,os,sys; print(json.dumps({"argv":sys.argv[1:],"flags":os.getenv("RUSTFLAGS"),"encoded":os.getenv("CARGO_ENCODED_RUSTFLAGS"),"target":os.getenv("CARGO_TARGET_DIR")}))' "$@" >> strict.jsonl
 echo 'error: retained strict runtime diagnostic' >&2
 exit "$STRICT_RC"
fi
case "$1" in
 metadata) echo '{"workspace_members":["native"],"packages":[{"id":"native","name":"acyclic-native-runtime","targets":[]}]}' ;;
 clean|test|clippy) exit 0 ;;
 *) exit 9 ;;
esac
`);
    put('.gate-scope-mode', 'affected\n');
    put('.gate-tests-packages', 'acyclic-native-runtime\n');
    put('.gate-clippy-packages', 'acyclic-native-runtime\n');
    const env = { ...process.env, PATH: join(cwd, 'bin') + ':' + process.env.PATH,
      TMPDIR: cwd, CARGO_TARGET_DIR: join(cwd, 'target'),
      GATE_WINDOWS_RUNTIME_TARGET_DIR: '', GATE_WINDOWS_RUNTIME_OUTPUT_DIR: join(cwd, 'output'),
      RUSTFLAGS: '--cap-lints warn', CARGO_ENCODED_RUSTFLAGS: '--cap-lints\x1fwarn',
      STRICT_RC: '0', MAIN_RC: '0', FEATURE_RC: '0', COMPLETION: 'normal', FORMAT_RC: '' };
    const invoke = (adapter, extra = {}) => {
      rmSync(join(cwd, 'strict.jsonl'), { force: true });
      rmSync(join(cwd, 'phases'), { force: true });
      rmSync(join(cwd, 'output/.gate-windows-runtime.out'), { force: true });
      rmSync(join(cwd, '.gate-windows-runtime.out'), { force: true });
      const args = adapter === 'direct' ? ['scripts/buildbox/coordinator/gate-checks.sh', '--windows-runtime-only'] :
        [adapter === 'pr' ? 'scripts/pr-gate-checks.sh' : 'scripts/buildbox/coordinator/gate-checks.sh'];
      const result = spawnSync('bash', args, { cwd, encoding: 'utf8', env: { ...env, ...extra } });
      const calls = readFile(join(cwd, 'strict.jsonl')).trim().split('\n').filter(Boolean).map(JSON.parse);
      return { ...result, calls, phases: readFile(join(cwd, 'phases')).trim().split('\n').filter(Boolean) };
    };
    run({ cwd, put, invoke });
  } finally { rmSync(cwd, { recursive: true, force: true }); }
}
function readFile(path) {
  try { return readFileSync(path, 'utf8'); } catch (error) { if (error.code === 'ENOENT') return ''; throw error; }
}
function strict(result, status) {
  assert.equal(result.status, status, result.stdout + result.stderr);
  assert.equal(result.calls.length, 1, 'exactly one canonical strict invocation');
  assert.deepEqual(result.calls[0].argv, ['clippy', '--locked', '--target', 'x86_64-pc-windows-gnu',
    '-p', 'acyclic-native-runtime', '--all-targets', '--all-features', '--message-format=short']);
  assert.equal(result.calls[0].flags, '');
  assert.equal(result.calls[0].encoded, '');
  assert.match(result.calls[0].target, /\/target\/.*gate-windows-runtime$/);
  assert.match(result.stdout, /error: retained strict runtime diagnostic/);
  assert.match(result.stdout, new RegExp('GATE_WINDOWS_RUNTIME_CLIPPY rc=' + status));
}

test('direct, PR and main adapters execute the same strict policy and preserve failure', () => fixture(({ cwd, invoke }) => {
  for (const adapter of ['direct', 'pr', 'main']) for (const rc of [0, 101]) {
    const result = invoke(adapter, { STRICT_RC: String(rc) });
    strict(result, rc);
    if (adapter !== 'direct') {
      assert.deepEqual(result.stdout.match(/^GATE_DONE rc=\d+$/gm), ['GATE_DONE rc=' + rc]);
      assert.deepEqual(result.phases, [adapter === 'pr' ? 'features' : 'main']);
    }
    assert.match(readFile(join(cwd, adapter === 'pr' ? '.gate-windows-runtime.out' : 'output/.gate-windows-runtime.out')), /retained strict runtime diagnostic/);
  }
}));

test('one policy mutation is rejected through every host adapter', () => fixture(({ put, invoke }) => {
  for (const [from, to] of [
    ['x86_64-pc-windows-gnu', 'x86_64-pc-windows-msvc'],
    ['-p acyclic-native-runtime', '-p wrong-package'],
    ['--all-targets --all-features', '--lib --features test-support'],
    ['RUSTFLAGS= CARGO_ENCODED_RUSTFLAGS=', ''],
    ['rc=${status[0]}', 'rc=0'],
  ]) {
    assert.ok(checker.includes(from));
    put('scripts/buildbox/coordinator/gate-checks.sh', checker.replaceAll(from, to));
    for (const adapter of ['direct', 'pr', 'main']) {
      const result = invoke(adapter, { STRICT_RC: '101' });
      assert.equal(result.calls.length, 1, 'negative control must reach the shared policy');
      assert.throws(() => strict(result, 101), assert.AssertionError);
    }
  }
}));

test('missing shared policy refuses PR dispatch instead of falling back to a second implementation', () => fixture(({ cwd, invoke }) => {
  rmSync(join(cwd, 'scripts/buildbox/coordinator/gate-checks.sh'));
  const result = invoke('pr');
  assert.notEqual(result.status, 0);
  assert.equal(result.calls.length, 0);
  assert.doesNotMatch(result.stdout, /^GATE_DONE rc=0$/m);
}));

test('main requires its reviewed runner, exact completion and its owned feature result without retry', () => fixture(({ cwd, put, invoke }) => {
  for (const [overrides, expected] of [
    [{ MAIN_RC: '101' }, 101], [{ FEATURE_RC: '101' }, 2],
    [{ COMPLETION: 'missing' }, 224], [{ COMPLETION: 'duplicate' }, 224],
  ]) {
    const result = invoke('main', overrides);
    assert.equal(result.status, expected, result.stdout + result.stderr);
    assert.deepEqual(result.phases, ['main']);
    assert.deepEqual(result.stdout.match(/^GATE_DONE rc=\d+$/gm), ['GATE_DONE rc=' + expected]);
  }
  put('scripts/main-gate-checks.sh', '#!/bin/bash\necho "GATE_WINDOWS_FEATURES rc=0"\necho "GATE_DONE rc=0"\nexit 101\n');
  const mismatch = invoke('main');
  assert.equal(mismatch.status, 224);
  assert.match(mismatch.stdout, /completion does not match/);
  rmSync(join(cwd, 'scripts/main-gate-checks.sh'));
  const missing = invoke('main');
  assert.equal(missing.status, 224);
  assert.equal(missing.calls.length, 0);
  assert.match(missing.stdout, /preparation-INFRA: missing reviewed/);
}));

test('retention failures remain nonzero rather than passing clippy through a broken pipe', () => fixture(({ put, invoke }) => {
  put('bin/tee', '#!/bin/bash\ncat\nexit 23\n');
  const result = invoke('direct');
  assert.equal(result.status, 2);
  assert.match(result.stdout, /could not retain Windows runtime clippy output \(tee rc=23\)/);
  // The predecessor shape guessed success from Cargo alone.
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('if [ "${status[1]}" != 0 ]; then', 'if false; then'));
  assert.equal(invoke('direct').status, 0);
}));

test('completion and missing-runner negative controls expose fabricated main success', () => fixture(({ cwd, put, invoke }) => {
  const guard = 'if [ "${#completed[@]}" != 1 ] || [ "${completed[0]:-}" != "GATE_DONE rc=$main_rc" ]; then';
  assert.ok(checker.includes(guard));
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace(guard, 'if false; then'));
  assert.equal(invoke('main', { COMPLETION: 'missing' }).status, 0);
  assert.equal(invoke('main', { COMPLETION: 'duplicate' }).status, 0);
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('exit 224', 'exit 0'));
  rmSync(join(cwd, 'scripts/main-gate-checks.sh'));
  assert.equal(invoke('main').status, 0);
}));

test('main preserves completed preflight source failures without starting a compiler', () => fixture(({ put, invoke }) => {
  for (const [format, expected] of [[1, 1], [127, 224]]) {
    const result = invoke('main', { FORMAT_RC: String(format) });
    assert.equal(result.status, expected, result.stdout + result.stderr);
    assert.equal(result.calls.length, 0);
    assert.deepEqual(result.phases, ['main']);
    assert.deepEqual(result.stdout.match(/^GATE_DONE rc=\d+$/gm), ['GATE_DONE rc=' + expected]);
  }
  for (const completion of ['missing', 'duplicate']) {
    const result = invoke('main', { FORMAT_RC: '1', COMPLETION: completion });
    assert.equal(result.status, 224, result.stdout + result.stderr);
    assert.equal(result.calls.length, 0);
  }
  put('scripts/main-gate-checks.sh', '#!/bin/bash\necho "GATE_RUSTFMT rc=1 seconds=0 scope="\necho "GATE_DONE rc=0"\nexit 1\n');
  assert.equal(invoke('main').status, 224, 'inconsistent payload remains INFRA before preflight');
  put('scripts/main-gate-checks.sh', '#!/bin/bash\necho "GATE_RUSTFMT rc=1 seconds=0 scope="\necho "GATE_DONE rc=1"\nexit 1\n');
  // Bypassing the shared preflight verdict launches clippy after a format failure.
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('gate_preflight_verdict "$main_output/main.out"; preflight_rc=$?', 'preflight_rc=0'));
  assert.equal(invoke('main', { FORMAT_RC: '1' }).calls.length, 1);
}));

test('required-phase policy distinguishes formatting, missing execution and exact strict dispatch', () => fixture(({ cwd, put }) => {
  const log = join(cwd, 'verdict.log');
  const required = 'GATE_WINDOWS_RUNTIME_REQUIRED target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all\n';
  const success = 'GATE_WINDOWS_RUNTIME_CLIPPY rc=0 seconds=1 target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all\n';
  const format = 'Diff in fixture.rs:1:\nGATE_RUSTFMT rc=1 seconds=0 scope=\n';
  const verdict = (text) => {
    writeFileSync(log, text);
    return spawnSync('bash', ['scripts/buildbox/coordinator/gate-checks.sh', '--required-phases', log], { cwd, encoding: 'utf8' });
  };
  for (const [text, expected] of [[required + format, 1], [required, 4], [required + success, 0],
    [required + success.replace('windows-gnu', 'windows-msvc'), 4],
    [required + success + success, 4], [required + success.replace('rc=0', 'rc=101'), 1],
    ...[1, 2, 23, 126, 127, 137, 224, -9].map(rc => [required + success.replace('rc=0', 'rc=' + rc), 4]),
    [required + success.replace('rc=0', 'rc=101') + success, 4],
    [format + format, 4], [format.replace('rc=1', 'rc=bad'), 4],
    [format.replace('rc=1', 'rc=127'), 4], [format + 'spawn rustfmt ENOENT\n', 4],
    [format + 'No space left on device\n', 4], [format + 'signal: 9, SIGKILL\n', 4]]) {
    const result = verdict(text);
    assert.equal(result.status, expected, result.stdout + result.stderr);
  }
  assert.match(verdict(required + format).stdout, /Diff in fixture.rs:1:/);
  // The previous strict-first ordering mistakes a format rejection for INFRA.
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('gate_preflight_verdict "$log"; preflight_rc=$?', 'preflight_rc=0'));
  assert.equal(verdict(required + format).status, 4);
}));

test('actual coordinator validates executor before formatting and never retries a completed rejection', () => fixture(({ cwd, put }) => {
  const source = readFileSync(join(coordinator, 'pr-gate.sh'), 'utf8');
  const from = source.indexOf('# An ended client stream');
  const to = source.indexOf('# One selector owns');
  assert.ok(from >= 0 && to > from);
  const publicScript = join(cwd, 'coordinator.sh');
  const chunk = source.slice(from, to).replaceAll('Q:/lanes', join(cwd, 'scripts/buildbox/coordinator'));
  put('coordinator.sh', '#!/bin/bash\n' + chunk + '\nexit 0\n');
  mkdirSync(join(cwd, '.codex-fleet'));
  const log = join(cwd, 'verdict.log');
  const required = 'GATE_WINDOWS_RUNTIME_REQUIRED target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all\n';
  const format = 'Diff in fixture.rs:1:\nGATE_RUSTFMT rc=1 seconds=0 scope=\n';
  const run = (text, rc) => {
    writeFileSync(log, required + text);
    return spawnSync('bash', [publicScript], { cwd, encoding: 'utf8', env: { ...process.env, HOME: cwd, log, rc: String(rc), n: 'fixture' } });
  };
  const failed = run(format + 'GATE_DONE rc=1\n', 1);
  assert.equal(failed.status, 1, failed.stdout + failed.stderr);
  assert.match(failed.stdout, /GATE FAIL #fixture: Rust formatting qualification failed/);
  assert.match(failed.stdout, /Diff in fixture.rs:1:/);
  assert.equal(run(format, 1).status, 4, 'cutoff never becomes a source verdict');
  assert.equal(run(format + 'GATE_DONE rc=0\n', 1).status, 4, 'executor mismatch never becomes a source verdict');
  assert.equal(run(format + 'GATE_DONE rc=1\n', 224).status, 4, 'executor infra never becomes a source verdict');
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('gate_preflight_verdict "$log"; preflight_rc=$?', 'preflight_rc=0'));
  assert.equal(run(format + 'GATE_DONE rc=1\n', 1).status, 4, 'old ordering is the INFRA-loop negative');
}));


test('public lint verdict uses the same required-phase policy and retains typed INFRA', () => fixture(({ cwd, put }) => {
  put('scripts/buildbox/coordinator/gate-checks-verdict.sh', readFileSync(join(coordinator, 'gate-checks-results.sh')));
  const log = join(cwd, 'verdict.log');
  const base = join(cwd, 'baseline.log');
  writeFileSync(base, 'GATE_CLIPPY rc=0\n');
  const required = 'GATE_WINDOWS_RUNTIME_REQUIRED target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all\n';
  const format = 'Diff in fixture.rs:1:\nGATE_RUSTFMT rc=1 seconds=0 scope=\n';
  const success = 'GATE_WINDOWS_RUNTIME_CLIPPY rc=0 seconds=1 target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all\n';
  const run = (text) => {
    writeFileSync(log, text + 'GATE_CLIPPY rc=0\n');
    return spawnSync('bash', ['scripts/buildbox/coordinator/gate-checks-verdict.sh', log, base], { cwd, encoding: 'utf8' });
  };
  for (const [text, rc] of [[required + format, 1], [required, 4], [required + success, 0],
    [required + success.replace('rc=0', 'rc=101'), 1]]) {
    const result = run(text);
    assert.equal(result.status, rc, result.stdout + result.stderr);
  }
  put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace('gate_preflight_verdict "$log"; preflight_rc=$?', 'preflight_rc=0'));
  assert.equal(run(required + format).status, 4, 'the old ordering also breaks the public lint verdict');
}));


test('real log-writer and tool failures stay INFRA through every public host adapter', () => fixture(({ cwd, put, invoke }) => {
  put('bin/tee', '#!/bin/bash\nif [[ "$1" == *gate-windows-runtime.out ]]; then cat; exit 23; fi\nexec /usr/bin/tee "$@"\n');
  const source = readFileSync(join(coordinator, 'pr-gate.sh'), 'utf8');
  const chunk = source.slice(source.indexOf('# An ended client stream'), source.indexOf('# One selector owns'))
    .replaceAll('Q:/lanes', join(cwd, 'scripts/buildbox/coordinator'));
  put('coordinator.sh', '#!/bin/bash\n' + chunk + '\nexit 0\n');
  mkdirSync(join(cwd, '.codex-fleet'));
  put('scripts/buildbox/coordinator/gate-checks-verdict.sh', readFileSync(join(coordinator, 'gate-checks-results.sh')));
  const log = join(cwd, 'infra.log');
  const base = join(cwd, 'baseline.log');
  writeFileSync(base, 'GATE_CLIPPY rc=0\n');
  const interpret = () => spawnSync('bash', ['scripts/buildbox/coordinator/gate-checks.sh', '--required-phases', log], { cwd, encoding: 'utf8' });
  for (const adapter of ['direct', 'pr', 'main']) {
    const output = invoke(adapter);
    assert.equal(output.status, 2, output.stdout + output.stderr);
    assert.equal(output.calls.length, 1, 'real strict writer must be reached');
    assert.match(output.stdout, /could not retain Windows runtime clippy output \(tee rc=23\)/);
    writeFileSync(log, output.stdout);
    assert.equal(interpret().status, 4);
    const verdict = spawnSync('bash', ['scripts/buildbox/coordinator/gate-checks-verdict.sh', log, base], { cwd, encoding: 'utf8' });
    assert.equal(verdict.status, 4, verdict.stdout + verdict.stderr);
    if (adapter !== 'direct') {
      const gate = spawnSync('bash', ['coordinator.sh'], { cwd, encoding: 'utf8', env: { ...process.env, HOME: cwd, log, n: 'fixture', rc: '2' } });
      assert.equal(gate.status, 4, gate.stdout + gate.stderr);
      assert.match(gate.stdout, /GATE INFRA #fixture: Windows runtime executor or output retention failed/);
    }
    const guard = 'if [ "$runtime_rc" != 0 ] && [ "$runtime_rc" != 101 ]; then';
    put('scripts/buildbox/coordinator/gate-checks.sh', checker.replace(guard, 'if false; then'));
    assert.notEqual(interpret().status, 4, 'old nonzero-as-source reader misclassifies the retained writer failure');
    put('scripts/buildbox/coordinator/gate-checks.sh', checker);
  }
  rmSync(join(cwd, 'bin/tee'));
  for (const adapter of ['direct', 'pr', 'main']) {
    const output = invoke(adapter, { STRICT_RC: '127' });
    assert.equal(output.status, 127, output.stdout + output.stderr);
    assert.equal(output.calls.length, 1);
    writeFileSync(log, output.stdout);
    assert.equal(interpret().status, 4, 'missing tool is infrastructure');
  }
}));
