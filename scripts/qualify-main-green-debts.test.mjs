import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { PassThrough } from 'node:stream';
import { cargoArguments, verify, testExecutable, executeTest } from './qualify-main-green-debts.mjs';

const entry = { package: 'acyclic-harness', target: 'lib', regression: 'module::regression', controls: ['module::negative'] };
const success = 'test module::regression ... ok\ntest module::negative - should panic ... ok\n\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.12s\n';

test('the checked-in plan names each debt once with its class and structural fix', async () => {
  const entries = JSON.parse(await readFile(new URL('./main-green-debts.json', import.meta.url), 'utf8'));
  assert.ok(entries.length > 0);
  assert.equal(new Set(entries.map(item => item.regression)).size, entries.length);
  for (const item of entries) {
    assert.ok(item.class.length > 0);
    assert.match(item.fixed_by, /^[0-9a-f]{9}(,[0-9a-f]{9})*$/);
    cargoArguments(item);
  }
});

test('regression and negative control are exact named tests in one package', () => {
  assert.deepEqual(cargoArguments(entry), ['test', '--locked', '-p', 'acyclic-harness', '--all-features', '--lib', '--', 'module::regression', 'module::negative', '--exact', '--test-threads=4', '--color=never']);
  assert.equal(verify(entry, 0, success).seconds, 0.12);
  assert.equal(verify(entry, 0, success.replaceAll('\n', '\r\n')).tests.length, 2);
});

test('integrated negative controls execute their property once', () => {
  const integrated = { ...entry, controls: [entry.regression] };
  const output = 'test module::regression ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.01s\n';
  assert.deepEqual(verify(integrated, 0, output).tests, [entry.regression]);
});

test('zero-test success is the negative control for exit-code-only qualification', () => {
  const output = 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 43 filtered out; finished in 0.00s\n';
  const exitCodeOnly = status => status === 0;
  assert.equal(exitCodeOnly(0), true);
  assert.throws(() => verify(entry, 0, output), /expected every regression/);
});

test('missing, substituted, duplicate and ignored controls cannot qualify', () => {
  for (const output of [success.replace('module::negative', 'module::unrelated'),
    success.replace('module::negative', 'module::regression'),
    success.replace('module::negative - should panic ... ok', 'module::negative ... ignored'),
    success.replace('0 ignored', '1 ignored'), success.replace('2 passed', '1 passed')]) {
    assert.throws(() => verify(entry, 0, output));
  }
});

test('process failures and incomplete or over-budget summaries cannot qualify', () => {
  for (const status of [1, 101, null]) assert.throws(() => verify(entry, status, success));
  for (const output of [success.replace('0.12s', '10.00s'), success.replace('0 failed', '1 failed'),
    success.replace('0 measured', '1 measured'), success.replace('test result:', 'incomplete result:'), success + success]) {
    assert.throws(() => verify(entry, 0, output));
  }
});

test('a debt requires a regression, a negative control and literal test names', () => {
  for (const invalid of [{ ...entry, controls: [] }, { ...entry, regression: '' },
    { ...entry, controls: ['--skip'] }, { ...entry, package: '--workspace' }, { ...entry, target: '--all-targets' },
    { ...entry, target: undefined }, { ...entry, controls: 'module::negative' }]) {
    assert.throws(() => cargoArguments(invalid));
  }
});

test('only the selected Cargo test artifact can start execution', () => {
  const artifact = { reason: 'compiler-artifact', profile: { test: true },
    target: { name: 'acyclic_harness', kind: ['lib'] }, executable: '/fixture/test' };
  const output = JSON.stringify(artifact) + '\n';
  assert.equal(testExecutable(entry, output), artifact.executable);
  const integration = { ...artifact, target: { name: 'main', kind: ['test'] } };
  assert.equal(testExecutable({ ...entry, target: 'main' }, JSON.stringify(integration)), artifact.executable);
  assert.throws(() => testExecutable({ ...entry, target: 'main' }, JSON.stringify({
    ...integration, target: { name: 'main', kind: ['test', 'bin'] },
  })), /selected test executable/);
  for (const invalid of ['', output + output,
    JSON.stringify({ ...artifact, profile: { test: false } }),
    JSON.stringify({ ...artifact, target: { name: 'other', kind: ['lib'] } }),
    JSON.stringify({ ...artifact, target: { name: 'acyclic_harness', kind: ['bin'] } })]) {
    assert.throws(() => testExecutable(entry, invalid), /selected test executable/);
  }
});

test('all Cargo library target kinds qualify including combined rlib and cdylib artifacts', () => {
  const artifact = { reason: 'compiler-artifact', profile: { test: true },
    target: { name: 'acyclic_harness', kind: ['rlib', 'cdylib'] }, executable: '/fixture/test' };
  // The former lib-only predicate drops the real Harness artifact despite its
  // test profile and authoritative executable. No crate-specific exception.
  assert.equal(artifact.target.kind.includes('lib'), false);
  const libraryKinds = ['lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro'];
  // Every nonempty subset, including all combined crate-type declarations.
  for (let mask = 1; mask < 2 ** libraryKinds.length; mask++) {
    const kinds = libraryKinds.filter((_, index) => mask & (2 ** index));
    const output = JSON.stringify({ ...artifact, target: { ...artifact.target, kind: kinds } });
    assert.equal(testExecutable(entry, output), artifact.executable);
  }
  for (const kinds of [[], ['lib', 'bin'], ['example'], ['bench'], ['custom-build']]) {
    assert.throws(() => testExecutable(entry, JSON.stringify({ ...artifact, target: { ...artifact.target, kind: kinds } })), /selected test executable/);
  }
});

test('a completed executable preserves its output and failure status', async () => {
  const result = await executeTest(process.execPath, ['-e', 'console.log("fixture output");process.exit(3)']);
  assert.equal(result.status, 3);
  assert.equal(result.output.trim(), 'fixture output');
});

test('the execution budget fails the debt without waiting for inherited pipes to close', async () => {
  const child = Object.assign(new EventEmitter(), {
    stdout: new PassThrough(), stderr: new PassThrough(), pid: 123,
  });
  let expire;
  let stopped = false;
  const execution = executeTest('/fixture/test', [], {
    launch: () => child,
    arm(callback, milliseconds) { assert.equal(milliseconds, 10_000); expire = callback; return 1; },
    disarm() {},
    terminate(actual) { assert.equal(actual, child); stopped = true; },
  });
  assert.equal(typeof expire, 'function', 'watchdog must own every test execution');
  const rejected = assert.rejects(execution, /debt remains unqualified/);
  expire();
  await rejected;
  assert.ok(stopped);
  assert.ok(child.stdout.destroyed && child.stderr.destroyed);
  // No close event was emitted. A failed debt still leaves the queue able to
  // start its successor rather than waiting on a test-owned pipe forever.
  assert.equal((await executeTest(process.execPath, ['-e', 'process.exit(0)'])).status, 0);
});

test('the watchdog terminates a real non-exiting test process group', async () => {
  let expire;
  let child;
  const execution = executeTest(process.execPath, ['-e',
    'process.stdout.write("ready\\n");setInterval(()=>{},1000)'], {
    launch(executable, args, options) { child = spawn(executable, args, options); return child; },
    arm(callback, milliseconds) { assert.equal(milliseconds, 10_000); expire = callback; return 1; },
    disarm() {},
  });
  try {
    assert.equal(typeof expire, 'function', 'watchdog must own every test execution');
    const rejected = assert.rejects(execution, /debt remains unqualified/);
    await once(child.stdout, 'data');
    const exited = once(child, 'exit');
    expire();
    await rejected;
    const [, signal] = await exited;
    assert.equal(signal, 'SIGKILL');
  } finally {
    try { process.kill(-child.pid, 'SIGKILL'); } catch (error) {
      if (error.code !== 'ESRCH') throw error;
    }
  }
});
