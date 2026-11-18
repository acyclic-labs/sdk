import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { PassThrough } from 'node:stream';
import { testArguments, verify, testExecutable, executeTest } from './qualify-main-green-debts.mjs';

const entry = { target: '//rust/crates/harness-local:acyclic_harness_local_test_cases', regression: 'module::regression', controls: ['module::negative'] };
const success = 'test module::regression ... ok\ntest module::negative - should panic ... ok\n\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 41 filtered out; finished in 0.12s\n';

test('the checked-in plan names each debt once with its class and structural fix', async () => {
  const entries = JSON.parse(await readFile(new URL('./main-green-debts.json', import.meta.url), 'utf8'));
  assert.ok(entries.length > 0);
  assert.equal(new Set(entries.map(item => item.regression)).size, entries.length);
  for (const item of entries) {
    assert.ok(item.class.length > 0);
    assert.match(item.fixed_by, /^[0-9a-f]{9}(,[0-9a-f]{9})*$/);
    testArguments(item);
  }
});

test('regression and negative control are exact named tests in one package', () => {
  assert.deepEqual(testArguments(entry), ['module::regression', 'module::negative', '--exact', '--test-threads=4', '--color=never']);
  assert.equal(verify(entry, 0, success).seconds, 0.12);
});

test('a debt requires a regression, a negative control and literal test names', () => {
  for (const invalid of [{ ...entry, controls: [] }, { ...entry, regression: '' },
    { ...entry, controls: ['--skip'] }, { ...entry, target: '--all-targets' },
    { ...entry, target: undefined }, { ...entry, controls: 'module::negative' }]) {
    assert.throws(() => testArguments(invalid));
  }
});

test('the queried target must supply exactly one executable under the execution root', () => {
  const root = resolve('execroot');
  const file = 'bazel-out/k8-fastbuild/bin/rust/crates/harness-local/acyclic_harness_local_test_cases';
  assert.equal(testExecutable(root, file + '\n'), resolve(root, file));
  for (const [directory, output] of [['execroot', file], [root, ''], [root, file + '\n' + file],
    [root, '/another/executable'], [root, 'bazel-out/../other']]) {
    assert.throws(() => testExecutable(directory, output), /selected test executable/);
  }
});

test('a completed executable preserves its output and failure status', async () => {
  const result = await executeTest(process.execPath, ['-e', 'console.log("fixture output");process.exit(3)']);
  assert.equal(result.status, 3);
  assert.equal(result.output.trim(), 'fixture output');
});

test('leader exit retires its real live descendant before output closure', async () => {
 for (const inheritedOutput of [false, true]) {
  const root = mkdtempSync(join(tmpdir(), 'qualification-descendant-'));
  const identity = join(root, 'descendant.pid');
  let group;
  let descendant;
  let exited;
  let stopped = 0;
  const source = `
    const {spawn} = require('node:child_process');
    const child = spawn('python3', ['-c',
      'import os, signal; os.write(3, str(os.getpid()).encode()); signal.pause()'],
      {stdio: ['ignore', '${inheritedOutput ? 'inherit' : 'ignore'}', 'ignore', 'pipe']});
    child.stdio[3].once('data', pid => {
      require('node:fs').writeFileSync(process.argv[1], pid);
      child.stdio[3].destroy();
      child.unref();
      console.log(pid.toString());
    });
  `;
  const execution = executeTest(process.execPath, ['-e', source, identity], {
    launch(executable, args, options) {
      const child = spawn(executable, args, options);
      group = child.pid;
      exited = once(child, 'exit');
      return child;
    },
    terminate(child) {
      assert.ok(child.pid !== undefined, 'descendant retirement: original child PID missing; rerun node --test scripts/qualify-main-green-debts.test.mjs');
      assert.equal(child.pid, group);
      descendant = Number(readFileSync(identity, 'utf8'));
      assert.ok(Number.isSafeInteger(descendant) && descendant > 0);
      assert.notEqual(descendant, group);
      process.kill(descendant, 0); // The parent passed while this child lives.
      process.kill(-child.pid, 'SIGKILL');
      stopped += 1;
    },
  });
  execution.catch(() => {});
  try {
    await exited;
    // Restoring close-only retirement fails here before any inherited-pipe wait.
    assert.equal(stopped, 1, 'original leader exit must retire its owned group');
    const result = await execution;
    assert.equal(result.status, 0);
    assert.equal(Number(result.output.trim()), descendant);
    assert.equal(stopped, 1, 'completion must end the owned group before qualifying');
  } finally {
    if (group !== undefined) {
      try { process.kill(-group, 'SIGKILL'); } catch (error) {
        if (error.code !== 'ESRCH' && error.code !== 'EPERM') throw error;
      }
    }
    await execution.catch(() => {});
    rmSync(root, { recursive: true, force: true });
  }
 }
});

test('failed spawn and process-group cleanup failure cannot qualify', async () => {
  await assert.rejects(executeTest('/definitely-absent-main-green-executable', []), { code: 'ENOENT' });
  const child = Object.assign(new EventEmitter(), {
    stdout: new PassThrough(), stderr: new PassThrough(), pid: 123,
  });
  const execution = executeTest('/fixture/test', [], {
    launch: () => child,
    terminate() { throw Object.assign(new Error('group cleanup refused'), { code: 'EPERM' }); },
  });
  child.emit('close', 0);
  await assert.rejects(execution, /group cleanup refused/);
  assert.ok(child.stdout.destroyed && child.stderr.destroyed);
});
