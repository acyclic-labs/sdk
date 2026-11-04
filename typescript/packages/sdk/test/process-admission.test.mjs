import assert from 'node:assert/strict';
import {mkdtemp, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PassThrough} from 'node:stream';
import test from 'node:test';
import {commandCapture, commandProcess} from '@acyclic-labs/sdk/process';

test('missing native host retains OS causes and joins accepted terminal work', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'process-admission-'));
  const host = join(directory, 'missing-host');
  try {
    for (const stdio of ['ipc', ['pipe', 'ipc', 'pipe'], ['pipe', 'pipe', 'pipe', 'pipe']]) {
      await assert.rejects(commandProcess(process.execPath, ['--version'], {host, stdio}), error => {
        assert.ok(error instanceof TypeError);
        assert.match(error.message, /^step=process-stdio-admission; recovery=/);
        return true;
      });
    }
    const input = new PassThrough();
    let entered, release, terminalCause, terminalCalls = 0, outputCalls = 0, settled = false;
    const terminalSeen = new Promise(resolve => { entered = resolve; });
    const terminalReleased = new Promise(resolve => { release = resolve; });
    const completed = commandProcess(process.execPath, ['--version'], {
      host, input,
      onOutput() { outputCalls++; },
      async onTerminal({cause}) {
        terminalCalls++;
        terminalCause = cause;
        entered();
        await terminalReleased;
      },
    }).then(result => { settled = true; return result; });
    try {
      // Original operation completion also ends this wait if its terminal
      // producer disappears; a missing callback cannot strand this assertion.
      const reached = await Promise.race([
        terminalSeen.then(() => true), completed.then(() => false),
      ]);
      assert.ok(reached, 'the original launch error must report its terminal');
      assert.equal(settled, false, 'accepted terminal work still owns completion');
      assert.ok(input.destroyed, 'launch refusal must close never-ended command input');
    } finally { release(); await completed; }
    const result = await completed;
    const original = result.failures.find(({phase}) => phase === 'spawn')?.error;
    assert.ok(original instanceof Error);
    assert.equal(original.code, 'ENOENT');
    assert.equal(original.path, host);
    assert.equal(terminalCause, original.message);
    assert.equal(terminalCalls, 1);
    assert.equal(outputCalls, 0, 'a refused host cannot produce command output');
    assert.equal(result.code, null, 'failed host admission has no selected root exit');
    const captureInput = new PassThrough();
    await assert.rejects(commandCapture(process.execPath, ['--version'], {
      host, input: captureInput, signal: new AbortController().signal,
    }), error => {
      const causes = error instanceof AggregateError ? error.errors : [error];
      return causes.some(cause => cause.code === 'ENOENT' && cause.path === host);
    });
    assert.ok(captureInput.destroyed);
  } finally { await rm(directory, {recursive: true, force: true}); }
});
