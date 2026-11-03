// Run by the native-runtime node_process integration target, which supplies
// the shipped executable from CARGO_BIN_EXE_acyclic-process-host.
import assert from 'node:assert/strict';
import {fork, spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {createConnection, createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PassThrough} from 'node:stream';
import {fileURLToPath} from 'node:url';
import {commandCapture, commandProcess, withProcessSignals} from '@acyclic-labs/sdk/process';
import {fuzz} from './process-completion.fuzz.mjs';

async function processSource() {
  return (await readFile(new URL('../native/process.mjs', import.meta.url), 'utf8')).replace(
    "from './process-completion.mjs'", `from '${new URL('../native/process-completion.mjs', import.meta.url).href}'`);
}

async function terminalControls(run, host) {
  // The same real native close must preserve synchronous status failures and
  // both terminal failure paths, including work accepted after root exit.
  for (const closeThrows of [false, true]) for (const terminalAsync of [false, true]) {
    const statusFailure = new Error('original status observer failure');
    const terminalFailure = new Error('original terminal observer failure');
    let entered, release, settled = false, closeCalls = 0, terminalCalls = 0;
    const terminalSeen = new Promise(resolve => { entered = resolve; });
    const terminalReleased = new Promise(resolve => { release = resolve; });
    const completed = run(process.execPath, [script, 'exit'], {
      host,
      onClose(result) {
        closeCalls++;
        assert.equal(result.code, 17);
        if (closeThrows) throw statusFailure;
      },
      onTerminal({exitCode}) {
        terminalCalls++;
        assert.equal(exitCode, 17);
        entered();
        if (!terminalAsync) throw terminalFailure;
        return terminalReleased.then(() => { throw terminalFailure; });
      },
    }).then(result => { settled = true; return result; });
    try {
      const reached = await Promise.race([
        terminalSeen.then(() => true), completed.then(() => false),
      ]);
      assert.ok(reached, 'the original native exit must report its terminal');
      if (terminalAsync) assert.equal(settled, false, 'accepted terminal work owns completion');
    } finally { release(); await completed; }
    const result = await completed;
    assert.equal(closeCalls, 1);
    assert.equal(terminalCalls, 1);
    assert.equal(result.code, 17);
    assert.ok(result.completion.process_tree_retired);
    assert.equal(result.failures.length, closeThrows ? 2 : 1);
    assert.equal(result.failures.find(({phase}) => phase === 'terminal')?.error, terminalFailure);
    assert.equal(result.failures.find(({phase}) => phase === 'status')?.error,
      closeThrows ? statusFailure : undefined);
  }
}

async function signalControls(mutation) {
  const child = fork(script, ['signal-owner', mutation], {stdio: ['ignore', 'inherit', 'pipe', 'ipc']});
  const diagnostics = [];
  child.stderr.on('data', bytes => diagnostics.push(bytes));
  const closed = once(child, 'close');
  try {
    // Each original IPC event names the handler that is now armed. Physical
    // child close also ends the wait if that producer disappears.
    for (const signal of ['SIGINT', 'SIGTERM']) {
      const armed = await Promise.race([
        once(child, 'message').then(([message]) => message),
        closed.then(() => null),
      ]);
      assert.equal(armed, signal, `original signal owner must arm before delivery: ${Buffer.concat(diagnostics)}`);
      assert.ok(child.kill(signal));
    }
    assert.deepEqual(await closed, [0, null], Buffer.concat(diagnostics).toString());
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await closed;
  }
}

async function processSignalControls() {
  if (process.platform !== 'win32') {
    await signalControls('original');
    await assert.rejects(() => signalControls('omit-release'), {
      code: 'ERR_ASSERTION', message: /completion must release exactly its original signal handlers/,
    });
  }
}

const script = fileURLToPath(import.meta.url);
const mode = process.argv[2];
if (mode === 'signal-owner') {
  let run = withProcessSignals;
  if (process.argv[3] === 'omit-release') {
    const original = await processSource();
    const planted = original.replace('process.removeListener(name, handler)', 'void handler');
    assert.notEqual(planted, original, 'production signal release must exist');
    ({withProcessSignals: run} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`));
  }
  try {
    const before = ['SIGINT', 'SIGTERM'].map(name => process.listeners(name));
    for (const name of ['SIGINT', 'SIGTERM']) {
      await assert.rejects(run(signal => new Promise((resolve, reject) => {
        const lost = () => reject(new Error('step=signal-producer; recovery=retain the original controlling process until signal delivery'));
        process.once('disconnect', lost);
        signal.addEventListener('abort', () => {
          process.removeListener('disconnect', lost);
          resolve();
        }, {once: true});
        process.send(name, error => { if (error) reject(error); });
      })), error => error.signal === name);
      assert.deepEqual(['SIGINT', 'SIGTERM'].map(signal => process.listeners(signal)), before,
        'completion must release exactly its original signal handlers');
    }
  } finally { process.disconnect(); }
} else if (mode === 'signals') {
  await processSignalControls();
} else if (mode === 'echo') {
  process.stderr.write('native stderr π\n');
  process.stdin.pipe(process.stdout);
} else if (mode === 'descendant') {
  const server = createServer(socket => socket.write('accepted\n'));
  server.listen(0, '127.0.0.1', () => process.stdout.write(`${server.address().port}\n`));
} else if (mode === 'tree') {
  const descendant = spawn(process.execPath, [script, 'descendant'], {stdio: ['ignore', 'pipe', 'inherit']});
  descendant.once('error', error => {
    process.stderr.write(`step=descendant-admission; recovery=run the original Node executable: ${error}\n`);
    process.exitCode = 1;
    process.stdin.destroy();
  });
  descendant.once('exit', code => { process.exitCode = code ?? 1; process.stdin.destroy(); });
  descendant.stdout.pipe(process.stdout);
  process.stdin.resume();
} else if (mode === 'join') {
  process.stdout.write('accepted write\n');
  process.stdin.once('data', () => process.stderr.write('sink refusal\n'));
  process.stdin.resume();
} else if (mode === 'exit') {
  process.exitCode = 17;
} else {
  const host = process.env.ACYCLIC_PROCESS_HOST;
  assert.ok(host, 'step=process-control-admission; recovery=run cargo test --locked -p acyclic-native-runtime --test node_process');
  const options = {host};
  await processSignalControls();
  const input = Buffer.from('stdin is independent of cancellation\nπ\n');
  const captured = await commandCapture(process.execPath, [script, 'echo'], {...options, input});
  assert.equal(captured.stdout, input.toString());
  assert.equal(captured.stderr, 'native stderr π\n');

  const unfinishedInput = new PassThrough();
  const normal = await commandProcess(process.execPath, [script, 'exit'], {...options, input: unfinishedInput});
  assert.equal(normal.code, 17);
  assert.equal(normal.failures.length, 0);
  assert.ok(unfinishedInput.destroyed, 'original exit must close an input producer that has no terminal');

  await terminalControls(commandProcess, host);
  // These are omissions in the actual shipped source. The identical public
  // topology/oracle must reject both ways terminal failures can be lost.
  const production = await processSource();
  for (const [guard, replacement] of [
    [".catch(error => retain('terminal', error))", '.catch(() => {})'],
    ["} catch (error) { retain('terminal', error); }", '} catch (error) {}'],
  ]) {
    const planted = production.replace(guard, replacement);
    assert.notEqual(planted, production, `production terminal guard missing: ${guard}`);
    const {commandProcess: mutant} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`);
    await assert.rejects(() => terminalControls(mutant, host), {code: 'ERR_ASSERTION'}, guard);
  }
  // Capture original wire bytes from the shipped producer for the retained
  // decoder fuzzer. No fixture response or codec model is handwritten.
  const wireDirectory = await mkdtemp(join(tmpdir(), 'process-wire-control-'));
  try {
    const receipt = join(wireDirectory, 'completion');
    const originalArguments = ['--completion', receipt, '--invocation', 'original-wire-control',
      '--inherit-input', '--', process.execPath, script, 'exit'];
    const producer = spawn(host, originalArguments, {stdio: 'ignore'});
    const [code] = await once(producer, 'close');
    assert.equal(code, 17);
    const originalWire = await readFile(receipt);
    fuzz(originalWire);
    // Preserve the producer's exact bytes for the decoder's retained corpus.
    console.log(`PROCESS_COMPLETION_CORPUS_BASE64=${originalWire.toString('base64')}`);
    console.log(`PROCESS_HOST_ARGUMENTS_BASE64=${Buffer.from(JSON.stringify(originalArguments)).toString('base64')}`);
  } finally { await rm(wireDirectory, {recursive: true, force: true}); }
  const missing = await commandProcess(join(wireDirectory, 'missing-executable'), [], options);
  assert.equal(missing.completion.root_status, null);
  assert.equal(missing.completion.process_tree_retired, false);
  assert.equal(missing.completion.failure.phase, 'root-admission');
  assert.equal(missing.completion.failure.error.kind, 'NotFound');
  assert.ok(missing.failures.some(failure => failure.phase === 'retirement'));

  const stop = new AbortController(), openInput = new PassThrough();
  const reason = new Error('original caller cancellation');
  let peer, peerClosed, pending = '';
  const cancelled = await commandProcess(process.execPath, [script, 'tree'], {
    ...options, signal: stop.signal, input: openInput,
    async onOutput(stream, bytes) {
      if (stream !== 'stdout' || peer) return;
      pending += bytes.toString('utf8');
      if (!pending.endsWith('\n')) return;
      peer = createConnection({host: '127.0.0.1', port: Number(pending.trim())});
      peerClosed = once(peer, 'close');
      const accepted = peer.iterator({destroyOnReturn: false});
      const first = await accepted.next();
      await accepted.return();
      if (first.done) throw new Error('step=descendant-accept; recovery=inspect the original descendant exit before retrying');
      peer.resume(); // Observe EOF from the original descendant, never destroy it here.
      stop.abort(reason);
    },
  });
  await peerClosed; // The original descendant's OS socket closes on retirement.
  assert.ok(peer, 'the descendant must actually have accepted a connection');
  assert.ok(cancelled.completion.cancelled);
  assert.ok(cancelled.completion.process_tree_retired);
  assert.ok(cancelled.failures.some(failure => failure.phase === 'abort' && failure.error === reason));
  assert.ok(openInput.destroyed);

  // Sink failure cancels the same physical owner, yet both accepted callbacks
  // finish before the caller receives its original cause.
  const sinkFailure = new Error('original sink failure');
  const stdoutFailure = new Error('original accepted stdout sink failure');
  const inputGate = new PassThrough();
  const inputClosed = once(inputGate, 'close');
  let writeStarted = false, settled = false, terminalCalls = 0;
  let release;
  const acceptedWrite = new Promise(resolve => { release = resolve; });
  const failed = commandProcess(process.execPath, [script, 'join'], {
    ...options, input: inputGate,
    async onOutput(stream) {
      if (stream === 'stderr') throw sinkFailure;
      writeStarted = true;
      inputGate.write('produce the original stderr failure\n');
      await acceptedWrite;
      throw stdoutFailure;
    },
    onTerminal() { terminalCalls++; },
  }).then(result => { settled = true; return result; });
  try {
    // Refusal or original host exit closes command input. Use that physical
    // event to release accepted work; do not depend on the callback we test.
    await Promise.race([inputClosed, failed]);
    assert.ok(writeStarted);
    assert.equal(settled, false, 'the accepted write still owns completion');
  } finally { release(); await failed; }
  const sinkResult = await failed;
  assert.ok(sinkResult.failures.some(failure => failure.error === sinkFailure));
  assert.ok(sinkResult.failures.some(failure => failure.error === stdoutFailure));
  assert.equal(terminalCalls, 1);
  assert.ok(sinkResult.completion.process_tree_retired);
  await assert.rejects(commandCapture(process.execPath, [script, 'echo'], {...options, input, maxBuffer: 0}),
    error => error.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER'
      || error instanceof AggregateError && error.errors.some(cause => cause.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER'));
  console.log('real native process bridge controls passed');
}
