// Run by the native-runtime node_process integration target, which supplies
// the shipped executable from CARGO_BIN_EXE_acyclic-process-host.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {createConnection, createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PassThrough} from 'node:stream';
import {fileURLToPath} from 'node:url';
import {commandCapture, commandProcess} from '@acyclic-labs/sdk/process';
import {fuzz} from './process-completion.fuzz.mjs';

const script = fileURLToPath(import.meta.url);
const mode = process.argv[2];
if (mode === 'echo') {
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
  const input = Buffer.from('stdin is independent of cancellation\nπ\n');
  const captured = await commandCapture(process.execPath, [script, 'echo'], {...options, input});
  assert.equal(captured.stdout, input.toString());
  assert.equal(captured.stderr, 'native stderr π\n');

  const unfinishedInput = new PassThrough();
  const normal = await commandProcess(process.execPath, [script, 'exit'], {...options, input: unfinishedInput});
  assert.equal(normal.code, 17);
  assert.equal(normal.failures.length, 0);
  assert.ok(unfinishedInput.destroyed, 'original exit must close an input producer that has no terminal');

  // The same real native close must preserve synchronous status failures and
  // both terminal failure paths, including work accepted after root exit.
  for (const closeThrows of [false, true]) for (const terminalAsync of [false, true]) {
    const statusFailure = new Error('original status observer failure');
    const terminalFailure = new Error('original terminal observer failure');
    let entered, release, settled = false, closeCalls = 0, terminalCalls = 0;
    const terminalSeen = new Promise(resolve => { entered = resolve; });
    const terminalReleased = new Promise(resolve => { release = resolve; });
    const completed = commandProcess(process.execPath, [script, 'exit'], {
      ...options,
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

  // Capture original wire bytes from the shipped producer for the retained
  // decoder fuzzer. No fixture response or codec model is handwritten.
  const wireDirectory = await mkdtemp(join(tmpdir(), 'process-wire-control-'));
  try {
    const receipt = join(wireDirectory, 'completion');
    const producer = spawn(host, ['--completion', receipt, '--invocation', 'original-wire-control',
      '--inherit-input', '--', process.execPath, script, 'exit'], {stdio: 'ignore'});
    const [code] = await once(producer, 'close');
    assert.equal(code, 17);
    const originalWire = await readFile(receipt);
    fuzz(originalWire);
    // Preserve the producer's exact bytes for the decoder's retained corpus.
    console.log(`PROCESS_COMPLETION_CORPUS_BASE64=${originalWire.toString('base64')}`);
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
