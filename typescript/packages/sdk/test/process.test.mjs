// Run by the native-runtime node_process integration target, which supplies
// the shipped executable from CARGO_BIN_EXE_acyclic-process-host.
import assert from 'node:assert/strict';
import {fork, spawn} from 'node:child_process';
import {once} from 'node:events';
import {subscribe, unsubscribe} from 'node:diagnostics_channel';
import {readFileSync, writeFileSync} from 'node:fs';
import {mkdtemp, open, readFile, rm, writeFile} from 'node:fs/promises';
import {createConnection, createServer} from 'node:net';
import {tmpdir} from 'node:os';
import {join, parse} from 'node:path';
import {PassThrough} from 'node:stream';
import {fileURLToPath} from 'node:url';
import {commandCapture, commandProcess, withProcessSignals} from '@acyclic-labs/sdk/process';
import {fuzz} from './process-completion.fuzz.mjs';

// Simple children execute only their fixture work, without importing this
// control runner or its SDK, fuzz and filesystem dependencies in every child.
const exitArguments = ['-e', 'process.exitCode = 17'];
const echoArguments = ['-e', String.raw`process.stderr.write('native stderr π\n');
process.stdin.pipe(process.stdout);`];
const joinArguments = ['-e', String.raw`process.stdout.write('accepted write\n');
process.stdin.once('data', () => process.stderr.write('sink refusal\n'));
process.stdin.resume();`];

const signalMutations = new Map([
  ['omit-release', ['process.removeListener(name, handler)', 'void handler']],
  ['omit-abort', ['stop.signal.throwIfAborted();', '']],
  ['omit-reason', ['stop.abort(reason(name))',
    'stop.abort(Object.assign(new Error(`process signal ${name}`), {signal: name}))']],
]);

async function processSource() {
  return (await readFile(new URL('../native/process.mjs', import.meta.url), 'utf8')).replace(
    "from './process-completion.mjs'", `from '${new URL('../native/process-completion.mjs', import.meta.url).href}'`);
}

async function statusBindingControl(run, host) {
  let original, originalWire, originalArguments, receipt, corrupted, originalStatus, spawns = 0;
  const spawned = ({process: child}) => {
    spawns++;
    // This listener precedes the bridge's exit listener. The original native
    // owner has retired its tree and closed its receipt before exit; corrupt
    // only its actual status field before the original reader consumes it.
    child.once('exit', (code, signal) => {
      // The diagnostic publishes at construction, before spawnargs exists.
      const at = child.spawnargs.indexOf('--completion');
      assert.ok(at > 0);
      receipt = child.spawnargs[at + 1];
      originalArguments = child.spawnargs.slice(1);
      const bytes = originalWire = readFileSync(receipt);
      original = JSON.parse(bytes);
      originalStatus = {code, signal};
      assert.equal(code >>> 0, original.host_exit_code);
      corrupted = {...original, host_exit_code: (original.host_exit_code + 1) >>> 0};
      const changed = Buffer.from(JSON.stringify(corrupted));
      writeFileSync(receipt, changed);
      console.log(`PROCESS_STATUS_BINDING_CORPUS_BASE64=${Buffer.from(JSON.stringify({
        original: bytes.toString('base64'), corrupted: changed.toString('base64'), originalStatus,
      })).toString('base64')}`);
    });
  };
  subscribe('child_process', spawned);
  let result;
  try { result = await run(process.execPath, exitArguments, {host}); }
  finally { unsubscribe('child_process', spawned); }
  assert.equal(spawns, 1);
  assert.ok(original.process_tree_retired);
  assert.equal(original.root_status.code, 17);
  assert.equal(result.code, null, 'a mismatched native receipt cannot certify root exit');
  assert.deepEqual(result.hostStatus, originalStatus);
  assert.equal(result.failures.length, 1);
  const [{phase, error}] = result.failures;
  assert.equal(phase, 'retirement');
  assert.equal(error.code, 'PROCESS_HOST_STATUS_MISMATCH');
  assert.deepEqual(error.hostStatus, originalStatus);
  assert.deepEqual(error.completion, corrupted);
  return {originalWire, originalArguments, receipt};
}

async function argumentControls(host) {
  const command = ['--', 'node', '-e', 'process.exit(17)'];
  async function capture(cases, label) {
    const records = [];
    for (const [args, expected] of cases) {
      const original = spawn(host, args, {stdio: ['ignore', 'pipe', 'pipe']});
      const stdout = [], stderr = [];
      original.stdout.on('data', bytes => stdout.push(bytes));
      original.stderr.on('data', bytes => stderr.push(bytes));
      const [code, signal] = await once(original, 'close');
      const record = {arguments: args, code, signal,
        stdout: Buffer.concat(stdout).toString('utf8'), stderr: Buffer.concat(stderr).toString('utf8')};
      assert.equal(signal, null);
      assert.equal(code, expected);
      assert.equal(record.stdout, '');
      if (code === 125) assert.match(record.stderr, /step=process-host-arguments;/);
      else assert.equal(record.stderr, '');
      records.push(record);
    }
    console.log(`${label}=${Buffer.from(JSON.stringify(records)).toString('base64')}`);
  }
  // Preserve the original portable records byte for byte. Endpoint records
  // below retain the actual platform's paths separately, without rewriting.
  await capture([[[], 125], [['--'], 125], [['--inherit-input', ...command], 17]],
    'PROCESS_HOST_PORTABLE_CORPUS_BASE64');
  // Every case refuses before any endpoint can be opened. Its spelling needs
  // only the platform's volume root, not an allocated file or private path.
  const path = join(parse(tmpdir()).root, 'acyclic-process-argument');
  const refused = [];
  for (const option of ['--completion', '--control', '--custody', '--lease']) {
    refused.push([option], [option, 'relative', ...command], [option, path],
      [option, path, option, path, ...command]);
  }
  for (const binding of ['', ' ', 'π']) refused.push(['--invocation', binding, ...command]);
  refused.push(['--invocation'], ['--invocation', 'valid_ID-17', ...command],
    ['--invocation', 'valid_ID-17'],
    ['--invocation', 'valid_ID-17', '--invocation', 'valid_ID-17', ...command],
    ['--inherit-input', '--inherit-input', ...command],
    ['--custody', path, '--lease', path, ...command], ['--unknown', ...command], ['--', '']);
  await capture(refused.map(args => [args, 125]), 'PROCESS_HOST_ENDPOINT_CORPUS_BASE64');
}

async function captureCancellationControl(run, host) {
  const stop = new AbortController(), input = new PassThrough();
  const reason = new Error('original capture cancellation');
  const observed = [];
  let spawns = 0;
  // Observe bytes on the original host pipe before its async capture accepts
  // them. Cancellation must not discard that already-written diagnostic.
  const spawned = ({process}) => {
    spawns++;
    process.once('spawn', () => {
      process.stdout.on('data', bytes => { observed.push(bytes); stop.abort(reason); });
    });
  };
  subscribe('child_process', spawned);
  try {
    await assert.rejects(run(process.execPath, [script, 'tree'], {
      host, input, signal: stop.signal,
    }), error => {
      const causes = error instanceof AggregateError ? error.errors : [error];
      assert.ok(causes.includes(reason), 'retain the original cancellation cause');
      assert.ok(observed.length > 0, 'the original child must have written its diagnostic');
      assert.equal(error.stdout, Buffer.concat(observed).toString('utf8'),
        'capture must retain original bytes already written before cancellation');
      return true;
    });
    assert.equal(spawns, 1, 'capture must keep the original native owner');
    assert.ok(input.destroyed, 'physical retirement must close unfinished input');
  } finally {
    unsubscribe('child_process', spawned);
    input.destroy();
  }
}

async function descriptorControl(host, input, original) {
  const directory = await mkdtemp(join(tmpdir(), 'process-descriptors-'));
  let stdout, stderr;
  try {
    stdout = await open(join(directory, 'stdout'), 'wx', 0o600);
    stderr = await open(join(directory, 'stderr'), 'wx', 0o600);
    for (const forwarded of [1, 2, 3]) {
      const observed = {stdout: [], stderr: []};
      const result = await commandProcess(process.execPath, echoArguments, {
        host, input: forwarded === 3 ? undefined : input, signal: AbortSignal.timeout(2000),
        stdio: ['pipe', forwarded & 1 ? stdout.fd : 'pipe', forwarded & 2 ? stderr.fd : 'pipe'],
        onOutput: (stream, bytes) => { observed[stream].push(bytes); },
      });
      assert.equal(result.code, 0);
      assert.deepEqual(result.failures, []);
      for (const [stream, bit] of [['stdout', 1], ['stderr', 2]]) {
        assert.equal(Buffer.concat(observed[stream]).toString('utf8'), forwarded & bit ? '' : original[stream],
          'forwarded descriptors must bypass capture and preserve the other original pipe');
      }
    }
    for (const stream of ['stdout', 'stderr']) {
      assert.equal(await readFile(join(directory, stream), 'utf8'), original[stream].repeat(stream === 'stdout' ? 1 : 2),
        'the original native child must write through the caller descriptor');
    }
  } finally {
    await Promise.all([stdout?.close(), stderr?.close()]);
    await rm(directory, {recursive: true});
  }
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
    const completed = run(process.execPath, exitArguments, {
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

async function exitBeforeOutputControl(run, host) {
  // The deployment consumer publishes the original nonzero terminal before
  // releasing its accepted last append. Use its actual child/input ordering
  // and a real read-only descriptor, rather than a synthetic sink refusal.
  const directory = await mkdtemp(join(tmpdir(), 'process-last-append-'));
  const path = join(directory, 'output');
  await writeFile(path, 'original output', {flag: 'wx'});
  const file = await open(path, 'r');
  const input = new PassThrough();
  const released = Promise.withResolvers();
  let terminal, outputFailure, status, completed;
  try {
    completed = run(process.execPath, ['-e',
      'process.stdout.write("held last append");process.stdin.once("data",()=>process.exit(23));'], {
      host, input, signal: AbortSignal.timeout(2000),
      async onOutput(_stream, bytes) {
        input.end('exit');
        await released.promise;
        try { await file.writeFile(bytes); }
        catch (error) { outputFailure = error; throw error; }
      },
      onTerminal(observed) { terminal = observed; released.resolve(); },
      onClose({code}) { status = code; },
    });
    const result = await completed;
    assert.equal(terminal.exitCode, 23,
      'the original nonzero exit must release its accepted last append before any deadline');
    assert.equal(status, 23);
    assert.equal(outputFailure?.code, 'EBADF');
    assert.ok(result.failures.some(({error}) => error === outputFailure));
    assert.equal(result.failures.some(({phase}) => phase === 'abort'), false);
    assert.ok(result.completion.process_tree_retired);
    assert.equal(await readFile(path, 'utf8'), 'original output');
  } finally {
    released.resolve();
    input.destroy();
    try { await completed; }
    finally {
      await file.close();
      await rm(directory, {recursive: true, force: true});
    }
  }
}

async function treeControl(run, host, interruption) {
  const stop = new AbortController(), input = new PassThrough();
  const inputClosed = once(input, 'close');
  const reason = new Error('original caller cancellation');
  let peer, peerClosed, peerFailure, pending = '', originalHost, spawns = 0;
  // Observe Node's original ChildProcess; no replacement spawn or host is used.
  const spawned = ({process}) => { originalHost = process; spawns++; };
  subscribe('child_process', spawned);
  let result;
  const execute = async signal => {
    result = await run(process.execPath, [script, 'tree'], {
      host, signal, input,
      async onOutput(stream, bytes) {
        if (stream !== 'stdout' || peer) return;
        pending += bytes.toString('utf8');
        if (!pending.endsWith('\n')) return;
        peer = createConnection({host: '127.0.0.1', port: Number(pending.trim())});
        peer.on('error', error => { peerFailure = error; });
        peerClosed = new Promise(resolve => peer.once('close', resolve));
        const accepted = peer.iterator({destroyOnReturn: false});
        const first = await accepted.next();
        await accepted.return();
        if (first.done) throw new Error('step=descendant-accept; recovery=inspect the original descendant exit before retrying');
        peer.resume(); // Observe the descendant's EOF; never close it here.
        assert.equal(spawns, 1);
        assert.equal(originalHost.spawnfile, host);
        if (interruption === 'caller') stop.abort(reason);
        else if (interruption === 'host-loss') {
          assert.ok(originalHost.kill('SIGKILL'), 'the original host must still own this live tree');
        } else {
          // The owned OS signal cancels this same accepted native tree. Keep
          // the output callback pending until its original input is closed.
          process.kill(process.pid, interruption);
          await inputClosed;
        }
      },
    });
  };
  let signalReason;
  try {
    if (interruption.startsWith('SIG')) {
      await assert.rejects(withProcessSignals(execute), error => {
        signalReason = error;
        return error.signal === interruption;
      });
    } else await execute(stop.signal);
  } finally { unsubscribe('child_process', spawned); }
  await peerClosed; // Forced native retirement can produce EOF or a TCP reset.
  assert.ok(!peerFailure || peerFailure.code === 'ECONNRESET',
    `original descendant connection failed: ${peerFailure}`);
  assert.ok(peer, 'the descendant must actually have accepted a connection');
  assert.ok(input.destroyed);
  if (interruption !== 'host-loss') {
    assert.ok(result.completion.cancelled);
    assert.ok(result.completion.process_tree_retired);
    assert.ok(result.failures.some(failure => failure.phase === 'abort'
      && failure.error === (signalReason ?? reason)));
  } else {
    assert.equal(result.code, null, 'host loss cannot certify the selected root status');
    assert.ok(result.hostStatus.signal || result.hostStatus.code !== 0);
    assert.ok(result.failures.some(({phase, error}) => phase === 'retirement'
      && ['PROCESS_COMPLETION_INVALID', 'ENOENT', 'PROCESS_HOST_STATUS_MISMATCH'].includes(error.code)),
    'loss of the original terminal producer must retain its typed failure');
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
    for (const customReason of [false, true]) for (const signal of ['SIGINT', 'SIGTERM']) {
      const armed = await Promise.race([
        once(child, 'message').then(([message]) => message),
        closed.then(() => null),
      ]);
      assert.deepEqual(armed, {signal, customReason},
        `original signal owner must arm before delivery: ${Buffer.concat(diagnostics)}`);
      assert.ok(child.kill(signal));
    }
    assert.deepEqual(await closed, [0, null], Buffer.concat(diagnostics).toString());
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await closed;
  }
}

async function processSignalControls() {
  // A caller-owned signal delegates cancellation without borrowing process
  // handlers, even when already aborted. The action owns its original result.
  const scopeOutcomes = async run => {
    const before = ['SIGINT', 'SIGTERM'].map(name => process.listeners(name));
    for (const ownership of ['caller', 'aborted-caller', 'process']) for (const fails of [false, true]) {
      const stop = new AbortController(), cause = new Error('original caller signal');
      if (ownership === 'aborted-caller') stop.abort(cause);
      let release;
      const pending = new Promise(resolve => { release = resolve; });
      const action = signal => {
        if (ownership !== 'process') {
          assert.equal(signal, stop.signal, 'the action must retain its original caller signal');
          assert.deepEqual(['SIGINT', 'SIGTERM'].map(name => process.listeners(name)), before,
            'caller-owned cancellation must not borrow process handlers');
        }
        if (fails) throw cause;
        return pending.then(() => cause);
      };
      const completed = run(action, {signal: ownership === 'process' ? undefined : stop.signal});
      const outcome = fails ? assert.rejects(completed, error => error === cause)
        : completed.then(value => assert.equal(value, cause));
      try {
        if (ownership === 'process' && !fails) {
          assert.deepEqual(['SIGINT', 'SIGTERM'].map(name => process.listeners(name).length),
            before.map(listeners => listeners.length + 1),
            'pending action must retain its owned signal handlers');
        }
      } finally { release(); await outcome; }
      assert.deepEqual(['SIGINT', 'SIGTERM'].map(name => process.listeners(name)), before,
        'every original result must release its owned signal handlers');
    }
  };
  await scopeOutcomes(withProcessSignals);
  const production = await processSource();
  const planted = production.replace('if (signal) return action(signal);', '');
  assert.notEqual(planted, production, 'production caller-signal delegation must exist');
  const {withProcessSignals: mutant} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`);
  await assert.rejects(() => scopeOutcomes(mutant), {
    code: 'ERR_ASSERTION', message: /the action must retain its original caller signal/,
  });
  const unjoined = production.replace('await action(stop.signal)', 'action(stop.signal)');
  assert.notEqual(unjoined, production, 'production signal scope must await its action');
  const {withProcessSignals: earlyRelease} = await import(`data:text/javascript;base64,${Buffer.from(unjoined).toString('base64')}`);
  await assert.rejects(() => scopeOutcomes(earlyRelease), {
    code: 'ERR_ASSERTION', message: /pending action must retain its owned signal handlers/,
  });
  if (process.platform !== 'win32') {
    await signalControls('original');
    for (const mutation of signalMutations.keys()) {
      await assert.rejects(() => signalControls(mutation), {code: 'ERR_ASSERTION'}, mutation);
    }
  }
}

const script = fileURLToPath(import.meta.url);
const mode = process.argv[2];
if (mode === 'signal-owner') {
  let run = withProcessSignals;
  if (signalMutations.has(process.argv[3])) {
    const original = await processSource();
    const [guard, replacement] = signalMutations.get(process.argv[3]);
    const planted = original.replace(guard, replacement);
    assert.notEqual(planted, original, `production signal guard must exist: ${guard}`);
    ({withProcessSignals: run} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`));
  }
  try {
    const before = ['SIGINT', 'SIGTERM'].map(name => process.listeners(name));
    for (const customReason of [false, true]) for (const name of ['SIGINT', 'SIGTERM']) {
      const reason = new Error('original interruption diagnostic');
      await assert.rejects(run(signal => new Promise((resolve, reject) => {
        const lost = () => reject(new Error('step=signal-producer; recovery=retain the original controlling process until signal delivery'));
        process.once('disconnect', lost);
        signal.addEventListener('abort', () => {
          process.removeListener('disconnect', lost);
          resolve();
        }, {once: true});
        process.send({signal: name, customReason}, error => { if (error) reject(error); });
      }), {reason: customReason ? () => reason : undefined}),
      error => customReason ? error === reason : error.signal === name);
      assert.deepEqual(['SIGINT', 'SIGTERM'].map(signal => process.listeners(signal)), before,
        'completion must release exactly its original signal handlers');
    }
  } finally { process.disconnect(); }
} else if (mode === 'signals') {
  await processSignalControls();
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
} else {
  const host = process.env.ACYCLIC_PROCESS_HOST;
  assert.ok(host, 'step=process-control-admission; recovery=run cargo test --locked -p acyclic-native-runtime --test node_process');
  const options = {host};
  await processSignalControls();
  await argumentControls(host);
  const input = Buffer.from('stdin is independent of cancellation\nπ\n');
  const captured = await commandCapture(process.execPath, echoArguments, {...options, input});
  assert.equal(captured.stdout, input.toString());
  assert.equal(captured.stderr, 'native stderr π\n');
  await captureCancellationControl(commandCapture, host);
  await descriptorControl(host, input, captured);

  const unfinishedInput = new PassThrough();
  const normal = await commandProcess(process.execPath, exitArguments, {...options, input: unfinishedInput});
  assert.equal(normal.code, 17);
  assert.equal(normal.failures.length, 0);
  assert.ok(unfinishedInput.destroyed, 'original exit must close an input producer that has no terminal');

  await terminalControls(commandProcess, host);
  const {originalWire, originalArguments, receipt} = await statusBindingControl(commandProcess, host);
  // Reuse the original producer bytes captured before status corruption.
  fuzz(originalWire);
  console.log(`PROCESS_COMPLETION_CORPUS_BASE64=${originalWire.toString('base64')}`);
  console.log(`PROCESS_HOST_ARGUMENTS_BASE64=${Buffer.from(JSON.stringify(originalArguments)).toString('base64')}`);
  await exitBeforeOutputControl(commandProcess, host);
  // These are omissions in the actual shipped source. The identical public
  // topology/oracles must reject lost terminal failures and receipt binding.
  const production = await processSource();
  for (const [guard, replacement, control = terminalControls] of [
    [".catch(error => retain('terminal', error))", '.catch(() => {})'],
    ["} catch (error) { retain('terminal', error); }", '} catch (error) {}'],
    ["decodeCompletion(await readFile(receipt, 'utf8'), invocation, hostStatus)",
      "decodeCompletion(await readFile(receipt, 'utf8'), invocation)", statusBindingControl],
  ]) {
    const planted = production.replace(guard, replacement);
    assert.notEqual(planted, production, `production process guard missing: ${guard}`);
    const {commandProcess: mutant} = await import(`data:text/javascript;base64,${Buffer.from(planted).toString('base64')}`);
    await assert.rejects(() => control(mutant, host), {code: 'ERR_ASSERTION'}, guard);
  }
  const missing = await commandProcess(join(receipt, 'missing-executable'), [], options);
  assert.equal(missing.completion.root_status, null);
  assert.equal(missing.completion.process_tree_retired, false);
  assert.equal(missing.completion.failure.phase, 'root-admission');
  assert.equal(missing.completion.failure.error.kind, 'NotFound');
  assert.ok(missing.failures.some(failure => failure.phase === 'retirement'));

  await treeControl(commandProcess, host, 'caller');
  if (process.platform !== 'win32') {
    for (const signal of ['SIGINT', 'SIGTERM']) await treeControl(commandProcess, host, signal);
  }
  await treeControl(commandProcess, host, 'host-loss');
  const lostFailure = production.replace("retain('retirement', error);", '');
  assert.notEqual(lostFailure, production, 'production retirement failure retention must exist');
  const {commandProcess: lostProducer} = await import(`data:text/javascript;base64,${Buffer.from(lostFailure).toString('base64')}`);
  await assert.rejects(() => treeControl(lostProducer, host, 'host-loss'), {
    code: 'ERR_ASSERTION', message: /must retain its typed failure/,
  });

  // Sink failure cancels the same physical owner, yet both accepted callbacks
  // finish before the caller receives its original cause.
  const sinkFailure = new Error('original sink failure');
  const stdoutFailure = new Error('original accepted stdout sink failure');
  const inputGate = new PassThrough();
  const inputClosed = once(inputGate, 'close');
  let writeStarted = false, settled = false, terminalCalls = 0;
  let release;
  const acceptedWrite = new Promise(resolve => { release = resolve; });
  const failed = commandProcess(process.execPath, joinArguments, {
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
  await assert.rejects(commandCapture(process.execPath, echoArguments, {...options, input, maxBuffer: 0}),
    error => error.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER'
      || error instanceof AggregateError && error.errors.some(cause => cause.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER'));
  console.log('real native process bridge controls passed');
}
