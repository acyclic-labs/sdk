import {spawn} from 'node:child_process';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {createServer} from 'node:net';
import {constants, tmpdir} from 'node:os';
import {isAbsolute, join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {decodeCompletion} from './process-completion.mjs';

const signalNames = new Map(Object.entries(constants.signals).map(([name, number]) => [number, name]));

/** Run under the SDK's native process-tree owner. The caller supplies the
 * installed acyclic-process-host executable, cancellation and output policy.
 * Cancellation closes its independent control channel; native custody kills
 * and reaps descendants. No Node PID/group signalling or fallback exists.
 * Both output sinks and the original physical close settle before returning.
 * onOutput owns each accepted chunk until its returned promise settles.
 */
export async function commandProcess(program, args, {host, signal, input, env, cwd,
  onOutput, onClose, onTerminal} = {}) {
  if (typeof host !== 'string' || !isAbsolute(host)) throw new TypeError(
    'step=process-host-admission; recovery=pass host as the absolute installed acyclic-process-host executable');
  if (typeof program !== 'string' || !program || !Array.isArray(args)
    || args.some(argument => typeof argument !== 'string')
    || (input !== undefined && typeof input !== 'string' && !ArrayBuffer.isView(input) && !(input instanceof Readable))) {
    throw new TypeError('step=process-command-admission; recovery=pass a program, string arguments and optional string, byte view or Readable input');
  }
  signal?.throwIfAborted();
  const directory = await mkdtemp(join(tmpdir(), 'acyclic-process-'));
  const invocation = randomUUID().replaceAll('-', '');
  const endpoint = process.platform === 'win32'
    ? `\\\\.\\pipe\\acyclic-process-${invocation}` : join(directory, 'control');
  const receipt = join(directory, 'completion');
  const server = createServer();
  let control, stopped = false, child, result, closed, terminalCompletion, completionRead;
  let inputClosed = Promise.resolve(), outputClosed = Promise.resolve();
  let terminalReported = false, rootClosed = false;
  const failures = new Map(); // One original cause per fixed phase/stream.
  const retain = (phase, error, stream) => {
    const key = stream ? `${phase}:${stream}` : phase;
    if (!failures.has(key)) failures.set(key, {phase, error, ...(stream ? {stream} : {})});
  };
  const terminal = (exitCode, signal, cause) => {
    if (terminalReported) return;
    terminalReported = true;
    try {
      terminalCompletion = Promise.resolve(onTerminal?.({exitCode, signal, cause}))
        .catch(error => retain('terminal', error));
    } catch (error) { retain('terminal', error); }
  };
  const endInput = () => {
    if (input instanceof Readable) input.destroy();
    child?.stdin?.destroy();
  };
  const stop = () => {
    stopped = true;
    control?.end();
    endInput();
  };
  const fail = (phase, error, stream) => {
    retain(phase, error, stream);
    terminal(null, null, error?.message ?? String(error));
    stop();
  };
  const aborted = () => fail('abort', signal.reason);
  server.on('connection', accepted => {
    if (control) { accepted.destroy(); return; }
    control = accepted;
    accepted.on('error', error => fail('control', error));
    if (stopped) accepted.end();
  });
  server.on('error', error => fail('control', error));
  try {
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(endpoint, () => { server.removeListener('error', reject); resolve(); });
    });
    // Abort before spawn admits no executable. After spawn it stays armed
    // through accepted output writes and physical retirement.
    signal?.throwIfAborted();
    child = spawn(host, ['--control', endpoint, '--completion', receipt,
      '--invocation', invocation, '--inherit-input', '--', program, ...args], {
      env, cwd, windowsHide: true, stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
    });
    closed = new Promise(resolve => {
      child.once('error', error => fail('spawn', error));
      child.once('close', (code, signal) => resolve({code, signal}));
    });
    const readCompletion = async hostStatus => {
      try {
        const completion = decodeCompletion(await readFile(receipt, 'utf8'), invocation);
        if (hostStatus.signal !== null || hostStatus.code === null
          || (hostStatus.code >>> 0) !== completion.host_exit_code) {
          throw Object.assign(new Error('step=process-host-close; recovery=retain the original host exit and completion receipt'),
            {code: 'PROCESS_HOST_STATUS_MISMATCH', hostStatus, completion});
        }
        result = {code: completion.root_status?.code ?? null,
          signal: signalNames.get(completion.root_status?.signal) ?? null, completion};
        if (!completion.process_tree_retired || completion.failure) fail('retirement', Object.assign(
          new Error('step=process-retirement; recovery=inspect the original completion before reusing its custody'),
          {code: 'PROCESS_HOST_FAILED', cause: completion.failure, completion}));
      } catch (error) {
        retain('retirement', error);
        result = {code: null, signal: null, hostStatus};
      }
      if (failures.size || result.code !== 0) terminal(result.code, result.signal,
        failures.size ? 'failed' : result.signal ? `signal ${result.signal}` : `process exit ${result.code}`);
    };
    child.once('exit', (code, signal) => {
      rootClosed = true;
      endInput();
      // The native owner has already retired the tree and published its receipt.
      // Nonzero terminal observers may release an accepted output write, so
      // observe that original exit before joining the write or stdio close.
      completionRead = readCompletion({code, signal});
    });
    const output = async (stream, source) => {
      try {
        for await (const bytes of source) {
          try { await onOutput?.(stream, bytes, failures.size === 0); }
          catch (error) { fail('observer', error, stream); }
        }
      } catch (error) { fail('output', error, stream); }
    };
    outputClosed = Promise.all([output('stdout', child.stdout), output('stderr', child.stderr)]);
    inputClosed = child.stdin
      ? pipeline(input instanceof Readable ? input : Readable.from([ArrayBuffer.isView(input)
        ? Buffer.from(input.buffer, input.byteOffset, input.byteLength) : input]), child.stdin)
        .catch(error => {
          if (!(rootClosed || stopped) || !['ERR_STREAM_PREMATURE_CLOSE', 'ERR_STREAM_DESTROYED'].includes(error.code)) {
            fail('input', error);
          }
        }) : Promise.resolve();
    signal?.addEventListener('abort', aborted, {once: true});
    if (signal?.aborted) aborted();
    const hostStatus = await closed;
    await (completionRead ?? readCompletion(hostStatus));
    await Promise.all([inputClosed, outputClosed]);
    try { onClose?.(result); } catch (error) { retain('status', error); }
    terminal(result.code, result.signal, failures.size ? 'failed'
      : result.code === 0 ? 'completed' : result.signal ? `signal ${result.signal}` : `process exit ${result.code}`);
    await terminalCompletion;
    return {...result, failures: Array.from(failures.values())};
  } finally {
    // Even a synchronous stream/setup exception keeps custody until physical
    // close. The native owner, never Node PID signalling, retires the tree.
    if (child && !rootClosed) { stop(); await closed; }
    await Promise.all([inputClosed, outputClosed, terminalCompletion]);
    signal?.removeEventListener('abort', aborted);
    control?.destroy();
    try {
      await new Promise((resolve, reject) => server.close(error => {
        if (error && error.code !== 'ERR_SERVER_NOT_RUNNING') reject(error); else resolve();
      }));
    } finally { await rm(directory, {recursive: true, force: true}); }
  }
}

/** Capture is explicit and unbounded unless the caller supplies maxBuffer.
 * Streaming callers use commandProcess and retain no output here.
 */
export async function commandCapture(program, args, {maxBuffer, ...options} = {}) {
  if (maxBuffer !== undefined && (!Number.isSafeInteger(maxBuffer) || maxBuffer < 0)) {
    throw new TypeError('step=process-capture-admission; recovery=omit maxBuffer or supply a nonnegative safe integer');
  }
  return withProcessSignals(async signal => {
    const buffers = {stdout: [], stderr: []}, sizes = {stdout: 0, stderr: 0};
    let overflow = false;
    const result = await commandProcess(program, args, {...options, signal,
      onOutput(stream, bytes) {
        if (overflow) return;
        const size = sizes[stream] + bytes.length;
        if (maxBuffer !== undefined && size > maxBuffer) {
          overflow = true;
          throw Object.assign(
            new RangeError('step=process-capture; recovery=stream output with commandProcess or raise the explicit maxBuffer reservation'),
            {code: 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER'});
        }
        sizes[stream] = size;
        buffers[stream].push(bytes);
      }});
    const output = {stdout: Buffer.concat(buffers.stdout, sizes.stdout).toString('utf8'),
      stderr: Buffer.concat(buffers.stderr, sizes.stderr).toString('utf8')};
    const errors = result.failures.map(({error}) => error);
    if (result.code !== 0) errors.push(Object.assign(
      new Error('step=process-exit; recovery=inspect the original exit status before retrying the command'),
      {code: result.code, signal: result.signal, ...output}));
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw Object.assign(new AggregateError(errors,
      'step=process-terminal; recovery=inspect all original process and sink causes', {cause: errors[0]}), output);
    return output;
  }, {signal: options.signal});
}

/** Process-level signal ownership is optional and ends after physical joins. */
export async function withProcessSignals(action, {signal,
  reason = signal => Object.assign(new Error(`process signal ${signal}`), {signal})} = {}) {
  if (signal) return action(signal);
  const stop = new AbortController();
  const handlers = ['SIGINT', 'SIGTERM'].map(name => [name, () => stop.abort(reason(name))]);
  for (const [name, handler] of handlers) process.on(name, handler);
  try {
    const result = await action(stop.signal);
    stop.signal.throwIfAborted();
    return result;
  }
  finally { for (const [name, handler] of handlers) process.removeListener(name, handler); }
}
