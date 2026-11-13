// Real producer mutations must fail their original regression or golden check.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {readFile, writeFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';

if (process.platform === 'win32') throw Error('Capture controls require POSIX process-group signals');
const directory = fileURLToPath(new URL('.', import.meta.url));
let active, interrupted, stopFailure;
function stop(signal) {
  interrupted ??= signal;
  if (active?.pid && active.exitCode === null && active.signalCode === null) {
    try { process.kill(-active.pid, 'SIGTERM'); }
    catch (error) { if (error.code !== 'ESRCH') stopFailure ??= error; }
  }
}
const interrupt = () => stop('SIGINT'), terminate = () => stop('SIGTERM');
process.on('SIGINT', interrupt);
process.on('SIGTERM', terminate);

async function run(binary, args) {
  if (interrupted) throw Error(`controls interrupted by ${interrupted}`, {cause: stopFailure});
  const child = spawn(binary, args, {cwd: directory, detached: true, stdio: ['ignore', 'pipe', 'pipe']});
  active = child;
  let stdout = '', stderr = '', launchError;
  child.stdout.on('data', bytes => { stdout += bytes; });
  child.stderr.on('data', bytes => { stderr += bytes; });
  const code = await new Promise(accept => {
    child.once('error', error => { launchError = error; });
    child.once('close', accept);
  });
  active = undefined;
  if (launchError) throw launchError;
  if (interrupted) throw Error(`controls interrupted by ${interrupted}`, {cause: stopFailure});
  return {code, output: stdout + stderr};
}

async function mutation(path, original, changed, operation) {
  if (original === changed) throw Error(`mutation missed its original boundary: ${path}`);
  try {
    await writeFile(path, changed);
    await operation();
  } finally {
    await writeFile(path, original);
  }
}

try {
  const capture = new URL('capture-actors.mjs', import.meta.url);
  const sockets = new URL('../crates/actors/src/conformance/sockets.rs', import.meta.url);
  const originalCapture = await readFile(capture, 'utf8');
  const originalSockets = await readFile(sockets);
  try {
    await mutation(capture, originalCapture,
      originalCapture.replace('    await writeFile(source, original);', ''), async () => {
        const result = await run(process.execPath, ['--test', 'capture-actors-interruption.mjs']);
        assert.equal(result.code, 1, result.output);
        assert.match(result.output, /retains the original source bytes/);
        assert.notDeepEqual(await readFile(sockets), originalSockets);
        console.log('ACTOR_RESTORATION_CONTROL_DETECTED');
      });
  } finally {
    await writeFile(sockets, originalSockets);
  }

  const writer = new URL('examples/capture.rs', import.meta.url);
  const originalWriter = await readFile(writer, 'utf8');
  const boundary = '    let frame = format!("corpus/{target}/{name}");';
  assert.equal(originalWriter.split(boundary).length, 2);
  const altered = `    let altered: Vec<u8> = bytes.iter().enumerate().map(|(index, byte)| {
        if target == "actor_codecs" && name == "state-acs-v1" && index == 0 { *byte ^ 1 } else { *byte }
    }).collect();
    let bytes = altered.as_slice();
`;
  await mutation(writer, originalWriter, originalWriter.replace(boundary, altered + boundary), async () => {
    const result = await run('cargo', ['run', '--locked', '--example', 'capture', '--', '--only-actors']);
    assert.equal(result.code, 1, result.output);
    assert.match(result.output, /encoder changed golden corpus\/actor_codecs\/state-acs-v1/);
    console.log('ACTOR_WRITER_CONTROL_DETECTED');
  });
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  process.off('SIGINT', interrupt);
  process.off('SIGTERM', terminate);
  if (interrupted) process.exitCode = interrupted === 'SIGINT' ? 130 : 143;
}
