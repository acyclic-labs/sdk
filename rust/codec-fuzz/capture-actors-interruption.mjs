import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {readFile} from 'node:fs/promises';
import {createInterface} from 'node:readline';
import {fileURLToPath} from 'node:url';

const script = fileURLToPath(new URL('capture-actors.mjs', import.meta.url));
const source = new URL('../crates/actors/src/conformance/sockets.rs', import.meta.url);
test('native captures restore their source or refuse unsupported signal ownership', async () => {
  const original = await readFile(source);
  const interruptions = process.platform === 'win32' ? [[null, 1]] : [['SIGINT', 130], ['SIGTERM', 143]];
  for (const [signal, expected] of interruptions) {
    const child = spawn(process.execPath, [script], {stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true});
    let stderr = '';
    child.stderr.on('data', bytes => { stderr += bytes; });
    const ended = new Promise((accept, refuse) => {
      child.once('error', refuse);
      child.once('close', (code, signal) => accept({code, signal}));
    });
    const started = signal && new Promise((accept, refuse) => {
      const lines = createInterface({input: child.stdout});
      lines.on('line', line => {
        if (line.startsWith('capture: cargo test ')) accept();
      });
      child.once('exit', () => refuse(Error(`capture ended before starting Cargo: ${stderr}`)));
    });
    try {
      if (started) {
        await started;
        child.kill(signal);
      }
      assert.deepEqual(await ended, {code: expected, signal: null}, stderr);
      if (!signal) assert.match(stderr, /Actor capture requires POSIX process-group signals/);
      assert.deepEqual(await readFile(source), original, `${signal} retains the original source bytes`);
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM');
      await ended;
    }
  }
});
