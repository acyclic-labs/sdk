import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { waitForFile, waitForFileValue } from './wait-path.mjs';

test('waits for an event after an incomplete publication', async () => {
  const root = mkdtempSync(join(tmpdir(), 'acyclic-file-value-'));
  try {
    const path = join(root, 'endpoint');
    writeFileSync(path, 'port\n');
    let incomplete = false;
    const ready = waitForFileValue(path, file => {
      const [port, endpoint] = readFileSync(file, 'utf8').split('\n');
      if (!endpoint) { incomplete = true; return undefined; }
      return { port, endpoint };
    }, { timeoutMs: 2000 });
    assert.equal(incomplete, true);
    writeFileSync(path, 'port\n/ready\n');
    assert.deepEqual(await ready, { port: 'port', endpoint: '/ready' });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('observes an atomic publication through newly created ancestors', async () => {
  const root = mkdtempSync(join(tmpdir(), 'acyclic-file-notification-'));
  try {
    const path = join(root, 'new', 'nested', 'ready');
    const ready = waitForFile(path, { timeoutMs: 2000 });
    const child = spawn(process.execPath, [fileURLToPath(new URL('./wait-path.mjs', import.meta.url)), path], {
      windowsHide: true, stdio: 'ignore', timeout: 2000,
    });
    const terminal = new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code, signal) => {
        if (code === 0) resolve();
        else reject(new Error(`file wait failed: ${code ?? signal}`));
      });
    });
    mkdirSync(join(root, 'new', 'nested'), { recursive: true });
    writeFileSync(join(root, 'staged'), 'committed');
    renameSync(join(root, 'staged'), path);
    await ready;
    await terminal;
    await waitForFile(path, { timeoutMs: 2000 });
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('cancellation and deadline close an unpublished wait', async () => {
  const root = mkdtempSync(join(tmpdir(), 'acyclic-file-cancel-'));
  try {
    const controller = new AbortController();
    const cancelled = waitForFile(join(root, 'missing'), { signal: controller.signal });
    controller.abort(new Error('original owner cancelled'));
    await assert.rejects(cancelled, /original owner cancelled/);
    await assert.rejects(waitForFile(join(root, 'missing'), { timeoutMs: 1 }), /readiness deadline/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
