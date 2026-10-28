import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { waitForFile } from './wait-path.mjs';

test('observes an atomic publication through newly created ancestors', async () => {
  const root = mkdtempSync(join(tmpdir(), 'acyclic-file-notification-'));
  try {
    const path = join(root, 'new', 'nested', 'ready');
    const ready = waitForFile(path, { timeoutMs: 2000 });
    mkdirSync(join(root, 'new', 'nested'), { recursive: true });
    writeFileSync(join(root, 'staged'), 'committed');
    renameSync(join(root, 'staged'), path);
    await ready;
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
