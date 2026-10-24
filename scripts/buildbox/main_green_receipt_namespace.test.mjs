import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { pathToFileURL } from 'node:url';
import * as receipt from './buildbox-receipt.mjs';

const source = fs.readFileSync(new URL('./buildbox-receipt.mjs', import.meta.url), 'utf8');
const live = () => ({ pid: process.pid, identity: receipt.processIdentity(process.pid), pid_namespace: receipt.pidNamespace() });
const dead = () => ({ ...live(), pid: 2147483647, identity: 'not-a-live-process' });
const foreign = () => ({ ...dead(), pid_namespace: 'pid:[foreign-fixture]' });

async function fixture(run) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'receipt-namespace-'));
  try { await run(root); }
  finally { fs.rmSync(root, { recursive: true, force: true }); }
}

function metadata(root, owner = foreign(), fields = {}) {
  const file = path.join(root, 'fixture.receipt-meta.v2.json');
  const meta = { side: 'fixture', argv: [], worktree: root,
    admission_start: new Date().toISOString(), receipt_file: path.join(root, 'terminal.json'), owner, ...fields };
  fs.writeFileSync(file, JSON.stringify(meta));
  return { file, meta };
}

async function mutant(root) {
  // Delete the single namespace authority check in the production code.
  const needle = 'return namespace !== null && owner?.pid_namespace === namespace;';
  assert.equal(source.split(needle).length, 2);
  const file = path.join(root, 'unfenced.mjs');
  fs.writeFileSync(file, source.replace(needle, 'return true;'));
  return import(pathToFileURL(file).href);
}

test('all receipt mutation entrypoints reject foreign owners and the production fence mutant fails', async () => {
  for (const operation of ['recover', 'watch', 'finish']) {
    for (const negative of [false, true]) await fixture(async root => {
      const api = negative ? await mutant(root) : receipt;
      const { file, meta } = metadata(root);
      if (operation === 'recover') api.recover(root);
      else if (operation === 'watch') await api.watch(file, 1);
      else api.finish(file, 23, 'command');
      assert.equal(fs.existsSync(meta.receipt_file), negative, `${operation}, unfenced=${negative}`);
      assert.equal(fs.existsSync(meta.receipt_file + '.lock'), false);
    });
  }
});

test('same-namespace recovery distinguishes live and exited owners', async () => {
  assert.ok(receipt.pidNamespace());
  assert.equal(receipt.canObserveOwner(live()), true);
  assert.equal(receipt.canObserveOwner(foreign()), false);
  assert.equal(receipt.canObserveOwner({}), false);
  await fixture(async root => {
    const { file, meta } = metadata(root, live());
    receipt.recover(root);
    assert.equal(fs.existsSync(meta.receipt_file), false);
    fs.writeFileSync(file, JSON.stringify({ ...meta, owner: dead() }));
    receipt.recover(root);
    const terminal = JSON.parse(fs.readFileSync(meta.receipt_file));
    assert.equal(terminal.exit_status, 137);
    assert.equal(terminal.reason, 'receipt owner absent during recovery');
  });
  await fixture(async root => {
    const { file, meta } = metadata(root, live());
    assert.equal(receipt.finish(file, 23, 'command').exit_status, 23);
    assert.equal(JSON.parse(fs.readFileSync(meta.receipt_file)).exit_status, 23);
  });
});

test('a foreign completed receipt cannot enter ledger mutation', async () => {
  for (const negative of [false, true]) await fixture(async root => {
    const api = negative ? await mutant(root) : receipt;
    const ledger = path.join(root, 'ledger.jsonl');
    const { meta } = metadata(root, foreign(), { ledger });
    fs.writeFileSync(meta.receipt_file, JSON.stringify({ receipt_file: meta.receipt_file, exit_status: 0 }));
    api.recover(root);
    assert.equal(fs.existsSync(ledger), negative);
  });
});

