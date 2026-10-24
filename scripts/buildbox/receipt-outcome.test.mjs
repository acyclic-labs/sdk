import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
import test from 'node:test';

const here = path.dirname(new URL(import.meta.url).pathname);
const source = fs.readFileSync(path.join(here, 'buildbox-receipt.mjs'), 'utf8');
const generator = fs.readFileSync(path.join(here, 'buildbox.mjs'), 'utf8');
function fixture(negative = false) {
  const root = fs.mkdtempSync(path.join(tmpdir(), 'receipt-outcome-'));
  const cleanup = path.join(root, 'cleanup.py');
  fs.writeFileSync(cleanup, 'import sys\nfrom pathlib import Path\nassert sys.argv[1] in ("cleanup-outer", "stop-job")\nwith (Path(__file__).parent/"operations").open("a") as log:log.write(sys.argv[1]+"\\n")\nprint("{}")\n');
  let text = source.replaceAll('/home/var/bin/bb-job.py', cleanup);
  if (negative) {
    const start = text.indexOf("    // A wrapper watchdog's exit code");
    const end = text.indexOf('    const runtimeFile =', start);
    assert.ok(start >= 0 && end > start);
    text = text.slice(0, start) + text.slice(end);
  }
  const module = path.join(root, 'buildbox-receipt.mjs');
  fs.writeFileSync(module, text);
  return {root, module};
}
function run(module, code) {
  const result = spawnSync(process.execPath, ['--input-type=module', '-e',
    `import * as r from ${JSON.stringify(module)};import fs from 'node:fs';${code}`], {encoding:'utf8'});
  assert.equal(result.status, 0, result.stderr + result.stdout);
  return result;
}
function complete(f, name, markers, rc, category = 'command') {
  const prefix = path.join(f.root, name), meta = prefix + '.meta', terminal = prefix + '.receipt.json';
  for (const marker of markers) fs.writeFileSync(prefix + '.' + marker, '');
  run(f.module, `r.atomic(${JSON.stringify(meta)}, {
    side:'bb2',job_prefix:${JSON.stringify(prefix)},receipt_file:${JSON.stringify(terminal)},
    argv:['fixture'],admission_start:new Date().toISOString(),
    owner:{pid:process.pid,identity:r.processIdentity(process.pid),pid_namespace:r.pidNamespace()}
  });r.finish(${JSON.stringify(meta)},${rc},${JSON.stringify(category)});`);
  return {prefix, meta, terminal, value:JSON.parse(fs.readFileSync(terminal))};
}

test('watchdog causes are terminal INFRA with the actual status; the old writer is the negative control', () => {
  for (const negative of [false, true]) {
    const f = fixture(negative);
    try {
      for (const marker of ['idle', 'ceiling']) {
        const {value} = complete(f, marker, [marker], 124);
        assert.equal(value.exit_status, 124);
        assert.equal(value.termination, negative ? 'command' : 'infra');
        assert.equal(value.reason, negative ? undefined : 'watchdog-' + marker);
        if (!negative) assert.ok(value.detail.length > 0);
      }
      // Owner recovery has a different exit status but the same recorded cause.
      const {value} = complete(f, 'owner-ended', ['idle'], 137, 'interrupted');
      assert.equal(value.exit_status, 137);
      assert.equal(value.termination, negative ? 'interrupted' : 'infra');
    } finally { fs.rmSync(f.root, {recursive:true, force:true}); }
  }
});

test('child statuses, explicit cancellation and heartbeat loss retain their own outcomes', () => {
  const f = fixture();
  try {
    for (const rc of [0, 101, 124]) {
      const {value} = complete(f, 'child-' + rc, [], rc);
      assert.equal(value.exit_status, rc);assert.equal(value.termination, 'command');
      assert.equal(value.reason, undefined, 'exit 124 alone proves no watchdog');
    }
    for (const category of ['cancelled', 'interrupted']) {
      const {value} = complete(f, category, ['idle', category], 143, category);
      assert.equal(value.termination, category);assert.equal(value.exit_status, 143);
      assert.equal(value.reason, undefined);
    }
    const {value} = complete(f, 'explicit-cancel', ['idle'], 130, 'cancelled');
    assert.equal(value.termination, 'cancelled');
  } finally { fs.rmSync(f.root, {recursive:true, force:true}); }
});

test('a completed receipt is immutable when a late watchdog marker arrives', () => {
  const f = fixture();
  try {
    const job = complete(f, 'completed', [], 0);
    const before = fs.readFileSync(job.terminal);
    fs.writeFileSync(job.prefix + '.idle', '');
    run(f.module, `r.finish(${JSON.stringify(job.meta)},124,'command');`);
    assert.deepEqual(fs.readFileSync(job.terminal), before);
  } finally { fs.rmSync(f.root, {recursive:true, force:true}); }
});

test('a watchdog completes process custody before outer cleanup at the shared terminal boundary', () => {
  const f = fixture();
  try {
    fs.writeFileSync(path.join(f.root, 'live.meta.job'), '{}');
    const {value} = complete(f, 'live', ['idle'], 124);
    assert.equal(value.termination, 'infra');
    assert.deepEqual(fs.readFileSync(path.join(f.root, 'operations'), 'utf8').trim().split('\n'),
      ['stop-job', 'cleanup-outer']);
  } finally { fs.rmSync(f.root, {recursive:true, force:true}); }
});

test('client receipt and ledger retain the remote cause; dropping that cause is the negative control', () => {
  for (const negative of [false, true]) {
    const f = fixture();
    try {
      const remote = complete(f, 'remote', ['idle'], 124).value;
      const module = path.join(f.root, 'generator.mjs');
      let text = generator;
      if (negative) text = text.replace(' || ctx.remote?.reason', '').replace(' || ctx.remote?.detail', '');
      assert.equal(text === generator, !negative);
      fs.writeFileSync(module, text);
      const meta = path.join(f.root, 'client.meta'), terminal = meta + '.terminal', ledger = meta + '.ledger';
      run(f.module, `const g=await import(${JSON.stringify(module)});
        r.atomic(${JSON.stringify(meta)},{side:'client',receipt_file:${JSON.stringify(terminal)},ledger:${JSON.stringify(ledger)},
          argv:['fixture'],admission_start:new Date().toISOString(),
          owner:{pid:process.pid,identity:r.processIdentity(process.pid),pid_namespace:r.pidNamespace()}});
        g.endReceipt({metaFile:${JSON.stringify(meta)},remote:${JSON.stringify(remote)}},124);`);
      const client = JSON.parse(fs.readFileSync(terminal));
      assert.equal(client.exit_status, 124);assert.equal(client.termination, 'infra');
      assert.equal(client.reason, negative ? null : remote.reason);
      assert.equal(client.detail, negative ? null : remote.detail);
      assert.deepEqual(JSON.parse(fs.readFileSync(ledger, 'utf8')), client);
      assert.deepEqual(client.remote_receipt, remote);
    } finally { fs.rmSync(f.root, {recursive:true, force:true}); }
  }
});
