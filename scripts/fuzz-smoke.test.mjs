import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {cpSync, mkdtempSync, readFileSync, readdirSync, writeFileSync, appendFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join, resolve} from 'node:path';
import {test} from 'node:test';
import {fileURLToPath} from 'node:url';

// The scoped validation supplies the real, instrumented production callback-path fuzzer.
const binary = process.env.SDK_FUZZ_TEST_BINARY;
const entry = fileURLToPath(new URL('./fuzz-smoke.mjs', import.meta.url));
const retained = fileURLToPath(new URL('../rust/fuzz/native-callback-path/corpus/callback_path', import.meta.url));
const snapshot = directory => readdirSync(directory, {withFileTypes: true})
  .sort((a, b) => a.name.localeCompare(b.name))
  .map(entry => [entry.name, entry.isDirectory() ? snapshot(join(directory, entry.name)) : readFileSync(join(directory, entry.name))]);

test('real campaign completion and exact closure reuse preserve every smoke obligation', () => {
  assert.ok(binary, 'build the callback_path fuzzer and set SDK_FUZZ_TEST_BINARY');
  const root = mkdtempSync(join(tmpdir(), 'fuzz-smoke-control-'));
  try {
    const executable = join(root, 'callback_path'), corpus = join(root, 'corpus');
    cpSync(resolve(binary), executable); cpSync(retained, corpus, {recursive: true});
    const env = {...process.env, ACTION_CACHE_ROOT: join(root, 'cache')};
    const run = (program = executable, args = ['-seed=1', '-runs=4096'], variables = env, status = 'PASS') => {
      const before = snapshot(corpus);
      const result = spawnSync(process.execPath, [entry, program, corpus, '--', ...args], {env: variables, encoding: 'utf8'});
      assert.deepEqual(snapshot(corpus), before, 'campaign must preserve every retained corpus name and byte');
      const lines = (result.stdout + result.stderr).split('\n').filter(line => line.startsWith('FUZZ_CAMPAIGN_TERMINAL '));
      assert.equal(lines.length, 1, result.stdout + result.stderr);
      const terminal = JSON.parse(lines[0].replace(/^FUZZ_CAMPAIGN_TERMINAL (PASS|FAIL) /, ''));
      console.log('FUZZ_SMOKE_CONTROL ' + JSON.stringify(terminal));
      assert.equal(terminal.status, status);
      assert.ok(lines[0].startsWith('FUZZ_CAMPAIGN_TERMINAL ' + status + ' '));
      assert.equal(result.status, status === 'PASS' ? 0 : 1, result.stdout + result.stderr);
      if (status === 'PASS') {
        assert.equal(terminal.campaign.instrumented, true);
        assert.equal(terminal.campaign.runs, Number(args.find(arg => arg.startsWith('-runs=')).slice('-runs='.length)));
      }
      return terminal;
    };
    const initial = run();
    assert.equal(initial.cached, false);
    assert.ok(initial.key, 'the real fuzzer trace must be recordable');
    const hit = run();
    assert.equal(hit.cached, true); assert.equal(hit.key, initial.key);
    assert.deepEqual(hit.campaign, initial.campaign);
    // An unrelated source edit and dispatch identity do not affect the executed obligation.
    writeFileSync(join(root, 'unrelated.txt'), 'changed outside the closure');
    assert.equal(run(executable, undefined, {...env, BB_JOB_ID: 'another-dispatch'}).key, initial.key);
    // Both corpus membership and bytes invalidate, while generated corpus stays private.
    writeFileSync(join(corpus, 'additional-seed'), Buffer.from([95, 0]));
    const added = run(); assert.equal(added.cached, false); assert.notEqual(added.key, initial.key);
    writeFileSync(join(corpus, 'additional-seed'), Buffer.from([96, 0]));
    const changed = run(); assert.equal(changed.cached, false); assert.notEqual(changed.key, added.key);
    assert.equal(run().key, changed.key);
    // Changing the seed, workload, executable or runtime environment cannot reuse PASS.
    assert.equal(run(executable, ['-seed=2', '-runs=4096']).cached, false);
    assert.equal(run(executable, ['-seed=1', '-runs=4097']).cached, false);
    assert.equal(run(executable, undefined, {...env, FUZZ_CONTROL_INPUT: 'different'}).cached, false);
    appendFileSync(executable, Buffer.from([0]));
    assert.equal(run().cached, false);
    // Successful non-fuzz programs, crashes and corpus-only replays never become PASS.
    run('/usr/bin/true', undefined, env, 'FAIL');
    run('/usr/bin/false', undefined, env, 'FAIL');
    run(executable, ['-seed=1', '-runs=0'], env, 'FAIL');
    // The CLI must publish one failure even when a supplied module cannot do so.
    const module = join(root, 'campaign.mjs');
    run(executable, undefined, {...env, SDK_FUZZ_CAMPAIGN: module}, 'FAIL');
    for (const source of [
      'export const campaign = null;',
      'export function campaign() { throw Error("before receipt"); }',
      'export function campaign() { throw null; }',
      'export function campaign() {}',
      'export function campaign({log}) { log(\'FUZZ_CAMPAIGN_TERMINAL FAIL {"status":"FAIL"}\'); throw Error("after receipt"); }',
    ]) {
      writeFileSync(module, source);
      run(executable, undefined, {...env, SDK_FUZZ_CAMPAIGN: module}, 'FAIL');
    }
  } finally { rmSync(root, {recursive: true, force: true}); }
});

// This checks the public command and supplied-module receipt protocol, not campaign quality.
test('supplied campaign receipts determine the command outcome', () => {
  const root = mkdtempSync(join(tmpdir(), 'campaign-receipt-control-'));
  try {
    const module = join(root, 'campaign.mjs');
    for (const [receipts, code] of [[['PASS'], 0], [['FAIL'], 1], [['PASS', 'FAIL'], 1]]) {
      writeFileSync(module, `export function campaign({log}) {
        for (const status of ${JSON.stringify(receipts)})
          log('FUZZ_CAMPAIGN_TERMINAL ' + status + ' ' + JSON.stringify({version: 1, status}));
      }`);
      const result = spawnSync(process.execPath, [entry, 'selected-binary', 'retained-corpus', '--', '-runs=1'],
        {env: {...process.env, SDK_FUZZ_CAMPAIGN: module}, encoding: 'utf8'});
      assert.equal(result.status, code, result.stdout + result.stderr);
      const terminals = (result.stdout + result.stderr).split(/\r?\n/)
        .filter(line => line.startsWith('FUZZ_CAMPAIGN_TERMINAL '));
      assert.equal(terminals.length, 1, result.stdout + result.stderr);
      assert.ok(terminals[0].startsWith('FUZZ_CAMPAIGN_TERMINAL ' + receipts[0] + ' '));
    }
  } finally { rmSync(root, {recursive: true, force: true}); }
});
