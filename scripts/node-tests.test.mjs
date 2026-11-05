import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import test from 'node:test';
import fc from 'fast-check';
import {affectedTests, bunResults, discoverTests} from './node-tests.mjs';

test('native Node and Bun failures survive discovery, selection and result reporting', () => {
 const root = mkdtempSync(join(tmpdir(), 'node-test-control-'));
 try {
  const planted = name => `import {describe,it} from '${name}:test';describe('parent',()=>{it('failure <&> 😀',()=>{throw Error('planted original failure')})});`;
  writeFileSync(join(root, 'node.test.mjs'), planted('node'));
  writeFileSync(join(root, 'bun.test.ts'), planted('bun'));
  writeFileSync(join(root, 'passed.test.mjs'), "import test from 'node:test';test('passes',()=>{});");
  for (const file of ['import.test.mjs', 'import.test.ts']) writeFileSync(join(root, file), "import './missing-module.mjs';");
  const run = args => spawnSync('node', [fileURLToPath(new URL('./node-tests.mjs', import.meta.url)), '--root', root, ...args], {cwd: root, encoding: 'utf8', env: {...process.env, NODE_TEST_CONTEXT: undefined}});
  const failed = run(['--all']);
  assert.equal(failed.status, 1, failed.stdout + failed.stderr);
  for (const file of ['node.test.mjs', 'bun.test.ts']) assert.ok(failed.stdout.includes(`NODE_TEST_FAIL ${file}::parent > failure <&> 😀`), failed.stdout + failed.stderr);
  for (const file of ['import.test.mjs', 'import.test.ts']) assert.ok(failed.stdout.includes(`NODE_TEST_FAIL ${file}::`), failed.stdout + failed.stderr);
  const summary = JSON.parse((failed.stdout.trim().split('\n').at(-1) ?? assert.fail('missing summary')).replace('NODE_TESTS_RESULT ', ''));
  assert.equal(summary.files, 5); assert.equal(summary.tests, 5); assert.equal(summary.passed, 1); assert.equal(summary.failed, 4);
  const selected = run(['passed.test.mjs']);
  assert.equal(selected.status, 0, selected.stdout + selected.stderr);
  assert.match(selected.stdout, /"files":1,"tests":1,"passed":1,"failed":0/);
  const inventory = discoverTests(root);
  assert.equal(affectedTests(inventory, ['deleted.test.mjs'], root).length, 5);
  assert.equal(affectedTests(inventory, ['scripts/node-tests.mjs'], root).length, 5);
  assert.equal(affectedTests(inventory, [], root).length, 0);
 } finally { rmSync(root, {recursive: true, force: true}); }
});

test('Bun report decoder consumes recorded native output and refuses corrupted XML', () => {
 const corpus = readFileSync(new URL('./test-support/fixtures/bun-node-tests.xml', import.meta.url), 'utf8');
 const failures = [];
 assert.equal(bunResults(corpus, (file, name) => failures.push([file, name])).failed, 1);
 assert.deepEqual(failures, [['bun.test.ts', 'parent > failure <&> 😀']]);
 fc.assert(fc.property(fc.string(), suffix => {
  assert.throws(() => bunResults(corpus + '<broken>' + suffix, () => {}));
 }), {numRuns: 64, seed: 0x4e4f4445});
});
