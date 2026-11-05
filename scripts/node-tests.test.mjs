import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
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
  writeFileSync(join(root, 'typescript.test.ts'), "import test from 'node:test';const value: number=1;test('TypeScript under native Bun',()=>{if(value!==1)throw Error('wrong value')});");
  writeFileSync(join(root, 'underscore_test.ts'), "import {test,expect} from 'bun:test';test('native underscore discovery',()=>expect(1).toBe(1));");
  writeFileSync(join(root, 'package.json'), JSON.stringify({scripts: {'test:prepare': 'bun prepare.mjs'}}));
  writeFileSync(join(root, 'prepare.mjs'), "import {writeFileSync} from 'node:fs';writeFileSync('prepared.mjs','export const prepared = true;');");
  writeFileSync(join(root, 'passed.test.mjs'), "import test from 'node:test';import {prepared} from './prepared.mjs';test('prepared input',()=>{if(!prepared)throw Error('missing preparation')});");
  for (const file of ['import.test.mjs', 'import.test.ts']) writeFileSync(join(root, file), "import './missing-module.mjs';");
  const run = args => spawnSync('node', [fileURLToPath(new URL('./node-tests.mjs', import.meta.url)), '--root', root, ...args], {cwd: root, encoding: 'utf8', env: {...process.env, SDK_TEMP_DIR: root, NODE_TEST_CONTEXT: undefined}});
  const failed = run(['--all']);
  assert.equal(failed.status, 1, failed.stdout + failed.stderr);
  for (const file of ['node.test.mjs', 'bun.test.ts']) assert.ok(failed.stdout.includes(`NODE_TEST_FAIL ${file}::parent > failure <&> 😀`), failed.stdout + failed.stderr);
  for (const file of ['import.test.mjs', 'import.test.ts']) assert.ok(failed.stdout.includes(`NODE_TEST_FAIL ${file}::`), failed.stdout + failed.stderr);
  const summary = JSON.parse((failed.stdout.trim().split('\n').at(-1) ?? assert.fail('missing summary')).replace('NODE_TESTS_RESULT ', ''));
  assert.equal(summary.files, 7); assert.equal(summary.tests, 7); assert.equal(summary.passed, 3); assert.equal(summary.failed, 4);
  assert.equal(bunResults(readFileSync(join(root, 'observability/bun-test.xml'), 'utf8'), () => {}).failed, 1);
  const selected = run(['passed.test.mjs']);
  assert.equal(selected.status, 0, selected.stdout + selected.stderr);
  assert.match(selected.stdout, /"files":1,"tests":1,"passed":1,"failed":0/);
  writeFileSync(join(root, 'prepare.mjs'), "throw Error('planted preparation failure');");
  const preparation = run(['passed.test.mjs']);
  assert.equal(preparation.status, 2, preparation.stdout + preparation.stderr);
  assert.match(preparation.stderr, /step=prepare test inputs; recovery command: bun run test:prepare/);
  assert.doesNotMatch(preparation.stdout, /"passed":1/);
  const inventory = discoverTests(root);
  assert.equal(affectedTests(inventory, ['deleted.test.mjs'], root).length, 7);
  assert.equal(affectedTests(inventory, ['scripts/node-tests.mjs'], root).length, 7);
  assert.equal(affectedTests(inventory, [], root).length, 0);
  writeFileSync(join(root, 'dependent.test.ts'), "import './bun.test.js';");
  assert.deepEqual(affectedTests(discoverTests(root), ['bun.test.ts'], root).map(test => test.file), ['bun.test.ts', 'dependent.test.ts']);
  writeFileSync(join(root, '.captured.test.ts'), planted('bun'));
  writeFileSync(join(root, 'test-scenario.mjs'), "if(!process.argv[2])throw Error('executable argument required');");
  mkdirSync(join(root, 'nested'));
  writeFileSync(join(root, 'nested/test-registered.mjs'), "import test from 'node:test';test('registered',()=>{});");
  assert.deepEqual(discoverTests(root).map(test => test.file), ['bun.test.ts', 'dependent.test.ts', 'import.test.mjs', 'import.test.ts', 'nested/test-registered.mjs', 'node.test.mjs', 'passed.test.mjs', 'typescript.test.ts', 'underscore_test.ts']);
  assert.deepEqual(affectedTests(discoverTests(root), ['nested/test-registered.mjs'], root).map(test => test.file), ['nested/test-registered.mjs']);
  if (process.platform === 'win32') for (const host of [undefined, 'relative-host.exe']) {
   const refused = spawnSync('node', [fileURLToPath(new URL('./node-tests.mjs', import.meta.url)), '--root', root, '--all'], {
    encoding: 'utf8', env: {...process.env, E2E_PROCESS_HOST: host, ACYCLIC_PROCESS_HOST: undefined, NODE_TEST_CONTEXT: undefined},
   });
   assert.equal(refused.status, 2, refused.stdout + refused.stderr);
   assert.match(refused.stderr, /step=admit native process owner; recovery command:/);
   assert.doesNotMatch(refused.stdout, /NODE_TEST_FAIL node\.test\.mjs/);
  }
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
