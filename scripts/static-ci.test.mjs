import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import {promisify} from 'node:util';
import test, {describe} from 'node:test';

const execute = promisify(execFile);

async function fixture(body) {
 const root = mkdtempSync(join(tmpdir(), 'sdk-release-static-'));
 const absent = process.platform === 'win32' ? 'NUL' : '/dev/null';
 const env = {...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: absent,
  GIT_CONFIG_COUNT: '2', GIT_CONFIG_KEY_0: 'core.autocrlf', GIT_CONFIG_VALUE_0: 'false',
  GIT_CONFIG_KEY_1: 'core.attributesFile', GIT_CONFIG_VALUE_1: absent};
 const git = (args, input) => new Promise((accept, reject) => {
  const child = execFile('git', args, {cwd: root, env}, error => error ? reject(error) : accept());
  child.stdin.on('error', reject);
  child.stdin.end(input);
 });
 const history = [];
 const previous = new Map();
 let mark = 0;
 const append = text => history.push(Buffer.from(text));
 const paths = (prefix = '') => readdirSync(join(root, prefix), {withFileTypes: true})
  .sort((left, right) => left.name.localeCompare(right.name))
  .flatMap(entry => {
   const path = prefix ? `${prefix}/${entry.name}` : entry.name;
   return entry.name === '.git' ? [] : entry.isDirectory() ? paths(path) : [path];
  });
 const commit = message => {
  append(`commit refs/heads/fixture\nmark :${++mark}\ncommitter Contract <contract@example.invalid> ${1700000000 + mark} +0000\ndata ${Buffer.byteLength(message)}\n${message}\n`);
  if (mark > 1) append(`from :${mark - 1}\n`);
  for (const path of paths()) {
   const bytes = readFileSync(join(root, path));
   if (previous.get(path)?.equals(bytes)) continue;
   previous.set(path, bytes);
   append(`M 100644 inline ${JSON.stringify(path)}\ndata ${bytes.length}\n`);
   history.push(bytes);
   append('\n');
  }
  append('\n');
 };
 const tag = name => append(`reset refs/tags/${name}\nfrom :${mark}\n\n`);
 const check = async () => {
  await git(['init', '-q', '--initial-branch=fixture', '--template=']);
  await git(['fast-import', '--quiet'], Buffer.concat([...history, Buffer.from('done\n')]));
  await git(['read-tree', 'HEAD']);
  try {
   return {status: 0, ...await execute(process.execPath, ['scripts/static-ci.mjs'], {cwd: root, env})};
  } catch (error) {
   return {status: error.code, stdout: error.stdout, stderr: error.stderr};
  }
 };
 try {
  mkdirSync(join(root, 'scripts/vendor'), {recursive: true});
  copyFileSync(resolve('.gitattributes'), join(root, '.gitattributes'));
  for (const name of ['static-ci.mjs', 'static-policy.mjs', 'vendor/yaml.cjs']) {
   copyFileSync(resolve('scripts', name), join(root, 'scripts', name));
  }
  commit('baseline');
  await body({root, tag, commit, check});
 } finally {
  assert.equal(resolve(dirname(root)), resolve(tmpdir()));
  rmSync(root, {recursive: true, force: true});
 }
}

describe('release static policy', {concurrency: true}, () => {
 test('release whitespace catches an earlier commit and cannot be hidden by a tag at HEAD', () => fixture(async ({root, tag, commit, check}) => {
  tag('acyclic-v0.1.0');
  writeFileSync(join(root, 'earlier.txt'), 'trailing space \n'); commit('earlier error');
  writeFileSync(join(root, 'later.txt'), 'clean\n'); commit('clean final commit');
  tag('acyclic-v0.2.0');
  const result = await check();
  assert.notEqual(result.status, 0);
  assert.match(result.stdout, /earlier\.txt.*trailing whitespace/);
 }));

 test('a release checks its full range without rechecking inherited whitespace', () => fixture(async ({root, tag, commit, check}) => {
  writeFileSync(join(root, 'inherited.txt'), 'old space \n'); commit('prior release'); tag('acyclic-v0.1.0');
  writeFileSync(join(root, 'current.txt'), 'clean\n'); commit('new release');
  const result = await check(); assert.equal(result.status, 0, result.stdout + result.stderr);
 }));

 test('the first release checks the tracked tree when no prior release exists', () => fixture(async ({root, commit, check}) => {
  writeFileSync(join(root, 'earlier.txt'), 'trailing space \n'); commit('earlier error'); commit('clean final commit');
  const result = await check(); assert.notEqual(result.status, 0); assert.match(result.stdout, /earlier\.txt.*trailing whitespace/);
 }));

 test('release policy fetches complete history and docs describe only release diagnostics', () => {
  const workflow = readFileSync('.github/workflows/qualification.yml', 'utf8');
  assert.match(workflow, /fetch-depth: \$\{\{ matrix\.lane == 'policy' && '0' \|\| '1' \}\}/);
  assert.match(workflow, /fetch-tags: \$\{\{ matrix\.lane == 'policy' \}\}/);
  const docs = readFileSync('docs/ci-releases.md', 'utf8');
  assert.match(docs, /publish=false/); assert.match(docs, /no independent manual triggers/);
  assert.doesNotMatch(docs, /Manual diagnostic qualification remains available|native-mount-qualification\.yml|five-minute job/);
 });

 test('release whitespace preserves byte-exact transport artifacts and generated declarations', () => fixture(async ({root, tag, commit, check}) => {
  tag('acyclic-v0.1.0');
  const artifacts = {
   'patches/example.patch': '@@ -1 +1 @@\n \n',
   'typescript/packages/actors/src/generated/ErrorMetadata.ts': 'export type ErrorMetadata = { \ncode: string, \n};\n',
   'typescript/packages/actors/src/generated/semantic/actors/ActorId.ts': 'export type ActorId = { \nvalue: string, \n};\n',
  };
  for (const [path, bytes] of Object.entries(artifacts)) { mkdirSync(join(root, path, '..'), {recursive: true}); writeFileSync(join(root, path), bytes); }
  commit('record producer artifacts'); commit('later source');
  const result = await check(); assert.equal(result.status, 0, result.stdout + result.stderr);
  for (const [path, bytes] of Object.entries(artifacts)) assert.equal(readFileSync(join(root, path), 'utf8'), bytes);
 }));

 test('artifact whitespace rules still reject handwritten source errors', () => fixture(async ({root, tag, commit, check}) => {
  tag('acyclic-v0.1.0'); mkdirSync(join(root, 'typescript/packages/actors/src'), {recursive: true});
  writeFileSync(join(root, 'typescript/packages/actors/src/client.ts'), 'export const value=1; \n'); commit('source whitespace'); commit('later commit');
  const result = await check(); assert.notEqual(result.status, 0); assert.match(result.stdout, /client\.ts.*trailing whitespace/);
 }));

 test('generated declaration whitespace still rejects extra blank lines at EOF', () => fixture(async ({root, tag, commit, check}) => {
  tag('acyclic-v0.1.0'); mkdirSync(join(root, 'typescript/packages/actors/src/generated/semantic/actors'), {recursive: true});
  writeFileSync(join(root, 'typescript/packages/actors/src/generated/semantic/actors/ActorId.ts'), 'export type ActorId = string; \n\n'); commit('generated EOF error'); commit('later commit');
  const result = await check(); assert.notEqual(result.status, 0); assert.match(result.stdout, /ActorId\.ts.*new blank line at EOF/);
 }));
});
