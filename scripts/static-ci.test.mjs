import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {copyFileSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, relative, resolve} from 'node:path';
import {promisify} from 'node:util';
import test, {describe} from 'node:test';

const execute = promisify(execFile);

// Copy the entry point and every relative module it imports, recursively, so a
// new import can never be missing from the isolated fixture.
function copyModule(root, file, seen = new Set()) {
 const absolute = resolve(file);
 if (seen.has(absolute)) return;
 seen.add(absolute);
 const target = join(root, 'scripts', relative(resolve('scripts'), absolute));
 mkdirSync(dirname(target), {recursive: true});
 const source = readFileSync(absolute, 'utf8');
 // A fixture rewrites its isolated entry point even when runfiles are read-only.
 writeFileSync(target, source);
 for (const match of source.matchAll(/\b(?:from|import|require)\s*\(?\s*['"]([^'"]+)['"]/g)) {
  if (match[1].startsWith('.')) copyModule(root, resolve(dirname(absolute), match[1]), seen);
 }
}

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
 const check = async ({runtime = process.execPath, args = ['--experimental-vm-modules'], extraEnv = {}} = {}) => {
  await git(['init', '-q', '--initial-branch=fixture', '--template=']);
  await git(['fast-import', '--quiet'], Buffer.concat([...history, Buffer.from('done\n')]));
  await git(['read-tree', 'HEAD']);
  try {
   return {status: 0, ...await execute(runtime, [...args, 'scripts/static-ci.mjs'], {cwd: root, env: {...env, ...extraEnv}})};
  } catch (error) {
   return {status: error.code, stdout: error.stdout, stderr: error.stderr};
  }
 };
 try {
  copyFileSync(resolve('.gitattributes'), join(root, '.gitattributes'));
  copyModule(root, resolve('scripts/static-ci.mjs'));
  commit('baseline');
  await body({root, tag, commit, check});
 } finally {
  assert.equal(resolve(dirname(root)), resolve(tmpdir()));
  rmSync(root, {recursive: true, force: true});
 }
}

describe('release static policy', {concurrency: true}, () => {
 test('Bun-hosted fixtures use one parser process and the captured old checker reaches a bounded recursion fence', () => fixture(async ({root, commit, check}) => {
  const old = readFileSync('scripts/fixtures/static-ci-b7333953cb.mjs.txt', 'utf8');
  const current = readFileSync(join(root, 'scripts/static-ci.mjs'), 'utf8');
  const processLog = join(root, 'checker-processes');
  const executionLog = join(root, 'checked-file-executed');
  const extraEnv = {CHECKER_PROCESS_LOG: processLog, CHECKED_FILE_LOG: executionLog};
  const audit = `import {appendFileSync as recordCheckerProcess} from 'node:fs';
recordCheckerProcess(process.env.CHECKER_PROCESS_LOG, process.pid + '\\n');
`;
  const processes = () => readFileSync(processLog, 'utf8').trim().split('\n');
  writeFileSync(join(root, 'a-self.mjs'), `import './a-self.mjs';
import {appendFileSync} from 'node:fs';
appendFileSync(process.env.CHECKED_FILE_LOG, process.pid + '\\n');
`);
  const fence = `const depth = Number(process.env.STATIC_CI_TEST_DEPTH ?? 0);
if (!Number.isSafeInteger(depth) || depth < 0 || depth >= 2) { console.error('BOUNDED_OLD_RECURSION'); process.exit(86); }
process.env.STATIC_CI_TEST_DEPTH = String(depth + 1);
`;
  writeFileSync(join(root, 'scripts/static-ci.mjs'), audit + fence + old);
  commit('captured original checker with test-only depth fence');
  const negative = await check({runtime: process.env.BUN_BINARY ?? 'bun', args: [], extraEnv: {...extraEnv, STATIC_CI_TEST_DEPTH: '0'}});
  assert.notEqual(negative.status, 0);
  assert.match(negative.stderr, /BOUNDED_OLD_RECURSION/);
  assert.equal(new Set(processes()).size, 3, 'old checker launches at depths zero, one and the bounded fence');
  assert.ok(readFileSync(executionLog, 'utf8').length > 0, 'old checker executes the self-referencing input');
  rmSync(processLog); rmSync(executionLog);
  writeFileSync(join(root, 'scripts/static-ci.mjs'), audit + current);
  // Existing fixture Git history remains valid; the corrected parser must
  // refuse before consulting Git or executing a checked file.
  const refused = await check({runtime: process.env.BUN_BINARY ?? 'bun', args: [], extraEnv});
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /step=javascript-syntax.*recovery command: node --experimental-vm-modules scripts\/static-ci.mjs/);
  assert.doesNotMatch(refused.stderr, /BOUNDED_OLD_RECURSION/);
  assert.equal(processes().length, 1, 'Bun refusal starts exactly one checker process');
  rmSync(processLog);
  // This is the fixture's normal spawn, including when the suite itself runs
  // under Bun: it always selects Node and the shared in-process parser.
  const parsed = await check({extraEnv});
  assert.equal(parsed.status, 0, parsed.stdout + parsed.stderr);
  assert.equal(processes().length, 1, 'fixture parsing starts exactly one checker process');
  assert.throws(() => readFileSync(executionLog), {code: 'ENOENT'}, 'neither corrected entry point executes checked input');
 }));

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

 test('release whitespace preserves byte-exact transport artifacts and generated declarations', () => fixture(async ({root, tag, commit, check}) => {
  tag('acyclic-v0.1.0');
  const artifacts = {
   'rust/vendor/upstream/source.rs': '// upstream original \n\n',
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
