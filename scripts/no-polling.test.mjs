import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import test from 'node:test';
import {newPollingSites, pollingSites, sourceInScope, validateDispositions} from './no-polling.mjs';

const rust = 'rust/crates/example/src/lib.rs';
const script = 'typescript/packages/example/test/control.ts';
const disposition = (site, classification, producer, terminal, negativeControl) => ({
 file: site.file, fingerprint: site.fingerprint, classification, producer, terminal, negativeControl,
});

test('new direct and indirect readiness mutations are rejected', () => {
 // These are intentional bad source edits, not external-tool/provider captures.
 for (const source of [
  'fn wait() { std::thread::sleep(duration); }',
  'use tokio::time::sleep as pause; async fn wait() { pause(duration).await; }',
  'fn wait() { let pause = std::thread::sleep; pause(duration); }',
  'fn wait() { while !ready() {} }',
  'fn wait() { loop { if registry_probe() { break; } } }',
  'async fn wait() { tokio::task::yield_now().await; }',
  'fn wait() { child.try_wait(); }',
  'fn wait() { changed.wait_timeout(state, duration); }',
  'const FLUSH_INTERVAL: u64 = 10;',
 ]) assert.ok(newPollingSites(rust, '', source).length, source);
 for (const source of [
  'setTimeout(ready, 1);',
  "import {setTimeout as pause} from 'node:timers/promises'; await pause(1);",
  'const pause = promisify(setTimeout); await pause(1);',
  'const pause = () => { const nested = promisify(setTimeout); return nested; }; await pause()(1);',
  'const later = setTimeout; later(ready, 1);',
  'await Bun.sleep(1);',
  'while (!await ready()) await tick();',
  'for (let attempt = 0; attempt < 3; attempt++) await probe();',
  'await page.waitForFunction(() => ready);',
  'const unused = `hello ${setInterval(probe, 1)}`;',
  "const unused = /[//]/; setTimeout(probe, 1);",
  String.raw`\u0073etTimeout(probe, 1);`,
  "globalThis['setTimeout'](probe, 1);",
  "const pause = globalThis['set' + ('Time' + 'out')]; pause(probe, 1);",
  'const pause = globalThis[chosenName]; pause(probe, 1);',
  'const host = globalThis; host[chosenName](probe, 1);',
  "Reflect.get(globalThis, ['set','Timeout'].join(''))(probe, 1);",
  "const generate = new Function('return ' + name); generate()();",
  'async function bypass() { while (!await registry()) await pause(); } await bypass();',
 ]) assert.ok(newPollingSites(script, '', source).length, source);
});

test('aliases cannot use an unchanged timer import to conceal a new call', () => {
 for (const declaration of [
  "import {setTimeout as pause} from 'node:timers/promises';",
  'const pause = promisify(setTimeout);',
 ]) {
  const before = declaration + '\n' + 'const padding = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16];\n';
  assert.ok(newPollingSites(script, before, before + 'await pause(1);').some(site => site.line === 3));
 }
});

test('comments and Future polling remain distinct from time and readiness rechecks', () => {
 assert.deepEqual(pollingSites(rust, `
 // while sleep timeout retry
 /* outer /* nested sleep */ while */
 fn drive<'a>(future: &'a mut Future) { future.poll(context); }
 `), []);
 assert.deepEqual(pollingSites(script, '// setTimeout retry while\nconst pattern = /[//]sleep/;'), []);
 assert.deepEqual(pollingSites(script, 'const name = globalThis["crypto"]; name.getRandomValues(bytes);'), []);
 assert.ok(pollingSites(rust, 'fn fixture() { command.arg(r#"sleep 30"#); }').some(site => site.kind === 'embedded-timer-or-retry'));
});

test('unchanged baseline sites do not authorize another occurrence', () => {
 const source = 'async function operation() { await Bun.sleep(1); }\n';
 assert.deepEqual(newPollingSites(script, source, source), []);
 const repeated = source.repeat(4);
 assert.ok(newPollingSites(script, source, repeated).length);
 assert.ok(newPollingSites(script, repeated, repeated + source).length);
});

test('legitimate deadline and transport sites require exact typed dispositions', () => {
 for (const [source, classification, producer, terminal] of [
  ['await context.timeout(deadline);', 'caller-deadline', 'original caller deadline', 'expiry or caller cancellation'],
  ['await transportBackoff(failure);', 'transient-transport-backoff', 'classified original transient transport failure', 'transport owner cancellation'],
 ]) {
  const sites = pollingSites(script, source);
  assert.ok(sites.length);
  const accepted = sites.map(site => disposition(site, classification, producer, terminal, 'same original request without an explicit deadline/failure creates no timer'));
  assert.deepEqual(newPollingSites(script, '', source, accepted), []);
  assert.ok(newPollingSites(script, source, source.repeat(4), accepted).length);
  assert.ok(newPollingSites(script, '', source.replace('deadline', 'otherDeadline').replace('failure', 'otherFailure'), accepted).length);
 }
});

test('event and finite-work loops have no file or helper exemption', () => {
 const source = 'loop { let event = receiver.recv()?; consume(event); }';
 const site = pollingSites(rust, source).find(site => site.kind === 'loop');
 assert.ok(site);
 const accepted = [disposition(site, 'event-wait', 'original operation sender', 'RecvError on sender drop', 'drop the original sender before receive')];
 assert.deepEqual(newPollingSites(rust, '', source, accepted), []);
 assert.ok(newPollingSites(rust, '', source.replace('receiver.recv()?', 'registry_ready()'), accepted).length);
 const distant = 'loop { let values = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]; receiver.recv()?; }';
 assert.ok(newPollingSites(rust, distant, distant.replace('receiver.recv()?', 'ready()')).length);
 assert.throws(() => newPollingSites(rust, '', source, [{...accepted[0], file: 'rust/crates/*', fingerprint: '*'}]), /invalid exact/);
 assert.throws(() => newPollingSites(rust, '', source, [{...accepted[0], terminal: ''}]), /invalid exact/);
 assert.throws(() => newPollingSites(rust, '', source, [...accepted, ...accepted]), /duplicate/);
});

test('scope covers native adapters and scripts as well as both language packages', () => {
 for (const file of [rust, script, 'rust/crates/fs/native/adapter.c', 'typescript/packages/client/test/start.sh', 'scripts/release.mjs', 'scripts/native.ps1', 'rust/crates/fs/Cargo.toml', 'typescript/packages/client/settings.json']) assert.ok(sourceInScope(file));
 assert.equal(sourceInScope('docs/no-polling.md'), false);
 assert.equal(sourceInScope('scripts/no-polling-dispositions.json'), false);
 assert.throws(() => validateDispositions({ignore: ['scripts/**']}), /must be an array/);
 assert.ok(pollingSites('typescript/packages/client/settings.json', '{"poll_interval": 10}').length);
});

test('the entry point compares real Git objects and rejects modified and untracked sources', () => {
 const root = mkdtempSync(join(tmpdir(), 'sdk-no-polling-'));
 const checker = resolve('scripts/check-no-polling.mjs');
 const absent = process.platform === 'win32' ? 'NUL' : '/dev/null';
 const env = {...process.env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: absent, GIT_CONFIG_COUNT: '0'};
 const git = args => execFileSync('git', args, {cwd: root, env, maxBuffer: Infinity});
 const write = (file, source) => { mkdirSync(dirname(join(root, file)), {recursive: true}); writeFileSync(join(root, file), source); };
 const check = base => {
  try {
   return {status: 0, output: execFileSync(process.execPath, [checker, base], {cwd: root, env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: Infinity})};
  } catch (error) {
   return {status: error.status, output: String(error.stdout) + String(error.stderr)};
  }
 };
 try {
  // The fixture invokes Git itself; no Git response or object header is mocked.
  git(['init', '-q', '--initial-branch=control', '--template=']);
  write(rust, 'fn current() {}\n');
  write('scripts/no-polling-dispositions.json', '[]\n');
  git(['add', '.']);
  git(['-c', 'user.name=Contract', '-c', 'user.email=contract@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '-q', '-m', 'source baseline']);
  assert.equal(check('HEAD').status, 0);
  write(rust, 'fn current() { std::thread::sleep(duration); }\n');
  const modified = check('HEAD');
  assert.notEqual(modified.status, 0);
  assert.match(modified.output, /step=no-polling file=rust\/crates\/example\/src\/lib.rs:1/);
  assert.match(modified.output, /recovery command: node scripts\/check-no-polling.mjs "HEAD"/);
  write(rust, 'fn current() {}\n');
  write(script, 'setTimeout(probe, 1);\n');
  const untracked = check('HEAD');
  assert.notEqual(untracked.status, 0);
  assert.match(untracked.output, /step=no-polling file=typescript\/packages\/example\/test\/control.ts:1/);
  const missingBase = check('missing-source-baseline');
  assert.notEqual(missingBase.status, 0);
  assert.match(missingBase.output, /step=no-polling.*recovery command:/s);
 } finally {
  assert.equal(resolve(dirname(root)), resolve(tmpdir()));
  rmSync(root, {recursive: true, force: true});
 }
});
