import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdirSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join, resolve} from 'node:path';
import test from 'node:test';
import {newPollingSites, pollingSites, sourceInScope, validateDispositions} from './no-polling.mjs';
import {newProjectPollingSites, projectPollingSites} from './no-polling-project.mjs';

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
  'fn wait() { unsafe { libc::pthread_cond_timedwait(condition, mutex, deadline); } }',
  'const FLUSH_INTERVAL: u64 = 10;',
 ]) assert.ok(newPollingSites(rust, '', source).length, source);
 for (const source of [
  'setTimeout(ready, 1);',
  "import {setTimeout as pause} from 'node:timers/promises'; await pause(1);",
  "import {scheduler as clock} from 'node:timers/promises'; await clock.wait(1);",
  'const pause = promisify(setTimeout); await pause(1);',
  'const pause = () => { const nested = promisify(setTimeout); return nested; }; await pause()(1);',
  'const later = setTimeout; later(ready, 1);',
  'await Bun.sleep(1);',
  'queueMicrotask(probe);',
  'requestAnimationFrame(probe);',
  'const clock = scheduler; await clock.yield();',
  'const atomic = Atomics; atomic.wait(state, 0, 0, 10);',
  'const wait = Atomics.wait; wait(state, 0, 0, 10);',
  "const pause = scheduler['y' + 'ield']; pause();",
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
  "const helper = await import(chosenModule); await helper.settle();",
  "const helper = require(chosenModule); helper.settle();",
  'async function bypass() { while (!await registry()) await pause(); } await bypass();',
 ]) assert.ok(newPollingSites(script, '', source).length, source);
});

test('unchanged helpers cannot hide behind renamed, namespace, default or re-exported bindings', () => {
 const helper = 'typescript/packages/example/test/helper.ts';
 const barrel = 'typescript/packages/example/test/barrel.ts';
 for (const [producer, relay, declaration, call] of [
  ['export async function settle() { await Bun.sleep(10); }', '', "import {settle as proceed} from './helper.js';", 'await proceed();'],
  ['export async function settle() { await Bun.sleep(10); }', '', "import * as producer from './helper.js';", 'await producer.settle();'],
  ['export default async function () { await Bun.sleep(10); }', '', "import proceed from './helper.js';", 'await proceed();'],
  ['export default async function original() { await Bun.sleep(10); }', '', "import proceed from './helper.js';", 'await proceed();'],
  ['async function settle() { await Bun.sleep(10); } export default settle;', '', "import proceed from './helper.js';", 'await proceed();'],
  ['export async function settle() { await Bun.sleep(10); }', "export {settle as proceed} from './helper.js';", "import {proceed} from './barrel.js';", 'await proceed();'],
  ['export async function settle() { await Bun.sleep(10); }', "export * from './helper.js';", "import {settle as proceed} from './barrel.js';", 'const chosen = proceed; await chosen();'],
  ['export const producer = {async settle() { await Bun.sleep(10); }};', '', "import {producer} from './helper.js';", 'const erased: {settle(): Promise<void>} = producer; await erased.settle();'],
  ['export const producer = {async settle() { await Bun.sleep(10); }};', '', "import * as namespace from './helper.js';", 'const {producer: chosen} = namespace; await chosen.settle();'],
  ['async function settle() { await Bun.sleep(10); } export const producer = {settle};', '', "import {producer} from './helper.js';", 'await producer.settle();'],
  ['export function settle() { if (!ready()) return settle(); }', '', "import {settle as proceed} from './helper.js';", 'proceed();'],
  ['export const settle = function () { if (!ready()) return settle(); };', '', "import {settle as proceed} from './helper.js';", 'proceed();'],
  ["import {scheduler as clock} from 'node:timers/promises'; export async function settle() { await clock.wait(10); }", '', "import {settle as proceed} from './helper.js';", 'await proceed();'],
 ]) {
  const before = new Map([[helper, producer], [barrel, relay], [script, declaration + '\nexport async function operation() {}']]);
  const after = new Map(before).set(script, declaration + '\nexport async function operation() { ' + call + ' }');
  assert.ok(newProjectPollingSites(before, after).some(site => site.file === script && site.kind === 'polling-helper-reference'), declaration);
 }
});

test('helper dependencies follow bindings rather than spelling or every export in a module', () => {
 const helper = 'typescript/packages/example/test/helper.ts';
 const event = 'typescript/packages/example/test/event.ts';
 const producer = 'export async function settle() { while (!ready()) await next(); }\nexport async function safe() { return originalEvent; }';
 for (const consumer of [
  "import {settle} from './event.js'; export async function operation() { await settle(); }",
  "import {safe} from './helper.js'; export async function operation() { await safe(); }",
  "import * as producer from './helper.js'; export async function operation() { await producer.safe(); }",
  "import {settle} from './helper.js'; export async function operation(settle) { await settle(); }",
 ]) {
  const before = new Map([[helper, producer], [event, 'export async function settle() { return originalEvent; }'], [script, 'export async function operation() {}']]);
  assert.deepEqual(newProjectPollingSites(before, new Map(before).set(script, consumer)), [], consumer);
 }
});

test('a producer edit reopens unchanged callers through recursive dependencies', () => {
 const first = 'typescript/packages/example/test/first.ts';
 const second = 'typescript/packages/example/test/second.ts';
 const unrelated = 'typescript/packages/example/test/unrelated.ts';
 const before = new Map([
  [first, "import {second} from './second.js'; export function first() { return second(); }"],
  [second, "import {first} from './first.js'; export function second() { if (ready()) return first(); return setTimeout(probe, 10); }"],
  [script, "import {first} from './first.js'; export function operation() { return first(); }"],
  [unrelated, 'export const value = 1;'],
 ]);
 assert.deepEqual(newProjectPollingSites(before, before), []);
 assert.deepEqual(newProjectPollingSites(before, new Map(before).set(second, '// retained source comment\n' + before.get(second))), []);
 assert.deepEqual(newProjectPollingSites(before, new Map(before).set(unrelated, 'export const value = 2;')), []);
 const changed = new Map(before).set(second, before.get(second).replace('ready()', 'otherReady()'));
 assert.ok(newProjectPollingSites(before, changed).some(site => site.file === script && site.kind === 'polling-helper-reference'));
 const accepted = projectPollingSites(before).map(site => disposition(site, 'event-wait', 'original producer', 'original producer loss', 'suppress the original event'));
 assert.ok(newProjectPollingSites(before, changed, accepted).some(site => site.file === script));
});

test('a legitimate helper requires an exact caller disposition rather than a helper exemption', () => {
 const helper = 'typescript/packages/example/test/helper.ts';
 for (const [producer, classification, event, terminal] of [
  ['export async function pause(context) { return context.timeout(deadline); }', 'caller-deadline', 'original caller chooses its deadline', 'caller cancellation or deadline'],
  ['export async function pause(error) { return transportBackoff(error); }', 'transient-transport-backoff', 'original classified transient transport failure', 'original transport owner cancellation'],
  ['export async function pause() { for (;;) { consume(await originalChannel.receive()); } }', 'event-wait', 'original channel producer', 'typed receive failure when that producer is dropped'],
 ]) {
  const before = new Map([[helper, producer], [script, "import {pause} from './helper.js'; export async function operation() {}"]]);
  const after = new Map(before).set(script, "import {pause} from './helper.js'; export async function operation() { await pause(original); }");
  const added = newProjectPollingSites(before, after);
  assert.ok(added.some(site => site.file === script && site.kind === 'polling-helper-reference'));
  const accepted = added.map(site => disposition(site, classification, event, terminal, 'remove or suppress the original event and require the original terminal result'));
  assert.deepEqual(newProjectPollingSites(before, after, accepted), []);
  assert.ok(newProjectPollingSites(before, new Map(after).set(script, after.get(script).replace('pause(original)', 'pause(replacement)')), accepted).length);
 }
});

test('module side effects and missing local producers remain reviewable dependencies', () => {
 const helper = 'typescript/packages/example/test/helper.ts';
 const before = new Map([[helper, 'while (!ready()) step();'], [script, 'export const value = 1;']]);
 const after = new Map(before).set(script, "import './helper.js'; export const value = 1;");
 assert.ok(newProjectPollingSites(before, after).some(site => site.file === script && site.kind === 'polling-module-initialization'));
 const missing = new Map([[script, "import {proceed} from './missing.js'; export function operation() { return proceed(); }"]]);
 assert.ok(newProjectPollingSites(new Map(), missing).some(site => site.kind === 'unresolved-local-helper'));
 const cyclicModules = new Map([
  [helper, "import './other.js'; export const first = 1;"],
  ['typescript/packages/example/test/other.ts', "import './helper.js'; export const second = 2;"],
  [script, "import {first} from './helper.js'; export const result = first;"],
 ]);
 assert.deepEqual(newProjectPollingSites(new Map(), cyclicModules), []);
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
 for (const file of [rust, script, 'typescript/packages/client/test/source.mts', 'typescript/packages/client/test/source.cts', 'typescript/packages/client/test/source.jsx', 'rust/crates/fs/native/adapter.c', 'typescript/packages/client/test/start.sh', 'scripts/release.mjs', 'scripts/native.ps1', 'rust/crates/fs/Cargo.toml', 'typescript/packages/client/settings.json']) assert.ok(sourceInScope(file));
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
  const helper = 'typescript/packages/example/test/helper.ts';
  const caller = 'typescript/packages/example/test/caller.ts';
  const beforeCaller = "import {settle} from './helper.js'; export async function operation() {}\n";
  write(helper, 'export async function settle() { await Bun.sleep(10); }\n');
  write(caller, beforeCaller);
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
  write(caller, beforeCaller.replace('operation() {}', 'operation() { await settle(); }'));
  const indirect = check('HEAD');
  assert.notEqual(indirect.status, 0);
  assert.match(indirect.output, /file=typescript\/packages\/example\/test\/caller.ts:1 kind=polling-helper-reference/);
  write(caller, beforeCaller);
  git(['rm', '-q', '--', helper]);
  const missingProducer = check('HEAD');
  assert.notEqual(missingProducer.status, 0);
  assert.match(missingProducer.output, /file=typescript\/packages\/example\/test\/caller.ts:1 kind=unresolved-local-helper/);
  git(['restore', '--staged', '--worktree', '--', helper]);
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
