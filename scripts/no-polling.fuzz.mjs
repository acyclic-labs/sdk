import assert from 'node:assert/strict';
import {newPollingSites, pollingSites, pollingUnit, SourceSyntaxError, supportedSource, validateDispositions} from './source-policy/index.mjs';
import {newProjectPollingSites, projectPollingSites} from './source-policy/project.mjs';

// Byte-oriented source mutations, not captured tool output. Exercise the real
// tokenizer: decoding must preserve raw candidates and expose escaped ones.
export function fuzz(data, analyze = pollingSites) {
 const text = new TextDecoder().decode(new Uint8Array(data));
 for (const command of ['sleep`' + text + '` 1', 'Start-S`leep ' + text,
  'Thread.' + '\\'.repeat(data.length + 1) + 'u0053leep(' + text + ')']) {
  const source = 'runHost(' + JSON.stringify(command) + ');';
  assert.ok(analyze('scripts/control.mjs', source).some(site => site.kind === 'embedded-timer-or-retry'), source);
 }
 const comment = '<#' + text.replaceAll('#', '') + '#>';
 assert.deepEqual(analyze('scripts/control.ps1', comment), []);
 const source = comment + ' Start-Sleep 1';
 assert.deepEqual(analyze('scripts/control.ps1', source).map(site => site.start), [source.lastIndexOf('Sleep')]);
 assert.throws(() => analyze('scripts/control.ps1', comment.slice(0, -2)), /unterminated block comment/);
 const word = 'value' + Buffer.from(data).toString('hex');
 for (const expression of ["globalThis['wait']", "globalThis['set' + 'Timeout']", "globalThis[('setTimeout')]",
  'globalThis[`setTimeout`]', 'globalThis[original]']) {
  assert.ok(analyze('scripts/control.mjs', expression + '(' + JSON.stringify(word) + ');').length);
 }
 assert.ok(analyze('scripts/control.mjs', "import {setTimeout as pause} from 'node:timers'; pause(" + JSON.stringify(word) + ');').length);
 for (const [file, literal] of [
  ['scripts/control.rs', `r#"${word}"#`], ['scripts/control.rs', "'x'"],
  ['scripts/control.ps1', `@"\n${word}\n"@`], ['scripts/control.py', `"""${word}"""`],
 ]) assert.ok(analyze(file, literal + '\nsleep(1)').some(site => site.kind === 'timer-or-readiness'));
 const sqlitePolicy = ['busy_timeout', 'busy_handler', 'sqlite3_busy_timeout', 'sqlite3_busy_handler'][data.length % 4];
 const sqliteSource = sqlitePolicy.startsWith('sqlite3_') ? `${sqlitePolicy}(db, ${word});` : `connection.${sqlitePolicy}(${word});`;
 assert.ok(analyze('lib/storage/control.' + (sqlitePolicy.startsWith('sqlite3_') ? 'c' : 'rs'), sqliteSource).length, sqliteSource);
 assert.ok(analyze('scripts/control.mjs', 'runHost(' + JSON.stringify(sqliteSource) + ')')
  .some(site => site.kind === 'embedded-timer-or-retry'), sqliteSource);
 for (const program of [`while ! ${word}; do step; done`, `for item in ${word}; do step; done`]) {
  for (const [file, source] of [
   ['scripts/control.yaml', 'run: |\n  ' + program],
   ['scripts/control.yml', 'run: ' + JSON.stringify(program)],
   ['scripts/control.mjs', 'runHost(' + JSON.stringify(program) + ')'],
  ]) assert.ok(analyze(file, source).some(site => site.kind === 'embedded-loop'), source);
 }
 const settings = `settings: [${word}\u00a0#owner's"fragment, {timeout: 1}]`;
 assert.deepEqual(analyze('scripts/control.yaml', settings).map(site => site.start), [settings.indexOf('timeout')]);
 for (const command of ['\\sleep', 'sl\\eep', 'sl\\\neep', 's\\l\\e\\e\\p']) {
  const source = `${command} ${word}`;
  assert.ok(analyze('scripts/control.sh', source).some(site => site.kind === 'timer-or-readiness'), source);
  assert.ok(analyze('scripts/control.mjs', 'runHost(' + JSON.stringify(source) + ')')
   .some(site => site.kind === 'embedded-timer-or-retry'), source);
 }
 for (const [extension, command, wait, escape] of [['ps1', 'Write-Output', 'Start-Sleep', '`'], ['sh', 'printf', 'sleep', '\\']]) {
  const arguments_ = [word + '#fragment', escape + '#fragment', word + escape + ' #fragment',
   word + escape + '\n#fragment', word + '"quoted"#fragment'];
  if (extension === 'ps1') arguments_.push(word + '<#fragment', word + '`\r\n#fragment');
  for (const argument of arguments_) {
   const source = `${command} ${argument}; ${wait} 1`;
   assert.deepEqual(analyze(`scripts/control.${extension}`, source).map(site => site.start), [source.indexOf(';') + 2 + wait.indexOf(extension === 'ps1' ? 'Sleep' : 'sleep')], source);
  }
  assert.deepEqual(analyze(`scripts/control.${extension}`, `${command} ${escape}\n# ${wait} 1`), []);
 }
}

// Raw untrusted bytes reach every caller-selected production language parser.
// Syntax refusal is valid; a crash or a diagnostic of another type is not.
export function fuzzSource(file, data) {
 assert.ok(supportedSource(file));
 const source = new TextDecoder().decode(new Uint8Array(data));
 let sites;
 try { sites = pollingSites(file, source); }
 catch (error) { assert.ok(error instanceof SourceSyntaxError, String(error)); return; }
 assert.deepEqual(newPollingSites(file, source, source), []);
 for (const site of sites) {
  assert.ok(Number.isInteger(site.start) && site.start >= 0 && site.start < source.length);
  assert.equal(site.line, source.slice(0, site.start).split('\n').length);
 }
}

// Exercise the shipped graph and exact-disposition boundary, not a graph model.
// The byte seed varies bindings, occurrence counts and the original source.
export function fuzzProject(data, {project = projectPollingSites, validate = validateDispositions} = {}) {
 const variant = (data[0] ?? 0) % 8;
 const word = 'value' + Buffer.from(data).toString('hex');
 const ordered = values => values.map(site => JSON.stringify(site)).sort();
 // Distinct Unicode paths can collate equally. A recursive component must keep
 // exactly the same review identity when the snapshot enumerates them in reverse.
 const composed = ['\u00e4', '\u00f6', '\u00e9', '\u00c5'][variant % 4];
 const decomposed = composed.normalize('NFD');
 const recursive = new Map([
  [`lib/${composed}.ts`, `import {run as next} from './${decomposed}.js'; export function run() { return next(); }`],
  [`lib/${decomposed}.ts`, `import {run as next} from './${composed}.js'; export function run() { return next(); }`],
 ]);
 assert.deepEqual(ordered(project(recursive)), ordered(project(new Map([...recursive].reverse()))),
  'snapshot enumeration changed recursive dependency identity');
 const helper = 'lib/helper.ts', barrel = 'lib/barrel.ts', caller = 'svc/consumer.ts';
 const load = ["import '../lib/helper.js';", "import original = require('../lib/helper.js');",
  "await import('../lib/helper.js');", "require('../lib/helper.js');"][variant % 4];
 const loaded = new Map([[helper, 'Bun.sleep(1); export const value = 1;'], [caller, load]]);
 const changedLoad = new Map(loaded).set(helper, 'Bun.sleep(2); export const value = 1;');
 const loadSites = project(loaded);
 const changedLoadSites = project(changedLoad);
 assert.ok(changedLoadSites.some(site => site.file === caller && site.kind === 'polling-module-initialization'),
  'lost runtime module initialization dependency');
 const loadFingerprints = new Set(loadSites.filter(site => site.file === caller).map(site => site.fingerprint));
 assert.ok(changedLoadSites.some(site => site.file === caller && !loadFingerprints.has(site.fingerprint)),
  'changed runtime module did not reopen its unchanged caller');
 assert.ok(project(new Map([[caller, load]])).some(site => site.file === caller && site.kind === 'unresolved-local-helper'),
  'missing runtime module producer was accepted');
 const exact = new Map(loaded).set('lib/helper.js', 'Bun.sleep(3); export const value = 1;');
 assert.ok(newProjectPollingSites(exact, new Map(exact).set('lib/helper.js', 'Bun.sleep(4);')).some(site => site.file === caller),
  'exact runtime source edit did not reopen its caller');
 assert.ok(!newProjectPollingSites(exact, new Map(exact).set(helper, 'Bun.sleep(4);')).some(site => site.file === caller),
  'shadowed source changed the exact runtime dependency');
 const typeOnly = new Map(loaded).set(caller, "import type original = require('../lib/helper.js');");
 assert.deepEqual(project(typeOnly).filter(site => site.file === caller), [], 'type-only import executed its producer');
 const passiveCycle = new Map([[helper, "import '../svc/consumer.js'; export const value = 1;"],
  [caller, "import '../lib/helper.js'; export const value = 2;"]]);
 assert.deepEqual(project(passiveCycle), [], 'passive module cycle became callable work');
 // These source extensions are accepted by the shipped parser. Relative
 // extensionless imports must retain the same producer identity as explicit ones.
 const relativeHelper = ['lib/helper.jsx', 'lib/helper/index.tsx', 'lib/helper/index.jsx', 'lib/helper/entry.ts'][variant % 4];
 const relativeLoad = "import {settle} from '../lib/helper'; settle();";
 const relative = new Map([[relativeHelper, 'export function settle() { return Bun.sleep(1); }'], [caller, relativeLoad]]);
 if (variant % 4 === 3) relative.set('lib/helper/package.json', '{"main":"entry.js"}');
 assert.ok(project(relative).some(site => site.file === caller && site.kind === 'polling-helper-reference'),
  'relative source resolution lost dependency on its producer');
 assert.ok(newProjectPollingSites(relative, new Map(relative).set(relativeHelper,
  'export function settle() { return Bun.sleep(2); }')).some(site => site.file === caller),
  'relative producer edit did not reopen its caller');
 const raw = new TextDecoder().decode(new Uint8Array(data));
 // Parser acceptance is determined by the same production unit boundary.
 // Accepted bytes must also survive graph construction, independent of order.
 let parsed = false;
 try { pollingUnit(helper, raw); parsed = true; }
 catch (error) { assert.ok(error instanceof SourceSyntaxError, String(error)); }
 const rawSources = new Map([[helper, raw], [caller, "import * as original from '../lib/helper.js'; export const value = original;"]]);
 if (parsed) {
  const sites = project(rawSources);
  assert.deepEqual(ordered(project(new Map([...rawSources].reverse()))), ordered(sites));
  assert.deepEqual(newProjectPollingSites(rawSources, rawSources), []);
 } else assert.throws(() => project(rawSources), SourceSyntaxError);
 const [binding, call, producer = 'export function settle() { return Bun.sleep(1); } export default settle;'] = [
  ["import {settle as invoke} from '../lib/helper.js';", 'invoke()'],
  ["import invoke from '../lib/helper.js';", 'invoke()'],
  ["import * as original from '../lib/helper.js';", 'original.settle()'],
  ["import {forward as invoke} from '../lib/barrel.js';", 'invoke()'],
  ["import * as original from '../lib/helper.js'; const {settle: invoke} = original;", 'invoke()'],
  ["import {producer as original} from '../lib/helper.js';", 'original.settle()',
   'export const producer = {settle() { return Bun.sleep(1); }};'],
  ["import invoke from '../lib/helper.js';", 'invoke()', 'export default function () { return Bun.sleep(1); }'],
  ["import {settle as invoke} from '../lib/helper.js';", 'invoke()',
   'export const settle = function () { return Bun.sleep(1); };'],
 ][variant];
 const before = new Map([
  [helper, producer],
  [barrel, "export {settle as forward} from './helper.js';"], [caller, binding],
 ]);
 const after = new Map(before).set(caller, binding + ` export const ${word} = ${call};`);
 assert.ok(project(after).some(site => site.file === caller && site.kind === 'polling-helper-reference'), 'lost dependency');
 assert.ok(newProjectPollingSites(before, after).some(site => site.file === caller), 'new reference inherited approval');
 assert.deepEqual(newProjectPollingSites(after, after), []);
 const accepted = projectPollingSites(after).map(site => ({file: site.file, fingerprint: site.fingerprint,
  classification: 'caller-deadline', producer: 'original caller deadline', terminal: 'original cancellation',
  negativeControl: 'change the original source or remove its producer'}));
 validate(accepted);
 assert.deepEqual(newProjectPollingSites(new Map(), after, accepted), []);
 const localSource = `setTimeout(${word}, 1);`;
 const localSites = pollingSites(caller, localSource);
 const localAccepted = localSites.map(site => ({...accepted[0], file: caller, fingerprint: site.fingerprint}));
 assert.deepEqual(newPollingSites(caller, '', localSource, [...accepted, ...localAccepted]), []);
 assert.ok(newPollingSites(caller, '', localSource.replace(', 1)', ', 2)'), [...accepted, ...localAccepted]).length);
 const changed = new Map(after).set(helper, before.get(helper).replace('sleep(1)', `sleep(${data.length + 2})`));
 assert.ok(newProjectPollingSites(new Map(), changed, accepted).some(site => site.file === caller), 'stale disposition authorized changed dependency');
 const missing = new Map(after);
 missing.delete(helper);
 assert.ok(project(missing).some(site => site.file === caller && site.kind === 'unresolved-local-helper'), 'lost producer was accepted');
 const malformed = new Map(after).set(helper, before.get(helper) + '}');
 assert.throws(() => project(malformed), SourceSyntaxError);
 const broken = new Map([
  [`lib/${word}.rs`, '\n/*'], [`lib/raw-${word}.rs`, 'r#"'],
  [`lib/${word}.ps1`, '<#'], [`lib/here-${word}.ps1`, '@"\n'],
  [`lib/${word}.py`, '"""'], [`lib/quoted-${word}.py`, '\n"'],
  [`lib/${word}.ts`, '}'], [`tools/${word}/package.json`, '{"name":'],
 ].slice(0, variant + 2));
 const causes = [...broken].map(([file, source]) => {
  let cause;
  assert.throws(() => project(new Map([[file, source]])), error => {
   assert.ok(error instanceof SourceSyntaxError);
   cause = error.message;
   return true;
  });
  return cause;
 });
 const allCauses = error => {
  assert.ok(error instanceof AggregateError, 'malformed source causes lost their aggregate');
  assert.ok(error.errors.every(cause => cause instanceof SourceSyntaxError));
  assert.deepEqual(error.errors.map(cause => cause.message), causes, 'malformed source causes were dropped or reordered');
  assert.equal(error.message, causes.join('\n'));
  return true;
 };
 assert.throws(() => project(broken), allCauses);
 assert.throws(() => newProjectPollingSites(broken, broken), allCauses);
 assert.deepEqual(newProjectPollingSites(broken, new Map()), []);
 const repaired = new Map([[helper, 'export function settle() { return 1; }'],
  [caller, "import {settle} from '../lib/helper.js'; settle();"]]);
 const invalidPredecessor = new Map(repaired).set(helper, repaired.get(helper) + '}');
 assert.deepEqual(newProjectPollingSites(invalidPredecessor, repaired), []);
 const currentWait = new Map(repaired).set(helper, repaired.get(helper).replace('return 1', `return Bun.sleep(${data.length + 1})`));
 assert.ok(newProjectPollingSites(invalidPredecessor, currentWait).some(site => site.file === caller),
  'malformed predecessor authorized a current wait');
 for (const field of ['file', 'fingerprint', 'classification', 'producer', 'terminal', 'negativeControl']) {
  const invalid = {...accepted[0], [field]: ''};
  assert.throws(() => validate([invalid]), /invalid exact/);
 }
 assert.throws(() => validate([accepted[0], accepted[0]]), /duplicate/);
 const source = 'async function operation() { await Bun.sleep(1); }\n';
 const repeated = source.repeat(variant + 2);
 assert.deepEqual(newPollingSites(caller, source, source), []);
 assert.ok(newPollingSites(caller, source, repeated).length);
 assert.ok(newPollingSites(caller, repeated, repeated + source).length);
 for (const root of ['typescript/packages/original', 'tools/original']) {
  const packageFile = root + '/package.json';
  const name = '@original/' + word;
  const local = new Map([[packageFile, JSON.stringify({name})],
   [caller, `import {settle} from '${name}'; settle();`]]);
  assert.ok(project(local).some(site => site.file === caller && site.kind === 'unresolved-local-helper'),
   'repository layout concealed a missing local producer');
  const manifest = new Map([[packageFile, '{"name":']]);
  assert.throws(() => project(manifest), SourceSyntaxError);
  assert.deepEqual(newProjectPollingSites(manifest, new Map()), []);
  project(new Map([[packageFile, 'null']]));
  let valid = true;
  try { JSON.parse(raw); } catch { valid = false; }
  const untrusted = new Map([[packageFile, raw], [root + '/index.ts', 'export const value = 1;'],
   [caller, `import '../${root}';`]]);
  if (valid) project(untrusted);
  else assert.throws(() => project(untrusted), SourceSyntaxError);
 }
}
