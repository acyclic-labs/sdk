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
 const helper = 'lib/helper.ts', barrel = 'lib/barrel.ts', caller = 'svc/consumer.ts';
 const raw = new TextDecoder().decode(new Uint8Array(data));
 // Parser acceptance is determined by the same production unit boundary.
 // Accepted bytes must also survive graph construction, independent of order.
 let parsed = false;
 try { pollingUnit(helper, raw); parsed = true; }
 catch (error) { assert.ok(error instanceof SourceSyntaxError, String(error)); }
 const rawSources = new Map([[helper, raw], [caller, "import * as original from '../lib/helper.js'; export const value = original;"]]);
 if (parsed) {
  const sites = project(rawSources);
  const ordered = values => values.map(site => JSON.stringify(site)).sort();
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
 for (const [file, source] of [
  ['lib/broken.rs', '/*'], ['lib/broken.rs', 'r#"'],
  ['lib/broken.ps1', '<#'], ['lib/broken.ps1', '@"\n'],
  ['lib/broken.py', '"""'], ['lib/broken.py', '"'],
 ]) {
  const broken = new Map([[file, source]]);
  assert.throws(() => project(broken), SourceSyntaxError);
  assert.deepEqual(newProjectPollingSites(broken, new Map()), []);
  assert.ok(newProjectPollingSites(broken, new Map([[file, 'sleep(1)']])).length,
   'malformed predecessor authorized a current wait');
 }
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
  const untrusted = new Map([[packageFile, raw]]);
  if (valid) project(untrusted);
  else assert.throws(() => project(untrusted), SourceSyntaxError);
 }
}
