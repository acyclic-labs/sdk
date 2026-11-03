import assert from 'node:assert/strict';
import {newPollingSites, pollingSites, SourceSyntaxError, supportedSource, validateDispositions} from './source-policy/index.mjs';
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
 const variant = (data[0] ?? 0) % 4;
 const word = 'value' + Buffer.from(data).toString('hex');
 const helper = 'lib/helper.ts', barrel = 'lib/barrel.ts', caller = 'svc/consumer.ts';
 const [binding, call] = [
  ["import {settle as invoke} from '../lib/helper.js';", 'invoke()'],
  ["import invoke from '../lib/helper.js';", 'invoke()'],
  ["import * as original from '../lib/helper.js';", 'original.settle()'],
  ["import {forward as invoke} from '../lib/barrel.js';", 'invoke()'],
 ][variant];
 const before = new Map([
  [helper, 'export function settle() { return Bun.sleep(1); } export default settle;'],
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
}
