import assert from 'node:assert/strict';
import {readFileSync, realpathSync} from 'node:fs';
import test from 'node:test';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {fuzzProject} from '../no-polling.fuzz.mjs';
import {newProjectPollingSites} from './project.mjs';

const moduleUrl = source => 'data:text/javascript;base64,' + Buffer.from(source).toString('base64');
const indexUrl = new URL('./index.mjs', import.meta.url);

test('source-only command preserves the recorded producer/consumer graph', () => {
 const declared = resolve('node_modules/typescript-parser/lib/typescript.js');
 assert.equal(fileURLToPath(import.meta.resolve('typescript-parser')),
  process.execArgv.includes('--preserve-symlinks') ? declared : realpathSync(declared),
  'the declared runtime package must own parser lookup under the active Node loader');
 const capture = JSON.parse(readFileSync(new URL('../fixtures/no-polling/actor-project.json', import.meta.url), 'utf8'));
 const before = new Map(capture.sources);
 const [producer, consumer] = capture.files;
 const after = new Map(before).set(producer, before.get(producer).replace('this.#initialize = initialize;', 'this.#initialize = initialize; Bun.sleep(1);'));
 const run = request => spawnSync(process.execPath, [fileURLToPath(new URL('./check.mjs', import.meta.url)),
  fileURLToPath(import.meta.resolve('typescript-parser'))], {input: JSON.stringify(request), encoding: 'utf8'});
 const request = {before: [...before], after: [...after]};
 const result = run(request);
 assert.equal(result.status, 0, result.stderr);
 const actual = JSON.parse(result.stdout);
 assert.deepEqual(actual, newProjectPollingSites(before, after));
 assert.ok(actual.some(site => site.file === consumer && site.kind === 'polling-helper-reference'));
 assert.ok(actual.some(site => site.file === producer && site.kind === 'timer-or-readiness'));
 assert.equal(run({...request, after: [...after, [...after][0]]}).status, 1);
});

test('closed project consumers use the original parser and production graph oracle', async () => {
 const original = readFileSync(indexUrl, 'utf8');
 // Counterfactual production branch: a closed artifact has no installed
 // package to resolve. The parser and the graph themselves remain original.
 const sourceUrl = moduleUrl(original
  .replace('createRequire(import.meta.url)', 'createRequire(' + JSON.stringify(indexUrl.href) + ')')
  .replace("require('typescript-parser')", '(() => { throw Error("runtime parser lookup"); })()'));
 const source = readFileSync(new URL('./project.mjs', import.meta.url), 'utf8')
  .replace("from 'typescript-parser'", 'from ' + JSON.stringify(import.meta.resolve('typescript-parser')))
  .replace("from './index.mjs'", 'from ' + JSON.stringify(sourceUrl));
 const [index, graph] = await Promise.all([import(sourceUrl), import(moduleUrl(source))]);
 const replay = api => fuzzProject(Uint8Array.of(0), {source: index, graph: api});
 assert.doesNotThrow(() => replay(graph));
 // Delete precisely the parser handoff in production. The unchanged oracle
 // must fail, including the recorded Actor constructor/dependency snapshots.
 // Use an independently loaded lexer so the successful pass cannot retain its
 // parser and conceal a missing dependency in the negative control.
 const freshUrl = moduleUrl(original
  .replace('createRequire(import.meta.url)', 'createRequire(' + JSON.stringify(indexUrl.href) + ')')
  .replace("require('typescript-parser')", '(() => { throw Error("runtime parser lookup"); })()') + '\n// independent control');
 const negative = await import(moduleUrl(source.replace(JSON.stringify(sourceUrl), JSON.stringify(freshUrl))
  .replace('pollingUnit(file, source, ts)', 'pollingUnit(file, source)')));
 assert.throws(() => replay(negative), /runtime parser lookup/);
});

// A site is new only where the change adds a timer or a chain newly reaches one. An edit to a module on an unchanged
// chain (an unrelated declaration, a moved function, a timer's argument, a dormant body, module state) reports
// nothing, however it changes the module's text (2026-11-21: one edited shared Cloud test module re-reported 375
// unchanged helper references in its import cone).
test('an edit to a module on an unchanged chain reports nothing; a new timer or a chain newly reaching one reports one', () => {
 const producer = 'lib/helper.ts', consumer = 'svc/consumer.ts';
 const helper = 'const ticks = 1; export function settle() { return Bun.sleep(ticks); }\nexport function unrelated() { return true; }';
 const before = new Map([[producer, helper], [consumer, "import {settle} from '../lib/helper.js'; export const out = settle();"]]);
 const edit = source => newProjectPollingSites(before, new Map(before).set(producer, source));
 for (const unchanged of [
  helper.replace('return true', 'return false'),
  'export const label = "unrelated";\n' + helper.split('\n').reverse().join('\n'),
  helper.replace('ticks = 1', 'ticks = 2'),
  'globalThis.ready = true; ' + helper,
 ]) assert.deepEqual(edit(unchanged), [], unchanged);
 const added = edit(helper.replace('return true', 'return setTimeout(wake, 5)'));
 assert.deepEqual(added.map(site => [site.file, site.token]), [[producer, 'setTimeout']]);
 const quiet = new Map(before).set(consumer, "import {unrelated} from '../lib/helper.js'; export const out = unrelated();");
 const reached = newProjectPollingSites(quiet, new Map(quiet).set(consumer, "import {settle} from '../lib/helper.js'; export const out = settle();"));
 assert.deepEqual(reached.map(site => [site.file, site.kind, site.token]), [[consumer, 'polling-helper-reference', 'settle']]);
 const loaded = new Map([[producer, 'Bun.sleep(1); export function dormant() { return true; }'], [consumer, "import '../lib/helper.js';"]]);
 assert.deepEqual(newProjectPollingSites(loaded, new Map(loaded).set(producer, 'Bun.sleep(2); export function dormant() { return false; }')), []);
});

// A literal that is data (a list of metadata key names) is its own site, never a timer origin to what imports it.
test('a data literal is no timer origin to its dependents', () => {
 const producer = 'lib/diagnostic.ts', consumer = 'svc/consumer.ts';
 const sources = new Map([[producer, "const metadata = ['exitCode', 'retryAfterMs']; export function keys() { return metadata; }"],
  [consumer, "import {keys} from '../lib/diagnostic.js'; export const out = keys();"]]);
 assert.ok(!newProjectPollingSites(new Map(), sources).some(site => site.file === consumer));
 const command = new Map(sources).set(producer, "export function keys() { return run('sleep 5'); }");
 assert.ok(newProjectPollingSites(new Map(), command).some(site => site.file === consumer), 'a command literal lost its dependents');
});
