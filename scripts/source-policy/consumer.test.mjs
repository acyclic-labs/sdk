import assert from 'node:assert/strict';
import {readFileSync, realpathSync} from 'node:fs';
import test from 'node:test';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {fuzzProject} from '../no-polling.fuzz.mjs';
import {newProjectPollingSites} from './project.mjs';
import {pollingSites} from './index.mjs';

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
 const index = await import(moduleUrl(original
  .replace('createRequire(import.meta.url)', 'createRequire(' + JSON.stringify(indexUrl.href) + ')')
  .replace("require('typescript-parser')", '(() => { throw Error("runtime parser lookup"); })()')));
 const sourceUrl = moduleUrl(original
  .replace('createRequire(import.meta.url)', 'createRequire(' + JSON.stringify(indexUrl.href) + ')')
  .replace("require('typescript-parser')", '(() => { throw Error("runtime parser lookup"); })()'));
 const source = readFileSync(new URL('./project.mjs', import.meta.url), 'utf8')
  .replace("from 'typescript-parser'", 'from ' + JSON.stringify(import.meta.resolve('typescript-parser')))
  .replace("from './index.mjs'", 'from ' + JSON.stringify(sourceUrl));
 const graph = await import(moduleUrl(source));
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

test('Bash substitutions and heredocs preserve the following command', () => {
 const source = [
  String.raw`channel="$(sed -nE 's/^channel = "([^"]+)"$/\1/p' rust-toolchain.toml)"`,
  String.raw`IFS=$'\t' read -r label <<< "$channel"`,
  "node --input-type=module - <<'JS'",
  "// a payload's quote does not start shell syntax",
  'JS',
  'sleep 1',
 ].join('\n');
 assert.ok(pollingSites('control.sh', source).some(site => site.start === source.lastIndexOf('sleep')));
 assert.throws(() => pollingSites('control.sh', "cat <<'EOF'\nunterminated"), /unterminated shell here-document/);
});
