import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import {fuzzProject} from '../no-polling.fuzz.mjs';

const moduleUrl = source => 'data:text/javascript;base64,' + Buffer.from(source).toString('base64');
const indexUrl = new URL('./index.mjs', import.meta.url);

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
