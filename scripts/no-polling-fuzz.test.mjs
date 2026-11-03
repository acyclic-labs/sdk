import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import * as targets from './no-polling.fuzz.mjs';

// Mutate the actual implementation; the unchanged fuzz oracle must reject it.
async function mutant(file, original, replacement) {
 const source = readFileSync(new URL(file, import.meta.url), 'utf8');
 assert.equal(source.split(original).length, 2, 'mutation must match one production branch');
 const mutated = source.replace(original, replacement)
  .replace("from 'typescript-parser'", 'from ' + JSON.stringify(import.meta.resolve('typescript-parser')))
  .replace("from './index.mjs'", 'from ' + JSON.stringify(new URL('./source-policy/index.mjs', import.meta.url).href));
 return import('data:text/javascript;base64,' + Buffer.from(mutated).toString('base64'));
}

test('source fuzz oracles reject planted decoder, graph and disposition defects', async () => {
 const lexer = await mutant('./source-policy/index.mjs', 'embeddedCandidate(spelling) ?', 'false ?');
 assert.throws(() => targets.fuzz(Buffer.from('original'), lexer.pollingSites), {name: 'AssertionError'});
 const graph = await mutant('./source-policy/project.mjs',
  'if (!unresolved && !component?.hazardous) continue;', 'if (!unresolved) continue;');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: graph.projectPollingSites}),
  {name: 'AssertionError', message: /lost dependency/});
 const policy = await mutant('./source-policy/index.mjs',
  "|| typeof item.producer !== 'string' || !item.producer.trim()", '|| false');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {validate: policy.validateDispositions}),
  {name: 'AssertionError', message: /Missing expected exception/});
 const manifest = await mutant('./source-policy/project.mjs',
  'throw new SourceSyntaxError(`${file}: ${error.message}`);', 'throw new Error(`${file}: ${error.message}`);');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: manifest.projectPollingSites}),
  {name: 'AssertionError', message: /SourceSyntaxError/});
 const layout = await mutant('./source-policy/project.mjs',
  'if (/(?:^|\\/)package\\.json$/.test(file)) {',
  'if (/^typescript\\/packages\\/[^/]+\\/package\\.json$/.test(file)) {');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: layout.projectPollingSites}),
  {name: 'AssertionError', message: /repository layout/});
});

test('raw source and project fuzz targets replay captured corpus and byte mutations', () => {
 for (const text of ["printf ''", '\\', '\\u{']) targets.fuzz(Buffer.from(text));
 for (let byte = 0; byte < 256; byte++) {
  targets.fuzz(Uint8Array.from({length: byte}, (_, index) => (byte + index) & 255));
 }
 // These are the original host/parser captures, with provenance in each file.
 for (const [capture, file] of [
  ['powershell-comments', 'control.ps1'], ['yaml-comments', 'control.yaml'],
  ['yaml-scripts', 'control.yml'], ['shell-words', 'control.sh'],
 ]) {
  const {records} = JSON.parse(readFileSync(new URL('./fixtures/no-polling/' + capture + '.json', import.meta.url), 'utf8'));
  for (const record of records) targets.fuzzSource(file, Buffer.from(record.source ?? record.word));
 }
 // Generated source mutations, not fabricated output from an external tool.
 for (const extension of ['js', 'mjs', 'cjs', 'ts', 'mts', 'cts', 'jsx', 'tsx', 'rs', 'c', 'h', 'cpp', 'cc', 'hh', 'hxx', 'hpp', 'cxx', 'cs', 'py', 'sh', 'bash', 'ps1', 'cmd', 'bat', 'proto', 'toml', 'json', 'yaml', 'yml']) {
  for (const seed of ['', '\\u{', '/*', '\r\n', String.fromCharCode(...Array.from({length: 256}, (_, byte) => byte))]) {
   targets.fuzzSource('control.' + extension, Buffer.from(seed));
  }
 }
 for (let byte = 0; byte < 8; byte++) targets.fuzzProject(Uint8Array.of(byte, 255 - byte));
 for (const json of ['null', '[]', '{}', '"ordinary"', '{"name":"@original/package"}']) targets.fuzzProject(Buffer.from(json));
});
