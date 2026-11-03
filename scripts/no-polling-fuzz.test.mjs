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
 for (const [original, replacement, file, source] of [
  ['throw new SourceSyntaxError(', 'throw new Error(', 'control.ts', '}'],
  ['const malformed = message => new SourceSyntaxError(', 'const malformed = message => new Error(', 'control.ps1', '<#'],
  ['sites.push({file, line: t.line, kind, fingerprint, start: t.start});',
   'sites.push({file, line: 0, kind, fingerprint, start: t.start});', 'control.ts', 'Bun.sleep(1);'],
  ['sites.push({file, line: t.line, kind, fingerprint, start: t.start});',
   'sites.push({file, line: t.line, kind, fingerprint, start: -1});', 'control.ts', 'Bun.sleep(1);'],
  ['sites.push({file, line: t.line, kind, fingerprint, start: t.start});',
   "sites.push({file: 'foreign.ts', line: t.line, kind, fingerprint, start: t.start});", 'control.ts', 'Bun.sleep(1);'],
 ]) {
  const parser = await mutant('./source-policy/index.mjs', original, replacement);
  assert.throws(() => targets.fuzzSource(file, Buffer.from(source), parser), {name: 'AssertionError'});
 }
 const lexer = await mutant('./source-policy/index.mjs', 'embeddedCandidate(spelling) ?', 'false ?');
 assert.throws(() => targets.fuzz(Buffer.from('original'), lexer.pollingSites), {name: 'AssertionError'});
 const graph = await mutant('./source-policy/project.mjs',
  'if (!unresolved && !component?.hazardous) continue;', 'if (!unresolved) continue;');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: graph.projectPollingSites}),
  {name: 'AssertionError', message: /lost dependency/});
 const collation = await mutant('./source-policy/project.mjs',
  'a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0', 'a[0].localeCompare(b[0])');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: collation.projectPollingSites}),
  {name: 'AssertionError', message: /snapshot enumeration/});
 const resolution = await mutant('./source-policy/project.mjs',
  'ts.resolveModuleName(name, containingFile, options, host, moduleCache).resolvedModule',
  "name.endsWith('/helper') ? undefined : ts.resolveModuleName(name, containingFile, options, host, moduleCache).resolvedModule");
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: resolution.projectPollingSites}),
  {name: 'AssertionError', message: /relative source resolution/});
 const declarations = await mutant('./source-policy/project.mjs',
  'script(file) && !unit.parsed?.isDeclarationFile', 'script(file)');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: declarations.projectPollingSites}),
  {name: 'AssertionError', message: /declaration hid executable producer/});
 for (const [original, replacement, seed, message] of [
  ['ts.isImportEqualsDeclaration(node) && !node.isTypeOnly', 'false', 1, /lost runtime module initialization/],
  ['node.expression.kind === ts.SyntaxKind.ImportKeyword', 'false', 2, /lost runtime module initialization/],
  ["node.expression.text === 'require'", 'false', 3, /lost runtime module initialization/],
  ['ts.isImportEqualsDeclaration(node) && !node.isTypeOnly', 'ts.isImportEqualsDeclaration(node)', 0, /type-only import executed/],
 ]) {
  const load = await mutant('./source-policy/project.mjs', original, replacement);
  assert.throws(() => targets.fuzzProject(Buffer.from([seed]), {project: load.projectPollingSites}),
   {name: 'AssertionError', message});
 }
 const policy = await mutant('./source-policy/index.mjs',
  "|| typeof item.producer !== 'string' || !item.producer.trim()", '|| false');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {validate: policy.validateDispositions}),
  {name: 'AssertionError', message: /Missing expected exception/});
 const fingerprint = await mutant('./source-policy/index.mjs',
  "typeof item.fingerprint !== 'string' || ", '');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {validate: fingerprint.validateDispositions}),
  {name: 'AssertionError', message: /invalid disposition field: fingerprint/});
 const manifest = await mutant('./source-policy/project.mjs',
  'throw new SourceSyntaxError(`${file}: ${error.message}`);', 'throw new Error(`${file}: ${error.message}`);');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: manifest.projectPollingSites}),
  {name: 'AssertionError', message: /SourceSyntaxError/});
 const layout = await mutant('./source-policy/project.mjs',
  'if (/(?:^|\\/)package\\.json$/.test(file)) {',
  'if (/^typescript\\/packages\\/[^/]+\\/package\\.json$/.test(file)) {');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: layout.projectPollingSites}),
  {name: 'AssertionError', message: /repository layout/});
 const current = await mutant('./source-policy/project.mjs',
  'if (historical && error instanceof SourceSyntaxError) continue;',
  'if (error instanceof SourceSyntaxError) continue;');
 assert.throws(() => targets.fuzzProject(Buffer.from('}'), {project: current.projectPollingSites}),
  {name: 'AssertionError', message: /Missing expected exception \(SourceSyntaxError\)/});
 const causes = await mutant('./source-policy/project.mjs',
  'new AggregateError(errors, errors.map', 'new AggregateError(errors.slice(1), errors.map');
 assert.throws(() => targets.fuzzProject(Buffer.from([0]), {project: causes.projectPollingSites}),
  {name: 'AssertionError', message: /malformed source causes/});
 const sqlite = await mutant('./source-policy/index.mjs', '(?:sqlite3_)?busy_(?:timeout|handler)|', '');
 assert.throws(() => targets.fuzz(Buffer.from('original'), sqlite.pollingSites),
  {name: 'AssertionError', message: /connection\.busy_timeout/});
});

test('raw source and project fuzz targets replay captured corpus and byte mutations', () => {
 for (const text of ["printf ''", '\\', '\\u{']) targets.fuzz(Buffer.from(text));
 for (let byte = 0; byte < 256; byte++) {
  targets.fuzz(Uint8Array.from({length: byte}, (_, index) => (byte + index) & 255));
 }
 for (const {file, source} of targets.sourceCorpus) targets.fuzzSource(file, Buffer.from(source));
 // Generated source mutations, not fabricated output from an external tool.
 for (const extension of ['js', 'mjs', 'cjs', 'ts', 'mts', 'cts', 'jsx', 'tsx', 'rs', 'c', 'h', 'cpp', 'cc', 'hh', 'hxx', 'hpp', 'cxx', 'cs', 'py', 'sh', 'bash', 'ps1', 'cmd', 'bat', 'proto', 'toml', 'json', 'yaml', 'yml']) {
  for (const seed of ['', '\\u{', '/*', '\r\n', String.fromCharCode(...Array.from({length: 256}, (_, byte) => byte))]) {
   targets.fuzzSource('control.' + extension, Buffer.from(seed));
  }
 }
 for (let byte = 0; byte < 8; byte++) targets.fuzzProject(Uint8Array.of(byte, 255 - byte));
 for (const json of ['null', '[]', '{}', '"ordinary"', '{"name":"@original/package"}']) targets.fuzzProject(Buffer.from(json));
 for (const source of [
  'export class Original { constructor() { this.ready = 1; } get value() { return Bun.sleep(1); } set value(next) { this.ready = next; } }',
  'export const Original = class Named { static value = Bun.sleep(1); };',
  'export function first() { return second(); } function second() { return first(); }',
  'export const {first: [value, ...rest]} = {first: [Bun.sleep(1)]}; export {value, rest};',
  '/** @typedef {{ready: number}} Original */ export const value = 1;',
  "export * from './helper.js'; export {default as original} from './helper.js';",
 ]) targets.fuzzProject(Buffer.from(source));
});
