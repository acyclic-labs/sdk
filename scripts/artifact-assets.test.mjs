import {test} from 'bun:test';
import assert from 'node:assert/strict';
import {mkdtempSync, mkdirSync, writeFileSync, rmSync, readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {wasmAssetClosure, loadHostedArtifact} from './artifact-assets.mjs';

test('hosted artifact initializes from compiled declared WASM without guest code generation', async () => {
  const root = mkdtempSync(join(tmpdir(), 'hosted-wasm-'));
  try {
    const auth = join(root, 'auth');
    mkdirSync(join(auth, 'dist'), {recursive:true});
    mkdirSync(join(auth, 'generated/wasm'), {recursive:true});
    const wasm = Buffer.from([0,97,115,109,1,0,0,0]);
    writeFileSync(join(auth, 'generated/wasm/acyclic_auth_wasm_bg.wasm'), wasm);
    writeFileSync(join(auth, 'generated/wasm/acyclic_auth_wasm.js'), `
let instance;
export function initSync({module}) { instance = new WebAssembly.Instance(module); }
export function ready() { return instance instanceof WebAssembly.Instance; }
export default async function initialize() {
  const url = new URL('./acyclic_auth_wasm_bg.wasm', import.meta.url);
  throw new Error('raw byte initialization must never run: '+url);
}`);
    writeFileSync(join(auth, 'dist/wasm.js'), `
import initialize from '../generated/wasm/acyclic_auth_wasm.js';
await initialize();
export * from '../generated/wasm/acyclic_auth_wasm.js';`);
    const entry = join(root, 'entry.mjs');
    writeFileSync(entry, "export {ready} from './auth/dist/wasm.js';");
    const closure = wasmAssetClosure('retain original artifact');
    const built = await Bun.build({entrypoints:[entry], target:'browser', format:'esm', plugins:[closure.plugin]});
    assert.equal(built.success, true, built.logs.join('\n'));
    const output = join(root, 'output');
    mkdirSync(output);
    const artifact = join(output, 'actor.mjs');
    writeFileSync(artifact, closure.rewrite(await built.outputs[0].text()));
    for (const [path, bytes] of closure.files) writeFileSync(join(output, path), bytes);
    const assets = [...closure.assets.values()];
    assert.equal(assets.length, 1);
    assert.equal((await loadHostedArtifact(artifact, assets, closure.files)).ready(), true);
    const corrupt = join(root, 'corrupt');
    mkdirSync(corrupt);
    writeFileSync(join(corrupt, 'actor.mjs'), closure.rewrite(await built.outputs[0].text()));
    writeFileSync(join(corrupt, assets[0].path), Buffer.from([0]));
    // The verified original bytes survive a later change to the staged file.
    assert.equal((await loadHostedArtifact(join(corrupt, 'actor.mjs'), assets, closure.files)).ready(), true);
    const undeclared = join(root, 'undeclared');
    mkdirSync(undeclared);
    writeFileSync(join(undeclared, 'other.wasm'), wasm);
    writeFileSync(join(undeclared, 'actor.mjs'), "import module from './other.wasm'; export default module;");
    await assert.rejects(loadHostedArtifact(join(undeclared, 'actor.mjs'), [], new Map()), /HostedWasmAssetUndeclared/);
  } finally {rmSync(root, {recursive:true, force:true});}
});

test('a hosted artifact replayed by the transpiler cache from another directory loads its declared WASM', async () => {
  const root = mkdtempSync(join(tmpdir(), 'hosted-replay-'));
  try {
    const generated = join(root, 'auth/generated/wasm');
    mkdirSync(generated, {recursive:true});
    writeFileSync(join(generated, 'acyclic_auth_wasm_bg.wasm'), Buffer.from([0,97,115,109,1,0,0,0]));
    writeFileSync(join(generated, 'acyclic_auth_wasm.js'), `
let instance;
export function initSync({module}) { instance = new WebAssembly.Instance(module); }
export function ready() { return instance instanceof WebAssembly.Instance; }
export default async function initialize() { throw new Error(String(new URL('./acyclic_auth_wasm_bg.wasm', import.meta.url))); }`);
    const entry = join(root, 'entry.mjs');
    writeFileSync(entry, "export {ready} from './auth/generated/wasm/acyclic_auth_wasm.js';");
    const closure = wasmAssetClosure('retain original artifact');
    const built = await Bun.build({entrypoints:[entry], target:'browser', format:'esm', plugins:[closure.plugin]});
    assert.equal(built.success, true, built.logs.join('
'));
    // Bun caches transpiled sources above 50 KB by content, including resolved imports.
    const artifact = closure.rewrite(await built.outputs[0].text()) + `
// ${'x'.repeat(64 * 1024)}
`;
    const assets = [...closure.assets.values()];
    const evidence = join(root, 'assets.json');
    writeFileSync(evidence, JSON.stringify({assets, files:[...closure.files].map(([path, bytes]) => [path, Buffer.from(bytes).toString('base64')])}));
    const cache = join(root, 'transpiler-cache');
    mkdirSync(cache);
    const loader = fileURLToPath(new URL('./artifact-assets.mjs', import.meta.url));
    const load = directory => {
      mkdirSync(directory);
      writeFileSync(join(directory, 'actor.mjs'), artifact);
      for (const [path, bytes] of closure.files) writeFileSync(join(directory, path), bytes);
      const run = Bun.spawnSync([process.execPath, '-e', `
const {loadHostedArtifact} = await import(${JSON.stringify(loader)});
const {assets, files} = JSON.parse(require('node:fs').readFileSync(${JSON.stringify(evidence)}, 'utf8'));
const module = await loadHostedArtifact(${JSON.stringify(join(directory, 'actor.mjs'))}, assets,
  new Map(files.map(([path, bytes]) => [path, Buffer.from(bytes, 'base64')])));
if (!module.ready()) throw new Error('declared WASM did not initialize');`],
        {env:{...process.env, BUN_RUNTIME_TRANSPILER_CACHE_PATH:cache}, stderr:'pipe', stdout:'pipe'});
      rmSync(directory, {recursive:true, force:true});
      return run;
    };
    const first = load(join(root, 'first'));
    assert.equal(first.exitCode, 0, first.stderr.toString());
    // The second process must actually replay the first one's cached transpile.
    assert.notEqual(readdirSync(cache).length, 0, 'transpiler cache was not exercised');
    const replayed = load(join(root, 'second'));
    assert.equal(replayed.exitCode, 0, replayed.stderr.toString());
  } finally {rmSync(root, {recursive:true, force:true});}
});
