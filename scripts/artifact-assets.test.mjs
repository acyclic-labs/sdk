import {test} from 'bun:test';
import assert from 'node:assert/strict';
import {mkdtempSync, mkdirSync, writeFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
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
    assert.equal((await loadHostedArtifact(artifact, assets)).ready(), true);
    const corrupt = join(root, 'corrupt');
    mkdirSync(corrupt);
    writeFileSync(join(corrupt, 'actor.mjs'), closure.rewrite(await built.outputs[0].text()));
    writeFileSync(join(corrupt, assets[0].path), Buffer.from([0]));
    await assert.rejects(loadHostedArtifact(join(corrupt, 'actor.mjs'), assets), /HostedWasmAssetChanged/);
    const undeclared = join(root, 'undeclared');
    mkdirSync(undeclared);
    writeFileSync(join(undeclared, 'other.wasm'), wasm);
    writeFileSync(join(undeclared, 'actor.mjs'), "import module from './other.wasm'; export default module;");
    await assert.rejects(loadHostedArtifact(join(undeclared, 'actor.mjs'), []), /HostedWasmAssetUndeclared/);
  } finally {rmSync(root, {recursive:true, force:true});}
});
