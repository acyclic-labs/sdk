// Produce the shared public Actor fixture from an SDK-admitted manifest.
import {createHash} from 'node:crypto';
import {cpSync, mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {wasmAssetClosure, loadHostedArtifact} from './artifact-assets.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const fixture = join(root, 'rust/crates/conformance/hosted-actors');
const recovery = 'bun scripts/build-hosted-actors-conformance.mjs <prepared> <manifest> <environment> <entry> <output>';

/** The fixture uses the same declared browser-module producer as public packages. */
export const hostedActorsWasm = {
  name:'hosted-actors', selection:['-p', 'acyclic-hosted-actors-conformance'],
  artifact:'acyclic_hosted_actors_conformance', outName:'acyclic_hosted_actors_conformance',
  directory:'rust/crates/conformance/hosted-actors/generated',
};

/** Compile the guest once, inside the action that supplies its immutable facades. */
export async function prepareHostedActors(output) {
  const closure = wasmAssetClosure(recovery);
  const built = await Bun.build({entrypoints:[join(fixture, 'guest.mjs')], target:'browser', format:'esm', plugins:[closure.plugin]});
  const artifact = built.success && built.outputs.find(value => value.kind === 'entry-point');
  if (!artifact) throw new AggregateError(built.logs, 'HostedActorsBundleFailed');
  const guest = closure.rewrite(await artifact.text());
  mkdirSync(output, {recursive:true});
  writeFileSync(join(output, 'guest.mjs'), guest, {flag:'wx'});
  for (const [name, bytes] of closure.files) writeFileSync(join(output, name), bytes, {flag:'wx'});
  writeFileSync(join(output, 'assets.json'), JSON.stringify([...closure.assets.values()]) + '\n', {flag:'wx'});
  cpSync(join(root, 'typescript/packages/workers/generated/wasm'), join(output, 'validator'), {recursive:true});
  const producer = await Bun.build({entrypoints:[fileURLToPath(import.meta.url)],
    target:'bun', format:'esm', outdir:output, naming:'prepare.mjs'});
  if (!producer.success) throw new AggregateError(producer.logs, 'HostedActorsProducerBundleFailed');
}

/** Admit all fixture inputs before sealing. The existing harness
 * producer supplies these same verified facades for both Actor artifacts. */
export async function admitHostedActorsManifest(prepared, manifestFile, environmentName, entryName) {
  const validator = join(resolve(prepared), 'validator');
  const text = readFileSync(resolve(manifestFile), 'utf8');
  const {default:initialize, validate, verifyArtifact} = await import(pathToFileURL(join(validator, 'acyclic_manifest.js')).href);
  await initialize({module_or_path:readFileSync(join(validator, 'acyclic_manifest_bg.wasm'))});
  validate(text);
  const manifest = JSON.parse(text);
  const environment = manifest.environments.find(value => value.name === environmentName);
  const entry = environment?.entries.find(value => value.name === entryName);
  if (!entry || entry.runtime.kind !== 'actorNamespace') throw Error('HostedActorsNamespaceEntryRequired');
  const selected = environment.bindings.filter(binding => entry.bindings.includes(binding.name));
  function original(predicate, kind) {
    const binding = selected.find(predicate);
    if (!binding) throw Error(`HostedActorsBindingRequired: ${kind}`);
    return binding.name;
  }
  const bindings = {
    names:[...entry.bindings, ...entry.secrets, ...(entry.fetch ? [entry.fetch.name] : [])],
    state:original(binding => binding.stateTransaction, 'stateTransaction'),
    streams:original(binding => binding.resource.startsWith('streams:/'), 'streams'),
    namespace:original(binding => binding.actorNamespace !== undefined, 'namespace'),
  };
  return {prepared, manifest, entry, bindings, environmentName, entryName, verifyArtifact};
}

/** Seal and load the guest using only the original prepared wasm facades.
 * This step performs no compilation, so artifact qualification can reuse the
 * first producer's immutable outputs. */
export async function bundleHostedActors(input, output) {
  const {prepared, manifest, entry, bindings, environmentName, entryName, verifyArtifact} = input;
  const assets = JSON.parse(readFileSync(join(prepared, 'assets.json'), 'utf8'));
  const files = new Map(assets.map(asset => [asset.path, readFileSync(join(prepared, asset.path))]));
  const bytes = Buffer.from(`const HOSTED_ACTOR_BINDINGS=${JSON.stringify(bindings)};\n` + readFileSync(join(prepared, 'guest.mjs'), 'utf8'));
  entry.artifactSha256 = createHash('sha256').update(bytes).digest('hex');
  entry.assets = assets;
  const text = JSON.stringify(manifest, null, 2) + '\n';
  verifyArtifact(text, environmentName, entryName, bytes, entry.assets.map(asset => asset.path),
    entry.assets.map(asset => files.get(asset.path)));
  const out = resolve(output);
  mkdirSync(out, {recursive:true});
  for (const [name, bytes] of files) writeFileSync(join(out, name), bytes, {flag:'wx'});
  writeFileSync(join(out, 'harness-actor.mjs'), bytes, {flag:'wx'});
  writeFileSync(join(out, 'agent-deploy.json'), text, {flag:'wx'});
  // The deployed loader gets exactly these sealed modules. All must initialize
  // before an empty environment refuses the first selected binding.
  const {actor} = await loadHostedArtifact(join(out, 'harness-actor.mjs'), entry.assets, files);
  const refused = await actor.restore(null, {env:{}, actor:{id:'conformance-controller'}})
    .then(() => null, error => error);
  if (refused?.message !== `ActorBindingUnserved: ${bindings.names[0]}`)
    throw new Error('HostedActorsArtifactLoadOrBindingRefusalFailed', {cause:refused});
  console.log(`HOSTED_ACTORS_ARTIFACT ${out} ${entry.artifactSha256}`);
}

if (import.meta.main) {
  const [prepared, manifestFile, environmentName, entryName, output, ...extra] = process.argv.slice(2);
  if (!output || extra.length) throw Error(recovery);
  const input = await admitHostedActorsManifest(prepared, manifestFile, environmentName, entryName);
  await bundleHostedActors(input, output);
}
