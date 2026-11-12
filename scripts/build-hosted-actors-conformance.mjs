// Produce the shared public Actor fixture from an SDK-admitted manifest.
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {accessSync, mkdirSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {wasmAssetClosure, loadHostedArtifact} from './artifact-assets.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const fixture = join(root, 'rust/crates/conformance/hosted-actors');
const validator = join(root, 'typescript/packages/workers/generated/wasm');
const recovery = 'bun scripts/build-hosted-actors-conformance.mjs <manifest> <environment> <entry> <output>';

/** Admit all fixture inputs before a compiler runs. The existing harness
 * producer supplies these same verified facades for both Actor artifacts. */
export async function admitHostedActorsManifest(manifestFile, environmentName, entryName) {
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
  accessSync(join(root, 'typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js'));
  return {manifest, entry, bindings, environmentName, entryName, verifyArtifact};
}

/** Seal and load the guest using only the original prepared wasm facades.
 * This step performs no compilation, so artifact qualification can reuse the
 * first producer's immutable outputs. */
export async function bundleHostedActors(input, output) {
  const {manifest, entry, bindings, environmentName, entryName, verifyArtifact} = input;
  const closure = wasmAssetClosure(recovery);
  const built = await Bun.build({entrypoints:[join(fixture, 'guest.mjs')], target:'browser', format:'esm', plugins:[closure.plugin]});
  const artifact = built.success && built.outputs.find(value => value.kind === 'entry-point');
  if (!artifact) throw new AggregateError(built.logs, 'HostedActorsBundleFailed');
  const bytes = Buffer.from(`const HOSTED_ACTOR_BINDINGS=${JSON.stringify(bindings)};\n` + closure.rewrite(await artifact.text()));
  entry.artifactSha256 = createHash('sha256').update(bytes).digest('hex');
  entry.assets = [...closure.assets.values()];
  const text = JSON.stringify(manifest, null, 2) + '\n';
  verifyArtifact(text, environmentName, entryName, bytes, entry.assets.map(asset => asset.path),
    entry.assets.map(asset => closure.files.get(asset.path)));
  const out = resolve(output);
  mkdirSync(out, {recursive:true});
  for (const [name, bytes] of closure.files) writeFileSync(join(out, name), bytes, {flag:'wx'});
  writeFileSync(join(out, 'harness-actor.mjs'), bytes, {flag:'wx'});
  writeFileSync(join(out, 'agent-deploy.json'), text, {flag:'wx'});
  // The deployed loader gets exactly these sealed modules. All must initialize
  // before an empty environment refuses the first selected binding.
  const {actor} = await loadHostedArtifact(join(out, 'harness-actor.mjs'), entry.assets);
  const refused = await actor.restore(null, {env:{}, actor:{id:'conformance-controller'}})
    .then(() => null, error => error);
  if (refused?.message !== `ActorBindingUnserved: ${bindings.names[0]}`)
    throw new Error('HostedActorsArtifactLoadOrBindingRefusalFailed', {cause:refused});
  console.log(`HOSTED_ACTORS_ARTIFACT ${out} ${entry.artifactSha256}`);
}

if (import.meta.main) {
  const [manifestFile, environmentName, entryName, output, ...extra] = process.argv.slice(2);
  if (!output || extra.length) throw Error(recovery);
  const input = await admitHostedActorsManifest(manifestFile, environmentName, entryName);
  function run(program, args, capture = false) {
    const result = spawnSync(program, args, {cwd:root, encoding:'utf8', windowsHide:true,
      stdio:capture ? ['ignore', 'pipe', 'inherit'] : 'inherit'});
    if (result.error) throw result.error;
    if (result.status !== 0) throw Error(`HostedActorsBuildFailed: ${program} exited ${result.status}; ${recovery}`);
    return result.stdout?.trim();
  }
  run('cargo', ['build', '--locked', '-p', 'acyclic-hosted-actors-conformance',
    '--target', 'wasm32-unknown-unknown', '--profile', 'wasm-release']);
  const metadata = JSON.parse(run('cargo', ['metadata', '--locked', '--no-deps', '--format-version', '1'], true));
  const bindgen = run('bash', ['scripts/ensure-wasm-bindgen.sh'], true);
  run(bindgen, ['--target', 'web', '--out-dir', join(fixture, 'generated'),
    join(metadata.target_directory, 'wasm32-unknown-unknown/wasm-release/acyclic_hosted_actors_conformance.wasm')]);
  await bundleHostedActors(input, output);
}
