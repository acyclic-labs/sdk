// Build the deployable harness Actor artifact from an owner configuration. The Bazel
// target //typescript/packages/harness-actor:bundle runs this after generating the
// protobuf projections and the wasm packages it consumes.
import { createHash } from "node:crypto";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { wasmAssetClosure, loadHostedArtifact } from "../scripts/artifact-assets.mjs";

const root = resolve(process.env.BUILD_WORKSPACE_DIRECTORY ?? resolve(dirname(fileURLToPath(import.meta.url)), ".."));
export async function bundleHarnessActor(configFile, outputDirectory) {
  const packages = join(root, "typescript/packages");
  const load = file => import(pathToFileURL(join(packages, "harness-actor/dist", file)).href);
  const { HARNESS_BINDINGS, HARNESS_MODULE } = await load("bindings.js");
  const { harnessManifest } = await load("manifest.js");
  const { deployment, ...config } = JSON.parse(readFileSync(resolve(configFile), "utf8"));
  const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");

  const closure = wasmAssetClosure(`bun scripts/build-harness-actor.mjs --config ${JSON.stringify(configFile)} --out ${JSON.stringify(outputDirectory)}`);
  const built = await Bun.build({ entrypoints: [join(packages, "harness-actor/dist/harness-actor.js")], target: "browser", format: "esm",
    plugins: [closure.plugin] });
  const output = built.success && built.outputs.find(artifact => artifact.kind === "entry-point");
  if (!output) throw new AggregateError(built.logs, "STEP bundle failed; recovery: bun scripts/build-harness-actor.mjs");
  const bundle = closure.rewrite(await output.text());
  const { files, assets } = closure;
  // The owner's configuration precedes the bundle, so the entry digest commits to it and every asset.
  const entry = Buffer.from(`const HARNESS_ACTOR = ${JSON.stringify({ config })};\n${bundle}`);
  const entrySha256 = sha256(entry);
  const manifest = `${JSON.stringify(harnessManifest(deployment, config.model.spec, config.options.filesystem_provider, entrySha256, [...assets.values()]), null, 2)}\n`;
  files.set(HARNESS_MODULE.file, entry).set("agent-deploy.json", manifest);
  const write = (directory, paths) => {
    mkdirSync(directory, { recursive: true });
    for (const path of paths) writeFileSync(join(directory, path), files.get(path));
  };

  const validator = join(packages, "workers/generated/wasm");
  const { default: initializeManifest, verifyArtifact } = await import(pathToFileURL(join(validator, "acyclic_manifest.js")).href);
  await initializeManifest({ module_or_path: readFileSync(join(validator, "acyclic_manifest_bg.wasm")) });
  // Conformance: admit and load the artifact using only what its manifest declares.
  const declared = JSON.parse(manifest).environments[0].entries[0].assets.map(({ path }) => path);
  const staged = mkdtempSync(join(tmpdir(), "harness-actor-"));
  try {
    write(staged, [HARNESS_MODULE.file, ...declared]);
    verifyArtifact(manifest, deployment.environment, deployment.entry, entry, declared,
      declared.map(path => readFileSync(join(staged, path))));
    const guest = await loadHostedArtifact(join(staged, HARNESS_MODULE.file), [...assets.values()], files);
    if (typeof guest.manifest !== "function" || typeof guest.onRecord !== "function"
      || typeof guest.acceptsInterrupt !== "function" || "actor" in guest) {
      throw new Error("STEP conformance failed: strict-wire guest exports required; recovery: bun scripts/build-harness-actor.mjs");
    }
    // The loader installs admitted bindings once at readiness, never an actor identity.
    const env = Object.fromEntries(Object.values(HARNESS_BINDINGS).filter(name => name !== HARNESS_BINDINGS.authority)
      .map(name => [name, {}]));
    const refused = await guest.manifest({ env }).then(() => null, error => error);
    // An absent binding is the typed guest failure the host commits as
    // DependencyUnavailable, naming the binding (#4096).
    if (refused?.type !== "dependencyUnavailable" || refused.detail?.dependency !== HARNESS_BINDINGS.authority) {
      throw new Error(`STEP conformance failed: ${refused?.stack ?? "manifest admitted an unbound environment"}; recovery: bun scripts/build-harness-actor.mjs`);
    }
  } finally {
    rmSync(staged, { recursive: true, force: true });
  }
  write(resolve(outputDirectory), files.keys());
  console.log(`harness Actor artifact: ${resolve(outputDirectory)} (${HARNESS_MODULE.file} sha256 ${entrySha256}, ${assets.size} module assets)`);
}

if (import.meta.main) {
  const { values } = parseArgs({ options: {
    config: { type: "string" }, out: { type: "string" }, proto: { type: "string" },
    wasm: { type: "string", multiple: true },
  } });
  if (!values.config || !values.out) throw new Error("usage: bun tools/harness-actor-bundle.mjs --config OWNER.json --out DIRECTORY");
  const step = (name, command, args) => {
    const result = spawnSync(command, args, { cwd: root, stdio: "inherit", windowsHide: true });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`STEP ${name} failed (exit ${result.status}); recovery: bun tools/harness-actor-bundle.mjs`);
  };
  if (values.proto) step("proto", resolve(values.proto), []);
  const place = (source, destination) => {
    if (!source) return;
    const target = join(root, destination);
    spawnSync("chmod", ["-R", "u+w", target], { stdio: "ignore" });
    rmSync(target, { recursive: true, force: true });
    mkdirSync(target, { recursive: true });
    cpSync(resolve(source), target, { recursive: true });
  };
  for (const spec of values.wasm ?? []) {
    const [source, destination] = spec.split("=");
    place(source, destination);
  }
  // The workspace package that re-exports the stream bindings resolves by name, as it does in an
  // installed workspace; the checkout has no node_modules tree of its own.
  const streamWasm = join(root, "node_modules/@acyclic/stream-wasm");
  mkdirSync(dirname(streamWasm), { recursive: true });
  rmSync(streamWasm, { recursive: true, force: true });
  cpSync(join(root, "typescript/packages/@acyclic/stream-wasm"), streamWasm, { recursive: true });
  step("typescript", "bun", ["x", "tsc", "-b", "typescript/packages/harness-actor"]);
  await bundleHarnessActor(values.config, values.out);
}
