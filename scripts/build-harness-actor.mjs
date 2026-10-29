// Build the deployable harness Actor artifact from an owner configuration:
//   bun scripts/build-harness-actor.mjs --config OWNER.json --out DIRECTORY
// DIRECTORY receives harness-actor.mjs, every module it loads as a declared
// asset, and agent-deploy.json. The artifact is admitted, then loaded from only
// the files that manifest declares, before the build succeeds.
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const { values } = parseArgs({ options: { config: { type: "string" }, out: { type: "string" } } });
if (!values.config || !values.out) throw new Error("usage: bun scripts/build-harness-actor.mjs --config OWNER.json --out DIRECTORY");
const step = (name, args) => {
  const result = spawnSync(process.execPath, args, { cwd: root, stdio: "inherit" });
  if (result.status !== 0) throw new Error(`STEP ${name} failed (exit ${result.status}); recovery: bun scripts/build-harness-actor.mjs`);
};
step("wasm", ["scripts/build-wasm.mjs", "harness-actor"]);
step("typescript", ["x", "tsc", "-b", "typescript/packages/harness-actor"]);

const packages = join(root, "typescript/packages");
const load = file => import(pathToFileURL(join(packages, "harness-actor/dist", file)).href);
const { HARNESS_BINDINGS, HARNESS_MODULE } = await load("bindings.js");
const { harnessManifest } = await load("manifest.js");
const { deployment, ...config } = JSON.parse(readFileSync(resolve(values.config), "utf8"));
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");

const built = await Bun.build({ entrypoints: [join(packages, "harness-actor/dist/harness-actor.js")], target: "browser", format: "esm" });
const output = built.success && built.outputs.find(artifact => artifact.kind === "entry-point");
if (!output) throw new AggregateError(built.logs, "STEP bundle failed; recovery: bun scripts/build-harness-actor.mjs");
// Every module the bundle loads becomes a content-addressed sibling asset of the entry.
const modules = new Map(readdirSync(packages).flatMap(name => {
  const directory = join(packages, name, "generated/wasm");
  return existsSync(directory)
    ? readdirSync(directory).filter(file => file.endsWith(".wasm")).map(file => [file, join(directory, file)]) : [];
}));
const files = new Map();
const assets = new Map();
const reference = /new URL\((["'])(?:[^"']*\/)?([^/"']+)\.wasm\1,\s*import\.meta\.url\)/g;
const bundle = (await output.text()).replace(reference, (_, _quote, name) => {
  const source = modules.get(`${name}.wasm`);
  if (!source) throw new Error(`STEP assets failed: ${name}.wasm has no generated module; recovery: bun scripts/build-wasm.mjs`);
  const bytes = readFileSync(source);
  const asset = { path: `${name}.${sha256(bytes)}.wasm`, sha256: sha256(bytes), size: bytes.length,
    mediaType: "application/wasm", role: "module" };
  files.set(asset.path, bytes);
  assets.set(asset.path, asset);
  return `new URL("./${asset.path}", import.meta.url)`;
});
// The owner's configuration precedes the bundle, so the entry digest commits to it and every asset.
const entry = Buffer.from(`const HARNESS_ACTOR = ${JSON.stringify({ config })};\n${bundle}`);
const manifest = `${JSON.stringify(harnessManifest(deployment, config.model.spec, sha256(entry), [...assets.values()]), null, 2)}\n`;
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
  const { actor } = await import(pathToFileURL(join(staged, HARNESS_MODULE.file)).href);
  // The modules instantiate from declared assets; the unbound authority is then refused typed.
  const env = Object.fromEntries(Object.values(HARNESS_BINDINGS).filter(name => name !== HARNESS_BINDINGS.authority)
    .map(name => [name, {}]));
  const refused = await actor.restore(null, { env, actor: { id: "root" } }).then(() => null, error => error);
  if (refused?.code !== "failed_precondition" || refused.subject !== HARNESS_BINDINGS.authority) {
    throw new Error(`STEP conformance failed: ${refused?.stack ?? "restore admitted an unbound environment"}; recovery: bun scripts/build-harness-actor.mjs`);
  }
} finally {
  rmSync(staged, { recursive: true, force: true });
}
write(resolve(values.out), files.keys());
console.log(`harness Actor artifact: ${resolve(values.out)} (${HARNESS_MODULE.file} sha256 ${sha256(entry)}, ${assets.size} module assets)`);
