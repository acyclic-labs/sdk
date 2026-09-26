import Ajv2020 from "ajv/dist/2020.js";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = new URL("..", import.meta.url);
const rootPath = resolve(fileURLToPath(root));
const load = async path => JSON.parse(await readFile(new URL(path, root), "utf8"));
const retiredStem = "p" + "y";
const retiredSuffixes = ["", "i", "c", "o"].map(suffix => `.${retiredStem}${suffix}`);
const retiredTerms = ["p" + "ython", "py" + "test", "bo" + "to3", "boto" + "core"];
const retiredPattern = `${retiredTerms.join("|")}|\\.${retiredStem}(?:i|c|o)?\\b`;
const hasRetiredSuffix = path => retiredSuffixes.some(suffix => path.toLowerCase().endsWith(suffix));
if (!retiredSuffixes.every(suffix => hasRetiredSuffix(`fixture${suffix}`)) || hasRetiredSuffix("fixture.mjs")) {
  throw new Error("retired-runtime suffix gate failed its fixtures");
}
const fixturePattern = new RegExp(retiredPattern, "i");
if (!fixturePattern.test(`${"p" + "ython"} fixture`) || fixturePattern.test("node fixture.mjs")) {
  throw new Error("retired-runtime content gate failed its fixtures");
}
const git = (...args) => spawnSync("git", args, { cwd: rootPath, encoding: "utf8" });
const tracked = git("ls-files", "-z", "--cached", "--others", "--exclude-standard");
if (tracked.error || tracked.status !== 0) throw tracked.error ?? new Error(tracked.stderr.trim());
const exemptPrefixes = ["arena/"];
const isExempt = path => exemptPrefixes.some(prefix => path.startsWith(prefix));
const presentFiles = tracked.stdout
  .split("\0")
  .filter(Boolean)
  .filter(path => !isExempt(path))
  .map(path => ({ path, fullPath: resolve(rootPath, path) }))
  .filter(({ fullPath }) => {
    if (fullPath !== rootPath && !fullPath.startsWith(`${rootPath}${sep}`)) {
      throw new Error(`repository path escapes its root: ${fullPath}`);
    }
    return existsSync(fullPath);
  });
const retiredFile = presentFiles.find(({ path }) => hasRetiredSuffix(path))?.path;
if (retiredFile) throw new Error(`retired-runtime source exists: ${retiredFile}`);
const retiredContent = presentFiles.find(({ fullPath }) => {
  const content = readFileSync(fullPath);
  return !content.includes(0) && fixturePattern.test(content.toString("utf8"));
})?.path;
if (retiredContent) throw new Error(`retired-runtime reference exists: ${retiredContent}`);
const ajv = new Ajv2020({ allErrors: true });
ajv.compile(await load("rust/crates/conformance/schemas/runner-report.schema.json"));
new Ajv2020({ allErrors: true }).compile(
  await load("rust/crates/conformance/schemas/qualification-receipt.schema.json"),
);
const documents = [
  ["provenance/manifest.json", "compatibility/schemas/provenance.schema.json"],
  ["languages/package-names.json", "compatibility/schemas/package-names.schema.json"],
  ["compatibility/manifest.json", "compatibility/schemas/compatibility.schema.json"],
];
for (const [documentPath, schemaPath] of documents) {
  const validate = ajv.compile(await load(schemaPath));
  if (!validate(await load(documentPath))) throw new Error(`${documentPath}: ${ajv.errorsText(validate.errors)}`);
}
const provenance = await load("provenance/manifest.json");
for (const item of provenance.imports) if (item.auditResult !== "approved") throw new Error(`unapproved import: ${item.destinationPath}`);

const digest = async path => `sha256:${createHash("sha256").update(await readFile(new URL(path, root))).digest("hex")}`;
const compatibility = await load("compatibility/manifest.json");
const harnessVersion = compatibility.families.harness.version;
const machinesVersion = compatibility.families.machines.version;
const streamVersion = compatibility.families.stream.version;
const streamCrateVersion = compatibility.families.stream.crateVersion ?? streamVersion;
const harnessPackage = await load("typescript/packages/harness/package.json");
const sdkPackage = await load("typescript/packages/sdk/package.json");
for (const item of await load("release/npm-packages.json")) {
  const directory = item.source === "plugin"
    ? "plugin"
    : `typescript/packages/${item.directory}`;
  const manifest = await load(`${directory}/package.json`);
  if (manifest.name !== item.name || manifest.version !== sdkPackage.version || manifest.private !== false) {
    throw new Error(`public npm package identity mismatch: ${item.name}`);
  }
  const readme = await readFile(new URL(`${directory}/README.md`, root), "utf8");
  const changelog = await readFile(new URL(`${directory}/CHANGELOG.md`, root), "utf8");
  if (!readme.startsWith("# ") || !changelog.includes(`## ${manifest.version}`)) {
    throw new Error(`public npm package documentation mismatch: ${item.name}`);
  }
}
const lock = Bun.JSONC.parse(await readFile(new URL("bun.lock", root), "utf8"));
for (const [path, locked] of Object.entries(lock.workspaces)) {
  const manifest = await load(path ? `${path}/package.json` : "package.json");
  if (locked.name !== manifest.name || (path && locked.version !== manifest.version)) {
    throw new Error(`Bun workspace identity mismatch: ${path || "root"}`);
  }
  for (const field of ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"]) {
    const entries = value => JSON.stringify(Object.entries(value ?? {}).sort(([a], [b]) => a.localeCompare(b)));
    if (entries(locked[field]) !== entries(manifest[field])) {
      throw new Error(`Bun workspace ${field} mismatch: ${path || "root"}`);
    }
  }
}
if (sdkPackage.version !== compatibility.umbrellaVersion) {
  throw new Error("TypeScript SDK version must match umbrella compatibility metadata");
}
if (harnessPackage.version !== harnessVersion || sdkPackage.dependencies["@acyclic-labs/harness"] !== harnessVersion) {
  throw new Error("Harness npm and umbrella dependency versions must match compatibility metadata");
}
const workspaceManifest = await readFile(new URL("Cargo.toml", root), "utf8");
const workspaceVersion = workspaceManifest.match(/\[workspace\.package\][\s\S]*?\nversion = "([^"]+)"/)?.[1];
const harnessManifest = await readFile(new URL("rust/crates/harness/Cargo.toml", root), "utf8");
const streamManifest = await readFile(new URL("rust/crates/stream/Cargo.toml", root), "utf8");
const rustStreamVersion = streamManifest.match(/\[package\][\s\S]*?\nversion = "([^"]+)"/)?.[1];
const typescriptStreamPackage = await load("typescript/packages/stream/package.json");
if (
  workspaceVersion !== harnessVersion || !harnessManifest.includes("version.workspace = true") ||
  rustStreamVersion !== streamCrateVersion ||
  typescriptStreamPackage.version !== streamVersion ||
  sdkPackage.dependencies["@acyclic-labs/stream"] !== streamVersion
) {
  throw new Error("Harness or Stream package versions do not match compatibility metadata");
}
for (const path of [
  "rust/crates/conformance/Cargo.toml",
  "rust/crates/filesystem/Cargo.toml",
  "rust/crates/filesystem-wasm/Cargo.toml",
  "rust/crates/harness/Cargo.toml",
]) {
  const manifest = await readFile(new URL(path, root), "utf8");
  const requirement = manifest.match(/acyclic-stream = \{ version = "([^"]+)"/)?.[1];
  if (requirement !== `=${streamCrateVersion}`) {
    throw new Error(`Stream dependency version mismatch: ${path}`);
  }
}
const machinesManifest = await readFile(new URL("rust/crates/machines/Cargo.toml", root), "utf8");
const rustMachinesVersion = machinesManifest.match(/\[package\][\s\S]*?\nversion = "([^"]+)"/)?.[1];
const typescriptMachinesPackage = await load("typescript/packages/machines/package.json");
if (
  rustMachinesVersion !== machinesVersion ||
  typescriptMachinesPackage.version !== machinesVersion ||
  sdkPackage.dependencies["@acyclic-labs/machines"] !== machinesVersion
) {
  throw new Error("Machines package versions do not match compatibility metadata");
}
for (const path of [
  "rust/crates/conformance/Cargo.toml",
  "rust/crates/harness/Cargo.toml",
]) {
  const manifest = await readFile(new URL(path, root), "utf8");
  const requirement = manifest.match(/acyclic-machines = \{ version = "([^"]+)"/)?.[1];
  if (requirement !== `=${machinesVersion}`) {
    throw new Error(`Machines dependency version mismatch: ${path}`);
  }
}
const inferenceVersion = compatibility.families.inference.version;
if ((await load("typescript/packages/inference/package.json")).version !== inferenceVersion) {
  throw new Error("TypeScript inference package version mismatch");
}
const rustInferenceManifest = await readFile(new URL("rust/crates/inference/Cargo.toml", root), "utf8");
if (rustInferenceManifest.match(/\[package\][\s\S]*?\nversion = "([^"]+)"/)?.[1] !== inferenceVersion) {
  throw new Error("Rust inference package version mismatch");
}
if ((await load("typescript/packages/sdk/package.json")).dependencies["@acyclic-labs/inference"] !== inferenceVersion) {
  throw new Error("TypeScript SDK inference dependency version mismatch");
}
const objectsVersion = compatibility.families.objects.version;
const objectsCrateVersion = compatibility.families.objects.crateVersion ?? objectsVersion;
const objectsManifest = await readFile(new URL("rust/crates/objects/Cargo.toml", root), "utf8");
if (
  (await load("typescript/packages/objects/package.json")).version !== objectsVersion ||
  sdkPackage.dependencies["@acyclic-labs/objects"] !== objectsVersion
) {
  throw new Error("Objects npm and umbrella dependency versions must match compatibility metadata");
}
if (objectsManifest.match(/\[package\][\s\S]*?\nversion = "([^"]+)"/)?.[1] !== objectsCrateVersion) {
  throw new Error("Objects crate version mismatch");
}
for (const path of [
  "rust/crates/conformance/Cargo.toml",
  "rust/crates/filesystem/Cargo.toml",
  "rust/crates/filesystem-wasm/Cargo.toml",
]) {
  const manifest = await readFile(new URL(path, root), "utf8");
  const requirement = manifest.match(/acyclic-objects = \{ version = "([^"]+)"/)?.[1];
  if (requirement !== `=${objectsCrateVersion}`) {
    throw new Error(`Objects dependency version mismatch: ${path}`);
  }
}
const filesystemVersion = compatibility.families.filesystem.version;
const filesystemCrateVersion = compatibility.families.filesystem.crateVersion ?? filesystemVersion;
for (const path of [
  "typescript/packages/filesystem/package.json",
  "typescript/packages/filesystem/generated/wasm/package.json",
]) {
  if ((await load(path)).version !== filesystemVersion) {
    throw new Error(`filesystem package version mismatch: ${path}`);
  }
}
for (const path of [
  "rust/crates/filesystem-napi/Cargo.toml",
  "rust/crates/filesystem-wasm/Cargo.toml",
]) {
  const manifest = await readFile(new URL(path, root), "utf8");
  if (!manifest.includes(`\nversion = "${filesystemVersion}"\n`)) {
    throw new Error(`filesystem package version mismatch: ${path}`);
  }
}
const filesystemManifest = await readFile(new URL("rust/crates/filesystem/Cargo.toml", root), "utf8");
if (!filesystemManifest.includes(`\nversion = "${filesystemCrateVersion}"\n`)) {
  throw new Error("filesystem crate version mismatch");
}
const nativeSource = await readFile(new URL("typescript/packages/filesystem/src/native.ts", root), "utf8");
const nativePackageVersion = nativeSource.match(/const PACKAGE_VERSION = "([^"]+)";/)?.[1];
if (nativePackageVersion !== filesystemVersion) {
  throw new Error("filesystem native companion version does not match package metadata");
}
const familyArtifacts = {
  harness: {
    schemaDigest: "proto/harness/v1/harness.proto",
    conformanceDigest: "conformance/vectors/core.json",
  },
  filesystem: {
    schemaDigest: "proto/filesystem/v2/filesystem.proto",
    descriptorDigest: "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
    conformanceDigest: "conformance/vectors/filesystem/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v2/stream.proto",
    conformanceDigest: "conformance/vectors/stream.json",
  },
  objects: {
    schemaDigest: "proto/objects/v1/objects.proto",
    descriptorDigest: "rust/crates/objects/src/generated/acyclic-objects-v1.bin",
    conformanceDigest: "conformance/vectors/objects.json",
  },
  machines: {
    schemaDigest: "proto/machines/v1/machines.proto",
    descriptorDigest: "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
    conformanceDigest: "conformance/vectors/machines.json",
  },
  inference: {
    schemaDigest: "proto/inference/v1/inference.proto",
    descriptorDigest: "rust/crates/inference/inference_descriptor.bin",
    conformanceDigest: "conformance/vectors/inference.json",
  },
};
for (const [family, artifacts] of Object.entries(familyArtifacts)) {
  for (const [field, path] of Object.entries(artifacts)) {
    if (compatibility.families[family][field] !== await digest(path)) {
      throw new Error(`${family} ${field} mismatch`);
    }
  }
}

for (const [canonical, packaged] of [
  ["conformance/vectors/core.json", "rust/crates/conformance/vectors/harness.json"],
  ["conformance/vectors/stream.json", "rust/crates/stream/conformance/stream.json"],
  ["conformance/vectors/stream.json", "rust/crates/conformance/vectors/stream.json"],
  ["conformance/vectors/objects.json", "rust/crates/conformance/vectors/objects.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  ["conformance/vectors/filesystem/dependency-content-range-v1.json", "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
]) {
  if (await digest(canonical) !== await digest(packaged)) {
    throw new Error(`packaged conformance vector drift: ${packaged}`);
  }
}

const validateProvenance = new Ajv2020().compile(await load("compatibility/schemas/provenance.schema.json"));
if (validateProvenance({ imports: [{ sourceCommit: "short" }] })) throw new Error("malformed provenance fixture was accepted");

// Inference 1.0.0-rc.3 is a released compatibility dependency. Keep its
// historical sparse-index entry available even after newer SDK releases move
// to crates.io.
const legacyRegistry = await load("registry/config.json");
if (
  legacyRegistry.dl !== "https://github.com/acyclic-labs/sdk/releases/download/inference-v{version}/{crate}-{version}.crate" ||
  legacyRegistry.api !== "https://github.com/acyclic-labs/sdk"
) {
  throw new Error("legacy sparse registry endpoints changed");
}
const legacyIndex = (await readFile(new URL("registry/in/fe/inference-sdk", root), "utf8"))
  .trim()
  .split("\n")
  .map(line => JSON.parse(line));
if (new Set(legacyIndex.map(entry => entry.vers)).size !== legacyIndex.length) {
  throw new Error("legacy sparse registry contains duplicate versions");
}
const legacyInference = legacyIndex.find(entry => entry.name === "inference-sdk" && entry.vers === "1.0.0-rc.3");
if (
  !legacyInference || legacyInference.yanked !== false ||
  legacyInference.cksum !== "b6ca7d6658bfe0b14728e25bad4caeb7ad6f4d74589c089ebbdf3c00d2fb846f"
) {
  throw new Error("released inference-sdk 1.0.0-rc.3 registry entry changed");
}

const inferenceIndex = (await readFile(new URL("registry/ac/yc/acyclic-inference", root), "utf8"))
  .trim()
  .split("\n")
  .map(line => JSON.parse(line));
if (new Set(inferenceIndex.map(entry => entry.vers)).size !== inferenceIndex.length) {
  throw new Error("acyclic-inference sparse registry contains duplicate versions");
}
const releasedInference = inferenceIndex.find(
  entry => entry.name === "acyclic-inference" && entry.vers === "1.0.0-rc.6",
);
if (
  !releasedInference || releasedInference.yanked !== false ||
  releasedInference.cksum !== "4fecfc3bf4d60d076d766f5128a36f0a6afd8c2dccdd3fec50eeebc592b2618c"
) {
  throw new Error("released acyclic-inference sparse registry entry changed");
}
