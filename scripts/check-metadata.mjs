import { Ajv2020 } from "ajv/dist/2020.js";
import { readFile } from "node:fs/promises";
import { checkManifestFields, repositoryRoot, repositoryUrl as root } from "./policy-core.mjs";
import "./check-compatibility-digests.mjs";
import { packagedSourceCopies } from "./generated-bindings.mjs";
import { packagedTypeScriptBindings } from "./protobuf-bindings.mjs";
import { sha256File } from "./file-digest.mjs";
import { retiredRuntimeFindings } from "./retired-runtime.mjs";
import { parseJsonc } from "./jsonc.mjs";

const load = async path => JSON.parse(await readFile(new URL(path, root), "utf8"));
const ajv = new Ajv2020({ allErrors: true });
ajv.compile(await load("rust/crates/conformance/schemas/runner-report.schema.json"));
new Ajv2020({ allErrors: true }).compile(
  await load("rust/crates/conformance/schemas/qualification-receipt.schema.json"),
);
const documents = [
  ["provenance/manifest.json", "compatibility/schemas/provenance.schema.json"],
  ["languages/package-names.json", "compatibility/schemas/package-names.schema.json"],
  ["compatibility/manifest.json", "compatibility/schemas/compatibility.schema.json"],
  ["release/cargo-crates.json", "compatibility/schemas/release-cargo-crates.schema.json"],
  ["release/cargo-equivalent-archives.json", "compatibility/schemas/release-cargo-equivalent-archives.schema.json"],
  ["release/npm-packages.json", "compatibility/schemas/release-npm-packages.schema.json"],
];
const checkedDocuments = new Map();
for (const [documentPath, schemaPath] of documents) {
  const validate = ajv.compile(await load(schemaPath));
  const document = await load(documentPath);
  if (!validate(document)) throw new Error(`${documentPath}: ${ajv.errorsText(validate.errors)}`);
  checkedDocuments.set(documentPath, document);
}
const retiredFindings = retiredRuntimeFindings(checkedDocuments);
if (retiredFindings.length) throw new Error(retiredFindings.join("\nerror: "));
const provenance = await load("provenance/manifest.json");
for (const item of provenance.imports) if (item.auditResult !== "approved") throw new Error(`unapproved import: ${item.destinationPath}`);

const digest = async path => `sha256:${await sha256File(new URL(path, root))}`;
const compatibility = await load("compatibility/manifest.json");
const sdkPackage = await load("typescript/packages/sdk/package.json");
for (const path of ["plugin/plugin.json", "plugin/.codex-plugin/plugin.json"]) {
  if ((await load(path)).version !== sdkPackage.version) {
    throw new Error(`host plugin manifest version mismatch: ${path}`);
  }
}
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
const lock = /** @type {{ workspaces: Record<string, { name: string, version?: string }> }} */ (
  parseJsonc(await readFile(new URL("bun.lock", root), "utf8"))
);
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
const fields = [
  ["Cargo.toml", ["workspace", "package", "version"], compatibility.families.harness.version],
  ["rust/crates/harness/Cargo.toml", ["package", "version", "workspace"], true],
  ["typescript/packages/sdk/package.json", ["version"], compatibility.umbrellaVersion],
];
/** @type {[string, string[], string[], string[]][]} */
const families = [
  ["harness", [], ["sdk"], []],
  ["stream", ["stream"], ["sdk"], ["conformance", "filesystem", "filesystem-wasm", "harness"]],
  ["machines", ["machines"], ["sdk"], ["conformance", "harness"]],
  ["inference", ["inference", "inference-wasm"], ["sdk"], ["inference-wasm"]],
  ["objects", ["objects"], ["sdk", "harness"], ["conformance", "filesystem", "filesystem-wasm", "harness"]],
  ["filesystem", ["filesystem", "filesystem-napi", "filesystem-wasm"], [], []],
];
for (const [family, crates, npmConsumers, rustConsumers] of families) {
  const {version} = compatibility.families[family];
  const crateVersion = ["stream", "objects", "filesystem"].includes(family) ? compatibility.families[family].crateVersion ?? version : version;
  fields.push([`typescript/packages/${family}/package.json`, ["version"], version]);
  for (const crate of crates)
    fields.push([`rust/crates/${crate}/Cargo.toml`, ["package", "version"], crate === family ? crateVersion : version]);
  for (const consumer of npmConsumers)
    fields.push([`typescript/packages/${consumer}/package.json`, ["dependencies", `@acyclic-labs/${family}`], version]);
  for (const consumer of rustConsumers)
    fields.push([`rust/crates/${consumer}/Cargo.toml`, ["dependencies", `acyclic-${family}`, "version"], `=${crateVersion}`]);
}
checkManifestFields(repositoryRoot, fields);
const filesystemVersion = compatibility.families.filesystem.version;
const nativeSource = await readFile(new URL("typescript/packages/filesystem/src/native.ts", root), "utf8");
const nativePackageVersion = nativeSource.match(/const PACKAGE_VERSION = "([^"]+)";/)?.[1];
if (nativePackageVersion !== filesystemVersion) {
  throw new Error("filesystem native companion version does not match package metadata");
}

// Generated code runs only on the protobuf runtime of its generator's release.
const protobufRuntime = (await load("package.json")).devDependencies?.["@bufbuild/protoc-gen-es"];
for (const [stem, packages] of packagedTypeScriptBindings) {
  const family = stem.split("/")[0];
  if (!packages.includes(family)) continue;
  const manifest = await load(`typescript/packages/${family}/package.json`);
  const prefix = `./generated/proto/${stem}`;
  const exported = Object.values(manifest.exports ?? {}).some(entry =>
    entry?.types === `${prefix}.d.ts` && entry?.default === `${prefix}.js`);
  if (!exported
    || !manifest.files?.some(path => path === "generated" || path === "generated/proto")
    || manifest.dependencies?.["@bufbuild/protobuf"] !== protobufRuntime) {
    throw new Error(`${family} generated protobuf package export mismatch`);
  }
}

for (const [canonical, packaged] of packagedSourceCopies) {
  if (await digest(canonical) !== await digest(packaged)) {
    throw new Error(`packaged source drift: ${packaged}`);
  }
}

const validateProvenance = new Ajv2020().compile(await load("compatibility/schemas/provenance.schema.json"));
if (validateProvenance({ imports: [{ sourceCommit: "short" }] })) throw new Error("malformed provenance fixture was accepted");
