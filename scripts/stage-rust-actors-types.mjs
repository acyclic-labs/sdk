import {
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageRoot = join(root, "typescript/packages/actors");
const usage = "usage: stage-rust-actors-types.mjs <write|check> <generation-bundle>";

const [operation, bundleArgument, ...extra] = process.argv.slice(2);
if (!["write", "check"].includes(operation) || !bundleArgument || extra.length) {
  throw new Error(usage);
}

const bundle = resolve(bundleArgument);
const sourceRoot = join(bundle, "generated/typescript/actors");
const sourceBarrel = join(sourceRoot, "types.ts");
const destinationRoot = join(packageRoot, "src/actors");
const destinationBarrel = join(packageRoot, "src/types.ts");

const rejectLink = path => {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink()) throw new Error(`symlink is not allowed: ${path}`);
  return stat;
};

const digest = path => {
  const bytes = readFileSync(path);
  return {
    sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    bytes: bytes.length,
  };
};

if (!existsSync(join(bundle, "generation-manifest.json"))) {
  throw new Error(`generation manifest is missing: ${bundle}`);
}
const manifest = JSON.parse(readFileSync(join(bundle, "generation-manifest.json"), "utf8"));
if (manifest.schema !== "acyclic.sdk.generation.v1" || manifest.family !== "acyclic_actors") {
  throw new Error("generation bundle is not an Actors SDK bundle");
}
const artifactByPath = new Map((manifest.artifacts ?? []).map(artifact => [artifact.path, artifact]));

if (!existsSync(sourceRoot) || !rejectLink(sourceRoot).isDirectory()) {
  throw new Error(`generated Actors TypeScript directory is missing: ${sourceRoot}`);
}
if (!existsSync(sourceBarrel) || !rejectLink(sourceBarrel).isFile()) {
  throw new Error(`generated Actors TypeScript barrel is missing: ${sourceBarrel}`);
}

const modules = readdirSync(sourceRoot, { withFileTypes: true })
  .filter(entry => entry.isFile() && entry.name.endsWith(".ts") && entry.name !== "types.ts")
  .map(entry => entry.name.slice(0, -3))
  .sort();
if (!modules.length) throw new Error("generated Actors TypeScript declarations are empty");

const sourceBarrelText = readFileSync(sourceBarrel, "utf8");
const expectedSourceBarrel = [
  "// Generated from Rust-owned Actors semantic declarations.",
  ...modules.map(module => `export * from \"./${module}.js\";`),
  "",
].join("\n");
if (sourceBarrelText !== expectedSourceBarrel) {
  throw new Error("generated Actors TypeScript barrel does not match its declaration modules");
}

const expected = new Map();
for (const module of modules) {
  const relative = `generated/typescript/actors/${module}.ts`;
  const source = join(sourceRoot, `${module}.ts`);
  rejectLink(source);
  const actual = digest(source);
  const recorded = artifactByPath.get(relative);
  if (!recorded || recorded.sha256 !== actual.sha256 || recorded.bytes !== actual.bytes) {
    throw new Error(`generation manifest does not attest ${relative}`);
  }
  expected.set(`${module}.ts`, readFileSync(source));
}
const barrelRelative = "generated/typescript/actors/types.ts";
const barrelDigest = digest(sourceBarrel);
const recordedBarrel = artifactByPath.get(barrelRelative);
if (!recordedBarrel || recordedBarrel.sha256 !== barrelDigest.sha256 || recordedBarrel.bytes !== barrelDigest.bytes) {
  throw new Error(`generation manifest does not attest ${barrelRelative}`);
}

const packageBarrel = [
  "// Generated from the Rust-owned Actors semantic declarations.",
  ...modules.map(module => `export * from \"./actors/${module}.js\";`),
  "",
].join("\n");
const existing = existsSync(destinationRoot) ? readdirSync(destinationRoot, { withFileTypes: true }) : [];
for (const entry of existing) {
  if (entry.isFile() && entry.name.endsWith(".ts") && !expected.has(entry.name)) {
    throw new Error(`unexpected authored or stale file in generated Actors source: ${entry.name}`);
  }
}

const compare = (path, bytes) => existsSync(path) && readFileSync(path).equals(bytes);
const writeAtomic = (path, bytes) => {
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.tmp-${process.pid}`;
  writeFileSync(temporary, bytes);
  renameSync(temporary, path);
};

for (const [name, bytes] of expected) {
  const destination = join(destinationRoot, name);
  if (operation === "check") {
    if (!compare(destination, bytes)) throw new Error(`staged Actors declaration is stale: ${destination}`);
  } else {
    writeAtomic(destination, bytes);
  }
}
const barrelBytes = Buffer.from(packageBarrel);
if (operation === "check") {
  if (!compare(destinationBarrel, barrelBytes)) throw new Error(`staged Actors types barrel is stale: ${destinationBarrel}`);
} else {
  writeAtomic(destinationBarrel, barrelBytes);
}

console.log(`${operation}: staged ${modules.length} Rust-owned Actors TypeScript declarations and src/types.ts`);
