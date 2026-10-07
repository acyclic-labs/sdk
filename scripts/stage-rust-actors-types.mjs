import {
  existsSync,
  closeSync,
  lstatSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  realpathSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = realpathSync(resolve(dirname(fileURLToPath(import.meta.url)), ".."));
const packageRoot = join(root, "typescript/packages/actors");
const usage = "usage: stage-rust-actors-types.mjs <write|check|contract-write|contract-check> <generation-bundle>";

const [operation, bundleArgument, ...extra] = process.argv.slice(2);
if (!["write", "check", "contract-write", "contract-check"].includes(operation) || !bundleArgument || extra.length) {
  throw new Error(usage);
}

const bundle = resolve(bundleArgument);
const manifestPath = join(bundle, "generation-manifest.json");
const sourceRoot = join(bundle, "generated/typescript/actors");
const sourceBarrel = join(sourceRoot, "types.ts");
const destinationRoot = join(packageRoot, "src/actors");
const destinationBarrel = join(packageRoot, "src/types.ts");

const rejectLink = path => {
  const stat = lstatSync(path);
  if (stat.isSymbolicLink()) throw new Error(`symlink is not allowed: ${path}`);
  return stat;
};

if (!existsSync(bundle) || !rejectLink(bundle).isDirectory()) {
  throw new Error(`generation bundle directory is missing or invalid: ${bundle}`);
}
const bundleRoot = realpathSync(bundle);

const isOutsideRoot = (base, path) => {
  const relativePath = relative(base, path);
  return relativePath === ".." || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath);
};

const assertDestination = path => {
  const absolute = resolve(path);
  if (isOutsideRoot(root, absolute)) throw new Error(`staging destination escapes checkout: ${absolute}`);
  let current = absolute;
  while (true) {
    let stat;
    try {
      stat = lstatSync(current);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    if (stat) {
      if (stat.isSymbolicLink()) {
        throw new Error(`staging destination contains a symlink or reparse point: ${current}`);
      }
      if (isOutsideRoot(root, realpathSync(current))) {
        throw new Error(`staging destination resolves outside checkout: ${current}`);
      }
    }
    if (current === root) return;
    const parent = dirname(current);
    if (parent === current) throw new Error(`staging destination has no checkout ancestor: ${absolute}`);
    current = parent;
  }
};

const assertSource = path => {
  const absolute = resolve(path);
  if (isOutsideRoot(bundleRoot, absolute)) {
    throw new Error(`generation source escapes bundle: ${absolute}`);
  }
  let current = absolute;
  while (true) {
    let stat;
    try {
      stat = lstatSync(current);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    if (stat) {
      if (stat.isSymbolicLink()) {
        throw new Error(`generation source contains a symlink or reparse point: ${current}`);
      }
      if (isOutsideRoot(bundleRoot, realpathSync(current))) {
        throw new Error(`generation source resolves outside bundle: ${current}`);
      }
    }
    if (relative(bundleRoot, current) === "") return;
    const parent = dirname(current);
    if (parent === current) throw new Error(`generation source has no bundle ancestor: ${absolute}`);
    current = parent;
  }
};

const temporaryPath = path => `${path}.tmp-${process.pid}`;
const preflightDestination = path => {
  assertDestination(path);
  const temporary = temporaryPath(path);
  assertDestination(temporary);
  if (existsSync(temporary)) throw new Error(`staging temporary already exists: ${temporary}`);
};

const digestBytes = bytes => {
  return {
    sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    bytes: bytes.length,
  };
};

if (!existsSync(manifestPath)) {
  throw new Error(`generation manifest is missing: ${bundle}`);
}
assertSource(manifestPath);
const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
if (manifest.schema !== "acyclic.sdk.generation.v1" || manifest.family !== "acyclic_actors") {
  throw new Error("generation bundle is not an Actors SDK bundle");
}
if (!Array.isArray(manifest.artifacts)) throw new Error("generation manifest artifacts are missing");
const artifactByPath = new Map();
for (const artifact of manifest.artifacts) {
  if (!artifact || typeof artifact.path !== "string" || !artifact.path || artifactByPath.has(artifact.path)) {
    throw new Error("generation manifest contains duplicate or invalid artifact paths");
  }
  artifactByPath.set(artifact.path, artifact);
}
const compare = (path, bytes) => {
  assertDestination(path);
  return existsSync(path) && readFileSync(path).equals(bytes);
};
const writeAtomic = (path, bytes) => {
  preflightDestination(path);
  mkdirSync(dirname(path), { recursive: true });
  const temporary = temporaryPath(path);
  let descriptor;
  let temporaryCreated = false;
  try {
    descriptor = openSync(temporary, "wx");
    temporaryCreated = true;
    writeFileSync(descriptor, bytes);
    closeSync(descriptor);
    descriptor = undefined;
    renameSync(temporary, path);
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
    if (temporaryCreated && existsSync(temporary)) unlinkSync(temporary);
  }
};

const requireAttestedArtifact = relative => {
  const source = join(bundle, relative);
  assertSource(source);
  if (!existsSync(source) || !rejectLink(source).isFile()) {
    throw new Error(`generated bundle artifact is missing: ${source}`);
  }
  const bytes = readFileSync(source);
  const actual = digestBytes(bytes);
  const recorded = artifactByPath.get(relative);
  if (!recorded || recorded.sha256 !== actual.sha256 || recorded.bytes !== actual.bytes) {
    throw new Error(`generation manifest does not attest ${relative}`);
  }
  return bytes;
};

function stageWorkersContract(stageOperation) {
  if (!Array.isArray(manifest.families) || !manifest.families.includes("acyclic_workers")) {
    throw new Error("generation bundle does not contain the Workers documentation family");
  }
  const protoRelative = "generated/workers/proto/workers/v1/workers.proto";
  const descriptorRelative = "generated/workers/acyclic-workers-v1.bin";
  const bytes = requireAttestedArtifact(protoRelative);
  requireAttestedArtifact(descriptorRelative);
  const header = "// Generated from Rust-owned Workers contract. Do not edit.\n";
  if (!bytes.toString("utf8").startsWith(header)) {
    throw new Error(`Workers proto is missing its Rust-generated header: ${join(bundle, protoRelative)}`);
  }
  const destination = join(root, "proto/workers/v1/workers.proto");
  preflightDestination(destination);
  if (stageOperation === "check") {
    if (!compare(destination, bytes)) throw new Error(`staged Workers proto is stale: ${destination}`);
  } else {
    writeAtomic(destination, bytes);
  }
  console.log(`${stageOperation}: staged Rust-owned Workers proto`);
}

if (operation === "contract-write" || operation === "contract-check") {
  stageWorkersContract(operation === "contract-write" ? "write" : "check");
  process.exit(0);
}

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

const barrelRelative = "generated/typescript/actors/types.ts";
assertSource(sourceBarrel);
const sourceBarrelBytes = requireAttestedArtifact(barrelRelative);
const sourceBarrelText = sourceBarrelBytes.toString("utf8");
const expectedSourceBarrel = [
  "// Generated from Rust-owned Actors semantic declarations.",
  ...modules.map(module => `export * from "./${module}.js";`),
  "",
].join("\n");
if (sourceBarrelText !== expectedSourceBarrel) {
  throw new Error("generated Actors TypeScript barrel does not match its declaration modules");
}

const expected = new Map();
for (const module of modules) {
  const relative = `generated/typescript/actors/${module}.ts`;
  const bytes = requireAttestedArtifact(relative);
  expected.set(`${module}.ts`, bytes);
}

const packageBarrel = [
  "// Generated from the Rust-owned Actors semantic declarations.",
  ...modules.map(module => `export * from "./actors/${module}.js";`),
  "",
].join("\n");
preflightDestination(destinationRoot);
preflightDestination(destinationBarrel);
const existing = existsSync(destinationRoot) ? readdirSync(destinationRoot, { withFileTypes: true }) : [];
for (const entry of existing) {
  rejectLink(join(destinationRoot, entry.name));
  if (entry.isFile() && entry.name.endsWith(".ts") && !expected.has(entry.name)) {
    throw new Error(`unexpected authored or stale file in generated Actors source: ${entry.name}`);
  }
}

for (const name of expected.keys()) preflightDestination(join(destinationRoot, name));

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
