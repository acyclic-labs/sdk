import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";

const packageRoot = process.env.ACTORS_INSTALLED_ROOT === undefined
  ? process.cwd()
  : resolve(process.env.ACTORS_INSTALLED_ROOT);
const actorsRoot = join(packageRoot, "node_modules", "@acyclic-labs", "actors");
const nativeRoot = join(actorsRoot, "generated", "native");
const manifest = JSON.parse(await readFile(join(actorsRoot, "package.json"), "utf8"));
const optionalNames = Object.keys(manifest.optionalDependencies ?? {}).filter(name => name.startsWith("@acyclic-labs/actors-"));
const optionalRoot = join(packageRoot, "node_modules", "@acyclic-labs");
const probe = "import { ActorId } from '@acyclic-labs/actors'; console.log(await ActorId('fallback-probe'));";

if (!existsSync(join(nativeRoot, "binding.cjs"))) {
  throw new Error(`installed Actors package is missing its generated native loader: ${nativeRoot}`);
}

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await walk(path));
    else files.push(path);
  }
  return files;
}

function run(executable) {
  return spawnSync(executable, ["--input-type=module", "-e", probe], { cwd: packageRoot, encoding: "utf8" });
}

function assertFallback(executable, label) {
  const result = run(executable);
  assert.equal(result.status, 0, `${label} fallback failed:\n${result.stderr}`);
  assert.match(result.stdout, /fallback-probe/);
}

function assertFatal(executable, label) {
  const result = run(executable);
  assert.notEqual(result.status, 0, `${label} unexpectedly fell back:\n${result.stdout}`);
}

async function withMutations(callback) {
  const moved = [];
  const created = [];
  const createdDirectories = new Set();
  const bytes = new Map();
  const move = async path => {
    const destination = `${path}.actors-fallback-${process.pid}-${moved.length}`;
    await rename(path, destination);
    moved.push({ path, destination });
  };
  const create = async (path, value) => {
    const directory = dirname(path);
    await mkdir(directory, { recursive: true });
    createdDirectories.add(directory);
    await writeFile(path, value);
    created.push(path);
  };
  try {
    await callback({ move, create, bytes });
  } finally {
    for (const [path, value] of bytes) await writeFile(path, value);
    for (const path of created.reverse()) await rm(path, { force: true });
    for (const directory of [...createdDirectories].reverse()) await rm(directory, { recursive: true, force: true });
    for (const { path, destination } of moved.reverse()) await rename(destination, path);
  }
}

const bunAvailable = spawnSync("bun", ["--version"], { encoding: "utf8" }).status === 0;
const executables = [{ name: "Node", command: process.execPath }];
if (bunAvailable) executables.push({ name: "Bun", command: "bun" });

// A package without the generated loader must use the maintained WASM path.
await withMutations(async ({ move }) => {
  await move(nativeRoot);
  for (const executable of executables) assertFallback(executable.command, `${executable.name} missing-loader`);
});

const localCandidates = (await walk(nativeRoot)).filter(path => /[\\/]index\.[^\\/]+\.(?:node|cjs)$/.test(path));
const companionDirectories = optionalNames
  .map(name => join(optionalRoot, name.slice("@acyclic-labs/".length)))
  .filter(existsSync);

// An intact generated loader with every target absent is an expected WASM fallback.
await withMutations(async ({ move }) => {
  for (const path of localCandidates) await move(path);
  for (const path of companionDirectories) await move(path);
  for (const executable of executables) assertFallback(executable.command, `${executable.name} missing-candidates`);
});

// A malformed native artifact must not be reclassified as optional absence.
const nativeBinaries = (await walk(nativeRoot)).filter(path => path.endsWith(".node"));
for (const directory of companionDirectories) nativeBinaries.push(...(await walk(directory)).filter(path => path.endsWith(".node")));
assert.ok(nativeBinaries.length > 0, "installed fixture has no native artifact to corrupt");
await withMutations(async ({ bytes }) => {
  for (const path of nativeBinaries) {
    bytes.set(path, await readFile(path));
    await writeFile(path, Buffer.from("malformed native artifact"));
  }
  assertFatal(process.execPath, "Node malformed-native");
});

// A missing dependency from inside a companion is fatal even when its name
// resembles an optional target package; the loader frame must be the source.
await withMutations(async ({ move, create }) => {
  for (const path of localCandidates) await move(path);
  for (const path of companionDirectories) await move(path);
  for (const name of optionalNames) {
    const packagePath = join(optionalRoot, name.slice("@acyclic-labs/".length));
    await create(join(packagePath, "package.json"), '{"type":"commonjs"}');
    await create(join(packagePath, "index.js"), "module.exports = require('@acyclic-labs/actors-transitive-spoof');\n");
  }
  assertFatal(process.execPath, "Node nested-companion-dependency");
});

console.log(JSON.stringify({ status: "passed", packageRoot, bun: bunAvailable }));
