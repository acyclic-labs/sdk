// Run with Node and Bun against the same installed neutral parent archive.
import assert from "node:assert/strict";
import { readFile, writeFile, rename, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const require = createRequire(import.meta.url);
const parent = resolve(dirname(require.resolve("@acyclic-labs/stream/native")), "..");
const dependencies = JSON.parse(await readFile(join(parent, "package.json"), "utf8")).optionalDependencies;
const names = Object.keys(dependencies).filter(name => { try { require.resolve(name); return true; } catch { return false; } });
assert.equal(names.length, 1, "fixture requires exactly the runtime-selected companion");
const companion = dirname(require.resolve(names[0]));
const binary = require.resolve(names[0]);
const manifestPath = join(companion, "package.json");
const manifest = await readFile(manifestPath);
const saved = await readFile(binary);
const moved = `${companion}.missing-${process.pid}`;
function check(expected) {
  const code = `import assert from 'node:assert/strict';import {nativeStreamAvailable} from '@acyclic-labs/stream/native';${expected}`;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", code], { cwd: process.cwd(), encoding: "utf8", timeout: 10000 });
  assert.equal(result.status, 0, result.stderr || String(result.error));
}
check("assert.equal(await nativeStreamAvailable(),true)");
try {
  await rename(companion, moved);
  check("assert.equal(await nativeStreamAvailable(),false)");
} finally { await rename(moved, companion); }
try {
  await rename(binary, `${binary}.missing`);
  check("await assert.rejects(nativeStreamAvailable())");
} finally { await rename(`${binary}.missing`, binary); }
try {
  await writeFile(binary, "corrupt native addon");
  check("await assert.rejects(nativeStreamAvailable())");
} finally { await writeFile(binary, saved); }
const fake = join(companion, "wrong-binding.cjs");
try {
  await writeFile(fake, "module.exports = {};");
  await writeFile(manifestPath, JSON.stringify({ ...JSON.parse(manifest), main: "wrong-binding.cjs" }));
  check("await assert.rejects(nativeStreamAvailable(),/generated Rust N-API binding/)");
  await writeFile(fake, "module.exports = {NativeStreamClient:{},NativeStreamCancellation:{}};");
  check("await assert.rejects(nativeStreamAvailable(),/generated Rust N-API binding/)");
  await writeFile(fake, "module.exports = {NativeStreamClient:class {},NativeStreamCancellation:class {}};");
  check("await assert.rejects(nativeStreamAvailable(),/generated Rust N-API binding/)");
  await writeFile(fake, "require('missing-internal-native-dependency');");
  check("await assert.rejects(nativeStreamAvailable())");
  await writeFile(fake, "require('@acyclic-labs/stream-missing-transitive');");
  check("await assert.rejects(nativeStreamAvailable())");
} finally { await writeFile(manifestPath, manifest); await rm(fake); }
console.log(JSON.stringify({ status: "passed", nativeAvailable: true, missingCompanionFallsBack: true, missingAddonRejected: true, corruptAddonRejected: true, wrongBindingRejected: true, nonCallableBindingRejected: true, missingConnectAbiRejected: true, missingInternalDependencyRejected: true, missingNamespacedDependencyRejected: true }));
