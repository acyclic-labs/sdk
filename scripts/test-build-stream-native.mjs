import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { assertExactInventory, assertSourceSnapshot, sourceSnapshot } from "./build-stream-native.mjs";

const root = fileURLToPath(new URL("..", import.meta.url));

test("native qualification rejects a compiled path dependency mutation", async () => {
  const snapshot = await sourceSnapshot();
  const dependency = resolve(root, "rust/crates/native-runtime/Cargo.toml");
  const original = await readFile(dependency);
  try {
    await writeFile(dependency, Buffer.concat([original, Buffer.from("\n# qualification mutation\n")]));
    await assert.rejects(assertSourceSnapshot(snapshot), /source closure changed/);
  } finally {
    await writeFile(dependency, original);
  }
  await assertSourceSnapshot(snapshot);
});

test("native qualification rejects a Windows junction in the source closure", { skip: process.platform !== "win32" }, async () => {
  const target = await mkdtemp(resolve(tmpdir(), "stream-native-junction-target-"));
  const junction = resolve(root, "rust/crates/native-runtime/.qualification-junction");
  try {
    await symlink(target, junction, "junction");
    await assert.rejects(sourceSnapshot(), /symlink or reparse point/);
  } finally {
    await rm(junction, { recursive: true, force: true });
    await rm(target, { recursive: true, force: true });
  }
});

test("native staging rejects stale package files", async () => {
  const output = await mkdtemp(resolve(tmpdir(), "stream-native-stale-output-"));
  try {
    await writeFile(resolve(output, "binding.cjs"), "ok\n");
    await writeFile(resolve(output, "stale.node"), "stale\n");
    await assert.rejects(assertExactInventory(output, new Set(["binding.cjs"])), /unstated files: stale\.node/);
  } finally {
    await rm(output, { recursive: true, force: true });
  }
});
