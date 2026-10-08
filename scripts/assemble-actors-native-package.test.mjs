import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { assertCommonBindings, assertNativeSet } from "./assemble-actors-native-package.mjs";

const targets = ["target-a", "target-b", "target-c"];
const source = "a".repeat(40);
const qualified = () => targets.map(selected_target => ({ selected_target, source_revision: source, source_sha256: "sha256:source", source_files: [{ path: "contract.rs", sha256: "sha256:contract", bytes: 10 }], package: "acyclic-actors-napi", version: "0.2.0", targets }));

test("assembly requires one bundle for each declared Rust target", () => {
  assert.doesNotThrow(() => assertNativeSet(qualified(), targets, source));
  const missing = qualified().slice(1);
  const duplicate = qualified(); duplicate[1].selected_target = targets[0];
  const unexpected = qualified(); unexpected[1].selected_target = "unqualified";
  for (const metadata of [missing, duplicate, unexpected]) assert.throws(() => assertNativeSet(metadata, targets, source), /exactly one bundle/);
});

test("assembly rejects divergent source, version and Rust target declarations", () => {
  /** @type {Array<[string, any]>} */
  const mutations = [["source_revision", "b".repeat(40)], ["source_sha256", "sha256:changed"], ["source_files", []], ["version", "0.3.0"], ["targets", [...targets, "unqualified"]]];
  for (const [field, value] of mutations) {
    const metadata = qualified(); metadata[1][field] = value;
    assert.throws(() => assertNativeSet(metadata, targets, source), /source|version|targets/);
  }
  assert.throws(() => assertNativeSet(qualified(), [...targets].reverse(), source), /declaration differs/);
});

test("common loader and declarations must match actual bytes across bundles", async () => {
  const root = await mkdtemp(join(tmpdir(), "actors-neutral-bindings-"));
  const bundles = targets.map(target => join(root, target));
  try {
    for (const bundle of bundles) {
      await mkdir(bundle);
      for (const name of ["binding.cjs", "binding.d.ts"]) await writeFile(join(bundle, name), "common bytes\n");
    }
    await assertCommonBindings(bundles);
    for (const name of ["binding.cjs", "binding.d.ts"]) {
      await writeFile(join(bundles[1], name), "different compiled bytes\n");
      await assert.rejects(() => assertCommonBindings(bundles), new RegExp(`${name.replaceAll(".", "\\.")} differs`));
      await writeFile(join(bundles[1], name), "common bytes\n");
    }
    await rm(join(bundles[2], "binding.cjs"));
    await assert.rejects(() => assertCommonBindings(bundles), { code: "ENOENT" });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("assembly rejects stale output before discovering Cargo", async () => {
  const output = await mkdtemp(join(tmpdir(), "actors-neutral-stale-"));
  try {
    await writeFile(join(output, "unqualified-companion.tgz"), "stale archive");
    const script = fileURLToPath(new URL("./assemble-actors-native-package.mjs", import.meta.url));
    const result = spawnSync(process.execPath, [script, "--bundle", "not-built", "--output", output, "--source-sha", source], { env: { ...process.env, PATH: "" }, encoding: "utf8" });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /unstated files: unqualified-companion/);
    assert.doesNotMatch(result.stderr, /cargo.*ENOENT/i);
  } finally { await rm(output, { recursive: true, force: true }); }
});
