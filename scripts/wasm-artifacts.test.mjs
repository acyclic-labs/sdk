import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { copyWasmArtifacts, isWasmArtifactPath, wasmArtifactFiles, wasmArtifactInventory } from "./archive-utils.mjs";

test("artifact trees retain nested snippets and refuse changed bytes or symlinks", () => {
  const temporary = mkdtempSync(join(tmpdir(), "wasm-artifact-tree-"));
  const source = join(temporary, "source"), output = join(temporary, "output");
  try {
    // Arbitrary file bytes exercise admission; this is not a compiler fixture.
    mkdirSync(join(source, "snippets", "nested"), { recursive: true });
    writeFileSync(join(source, "module.js"), "module bytes");
    const snippet = join("snippets", "nested", "inline.js");
    writeFileSync(join(source, snippet), "original snippet");
    const receipt = wasmArtifactInventory(wasmArtifactFiles(source));
    assert.deepEqual(receipt.map(item => item.path), ["generated/wasm/module.js", "generated/wasm/snippets/nested/inline.js"]);
    mkdirSync(join(output, "snippets", "stale"), { recursive: true });
    writeFileSync(join(output, "snippets", "stale", "inline.js"), "previous build");
    assert.deepEqual(copyWasmArtifacts(source, output), receipt);
    assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(output)), receipt);
    writeFileSync(join(output, snippet), "modified snippet");
    assert.notDeepEqual(wasmArtifactInventory(wasmArtifactFiles(output)), receipt);
    symlinkSync(source, join(output, "snippets", "linked"), process.platform === "win32" ? "junction" : "dir");
    assert.throws(() => copyWasmArtifacts(source, output), /non-regular artifact; recovery:/);
    assert.equal(readFileSync(join(output, snippet), "utf8"), "modified snippet");
    rmSync(join(source, "snippets", "nested"), { recursive: true });
    symlinkSync(output, join(source, "snippets", "linked"), process.platform === "win32" ? "junction" : "dir");
    assert.throws(() => [...wasmArtifactFiles(source)], /non-regular artifact; recovery:/);
    for (const path of ["../outside", "snippets/../outside", "snippets//inline.js", "other/nested.js", "snippets\\outside"]) {
      assert.equal(isWasmArtifactPath(path), false, path);
    }
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});
