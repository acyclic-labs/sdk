import assert from "node:assert/strict";
import { cpSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { isWasmArtifactPath, wasmArtifactFiles, wasmArtifactInventory } from "./archive-utils.mjs";

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
    cpSync(source, output, { recursive: true });
    assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(output)), receipt);
    writeFileSync(join(output, snippet), "modified snippet");
    assert.notDeepEqual(wasmArtifactInventory(wasmArtifactFiles(output)), receipt);
    rmSync(join(source, "snippets", "nested"), { recursive: true });
    symlinkSync(output, join(source, "snippets", "linked"), process.platform === "win32" ? "junction" : "dir");
    assert.throws(() => [...wasmArtifactFiles(source)], /non-regular artifact; recovery:/);
    for (const path of ["../outside", "snippets/../outside", "snippets//inline.js", "other/nested.js", "snippets\\outside"]) {
      assert.equal(isWasmArtifactPath(path), false, path);
    }
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});
