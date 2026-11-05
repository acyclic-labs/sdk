import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { copyWasmArtifacts, isWasmArtifactPath, preserveWasmArtifacts, wasmArtifactFiles, wasmArtifactInventory } from "./archive-utils.mjs";

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
    utimesSync(join(output, snippet), 1, 1);
    const unchanged = statSync(join(output, snippet), { bigint: true }).mtimeNs;
    const producerReceipt = join(output, "producer-receipt.json");
    const producerBytes = readFileSync(new URL("./build-wasm.mjs", import.meta.url));
    writeFileSync(producerReceipt, producerBytes);
    utimesSync(producerReceipt, 1, 1);
    assert.deepEqual(copyWasmArtifacts(source, output), receipt);
    assert.equal(statSync(join(output, snippet), { bigint: true }).mtimeNs, unchanged);
    assert.equal(statSync(producerReceipt, { bigint: true }).mtimeNs, unchanged);
    assert.deepEqual(readFileSync(producerReceipt), producerBytes);
    writeFileSync(join(output, snippet), "modified snippet");
    assert.notDeepEqual(wasmArtifactInventory(wasmArtifactFiles(output)), receipt);
    for (const present of [true, false]) {
      const selected = present ? source : join(temporary, "absent");
      const original = present ? wasmArtifactInventory(wasmArtifactFiles(selected)) : [];
      for (const outcome of ["built", "refused", "no-output"]) {
        const custody = preserveWasmArtifacts(selected, "node scripts/build-wasm.mjs harness");
        let candidate;
        try {
          if (outcome === "no-output") throw new Error("qualification refused");
          if (present) {
            mkdirSync(selected, { recursive: true });
            cpSync(custody.source(join(selected, "module.js")), join(selected, "module.js"));
            assert.equal(readFileSync(join(selected, "module.js"), "utf8"), "module bytes");
          }
          mkdirSync(join(selected, "snippets", "candidate"), { recursive: true });
          // Derived source bytes exercise real directory custody, not a compiler fixture.
          writeFileSync(join(selected, "module.js"), readFileSync(new URL("./build-wasm.mjs", import.meta.url)));
          writeFileSync(join(selected, "snippets", "candidate", "inline.js"), readFileSync(import.meta.filename));
          candidate = wasmArtifactInventory(wasmArtifactFiles(selected));
          if (outcome === "refused") throw new Error("qualification refused");
        } catch (error) {
          if (outcome === "built" || error.message !== "qualification refused") throw error;
        } finally { custody.restore(); }
        if (candidate) assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(custody.archive)), candidate,
          "temporary build was not retained separately");
        else assert.equal(existsSync(custody.archive), false, "absent temporary build was fabricated");
        if (candidate) assert.deepEqual(readFileSync(custody.retained(join(selected, "module.js"))),
          readFileSync(new URL("./build-wasm.mjs", import.meta.url)), "receipt names replaced output bytes");
        const outside = join(temporary, "outside", "module.js");
        assert.equal(custody.source(outside), outside);
        assert.equal(custody.retained(outside), outside);
        assert.equal(existsSync(selected), present, "temporary build replaced original output presence");
        if (present) assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(selected)), original,
          "temporary build replaced original output bytes");
      }
    }
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
