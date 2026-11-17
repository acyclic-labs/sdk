import assert from "node:assert/strict";
import test from "node:test";
import { nativeFamily } from "./native-family.mjs";
import { wasmPackages } from "./wasm-packages.mjs";

test("a native family's wasm selection builds the package its JavaScript entry points live in", () => {
  for (const key of ["actors", "stream"]) {
    const family = nativeFamily(key);
    const selection = wasmPackages[key];
    assert.ok(selection, `${key} has a wasm selection`);
    assert.equal(selection.selection[selection.selection.indexOf("-p") + 1], family.wasmPackageName, `${key} wasm package`);
    assert.equal(selection.artifact, family.wasm.artifact, `${key} wasm artifact`);
    assert.equal(selection.outName ?? selection.artifact, family.wasm.outName, `${key} wasm output name`);
  }
});
