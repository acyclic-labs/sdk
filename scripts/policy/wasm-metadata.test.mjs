import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

test("browser package selections load with only declared metadata", () => {
  const root = mkdtempSync(join(tmpdir(), "wasm-metadata-"));
  try {
    for (const path of ["scripts/wasm-packages.mjs", "scripts/native-family-metadata.mjs",
      "scripts/native-family.mjs", "scripts/proto-codegen.mjs", "scripts/bazel-executables.mjs",
      "scripts/generated-bindings.mjs", "scripts/policy-core.mjs",
      "compatibility/native-families/actors.json", "compatibility/native-families/stream.json"]) {
      mkdirSync(dirname(join(root, path)), { recursive: true });
      cpSync(path, join(root, path));
    }
    writeFileSync(join(root, "no-packages.mjs"), `
      export function resolve(specifier, context, nextResolve) {
        if (!specifier.startsWith('node:') && !specifier.startsWith('.') && !specifier.startsWith('/')) {
          throw Error('undeclared package: ' + specifier);
        }
        return nextResolve(specifier, context);
      }
    `);
    const result = spawnSync(process.execPath, ["--experimental-loader", "./no-packages.mjs", "--input-type=module", "-e", `
      import { wasmPackages, wasmSelection } from './scripts/wasm-packages.mjs';
      for (const name of Object.keys(wasmPackages)) {
        const { selection, artifact, outName } = wasmSelection(name);
        if (!selection.includes('-p') || !artifact || !outName) throw Error(name);
      }
    `], { cwd: root, encoding: "utf8" });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
