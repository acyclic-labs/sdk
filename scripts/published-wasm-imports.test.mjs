import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { rewriteModuleSpecifier } from "./assemble-native-family.mjs";

test("compiled package output imports the wasm that ships beside it", () => {
  const directory = mkdtempSync(join(tmpdir(), "published-wasm-imports-"));
  try {
    const files = {
      "contract.js": 'import init from "@acyclic/stream-wasm";\nexport * from "@acyclic/stream-wasm";\n',
      "contract.d.ts": 'export type Stream = import("@acyclic/stream-wasm").Stream;\n',
    };
    for (const [name, source] of Object.entries(files)) writeFileSync(join(directory, name), source);
    rewriteModuleSpecifier(directory, "@acyclic/stream-wasm", "../generated/wasm/acyclic_stream_wasm.js");
    for (const [name, source] of Object.entries(files)) {
      assert.equal(readFileSync(join(directory, name), "utf8"), source.replaceAll("@acyclic/stream-wasm", "../generated/wasm/acyclic_stream_wasm.js"), name);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
