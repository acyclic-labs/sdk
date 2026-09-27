import { afterEach, expect, test } from "bun:test";
import { openMemoryFs } from "../src/memory-node.js";
import type { FsVolumeEngine } from "../src/contracts.js";

const engines: FsVolumeEngine[] = [];

afterEach(async () => {
  await Promise.all(engines.splice(0).map((engine) => engine.close()));
});

test("WASM workspace operations use the Rust canonical name", async () => {
  const engine = await openMemoryFs();
  engines.push(engine);

  const workspace = await engine.createWorkspace("cafe\u0301");
  expect(workspace.name).toBe("caf\u00e9");

  for (const invalid of ["", ".", "..", "a/b", "a\\\\b", "a\u0000b", "a".repeat(256)]) {
    await expect(engine.createWorkspace(invalid)).rejects.toBeDefined();
  }
});
