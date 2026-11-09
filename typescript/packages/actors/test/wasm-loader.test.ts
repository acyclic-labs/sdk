import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

test.each(["preinitialized", "provided", "file"])("guest restore admits declared bindings with %s WASM", mode => {
  // Each process owns a fresh real bridge; the first two forbid even importing fs.
  const result = spawnSync("node", [fileURLToPath(new URL("./wasm-loader.fixture.mjs", import.meta.url)), mode], { encoding: "utf8" });
  expect(result.stderr).toBe("");
  expect(result.stdout.trim()).toBe(`restored:${mode}`);
  expect(result.status).toBe(0);
});
