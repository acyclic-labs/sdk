import { expect, test } from "bun:test";
import fc from "fast-check";
import { bindingKey, cachedRunner } from "./run-node-wasm-tests.mjs";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

function invalidationOracle(keyFor, digest, change) {
  const key = keyFor(digest, "runner", [], [], "script");
  expect(keyFor(digest, "runner", [], [], "script")).toBe(key);
  expect(keyFor(digest + change, "runner", [], [], "script")).not.toBe(key);
  expect(keyFor(digest, "runner" + change, [], [], "script")).not.toBe(key);
  expect(keyFor(digest, "runner", [change], [], "script")).not.toBe(key);
  expect(keyFor(digest, "runner", [], [["NODE_ARGS",change]], "script")).not.toBe(key);
  expect(keyFor(digest, "runner", [], [], "script" + change)).not.toBe(key);
}

test("binding cache invalidates every input and retains identical inputs", () => {
  fc.assert(fc.property(fc.string(), fc.string({minLength:1}), (digest, change) => {
    invalidationOracle(bindingKey, digest, change);
    expect(() => invalidationOracle((_digest, version, args, env, source) => bindingKey("omitted", version, args, env, source), digest, change)).toThrow();
  }), {numRuns:64});
});

test("cache finds both upstream Node module formats", () => {
  for (const file of ["run.cjs", "run.mjs"]) {
    const cache = mkdtempSync(join(tmpdir(), "node-wasm-cache-"));
    try {
      mkdirSync(join(cache,"generated"));
      const entry = join(cache,"generated",file);
      writeFileSync(entry, "// upstream runner artifact");
      expect(cachedRunner(cache)).toBe(entry);
      if (file === "run.cjs") {
        expect(() => expect(existsSync(join(cache,"generated","run.mjs"))).toBe(true)).toThrow();
      }
    } finally { rmSync(cache,{recursive:true,force:true}); }
  }
});
