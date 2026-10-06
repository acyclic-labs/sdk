import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createNativeProcessOwner, NATIVE_PROCESS_OWNER_CAPABILITY, NATIVE_PROCESS_OWNER_VERSION } from "../src/native-process.js";

test("native process companion identity is a small validated public contract", () => {
  const binding = {
    capability: NATIVE_PROCESS_OWNER_CAPABILITY,
    version: NATIVE_PROCESS_OWNER_VERSION,
    spawn: () => undefined,
    terminate: async () => ({ kind: "unknown", pid: 1, reason: "fixture" }),
  };
  const owner = createNativeProcessOwner(binding);
  expect(Object.isFrozen(owner)).toBe(true);
  expect(owner.spawn).toBe(binding.spawn);
  expect(owner.terminate).toBe(binding.terminate);
  expect(() => createNativeProcessOwner({ ...binding, capability: "other" })).toThrow(/owned process capability/u);
  expect(() => createNativeProcessOwner({ ...binding, version: "0.0.0" })).toThrow(/version does not match/u);
  expect(() => createNativeProcessOwner({ ...binding, terminate: undefined })).toThrow(/incomplete/u);
});

test("filesystem package publishes the native process contract through declarations", () => {
  const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
    readonly files?: readonly string[];
    readonly exports?: Record<string, { readonly types?: string; readonly default?: string }>;
  };
  expect(manifest.files).toContain("dist");
  expect(manifest.exports?.["./native-process"]).toEqual({
    types: "./dist/native-process.d.ts",
    node: "./dist/native-process.js",
    default: "./dist/native-process.js",
  });
});
