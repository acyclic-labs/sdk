import { expect, test } from "bun:test";
import {
  DEFAULT_OBJECT_CACHE_OPTIONS as rawCache,
  DEFAULT_VOLUME_LIMITS as rawLimits,
} from "../generated/defaults.js";
import { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS } from "../src/contracts.js";
import { DEFAULT_MEMORY_FS_OPTIONS } from "../src/memory-options.js";

test("public Filesystem defaults follow the Rust-generated contract", () => {
  expect(DEFAULT_VOLUME_LIMITS).toBe(rawLimits);
  expect(DEFAULT_OBJECT_CACHE_OPTIONS).toEqual({
    ...rawCache,
    maximumBytes: Number(rawCache.maximumBytes),
  });
  expect(Number.isSafeInteger(DEFAULT_OBJECT_CACHE_OPTIONS.maximumBytes)).toBe(true);
  expect(DEFAULT_MEMORY_FS_OPTIONS.objectCache).toBe(DEFAULT_OBJECT_CACHE_OPTIONS);
  expect(DEFAULT_MEMORY_FS_OPTIONS.maximumObjectBytes).toBe(Number(rawLimits.maximumObjectBytes));
});
