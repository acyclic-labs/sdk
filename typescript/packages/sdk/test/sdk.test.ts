import { describe, expect, test } from "bun:test";
import { filesystem, recursiveSum } from "../src/index.ts";

describe("in-memory SDK", () => {
  test("recursive workload joins", async () => {
    expect(await recursiveSum(Array.from({ length: 16 }, (_, index) => index + 1), 2)).toBe(136);
  });

  test("filesystem is exposed as one family namespace", () => {
    expect(filesystem.DEFAULT_OBJECT_CACHE_OPTIONS.maximumEntries).toBeGreaterThan(0);
  });
});
