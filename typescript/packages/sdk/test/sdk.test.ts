import { describe, expect, test } from "bun:test";
import { filesystem, harness, inference, machines, objects, recursiveSum, stream } from "../src/index.ts";

describe("in-memory SDK", () => {
  test("each product is exposed from its semantic owner", () => {
    expect(filesystem.DEFAULT_OBJECT_CACHE_OPTIONS.maximumEntries).toBeGreaterThan(0);
    expect(harness.Harness).toBeDefined();
    expect(inference.Inference).toBeDefined();
    expect(machines.Machines).toBeDefined();
    expect(objects.Objects).toBeDefined();
    expect(stream.Stream).toBeDefined();
  });

  test("retains the recursive workload compatibility export", async () => {
    expect(await recursiveSum([1, 2, 3, 4, 5])).toBe(15);
  });
});
