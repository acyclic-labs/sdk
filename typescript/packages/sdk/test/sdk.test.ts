import { describe, expect, test } from "bun:test";
import { MemoryFilesystem, MemoryStream, recursiveSum } from "../src/index.ts";

describe("in-memory SDK", () => {
  test("recursive workload joins", async () => {
    expect(await recursiveSum(Array.from({ length: 16 }, (_, index) => index + 1), 2)).toBe(136);
  });

  test("filesystem generations are immutable to callers", async () => {
    const filesystem = new MemoryFilesystem();
    const base = await filesystem.create();
    const generation = await filesystem.write(base.id, "answer", new Uint8Array([42]));
    const files = generation.files as Map<string, Uint8Array>;
    files.get("answer")![0] = 0;
    files.set("injected", new Uint8Array([1]));
    const reread = await filesystem.get(generation.id);
    expect(reread.files.get("answer")![0]).toBe(42);
    expect(reread.files.has("injected")).toBe(false);
  });

  test("stream records are immutable to callers", async () => {
    const stream = new MemoryStream();
    const appended = await stream.append("history", 0, new Uint8Array([42]));
    appended.payload[0] = 0;
    const read = await stream.read("history", 0);
    read[0]!.payload[0] = 1;
    expect((await stream.read("history", 0))[0]!.payload[0]).toBe(42);
  });
});
