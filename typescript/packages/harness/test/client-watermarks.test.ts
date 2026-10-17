import { beforeAll, expect, test } from "bun:test";
import "fake-indexeddb/auto";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { TailRequestSchema, TailResponseSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import { TaskVolume, WasmStream, initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

function exactWatermarks(value: { tail: bigint; durable?: bigint | undefined }, tail: bigint, durable: bigint) {
  return value.tail === tail && value.durable === durable;
}

for (const persistent of [false, true]) {
  test(`platform tail preserves confirmed durability through WASM: ${persistent}`, async () => {
    const stream = persistent
      ? await WasmStream.openBrowser(`watermarks-${crypto.randomUUID()}`, 64, 65536n)
      : new WasmStream();
    const files = await TaskVolume.create(stream, "client/files");
    await files.write(await files.head(), "Goal", new TextEncoder().encode("one"), null);
    const tail = (await files.head()).position;
    const durable = persistent ? tail : 0n;
    const input = (accepted: boolean) => toBinary(TailRequestSchema, create(TailRequestSchema, { path: "client/files", accepted }));
    const observed = fromBinary(TailResponseSchema, await stream.dispatch("tail", input(true)));
    expect(exactWatermarks(observed, tail, durable)).toBe(true);
    expect(exactWatermarks({ tail }, tail, durable)).toBe(false);
    if (persistent) expect(exactWatermarks({ tail, durable: 0n }, tail, durable)).toBe(false);
    const ordinary = fromBinary(TailResponseSchema, await stream.dispatch("tail", input(false)));
    expect(ordinary.tail).toBe(tail);
    expect(ordinary.durable).toBeUndefined();
    files.free(); stream.free();
  });
}
