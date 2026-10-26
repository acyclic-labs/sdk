import { beforeAll, expect, test } from "bun:test";
import "fake-indexeddb/auto";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema, AppendResponseSchema, TailRequestSchema, TailResponseSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import { WasmStream, initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

function exactWatermarks(value: { tail: bigint; durable?: bigint | undefined }, tail: bigint, durable: bigint) {
  return value.tail === tail && value.durable === durable;
}

for (const persistent of [false, true]) {
  test(`platform tail preserves confirmed durability through WASM: ${persistent}`, async () => {
    const stream = persistent
      ? await WasmStream.openBrowser(`watermarks-${crypto.randomUUID()}`, 64, 65536n)
      : new WasmStream();
    const path = "client/watermarks";
    const appended = fromBinary(AppendResponseSchema, await stream.dispatch("append",
      toBinary(AppendRequestSchema, create(AppendRequestSchema, {
        path, ifTail: 0n, records: [{ value: new TextEncoder().encode("one") }],
      }))));
    if (appended.outcome.case !== "committed") throw new Error("initial append did not commit");
    const tail = appended.outcome.value.tail;
    expect(tail).toBe(1n);
    const durable = persistent ? tail : 0n;
    const input = (accepted: boolean) => toBinary(TailRequestSchema, create(TailRequestSchema, { path, accepted }));
    const observed = fromBinary(TailResponseSchema, await stream.dispatch("tail", input(true)));
    expect(exactWatermarks(observed, tail, durable)).toBe(true);
    expect(exactWatermarks({ tail }, tail, durable)).toBe(false);
    if (persistent) expect(exactWatermarks({ tail, durable: 0n }, tail, durable)).toBe(false);
    const ordinary = fromBinary(TailResponseSchema, await stream.dispatch("tail", input(false)));
    expect(ordinary.tail).toBe(tail);
    expect(ordinary.durable).toBeUndefined();
    stream.free();
  });
}
