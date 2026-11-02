import { describe, expect, test } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import { FollowRequestSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { WasmOpened, WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";
import { ensureStreamWasm, wireAppendRequest } from "../src/contract.js";
import { MemoryStreamProvider } from "../src/memory.js";
import { idempotencyKey } from "../src/types.js";
import { openWasmCursor } from "../src/wasm-records.js";

const key = (value: string) => idempotencyKey(new TextEncoder().encode(value));

describe("selected cursor ownership", () => {
  test("record and cleanup failures with equal primitive values remain distinct", async () => {
    const provider = new MemoryStreamProvider();
    await provider.append("close/causes", [new Uint8Array([1])], { idempotencyKey: key("close/causes") });
    const source = provider.follow("close/causes", { from: 0n });
    await source.opened;
    const close = source.close.bind(source);
    source.close = async () => { await close(); throw undefined; };
    const facade = source.map(() => { throw undefined; });
    const failed = await facade.next().catch((error: unknown) => error);
    expect(failed).toBeInstanceOf(AggregateError);
    expect((failed as AggregateError).errors).toEqual([undefined, undefined]);
    await expect(facade[Symbol.asyncDispose]()).rejects.toBeUndefined();
    expect(await source.next()).toEqual({ done: true, value: undefined });
  });

  test("close preserves an undefined original cursor cleanup failure", async () => {
    const provider = new MemoryStreamProvider();
    await provider.append("close/undefined", [new Uint8Array([1])], { idempotencyKey: key("close/undefined") });
    const source = provider.follow("close/undefined", { from: 1n });
    await source.opened;
    const close = source.close.bind(source);
    source.close = async () => { await close(); throw undefined; };
    const facade = source.map(record => record);
    await facade.opened;
    await expect(facade.close()).rejects.toBeUndefined();
    await expect(facade[Symbol.asyncDispose]()).rejects.toBeUndefined();
    expect(await source.next()).toEqual({ done: true, value: undefined });
  });

  test("early facade close retains failure disposing its original WASM cursor", async () => {
    const provider = new MemoryStreamProvider();
    await provider.append("close/cleanup", [new Uint8Array([1])], { idempotencyKey: key("close/cleanup") });
    const source = provider.follow("close/cleanup", { from: 1n });
    await source.opened;
    const cleanup = new Error("original cursor cleanup failed");
    const close = source.close.bind(source);
    source.close = async () => { await close(); throw cleanup; };
    const facade = source.map(record => record);
    const closing = facade.close();
    const failed = await closing.catch((error: unknown) => error);
    expect(failed).toBeInstanceOf(AggregateError);
    expect((failed as AggregateError).errors[1]).toBe(cleanup);
    await expect(facade[Symbol.asyncDispose]()).rejects.toBe(failed);
    expect(await source.next()).toEqual({ done: true, value: undefined });
  });

  test("closed Rust follow handles end before or during a pending read", async () => {
    await ensureStreamWasm();
    const wasm = new WasmStream();
    await wasm.dispatch("append", wireAppendRequest("events", [new Uint8Array([1])], { idempotencyKey: key("closed-follow-seed") }));
    const request = toBinary(FollowRequestSchema, create(FollowRequestSchema, { path: "events", from: 1n }));
    const closed = await openWasmCursor(async () => wasm.open_follow(request), new AbortController().signal);
    await closed.close();
    expect(await closed.next()).toBeNull();
    const pending = await openWasmCursor(async () => wasm.open_follow(request), new AbortController().signal);
    const next = pending.next();
    await pending.close();
    expect(await next).toBeNull();
  });

  test("Rust cursor ownership refusal preserves the original pending follow", async () => {
    await ensureStreamWasm();
    const wasm = new WasmStream();
    await wasm.dispatch("append", wireAppendRequest("owner", [new Uint8Array([1])], { idempotencyKey: key("pending-owner-seed") }));
    const request = wasm.open_follow(toBinary(FollowRequestSchema, create(FollowRequestSchema, { path: "owner", from: 1n })));
    let cursor: WasmOpened | undefined;
    let pulling: Promise<Uint8Array | null> | undefined;
    try {
      const opened = await request.next();
      if (!(opened instanceof WasmOpened)) throw new Error("original follow omitted its selected cursor");
      cursor = opened;
      pulling = cursor.next();
      await expect(cursor.next()).rejects.toMatchObject({ code: "unavailable" });
      await wasm.dispatch("append", wireAppendRequest("owner", [new Uint8Array([2])], { idempotencyKey: key("pending-owner-delivery") }));
      expect(await pulling).toBeInstanceOf(Uint8Array);
      cursor.close();
      expect(await cursor.next()).toBeNull();
    } finally {
      cursor?.close();
      await pulling?.catch(() => {});
      cursor?.free();
      request.close(); request.free();
      wasm.free();
    }
  });
});
