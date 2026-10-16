import { beforeAll, expect, test } from "bun:test";
import initialize from "../generated/wasm/acyclic_harness_wasm.js";
import { HarnessClient, TaskVolume, WasmStream, initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

for (const resizable of [false, true]) {
test(`waiting quotas bound the backing buffer, including growth: ${resizable}`, async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const client = await HarnessClient.open(outbox, target, { commands: 1n, bytes: 3n, durability: "Accepted" });
  const buffer = resizable ? Reflect.construct(ArrayBuffer, [3, { maxByteLength: 16384 }]) as ArrayBuffer : new ArrayBuffer(16384);
  const bytes = new Uint8Array(buffer, 0, 3);
  const key = { Task: { task: "task", file: { Request: "one.json" } } } as const;
  await expect(client.enqueue(key, bytes)).rejects.toEqual({ kind: "ByteQuota", detail: { limit: 3n } });
  expect(client.pending()).toBe(0);
  client.free(); outbox.free(); target.free(); stream.free();
});
}

for (const quota of ["CommandQuota", "ByteQuota"] as const) {
test(`waiting ${quota} is reserved before futures retain caller buffers`, async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const payload = new TextEncoder().encode(JSON.stringify({ operationId: "one", authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} }));
  const limits = { commands: quota === "CommandQuota" ? 1n : 4n, bytes: BigInt(payload.length) * (quota === "ByteQuota" ? 1n : 4n), durability: "Accepted" } as const;
  const client = await HarnessClient.open(outbox, target, limits);
  const key = { Task: { task: "task", file: { Request: "one.json" } } } as const;
  // These calls happen in the same JS turn, before the worker can consume either.
  const first = client.enqueue(key, payload);
  const second = client.enqueue(key, payload);
  await expect(second).rejects.toMatchObject({ kind: quota });
  await first;
  // Successful completion releases the waiting reservation, even with a full outbox.
  await client.enqueue(key, payload);
  expect(client.pending()).toBe(1);
  expect(await client.flush()).toBe(1);
  client.free(); outbox.free(); target.free(); stream.free();
});
}

function bounded(before: number, after: number) {
  if (after !== before) throw new Error("oversize admission grew WASM memory");
}

test("oversize enqueue checks its JS buffer length without growing WASM memory", async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const client = await HarnessClient.open(outbox, target, { commands: 1n, bytes: 3n, durability: "Accepted" });
  const { memory } = await initialize();
  const payload = new Uint8Array(Math.max(16 * 1024 * 1024, memory.buffer.byteLength + 65536));
  const key = { Task: { task: "task", file: { Request: "large.json" } } } as const;
  const before = memory.buffer.byteLength;
  for (let retry = 0; retry < 8; retry++) {
    await expect(client.enqueue(key, payload)).rejects.toEqual({ kind: "ByteQuota", detail: { limit: 3n } });
    bounded(before, memory.buffer.byteLength);
  }
  expect(client.pending()).toBe(0);
  // The former Vec<u8> boundary still exists on an unbounded volume write.
  // The same checker must detect its eager copy, independently of its result.
  const controlBefore = memory.buffer.byteLength;
  await target.write(await target.head(), "Goal", payload, null).catch(() => {});
  expect(() => bounded(controlBefore, memory.buffer.byteLength)).toThrow("oversize admission grew WASM memory");
  client.free(); outbox.free(); target.free(); stream.free();
});
