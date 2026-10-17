import { beforeAll, expect, test } from "bun:test";
import fc from "fast-check";
import initialize from "../generated/wasm/acyclic_harness_wasm.js";
import { HarnessClient, ClientQuery, TaskVolume, WasmStream, initializeClientQueries } from "../src/client.js";

beforeAll(initializeClientQueries);

test("a registered idle wait leaves the only command slot available for flush", async () => {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const payload = new TextEncoder().encode(JSON.stringify({ operationId: "one", authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} }));
  const client = await HarnessClient.open(outbox, target, { commands: 1n, bytes: BigInt(payload.length), durability: "Accepted" });
  try {
    await client.enqueue({ Task: { task: "task", file: { Request: "one.json" } } }, payload);
    let settled = false;
    const idle = client.whenIdle().then(() => { settled = true; });
    // spawn_local polls the wait on the next microtask. This queued barrier lets
    // registration run before flush without a timer or polling for readiness.
    await new Promise<void>(resolve => queueMicrotask(resolve));
    expect(settled).toBe(false);
    expect(client.pending()).toBe(1);
    expect(await client.flush()).toBe(1);
    await idle;
    expect(settled).toBe(true);
    expect(client.pending()).toBe(0);
  } finally {
    client.free(); outbox.free(); target.free(); stream.free();
  }
});

test("enqueue snapshots exactly the intrinsic view bytes before yielding, across shrinking backing stores", async () => {
  const payload = new TextEncoder().encode(JSON.stringify({ operationId: "one", authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} }));
  const key = { Task: { task: "task", file: { Request: "one.json" } } } as const;
  await fc.assert(fc.asyncProperty(
    fc.integer({ min: payload.length + 1, max: 65536 }),
    async maximum => {
      for (const shared of [false, true]) for (const resizable of [false, true]) {
        const stream = new WasmStream();
        const outbox = await TaskVolume.create(stream, "client/files");
        const target = await TaskVolume.create(stream, "agent/files");
        const client = await HarnessClient.open(outbox, target, { commands: 1n, bytes: BigInt(payload.length), durability: "Accepted" });
        try {
          const Constructor = shared ? SharedArrayBuffer : ArrayBuffer;
          const offset = shared || resizable ? 1 : 0;
          const buffer = resizable ? Reflect.construct(Constructor, [payload.length + offset, { maxByteLength: maximum }])
            : Reflect.construct(Constructor, [shared ? maximum : payload.length]);
          const bytes = new Uint8Array(buffer, offset, payload.length);
          bytes.set(payload);
          let getterCalls = 0;
          for (const name of ["byteLength", "maxByteLength"]) Object.defineProperty(buffer, name, { get: () => { getterCalls++; return 0; } });
          Object.defineProperty(bytes, "buffer", { get: () => { getterCalls++; return new ArrayBuffer(0); } });
          // The review reproducer: a second shadowable length read asks for 16 MiB.
          Object.defineProperty(bytes, "length", { get: () => ++getterCalls === 1 ? payload.length : 16 * 1024 * 1024 });
          Object.defineProperty(bytes, "byteLength", { get: () => { getterCalls++; return 0; } });
          const enqueued = client.enqueue(key, bytes);
          // No JS storage may survive the synchronous snapshot, including mutable
          // contents, an offset view, or future growth of the backing store.
          bytes.fill(0);
          if (resizable) Reflect.apply(Reflect.get(buffer, shared ? "grow" : "resize"), buffer, [maximum]);
          await expect(client.enqueue(key, bytes)).rejects.toEqual({ kind: "ByteQuota", detail: { limit: BigInt(payload.length) } });
          await enqueued;
          expect(getterCalls).toBe(0);
          expect(client.pending()).toBe(1);
          expect(await client.flush()).toBe(1);
          const query = await ClientQuery.open(target, { File: key }, null, { files: 1n, segments: 1n, bytes: BigInt(payload.length) });
          const views = query.views();
          try { expect(views.segments(key)).toEqual([payload]); }
          finally { views.free(); query.free(); }
        } finally {
          client.close(); client.free(); outbox.free(); target.free(); stream.free();
        }
      }
    },
  ), { numRuns: 32, includeErrorInReport: true });
});

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

// A rendering-capable local provider must not imply network recovery policy.
async function explicitFlushOracle(priorities: number[], automatic: boolean) {
  const stream = new WasmStream();
  const outbox = await TaskVolume.create(stream, "client/files");
  const target = await TaskVolume.create(stream, "agent/files");
  const payloads = priorities.map((priority, index) => new TextEncoder().encode(JSON.stringify({ operationId: `request-${index}`, authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: { metadata: { priority } } })));
  const client = await HarnessClient.open(outbox, target, { commands: BigInt(priorities.length), bytes: BigInt(payloads.reduce((sum, bytes) => sum + bytes.length, 0)), durability: "Accepted" });
  try {
    const before = await target.head();
    for (const [index, payload] of payloads.entries()) {
      await client.enqueue({ Task: { task: "task", file: { Request: `${index}.json` } } }, payload);
    }
    // Mutate the policy by publishing without an explicit caller flush. The
    // same oracle must catch the resulting lost pending state and target write.
    if (automatic) await client.flush();
    const retained = client.pending() === priorities.length && (await target.head()).position === before.position;
    await client.flush();
    await client.whenIdle();
    if (!retained) throw new Error("local publication preceded explicit flush");
    expect(client.pending()).toBe(0);
    expect((await target.head()).position).toBe(before.position + BigInt(priorities.length));
  } finally {
    client.close(); client.free(); outbox.free(); target.free(); stream.free();
  }
}
const localRequests = fc.array(fc.integer({ min: 0, max: 65535 }), { minLength: 1, maxLength: 4 });
test("local rendering sessions preserve explicit publication across shrinking request histories", async () => {
  await fc.assert(fc.asyncProperty(localRequests, priorities => explicitFlushOracle(priorities, false)), { numRuns: 16, includeErrorInReport: true });
});
test("negative control: automatic local publication fails the same shrinking oracle", async () => {
  const result = await fc.check(fc.asyncProperty(localRequests, priorities => explicitFlushOracle(priorities, true)), { numRuns: 16 });
  expect(result.failed).toBe(true);
  expect(result.errorInstance).toBeInstanceOf(Error);
  expect(String(result.errorInstance)).toContain("local publication preceded explicit flush");
  expect(result.counterexample).toEqual([[0]]);
});
