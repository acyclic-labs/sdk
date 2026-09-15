import { describe, expect, test } from "bun:test";
import { HttpStreamProvider, MemoryStreamProvider, StreamClient, StreamError, jsonCodec, type Record as StreamRecord } from "../src/index.js";

type RunEvent =
  | { readonly type: "run.started" }
  | { readonly type: "run.continued" }
  | { readonly type: "strategy.changed" };

describe("website Stream contract", () => {
  test("JSON streams reject values that cannot satisfy their declared recursive type", () => {
    const client = new StreamClient(new MemoryStreamProvider());
    // @ts-expect-error Date is not a JSON value and must not be promised by the codec.
    client.json<Date>("dates");
    const codec = jsonCodec();
    expect(() => codec.encode(Number.POSITIVE_INFINITY)).toThrow("finite");
    expect(() => codec.encode(new Date() as never)).toThrow("plain records");
    const cyclic: { self?: unknown } = {};
    cyclic.self = cyclic;
    expect(() => codec.encode(cyclic as never)).toThrow("cycles");
    const substituted = Object.defineProperty({ answer: 42 }, "toJSON", { value: () => "substituted" });
    expect(() => codec.encode(substituted as never)).toThrow("must not define toJSON");
    let reads = 0;
    const accessor = Object.defineProperty({ answer: 42 }, "toJSON", { get: () => reads++ === 0 ? undefined : () => "substituted" });
    expect(() => codec.encode(accessor as never)).toThrow("must not define toJSON");
  });

  test("appends, forks an exact prefix, and follows typed JSON", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const source = client.json<RunEvent>("runs/run_42");
    const first = await source.append({ type: "run.started" });
    expect(first).toMatchObject({ ok: true, start: 0, end: 1, tail: 1 });
    const observed = await source.tail();
    const { stream: experiment, forkedAt, tail, commitId } = await source.fork(
      "runs/run_42-experiment",
      { atTail: observed, idempotencyKey: "fork-run-42" },
    );
    expect({ forkedAt, tail, commitId }).toMatchObject({ forkedAt: 1, tail: 1 });
    await Promise.all([
      source.append({ type: "run.continued" }, { ifTail: observed }),
      experiment.append({ type: "strategy.changed" }, { ifTail: observed }),
    ]);
    const controller = new AbortController();
    const followed: StreamRecord<RunEvent>[] = [];
    for await (const record of experiment.follow({ from: observed, signal: controller.signal })) {
      followed.push(record); controller.abort();
    }
    expect(followed).toEqual([{ sequence: 1, value: { type: "strategy.changed" }, commitId: expect.any(String) }]);
  });

  test("uses safe integers, flattened receipts, replay identities, and typed commit inputs", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const jobs = client.json<{ readonly job: string }>("runs/42/jobs");
    const workers = client.json<{ readonly worker: string }>("runs/42/workers");
    const jobsAppend = await jobs.append({ job: "one" }, { idempotencyKey: "job-one" });
    expect(jobsAppend).toMatchObject({ ok: true, start: 0, end: 1, tail: 1 });
    expect(await jobs.append({ job: "one" }, { idempotencyKey: "job-one" })).toEqual(jobsAppend);
    await expect(jobs.append({ job: "different" }, { idempotencyKey: "job-one" })).rejects.toMatchObject({ code: "idempotency_mismatch" });
    await workers.append({ worker: "idle" });
    const result = await client.commit({
      conditions: [
        { stream: jobs, ifTail: 1 },
        { stream: workers, ifTail: 1 },
        { path: "runs/42/attempt-2", ifAbsent: true },
      ],
      mutations: [
        { append: { stream: jobs, values: [{ job: "two" }] } },
        { append: { stream: workers, values: [{ worker: "busy" }] } },
        { fork: { source: jobs, destination: "runs/42/attempt-2", atTail: 1 } },
      ],
    }, { idempotencyKey: "assignment-1" });
    expect(result).toMatchObject({ ok: true, tails: { "runs/42/jobs": 2, "runs/42/workers": 2 }, forks: [{ path: "runs/42/attempt-2", tail: 1 }] });
    if (!result.ok) throw new Error("commit unexpectedly conflicted");
    expect((await client.readCommit(result.commitId)).mutations).toHaveLength(3);
    await expect(jobs.read({ from: -1, limit: 1 }).next()).rejects.toBeInstanceOf(RangeError);
    expect(() => client.json("../escape")).toThrow(StreamError);
  });

  test("returns CAS conflicts without mutating", async () => {
    const stream = new StreamClient(new MemoryStreamProvider()).bytes("events");
    await stream.append(new Uint8Array([1]));
    const conflict = await stream.append(new Uint8Array([2]), { ifTail: 0 });
    expect(conflict).toEqual({ ok: false, code: "tail_conflict", actualTail: 1 });
    expect(await stream.tail()).toBe(1);
  });

  test("does not create a path when its first conditional append conflicts", async () => {
    const provider = new MemoryStreamProvider();
    const conflict = await provider.append("parent/phantom", [new Uint8Array([1])], { ifTail: 1 });
    expect(conflict).toEqual({ ok: false, code: "tail_conflict", actualTail: 0 });
    const children = [];
    for await (const child of provider.children("parent", 10)) children.push(child);
    expect(children).toEqual([]);
    await expect(provider.tail("parent/phantom")).rejects.toMatchObject({ code: "stream_not_found" });
  });

  test("removes cancelled memory followers without waiting for a later append", async () => {
    const provider = new MemoryStreamProvider();
    await provider.append("events", [new Uint8Array([1])]);
    const controller = new AbortController();
    let removed = 0;
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => { removed += 1; return remove(...args); }) as AbortSignal["removeEventListener"];
    const next = provider.follow("events", { from: 1, signal: controller.signal })[Symbol.asyncIterator]().next();
    await Promise.resolve();
    controller.abort();
    await expect(next).resolves.toEqual({ done: true, value: undefined });
    expect(removed).toBe(1);
    await provider.append("events", [new Uint8Array([2])]);
    expect(removed).toBe(1);
  });

  test("rejects a failed multi-stream commit atomically", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const source = client.bytes("source");
    await source.append(new Uint8Array([1]));
    await expect(client.commit({ conditions: [], mutations: [
      { append: { stream: source, values: [new Uint8Array([2])] } },
      { fork: { source, destination: "invalid", atTail: 99 } },
    ] }, { idempotencyKey: "atomic" })).rejects.toMatchObject({ code: "prefix_not_retained" });
    expect(await source.tail()).toBe(1);
    expect(await client.inspectIdempotency("atomic")).toBeUndefined();
  });

  test("managed transport validates responses and propagates follow cancellation", async () => {
    const malformed = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ ok: true })) });
    await expect(malformed.append("events", [new Uint8Array([1])])).rejects.toMatchObject({ code: "invalid_response" });
    const impossible = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ ok: true, start: 3, end: 2, tail: 1, commitId: "commit" })) });
    await expect(impossible.append("events", [new Uint8Array([1])])).rejects.toMatchObject({ code: "invalid_response" });
    const invalidPath = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ source: "bad//path", destination: "ok", forkedAt: 0, tail: 0, commitId: "commit" })) });
    await expect(invalidPath.fork("events", "copy")).rejects.toMatchObject({ code: "invalid_response" });
    const invalidUtf8 = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(new Uint8Array([255])) });
    await expect(invalidUtf8.tail("events")).rejects.toMatchObject({ code: "invalid_response" });
    const controller = new AbortController();
    let observedSignal: AbortSignal | null | undefined;
    const hanging = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => {
      observedSignal = init?.signal;
      return new Promise<Response>((_resolve, reject) => init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true }));
    } });
    const next = hanging.follow("events", { from: 0, signal: controller.signal })[Symbol.asyncIterator]().next();
    await Promise.resolve(); controller.abort(new Error("stop"));
    await expect(next).rejects.toThrow("stop");
    expect(observedSignal).toBe(controller.signal);

    const polling = new AbortController(); let requests = 0; let added = 0; let removed = 0;
    const add = polling.signal.addEventListener.bind(polling.signal); const remove = polling.signal.removeEventListener.bind(polling.signal);
    polling.signal.addEventListener = ((...args: Parameters<AbortSignal["addEventListener"]>) => { added += 1; return add(...args); }) as AbortSignal["addEventListener"];
    polling.signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => { removed += 1; return remove(...args); }) as AbortSignal["removeEventListener"];
    const idle = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => { if (++requests === 3) polling.abort(); return new Response("[]"); } });
    expect(await idle.follow("events", { from: 0, signal: polling.signal })[Symbol.asyncIterator]().next()).toEqual({ done: true, value: undefined });
    expect(added).toBe(removed);
  });

  test("cancels oversized streaming transport responses at the configured bound", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel() { cancelled = true; },
    });
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response(body) });
    await expect(provider.tail("events")).rejects.toMatchObject({ code: "response_too_large" });
    expect(cancelled).toBeTrue();
  });
});
