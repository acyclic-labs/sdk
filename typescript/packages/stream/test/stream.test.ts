import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { HttpStreamProvider, MemoryStreamProvider, StreamClient, StreamError, bytesCodec, commitId, idempotencyKey, jsonCodec, type IdempotencyKey, type JsonValue, type Record as StreamRecord } from "../src/index.js";

const key = (value: string) => idempotencyKey(new TextEncoder().encode(value));
const encodedCommitId = btoa(String.fromCharCode(...new Uint8Array(32).fill(7)));

type RunEvent =
  | { readonly type: "run.started" }
  | { readonly type: "run.continued" }
  | { readonly type: "strategy.changed" };

function isJsonRecord(value: JsonValue): value is { readonly [name: string]: JsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function parseAnswer(value: JsonValue): { readonly answer: number } {
  if (!isJsonRecord(value) || typeof value.answer !== "number") throw new TypeError("expected answer");
  return { answer: value.answer };
}
function parseJob(value: JsonValue): { readonly job: string } {
  if (!isJsonRecord(value) || typeof value.job !== "string") throw new TypeError("expected job");
  return { job: value.job };
}
function parseWorker(value: JsonValue): { readonly worker: string } {
  if (!isJsonRecord(value) || typeof value.worker !== "string") throw new TypeError("expected worker");
  return { worker: value.worker };
}
function parseRunEvent(value: JsonValue): RunEvent {
  if (!isJsonRecord(value) || (value.type !== "run.started" && value.type !== "run.continued" && value.type !== "strategy.changed")) {
    throw new TypeError("expected run event");
  }
  return { type: value.type };
}

describe("website Stream contract", () => {
  test("rejects limits that cannot be represented on the protobuf wire", async () => {
    const provider = new MemoryStreamProvider();
    await expect(provider.childrenPage({ limit: 1.5 })).rejects.toMatchObject({ code: "invalid_argument" });
    const read = async () => { for await (const _record of provider.read("events", { from: 0n, limit: 0x1_0000_0000 })) {} };
    await expect(read()).rejects.toMatchObject({ code: "invalid_argument" });
  });
  test("memory clients do not mint synthetic access tokens", async () => {
    await expect(StreamClient.memory().tokens.create({ expiresIn: "1h", allow: [{ path: "events", operations: ["read"] }] })).rejects.toMatchObject({ code: "unsupported" });
  });
  test("copies native Buffer bytes at identity, codec, and memory boundaries", async () => {
    const input = Buffer.from([1, 2]);
    const identity = Buffer.alloc(32, 7);
    const copiedId = commitId(identity);
    const copiedKey = idempotencyKey(input);
    const encoded = bytesCodec.encode(input);
    const decoded = bytesCodec.decode(input);
    const provider = new MemoryStreamProvider();
    const pendingAppend = provider.append("events", [input]);
    input[0] = 9;
    await pendingAppend;
    identity[0] = 9;
    expect([copiedId[0], copiedKey[0], encoded[0], decoded[0]]).toEqual([7, 1, 1, 1]);
    const values: Uint8Array[] = [];
    for await (const record of provider.read("events", { from: 0n, limit: 1 })) values.push(record.value);
    expect(values[0]?.[0]).toBe(1);
  });
  test("snapshots queued mutation bodies and idempotency keys before yielding", async () => {
    const provider = new MemoryStreamProvider();
    const body = Buffer.from([3]);
    const rawKey = Buffer.from([4]) as unknown as IdempotencyKey;
    const values: Uint8Array[] = [body];
    const pendingAppend = provider.append("append", values, { idempotencyKey: rawKey });
    body[0] = 9;
    values[0] = Buffer.from([8]);
    rawKey[0] = 9;
    await pendingAppend;
    const appended: Uint8Array[] = [];
    for await (const record of provider.read("append", { from: 0n, limit: 1 })) appended.push(record.value);
    expect(appended[0]).toEqual(Uint8Array.of(3));
    expect((await provider.inspectIdempotency(idempotencyKey(Uint8Array.of(4))))?.idempotencyKey)
      .toEqual(Uint8Array.of(4));
    expect(await provider.inspectIdempotency(idempotencyKey(Uint8Array.of(9)))).toBeUndefined();

    const commitBody = Buffer.from([5]);
    const commitValues: Uint8Array[] = [commitBody];
    const pendingCommit = provider.commit({ conditions: [{ path: "commit", ifAbsent: true }], mutations: [{ append: { path: "commit", values: commitValues } }] },
      { idempotencyKey: idempotencyKey(Uint8Array.of(6)) });
    commitBody[0] = 9;
    commitValues[0] = Buffer.from([8]);
    await pendingCommit;
    const committed: Uint8Array[] = [];
    for await (const record of provider.read("commit", { from: 0n, limit: 1 })) committed.push(record.value);
    expect(committed[0]).toEqual(Uint8Array.of(5));
  });
  test("opens the local provider without exposing provider setup", async () => {
    const stream = StreamClient.memory().json("runs/memory", parseAnswer);
    await stream.append({ answer: 42 });
    const records = [];
    for await (const record of stream.read({ from: 0n, limit: 1 })) records.push(record);
    expect(records).toHaveLength(1);
    expect(records[0]?.value).toEqual({ answer: 42 });
  });

  test("JSON streams reject values that cannot satisfy their declared recursive type", () => {
    const client = new StreamClient(new MemoryStreamProvider());
    // @ts-expect-error Date is not a JSON value and must not be promised by the codec.
    client.json("dates", (): Date => new Date());
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

  test("typed JSON streams reject records that fail their parser", async () => {
    const client = StreamClient.memory();
    await client.bytes("runs/malformed").append(new TextEncoder().encode('{"answer":"wrong"}'));
    await expect(client.json("runs/malformed", parseAnswer).read({ from: 0n, limit: 1 }).next())
      .rejects.toThrow("expected answer");
  });

  test("appends, forks an exact prefix, and follows typed JSON", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const source = client.json("runs/run_42", parseRunEvent);
    const first = await source.append({ type: "run.started" });
    expect(first).toMatchObject({ ok: true, start: 0n, end: 1n, tail: 1n });
    const observed = await source.tail();
    const { stream: experiment, forkedAt, tail, commitId } = await source.fork(
      "runs/run_42-experiment",
      { atTail: observed, idempotencyKey: key("fork-run-42") },
    );
    expect({ forkedAt, tail, commitId }).toMatchObject({ forkedAt: 1n, tail: 1n, commitId: expect.any(Uint8Array) });
    await Promise.all([
      source.append({ type: "run.continued" }, { ifTail: observed }),
      experiment.append({ type: "strategy.changed" }, { ifTail: observed }),
    ]);
    const controller = new AbortController();
    const followed: StreamRecord<RunEvent>[] = [];
    for await (const record of experiment.follow({ from: observed, signal: controller.signal })) {
      followed.push(record); controller.abort();
    }
    expect(followed).toEqual([{ sequence: 1n, value: { type: "strategy.changed" }, commitId: expect.any(Uint8Array) }]);
  });

  test("uses exact uint64 positions, opaque identities, flattened receipts, and typed commit inputs", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const jobs = client.json("runs/42/jobs", parseJob);
    const workers = client.json("runs/42/workers", parseWorker);
    const jobKey = key("job-one");
    const jobsAppend = await jobs.append({ job: "one" }, { idempotencyKey: jobKey });
    expect(jobsAppend).toMatchObject({ ok: true, start: 0n, end: 1n, tail: 1n });
    expect(await jobs.append({ job: "one" }, { idempotencyKey: jobKey })).toEqual(jobsAppend);
    await expect(jobs.append({ job: "different" }, { idempotencyKey: jobKey })).rejects.toMatchObject({ code: "idempotency_mismatch" });
    await workers.append({ worker: "idle" });
    const result = await client.commit({
      conditions: [
        { stream: jobs, ifTail: 1n },
        { stream: workers, ifTail: 1n },
        { path: "runs/42/attempt-2", ifAbsent: true },
      ],
      mutations: [
        { append: { stream: jobs, values: [{ job: "two" }] } },
        { append: { stream: workers, values: [{ worker: "busy" }] } },
        { fork: { source: jobs, destination: "runs/42/attempt-2", atTail: 1n } },
      ],
    }, { idempotencyKey: key("assignment-1") });
    expect(result).toMatchObject({ ok: true, tails: { "runs/42/jobs": 2n, "runs/42/workers": 2n }, forks: [{ path: "runs/42/attempt-2", tail: 1n }] });
    if (!result.ok) throw new Error("commit unexpectedly conflicted");
    expect((await client.readCommit(result.commitId)).mutations).toHaveLength(3);
    await expect(jobs.read({ from: -1n, limit: 1 }).next()).rejects.toBeInstanceOf(RangeError);
    expect(() => client.json("../escape")).toThrow(StreamError);
  });

  test("forks a prefix and extends the new path in one commit", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const lineage = client.json<{ readonly generation: string }>("authorities/source/lineage");
    await lineage.append({ generation: "one" });
    await lineage.append({ generation: "two" });
    const result = await client.commit({
      conditions: [{ stream: lineage, ifTail: 2n }, { path: "authorities/child/lineage", ifAbsent: true }],
      mutations: [{ fork: { source: lineage, destination: "authorities/child/lineage", atTail: 1n, values: [{ generation: "child" }] } }],
    }, { idempotencyKey: key("fork-and-extend") });
    expect(result).toMatchObject({ ok: true, forks: [{ path: "authorities/child/lineage", tail: 2n }] });
    if (!result.ok) throw new Error("commit unexpectedly conflicted");
    const [fork] = (await client.readCommit(result.commitId)).mutations;
    expect(fork).toMatchObject({ type: "fork", forkedAt: 1n, tail: 2n, records: [{ sequence: 1n, commitId: result.commitId }] });
    const child = client.json<{ readonly generation: string }>("authorities/child/lineage");
    const values: { readonly generation: string }[] = [];
    for await (const record of child.read({ from: 0n, limit: 8 })) values.push(record.value);
    expect(values).toEqual([{ generation: "one" }, { generation: "child" }]);
  });

  test("binds commit identities to request content", async () => {
    const left = await new MemoryStreamProvider().append("events", [new Uint8Array([1])]);
    const right = await new MemoryStreamProvider().append("events", [new Uint8Array([2])]);
    if (!left.ok || !right.ok) throw new Error("unconditional append unexpectedly conflicted");
    expect(left.commitId).not.toEqual(right.commitId);
  });

  test("serializes concurrent mutations and replays a shared idempotency key once", async () => {
    const provider = new MemoryStreamProvider();
    const shared = key("concurrent-replay");
    const [first, replay] = await Promise.all([
      provider.append("events", [new Uint8Array([1])], { idempotencyKey: shared }),
      provider.append("events", [new Uint8Array([1])], { idempotencyKey: shared }),
    ]);
    expect(replay).toEqual(first);
    expect(await provider.tail("events")).toBe(1n);

    const [second, third] = await Promise.all([
      provider.append("events", [new Uint8Array([2])]),
      provider.append("events", [new Uint8Array([3])]),
    ]);
    if (!second.ok || !third.ok) throw new Error("unconditional append unexpectedly conflicted");
    expect([second.start, third.start]).toEqual([1n, 2n]);
    expect(await provider.tail("events")).toBe(3n);
  });

  test("returns CAS conflicts without mutating", async () => {
    const stream = new StreamClient(new MemoryStreamProvider()).bytes("events");
    await stream.append(new Uint8Array([1]));
    const conflict = await stream.append(new Uint8Array([2]), { ifTail: 0n });
    expect(conflict).toEqual({ ok: false, code: "tail_conflict", actualTail: 1n });
    expect(await stream.tail()).toBe(1n);
    expect(await stream.bounds()).toEqual({ trimPoint: 0n, tail: 1n });
    await stream.trim(1n);
    expect(await stream.bounds()).toEqual({ trimPoint: 1n, tail: 1n });
  });

  test("does not create a path when its first conditional append conflicts", async () => {
    const provider = new MemoryStreamProvider();
    const conflict = await provider.append("parent/phantom", [new Uint8Array([1])], { ifTail: 1n });
    expect(conflict).toEqual({ ok: false, code: "tail_conflict", actualTail: 0n });
    const children = [];
    for await (const child of provider.children("parent", 10)) children.push(child);
    expect(children).toEqual([]);
    await expect(provider.tail("parent/phantom")).rejects.toMatchObject({ code: "stream_not_found" });
  });

  test("pages more than 1024 nested agents and rejects stale continuations", async () => {
    const client = StreamClient.memory();
    for (let index = 0; index < 1_030; index += 1) {
      await client.bytes(`teams/red/agents/agent-${String(index).padStart(4, "0")}`).append(Uint8Array.of(index & 255));
    }
    expect((await client.childrenPage({ limit: 4 })).children).toEqual([{ path: "teams" }]);
    expect((await client.childrenPage({ parent: "teams/red", limit: 4 })).children).toEqual([{ path: "teams/red/agents" }]);
    const page = await client.childrenPage({ parent: "teams/red/agents", limit: 127 });
    expect(page.children).toHaveLength(127);
    expect(page.nextAfter).toBe("teams/red/agents/agent-0126");
    const all = [];
    for await (const child of client.childrenAll("teams/red/agents", 127)) all.push(child.path);
    expect(all).toHaveLength(1_030);
    expect(new Set(all).size).toBe(1_030);
    await client.bytes("teams/red/agents/agent-new").append(Uint8Array.of(1));
    await expect(client.childrenPage({ parent: "teams/red/agents", limit: 127,
      after: page.nextAfter!, hierarchyVersion: page.hierarchyVersion }))
      .rejects.toMatchObject({ code: "hierarchy_changed" });
  });

  test("removes cancelled memory followers without waiting for a later append", async () => {
    const provider = new MemoryStreamProvider();
    await provider.append("events", [new Uint8Array([1])]);
    const controller = new AbortController();
    let removed = 0;
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => { removed += 1; return remove(...args); }) as AbortSignal["removeEventListener"];
    const next = provider.follow("events", { from: 1n, signal: controller.signal })[Symbol.asyncIterator]().next();
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
    await expect(client.commit({ conditions: [
      { stream: source, ifTail: 1n }, { path: "invalid", ifAbsent: true },
    ], mutations: [
      { append: { stream: source, values: [new Uint8Array([2])] } },
      { fork: { source, destination: "invalid", atTail: 99n } },
    ] }, { idempotencyKey: key("atomic") })).rejects.toMatchObject({ code: "prefix_not_retained" });
    expect(await source.tail()).toBe(1n);
    expect(await client.inspectIdempotency(key("atomic"))).toBeUndefined();
  });

  test("managed transport validates responses and propagates follow cancellation", async () => {
    const malformed = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ ok: true })) });
    await expect(malformed.append("events", [new Uint8Array([1])])).rejects.toMatchObject({ code: "invalid_response" });
    const impossible = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ ok: true, start: "3", end: "2", tail: "1", commitId: encodedCommitId })) });
    await expect(impossible.append("events", [new Uint8Array([1])])).rejects.toMatchObject({ code: "invalid_response" });
    const invalidPath = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ source: "bad//path", destination: "ok", forkedAt: "0", tail: "0", commitId: encodedCommitId })) });
    await expect(invalidPath.fork("events", "copy")).rejects.toMatchObject({ code: "invalid_response" });
    const invalidUtf8 = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(new Uint8Array([255])) });
    await expect(invalidUtf8.tail("events")).rejects.toMatchObject({ code: "invalid_response" });
    const controller = new AbortController();
    let observedSignal: AbortSignal | null | undefined;
    const hanging = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => {
      observedSignal = init?.signal;
      return new Promise<Response>((_resolve, reject) => init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true }));
    } });
    const next = hanging.follow("events", { from: 0n, signal: controller.signal })[Symbol.asyncIterator]().next();
    await Promise.resolve(); controller.abort(new Error("stop"));
    await expect(next).rejects.toThrow("stop");
    expect(observedSignal).toBe(controller.signal);

    const polling = new AbortController(); let requests = 0; let added = 0; let removed = 0;
    const add = polling.signal.addEventListener.bind(polling.signal); const remove = polling.signal.removeEventListener.bind(polling.signal);
    polling.signal.addEventListener = ((...args: Parameters<AbortSignal["addEventListener"]>) => { added += 1; return add(...args); }) as AbortSignal["addEventListener"];
    polling.signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => { removed += 1; return remove(...args); }) as AbortSignal["removeEventListener"];
    const idle = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => { if (++requests === 3) polling.abort(); return new Response("[]"); } });
    expect(await idle.follow("events", { from: 0n, signal: polling.signal })[Symbol.asyncIterator]().next()).toEqual({ done: true, value: undefined });
    expect(added).toBe(removed);
  });

  test("does not expose server error bodies in transport errors", async () => {
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response("reflected-secret", { status: 403 }) });
    await expect(provider.tail("private")).rejects.toMatchObject({ code: "transport", message: "HTTP 403", status: 403 });
  });

  test("HTTP child pages preserve opaque hierarchy versions and stale-page errors", async () => {
    let body: unknown;
    let stale = false;
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x",
      fetcher: async (_input, init) => {
        body = JSON.parse(String(init?.body));
        if (stale) return new Response(JSON.stringify({ code: "hierarchy_changed" }), { status: 409 });
        return new Response(JSON.stringify({ hierarchyVersion: encodedCommitId,
          children: [{ path: "agents/a" }], nextAfter: "agents/a" }));
      } });
    const client = new StreamClient(provider);
    const page = await client.childrenPage({ parent: "agents", limit: 1 });
    expect(page.hierarchyVersion).toEqual(new Uint8Array(32).fill(7));
    expect(body).toEqual({ parent: "agents", limit: 1 });
    stale = true;
    await expect(client.childrenPage({ parent: "agents", limit: 1,
      after: page.nextAfter!, hierarchyVersion: page.hierarchyVersion }))
      .rejects.toMatchObject({ code: "hierarchy_changed" });
  });

  test("cancels oversized streaming transport responses at the configured bound", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel() { cancelled = true; throw new Error("cancel failed"); },
    });
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response(body) });
    await expect(provider.tail("events")).rejects.toMatchObject({ code: "response_too_large" });
    expect(cancelled).toBeTrue();
  });

  test("round-trips full uint64 positions and opaque identities through the HTTP wire codec", async () => {
    const maximum = 0xffff_ffff_ffff_ffffn;
    const identity = idempotencyKey(new Uint8Array([0, 255, 128, 1]));
    let requestBody: unknown;
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => {
      requestBody = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({ ok: true, start: maximum.toString(), end: maximum.toString(), tail: maximum.toString(), commitId: encodedCommitId }));
    } });
    const result = await provider.append("events", [new Uint8Array([1])], { ifTail: maximum, idempotencyKey: identity });
    expect(requestBody).toMatchObject({ options: { ifTail: maximum.toString(), idempotencyKey: btoa(String.fromCharCode(...identity)) } });
    expect(result).toMatchObject({ ok: true, start: maximum, end: maximum, tail: maximum });
    if (!result.ok) throw new Error("append unexpectedly conflicted");
    expect(result.commitId).toEqual(new Uint8Array(32).fill(7));
    if (false) {
      // @ts-expect-error sequences must preserve uint64 precision rather than accepting JS numbers.
      provider.read("events", { from: 1, limit: 1 });
      // @ts-expect-error opaque identities must be byte values rather than text conventions.
      await provider.readCommit("commit");
    }
  });
});
