import { describe, expect, test } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import { AppendMutationSchema, AppendRequestSchema, AppendResponseSchema, ChildrenRequestSchema, CommitConditionSchema, CommitMutationSchema, CommitRequestSchema, CommitResponseSchema, CommittedEnvelopeSchema, DeleteRequestSchema, FollowRequestSchema, ForkRequestSchema, InspectIdempotencyRequestSchema, ReadCommitRequestSchema, ReadRequestSchema, StreamLimit, TailConditionSchema, TailRequestSchema, TrimRequestSchema } from "../generated/proto/stream/v2/stream_pb.js";
import { is_stream_error_code, WasmMemoryStream, decodeHttpResponse, encodeHttpRequest, normalizeCommitRequest, projectMemoryResponse, validateAppendRequest } from "../generated/wasm/acyclic_stream_wasm.js";
import { ensureStreamWasm } from "../src/contract.js";
import { HttpStreamProvider, MemoryStreamProvider, StreamClient, StreamError, idempotencyKey, jsonCodec, sequence, type Record as StreamRecord } from "../src/index.js";

const key = (value: string) => idempotencyKey(new TextEncoder().encode(value));
const encodedCommitId = btoa(String.fromCharCode(...new Uint8Array(32).fill(7)));

type RunEvent =
  | { readonly type: "run.started" }
  | { readonly type: "run.continued" }
  | { readonly type: "strategy.changed" };

const answer = (value: import("../src/index.js").JsonValue): { readonly answer: number } => {
  if (typeof value !== "object" || value === null || Array.isArray(value) || typeof value.answer !== "number") {
    throw new TypeError("expected an answer record");
  }
  return { answer: value.answer };
};
const runEvent = (value: import("../src/index.js").JsonValue): RunEvent => {
  if (typeof value !== "object" || value === null || Array.isArray(value) || typeof value.type !== "string") {
    throw new TypeError("expected a run event");
  }
  if (value.type === "run.started" || value.type === "run.continued" || value.type === "strategy.changed") return { type: value.type };
  throw new TypeError("unknown run event");
};
const job = (value: import("../src/index.js").JsonValue): { readonly job: string } => {
  if (typeof value !== "object" || value === null || Array.isArray(value) || typeof value.job !== "string") throw new TypeError("expected a job");
  return { job: value.job };
};
const worker = (value: import("../src/index.js").JsonValue): { readonly worker: string } => {
  if (typeof value !== "object" || value === null || Array.isArray(value) || typeof value.worker !== "string") throw new TypeError("expected a worker");
  return { worker: value.worker };
};

describe("website Stream contract", () => {
  test("opens the local provider without exposing provider setup", async () => {
    const stream = StreamClient.memory().json("runs/memory", answer);
    await stream.append({ answer: 42 });
    const records = [];
    for await (const record of stream.read({ from: 0n, limit: 1 })) records.push(record);
    expect(records).toHaveLength(1);
    expect(records[0]?.value).toEqual({ answer: 42 });
  });

  test("local streams do not issue unused hosted access tokens", async () => {
    expect(() => StreamClient.memory().tokens.create({
      expiresIn: "1h", allow: [{ path: "runs", operations: ["read"] }],
    })).toThrow("provider does not support token creation");
  });

  test("JSON streams reject values that cannot satisfy their declared recursive type", () => {
    const client = new StreamClient(new MemoryStreamProvider());
    // @ts-expect-error Date is not a JSON value and must not be promised by the codec.
    // @ts-expect-error A typed JSON stream must provide a parser; a caller-selected generic is not an overload.
    client.json<{ answer: number }>("dates");
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

    const typed = jsonCodec(answer);
    expect(typed.decode(typed.encode({ answer: 42 }))).toEqual({ answer: 42 });
    expect(() => typed.decode(new TextEncoder().encode(JSON.stringify({ answer: "wrong" })))).toThrow("expected an answer record");
  });

  test("enforces the Rust Stream path, batch, and read bounds before mutation", async () => {
    const client = StreamClient.memory();
    for (const path of ["space in/path", "bad\\path", "emoji/🚀", "control/\u000b", "a".repeat(65_536), Array(1_025).fill("a").join("/")]) {
      expect(() => client.bytes(path)).toThrow();
    }
    const stream = client.bytes("bounded");
    await expect(stream.appendBatch([])).rejects.toThrow("records");
    await expect(stream.appendBatch([new Uint8Array(65_537)])).rejects.toThrow("record");
    await expect(stream.appendBatch(Array(1_025).fill(new Uint8Array()))).rejects.toThrow("records");
    await expect(stream.read({ from: 0n, limit: 1_025 }).next()).rejects.toThrow("limit");
    expect(() => client.children(undefined, 1_025)).toThrow("limit");
    const cursor = client.bytes("cursor-bounds");
    await cursor.append(new Uint8Array([1]));
    await expect(cursor.read({ from: 2n, limit: 1 }).next()).rejects.toMatchObject({ code: "out_of_range" });
    await expect(cursor.follow({ from: 2n })[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "out_of_range" });
    expect(await stream.tail().catch(() => "absent")).toBe("absent");
  });

  test("uses Rust's canonical unsigned sequence width", () => {
    const maximum = 0xffff_ffff_ffff_ffffn;
    expect(sequence(0n)).toBe(0n);
    expect(sequence(maximum)).toBe(maximum);
    expect(() => sequence(-1n)).toThrow(RangeError);
    expect(() => sequence(maximum + 1n)).toThrow(RangeError);
  });

  test("rejects oversized commands and invalid cursors before they can change state", async () => {
    const client = StreamClient.memory();
    const wide = client.bytes("a".repeat(65_535));
    await expect(wide.appendBatch(Array(16).fill(new Uint8Array(65_536)))).rejects.toThrow("command exceeds");
    await expect(wide.tail()).rejects.toMatchObject({ code: "stream_not_found" });
    const stream = client.bytes("cursor");
    await stream.append(new Uint8Array([1]));
    await expect(stream.read({ from: 2n, limit: 1 }).next()).rejects.toMatchObject({ code: "out_of_range" });
    await expect(stream.follow({ from: 2n }).next()).rejects.toMatchObject({ code: "out_of_range" });
    const body = new Uint8Array(65_536);
    const conditions = Array.from({ length: 17 }, (_, index) => ({ path: `large/${index}`, ifAbsent: true as const }));
    const mutations = conditions.map(condition => ({ append: { stream: client.bytes(condition.path), values: [body] } }));
    await expect(client.commit({ conditions, mutations }, { idempotencyKey: key("large") })).rejects.toThrow("command exceeds");
    await expect(client.bytes("large/0").tail()).rejects.toMatchObject({ code: "stream_not_found" });

    let hostedCalls = 0;
    const hosted = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => {
      hostedCalls += 1;
      return new Response("null");
    } });
    await expect(hosted.append("a".repeat(65_535), Array(16).fill(new Uint8Array(65_536)))).rejects.toThrow("command exceeds");
    const hostedMutations = conditions.map(condition => ({ append: { path: condition.path, values: [body] } }));
    await expect(hosted.commit({ conditions, mutations: hostedMutations }, { idempotencyKey: key("hosted-large") })).rejects.toThrow("command exceeds");
    expect(hostedCalls).toBe(0);
  });

  test("allows a follow cursor exactly at the Rust tail and resumes after the next append", async () => {
    const stream = StreamClient.memory().bytes("cursor/tail");
    await stream.append(new Uint8Array([1]));
    const iterator = stream.follow({ from: 1n })[Symbol.asyncIterator]();
    const pending = iterator.next();
    await stream.append(new Uint8Array([2]));
    await expect(pending).resolves.toMatchObject({ done: false, value: { sequence: 1n, value: new Uint8Array([2]) } });
    await iterator.return?.();
  });

  test("Rust rejects oversized encoded requests before decoding or dispatch", async () => {
    await ensureStreamWasm();
    const oversized = new Uint8Array(StreamLimit.MAX_COMMAND_BYTES + 1);
    expect(validateAppendRequest(oversized)).toBe("limit_exceeded");
    expect(() => normalizeCommitRequest(oversized)).toThrow("limit_exceeded");
    await expect(new WasmMemoryStream().dispatch("tail", oversized)).rejects.toMatchObject({ code: "limit_exceeded" });
    expect(is_stream_error_code("limit_exceeded")).toBe(true);
    expect(is_stream_error_code("not_a_stream_error")).toBe(false);
  });

  test("requires matching conditions and canonicalizes coordinated retry identity", async () => {
    const client = StreamClient.memory();
    const first = client.bytes("coordinated/a");
    const second = client.bytes("coordinated/b");
    await expect(client.commit({ conditions: [{ path: first.path, ifAbsent: true }], mutations: [
      { append: { stream: second, values: [new Uint8Array([1])] } },
    ] }, { idempotencyKey: key("missing-condition") })).rejects.toThrow("matching");
    const options = { idempotencyKey: key("ordered") };
    const conditions = [{ path: first.path, ifAbsent: true as const }, { path: second.path, ifAbsent: true as const }];
    const mutations = [
      { append: { stream: first, values: [new Uint8Array([1])] } },
      { append: { stream: second, values: [new Uint8Array([2])] } },
    ];
    const accepted = await client.commit({ conditions, mutations }, options);
    const replay = await client.commit({ conditions: [...conditions].reverse(), mutations: [...mutations].reverse() }, options);
    expect(replay).toEqual(accepted);
    const withExtraProperties = await client.commit({
      conditions: conditions.map(condition => ({ ...condition, incidental: "ignored" })),
      mutations: mutations.map(mutation => ({ ...mutation, incidental: "ignored" })),
    }, options);
    expect(withExtraProperties).toEqual(accepted);
    await expect(client.provider.commit({ conditions: [{ path: "forged", ifAbsent: false } as never], mutations: [
      { append: { path: "forged", values: [new Uint8Array([1])] } },
    ] }, { idempotencyKey: key("forged") })).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(client.bytes("forged").tail()).rejects.toMatchObject({ code: "stream_not_found" });

    await expect(client.provider.commit({
      conditions: [{ path: "duplicate", ifAbsent: true }, { path: "duplicate", ifAbsent: true }],
      mutations: [{ append: { path: "duplicate", values: [new Uint8Array([1])] } }],
    }, { idempotencyKey: key("duplicate-condition") })).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(client.provider.commit({
      conditions: [{ path: "duplicate-mutation", ifAbsent: true }],
      mutations: [
        { append: { path: "duplicate-mutation", values: [new Uint8Array([1])] } },
        { append: { path: "duplicate-mutation", values: [new Uint8Array([2])] } },
      ],
    }, { idempotencyKey: key("duplicate-mutation") })).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(client.provider.commit({ conditions: null as never, mutations: [] }, { idempotencyKey: key("null-conditions") }))
      .rejects.toMatchObject({ code: "invalid_argument" });
    await expect(client.provider.commit({ conditions: [{ path: "malformed", ifAbsent: true }], mutations: [null as never] }, { idempotencyKey: key("null-mutation") }))
      .rejects.toMatchObject({ code: "invalid_argument" });
  });

  test("distinguishes a missing tail from zero and retires paths hierarchically", async () => {
    const client = StreamClient.memory();
    const missing = client.bytes("missing");
    const outcome = await client.commit({
      conditions: [{ stream: missing, ifTail: 0n }],
      mutations: [{ append: { stream: missing, values: [new Uint8Array([1])] } }],
    }, { idempotencyKey: key("missing-zero") });
    expect(outcome).toEqual({ ok: false, code: "conflict", conflicts: [{ path: "missing", expectedTail: 0n }] });
    await expect(missing.tail()).rejects.toMatchObject({ code: "stream_not_found" });

    const parent = client.bytes("retired-parent");
    const child = client.bytes("retired-parent/child");
    await parent.append(new Uint8Array([1]));
    await child.append(new Uint8Array([2]));
    await expect(parent.delete()).rejects.toMatchObject({ code: "invalid_argument" });
    await child.delete();
    await parent.delete();
    await expect(child.append(new Uint8Array([3]))).rejects.toMatchObject({ code: "stream_retired" });
    await expect(child.tail()).rejects.toMatchObject({ code: "stream_retired" });
    const retiredConflict = await client.commit({
      conditions: [{ path: "retired-parent/new", ifAbsent: true }],
      mutations: [{ append: { stream: client.bytes("retired-parent/new"), values: [new Uint8Array([4])] } }],
    }, { idempotencyKey: key("retired-child") });
    expect(retiredConflict).toEqual({ ok: false, code: "conflict", conflicts: [{ path: "retired-parent/new", expectedAbsent: true, actual: "retired" }] });

    await client.bytes("implicit/child").append(new Uint8Array([1]));
    expect(await client.bytes("implicit").tail()).toBe(0n);
  });

  test("appends, forks an exact prefix, and follows typed JSON", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const source = client.json("runs/run_42", runEvent);
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
    const jobs = client.json("runs/42/jobs", job);
    const workers = client.json("runs/42/workers", worker);
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

  test("projects every Rust memory response oneof with isolated exact values", async () => {
    const provider = new MemoryStreamProvider();
    const appendKey = key("projection-append");
    const append = await provider.append("projection/events", [new Uint8Array([1, 2])], { idempotencyKey: appendKey });
    expect(append).toMatchObject({ ok: true, start: 0n, end: 1n, tail: 1n });
    if (!append.ok) throw new Error("append unexpectedly conflicted");
    const appendId = append.commitId.slice();
    append.commitId[0] = 99;
    expect((await provider.readCommit(appendId)).commitId).toEqual(appendId);

    const appendConflict = await provider.append("projection/events", [new Uint8Array([3])], { ifTail: 0n });
    expect(appendConflict).toEqual({ ok: false, code: "tail_conflict", actualTail: 1n });
    const forkKey = key("projection-fork");
    const fork = await provider.fork("projection/events", "projection/copy", { idempotencyKey: forkKey });
    expect(fork).toMatchObject({ source: "projection/events", destination: "projection/copy", forkedAt: 1n, tail: 1n });
    const trimKey = key("projection-trim");
    const trim = await provider.trim("projection/events", 1n, trimKey);
    expect(trim).toMatchObject({ path: "projection/events", trimPoint: 1n });
    const deleteKey = key("projection-delete");
    const deletion = await provider.delete("projection/copy", deleteKey);
    expect(deletion).toMatchObject({ path: "projection/copy", commitId: expect.any(Uint8Array) });

    const commitKey = key("projection-commit");
    const committed = await provider.commit({
      conditions: [{ path: "projection/events", ifTail: 1n }],
      mutations: [{ append: { path: "projection/events", values: [new Uint8Array([4])] } }],
    }, { idempotencyKey: commitKey });
    expect(committed).toMatchObject({ ok: true, tails: { "projection/events": 2n }, forks: [] });
    const commitConflict = await provider.commit({
      conditions: [{ path: "projection/events", ifTail: 0n }],
      mutations: [{ append: { path: "projection/events", values: [new Uint8Array([5])] } }],
    }, { idempotencyKey: key("projection-conflict") });
    expect(commitConflict).toEqual({ ok: false, code: "conflict", conflicts: [{ path: "projection/events", expectedTail: 0n, actualTail: 2n }] });

    for (const [idempotencyKeyValue, type] of [
      [appendKey, "append"],
      [forkKey, "fork"],
      [trimKey, "trim"],
      [deleteKey, "delete"],
      [commitKey, "commit"],
    ] as const) {
      expect((await provider.inspectIdempotency(idempotencyKeyValue))?.outcome.type).toBe(type);
    }
    const firstRead = (await provider.read("projection/events", { from: 1n, limit: 1 }).next()).value;
    if (!firstRead) throw new Error("expected a retained record");
    firstRead.value[0] = 77;
    const secondRead = (await provider.read("projection/events", { from: 1n, limit: 1 }).next()).value;
    expect(secondRead?.value).toEqual(new Uint8Array([4]));
  });

  test("Rust memory response projector owns widths, copies, and missing oneofs", () => {
    const sourceCommitId = new Uint8Array(32).fill(9);
    const projected = projectMemoryResponse("append", toBinary(AppendResponseSchema, create(AppendResponseSchema, {
      outcome: { case: "committed", value: { start: 0xffff_ffff_ffff_ffffn, end: 0xffff_ffff_ffff_ffffn, tail: 0xffff_ffff_ffff_ffffn, commitId: sourceCommitId } },
    }))) as { readonly ok: true; readonly start: bigint; readonly commitId: Uint8Array };
    expect(projected).toMatchObject({ ok: true, start: 0xffff_ffff_ffff_ffffn });
    expect(projected.commitId).toEqual(sourceCommitId);
    expect(projected.commitId).not.toBe(sourceCommitId);
    sourceCommitId[0] = 1;
    expect(projected.commitId[0]).toBe(9);
    expect(() => projectMemoryResponse("append", toBinary(AppendResponseSchema, create(AppendResponseSchema)))).toThrow();
    expect(() => projectMemoryResponse("commit", toBinary(CommitResponseSchema, create(CommitResponseSchema)))).toThrow();
    expect(() => projectMemoryResponse("read_commit", toBinary(CommittedEnvelopeSchema, create(CommittedEnvelopeSchema, { commitId: sourceCommitId, mutations: [{}] })))).toThrow();
    expect(() => projectMemoryResponse("commit", toBinary(CommitResponseSchema, create(CommitResponseSchema, {
      outcome: { case: "committed", value: {
        commitId: sourceCommitId,
        mutations: [{ mutation: { case: "append", value: {
          path: "events", start: 0n, end: 1n, tail: 1n,
          records: [{ sequence: 0n, value: new Uint8Array([1]), commitId: new Uint8Array() }],
        } } }],
      } },
    })))).toThrow();
  });

  test("projects commit responses larger than the bounded request size", () => {
    const commitId = new Uint8Array(32).fill(9);
    const records = Array.from({ length: 1024 }, (_, sequence) => ({
      sequence: BigInt(sequence), value: new Uint8Array(1050).fill(sequence & 0xff), commitId,
    }));
    const bytes = toBinary(CommitResponseSchema, create(CommitResponseSchema, {
      outcome: { case: "committed", value: {
        commitId,
        mutations: [{ mutation: { case: "append", value: {
          path: "events", start: 0n, end: 1024n, tail: 1024n, records,
        } } }],
      } },
    }));
    expect(bytes.byteLength).toBeGreaterThan(StreamLimit.MAX_COMMAND_BYTES);
    expect(projectMemoryResponse("commit", bytes)).toMatchObject({ ok: true, tails: { events: 1024n }, forks: [] });
  });

  test("binds commit identities to request content", async () => {
    const left = await new MemoryStreamProvider().append("events", [new Uint8Array([1])]);
    const right = await new MemoryStreamProvider().append("events", [new Uint8Array([2])]);
    if (!left.ok || !right.ok) throw new Error("unconditional append unexpectedly conflicted");
    expect(left.commitId).not.toEqual(right.commitId);
  });

  test("enforces Rust commit deadlines at the WASM boundary", async () => {
    const provider = new MemoryStreamProvider();
    await expect(provider.commit({
      conditions: [{ path: "deadline", ifAbsent: true }],
      mutations: [{ append: { path: "deadline", values: [new Uint8Array([1])] } }],
    }, { idempotencyKey: key("expired-deadline"), deadlineUnixMillis: 0n })).rejects.toMatchObject({ code: "deadline_elapsed" });
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
    const observed = await provider.inspectIdempotency(shared);
    expect(observed?.requestDigest).toHaveLength(32);
    expect(observed?.outcome).toEqual({ type: "append", outcome: first });

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

  test("removes cancelled memory followers without waiting for a later append", async () => {
    const provider = new MemoryStreamProvider();
    await provider.append("events", [new Uint8Array([1])]);
    const controller = new AbortController();
    let removed = 0;
    let followerReady!: () => void;
    const followerStarted = new Promise<void>(resolve => { followerReady = resolve; });
    const add = controller.signal.addEventListener.bind(controller.signal);
    controller.signal.addEventListener = ((...args: Parameters<AbortSignal["addEventListener"]>) => { followerReady(); return add(...args); }) as AbortSignal["addEventListener"];
    const remove = controller.signal.removeEventListener.bind(controller.signal);
    controller.signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => { removed += 1; return remove(...args); }) as AbortSignal["removeEventListener"];
    const next = provider.follow("events", { from: 1n, signal: controller.signal })[Symbol.asyncIterator]().next();
    await followerStarted;
    controller.abort();
    await expect(next).resolves.toEqual({ done: true, value: undefined });
    expect(removed).toBe(1);
    await provider.append("events", [new Uint8Array([2])]);
    expect(removed).toBe(1);
  });

  test("closed Rust follow handles end before or during a pending read", async () => {
    await ensureStreamWasm();
    const wasm = new WasmMemoryStream();
    await wasm.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: "events", records: [new Uint8Array([1])] })));
    const request = toBinary(FollowRequestSchema, create(FollowRequestSchema, { path: "events", from: 1n }));
    const closed = await wasm.open_follow(request);
    closed.close();
    expect(await closed.next()).toBeNull();
    const pending = await wasm.open_follow(request);
    const next = pending.next();
    pending.close();
    expect(await next).toBeNull();
  });

  test("rejects a failed multi-stream commit atomically", async () => {
    const client = new StreamClient(new MemoryStreamProvider());
    const source = client.bytes("source");
    await source.append(new Uint8Array([1]));
    await expect(client.commit({ conditions: [{ stream: source, ifTail: 1n }, { path: "invalid", ifAbsent: true }], mutations: [
      { append: { stream: source, values: [new Uint8Array([2])] } },
      { fork: { source, destination: "invalid", atTail: 99n } },
    ] }, { idempotencyKey: key("atomic") })).rejects.toMatchObject({ code: "invalid_argument" });
    expect(await source.tail()).toBe(1n);
    expect(await client.inspectIdempotency(key("atomic"))).toBeUndefined();
  });

  test("retains a valid prototype-named tail in memory and hosted commit results", async () => {
    const request = {
      conditions: [{ path: "__proto__", ifAbsent: true as const }],
      mutations: [{ append: { path: "__proto__", values: [new Uint8Array([1])] } }],
    };
    const local = await new MemoryStreamProvider().commit(request, { idempotencyKey: key("prototype-local") });
    if (!local.ok) throw new Error("local commit unexpectedly conflicted");
    expect(Object.hasOwn(local.tails, "__proto__")).toBe(true);
    expect(local.tails["__proto__"]).toBe(1n);

    const remote = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () =>
      new Response(`{"ok":true,"commitId":"${encodedCommitId}","tails":{"__proto__":"1"},"forks":[]}`) });
    const hosted = await remote.commit(request, { idempotencyKey: key("prototype-hosted") });
    if (!hosted.ok) throw new Error("hosted commit unexpectedly conflicted");
    expect(Object.hasOwn(hosted.tails, "__proto__")).toBe(true);
    expect(hosted.tails["__proto__"]).toBe(1n);
  });

  test("hosted append sends the bytes and key validated at call time", async () => {
    let sent: { values: string[]; options: { idempotencyKey: string } } | undefined;
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_url, init) => {
      sent = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({ ok: true, start: "0", end: "1", tail: "1", commitId: encodedCommitId }));
    } });
    const value = new Uint8Array([1]);
    const retryKey = key("snap");
    const pending = provider.append("events", [value], { idempotencyKey: retryKey });
    value[0] = 9;
    retryKey[0] = 9;
    await pending;
    expect(sent?.values).toEqual(["AQ=="]);
    expect(sent?.options.idempotencyKey).toBe(btoa("snap"));
  });

  test("Rust encodes every protobuf-backed hosted request shape", async () => {
    await ensureStreamWasm();
    const retry = new Uint8Array([0, 255, 128, 1]);
    const retryWire = btoa(String.fromCharCode(...retry));
    const encoded = (route: Parameters<typeof encodeHttpRequest>[0], input: Uint8Array) => JSON.parse(encodeHttpRequest(route, input));
    expect(encoded("idempotency/inspect", toBinary(InspectIdempotencyRequestSchema, create(InspectIdempotencyRequestSchema, { idempotencyKey: retry })))).toEqual({ idempotencyKey: retryWire });
    expect(encoded("tail", toBinary(TailRequestSchema, create(TailRequestSchema, { path: "events" })))).toEqual({ path: "events" });
    expect(encoded("bounds", toBinary(TailRequestSchema, create(TailRequestSchema, { path: "events" })))).toEqual({ path: "events" });
    expect(encoded("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: "events", records: [new Uint8Array([1, 2])], ifTail: 0xffff_ffff_ffff_ffffn, idempotencyKey: retry })))).toEqual({ path: "events", values: ["AQI="], options: { ifTail: "18446744073709551615", idempotencyKey: retryWire } });
    expect(encoded("fork", toBinary(ForkRequestSchema, create(ForkRequestSchema, { source: "events", destination: "copy", atTail: 3n, idempotencyKey: retry })))).toEqual({ source: "events", destination: "copy", options: { atTail: "3", idempotencyKey: retryWire } });
    expect(encoded("trim", toBinary(TrimRequestSchema, create(TrimRequestSchema, { path: "events", before: 4n, idempotencyKey: retry })))).toEqual({ path: "events", before: "4", idempotencyKey: retryWire });
    expect(encoded("delete", toBinary(DeleteRequestSchema, create(DeleteRequestSchema, { path: "events", idempotencyKey: retry })))).toEqual({ path: "events", idempotencyKey: retryWire });
    expect(encoded("read", toBinary(ReadRequestSchema, create(ReadRequestSchema, { path: "events", from: 5n, limit: 6 })))).toEqual({ path: "events", from: "5", limit: 6 });
    expect(encoded("children", toBinary(ChildrenRequestSchema, create(ChildrenRequestSchema, { parent: "runs", limit: 7 })))).toEqual({ parent: "runs", limit: 7 });
    const commit = create(CommitRequestSchema, {
      conditions: [create(CommitConditionSchema, { condition: { case: "tail", value: create(TailConditionSchema, { path: "events", expected: 8n }) } })],
      mutations: [create(CommitMutationSchema, { mutation: { case: "append", value: create(AppendMutationSchema, { path: "events", records: [new Uint8Array([9])] }) } })],
      idempotencyKey: retry,
    });
    const commitBody = encoded("commit", normalizeCommitRequest(toBinary(CommitRequestSchema, commit)));
    expect(commitBody).toEqual({ request: { conditions: [{ path: "events", ifTail: "8" }], mutations: [{ append: { path: "events", values: ["CQ=="] } }] }, options: { idempotencyKey: retryWire } });
    expect(encoded("commits/read", toBinary(ReadCommitRequestSchema, create(ReadCommitRequestSchema, { commitId: new Uint8Array(32).fill(7) })))).toEqual({ commitId: encodedCommitId });
  });

  test("read captures one cursor before asynchronous validation", async () => {
    let localReads = 0;
    const local = new MemoryStreamProvider();
    await local.append("events", [new Uint8Array([1])]);
    const localOptions = { get from() { localReads += 1; return 0n; }, limit: 1 };
    for await (const _record of local.read("events", localOptions)) { /* consume */ }
    expect(localReads).toBe(1);

    let hostedReads = 0;
    const remote = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async input => new Response(new URL(String(input)).pathname.endsWith("/tail") ? '"0"' : "[]") });
    const hostedOptions = { get from() { hostedReads += 1; return 0n; }, limit: 1 };
    for await (const _record of remote.read("events", hostedOptions)) { /* consume */ }
    expect(hostedReads).toBe(1);
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
    let requestStarted!: () => void;
    const requested = new Promise<void>(resolve => { requestStarted = resolve; });
    const hanging = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => {
      observedSignal = init?.signal;
      requestStarted();
      return new Promise<Response>((_resolve, reject) => init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true }));
    } });
    const next = hanging.follow("events", { from: 0n, signal: controller.signal })[Symbol.asyncIterator]().next();
    await requested; controller.abort(new Error("stop"));
    await expect(next).rejects.toThrow("stop");
    expect(observedSignal).toBe(controller.signal);

    const polling = new AbortController(); let requests = 0; let added = 0; let removed = 0;
    const add = polling.signal.addEventListener.bind(polling.signal); const remove = polling.signal.removeEventListener.bind(polling.signal);
    polling.signal.addEventListener = ((...args: Parameters<AbortSignal["addEventListener"]>) => { added += 1; return add(...args); }) as AbortSignal["addEventListener"];
    polling.signal.removeEventListener = ((...args: Parameters<AbortSignal["removeEventListener"]>) => { removed += 1; return remove(...args); }) as AbortSignal["removeEventListener"];
    const idle = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async input => { const route = new URL(String(input)).pathname.split("/").pop(); if (route === "tail") return new Response('"0"'); if (++requests === 3) polling.abort(); return new Response("[]"); } });
    expect(await idle.follow("events", { from: 0n, signal: polling.signal })[Symbol.asyncIterator]().next()).toEqual({ done: true, value: undefined });
    expect(added).toBe(removed);
  });

  test("hosted non-success responses preserve canonical Stream error codes", async () => {
    for (const failure of [
      { wireCode: "not_found", code: "stream_not_found", status: 404 },
      { wireCode: "out_of_range", code: "cursor_trimmed", status: 409 },
      { wireCode: "access_denied", code: "access_denied", status: 403 },
      { wireCode: "stream_not_found", code: "stream_not_found", status: 404 },
    ]) {
      const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () =>
        new Response(JSON.stringify({ code: failure.wireCode, message: "service detail" }), { status: failure.status }) });
      await expect(provider.tail("events")).rejects.toMatchObject({ code: failure.code, message: "service detail", status: failure.status });
    }

    const unknown = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () =>
      new Response(JSON.stringify({ code: "internal_failure", message: "service detail" }), { status: 500 }) });
    await expect(unknown.tail("events")).rejects.toMatchObject({ code: "transport", message: JSON.stringify({ code: "internal_failure", message: "service detail" }), status: 500 });
  });

  test("hosted reads keep the canonical cursor contiguous", async () => {
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify([{
      sequence: "1", value: "AQ==", commitId: encodedCommitId,
    }])) });
    await expect(provider.read("events", { from: 0n, limit: 1 })[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "invalid_response" });
  });

  test("hosted reads reject an empty page beyond the canonical tail", async () => {
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async input => {
      const route = new URL(String(input)).pathname.split("/").pop();
      return route === "read" ? new Response("[]") : new Response('"1"');
    } });
    await expect(provider.read("events", { from: 2n, limit: 1 })[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "out_of_range" });
  });

  test("hosted bounds use the Rust response contract and preserve uint64 values", async () => {
    const maximum = 0xffff_ffff_ffff_ffffn;
    let requestBody: unknown;
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => {
      requestBody = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({ trimPoint: maximum.toString(), tail: maximum.toString() }));
    } });
    await expect(provider.bounds("events")).resolves.toEqual({ trimPoint: maximum, tail: maximum });
    expect(requestBody).toEqual({ path: "events" });

    const invalid = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ trimPoint: "2", tail: "1" })) });
    await expect(invalid.bounds("events")).rejects.toMatchObject({ code: "invalid_response" });
  });

  test("HTTP provider rejects invalid paths and commit shapes before fetching", async () => {
    let calls = 0;
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => { calls += 1; return new Response("null"); } });
    for (const operation of [
      () => provider.tail("bad path"),
      () => provider.bounds("bad path"),
      () => provider.tail(123 as never),
      () => provider.fork("source", "bad\\path"),
      () => provider.fork("same", "same"),
      () => provider.trim("bad path", 0n),
      () => provider.trim("valid", 0n, new Uint8Array() as never),
      () => provider.delete("bad path"),
      () => provider.delete("valid", new Uint8Array() as never),
      () => provider.append("bad path", [new Uint8Array([1])]),
      () => provider.commit({ conditions: [{ path: "valid", ifAbsent: true }], mutations: [{ append: { path: "unconditioned", values: [new Uint8Array([1])] } }] }, { idempotencyKey: key("invalid-commit") }),
      () => provider.commit({ conditions: [{ path: "forged", ifAbsent: false } as never], mutations: [{ append: { path: "forged", values: [new Uint8Array([1])] } }] }, { idempotencyKey: key("forged") }),
      () => provider.commit({ conditions: null as never, mutations: [] }, { idempotencyKey: key("null-conditions") }),
      () => provider.commit({ conditions: [{ path: "malformed", ifAbsent: true }], mutations: [null as never] }, { idempotencyKey: key("null-mutation") }),
    ]) await expect(Promise.resolve().then(operation)).rejects.toThrow();
    await expect(provider.read("bad path", { from: 0n, limit: 1 })[Symbol.asyncIterator]().next()).rejects.toThrow();
    await expect(provider.read("valid", { from: 0n, limit: 0 })[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "limit_exceeded" });
    await expect(provider.read("valid", { from: 0n, limit: 1.5 })[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(provider.follow("bad path", { from: 0n })[Symbol.asyncIterator]().next()).rejects.toThrow();
    await expect(provider.follow("valid", { from: -1n })[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(provider.children("bad path", 1)[Symbol.asyncIterator]().next()).rejects.toThrow();
    await expect(provider.childrenPage({ parent: "bad path", limit: 1 })).rejects.toThrow();
    await expect(provider.childrenPage({ limit: 0 })).rejects.toMatchObject({ code: "limit_exceeded" });
    await expect(provider.childrenPage(null as never)).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(provider.children(undefined, 0)[Symbol.asyncIterator]().next()).rejects.toMatchObject({ code: "limit_exceeded" });
    expect(calls).toBe(0);
  });

  test("HTTP token grants reject invalid paths before fetching", async () => {
    let calls = 0;
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => { calls += 1; return new Response("null"); } });
    await expect(Promise.resolve().then(() => provider.createToken!({ expiresIn: "1h", allow: [{ path: "bad path", operations: ["read"] }] }))).rejects.toMatchObject({ code: "invalid_path" });
    await expect(Promise.resolve().then(() => provider.createToken!({ expiresIn: "1h", allow: [{ path: "runs//child", operations: ["read"] }] }))).rejects.toMatchObject({ code: "invalid_path" });
    await expect(Promise.resolve().then(() => provider.createToken!({ expiresIn: "1h", allow: [{ path: "runs", operations: ["unknown" as never] }] }))).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(Promise.resolve().then(() => provider.createToken!({ expiresIn: "1h", allow: [] }))).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(Promise.resolve().then(() => provider.createToken!({ expiresIn: "1h", allow: null as never }))).rejects.toMatchObject({ code: "invalid_argument" });
    await expect(Promise.resolve().then(() => provider.createToken!({ expiresIn: "1h", allow: [null as never] }))).rejects.toMatchObject({ code: "invalid_argument" });
    expect(calls).toBe(0);
  });

  test("HTTP token creation serializes the Rust-validated request", async () => {
    let requestBody: unknown;
    let pathReads = 0;
    const grant = {
      get path() {
        pathReads += 1;
        return pathReads === 1 ? "runs" : "bad path";
      },
      operations: ["read" as const],
      toJSON() {
        return { path: "bad path", operations: ["delete"] };
      },
    };
    const provider = new HttpStreamProvider({ endpoint: "https://example.test", token: "x", fetcher: async (_input, init) => {
      requestBody = JSON.parse(String(init?.body));
      return new Response(JSON.stringify({ token: "secret", expiresAt: "2030-01-02T03:04:05.000Z" }));
    } });

    const token = await provider.createToken!({ expiresIn: "1h", allow: [grant] });

    expect(token.token).toBe("secret");
    expect(pathReads).toBe(1);
    expect(requestBody).toEqual({ expiresIn: "1h", allow: [{ path: "runs", operations: ["read"] }] });
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

  test("Rust owns HTTP scalar projection after validating the response schema", async () => {
    await ensureStreamWasm();
    const maximum = 0xffff_ffff_ffff_ffffn;
    const record = decodeHttpResponse("read", JSON.stringify([{
      sequence: maximum.toString(), value: "AQI=", commitId: encodedCommitId,
    }, {
      sequence: (maximum - 1n).toString(), value: "", commitId: encodedCommitId,
    }])) as { readonly sequence: bigint; readonly value: Uint8Array; readonly commitId: Uint8Array }[];
    expect(record[0]?.sequence).toBe(maximum);
    expect(record[0]?.value).toEqual(new Uint8Array([1, 2]));
    expect(record[0]?.commitId).toEqual(new Uint8Array(32).fill(7));
    expect(record[1]?.value).toEqual(new Uint8Array());

    const bounds = decodeHttpResponse("bounds", JSON.stringify({ trimPoint: maximum.toString(), tail: maximum.toString() })) as { readonly trimPoint: bigint; readonly tail: bigint };
    expect(bounds).toEqual({ trimPoint: maximum, tail: maximum });

    const token = decodeHttpResponse("tokens/create", JSON.stringify({ token: "secret", expiresAt: "2030-01-02T03:04:05.000Z" })) as { readonly token: string; readonly expiresAt: Date };
    expect(token.token).toBe("secret");
    expect(token.expiresAt).toBeInstanceOf(Date);
    expect(token.expiresAt.toISOString()).toBe("2030-01-02T03:04:05.000Z");
    expect(() => decodeHttpResponse("tokens/create", JSON.stringify({ token: "secret", expiresAt: "not-a-date" }))).toThrow();
  });
});
