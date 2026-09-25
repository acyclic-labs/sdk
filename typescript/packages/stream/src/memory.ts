import { create, fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import {
  AppendRequestSchema, AppendResponseSchema, ChildrenPageRequestSchema, ChildrenPageResponseSchema,
  CommitRequestSchema, CommitResponseSchema, CommittedEnvelopeSchema, DeleteRequestSchema,
  DeleteReceiptSchema, ForkRequestSchema, ForkReceiptSchema, InspectIdempotencyRequestSchema,
  InspectIdempotencyResponseSchema, FollowRequestSchema, ReadCommitRequestSchema, ReadRequestSchema, ReadResponseSchema,
  TailRequestSchema, TailResponseSchema, TrimRequestSchema, TrimReceiptSchema,
} from "../generated/proto/stream/v2/stream_pb.js";
import type {
  AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommitConflict,
  CommittedEnvelope, CommittedMutation, CommitOptions, CommitResult,
  DeleteReceipt, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey,
  IdempotencyObservation, IdempotencyOutcome, ProviderCommitRequest, ReadOptions, Sequence,
  StreamBounds, StreamProvider, TrimReceipt,
} from "./types.js";
import { StreamError, commitId, idempotencyKey } from "./types.js";

type WasmModule = typeof import("../generated/wasm/acyclic_stream_wasm.js");
type RawMemoryStream = InstanceType<WasmModule["MemoryStreamBinding"]>;
type RawFollow = Awaited<ReturnType<RawMemoryStream["follow"]>>;
type UnaryMethod = { [Name in keyof RawMemoryStream]: RawMemoryStream[Name] extends (input: Uint8Array) => Promise<Uint8Array> ? Name : never }[keyof RawMemoryStream];

let modulePromise: Promise<WasmModule> | undefined;
async function loadModule(): Promise<WasmModule> {
  modulePromise ??= (async () => {
    const module = await import("../generated/wasm/acyclic_stream_wasm.js");
    const node = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node;
    if (node === undefined) await module.default();
    else {
      const fsPath: string = "node:fs/promises";
      const { readFile } = await import(fsPath) as { readFile(url: URL): Promise<Uint8Array> };
      await module.default(await readFile(new URL("../generated/wasm/acyclic_stream_wasm_bg.wasm", import.meta.url)));
    }
    return module;
  })().catch(error => { modulePromise = undefined; throw error; });
  return modulePromise;
}

function encode<Schema extends DescMessage>(schema: Schema, value: MessageShape<Schema>): Uint8Array {
  try { return toBinary(schema, value); }
  catch { throw new StreamError("invalid_argument", "request cannot be encoded as protobuf"); }
}
function decodeResponse<Schema extends DescMessage>(schema: Schema, value: Uint8Array): MessageShape<Schema> {
  try { return fromBinary(schema, value); }
  catch { throw new StreamError("invalid_response", "provider returned malformed protobuf bytes"); }
}
function asStreamError(error: unknown): StreamError {
  const source = error as { code?: unknown; message?: unknown };
  return new StreamError(typeof source?.code === "string" ? source.code : "unavailable", String(source?.message ?? error));
}
function uint32(value: number): number {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) throw new StreamError("invalid_argument", "limit is outside the uint32 wire range");
  return value;
}
function record(value: NonNullable<MessageShape<typeof ReadResponseSchema>["record"]>): EncodedRecord {
  return { sequence: value.sequence, value: Uint8Array.from(value.value), commitId: commitId(value.commitId) };
}
function envelope(value: MessageShape<typeof CommittedEnvelopeSchema>): CommittedEnvelope {
  const mutations: CommittedMutation[] = value.mutations.map(item => {
    switch (item.mutation.case) {
      case "append": { const data = item.mutation.value; return {
        type: "append", path: data.path, start: data.start, end: data.end, tail: data.tail,
        records: data.records.map(item => ({ sequence: item.sequence, value: Uint8Array.from(item.value), commitId: commitId(item.commitId) })),
      }; }
      case "fork": { const data = item.mutation.value; return { type: "fork", source: data.source, destination: data.destination, forkedAt: data.forkedAt, tail: data.tail }; }
      case "trim": { const data = item.mutation.value; return { type: "trim", path: data.path, trimPoint: data.trimPoint }; }
      case "delete": return { type: "delete", path: item.mutation.value.path };
      default: throw new StreamError("invalid_response", "commit contains a mutation without a kind");
    }
  });
  return { commitId: commitId(value.commitId), mutations };
}
function appendResult(value: MessageShape<typeof AppendResponseSchema>): AppendResult {
  if (value.outcome.case === "committed") {
    const item = value.outcome.value;
    return { ok: true, start: item.start, end: item.end, tail: item.tail, commitId: commitId(item.commitId) };
  }
  if (value.outcome.case === "conflict") return { ok: false, code: "tail_conflict", actualTail: value.outcome.value.actualTail };
  throw new StreamError("invalid_response", "append response has no outcome");
}
function forkReceipt(value: MessageShape<typeof ForkReceiptSchema>): ForkReceipt {
  return { source: value.source, destination: value.destination, forkedAt: value.forkedAt, tail: value.tail, commitId: commitId(value.commitId) };
}
function trimReceipt(value: MessageShape<typeof TrimReceiptSchema>): TrimReceipt {
  return { path: value.path, trimPoint: value.trimPoint, commitId: commitId(value.commitId) };
}
function deleteReceipt(value: MessageShape<typeof DeleteReceiptSchema>): DeleteReceipt {
  return { path: value.path, commitId: commitId(value.commitId) };
}
function commitResult(value: MessageShape<typeof CommitResponseSchema>): CommitResult {
  if (value.outcome.case === "conflict") {
    const conflicts: CommitConflict[] = value.outcome.value.conflicts.map(item => {
      switch (item.conflict.case) {
        case "tail": return { path: item.conflict.value.path, expectedTail: item.conflict.value.expected, actualTail: item.conflict.value.actual ?? 0n };
        case "exists": return { path: item.conflict.value.path, expectedAbsent: true, actual: "exists" };
        case "retired": return { path: item.conflict.value.path, expectedAbsent: true, actual: "retired" };
        default: throw new StreamError("invalid_response", "commit conflict has no kind");
      }
    });
    return { ok: false, code: "conflict", conflicts };
  }
  if (value.outcome.case !== "committed") throw new StreamError("invalid_response", "commit response has no outcome");
  const committed = envelope(value.outcome.value);
  const tails: { [path: string]: Sequence } = {};
  const forks: { path: string; tail: Sequence }[] = [];
  for (const item of committed.mutations) {
    if (item.type === "append") tails[item.path] = item.tail;
    if (item.type === "fork") forks.push({ path: item.destination, tail: item.tail });
  }
  return { ok: true, commitId: committed.commitId, tails, forks };
}
function observation(value: NonNullable<MessageShape<typeof InspectIdempotencyResponseSchema>["observation"]>): IdempotencyObservation {
  let outcome: IdempotencyOutcome;
  switch (value.outcome.case) {
    case "append": outcome = { type: "append", outcome: appendResult(value.outcome.value) }; break;
    case "fork": outcome = { type: "fork", receipt: forkReceipt(value.outcome.value) }; break;
    case "trim": outcome = { type: "trim", receipt: trimReceipt(value.outcome.value) }; break;
    case "delete": outcome = { type: "delete", receipt: deleteReceipt(value.outcome.value) }; break;
    case "commit": outcome = { type: "commit", outcome: commitResult(value.outcome.value) }; break;
    default: throw new StreamError("invalid_response", "idempotency observation has no outcome");
  }
  return { idempotencyKey: idempotencyKey(value.idempotencyKey), requestDigest: Uint8Array.from(value.requestDigest), outcome };
}

/** Process-local Stream provider backed by the canonical Rust state machine. */
export class MemoryStreamProvider implements StreamProvider {
  #raw: Promise<RawMemoryStream> | undefined;
  #binding(): Promise<RawMemoryStream> {
    return this.#raw ??= loadModule().then(module => new module.MemoryStreamBinding())
      .catch(error => { this.#raw = undefined; throw error; });
  }

  async #call<Schema extends DescMessage>(method: UnaryMethod, input: Uint8Array, schema: Schema): Promise<MessageShape<Schema>> {
    let result: Uint8Array;
    try {
      result = await (await this.#binding())[method](input);
    } catch (error) { throw asStreamError(error); }
    if (!(result instanceof Uint8Array)) throw new StreamError("invalid_response", "provider returned a non-binary response");
    return decodeResponse(schema, result);
  }
  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const value = await this.#call("inspect_idempotency", encode(InspectIdempotencyRequestSchema,
      create(InspectIdempotencyRequestSchema, { idempotencyKey: key })), InspectIdempotencyResponseSchema);
    return value.observation === undefined ? undefined : observation(value.observation);
  }
  async tail(path: string): Promise<Sequence> { return (await this.bounds(path)).tail; }
  async bounds(path: string): Promise<StreamBounds> {
    const value = await this.#call("tail", encode(TailRequestSchema, create(TailRequestSchema, { path })), TailResponseSchema);
    if (value.trimPoint === undefined) throw new StreamError("invalid_response", "memory provider omitted trim point");
    return { trimPoint: value.trimPoint, tail: value.tail };
  }
  async append(path: string, values: readonly Uint8Array[], options: AppendOptions = {}): Promise<AppendResult> {
    const input = encode(AppendRequestSchema, create(AppendRequestSchema, {
      path, records: values.map(item => Uint8Array.from(item)),
      ...(options.ifTail === undefined ? {} : { ifTail: options.ifTail }),
      ...(options.idempotencyKey === undefined ? {} : { idempotencyKey: Uint8Array.from(options.idempotencyKey) }),
    }));
    return appendResult(await this.#call("append", input, AppendResponseSchema));
  }
  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    return forkReceipt(await this.#call("fork", encode(ForkRequestSchema, create(ForkRequestSchema, {
      source, destination, ...(options.atTail === undefined ? {} : { atTail: options.atTail }),
      ...(options.idempotencyKey === undefined ? {} : { idempotencyKey: Uint8Array.from(options.idempotencyKey) }),
    })), ForkReceiptSchema));
  }
  async trim(path: string, before: Sequence, key?: IdempotencyKey): Promise<TrimReceipt> {
    return trimReceipt(await this.#call("trim", encode(TrimRequestSchema,
      create(TrimRequestSchema, { path, before, idempotencyKey: key ?? crypto.getRandomValues(new Uint8Array(16)) })), TrimReceiptSchema));
  }
  async delete(path: string, key?: IdempotencyKey): Promise<DeleteReceipt> {
    return deleteReceipt(await this.#call("delete", encode(DeleteRequestSchema,
      create(DeleteRequestSchema, { path, idempotencyKey: key ?? crypto.getRandomValues(new Uint8Array(16)) })), DeleteReceiptSchema));
  }
  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const input = encode(ReadRequestSchema, create(ReadRequestSchema, { path, from: options.from, limit: uint32(options.limit) }));
    let frames: Uint8Array[];
    try { frames = await (await this.#binding()).read(input); }
    catch (error) { throw asStreamError(error); }
    for (const frame of frames) {
      const value = decodeResponse(ReadResponseSchema, frame).record;
      if (value === undefined) throw new StreamError("invalid_response", "read response omitted record");
      yield record(value);
    }
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    if (options.signal?.aborted) return;
    let cursor: RawFollow | undefined;
    let aborted = false;
    const abort = () => { aborted = true; cursor?.close(); };
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      if (options.signal?.aborted) return;
      const input = encode(FollowRequestSchema, create(FollowRequestSchema, { path, from: options.from }));
      try { cursor = await (await this.#binding()).follow(input); }
      catch (error) { throw asStreamError(error); }
      if (aborted) { cursor.close(); return; }
      for (;;) {
        let frame: Uint8Array | undefined;
        try { frame = await cursor.next(); }
        catch (error) { throw asStreamError(error); }
        if (frame === undefined || aborted) return;
        const value = decodeResponse(ReadResponseSchema, frame).record;
        if (value === undefined) throw new StreamError("invalid_response", "follow response omitted record");
        yield record(value);
      }
    } finally {
      options.signal?.removeEventListener("abort", abort);
      cursor?.close();
      cursor?.free();
    }
  }
  async *children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }> {
    for (const child of (await this.childrenPage({ ...(parent === undefined ? {} : { parent }), limit })).children) yield child;
  }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const value = await this.#call("children_page", encode(ChildrenPageRequestSchema,
      create(ChildrenPageRequestSchema, {
        ...(request.parent === undefined ? {} : { parent: request.parent }),
        ...(request.after === undefined ? {} : { after: request.after }),
        ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
        limit: uint32(request.limit),
      })), ChildrenPageResponseSchema);
    return { hierarchyVersion: commitId(value.hierarchyVersion), children: value.children.map(item => ({ path: item.path })),
      ...(value.nextAfter === undefined ? {} : { nextAfter: value.nextAfter }) };
  }
  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    const conditions = request.conditions.map(item => "ifTail" in item
      ? { condition: { case: "tail" as const, value: { path: item.path, expected: item.ifTail } } }
      : { condition: { case: "absent" as const, value: { path: item.path } } });
    const mutations = request.mutations.map(item => {
      if ("append" in item) return { mutation: { case: "append" as const, value: { path: item.append.path, records: item.append.values.map(value => Uint8Array.from(value)) } } };
      if ("fork" in item) return { mutation: { case: "fork" as const, value: { source: item.fork.source, destination: item.fork.destination, atTail: item.fork.atTail } } };
      if ("trim" in item) return { mutation: { case: "trim" as const, value: { path: item.trim.path, before: item.trim.before } } };
      return { mutation: { case: "delete" as const, value: { path: item.delete.path } } };
    });
    return commitResult(await this.#call("commit", encode(CommitRequestSchema,
      create(CommitRequestSchema, { conditions, mutations, idempotencyKey: Uint8Array.from(options.idempotencyKey) })), CommitResponseSchema));
  }
  async readCommit(identity: Uint8Array): Promise<CommittedEnvelope> {
    return envelope(await this.#call("read_commit", encode(ReadCommitRequestSchema,
      create(ReadCommitRequestSchema, { commitId: commitId(identity) })), CommittedEnvelopeSchema));
  }
}
