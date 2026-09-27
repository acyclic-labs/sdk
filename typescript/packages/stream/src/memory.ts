import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { is_stream_error_code, WasmMemoryStream } from "../generated/wasm/acyclic_stream_wasm.js";
import type { StreamErrorCode as WasmStreamErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import {
  AppendRequestSchema, AppendResponseSchema,
  ChildrenRequestSchema, ChildrenResponseSchema, ChildrenPageRequestSchema, ChildrenPageResponseSchema,
  CommitResponseSchema, CommittedEnvelopeSchema,
  DeleteRequestSchema, DeleteReceiptSchema, ForkReceiptSchema,
  ForkRequestSchema, FollowRequestSchema, InspectIdempotencyRequestSchema, InspectIdempotencyResponseSchema,
  ReadCommitRequestSchema, ReadRequestSchema, ReadResponseSchema,
  TailRequestSchema, TailResponseSchema, TrimRequestSchema, TrimReceiptSchema,
} from "../generated/proto/stream/v2/stream_pb.js";
import { validateAppend } from "./client.js";
import { ensureStreamWasm, normalizeWireCommitBytes, validateWireRequest } from "./contract.js";
import type {
  AppendOptions, AppendResult, CommitConflict, CommittedEnvelope,
  CommittedMutation, CommitId, CommitOptions, CommitResult,
  DeleteReceipt, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey,
  IdempotencyObservation, IdempotencyOutcome, ProviderCommitRequest, ReadOptions, Sequence,
  StreamBounds, StreamProvider, TrimReceipt, ChildrenPage, ChildrenPageRequest,
} from "./types.js";
import { StreamError, commitId, idempotencyKey } from "./types.js";

/** Process-local adapter over the canonical Rust MemoryStream. */
export class MemoryStreamProvider implements StreamProvider {
  readonly #inner: Promise<WasmMemoryStream>;

  constructor() {
    this.#inner = ensureStreamWasm().then(() => new WasmMemoryStream());
  }

  async #dispatch(operation: string, request: Uint8Array): Promise<Uint8Array> {
    try { return await (await this.#inner).dispatch(operation, request); }
    catch (error) { throw streamError(error, operation); }
  }

  async #read(request: Uint8Array): Promise<readonly EncodedRecord[]> {
    try {
      const values = await (await this.#inner).read(request);
      return values.map(value => {
        const response = fromBinary(ReadResponseSchema, value);
        if (response.record === undefined) throw new StreamError("invalid_response", "Rust read response omitted its record");
        return record(response.record);
      });
    } catch (error) { throw streamError(error, "read"); }
  }

  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const request = toBinary(InspectIdempotencyRequestSchema, create(InspectIdempotencyRequestSchema, { idempotencyKey: idempotencyKey(key) }));
    const response = fromBinary(InspectIdempotencyResponseSchema, await this.#dispatch("inspect_idempotency", request));
    if (response.observation === undefined) return undefined;
    const value = response.observation;
    let outcome: IdempotencyOutcome;
    switch (value.outcome.case) {
      case "append": outcome = { type: "append", outcome: appendResult(value.outcome.value) }; break;
      case "fork": outcome = { type: "fork", receipt: forkReceipt(value.outcome.value) }; break;
      case "trim": outcome = { type: "trim", receipt: trimReceipt(value.outcome.value) }; break;
      case "delete": outcome = { type: "delete", receipt: deleteReceipt(value.outcome.value) }; break;
      case "commit": outcome = { type: "commit", outcome: commitResult(value.outcome.value) }; break;
      default: throw new StreamError("invalid_response", "Rust idempotency observation omitted its outcome");
    }
    return { idempotencyKey: idempotencyKey(value.idempotencyKey), requestDigest: value.requestDigest, outcome };
  }

  async tail(path: string): Promise<Sequence> {
    await validateWireRequest({ kind: "tail", path });
    const request = toBinary(TailRequestSchema, create(TailRequestSchema, { path }));
    return fromBinary(TailResponseSchema, await this.#dispatch("tail", request)).tail;
  }
  async bounds(path: string): Promise<StreamBounds> {
    await validateWireRequest({ kind: "tail", path });
    const response = fromBinary(TailResponseSchema, await this.#dispatch("bounds", toBinary(TailRequestSchema, create(TailRequestSchema, { path }))));
    if (response.trimPoint === undefined) throw new StreamError("invalid_response", "Rust bounds response omitted trim point");
    return { trimPoint: response.trimPoint, tail: response.tail };
  }

  async append(path: string, values: readonly Uint8Array[], options: AppendOptions = {}): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = structuredClone(options);
    await validateAppend(path, records, authored);
    const request = toBinary(AppendRequestSchema, create(AppendRequestSchema, {
      path, records,
      ...(authored.ifTail === undefined ? {} : { ifTail: authored.ifTail }),
      ...(authored.idempotencyKey === undefined ? {} : { idempotencyKey: authored.idempotencyKey }),
    }));
    return appendResult(fromBinary(AppendResponseSchema, await this.#dispatch("append", request)));
  }

  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    const authored = structuredClone(options);
    await validateWireRequest({ kind: "fork", source, destination, options: authored });
    const request = toBinary(ForkRequestSchema, create(ForkRequestSchema, {
      source, destination,
      ...(authored.atTail === undefined ? {} : { atTail: authored.atTail }),
      ...(authored.idempotencyKey === undefined ? {} : { idempotencyKey: authored.idempotencyKey }),
    }));
    return forkReceipt(fromBinary(ForkReceiptSchema, await this.#dispatch("fork", request)));
  }

  async trim(path: string, before: Sequence, key?: IdempotencyKey): Promise<TrimReceipt> {
    const retainedKey = key === undefined ? randomKey() : idempotencyKey(key);
    await validateWireRequest({ kind: "trim", path, before, key: retainedKey });
    const request = toBinary(TrimRequestSchema, create(TrimRequestSchema, { path, before, idempotencyKey: retainedKey }));
    return trimReceipt(fromBinary(TrimReceiptSchema, await this.#dispatch("trim", request)));
  }

  async delete(path: string, key?: IdempotencyKey): Promise<DeleteReceipt> {
    const retainedKey = key === undefined ? randomKey() : idempotencyKey(key);
    await validateWireRequest({ kind: "delete", path, key: retainedKey });
    const request = toBinary(DeleteRequestSchema, create(DeleteRequestSchema, { path, idempotencyKey: retainedKey }));
    return deleteReceipt(fromBinary(DeleteReceiptSchema, await this.#dispatch("delete", request)));
  }

  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const { from, limit } = options;
    await validateWireRequest({ kind: "read", path, from, limit });
    const request = toBinary(ReadRequestSchema, create(ReadRequestSchema, { path, from, limit }));
    for (const value of await this.#read(request)) yield value;
  }

  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const { from, signal } = options;
    await validateWireRequest({ kind: "follow", path, from });
    if (signal?.aborted) return;
    const request = toBinary(FollowRequestSchema, create(FollowRequestSchema, { path, from }));
    let handle: Awaited<ReturnType<WasmMemoryStream["open_follow"]>>;
    try { handle = await (await this.#inner).open_follow(request); }
    catch (error) { throw streamError(error, "follow"); }
    const close = () => handle.close();
    signal?.addEventListener("abort", close, { once: true });
    try {
      while (!signal?.aborted) {
        let bytes: Uint8Array | null;
        try { bytes = await handle.next(); }
        catch (error) { throw streamError(error, "follow"); }
        if (bytes === null || signal?.aborted) return;
        const response = fromBinary(ReadResponseSchema, bytes);
        if (response.record === undefined) throw new StreamError("invalid_response", "Rust follow response omitted its record");
        yield record(response.record);
      }
    } finally {
      signal?.removeEventListener("abort", close);
      handle.close();
      handle.free();
    }
  }

  async *children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }> {
    await validateWireRequest({ kind: "children", limit, ...(parent === undefined ? {} : { parent }) });
    const request = toBinary(ChildrenRequestSchema, create(ChildrenRequestSchema, { limit, ...(parent === undefined ? {} : { parent }) }));
    let values: Uint8Array[];
    try { values = await (await this.#inner).children(request); }
    catch (error) { throw streamError(error, "children"); }
    for (const value of values) {
      const response = fromBinary(ChildrenResponseSchema, value);
      if (response.child === undefined) throw new StreamError("invalid_response", "Rust children response omitted its child");
      yield { path: response.child.path };
    }
  }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const input = toBinary(ChildrenPageRequestSchema, create(ChildrenPageRequestSchema, {
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    }));
    const response = fromBinary(ChildrenPageResponseSchema, await this.#dispatch("children_page", input));
    return { hierarchyVersion: commitId(response.hierarchyVersion), children: response.children.map(item => ({ path: item.path })), ...(response.nextAfter === undefined ? {} : { nextAfter: response.nextAfter }) };
  }

  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    const authored = structuredClone(request);
    const retainedOptions = structuredClone(options);
    idempotencyKey(retainedOptions.idempotencyKey);
    const input = await normalizeWireCommitBytes(authored, retainedOptions);
    return commitResult(fromBinary(CommitResponseSchema, await this.#dispatch("commit", input)));
  }

  async readCommit(value: CommitId): Promise<CommittedEnvelope> {
    const request = toBinary(ReadCommitRequestSchema, create(ReadCommitRequestSchema, { commitId: commitId(value) }));
    return envelope(fromBinary(CommittedEnvelopeSchema, await this.#dispatch("read_commit", request)));
  }

}

function randomKey(): IdempotencyKey { return idempotencyKey(crypto.getRandomValues(new Uint8Array(16))); }
export type StreamErrorCode = WasmStreamErrorCode;

const isKnownStreamErrorCode = (value: string): value is StreamErrorCode => is_stream_error_code(value);

function streamError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  let message: string;
  let rawCode: unknown;
  try {
    message = error instanceof Error ? error.message : String(error);
  } catch {
    message = "Stream operation failed";
  }
  try {
    rawCode = typeof error === "object" && error !== null ? (error as { readonly code?: unknown }).code : undefined;
  } catch {
    rawCode = undefined;
  }
  if (typeof rawCode !== "string" || !isKnownStreamErrorCode(rawCode)) {
    return error instanceof Error ? error : new Error(message);
  }
  let code: string = rawCode;
  if (rawCode === "not_found") code = operation === "read_commit" ? "commit_not_found" : "stream_not_found";
  else if (rawCode === "already_exists") code = "destination_exists";
  else if (rawCode === "retired") code = "stream_retired";
  else if (rawCode === "prefix_not_retained" && operation === "commit") code = "invalid_argument";
  return new StreamError(code, message);
}
function record(value: { sequence: bigint; value: Uint8Array; commitId: Uint8Array }): EncodedRecord {
  return { sequence: value.sequence, value: value.value, commitId: commitId(value.commitId) };
}
function appendResult(value: import("../generated/proto/stream/v2/stream_pb.js").AppendResponse): AppendResult {
  switch (value.outcome.case) {
    case "committed": return { ok: true, start: value.outcome.value.start, end: value.outcome.value.end, tail: value.outcome.value.tail, commitId: commitId(value.outcome.value.commitId) };
    case "conflict": return { ok: false, code: "tail_conflict", actualTail: value.outcome.value.actualTail };
    default: throw new StreamError("invalid_response", "Rust append response omitted its outcome");
  }
}
function forkReceipt(value: import("../generated/proto/stream/v2/stream_pb.js").ForkReceipt): ForkReceipt {
  return { source: value.source, destination: value.destination, forkedAt: value.forkedAt, tail: value.tail, commitId: commitId(value.commitId) };
}
function trimReceipt(value: import("../generated/proto/stream/v2/stream_pb.js").TrimReceipt): TrimReceipt {
  return { path: value.path, trimPoint: value.trimPoint, commitId: commitId(value.commitId) };
}
function deleteReceipt(value: import("../generated/proto/stream/v2/stream_pb.js").DeleteReceipt): DeleteReceipt {
  return { path: value.path, commitId: commitId(value.commitId) };
}
function envelope(value: import("../generated/proto/stream/v2/stream_pb.js").CommittedEnvelope): CommittedEnvelope {
  const mutations: CommittedMutation[] = value.mutations.map(item => {
    switch (item.mutation.case) {
      case "append": { const part = item.mutation.value; return { type: "append", path: part.path, start: part.start, end: part.end, tail: part.tail, records: part.records.map(record) }; }
      case "fork": { const part = item.mutation.value; return { type: "fork", source: part.source, destination: part.destination, forkedAt: part.forkedAt, tail: part.tail, records: part.records.map(record) }; }
      case "trim": { const part = item.mutation.value; return { type: "trim", path: part.path, trimPoint: part.trimPoint }; }
      case "delete": return { type: "delete", path: item.mutation.value.path };
      default: throw new StreamError("invalid_response", "Rust commit envelope contains an unknown mutation");
    }
  });
  return { commitId: commitId(value.commitId), mutations };
}
function commitResult(value: import("../generated/proto/stream/v2/stream_pb.js").CommitResponse): CommitResult {
  switch (value.outcome.case) {
    case "committed": {
      const retained = envelope(value.outcome.value);
      const tails: Record<string, Sequence> = {};
      const forks: { path: string; tail: Sequence }[] = [];
      for (const mutation of retained.mutations) {
        if (mutation.type === "append") Object.defineProperty(tails, mutation.path, { value: mutation.tail, enumerable: true, configurable: true, writable: true });
        if (mutation.type === "fork") forks.push({ path: mutation.destination, tail: mutation.tail });
      }
      return { ok: true, commitId: retained.commitId, tails, forks };
    }
    case "conflict": {
      const conflicts: CommitConflict[] = value.outcome.value.conflicts.map(item => {
        switch (item.conflict.case) {
          case "tail": { const part = item.conflict.value; return { path: part.path, expectedTail: part.expected, ...(part.actual === undefined ? {} : { actualTail: part.actual }) }; }
          case "exists": return { path: item.conflict.value.path, expectedAbsent: true, actual: "exists" };
          case "retired": return { path: item.conflict.value.path, expectedAbsent: true, actual: "retired" };
          default: throw new StreamError("invalid_response", "Rust commit conflict omitted its kind");
        }
      });
      return { ok: false, code: "conflict", conflicts };
    }
    default: throw new StreamError("invalid_response", "Rust commit response omitted its outcome");
  }
}
