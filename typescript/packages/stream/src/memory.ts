import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { is_stream_error_code, projectMemoryResponse, WasmMemoryStream } from "../generated/wasm/acyclic_stream_wasm.js";
import type { StreamErrorCode as WasmStreamErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import {
  AppendRequestSchema,
  ChildrenRequestSchema, ChildrenResponseSchema, ChildrenPageRequestSchema,
  DeleteRequestSchema,
  ForkRequestSchema, FollowRequestSchema, InspectIdempotencyRequestSchema,
  ReadCommitRequestSchema, ReadRequestSchema, ReadResponseSchema,
  TailRequestSchema, TailResponseSchema, TrimRequestSchema,
} from "../generated/proto/stream/v2/stream_pb.js";
import { validateAppend } from "./client.js";
import { ensureStreamWasm, normalizeWireCommitBytes, validateWireRequest } from "./contract.js";
import type {
  AppendOptions, AppendResult, CommittedEnvelope,
  CommitId, CommitOptions, CommitResult,
  DeleteReceipt, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey,
  IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence,
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

  async #project<Result>(operation: string, request: Uint8Array): Promise<Result> {
    const response = await this.#dispatch(operation, request);
    try { return projectMemoryResponse(operation, response) as Result; }
    catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new StreamError("invalid_response", `Rust ${operation} response is invalid: ${message}`);
    }
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
    return this.#project<IdempotencyObservation | undefined>("inspect_idempotency", request);
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
    return this.#project<AppendResult>("append", request);
  }

  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    const authored = structuredClone(options);
    await validateWireRequest({ kind: "fork", source, destination, options: authored });
    const request = toBinary(ForkRequestSchema, create(ForkRequestSchema, {
      source, destination,
      ...(authored.atTail === undefined ? {} : { atTail: authored.atTail }),
      ...(authored.idempotencyKey === undefined ? {} : { idempotencyKey: authored.idempotencyKey }),
    }));
    return this.#project<ForkReceipt>("fork", request);
  }

  async trim(path: string, before: Sequence, key?: IdempotencyKey): Promise<TrimReceipt> {
    const retainedKey = key === undefined ? randomKey() : idempotencyKey(key);
    await validateWireRequest({ kind: "trim", path, before, key: retainedKey });
    const request = toBinary(TrimRequestSchema, create(TrimRequestSchema, { path, before, idempotencyKey: retainedKey }));
    return this.#project<TrimReceipt>("trim", request);
  }

  async delete(path: string, key?: IdempotencyKey): Promise<DeleteReceipt> {
    const retainedKey = key === undefined ? randomKey() : idempotencyKey(key);
    await validateWireRequest({ kind: "delete", path, key: retainedKey });
    const request = toBinary(DeleteRequestSchema, create(DeleteRequestSchema, { path, idempotencyKey: retainedKey }));
    return this.#project<DeleteReceipt>("delete", request);
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
    return this.#project<ChildrenPage>("children_page", input);
  }

  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    const authored = structuredClone(request);
    const retainedOptions = structuredClone(options);
    idempotencyKey(retainedOptions.idempotencyKey);
    const input = await normalizeWireCommitBytes(authored, retainedOptions);
    return this.#project<CommitResult>("commit", input);
  }

  async readCommit(value: CommitId): Promise<CommittedEnvelope> {
    const request = toBinary(ReadCommitRequestSchema, create(ReadCommitRequestSchema, { commitId: commitId(value) }));
    return this.#project<CommittedEnvelope>("read_commit", request);
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
