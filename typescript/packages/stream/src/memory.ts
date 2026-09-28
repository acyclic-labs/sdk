import { fromBinary } from "@bufbuild/protobuf";
import { is_stream_error_code, projectMemoryResponse, WasmMemoryStream } from "../generated/wasm/acyclic_stream_wasm.js";
import type { StreamErrorCode as WasmStreamErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import {
  ChildrenResponseSchema, ReadResponseSchema, TailResponseSchema,
} from "../generated/proto/stream/v2/stream_pb.js";
import { validateAppend } from "./client.js";
import { ensureStreamWasm, normalizeWireCommitBytes, validateWireRequest, wireAppendRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import type {
  AppendOptions, AppendResult, CommittedEnvelope,
  CommitId, CommitOptions, CommitResult,
  EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey,
  IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence,
  StreamProvider, ChildrenPage, ChildrenPageRequest,
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
    const request = wireInspectIdempotencyRequest(key);
    return this.#project<IdempotencyObservation | undefined>("inspect_idempotency", request);
  }

  async tail(path: string): Promise<Sequence> {
    const request = await this.#request("tail", path);
    return fromBinary(TailResponseSchema, await this.#dispatch("tail", request)).tail;
  }
  async append(path: string, values: readonly Uint8Array[], options: AppendOptions = {}): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = structuredClone(options);
    await validateAppend(path, records, authored);
    const request = wireAppendRequest(path, records, authored);
    return this.#project<AppendResult>("append", request);
  }

  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    const authored = structuredClone(options);
    await validateWireRequest({ kind: "fork", source, destination, options: authored });
    const request = wireRequest({ kind: "fork", source, destination, options: authored });
    return this.#project<ForkReceipt>("fork", request);
  }

  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const { from, limit } = options;
    await validateWireRequest({ kind: "read", path, from, limit });
    const request = wireRequest({ kind: "read", path, from, limit });
    for (const value of await this.#read(request)) yield value;
  }

  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const { from, signal } = options;
    await validateWireRequest({ kind: "follow", path, from });
    if (signal?.aborted) return;
    const request = wireRequest({ kind: "follow", path, from });
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
    const request = wireRequest({ kind: "children", limit, ...(parent === undefined ? {} : { parent }) });
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
    if (request === null || typeof request !== "object") {
      throw new StreamError("invalid_argument", "children page request must be an object");
    }
    const authored = { kind: "children_page" as const,
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    };
    await validateWireRequest(authored);
    const input = wireRequest(authored);
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
    const request = wireReadCommitRequest(value);
    return this.#project<CommittedEnvelope>("read_commit", request);
  }

  async #request(kind: "tail", path: string): Promise<Uint8Array> {
    await validateWireRequest({ kind, path });
    return wireRequest({ kind, path });
  }

}

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
  else if (rawCode === "prefix_not_retained" && operation === "commit") code = "invalid_argument";
  return new StreamError(code, message);
}
function record(value: { sequence: bigint; value: Uint8Array; commitId: Uint8Array }): EncodedRecord {
  return { sequence: value.sequence, value: value.value, commitId: commitId(value.commitId) };
}
