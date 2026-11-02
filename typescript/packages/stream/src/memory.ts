import { wireWhenAbsent } from "./contract.js";
import { streamError } from "./wasm-error.js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { projectMemoryResponse, WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";
import {
  TailResponseSchema, FollowRequestSchema,
} from "../generated/proto/stream/v1/stream_pb.js";
import type { Opened } from "./opened.js";
import { wasmRecords } from "./wasm-records.js";
import { validateAppend } from "./client.js";
import { checkedChild } from "./child.js";
import { ensureStreamWasm, normalizeWireCommitBytes, validateWireRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import type {
  AppendOptions, AppendResult, CommittedEnvelope,
  CommitId, CommitOptions, CommitResult,
  FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey,
  IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence,
  StreamProvider, ChildrenPage, ChildrenPageRequest, StreamFailureCode,
} from "./types.js";
import { StreamError, idempotencyKey } from "./types.js";

/** Shared projection over the canonical Rust provider ABI. */
class RustStreamProvider implements StreamProvider {
  readonly #inner: Promise<WasmStream>;

  protected constructor(database?: { readonly name: string; readonly maximumCommands?: number; readonly maximumJournalBytes?: bigint }) {
    if (database !== undefined && (typeof database.name !== "string" || database.name.length === 0
      || (database.maximumCommands !== undefined && (!Number.isSafeInteger(database.maximumCommands) || database.maximumCommands <= 0 || database.maximumCommands > 0xffff_ffff))
      || (database.maximumJournalBytes !== undefined && (typeof database.maximumJournalBytes !== "bigint" || database.maximumJournalBytes <= 0n || database.maximumJournalBytes > 0xffff_ffff_ffff_ffffn)))) {
      throw new StreamError("invalid_argument", "browser database and journal bounds must be positive and representable");
    }
    this.#inner = ensureStreamWasm().then(() => database === undefined
      ? new WasmStream()
      : WasmStream.openBrowser(database.name, database.maximumCommands, database.maximumJournalBytes));
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

  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const request = wireInspectIdempotencyRequest(key);
    return this.#project<IdempotencyObservation | undefined>("inspect_idempotency", request);
  }

  async heads(options: import("./types.js").HeadsRequest): Promise<import("./types.js").Heads> {
    const request = { kind: "heads" as const, ...structuredClone(options) };
    await validateWireRequest(request);
    return this.#project("heads", wireRequest(request));
  }
  async head(path: string): Promise<import("./types.js").PathHead> {
    const request = { kind: "head" as const, path };
    await validateWireRequest(request);
    return this.#project("head", wireRequest(request));
  }
  async tail(path: string): Promise<Sequence> {
    const request = { kind: "tail" as const, path };
    await validateWireRequest(request);
    return fromBinary(TailResponseSchema, await this.#dispatch("tail", wireRequest(request))).tail;
  }
  async append(path: string, values: readonly Uint8Array[], options: AppendOptions = {}): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = structuredClone(options);
    const request = await validateAppend(path, records, authored);
    return this.#project<AppendResult>("append", request);
  }

  async fork(source: string, destination: string, options: ForkOptions = {}): Promise<ForkReceipt> {
    const authored = structuredClone(options);
    await validateWireRequest({ kind: "fork", source, destination, options: authored });
    const request = wireRequest({ kind: "fork", source, destination, options: authored });
    return this.#project<ForkReceipt>("fork", request);
  }

  read(path: string, options: ReadOptions): Opened {
    const authored = structuredClone(options);
    return this.#records("read", path, authored);
  }

  follow(path: string, options: FollowOptions): Opened {
    const authored = { ...options, ...(options.filter === undefined ? {} : { filter: structuredClone(options.filter) }) };
    return this.#records("follow", path, authored, options.signal);
  }

  #records(operation: "read" | "follow", path: string, options: ReadOptions | FollowOptions, signal?: AbortSignal): Opened {
    return wasmRecords(async signal => {
      const input = operation === "read"
        ? await validateWireRequest({ kind: "read", path, ...(options as ReadOptions) })
        : (await validateWireRequest({ kind: "follow", path, from: options.from }),
          toBinary(FollowRequestSchema, create(FollowRequestSchema, { path, from: options.from,
            whenAbsent: wireWhenAbsent((options as FollowOptions).whenAbsent),
            ...((options as FollowOptions).filter === undefined ? {} : { filter: (options as FollowOptions).filter }),
          })));
      const inner = await this.#inner;
      signal.throwIfAborted();
      return operation === "read" ? inner.read(input) : inner.open_follow(input);
    }, path, operation === "read" ? options as ReadOptions : undefined, signal);
  }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    if (request === null || typeof request !== "object") {
      throw new StreamError("invalid_argument", "children page request must be an object");
    }
    const authored = { kind: "children_page" as const,
      ...(request.tailStream === undefined ? {} : { tailStream: request.tailStream }),
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

}


/** Durable account-local provider using strict IndexedDB publication and Rust recovery. */
export class BrowserStreamProvider extends RustStreamProvider {
  constructor(name: string, options: { readonly maximumCommands?: number; readonly maximumJournalBytes?: bigint } = {}) {
    super({ name, ...options });
  }
}

/** Process-local adapter over the canonical Rust MemoryStream. */
export class MemoryStreamProvider extends RustStreamProvider {
  constructor() { super(); }
}
