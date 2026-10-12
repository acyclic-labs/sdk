import { rootCertificates } from "node:tls";
import { createClient, ConnectError, Code, type Interceptor } from "@connectrpc/connect";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/stream_pb.js";
import { projectMemoryResponse } from "../generated/wasm/acyclic_stream_wasm.js";
import { validateAppend } from "./client.js";
import { checkedRecord, normalizeWireCommitBytes, readResponseRecords, validateWireRequest, wireAppendRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { StreamError, type StreamFailureCode } from "./types.js";
import type { StreamProvider, AppendOptions, AppendResult, ForkOptions, ForkReceipt, ReadOptions, FollowOptions, EncodedRecord, ChildrenPageRequest, ChildrenPage, ProviderCommitRequest, CommitOptions, CommitResult, CommitId, CommittedEnvelope, IdempotencyKey, IdempotencyObservation } from "./types.js";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { StreamService } from "../generated/proto/stream/v1/stream_pb.js";
import { observeInterceptors, resolveObserver, type AcyclicObserver } from "@acyclic-labs/actors/observe";

export interface StreamGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
  readonly observer?: AcyclicObserver;
  /** Best-effort validated frame receipts, enabled by default. */
  readonly autoAcknowledge?: boolean;
}

/** Complete Stream v1 gRPC client, including streaming reads/follow and atomic Commit. */
export function createStreamGrpcClient(options: StreamGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  if (!options.token.trim() || /[\r\n]/.test(options.token)) throw new TypeError("invalid bearer token");
  const maximum = options.maximumMessageBytes ?? 16 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    return next(request);
  };
  const client = createClient(StreamService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: observeInterceptors([authenticate], resolveObserver(options.observer), "stream"), readMaxBytes: maximum, writeMaxBytes: maximum, ...(options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } }) }));
  const acknowledgeDelivery: typeof client.acknowledgeDelivery = (request, callOptions) => {
    validateDeliveryToken(request.deliveryToken);
    return client.acknowledgeDelivery({ ...request, deliveryToken: request.deliveryToken.slice() }, callOptions);
  };
  return { ...client, acknowledgeDelivery };
}

/** Existing Stream provider API over native HTTP/2 gRPC in Node and Bun. */
export class GrpcStreamProvider implements StreamProvider {
  readonly #client: ReturnType<typeof createStreamGrpcClient>;
  readonly #autoAcknowledge: boolean;
  constructor(options: StreamGrpcOptions) {
    this.#client = createStreamGrpcClient(options);
    this.#autoAcknowledge = options.autoAcknowledge ?? true;
  }
  /** Explicit receipt on the authenticated issuing endpoint. */
  async acknowledgeDelivery(deliveryToken: Uint8Array): Promise<void> {
    await this.#call("acknowledge_delivery", () => this.#client.acknowledgeDelivery({ deliveryToken }));
  }

  async #call<T>(operation: string, call: () => Promise<T>): Promise<T> {
    try { return await call(); } catch (error) { throw providerError(error, operation); }
  }
  #project<T>(operation: string, bytes: Uint8Array): T {
    try { return projectMemoryResponse(operation, bytes) as T; }
    catch { throw new StreamError("invalid_response", `invalid ${operation} response`); }
  }
  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const request = fromBinary(wire.InspectIdempotencyRequestSchema, wireInspectIdempotencyRequest(key));
    const response = await this.#call("inspect_idempotency", () => this.#client.inspectIdempotency(request));
    if (response.observation !== undefined && !sameBytes(response.observation.idempotencyKey, key)) throw new StreamError("invalid_response", "idempotency response names another retry identity");
    return this.#project("inspect_idempotency", toBinary(wire.InspectIdempotencyResponseSchema, response));
  }
  async tail(path: string): Promise<bigint> {
    await validateWireRequest({ kind: "tail", path });
    return (await this.#call("tail", () => this.#client.tail(fromBinary(wire.TailRequestSchema, wireRequest({ kind: "tail", path }))))).tail;
  }
  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = options === undefined ? undefined : structuredClone(options);
    await validateAppend(path, records, authored);
    const request = fromBinary(wire.AppendRequestSchema, wireAppendRequest(path, records, authored));
    const response = await this.#call("append", () => this.#client.append(request));
    return this.#project("append", toBinary(wire.AppendResponseSchema, response));
  }
  async fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> {
    const request = { kind: "fork" as const, source, destination, ...(options === undefined ? {} : { options: structuredClone(options) }) };
    await validateWireRequest(request);
    const response = await this.#call("fork", () => this.#client.fork(fromBinary(wire.ForkRequestSchema, wireRequest(request))));
    return this.#project("fork", toBinary(wire.ForkReceiptSchema, response));
  }
  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const request = { kind: "read" as const, path, from: options.from, limit: options.limit };
    await validateWireRequest(request);
    yield* this.#frames("read", this.#client.read(fromBinary(wire.ReadRequestSchema, wireRequest(request))), request.from, request.limit);
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const request = { kind: "follow" as const, path, from: options.from };
    await validateWireRequest(request);
    if (options.signal?.aborted) return;
    yield* this.#frames("follow", this.#client.follow(fromBinary(wire.FollowRequestSchema, wireRequest(request)), options.signal === undefined ? {} : { signal: options.signal }), request.from, undefined, options.signal);
  }
  async *#frames(operation: string, frames: AsyncIterable<wire.ReadResponse>, next: bigint,
    remaining?: number, signal?: AbortSignal): AsyncIterable<EncodedRecord> {
    const acknowledgements = this.#autoAcknowledge ? deliveryAcknowledgements(this.#client, signal) : undefined;
    try {
      for await (const response of frames) {
        if (signal?.aborted) return;
        if (response.deliveryToken !== undefined && response.deliveryToken.byteLength !== 32) {
          throw new StreamError("invalid_response", "stream frame delivery token must contain exactly 32 bytes");
        }
        const values = readResponseRecords(response);
        if (remaining !== undefined && values.length > remaining) throw new StreamError("invalid_response", "read exceeds requested limit");
        const records = values.map((value, index) => checkedRecord(value, next + BigInt(index)));
        next += BigInt(records.length);
        if (remaining !== undefined) remaining -= records.length;
        if (response.deliveryToken !== undefined) acknowledgements?.enqueue(response.deliveryToken);
        for (const record of records) {
          if (signal?.aborted) return;
          yield record;
        }
      }
    } catch (error) {
      if (!signal?.aborted) throw providerError(error, operation);
    } finally { acknowledgements?.close(); }
  }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const authored = { kind: "children_page" as const, limit: request.limit,
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion.slice() }),
    };
    await validateWireRequest(authored);
    const response = await this.#call("children_page", () => this.#client.childrenPage(fromBinary(wire.ChildrenPageRequestSchema, wireRequest(authored))));
    return this.#project("children_page", toBinary(wire.ChildrenPageResponseSchema, response));
  }
  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    const input = await normalizeWireCommitBytes(structuredClone(request), structuredClone(options));
    const response = await this.#call("commit", () => this.#client.commit(fromBinary(wire.CommitRequestSchema, input)));
    return this.#project("commit", toBinary(wire.CommitResponseSchema, response));
  }
  async readCommit(id: CommitId): Promise<CommittedEnvelope> {
    const request = fromBinary(wire.ReadCommitRequestSchema, wireReadCommitRequest(id));
    const response = await this.#call("read_commit", () => this.#client.readCommit(request));
    if (!sameBytes(response.commitId, id)) throw new StreamError("invalid_response", "commit response names another commit");
    return this.#project("read_commit", toBinary(wire.CommittedEnvelopeSchema, response));
  }
}

function validateDeliveryToken(token: unknown): asserts token is Uint8Array {
  if (!(token instanceof Uint8Array) || token.byteLength !== 32) throw new StreamError("invalid_argument", "delivery token must contain exactly 32 bytes");
}

function deliveryAcknowledgements(client: ReturnType<typeof createStreamGrpcClient>, signal?: AbortSignal) {
  const abort = new AbortController();
  const queue: Uint8Array[] = [];
  let wake: (() => void) | undefined;
  const close = () => {
    abort.abort();
    queue.length = 0;
    signal?.removeEventListener("abort", close);
    wake?.();
  };
  signal?.addEventListener("abort", close, { once: true });
  if (signal?.aborted) close();
  void (async () => {
    while (!abort.signal.aborted) {
      const token = queue.shift();
      if (token === undefined) {
        await new Promise<void>(resolve => { wake = resolve; });
        wake = undefined;
      } else {
        try { await client.acknowledgeDelivery({ deliveryToken: token }, { signal: abort.signal, timeoutMs: 500 }); }
        catch { /* Receipts never affect record delivery. */ }
      }
    }
  })();
  return { close, enqueue(token: Uint8Array) {
    if (abort.signal.aborted || queue.length >= 16) return;
    queue.push(token.slice());
    wake?.();
  } };
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}
/** Matches the Rust gRPC status mapping; unknown peer statuses remain unavailable. */
function providerError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  if (!(error instanceof ConnectError)) return error instanceof Error ? error : new StreamError("unavailable", String(error));
  const reason = error.rawMessage;
  let code: StreamFailureCode = "unavailable";
  switch (error.code) {
    case Code.InvalidArgument: code = reason === "invalid_path" || reason === "limit_exceeded" ? reason : "invalid_argument"; break;
    case Code.NotFound: code = operation === "read_commit" ? "commit_not_found" : "stream_not_found"; break;
    case Code.AlreadyExists: code = "destination_exists"; break;
    case Code.OutOfRange: code = "out_of_range"; break;
    case Code.PermissionDenied: case Code.Unauthenticated: code = "access_denied"; break;
    case Code.ResourceExhausted: code = "capacity_exhausted"; break;
    case Code.Unimplemented: if (operation === "acknowledge_delivery" || reason === "unsupported_capability") code = "unsupported"; break;
    case Code.FailedPrecondition:
      if (reason === "hierarchy_changed" || reason === "idempotency_mismatch" || reason === "prefix_not_retained" || reason === "deadline_elapsed") code = reason;
      break;
  }
  if (code === "prefix_not_retained" && operation === "commit") code = "invalid_argument";
  return new StreamError(code, reason);
}
