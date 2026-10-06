import { rootCertificates } from "node:tls";
import { createClient, ConnectError, Code, type Interceptor } from "@connectrpc/connect";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v2/stream_pb.js";
import { is_stream_error_code, projectGrpcReadResponse, projectMemoryResponse, publicHttpErrorCode, validateGrpcResponseIdentity } from "../generated/wasm/acyclic_stream_wasm.js";
import { validateAppend } from "./client.js";
import { normalizeWireCommitBytes, validateWireRequest, wireAppendRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { StreamError } from "./types.js";
import type { StreamProvider, AppendOptions, AppendResult, ForkOptions, ForkReceipt, ReadOptions, FollowOptions, EncodedRecord, ChildrenPageRequest, ChildrenPage, ProviderCommitRequest, CommitOptions, CommitResult, CommitId, CommittedEnvelope, IdempotencyKey, IdempotencyObservation } from "./types.js";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { StreamService } from "../generated/proto/stream/v2/stream_pb.js";
import { ProtocolService } from "../generated/proto/transport/v1/transport_pb.js";
import { STREAM_HANDSHAKE, STREAM_REMOTE_POLICY, rustOwnedGrpcHandshakeRequest, validateRustOwnedCredentialPolicy, validateRustOwnedGrpcHandshake } from "./generated-client.js";

export interface StreamGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Complete Stream v2 gRPC client, including streaming reads/follow and atomic Commit. */
export function createStreamGrpcClient(options: StreamGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  validateRustOwnedCredentialPolicy(options.token);
  const maximum = options.maximumMessageBytes ?? STREAM_REMOTE_POLICY.maximumMessageBytes;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    request.header.set("acyclic-family", "stream");
    return next(request);
  };
  const tls = options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } };
  const control = createClient(ProtocolService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: STREAM_REMOTE_POLICY.maximumMessageBytes, writeMaxBytes: STREAM_REMOTE_POLICY.maximumMessageBytes, ...tls }));
  let handshake: Promise<void> | undefined;
  const applicationAuthenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    request.header.set("acyclic-family", "stream");
    if (handshake === undefined) {
      const pending = control.handshake(rustOwnedGrpcHandshakeRequest(STREAM_HANDSHAKE, "stream"), { timeoutMs: STREAM_REMOTE_POLICY.requestTimeoutMillis })
        .then(response => { validateRustOwnedGrpcHandshake(response, STREAM_HANDSHAKE, "stream"); });
      const wrapped = pending.catch(error => { if (handshake === wrapped) handshake = undefined; throw error; });
      handshake = wrapped;
    }
    await handshake;
    return next(request);
  };
  return createClient(StreamService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [applicationAuthenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...tls }));
}

/** Existing Stream provider API over native HTTP/2 gRPC in Node and Bun. */
export class GrpcStreamProvider implements StreamProvider {
  readonly #client: ReturnType<typeof createStreamGrpcClient>;
  constructor(options: StreamGrpcOptions) { this.#client = createStreamGrpcClient(options); }

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
    try { validateGrpcResponseIdentity("inspect_idempotency", toBinary(wire.InspectIdempotencyResponseSchema, response), key); }
    catch { throw new StreamError("invalid_response", "idempotency response names another retry identity"); }
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
    let next = request.from;
    let count = 0;
    try {
      for await (const response of this.#client.read(fromBinary(wire.ReadRequestSchema, wireRequest(request)))) {
        if (++count > request.limit) throw new StreamError("invalid_response", "read exceeds requested limit");
        const record = this.#projectGrpcRecord(response, next);
        next = record.sequence + 1n;
        yield record;
      }
    } catch (error) { throw providerError(error, "read"); }
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const request = { kind: "follow" as const, path, from: options.from };
    await validateWireRequest(request);
    if (options.signal?.aborted) return;
    let next = request.from;
    try {
      for await (const response of this.#client.follow(fromBinary(wire.FollowRequestSchema, wireRequest(request)), options.signal === undefined ? {} : { signal: options.signal })) {
        if (options.signal?.aborted) return;
        const record = this.#projectGrpcRecord(response, next);
        next = record.sequence + 1n;
        yield record;
      }
    } catch (error) {
      if (options.signal?.aborted) return;
      throw providerError(error, "follow");
    }
  }
  #projectGrpcRecord(response: wire.ReadResponse, expected: bigint): EncodedRecord {
    try { return projectGrpcReadResponse(toBinary(wire.ReadResponseSchema, response), expected) as EncodedRecord; }
    catch { throw new StreamError("invalid_response", "stream response contains an invalid record cursor or body"); }
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
    try { validateGrpcResponseIdentity("read_commit", toBinary(wire.CommittedEnvelopeSchema, response), id); }
    catch { throw new StreamError("invalid_response", "commit response names another commit"); }
    return this.#project("read_commit", toBinary(wire.CommittedEnvelopeSchema, response));
  }
}

/** Matches the Rust gRPC status mapping; unknown peer statuses remain unavailable. */
function providerError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  if (!(error instanceof ConnectError)) return error instanceof Error ? error : new StreamError("unavailable", String(error));
  let code = "unavailable";
  switch (error.code) {
    case Code.InvalidArgument: code = is_stream_error_code(error.rawMessage) ? error.rawMessage : "invalid_argument"; break;
    case Code.NotFound: code = operation === "read_commit" ? "commit_not_found" : "stream_not_found"; break;
    case Code.AlreadyExists: code = "destination_exists"; break;
    case Code.OutOfRange: code = "out_of_range"; break;
    case Code.PermissionDenied: case Code.Unauthenticated: code = "access_denied"; break;
    case Code.ResourceExhausted: code = "capacity_exhausted"; break;
    case Code.FailedPrecondition:
      if (is_stream_error_code(error.rawMessage)) code = error.rawMessage;
      break;
    case Code.Unimplemented: if (error.rawMessage === "unsupported_capability") code = "unsupported"; break;
  }
  const projected = publicHttpErrorCode(error.rawMessage, operation);
  if (projected !== undefined) code = projected;
  return new StreamError(code, error.rawMessage);
}
