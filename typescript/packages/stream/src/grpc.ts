import { checkedReadSequence } from "./record.js";
import type { SessionFollow, SessionData } from "../generated/proto/stream/v1/session_pb.js";
import { credentialProvider, credentialStream, resolveCredential, type CredentialOptions, type CredentialProvider } from "@acyclic-labs/actors/credential";
import { StreamSessions } from "./session.js";
import { SessionQueue } from "./session-queue.js";
import { StreamSessionService, type SessionFrame } from "../generated/proto/stream/v1/session_pb.js";
import { connect as connectTls, rootCertificates } from "node:tls";
import { createClient, ConnectError, Code, type Interceptor, type CallOptions } from "@connectrpc/connect";
import { createDeadlineSignal, createLinkedAbortController } from "@connectrpc/connect/protocol";
import { create, fromBinary, toBinary, type MessageInitShape } from "@bufbuild/protobuf";

import * as wire from "../generated/proto/stream/v1/stream_pb.js";
import { grpcErrorCode, projectMemoryResponse } from "../generated/wasm/acyclic_stream_wasm.js";
import { validateAppend } from "./client.js";
import { encodeHttpRequest, checkedRecord, normalizeWireCommitBytes, readResponseRecords, validateWireRequest, wireAppendRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { StreamError, type StreamFailureCode } from "./types.js";
import type { StreamProvider, AppendOptions, AppendResult, ForkOptions, ForkReceipt, ReadOptions, FollowOptions, EncodedRecord, ChildrenPageRequest, ChildrenPage, ProviderCommitRequest, CommitOptions, CommitResult, CommitId, CommittedEnvelope, IdempotencyKey, IdempotencyObservation, Revision } from "./types.js";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { StreamService } from "../generated/proto/stream/v1/stream_pb.js";
import { observeInterceptors, resolveObserver, type AcyclicObserver } from "@acyclic-labs/actors/observe";

export interface StreamGrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
  readonly observer?: AcyclicObserver;
  /** Translating servers acknowledge their own downstream receipt. */
  readonly acknowledgeDeliveries?: boolean;
  /** Implicit causal authority selected from an authenticated receipt; explicit minima remain intact. */
  readonly revisionScope?: Uint8Array;
}

/** Virtual follows support cancellation/deadlines; metadata belongs to the shared session. */
export type StreamFollowCallOptions = Pick<CallOptions, "signal" | "timeoutMs"> & {
  [Option in Exclude<keyof CallOptions, "signal" | "timeoutMs">]?: never;
};
type Follow = (request: MessageInitShape<typeof wire.FollowRequestSchema>,
  options?: StreamFollowCallOptions) => AsyncIterable<wire.ReadResponse>;


/** Stream v1 client; virtual follow accepts only signal and timeoutMs call options. */
export function createStreamGrpcClient(options: StreamGrpcOptions) {
  const { client, sessions } = streamGrpc(options);
  const follow: Follow = async function* (request, callOptions) {
    const { signal, timeoutMs } = callOptions ?? {};
    const { path, from, filter, whenAbsent } = create(wire.FollowRequestSchema, structuredClone(request));
    await validateWireRequest({ kind: "follow", path, from });
    const deadline = createDeadlineSignal(timeoutMs === undefined || timeoutMs <= 0 ? undefined : timeoutMs);
    const controller = createLinkedAbortController(deadline.signal, signal);
    try {
      if (whenAbsent !== wire.WhenAbsent.NOT_FOUND && whenAbsent !== wire.WhenAbsent.WAIT) throw new StreamError("invalid_argument", "invalid absence policy");
      for await (const record of sessions.follow(path, { from, signal: controller.signal, whenAbsent: whenAbsent === wire.WhenAbsent.WAIT ? "wait" : "not_found",
        ...(filter === undefined ? {} : { filter }) })) {
        const data = toBinary(wire.RecordBatchSchema, create(wire.RecordBatchSchema, { records: [record] }));
        yield create(wire.ReadResponseSchema, { codec: wire.Codec.NONE, data, decodedLength: BigInt(data.length) });
      }
      if (controller.signal.aborted) throw ConnectError.from(controller.signal.reason, Code.Canceled);
    } finally { controller.abort(); deadline.cleanup(); }
  };
  return { ...client, follow };
}

function createStreamGrpcTransport(options: Omit<StreamGrpcOptions, keyof CredentialOptions>, credential: CredentialProvider) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  const maximum = options.maximumMessageBytes ?? 16 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    // Subscribe before resolution so a quiet physical session cannot miss a
    // source event between admitting its lease and starting the HTTP/2 call.
    const watch = request.stream ? credential.watch?.() : undefined;
    const controller = request.stream ? createLinkedAbortController(request.signal) : undefined;
    const signal = controller?.signal ?? request.signal;
    const sourceChanged = () => controller?.abort(new ConnectError("credential source changed or unavailable", Code.PermissionDenied));
    const cleanup = () => { watch?.changed.removeEventListener("abort", sourceChanged); watch?.close(); controller?.abort(); };
    watch?.changed.addEventListener("abort", sourceChanged, { once: true });
    if (watch?.changed.aborted) sourceChanged();
    const rejected = (error: unknown) => error instanceof ConnectError && error.code === Code.Unauthenticated;
    const failure = (error: unknown) => watch?.changed.aborted && !request.signal.aborted
      ? new ConnectError("credential source changed or unavailable", Code.PermissionDenied) : error;
    let retained = false;
    let lease: Awaited<ReturnType<typeof resolveCredential>> | undefined;
    try {
      const admitted = await resolveCredential(credential, signal);
      lease = admitted;
      const custody = { ...admitted, check() {
        request.signal.throwIfAborted();
        try { admitted.check(); }
        catch { throw new ConnectError("credential source changed or expired", Code.PermissionDenied); }
      } };
      request.header.set("authorization", `Bearer ${admitted.token()}`);
      custody.check();
      const response = await next(request.stream
        ? { ...request, signal, message: credentialStream(request.message, custody, () => false) }
        : request);
      custody.check();
      if (!response.stream) return response;
      const message = async function* () {
        try { yield* credentialStream(response.message, custody, rejected); }
        catch (error) { throw failure(error); }
        finally { cleanup(); }
      };
      retained = true;
      return { ...response, message: message() };
    }
    catch (error) {
      if (rejected(error)) lease?.reject();
      throw failure(error);
    }
    finally { if (!retained) cleanup(); }
  };
  return createGrpcTransport({ baseUrl: endpoint.href, interceptors: observeInterceptors([authenticate], resolveObserver(options.observer), "stream"), readMaxBytes: maximum, writeMaxBytes: maximum, nodeOptions: {
    ...(options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] }),
    createConnection(authority, tlsOptions) {
      const address = new URL(authority);
      const socket = connectTls({ ...tlsOptions, host: address.hostname.replace(/^\[|\]$/g, ""),
        port: Number(address.port || 443), servername: address.hostname, ALPNProtocols: ["h2"], rejectUnauthorized: true });
      socket.setNoDelay(true);
      return socket;
    },
  } });
}

function streamGrpc(options: StreamGrpcOptions) {
    const credential = credentialProvider(options);
    const endpoint = options.endpoint;
    const caCertificate = options.caCertificate;
    const maximumMessageBytes = options.maximumMessageBytes;
    const observer = options.observer;
    const acknowledgeDeliveries = options.acknowledgeDeliveries;
    const transportOptions = {
      endpoint,
      ...(caCertificate === undefined ? {} : { caCertificate }),
      ...(maximumMessageBytes === undefined ? {} : { maximumMessageBytes }),
      ...(observer === undefined ? {} : { observer }),
    };
    const home = createStreamGrpcTransport(transportOptions, credential);
    const transports = new Map([[new URL(endpoint).href, home]]);
    const client = createClient(StreamService, home);

    const sessions = new StreamSessions(async (node, signal) => {
      const nodeEndpoint = new URL(node?.endpoint ?? endpoint).href;
      let transport = transports.get(nodeEndpoint);
      if (transport === undefined) {
        transport = createStreamGrpcTransport({ ...transportOptions, endpoint: nodeEndpoint }, credential);
        transports.set(nodeEndpoint, transport);
      }
      const input = new SessionQueue<SessionFrame>(wire.StreamLimit.MAX_ITEMS);
      const client = createClient(StreamSessionService, transport);
      const incoming = async function* () {
        try { yield* client.session(input, { signal }); }
        catch (error) { throw sessionTransportError(error); }
      };
      return { incoming: incoming(), send: (frame, writeSignal) => input.send(frame, writeSignal), close: () => input.end() };
    }, acknowledgeDeliveries !== false, options.revisionScope);
    return { client, sessions };
}

/** Existing Stream provider API over native HTTP/2 gRPC in Node and Bun. */
export class GrpcStreamProvider implements StreamProvider {
  readonly #client: ReturnType<typeof streamGrpc>["client"];
  readonly #acknowledgeDeliveries: boolean;
  readonly #sessions: StreamSessions;
  constructor(options: StreamGrpcOptions) {
    const { client, sessions } = streamGrpc(options);
    this.#client = client;
    this.#sessions = sessions;
    this.#acknowledgeDeliveries = options.acknowledgeDeliveries !== false;
  }
  async #call<T>(operation: string, call: () => Promise<T>): Promise<T> {
    try { return await call(); } catch (error) {
      const failure = providerError(error, operation);
      throw failure;
    }
  }
  #project<T>(operation: string, bytes: Uint8Array): T {
    try { return projectMemoryResponse(operation, bytes) as T; }
    catch { throw new StreamError("invalid_response", `invalid ${operation} response`); }
  }
  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const request = fromBinary(wire.InspectIdempotencyRequestSchema, wireInspectIdempotencyRequest(key));
    const response = await this.#call("inspect_idempotency", () => this.#client.inspectIdempotency(request));
    if (response.observation !== undefined && !sameBytes(response.observation.idempotencyKey, key)) throw new StreamError("invalid_response", "idempotency response names another retry identity");
    const observation: IdempotencyObservation | undefined = this.#project("inspect_idempotency", toBinary(wire.InspectIdempotencyResponseSchema, response));
    // A recovered write floors later reads exactly like its lost reply would have.
    this.#sessions.observe(observation?.revision);
    return observation;
  }
  async heads(options: import("./types.js").HeadsRequest): Promise<import("./types.js").Heads> {
    return this.#sessions.heads(options);
  }

  async head(path: string): Promise<import("./types.js").PathHead> {
    const request = { kind: "head" as const, path };
    await validateWireRequest(request);
    const response = await this.#call("head", () => this.#client.head(fromBinary(wire.HeadRequestSchema, wireRequest(request))));
    return this.#project("head", toBinary(wire.PathHeadSchema, response));
  }
  async tail(path: string): Promise<bigint> {
    await validateWireRequest({ kind: "tail", path });
    const atLeast = this.#sessions.revisionFloor();
    const request = wireRequest({ kind: "tail", path, atLeast });
    const response = await this.#call("tail", () => this.#client.tail(fromBinary(wire.TailRequestSchema, request)));
    this.#sessions.served(atLeast, response.applied);
    return response.tail;
  }
  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = options === undefined ? undefined : structuredClone(options);
    await validateAppend(path, records, authored);
    const request = fromBinary(wire.AppendRequestSchema, wireAppendRequest(path, records, authored));
    const response = await this.#call("append", () => this.#client.append(request));
    const result = this.#project<AppendResult>("append", toBinary(wire.AppendResponseSchema, response));
    if (result.ok) this.#sessions.observe(result.revision);
    return result;
  }
  async fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> {
    const request = { kind: "fork" as const, source, destination, ...(options === undefined ? {} : { options: structuredClone(options) }) };
    await validateWireRequest(request);
    const response = await this.#call("fork", () => this.#client.fork(fromBinary(wire.ForkRequestSchema, wireRequest(request))));
    const receipt = this.#project<ForkReceipt>("fork", toBinary(wire.ForkReceiptSchema, response));
    this.#sessions.observe(receipt.revision);
    return receipt;
  }
  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const request = { kind: "read" as const, path, ...structuredClone(options) };
    await validateWireRequest(request);
    const controller = new AbortController();
    try {
      const atLeast = this.#sessions.revisionFloor();
      const input = wireRequest({ ...request, atLeast });
      yield* this.#frames("read", this.#client.read(fromBinary(wire.ReadRequestSchema, input), { signal: controller.signal }), request.from, request.limit, undefined, atLeast, request.key);
    } finally { controller.abort(); }
  }
  async registerCredential(slot: number, credential: Uint8Array): Promise<void> {
    await this.#sessions.registerCredential(slot, credential);
  }
  async *followData(follow: SessionFollow, options: FollowOptions): AsyncIterable<SessionData> {
    const authored = structuredClone(follow);
    yield* this.#sessions.followData(authored, options);
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const authored = { ...options, ...(options.filter === undefined ? {} : { filter: structuredClone(options.filter) }) };
    await validateWireRequest({ kind: "follow", path, from: authored.from });
    yield* this.#sessions.follow(path, authored);
  }
  async *#frames(operation: string, frames: AsyncIterable<wire.ReadResponse>, next: bigint,
    remaining?: number, signal?: AbortSignal, atLeast?: readonly Revision[], key?: import("./types.js").ReadKeySeek): AsyncIterable<EncodedRecord> {
    // The first frame carries the read's applied revision; it must prove the minimum sent.
    let minimum = atLeast;
    try {
      for await (const response of frames) {
        if (signal?.aborted) return;
        if (minimum !== undefined) { this.#sessions.served(minimum, response.applied); minimum = undefined; }
        else this.#sessions.observe(response.applied);
        const values = readResponseRecords(response);
        if (remaining !== undefined && values.length > remaining) throw new StreamError("invalid_response", "read exceeds requested limit");
        const records = values.map(value => {
          const record = checkedRecord(value, checkedReadSequence(value, next, key));
          next = record.sequence + 1n;
          return record;
        });
        if (remaining !== undefined) remaining -= records.length;
        for (const record of records) {
          if (signal?.aborted) return;
          yield record;
        }
      }
      // A reply that ends before its first frame proves no minimum.
      if (minimum !== undefined && !signal?.aborted) this.#sessions.served(minimum, undefined);
    } catch (error) {
      if (signal?.aborted) return;
      const failure = providerError(error, operation);
      throw failure;
    }
  }

  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const authored = { kind: "children_page" as const,
      ...(request.tailStream === undefined ? {} : { tailStream: request.tailStream }), limit: request.limit,
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
    const result = this.#project<CommitResult>("commit", toBinary(wire.CommitResponseSchema, response));
    if (result.ok) this.#sessions.observe(result.revision);
    return result;
  }
  async readCommit(id: CommitId): Promise<CommittedEnvelope> {
    const request = fromBinary(wire.ReadCommitRequestSchema, wireReadCommitRequest(id));
    const response = await this.#call("read_commit", () => this.#client.readCommit(request));
    if (!sameBytes(response.commitId, id)) throw new StreamError("invalid_response", "commit response names another commit");
    return this.#project("read_commit", toBinary(wire.CommittedEnvelopeSchema, response));
  }
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
    case Code.Unimplemented: if (reason === "unsupported_capability") code = "unsupported"; break;
    case Code.FailedPrecondition:
      if (reason === "hierarchy_changed" || reason === "idempotency_mismatch" || reason === "prefix_not_retained" || reason === "deadline_elapsed" || reason === "revision_foreign") code = reason;
      break;
  }
  if (code === "prefix_not_retained" && operation === "commit") code = "invalid_argument";
  return new StreamError(code, reason);
}

/** Platform callbacks use Rust codes; public aliases never enter the session engine. */
function sessionTransportError(error: unknown): unknown {
  if (!(error instanceof ConnectError)) return error;
  return Object.assign(new Error(error.rawMessage), { code: grpcErrorCode(error.code, error.rawMessage) });
}
