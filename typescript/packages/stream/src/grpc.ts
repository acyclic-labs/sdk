import { credentialInterceptor } from "@acyclic-labs/auth/credential-connect";
import { refusalFromGrpcHeaders } from "@acyclic-labs/auth/grpc-refusal";
import type { Opened } from "./opened.js";
import type { SessionFollow, SessionData } from "../generated/proto/stream/v1/session_pb.js";
import { credentialProvider, type CredentialOptions, type CredentialProvider } from "@acyclic-labs/actors/credential";
import { StreamSessions } from "./session.js";
import { SessionQueue } from "./session-queue.js";
import { StreamSessionService, SessionFrameSchema, type SessionFrame } from "../generated/proto/stream/v1/session_pb.js";
import { connect as connectTls, rootCertificates } from "node:tls";
import { AsyncLocalStorage } from "node:async_hooks";
import { constants, type ClientHttp2Stream, type Http2Session, type IncomingHttpHeaders } from "node:http2";
import { createClient, ConnectError, Code, type CallOptions, type Interceptor } from "@connectrpc/connect";
import { createDeadlineSignal, createLinkedAbortController } from "@connectrpc/connect/protocol";
import { create, fromBinary, toBinary, type MessageInitShape } from "@bufbuild/protobuf";

import * as wire from "../generated/proto/stream/v1/stream_pb.js";
import { grpcStatusError, projectMemoryResponse } from "../generated/wasm/acyclic_stream_wasm.js";
import { validateAppend } from "./client.js";
import { encodeHttpRequest, normalizeWireCommitBytes, validateWireRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { StreamError, type StreamRefusal } from "./types.js";
import { streamError } from "./wasm-error.js";
import type { StreamProvider, AppendOptions, AppendResult, ForkOptions, ForkReceipt, ReadOptions, FollowOptions, ChildrenPageRequest, ChildrenPage, ProviderCommitRequest, CommitOptions, CommitResult, CommitId, CommittedEnvelope, IdempotencyKey, IdempotencyObservation } from "./types.js";
import { createGrpcTransport, Http2SessionManager, type GrpcTransportOptions } from "@connectrpc/connect-node";
import { StreamService } from "../generated/proto/stream/v1/stream_pb.js";
import { observeInterceptors, resolveObserver, type AcyclicObserver } from "@acyclic-labs/actors/observe";

export interface StreamGrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  /** Optional caller message capacity; omission retains the wire encoding maximum. */
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
  const maximum = options.maximumMessageBytes;
  if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 1)) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate = credentialInterceptor(credential);
  const transportOptions: GrpcTransportOptions = { baseUrl: endpoint.href, interceptors: observeInterceptors([authenticate], resolveObserver(options.observer), "stream"), nodeOptions: {
    ...(options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] }),
    createConnection(authority, tlsOptions) {
      const address = new URL(authority);
      const socket = connectTls({ ...tlsOptions, host: address.hostname.replace(/^\[|\]$/g, ""),
        port: Number(address.port || 443), servername: address.hostname, ALPNProtocols: ["h2"], rejectUnauthorized: true });
      socket.setNoDelay(true);
      return socket;
    },
  } };
  if (maximum !== undefined) {
    transportOptions.readMaxBytes = maximum;
    transportOptions.writeMaxBytes = maximum;
  }
  const manager = new Http2SessionManager(endpoint, undefined, transportOptions.nodeOptions);
  const current = new AsyncLocalStorage<PeerResponse>();
  const observe: Interceptor = next => request => {
    const peer: PeerResponse = { statusReceived: false };
    const failed = (error: unknown): never => {
      if (error instanceof ConnectError) {
        Object.assign(error, {
          [peerRefusal]: refusalFromGrpcHeaders(error, [peer.header, peer.trailer]),
          [peerInterrupted]: !peer.statusReceived
            && peer.connection !== undefined && (peer.connection.closed || peer.connection.destroyed),
        });
      }
      throw error;
    };
    return current.run(peer, async () => {
      try {
        const response = await next(request);
        if (!response.stream) { releasePeer(peer); return response; }
        const messages = response.message;
        return { ...response, message: (async function* () {
          try { yield* messages; } catch (error) { failed(error); }
          finally { releasePeer(peer); }
        })() };
      } catch (error) {
        try { return failed(error); } finally { releasePeer(peer); }
      }
    });
  };
  transportOptions.interceptors = [observe, ...(transportOptions.interceptors ?? [])];
  const observedManager = {
    authority: manager.authority,
    async request(...args: Parameters<Http2SessionManager["request"]>) {
      const peer = current.getStore();
      const request = await manager.request(...args);
      if (peer !== undefined) {
        peer.connection = request.session;
        request.once("response", (headers: IncomingHttpHeaders, _flags, raw: string[]) => {
          peer.header = raw;
          peer.statusReceived ||= headers["grpc-status"] !== undefined;
        });
        request.once("trailers", (headers: IncomingHttpHeaders, _flags, raw: string[]) => {
          peer.trailer = raw;
          peer.statusReceived ||= headers["grpc-status"] !== undefined;
        });
      }
      return request;
    },
    notifyResponseByteRead(request: ClientHttp2Stream) { manager.notifyResponseByteRead(request); },
  };
  transportOptions.sessionManager = observedManager;
  const transport = createGrpcTransport(transportOptions);
  return {
    transport,
    session(input: AsyncIterable<SessionFrame>, signal: AbortSignal) {
      // Retain this RPC's original connection, even if the shared manager has
      // already opened its successor. A peer status always remains terminal.
      let connection: Http2Session | undefined;
      let rpc: ClientHttp2Stream | undefined;
      let rpcCancelled = false;
      let closed: Promise<void> | undefined;
      let statusReceived = false;
      const receiveStatus = (headers: IncomingHttpHeaders): void => {
        statusReceived ||= headers["grpc-status"] !== undefined;
      };
      const transport = createGrpcTransport({ ...transportOptions, sessionManager: {
        authority: manager.authority,
        async request(...args: Parameters<Http2SessionManager["request"]>) {
          const request = await observedManager.request(...args);
          connection = request.session;
          rpc = request;
          // Some runtimes end the reader before publishing the reset. Close
          // completion belongs to this original RPC, never its successor.
          closed = new Promise<void>(resolve => request.once("close", () => {
            rpcCancelled = request.rstCode === constants.NGHTTP2_CANCEL;
            resolve();
          }));
          request.once("response", receiveStatus);
          request.once("trailers", receiveStatus);
          return request;
        },
        notifyResponseByteRead(request) { manager.notifyResponseByteRead(request); },
      } });
      return {
        incoming: createClient(StreamSessionService, transport).session(input, { signal }),
        async fail(error: unknown): Promise<never> {
          if (!signal.aborted && !statusReceived && rpc !== undefined) {
            // The peer's close carries its final reset code. Local cleanup
            // would replace it; only caller cancellation may close this RPC.
            const request = rpc;
            const abort = (): void => request.close(constants.NGHTTP2_CANCEL);
            signal.addEventListener("abort", abort, { once: true });
            try {
              if (signal.aborted) abort();
              await closed;
            } finally { signal.removeEventListener("abort", abort); }
          }
          const interrupted = !signal.aborted && !statusReceived
            && ((connection !== undefined && (connection.closed || connection.destroyed))
              || rpcCancelled || rpc?.rstCode === constants.NGHTTP2_CANCEL);
          throw sessionTransportError(error, interrupted);
        },
      };
    },
  };
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
    const client = createClient(StreamService, home.transport);

    const sessions = new StreamSessions(async (node, signal) => {
      const nodeEndpoint = new URL(node?.endpoint ?? endpoint).href;
      let transport = transports.get(nodeEndpoint);
      if (transport === undefined) {
        transport = createStreamGrpcTransport({ ...transportOptions, endpoint: nodeEndpoint }, credential);
        transports.set(nodeEndpoint, transport);
      }
      const input = new SessionQueue<SessionFrame>(wire.StreamLimit.MAX_ITEMS);
      const session = transport.session(input, signal);
      const incoming = async function* () {
        try { for await (const frame of session.incoming) yield toBinary(SessionFrameSchema, frame); }
        catch (error) { await session.fail(error); }
      };
      return { incoming: incoming(), send: (frame, writeSignal) => input.send(fromBinary(SessionFrameSchema, frame), writeSignal), close: () => input.end() };
    }, acknowledgeDeliveries !== false, options.revisionScope);
    return { client, sessions };
}

/** Existing Stream provider API over native HTTP/2 gRPC in Node and Bun. */
export class GrpcStreamProvider implements StreamProvider {
  readonly #client: ReturnType<typeof streamGrpc>["client"];
  readonly #sessions: StreamSessions;
  constructor(options: StreamGrpcOptions) {
    const { client, sessions } = streamGrpc(options);
    this.#client = client;
    this.#sessions = sessions;
  }
  async #call<T>(operation: string, call: () => Promise<T>): Promise<T> {
    try { return await call(); } catch (error) {
      const failure = providerError(error, operation);
      throw failure;
    }
  }
  #project<T>(operation: string, bytes: Uint8Array, childrenRequest?: Uint8Array): T {
    try { return projectMemoryResponse(operation, bytes, childrenRequest) as T; }
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
    const input = await validateWireRequest(request);
    const response = await this.#call("head", () => this.#client.head(fromBinary(wire.HeadRequestSchema, input)));
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
  async append(path: string, values: readonly Uint8Array[], options: AppendOptions): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = structuredClone(options);
    const input = await validateAppend(path, records, authored);
    const request = fromBinary(wire.AppendRequestSchema, input);
    const response = await this.#call("append", () => this.#client.append(request));
    const result = this.#project<AppendResult>("append", toBinary(wire.AppendResponseSchema, response));
    if (result.ok) this.#sessions.observe(result.revision);
    return result;
  }
  async fork(source: string, destination: string, options: ForkOptions): Promise<ForkReceipt> {
    const request = { kind: "fork" as const, source, destination, options: structuredClone(options) };
    const input = await validateWireRequest(request);
    const response = await this.#call("fork", () => this.#client.fork(fromBinary(wire.ForkRequestSchema, input)));
    const receipt = this.#project<ForkReceipt>("fork", toBinary(wire.ForkReceiptSchema, response));
    this.#sessions.observe(receipt.revision);
    return receipt;
  }
  read(path: string, options: ReadOptions): Opened {
    return this.#sessions.read(path, options);
  }
  async registerCredential(slot: number, credential: Uint8Array): Promise<void> {
    await this.#sessions.registerCredential(slot, credential);
  }
  async *followData(follow: SessionFollow, options: FollowOptions): AsyncIterable<SessionData> {
    const authored = structuredClone(follow);
    yield* this.#sessions.followData(authored, options);
  }
  follow(path: string, options: FollowOptions): Opened {
    const authored = { ...options, ...(options.filter === undefined ? {} : { filter: structuredClone(options.filter) }) };
    return this.#sessions.follow(path, authored);
  }

  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const authored = { kind: "children_page" as const,
      ...(request.tailStream === undefined ? {} : { tailStream: request.tailStream }), limit: request.limit,
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion.slice() }),
    };
    const input = await validateWireRequest(authored);
    const response = await this.#call("children_page", () => this.#client.childrenPage(fromBinary(wire.ChildrenPageRequestSchema, input)));
    return this.#project("children_page", toBinary(wire.ChildrenPageResponseSchema, response), input);
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
/** Rust owns status categories and retention of original member diagnostics. */
function grpcStatus(error: ConnectError): Error {
  const refusal = grpcRefusal(error);
  return Object.assign(grpcStatusError(refusal.status, refusal.message, []) as Error, { refusal });
}

/** Public Stream failure for a gRPC call. */
function providerError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  if (!(error instanceof ConnectError)) return error instanceof Error ? error : new StreamError("unavailable", String(error));
  const failure = streamError(grpcStatus(error), operation);
  return failure instanceof StreamError && failure.code === "capacity"
    ? new StreamError("capacity_exhausted", failure.message, failure.status, failure.refusal)
    : failure;
}

const peerRefusal = Symbol("original peer refusal");
const peerInterrupted = Symbol("original connection interruption");
type PeerError = ConnectError & {
  [peerRefusal]?: StreamRefusal;
  [peerInterrupted]?: boolean;
};
interface PeerResponse {
  connection?: Http2Session | undefined;
  statusReceived: boolean;
  header?: readonly string[];
  trailer?: readonly string[];
}

function releasePeer(peer: PeerResponse): void {
  delete peer.header;
  delete peer.trailer;
  delete peer.connection;
}

function grpcRefusal(error: PeerError): StreamRefusal {
  return error[peerRefusal] ?? refusalFromGrpcHeaders(error, []);
}

/** Platform callbacks use Rust codes; public aliases never enter the session engine. */
function sessionTransportError(error: unknown, interrupted: boolean): unknown {
  if (!(error instanceof ConnectError)) return error;
  return Object.assign(grpcStatus(error), {
    cause: error, transient: interrupted,
  });
}
