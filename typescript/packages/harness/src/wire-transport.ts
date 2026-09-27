import { create, fromBinary, fromJsonString, toBinary, toJsonString } from "@bufbuild/protobuf";
/** Injectable HTTP function; host-specific fetch extras are not required of test or custom transports. */
export type HttpFetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
import {
  AdmissionSchema,
  AdmissionState,
  CancelRequestSchema,
  CancelResponseSchema,
  ClientFrameSchema,
  CommandEnvelopeSchema,
  ErrorCode,
  ErrorSchema,
  ObserveRequestSchema,
  OperationStatusSchema,
  ResumeRequestSchema,
  ServerFrameSchema,
  type Admission,
  type Authority,
  type CommandEnvelope,
  type CancelRequest,
  type CancelResponse,
  type ClientFrame,
  type Delivery,
  type ObserveRequest,
  type OperationStatus,
  type ResumeRequest,
  type Scope,
  type ServerFrame,
} from "../generated/proto/harness/v2/harness_pb.js";
import {
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  type HandshakeRequest,
  type HandshakeResponse,
} from "../generated/proto/protocol/v1/protocol_pb.js";
import { TerminalAdmissionError } from "./client.js";
import { NativeContracts } from "./native-contracts.js";

export interface WireConnection extends AsyncIterable<Delivery> {
  send(command: CommandEnvelope): Promise<void>;
  observe(request: ObserveRequest): Promise<OperationStatus>;
  cancel(request: CancelRequest): Promise<CancelResponse>;
  close(): Promise<void> | void;
}

export interface WireTransport {
  connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection>;
}

/** Typed control handle over the exact wire connection used for replay and submit. */
export class WireOperationHandle {
  constructor(
    readonly connection: WireConnection,
    readonly owner: Authority,
    readonly operationId: string,
    readonly scope: Scope,
  ) {}

  observe(): Promise<OperationStatus> {
    return this.connection.observe(create(ObserveRequestSchema, {
      owner: this.owner,
      operationId: this.operationId,
      scope: this.scope,
    }));
  }

  cancel(idempotencyKey: string, recursive = false): Promise<CancelResponse> {
    return this.connection.cancel(create(CancelRequestSchema, {
      owner: this.owner,
      operationId: this.operationId,
      scope: this.scope,
      idempotencyKey,
      recursive,
    }));
  }
}

export interface EmbeddedWireApi {
  handshake(request: HandshakeRequest): Promise<HandshakeResponse>;
  replay(resume: ResumeRequest, signal?: AbortSignal): AsyncIterable<Delivery>;
  submit(command: CommandEnvelope): Promise<void>;
  observe(request: ObserveRequest): Promise<OperationStatus>;
  cancel(request: CancelRequest): Promise<CancelResponse>;
}

export class EmbeddedWireTransport implements WireTransport {
  constructor(readonly api: EmbeddedWireApi, readonly negotiation: HandshakeRequest) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = withResumeProtocol(resume, this.negotiation);
    await validateResume(resume);
    await validateHandshake(this.negotiation, await this.api.handshake(this.negotiation));
    const deliveries = this.api.replay(resume, signal);
    return {
      send: async command => {
        command = withProtocol(command, this.negotiation);
        await validateCommand(command);
        return this.api.submit(command);
      },
      observe: async request => {
        request = withObserveProtocol(request, this.negotiation);
        await validateObserve(request);
        return validateStatus(request, this.negotiation, await this.api.observe(request));
      },
      cancel: async request => {
        request = withCancelProtocol(request, this.negotiation);
        await validateCancel(request);
        const response = await this.api.cancel(request);
        await validateCancelIdentity(request, this.negotiation, response);
        return response;
      },
      close() {},
      [Symbol.asyncIterator]: () => deliveries[Symbol.asyncIterator](),
    };
  }
}

export interface JsonlChannel extends AsyncIterable<string> {
  write(line: string): Promise<void>;
  close(): Promise<void> | void;
}

export class JsonlWireTransport implements WireTransport {
  constructor(
    readonly open: (signal?: AbortSignal) => Promise<JsonlChannel>,
    readonly negotiation: HandshakeRequest,
  ) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = withResumeProtocol(resume, this.negotiation);
    await validateResume(resume);
    const channel = await this.open(signal);
    const source = channel[Symbol.asyncIterator]();
    await channel.write(clientFrameJson({ case: "handshake", value: this.negotiation }));
    const first = await source.next();
    if (first.done) throw new WireError(ErrorCode.UNSUPPORTED, "missing handshake response");
    await validateHandshake(this.negotiation, handshakeFromFrame(parseServerFrame(first.value)));
    const connection = new FramedConnection(source, line => channel.write(line), () => channel.close(), this.negotiation);
    await channel.write(clientFrameJson({ case: "resume", value: resume }));
    return connection;
  }
}

export type WebSocketFactory = (url: string, protocols: string[]) => WebSocket;

export class WebSocketWireTransport implements WireTransport {
  constructor(
    readonly url: string,
    readonly negotiation: HandshakeRequest,
    readonly factory: WebSocketFactory = (url, protocols) => new WebSocket(url, protocols),
  ) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = withResumeProtocol(resume, this.negotiation);
    await validateResume(resume);
    const socket = this.factory(this.url, ["acyclic.harness.v2"]);
    const incoming = new AsyncQueue<string>();
    const onMessage = (event: MessageEvent) => {
      void websocketText(event.data).then(value => incoming.push(value), error => incoming.fail(error));
    };
    const onClose = () => incoming.fail(new WireError(ErrorCode.INDETERMINATE, "WebSocket closed"));
    const onError = () => incoming.fail(new WireError(ErrorCode.INDETERMINATE, "WebSocket failed"));
    socket.addEventListener("message", onMessage);
    socket.addEventListener("close", onClose);
    socket.addEventListener("error", onError);
    await waitForOpen(socket, signal);
    const source = incoming[Symbol.asyncIterator]();
    socket.send(clientFrameJson({ case: "handshake", value: this.negotiation }));
    const first = await source.next();
    if (first.done) throw new WireError(ErrorCode.UNSUPPORTED, "missing handshake response");
    await validateHandshake(this.negotiation, handshakeFromFrame(parseServerFrame(first.value)));
    const close = () => {
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
      socket.removeEventListener("error", onError);
      socket.close(1000, "client closed");
      incoming.end();
    };
    const connection = new FramedConnection(source, async line => socket.send(line), close, this.negotiation);
    socket.send(clientFrameJson({ case: "resume", value: resume }));
    return connection;
  }
}

export class HttpSseWireTransport implements WireTransport {
  constructor(
    readonly baseUrl: string,
    readonly negotiation: HandshakeRequest,
    readonly fetcher: HttpFetcher = fetch,
  ) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = withResumeProtocol(resume, this.negotiation);
    await validateResume(resume);
    const handshakeResponse = await this.fetcher(new URL("v2/harness/handshake", withSlash(this.baseUrl)), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: toJsonString(HandshakeRequestSchema, this.negotiation),
      ...(signal === undefined ? {} : { signal }),
    });
    if (!handshakeResponse.ok) throw new WireError(ErrorCode.UNSUPPORTED, "handshake failed");
    await validateHandshake(this.negotiation, fromJsonString(HandshakeResponseSchema, await handshakeResponse.text()));
    const response = await this.fetcher(new URL("v2/harness/replay", withSlash(this.baseUrl)), {
      method: "POST",
      headers: { accept: "text/event-stream", "content-type": "application/json" },
      body: toJsonString(ResumeRequestSchema, resume),
      ...(signal === undefined ? {} : { signal }),
    });
    if (!response.ok || response.body === null) throw new Error(`replay failed: ${response.status}`);
    const fetcher = this.fetcher;
    const baseUrl = this.baseUrl;
    const negotiation = this.negotiation;
    return {
      async send(command) {
        command = withProtocol(command, negotiation);
        await validateCommand(command);
        const submitted = await fetcher(new URL("v2/harness/commands", withSlash(baseUrl)), {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: toJsonString(CommandEnvelopeSchema, command),
          ...(signal === undefined ? {} : { signal }),
        });
        if (!submitted.ok) {
          try {
            const admission = fromJsonString(AdmissionSchema, await submitted.text());
            await validateAdmissionIdentity(command, admission);
            if (admission.state === AdmissionState.REJECTED) {
              throw new TerminalAdmissionError(admission.error?.message ?? "command was rejected");
            }
          } catch (error) {
            if (error instanceof TerminalAdmissionError) throw error;
          }
          throw new WireError(ErrorCode.INDETERMINATE, `command failed: ${submitted.status}`);
        }
        const admission = fromJsonString(AdmissionSchema, await submitted.text());
        await validateAdmissionIdentity(command, admission);
        if (admission.state === AdmissionState.REJECTED) {
          throw new TerminalAdmissionError(admission.error?.message ?? "command was rejected");
        }
        if (admission.state !== AdmissionState.ACCEPTED) {
          throw new WireError(
            admission.error?.code ?? ErrorCode.INDETERMINATE,
            admission.error?.message ?? "command outcome is indeterminate",
          );
        }
      },
      async observe(request) {
        request = withObserveProtocol(request, negotiation);
        await validateObserve(request);
        const observed = await fetcher(new URL("v2/harness/operations/observe", withSlash(baseUrl)), {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: toJsonString(ObserveRequestSchema, request),
          ...(signal === undefined ? {} : { signal }),
        });
        if (!observed.ok) throw await httpWireError(observed, "observe");
        return validateStatus(request, negotiation, fromJsonString(OperationStatusSchema, await observed.text()));
      },
      async cancel(request) {
        request = withCancelProtocol(request, negotiation);
        await validateCancel(request);
        const cancelled = await fetcher(new URL("v2/harness/operations/cancel", withSlash(baseUrl)), {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: toJsonString(CancelRequestSchema, request),
          ...(signal === undefined ? {} : { signal }),
        });
        if (!cancelled.ok) throw await httpWireError(cancelled, "cancel");
        const response = fromJsonString(CancelResponseSchema, await cancelled.text());
        await validateCancelIdentity(request, negotiation, response);
        return response;
      },
      async close() {},
      async *[Symbol.asyncIterator]() {
        for await (const data of sseData(response.body!)) {
          const frame = parseServerFrame(data);
          if (frame.frame.case === "delivery") yield frame.frame.value;
          else if (frame.frame.case === "error") {
            throw new WireError(frame.frame.value.code, frame.frame.value.message);
          }
        }
      },
    };
  }
}

export class GrpcWireTransport implements WireTransport {
  constructor(
    readonly negotiation: HandshakeRequest,
    readonly handshakeGrpc: (request: HandshakeRequest) => Promise<HandshakeResponse>,
    readonly connectGrpc: WireTransport["connect"],
  ) {}
  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = withResumeProtocol(resume, this.negotiation);
    await validateResume(resume);
    await validateHandshake(this.negotiation, await this.handshakeGrpc(this.negotiation));
    const connection = await this.connectGrpc(resume, signal);
    return {
      send: async command => {
        command = withProtocol(command, this.negotiation);
        await validateCommand(command);
        return connection.send(command);
      },
      observe: async request => {
        request = withObserveProtocol(request, this.negotiation);
        await validateObserve(request);
        return validateStatus(request, this.negotiation, await connection.observe(request));
      },
      cancel: async request => {
        request = withCancelProtocol(request, this.negotiation);
        await validateCancel(request);
        const response = await connection.cancel(request);
        await validateCancelIdentity(request, this.negotiation, response);
        return response;
      },
      close: () => connection.close(),
      [Symbol.asyncIterator]: () => connection[Symbol.asyncIterator](),
    };
  }
}

export class WireError extends Error {
  constructor(readonly code: ErrorCode, message: string) {
    super(message);
  }
}

class FramedConnection implements WireConnection {
  readonly #deliveries = new AsyncQueue<Delivery>();
  readonly #pending = new Map<string, { command: CommandEnvelope; resolve(): void; reject(error: unknown): void }>();
  readonly #observations = new Map<string, {
    request: ObserveRequest;
    resolve(status: OperationStatus): void;
    reject(error: unknown): void;
  }>();
  readonly #cancellations = new Map<string, {
    request: CancelRequest;
    resolve(response: CancelResponse): void;
    reject(error: unknown): void;
  }>();
  /** Reservations cover the async Rust validation window before a request is framed. */
  readonly #controls = new Set<string>();

  constructor(
    source: AsyncIterator<string>,
    readonly write: (line: string) => Promise<void>,
    readonly closeChannel: () => Promise<void> | void,
    readonly negotiation: HandshakeRequest,
  ) {
    void this.#pump(source);
  }

  async send(command: CommandEnvelope): Promise<void> {
    command = withProtocol(command, this.negotiation);
    await validateCommand(command);
    const operationId = command.operation?.operationId;
    if (!operationId) throw new TypeError("command operation identity is missing");
    if (this.#pending.has(operationId)) throw new Error("command admission is already pending");
    if (!command.operation?.idempotencyKey) throw new TypeError("command idempotency key is missing");
    const admission = new Promise<void>((resolve, reject) =>
      this.#pending.set(operationId, { command, resolve, reject }),
    );
    try {
      await this.write(clientFrameJson({ case: "command", value: command }));
    } catch (error) {
      this.#pending.delete(operationId);
      throw error;
    }
    return admission;
  }

  async observe(request: ObserveRequest): Promise<OperationStatus> {
    request = withObserveProtocol(request, this.negotiation);
    const operationId = request.operationId;
    if (!operationId) throw new TypeError("observe operation identity is missing");
    if (this.#controls.has(operationId)) {
      throw new Error("operation control request is already pending");
    }
    this.#controls.add(operationId);
    try {
      await validateObserve(request);
      if (!this.#controls.has(operationId)) {
        throw new WireError(ErrorCode.INDETERMINATE, "connection closed during operation control validation");
      }
      if (this.#observations.has(operationId) || this.#cancellations.has(operationId)) {
        throw new Error("operation control request is already pending");
      }
      const observed = new Promise<OperationStatus>((resolve, reject) =>
        this.#observations.set(operationId, { request, resolve, reject }),
      );
      await this.write(clientFrameJson({ case: "observe", value: request }));
      return observed;
    } catch (error) {
      this.#observations.delete(operationId);
      this.#controls.delete(operationId);
      throw error;
    }
  }

  async cancel(request: CancelRequest): Promise<CancelResponse> {
    request = withCancelProtocol(request, this.negotiation);
    const operationId = request.operationId;
    const idempotencyKey = request.idempotencyKey;
    if (!operationId || !idempotencyKey) throw new TypeError("cancel operation identity is missing");
    if (this.#controls.has(operationId)) {
      throw new Error("operation control request is already pending");
    }
    this.#controls.add(operationId);
    try {
      await validateCancel(request);
      if (!this.#controls.has(operationId)) {
        throw new WireError(ErrorCode.INDETERMINATE, "connection closed during operation control validation");
      }
      if (this.#cancellations.has(operationId) || this.#observations.has(operationId)) {
        throw new Error("operation control request is already pending");
      }
      const cancelled = new Promise<CancelResponse>((resolve, reject) =>
        this.#cancellations.set(operationId, { request, resolve, reject }),
      );
      await this.write(clientFrameJson({ case: "cancel", value: request }));
      return cancelled;
    } catch (error) {
      this.#cancellations.delete(operationId);
      this.#controls.delete(operationId);
      throw error;
    }
  }

  async close(): Promise<void> {
    await this.closeChannel();
    this.#fail(new WireError(ErrorCode.INDETERMINATE, "connection closed"));
  }

  [Symbol.asyncIterator](): AsyncIterator<Delivery> {
    return this.#deliveries[Symbol.asyncIterator]();
  }

  async #pump(source: AsyncIterator<string>): Promise<void> {
    try {
      for (;;) {
        const item = await source.next();
        if (item.done) break;
        const frame = parseServerFrame(item.value);
        if (frame.frame.case === "delivery") this.#deliveries.push(frame.frame.value);
        else if (frame.frame.case === "admission") {
          const id = frame.frame.value.operation?.operationId;
          const pending = id ? this.#pending.get(id) : undefined;
          if (!pending) throw new Error("admission has no matching command");
          this.#pending.delete(id!);
          try {
            await validateAdmissionIdentity(pending.command, frame.frame.value);
            if (frame.frame.value.state === AdmissionState.ACCEPTED) pending.resolve();
            else if (frame.frame.value.state === AdmissionState.REJECTED) {
              pending.reject(new TerminalAdmissionError(frame.frame.value.error?.message ?? "rejected"));
            } else {
              pending.reject(new WireError(ErrorCode.INDETERMINATE, "command outcome is indeterminate"));
            }
          } catch (error) {
            pending.reject(error);
          }
        } else if (frame.frame.case === "error") {
          const error = new WireError(frame.frame.value.code, frame.frame.value.message);
          const operationId = frame.frame.value.operationId;
          let correlated = false;
          const observation = this.#observations.get(operationId);
          if (observation) {
            this.#observations.delete(operationId);
            this.#controls.delete(operationId);
            observation.reject(error);
            correlated = true;
          }
          const cancellation = this.#cancellations.get(operationId);
          if (cancellation) {
            this.#cancellations.delete(operationId);
            this.#controls.delete(operationId);
            cancellation.reject(error);
            correlated = true;
          }
          if (!correlated) throw error;
        } else if (frame.frame.case === "status") {
          const operationId = frame.frame.value.operation?.operationId ?? "";
          const pending = this.#observations.get(operationId);
          if (!pending) throw new Error("operation status has no matching observation");
          const status = await validateStatus(pending.request, this.negotiation, frame.frame.value);
          this.#observations.delete(operationId);
          this.#controls.delete(operationId);
          pending.resolve(status);
        } else if (frame.frame.case === "cancellation") {
          const operationId = frame.frame.value.operation?.operationId;
          const pending = operationId ? this.#cancellations.get(operationId) : undefined;
          if (!pending) throw new Error("cancellation has no matching request");
          this.#cancellations.delete(operationId!);
          this.#controls.delete(operationId!);
          try {
            await validateCancelIdentity(pending.request, this.negotiation, frame.frame.value);
            pending.resolve(frame.frame.value);
          } catch (error) {
            pending.reject(error);
          }
        }
      }
      if (this.#pending.size > 0 || this.#observations.size > 0 || this.#cancellations.size > 0 || this.#controls.size > 0) {
        const error = new WireError(ErrorCode.INDETERMINATE, "connection ended before admission");
        this.#fail(error);
        return;
      }
      this.#deliveries.end();
    } catch (error) {
      this.#fail(error);
    }
  }

  #fail(error: unknown): void {
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
    for (const pending of this.#observations.values()) pending.reject(error);
    this.#observations.clear();
    for (const pending of this.#cancellations.values()) pending.reject(error);
    this.#cancellations.clear();
    this.#controls.clear();
    this.#deliveries.fail(error);
  }
}

function withProtocol(command: CommandEnvelope, negotiation: HandshakeRequest): CommandEnvelope {
  return create(CommandEnvelopeSchema, { ...command, protocol: negotiation.protocol });
}

function withResumeProtocol(resume: ResumeRequest, negotiation: HandshakeRequest): ResumeRequest {
  return create(ResumeRequestSchema, { ...resume, protocol: negotiation.protocol });
}

function withObserveProtocol(request: ObserveRequest, negotiation: HandshakeRequest): ObserveRequest {
  return create(ObserveRequestSchema, { ...request, protocol: negotiation.protocol });
}

function withCancelProtocol(request: CancelRequest, negotiation: HandshakeRequest): CancelRequest {
  return create(CancelRequestSchema, { ...request, protocol: negotiation.protocol });
}

class AsyncQueue<T> implements AsyncIterable<T> {
  readonly #values: T[] = [];
  readonly #waiters: Array<{ resolve(value: IteratorResult<T>): void; reject(error: unknown): void }> = [];
  #ended = false;
  #error: unknown;

  push(value: T): void {
    const waiter = this.#waiters.shift();
    if (waiter) waiter.resolve({ done: false, value });
    else this.#values.push(value);
  }
  end(): void {
    this.#ended = true;
    for (const waiter of this.#waiters.splice(0)) waiter.resolve({ done: true, value: undefined });
  }
  fail(error: unknown): void {
    this.#error = error;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
  }
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async () => {
        const value = this.#values.shift();
        if (value !== undefined) return { done: false, value };
        if (this.#error !== undefined) throw this.#error;
        if (this.#ended) return { done: true, value: undefined };
        return new Promise<IteratorResult<T>>((resolve, reject) => this.#waiters.push({ resolve, reject }));
      },
    };
  }
}

function clientFrameJson(frameValue: Exclude<ClientFrame["frame"], { case: undefined }>): string {
  const frame = create(ClientFrameSchema, { frame: frameValue });
  return `${toJsonString(ClientFrameSchema, frame)}\n`;
}

async function validateStatus(
  request: ObserveRequest,
  _negotiation: HandshakeRequest,
  status: OperationStatus,
): Promise<OperationStatus> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireStatus(toBinary(ObserveRequestSchema, request), toBinary(OperationStatusSchema, status)),
    ErrorCode.CONFLICT,
    "operation status identity mismatch",
  );
  return status;
}

async function validateCancelIdentity(
  request: CancelRequest,
  _negotiation: HandshakeRequest,
  response: CancelResponse,
): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireCancellation(toBinary(CancelRequestSchema, request), toBinary(CancelResponseSchema, response)),
    ErrorCode.CONFLICT,
    "cancellation identity mismatch",
  );
}

function parseServerFrame(value: string): ServerFrame {
  return fromJsonString(ServerFrameSchema, value.trim());
}

function handshakeFromFrame(frame: ServerFrame): HandshakeResponse {
  if (frame.frame.case !== "handshake") {
    throw new WireError(ErrorCode.UNSUPPORTED, "handshake must be the first server frame");
  }
  return frame.frame.value;
}

async function validateAdmissionIdentity(command: CommandEnvelope, admission: Admission): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireAdmission(toBinary(CommandEnvelopeSchema, command), toBinary(AdmissionSchema, admission)),
    ErrorCode.CONFLICT,
    "admission identity mismatch",
  );
}

async function validateHandshake(request: HandshakeRequest, response: HandshakeResponse): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireHandshake(toBinary(HandshakeRequestSchema, request), toBinary(HandshakeResponseSchema, response)),
    ErrorCode.UNSUPPORTED,
    "protocol identity mismatch",
  );
}

async function validateCommand(command: CommandEnvelope): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireCommand(toBinary(CommandEnvelopeSchema, command)),
    ErrorCode.INVALID,
    "invalid command envelope",
  );
  assertNativeWireResult(
    contracts.validateWireCommandProtocol(toBinary(CommandEnvelopeSchema, command)),
    ErrorCode.UNSUPPORTED,
    "protocol identity mismatch",
  );
}

async function validateResume(request: ResumeRequest): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireResume(toBinary(ResumeRequestSchema, request)),
    ErrorCode.UNSUPPORTED,
    "protocol identity mismatch",
  );
}

async function validateObserve(request: ObserveRequest): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireObserve(toBinary(ObserveRequestSchema, request)),
    ErrorCode.INVALID,
    "invalid observe request",
  );
}

async function validateCancel(request: CancelRequest): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireCancel(toBinary(CancelRequestSchema, request)),
    ErrorCode.INVALID,
    "invalid cancel request",
  );
}

function assertNativeWireResult(bytes: Uint8Array, fallbackCode: ErrorCode, fallbackMessage: string): void {
  if (bytes.byteLength === 0) return;
  try {
    const error = fromBinary(ErrorSchema, bytes);
    throw new WireError(
      error.code === ErrorCode.UNSPECIFIED ? fallbackCode : error.code,
      error.message || fallbackMessage,
    );
  } catch (error) {
    if (error instanceof WireError) throw error;
    throw new WireError(fallbackCode, fallbackMessage);
  }
}

async function waitForOpen(socket: WebSocket, signal?: AbortSignal): Promise<void> {
  if (socket.readyState === WebSocket.OPEN) return;
  await new Promise<void>((resolve, reject) => {
    const done = (finish: () => void) => {
      socket.removeEventListener("open", opened);
      socket.removeEventListener("error", failed);
      signal?.removeEventListener("abort", aborted);
      finish();
    };
    const opened = () => done(resolve);
    const failed = () => done(() => reject(new WireError(ErrorCode.INDETERMINATE, "WebSocket failed to open")));
    const aborted = () => done(() => reject(signal?.reason));
    socket.addEventListener("open", opened, { once: true });
    socket.addEventListener("error", failed, { once: true });
    signal?.addEventListener("abort", aborted, { once: true });
  });
}

async function websocketText(value: unknown): Promise<string> {
  if (typeof value === "string") return value;
  if (value instanceof ArrayBuffer) return new TextDecoder().decode(value);
  if (ArrayBuffer.isView(value)) return new TextDecoder().decode(value);
  if (value instanceof Blob) return value.text();
  throw new TypeError("unsupported WebSocket message type");
}

function withSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

async function httpWireError(response: Response, operation: string): Promise<WireError> {
  try {
    const error = fromJsonString(ErrorSchema, await response.text());
    if (error.code !== ErrorCode.UNSPECIFIED && error.message.length > 0) {
      return new WireError(error.code, error.message);
    }
  } catch {}
  return new WireError(ErrorCode.INDETERMINATE, `${operation} failed: ${response.status}`);
}

async function* sseData(stream: ReadableStream<Uint8Array>): AsyncIterable<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      buffered += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
      let boundary: number;
      while ((boundary = buffered.indexOf("\n\n")) >= 0) {
        const event = buffered.slice(0, boundary);
        buffered = buffered.slice(boundary + 2);
        const data = event.split("\n").filter(line => line.startsWith("data:"))
          .map(line => line.slice(5).trimStart()).join("\n");
        if (data !== "") yield data;
      }
      if (done) break;
    }
  } finally {
    reader.releaseLock();
  }
}
