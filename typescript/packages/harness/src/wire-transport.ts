import { create, fromJson, toJsonString } from "@bufbuild/protobuf";
import {
  AdmissionSchema,
  AdmissionState,
  CancelRequestSchema,
  CancelResponseSchema,
  ClientFrameSchema,
  CommandEnvelopeSchema,
  CompletionState,
  ErrorCode,
  ErrorSchema,
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  ObserveRequestSchema,
  OperationStatusSchema,
  ResumeRequestSchema,
  ServerFrameSchema,
  type Admission,
  type Authority,
  type CommandEnvelope,
  type CancelRequest,
  type CancelResponse,
  type Delivery,
  type HandshakeRequest,
  type HandshakeResponse,
  type ObserveRequest,
  type OperationStatus,
  type ResumeRequest,
  type Scope,
  type ServerFrame,
} from "../generated/proto/harness/v1/harness_pb.js";
import { TerminalAdmissionError } from "./client.js";

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
    validateHandshake(this.negotiation, await this.api.handshake(this.negotiation));
    const deliveries = this.api.replay(resume, signal);
    return {
      send: command => this.api.submit(withProtocol(command, this.negotiation)),
      observe: async request => {
        request = withObserveProtocol(request, this.negotiation);
        return validateStatus(request, this.negotiation, await this.api.observe(request));
      },
      cancel: async request => {
        request = withCancelProtocol(request, this.negotiation);
        const response = await this.api.cancel(request);
        validateCancelIdentity(request, this.negotiation, response);
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
    const channel = await this.open(signal);
    const source = channel[Symbol.asyncIterator]();
    await channel.write(clientFrameJson("handshake", this.negotiation));
    const first = await source.next();
    if (first.done) throw new WireError(ErrorCode.UNSUPPORTED, "missing handshake response");
    validateHandshake(this.negotiation, handshakeFromFrame(parseServerFrame(first.value)));
    const connection = new FramedConnection(source, line => channel.write(line), () => channel.close(), this.negotiation);
    await channel.write(clientFrameJson("resume", resume));
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
    const socket = this.factory(this.url, ["acyclic.harness.v1"]);
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
    socket.send(clientFrameJson("handshake", this.negotiation));
    const first = await source.next();
    if (first.done) throw new WireError(ErrorCode.UNSUPPORTED, "missing handshake response");
    validateHandshake(this.negotiation, handshakeFromFrame(parseServerFrame(first.value)));
    const close = () => {
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
      socket.removeEventListener("error", onError);
      socket.close(1000, "client closed");
      incoming.end();
    };
    const connection = new FramedConnection(source, async line => socket.send(line), close, this.negotiation);
    socket.send(clientFrameJson("resume", resume));
    return connection;
  }
}

export class HttpSseWireTransport implements WireTransport {
  constructor(
    readonly baseUrl: string,
    readonly negotiation: HandshakeRequest,
    readonly fetcher: typeof fetch = fetch,
  ) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = withResumeProtocol(resume, this.negotiation);
    const handshakeResponse = await this.fetcher(new URL("v1/harness/handshake", withSlash(this.baseUrl)), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: toJsonString(HandshakeRequestSchema, this.negotiation),
      ...(signal === undefined ? {} : { signal }),
    });
    if (!handshakeResponse.ok) throw new WireError(ErrorCode.UNSUPPORTED, "handshake failed");
    validateHandshake(this.negotiation, fromJson(HandshakeResponseSchema, await handshakeResponse.json()));
    const response = await this.fetcher(new URL("v1/harness/replay", withSlash(this.baseUrl)), {
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
        const submitted = await fetcher(new URL("v1/harness/commands", withSlash(baseUrl)), {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: toJsonString(CommandEnvelopeSchema, command),
          ...(signal === undefined ? {} : { signal }),
        });
        if (!submitted.ok) {
          try {
            const admission = fromJson(AdmissionSchema, await submitted.json());
            validateAdmissionIdentity(command, admission);
            if (admission.state === AdmissionState.REJECTED) {
              throw new TerminalAdmissionError(admission.error?.message ?? "command was rejected");
            }
          } catch (error) {
            if (error instanceof TerminalAdmissionError) throw error;
          }
          throw new WireError(ErrorCode.INDETERMINATE, `command failed: ${submitted.status}`);
        }
        const admission = fromJson(AdmissionSchema, await submitted.json());
        validateAdmissionIdentity(command, admission);
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
        const observed = await fetcher(new URL("v1/harness/operations/observe", withSlash(baseUrl)), {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: toJsonString(ObserveRequestSchema, request),
          ...(signal === undefined ? {} : { signal }),
        });
        if (!observed.ok) throw await httpWireError(observed, "observe");
        return validateStatus(request, negotiation, fromJson(OperationStatusSchema, await observed.json()));
      },
      async cancel(request) {
        request = withCancelProtocol(request, negotiation);
        const cancelled = await fetcher(new URL("v1/harness/operations/cancel", withSlash(baseUrl)), {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: toJsonString(CancelRequestSchema, request),
          ...(signal === undefined ? {} : { signal }),
        });
        if (!cancelled.ok) throw await httpWireError(cancelled, "cancel");
        const response = fromJson(CancelResponseSchema, await cancelled.json());
        validateCancelIdentity(request, negotiation, response);
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
    validateHandshake(this.negotiation, await this.handshakeGrpc(this.negotiation));
    const connection = await this.connectGrpc(resume, signal);
    return {
      send: command => connection.send(withProtocol(command, this.negotiation)),
      observe: async request => {
        request = withObserveProtocol(request, this.negotiation);
        return validateStatus(request, this.negotiation, await connection.observe(request));
      },
      cancel: async request => {
        request = withCancelProtocol(request, this.negotiation);
        const response = await connection.cancel(request);
        validateCancelIdentity(request, this.negotiation, response);
        return response;
      },
      close: () => connection.close(),
      [Symbol.asyncIterator]: () => connection[Symbol.asyncIterator](),
    };
  }
}

export class WireError extends Error {
  constructor(readonly code: number, message: string) {
    super(message);
  }
}

class FramedConnection implements WireConnection {
  readonly #deliveries = new AsyncQueue<Delivery>();
  readonly #pending = new Map<string, { idempotencyKey: string; resolve(): void; reject(error: unknown): void }>();
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
    const operationId = command.operation?.operationId;
    if (!operationId) throw new TypeError("command operation identity is missing");
    if (this.#pending.has(operationId)) throw new Error("command admission is already pending");
    const idempotencyKey = command.operation?.idempotencyKey;
    if (!idempotencyKey) throw new TypeError("command idempotency key is missing");
    const admission = new Promise<void>((resolve, reject) =>
      this.#pending.set(operationId, { idempotencyKey, resolve, reject }),
    );
    try {
      await this.write(clientFrameJson("command", command));
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
    if (this.#observations.has(operationId) || this.#cancellations.has(operationId)) {
      throw new Error("operation control request is already pending");
    }
    const observed = new Promise<OperationStatus>((resolve, reject) =>
      this.#observations.set(operationId, { request, resolve, reject }),
    );
    try {
      await this.write(clientFrameJson("observe", request));
    } catch (error) {
      this.#observations.delete(operationId);
      throw error;
    }
    return observed;
  }

  async cancel(request: CancelRequest): Promise<CancelResponse> {
    request = withCancelProtocol(request, this.negotiation);
    const operationId = request.operationId;
    const idempotencyKey = request.idempotencyKey;
    if (!operationId || !idempotencyKey) throw new TypeError("cancel operation identity is missing");
    if (this.#cancellations.has(operationId) || this.#observations.has(operationId)) {
      throw new Error("operation control request is already pending");
    }
    const cancelled = new Promise<CancelResponse>((resolve, reject) =>
      this.#cancellations.set(operationId, { request, resolve, reject }),
    );
    try {
      await this.write(clientFrameJson("cancel", request));
    } catch (error) {
      this.#cancellations.delete(operationId);
      throw error;
    }
    return cancelled;
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
          if (frame.frame.value.operation?.idempotencyKey !== pending.idempotencyKey) {
            pending.reject(new WireError(ErrorCode.CONFLICT, "admission identity mismatch"));
            continue;
          }
          if (frame.frame.value.state === AdmissionState.ACCEPTED) pending.resolve();
          else if (frame.frame.value.state === AdmissionState.REJECTED) {
            pending.reject(new TerminalAdmissionError(frame.frame.value.error?.message ?? "rejected"));
          } else {
            pending.reject(new WireError(ErrorCode.INDETERMINATE, "command outcome is indeterminate"));
          }
        } else if (frame.frame.case === "error") {
          const error = new WireError(frame.frame.value.code, frame.frame.value.message);
          const operationId = frame.frame.value.operationId;
          let correlated = false;
          const observation = this.#observations.get(operationId);
          if (observation) {
            this.#observations.delete(operationId);
            observation.reject(error);
            correlated = true;
          }
          const cancellation = this.#cancellations.get(operationId);
          if (cancellation) {
            this.#cancellations.delete(operationId);
            cancellation.reject(error);
            correlated = true;
          }
          if (!correlated) throw error;
        } else if (frame.frame.case === "status") {
          const operationId = frame.frame.value.operation?.operationId ?? "";
          const pending = this.#observations.get(operationId);
          if (!pending) throw new Error("operation status has no matching observation");
          const status = validateStatus(pending.request, this.negotiation, frame.frame.value);
          this.#observations.delete(operationId);
          pending.resolve(status);
        } else if (frame.frame.case === "cancellation") {
          const operationId = frame.frame.value.operation?.operationId;
          const pending = operationId ? this.#cancellations.get(operationId) : undefined;
          if (!pending) throw new Error("cancellation has no matching request");
          this.#cancellations.delete(operationId!);
          try {
            validateCancelIdentity(pending.request, this.negotiation, frame.frame.value);
            pending.resolve(frame.frame.value);
          } catch (error) {
            pending.reject(error);
          }
        }
      }
      if (this.#pending.size > 0 || this.#observations.size > 0 || this.#cancellations.size > 0) {
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

function clientFrameJson(
  case_: "handshake" | "resume" | "command" | "observe" | "cancel",
  value: HandshakeRequest | ResumeRequest | CommandEnvelope | ObserveRequest | CancelRequest,
): string {
  const frame = create(ClientFrameSchema, { frame: { case: case_, value } } as never);
  return `${toJsonString(ClientFrameSchema, frame)}\n`;
}

function validateStatus(
  request: ObserveRequest,
  negotiation: HandshakeRequest,
  status: OperationStatus,
): OperationStatus {
  if (
    request.operationId.length === 0 || status.operation?.operationId !== request.operationId ||
    request.owner === undefined || status.owner === undefined ||
    request.owner.kind !== status.owner.kind || request.owner.id !== status.owner.id ||
    (status.error?.operationId !== undefined && status.error.operationId.length > 0 &&
      status.error.operationId !== request.operationId) ||
    status.protocol === undefined || negotiation.protocol === undefined ||
    status.protocol.version !== negotiation.protocol.version ||
    status.protocol.descriptorDigest !== negotiation.protocol.descriptorDigest ||
    status.state < CompletionState.RUNNING || status.state > CompletionState.INDETERMINATE
  ) {
    throw new WireError(ErrorCode.CONFLICT, "operation status identity mismatch");
  }
  return status;
}

function validateCancelIdentity(
  request: CancelRequest,
  negotiation: HandshakeRequest,
  response: CancelResponse,
): void {
  const expected = {
    operationId: request.operationId,
    idempotencyKey: request.idempotencyKey,
  };
  const actual = response.operation;
  if (
    actual === undefined || response.status === undefined ||
    expected.operationId.length === 0 || expected.idempotencyKey.length === 0 ||
    expected.operationId !== actual.operationId || expected.idempotencyKey !== actual.idempotencyKey
  ) {
    throw new WireError(ErrorCode.CONFLICT, "cancellation identity mismatch");
  }
  validateStatus(create(ObserveRequestSchema, {
    protocol: request.protocol,
    owner: request.owner,
    operationId: request.operationId,
    scope: request.scope,
  }), negotiation, response.status);
}

function parseServerFrame(value: string): ServerFrame {
  return fromJson(ServerFrameSchema, JSON.parse(value.trim()));
}

function handshakeFromFrame(frame: ServerFrame): HandshakeResponse {
  if (frame.frame.case !== "handshake") {
    throw new WireError(ErrorCode.UNSUPPORTED, "handshake must be the first server frame");
  }
  return frame.frame.value;
}

function validateAdmissionIdentity(command: CommandEnvelope, admission: Admission): void {
  const expected = command.operation;
  const actual = admission.operation;
  if (
    expected === undefined || actual === undefined ||
    expected.operationId.length === 0 || expected.idempotencyKey.length === 0 ||
    actual.operationId.length === 0 || actual.idempotencyKey.length === 0 ||
    actual.operationId !== expected.operationId ||
    actual.idempotencyKey !== expected.idempotencyKey
  ) {
    throw new WireError(ErrorCode.CONFLICT, "admission identity mismatch");
  }
}

function validateHandshake(request: HandshakeRequest, response: HandshakeResponse): void {
  if (!request.protocol || !response.protocol ||
      request.protocol.version !== response.protocol.version ||
      request.protocol.descriptorDigest !== response.protocol.descriptorDigest) {
    throw new WireError(ErrorCode.UNSUPPORTED, "protocol identity mismatch");
  }
  const supported = new Set(
    (response.supported?.capabilities ?? []).map(value => `${value.name}\u0000${value.version}`),
  );
  for (const capability of request.required?.capabilities ?? []) {
    if (!supported.has(`${capability.name}\u0000${capability.version}`)) {
      throw new WireError(ErrorCode.UNSUPPORTED, `unsupported capability: ${capability.name}`);
    }
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
    const error = fromJson(ErrorSchema, await response.json());
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
