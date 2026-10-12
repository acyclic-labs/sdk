import { create, fromBinary, fromJsonString, toBinary, toJsonString } from "@bufbuild/protobuf";
/** Injectable HTTP function; host-specific fetch extras are not required of test or custom transports. */
export type HttpFetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
import { AdmissionSchema, AdmissionState, CancelRequestSchema, CancelResponseSchema, ClientFrameSchema, CommandEnvelopeSchema, ErrorCode, ErrorSchema, ObserveRequestSchema, OperationStatusSchema, ResumeRequestSchema, ServerFrameSchema, type Admission, type CommandEnvelope, type CancelRequest, type CancelResponse, type ClientFrame, type Delivery, type ObserveRequest, type OperationStatus, type ResumeRequest, type ServerFrame } from "../generated/proto/harness/v2/harness_pb.js";
import {
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  type HandshakeRequest,
  type HandshakeResponse,
} from "../generated/proto/protocol/v1/protocol_pb.js";
import { TerminalAdmissionError } from "./client.js";
import * as wasm from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";

export interface WireConnection extends AsyncIterable<Delivery> {
  send(command: CommandEnvelope): Promise<void>;
  observe(request: ObserveRequest): Promise<OperationStatus>;
  cancel(request: CancelRequest): Promise<CancelResponse>;
  close(): Promise<void> | void;
}

export interface WireTransport {
  connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection>;
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
    resume = create(ResumeRequestSchema, { ...resume, protocol: this.negotiation.protocol });
    await validateResume(resume);
    const channel = await this.open(signal);
    const source = channel[Symbol.asyncIterator]();
    try {
      await channel.write(clientFrameJson({ case: "handshake", value: this.negotiation }));
      const first = await source.next();
      if (first.done) throw new WireError(ErrorCode.UNSUPPORTED, "missing handshake response");
      await validateHandshake(this.negotiation, handshakeFromFrame(fromJsonString(ServerFrameSchema, first.value.trim())));
    } catch (error) {
      // A rejected setup returns no connection, so nothing else can close it.
      await Promise.resolve().then(() => channel.close()).catch(() => undefined);
      throw error;
    }
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
    /** Ceiling, in UTF-8 bytes, for each server message. */
    readonly maximumMessageBytes = Number.MAX_SAFE_INTEGER,
  ) {
    assertMessageBound(maximumMessageBytes);
  }

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = create(ResumeRequestSchema, { ...resume, protocol: this.negotiation.protocol });
    await validateResume(resume);
    const socket = this.factory(this.url, ["acyclic.harness.v2"]);
    const incoming = new AsyncQueue<string>();
    const onMessage = (event: MessageEvent) => {
      void websocketText(event.data, this.maximumMessageBytes).then(value => incoming.push(value), error => incoming.fail(error));
    };
    const onClose = () => incoming.fail(new WireError(ErrorCode.INDETERMINATE, "WebSocket closed"));
    const onError = () => incoming.fail(new WireError(ErrorCode.INDETERMINATE, "WebSocket failed"));
    socket.addEventListener("message", onMessage);
    socket.addEventListener("close", onClose);
    socket.addEventListener("error", onError);
    const close = () => {
      socket.removeEventListener("message", onMessage);
      socket.removeEventListener("close", onClose);
      socket.removeEventListener("error", onError);
      socket.close(1000, "client closed");
      incoming.end();
    };
    const source = incoming[Symbol.asyncIterator]();
    try {
      await waitForOpen(socket, signal);
      socket.send(clientFrameJson({ case: "handshake", value: this.negotiation }));
      const first = await source.next();
      if (first.done) throw new WireError(ErrorCode.UNSUPPORTED, "missing handshake response");
      await validateHandshake(this.negotiation, handshakeFromFrame(fromJsonString(ServerFrameSchema, first.value.trim())));
    } catch (error) {
      // A rejected setup returns no connection, so nothing else can close it.
      close();
      throw error;
    }
    const connection = new FramedConnection(source, async line => socket.send(line), close, this.negotiation);
    socket.send(clientFrameJson({ case: "resume", value: resume }));
    return connection;
  }
}

export class HttpSseWireTransport implements WireTransport {
  readonly #endpoint: URL;
  constructor(
    readonly baseUrl: string,
    readonly negotiation: HandshakeRequest,
    readonly fetcher: HttpFetcher = fetch,
    /** Ceiling for each JSON response body and each buffered server-sent event. */
    readonly maximumMessageBytes = Number.MAX_SAFE_INTEGER,
  ) {
    let endpoint: URL;
    try { endpoint = new URL((baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`)); }
    catch { throw new TypeError("Harness endpoint must be an absolute URL"); }
    const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname);
    if ((endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && loopback)) ||
        endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      throw new TypeError("Harness endpoint must be HTTPS or loopback HTTP without credentials, query, or fragment");
    }
    assertMessageBound(maximumMessageBytes);
    this.#endpoint = endpoint;
  }

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = create(ResumeRequestSchema, { ...resume, protocol: this.negotiation.protocol });
    await validateResume(resume);
    const maximum = this.maximumMessageBytes;
    const post = (path: string, body: string, accept?: string) => this.fetcher(new URL(path, this.#endpoint), {
      method: "POST",
      redirect: "error",
      headers: { ...(accept === undefined ? {} : { accept }), "content-type": "application/json" },
      body,
      ...(signal === undefined ? {} : { signal }),
    });
    const handshakeResponse = await post("v2/harness/handshake", toJsonString(HandshakeRequestSchema, this.negotiation));
    if (!handshakeResponse.ok) throw new WireError(ErrorCode.UNSUPPORTED, "handshake failed");
    await validateHandshake(this.negotiation, fromJsonString(HandshakeResponseSchema, await boundedText(handshakeResponse, maximum)));
    const response = await post("v2/harness/replay", toJsonString(ResumeRequestSchema, resume), "text/event-stream");
    const events = response.body;
    if (!response.ok || events === null) throw new Error(`replay failed: ${response.status}`);
    const negotiation = this.negotiation;
    return {
      async send(command) {
        command = create(CommandEnvelopeSchema, { ...command, protocol: negotiation.protocol });
        await validateCommand(command);
        const submitted = await post("v2/harness/commands", toJsonString(CommandEnvelopeSchema, command));
        if (!submitted.ok) {
          try {
            const admission = fromJsonString(AdmissionSchema, await boundedText(submitted, maximum));
            await validateAdmissionIdentity(command, admission);
            if (admission.state === AdmissionState.REJECTED) {
              throw new TerminalAdmissionError(admission.error?.message ?? "command was rejected");
            }
          } catch (error) {
            if (error instanceof TerminalAdmissionError) throw error;
          }
          throw new WireError(ErrorCode.INDETERMINATE, `command failed: ${submitted.status}`);
        }
        const admission = fromJsonString(AdmissionSchema, await boundedText(submitted, maximum));
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
        request = create(ObserveRequestSchema, { ...request, protocol: negotiation.protocol });
        await validateObserve(request);
        const observed = await post("v2/harness/operations/observe", toJsonString(ObserveRequestSchema, request));
        if (!observed.ok) throw await httpWireError(observed, "observe", maximum);
        return validateStatus(request, fromJsonString(OperationStatusSchema, await boundedText(observed, maximum)));
      },
      async cancel(request) {
        request = create(CancelRequestSchema, { ...request, protocol: negotiation.protocol });
        await validateCancel(request);
        const cancelled = await post("v2/harness/operations/cancel", toJsonString(CancelRequestSchema, request));
        if (!cancelled.ok) throw await httpWireError(cancelled, "cancel", maximum);
        const response = fromJsonString(CancelResponseSchema, await boundedText(cancelled, maximum));
        await validateCancelIdentity(request, response);
        return response;
      },
      async close() {},
      async *[Symbol.asyncIterator]() {
        for await (const data of sseData(events, maximum)) {
          const frame = fromJsonString(ServerFrameSchema, data.trim());
          if (frame.frame.case === "delivery") yield frame.frame.value;
          else if (frame.frame.case === "error") {
            throw new WireError(frame.frame.value.code, frame.frame.value.message);
          }
        }
      },
    };
  }
}

export class EmbeddedWireTransport implements WireTransport {
  constructor(
    readonly negotiation: HandshakeRequest,
    readonly handshake: (request: HandshakeRequest) => Promise<HandshakeResponse>,
    readonly open: WireTransport["connect"],
  ) {}
  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    resume = create(ResumeRequestSchema, { ...resume, protocol: this.negotiation.protocol });
    await validateResume(resume);
    await validateHandshake(this.negotiation, await this.handshake(this.negotiation));
    const connection = await this.open(resume, signal);
    return {
      send: async command => {
        command = create(CommandEnvelopeSchema, { ...command, protocol: this.negotiation.protocol });
        await validateCommand(command);
        return connection.send(command);
      },
      observe: async request => {
        request = create(ObserveRequestSchema, { ...request, protocol: this.negotiation.protocol });
        await validateObserve(request);
        return validateStatus(request, await connection.observe(request));
      },
      cancel: async request => {
        request = create(CancelRequestSchema, { ...request, protocol: this.negotiation.protocol });
        await validateCancel(request);
        const response = await connection.cancel(request);
        await validateCancelIdentity(request, response);
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
  // Map entries reserve control IDs across asynchronous validation.
  readonly #observations = new Map<string, {
    request: ObserveRequest;
    resolve(status: OperationStatus): void;
    reject(error: unknown): void;
  } | undefined>();
  readonly #cancellations = new Map<string, {
    request: CancelRequest;
    resolve(response: CancelResponse): void;
    reject(error: unknown): void;
  } | undefined>();
  /** Cleared once the server stream ends; later requests could never be answered. */
  #open = true;

  constructor(
    source: AsyncIterator<string>,
    readonly write: (line: string) => Promise<void>,
    readonly closeChannel: () => Promise<void> | void,
    readonly negotiation: HandshakeRequest,
  ) {
    void this.#pump(source);
  }

  async send(command: CommandEnvelope): Promise<void> {
    command = create(CommandEnvelopeSchema, { ...command, protocol: this.negotiation.protocol });
    await validateCommand(command);
    this.#assertOpen();
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
    request = create(ObserveRequestSchema, { ...request, protocol: this.negotiation.protocol });
    const operationId = request.operationId;
    if (!operationId) throw new TypeError("observe operation identity is missing");
    if (this.#observations.has(operationId) || this.#cancellations.has(operationId)) {
      throw new Error("operation control request is already pending");
    }
    this.#observations.set(operationId, undefined);
    try {
      await validateObserve(request);
      this.#assertOpen();
      if (!this.#observations.has(operationId)) {
        throw new WireError(ErrorCode.INDETERMINATE, "connection closed during operation control validation");
      }
      const observed = new Promise<OperationStatus>((resolve, reject) =>
        this.#observations.set(operationId, { request, resolve, reject }),
      );
      await this.write(clientFrameJson({ case: "observe", value: request }));
      return observed;
    } catch (error) {
      this.#observations.delete(operationId);
      throw error;
    }
  }

  async cancel(request: CancelRequest): Promise<CancelResponse> {
    request = create(CancelRequestSchema, { ...request, protocol: this.negotiation.protocol });
    const operationId = request.operationId;
    const idempotencyKey = request.idempotencyKey;
    if (!operationId || !idempotencyKey) throw new TypeError("cancel operation identity is missing");
    if (this.#observations.has(operationId) || this.#cancellations.has(operationId)) {
      throw new Error("operation control request is already pending");
    }
    this.#cancellations.set(operationId, undefined);
    try {
      await validateCancel(request);
      this.#assertOpen();
      if (!this.#cancellations.has(operationId)) {
        throw new WireError(ErrorCode.INDETERMINATE, "connection closed during operation control validation");
      }
      const cancelled = new Promise<CancelResponse>((resolve, reject) =>
        this.#cancellations.set(operationId, { request, resolve, reject }),
      );
      await this.write(clientFrameJson({ case: "cancel", value: request }));
      return cancelled;
    } catch (error) {
      this.#cancellations.delete(operationId);
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
        const frame = fromJsonString(ServerFrameSchema, item.value.trim());
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
          const status = await validateStatus(pending.request, frame.frame.value);
          this.#observations.delete(operationId);
          pending.resolve(status);
        } else if (frame.frame.case === "cancellation") {
          const operationId = frame.frame.value.operation?.operationId;
          const pending = operationId ? this.#cancellations.get(operationId) : undefined;
          if (!pending) throw new Error("cancellation has no matching request");
          this.#cancellations.delete(operationId!);
          try {
            await validateCancelIdentity(pending.request, frame.frame.value);
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
      this.#open = false;
      this.#deliveries.end();
    } catch (error) {
      this.#fail(error);
    }
  }

  #assertOpen(): void {
    if (!this.#open) throw new WireError(ErrorCode.INDETERMINATE, "connection closed before the request was sent");
  }

  #fail(error: unknown): void {
    this.#open = false;
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
    for (const pending of this.#observations.values()) pending?.reject(error);
    this.#observations.clear();
    for (const pending of this.#cancellations.values()) pending?.reject(error);
    this.#cancellations.clear();
    this.#deliveries.fail(error);
  }
}

class AsyncQueue<T> implements AsyncIterable<T> {
  readonly #values: T[] = [];
  readonly #waiters: Array<{ resolve(value: IteratorResult<T>): void; reject(error: unknown): void }> = [];
  #ended = false;
  #error: unknown;

  push(value: T): void {
    // A failed stream must not deliver later values past the failure.
    if (this.#error !== undefined) return;
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
  status: OperationStatus,
): Promise<OperationStatus> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireStatus(toBinary(ObserveRequestSchema, request), toBinary(OperationStatusSchema, status)),
    ErrorCode.CONFLICT,
    "operation status identity mismatch",
  );
  return status;
}

async function validateCancelIdentity(
  request: CancelRequest,
  response: CancelResponse,
): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireCancellation(toBinary(CancelRequestSchema, request), toBinary(CancelResponseSchema, response)),
    ErrorCode.CONFLICT,
    "cancellation identity mismatch",
  );
}

function handshakeFromFrame(frame: ServerFrame): HandshakeResponse {
  if (frame.frame.case !== "handshake") {
    throw new WireError(ErrorCode.UNSUPPORTED, "handshake must be the first server frame");
  }
  return frame.frame.value;
}

async function validateAdmissionIdentity(command: CommandEnvelope, admission: Admission): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireAdmission(toBinary(CommandEnvelopeSchema, command), toBinary(AdmissionSchema, admission)),
    ErrorCode.CONFLICT,
    "admission identity mismatch",
  );
}

async function validateHandshake(request: HandshakeRequest, response: HandshakeResponse): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireHandshake(toBinary(HandshakeRequestSchema, request), toBinary(HandshakeResponseSchema, response)),
    ErrorCode.UNSUPPORTED,
    "protocol identity mismatch",
  );
}

async function validateCommand(command: CommandEnvelope): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireCommand(toBinary(CommandEnvelopeSchema, command)),
    ErrorCode.INVALID,
    "invalid command envelope",
  );
  assertNativeWireResult(
    wasm.validateWireCommandProtocol(toBinary(CommandEnvelopeSchema, command)),
    ErrorCode.UNSUPPORTED,
    "protocol identity mismatch",
  );
}

async function validateResume(request: ResumeRequest): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireResume(toBinary(ResumeRequestSchema, request)),
    ErrorCode.UNSUPPORTED,
    "protocol identity mismatch",
  );
}

async function validateObserve(request: ObserveRequest): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireObserve(toBinary(ObserveRequestSchema, request)),
    ErrorCode.INVALID,
    "invalid observe request",
  );
}

async function validateCancel(request: CancelRequest): Promise<void> {
  await ensureHarnessWasm();
  assertNativeWireResult(
    wasm.validateWireCancel(toBinary(CancelRequestSchema, request)),
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

async function websocketText(value: unknown, maximum: number): Promise<string> {
  const exceeded = () => new WireError(ErrorCode.INDETERMINATE, "server message exceeds configured bound");
  if (typeof value === "string") {
    if (utf8Exceeds(value, maximum)) throw exceeded();
    return value;
  }
  const binary = value instanceof ArrayBuffer || ArrayBuffer.isView(value);
  if (!binary && !(value instanceof Blob)) throw new TypeError("unsupported WebSocket message type");
  if ((binary ? value.byteLength : value.size) > maximum) throw exceeded();
  return binary ? new TextDecoder().decode(value) : value.text();
}

function assertMessageBound(maximum: number): void {
  if (!Number.isSafeInteger(maximum) || maximum < 1) {
    throw new RangeError("maximumMessageBytes must be a positive safe integer");
  }
}

const utf8 = new TextEncoder();
function utf8Exceeds(text: string, maximum: number): boolean {
  return text.length > maximum ||
    (text.length * 3 > maximum && utf8.encode(text).byteLength > maximum);
}

async function httpWireError(response: Response, operation: string, maximum: number): Promise<WireError> {
  try {
    const error = fromJsonString(ErrorSchema, await boundedText(response, maximum));
    if (error.code !== ErrorCode.UNSPECIFIED && error.message.length > 0) {
      return new WireError(error.code, error.message);
    }
  } catch {}
  return new WireError(ErrorCode.INDETERMINATE, `${operation} failed: ${response.status}`);
}

/** Read a whole response body, cancelling it once it exceeds `maximum` bytes. */
async function boundedText(response: Response, maximum: number): Promise<string> {
  const reader = response.body?.getReader();
  if (reader === undefined) return "";
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maximum) {
        await reader.cancel().catch(() => undefined);
        throw new WireError(ErrorCode.INDETERMINATE, "response exceeds configured bound");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return new TextDecoder().decode(bytes);
}

async function* sseData(stream: ReadableStream<Uint8Array>, maximum: number): AsyncIterable<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  let completed = false;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      buffered += decoder.decode(value, { stream: !done }).replaceAll("\r\n", "\n");
      let boundary: number;
      while ((boundary = buffered.indexOf("\n\n")) >= 0) {
        const event = buffered.slice(0, boundary);
        if (utf8Exceeds(event, maximum)) throw new WireError(ErrorCode.INDETERMINATE, "server event exceeds configured bound");
        buffered = buffered.slice(boundary + 2);
        const data = event.split("\n").filter(line => line.startsWith("data:"))
          .map(line => line.slice(5).trimStart()).join("\n");
        if (data !== "") yield data;
      }
      if (utf8Exceeds(buffered, maximum)) throw new WireError(ErrorCode.INDETERMINATE, "server event exceeds configured bound");
      if (done) break;
    }
    completed = true;
  } finally {
    if (!completed) await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}
