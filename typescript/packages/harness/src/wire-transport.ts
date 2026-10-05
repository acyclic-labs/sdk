import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

/** Injectable HTTP function retained for host integrations that expose fetch. */
export type HttpFetcher = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
import {
  AdmissionSchema,
  AdmissionState,
  CancelRequestSchema,
  CancelResponseSchema,
  type CancelResponse,
  CommandEnvelopeSchema,
  DeliverySchema,
  ErrorCode,
  ErrorSchema,
  ObserveRequestSchema,
  OperationStatusSchema,
  ResumeRequestSchema,
  type Admission,
  type Authority,
  type CancelRequest,
  type CommandEnvelope,
  type Delivery,
  type ObserveRequest,
  type OperationStatus,
  type ResumeRequest,
  type Scope,
} from "../generated/proto/harness/v2/harness_pb.js";
import {
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  type HandshakeRequest,
  type HandshakeResponse,
} from "../generated/proto/protocol/v1/protocol_pb.js";
import { TerminalAdmissionError } from "./client.js";
import { NativeContracts } from "./native-contracts.js";
import { loadHarnessWasm } from "./wasm-runtime.js";

export interface WireConnection extends AsyncIterable<Delivery> {
  send(command: CommandEnvelope): Promise<void>;
  observe(request: ObserveRequest): Promise<OperationStatus>;
  cancel(request: CancelRequest): Promise<CancelResponse>;
  close(): Promise<void> | void;
}

export interface WireTransport {
  connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection>;
}

/** Typed control handle over the exact Rust-backed wire connection. */
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

  cancel(idempotencyKey: string, recursive = false) {
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

/** Adapter for an already Rust-owned embedded host. */
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

/**
 * Default browser remote transport. Authentication, negotiation, gRPC-Web,
 * replay, operation control, response limits, and protobuf framing execute in
 * the Rust `sdk-remote-web` facade. TypeScript only maps generated messages to
 * and from the Rust byte ABI.
 */
export class RustBrowserWireTransport implements WireTransport {
  constructor(
    readonly endpoint: string,
    readonly bearerToken: string,
    readonly negotiation?: HandshakeRequest,
  ) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
    const module = await loadHarnessWasm();
    const client = await abortable(module.BrowserHarnessRemoteClient.connect(this.endpoint, this.bearerToken), signal);
    // The Rust handshake is authoritative when callers omit a negotiation value.
    const negotiation = this.negotiation ?? handshakeFromCapabilities(client.capabilities());
    const request = withResumeProtocol(resume, negotiation);
    await validateResume(request);
    const replayed = await abortable(client.replay(toBinary(ResumeRequestSchema, request)), signal);
    const deliveries = replayed.map(value => fromBinary(DeliverySchema, bytes(value)));
    return {
      send: async command => {
        const prepared = withProtocol(command, negotiation);
        await validateCommand(prepared);
        const encoded = await abortable(client.submit(toBinary(CommandEnvelopeSchema, prepared)), signal);
        const admission = fromBinary(AdmissionSchema, encoded);
        await validateAdmissionIdentity(prepared, admission);
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
      observe: async value => {
        const prepared = withObserveProtocol(value, negotiation);
        await validateObserve(prepared);
        const status = fromBinary(OperationStatusSchema, await abortable(
          client.observe(toBinary(ObserveRequestSchema, prepared)), signal,
        ));
        return validateStatus(prepared, negotiation, status);
      },
      cancel: async value => {
        const prepared = withCancelProtocol(value, negotiation);
        await validateCancel(prepared);
        const response = fromBinary(CancelResponseSchema, await abortable(
          client.cancel(toBinary(CancelRequestSchema, prepared)), signal,
        ));
        await validateCancelIdentity(prepared, this.negotiation, response);
        return response;
      },
      close: () => client.free(),
      async *[Symbol.asyncIterator]() {
        for (const delivery of deliveries) yield delivery;
      },
    };
  }
}

export class WireError extends Error {
  constructor(readonly code: ErrorCode, message: string) {
    super(message);
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

function handshakeFromCapabilities(value: unknown): HandshakeRequest {
  if (value === null || typeof value !== "object") throw new WireError(ErrorCode.UNSUPPORTED, "Rust Harness handshake omitted capabilities");
  const capabilities = value as { version?: unknown; descriptorDigest?: unknown };
  if (typeof capabilities.version !== "string" || typeof capabilities.descriptorDigest !== "string") {
    throw new WireError(ErrorCode.UNSUPPORTED, "Rust Harness handshake returned invalid capabilities");
  }
  return create(HandshakeRequestSchema, {
    protocol: { version: capabilities.version, descriptorDigest: capabilities.descriptorDigest },
    required: { capabilities: [] },
  });
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
  _negotiation: HandshakeRequest | undefined,
  response: CancelResponse,
): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(
    contracts.validateWireCancellation(toBinary(CancelRequestSchema, request), toBinary(CancelResponseSchema, response)),
    ErrorCode.CONFLICT,
    "cancellation identity mismatch",
  );
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
  assertNativeWireResult(contracts.validateWireCommand(toBinary(CommandEnvelopeSchema, command)), ErrorCode.INVALID, "invalid command envelope");
  assertNativeWireResult(contracts.validateWireCommandProtocol(toBinary(CommandEnvelopeSchema, command)), ErrorCode.UNSUPPORTED, "protocol identity mismatch");
}
async function validateResume(request: ResumeRequest): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(contracts.validateWireResume(toBinary(ResumeRequestSchema, request)), ErrorCode.UNSUPPORTED, "protocol identity mismatch");
}
async function validateObserve(request: ObserveRequest): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(contracts.validateWireObserve(toBinary(ObserveRequestSchema, request)), ErrorCode.INVALID, "invalid observe request");
}
async function validateCancel(request: CancelRequest): Promise<void> {
  const contracts = await NativeContracts.create();
  assertNativeWireResult(contracts.validateWireCancel(toBinary(CancelRequestSchema, request)), ErrorCode.INVALID, "invalid cancel request");
}
function assertNativeWireResult(encoded: Uint8Array, fallbackCode: ErrorCode, fallbackMessage: string): void {
  if (encoded.byteLength === 0) return;
  try {
    const error = fromBinary(ErrorSchema, encoded);
    throw new WireError(error.code === ErrorCode.UNSPECIFIED ? fallbackCode : error.code, error.message || fallbackMessage);
  } catch (error) {
    if (error instanceof WireError) throw error;
    throw new WireError(fallbackCode, fallbackMessage);
  }
}
function bytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  throw new TypeError("Rust Harness replay returned a non-byte delivery");
}
function abortable<Value>(promise: Promise<Value>, signal?: AbortSignal): Promise<Value> {
  if (signal === undefined) return promise;
  if (signal.aborted) return Promise.reject(signal.reason ?? new DOMException("The operation was aborted", "AbortError"));
  return new Promise<Value>((resolve, reject) => {
    const onAbort = () => reject(signal.reason ?? new DOMException("The operation was aborted", "AbortError"));
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  });
}
