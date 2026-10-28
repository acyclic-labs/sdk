import { credentialProvider, type CredentialOptions } from "@acyclic-labs/actors/credential";
import { rootCertificates } from "node:tls";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient, type CallOptions, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";

export const MAX_OUTPUT_READ_BYTES = 65_536;
const MAX_OFFSET = (1n << 64n) - 1n;

export interface MachinesGrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Node/Bun client for the Cloud product API's protobuf admissions and observations. */
export function createMachinesGrpcClient(options: MachinesGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  const credential = credentialProvider(options);
  const maximum = options.maximumMessageBytes ?? 64 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${await credential(request.signal)}`);
    return next(request);
  };
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transport = createGrpcTransport({
    sessionManager: session, baseUrl: endpoint.href,
    acceptCompression: [], interceptors: [authenticate],
    readMaxBytes: maximum, writeMaxBytes: maximum,
  });
  const machines = createClient(wire.MachinesService, transport);
  return {
    machines: {
      ...machines,
      async readFile(request: MessageInitShape<typeof wire.ReadFileRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ReadFileRequestSchema, request);
        const access = checkFileAccess(query.access, query.limit);
        if (query.limit < 1) throw new RangeError("file read chunk must be positive");
        const result = await machines.readFile(query, fileOptions(access, callOptions));
        checkFileResult(access, result);
        if (result.result.case === "data") {
          if (result.result.value.byteLength > query.limit) throw new ConnectError("file read exceeds requested chunk", Code.DataLoss);
        } else if (result.result.case !== "errno" || result.result.value === 0) throw new ConnectError("file read terminal result is invalid", Code.DataLoss);
        return result;
      },
      async writeFile(request: MessageInitShape<typeof wire.WriteFileRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.WriteFileRequestSchema, request);
        const access = checkFileAccess(query.access, query.data.byteLength);
        if (query.truncate && access.offset !== 0n) throw new RangeError("file truncation requires offset zero");
        const result = await machines.writeFile(query, fileOptions(access, callOptions));
        checkFileResult(access, result);
        if (result.result.case === "written") {
          if (result.result.value > query.data.byteLength) throw new ConnectError("file write exceeds supplied chunk", Code.DataLoss);
        } else if (result.result.case !== "errno" || result.result.value === 0) throw new ConnectError("file write terminal result is invalid", Code.DataLoss);
        return result;
      },
      exec(request: MessageInitShape<typeof wire.ExecRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ExecRequestSchema, request);
        const lengths = query.argv.map(value => new TextEncoder().encode(value).byteLength);
        if (query.argv.length < 1 || query.argv.length > 64 || !query.argv[0] || query.argv.some(value => value.includes("\0")) || lengths.some(length => length > 4096) || lengths.reduce((sum, length) => sum + length, 0) > 32768) throw new RangeError("invalid exec argv");
        if (!Number.isInteger(query.timeoutMs) || query.timeoutMs < 1 || query.timeoutMs > 0xffffffff) throw new RangeError("timeoutMs must be a positive uint32");
        if (query.cwd !== undefined && (!query.cwd || query.cwd.includes("\0"))) throw new RangeError("cwd must be nonempty and NUL-free");
        return ownedStream(async function* (signal) {
          for await (const frame of machines.exec(query, { ...callOptions, signal, timeoutMs: callOptions?.timeoutMs ?? query.timeoutMs + 1000 })) {
            checkIdentity(query.machine, frame.machine, "machine");
            if (frame.output.case === "stopped") {
              if (![wire.ExecStopKind.TIMEOUT, wire.ExecStopKind.EXPIRED, wire.ExecStopKind.OUTPUT_LIMIT, wire.ExecStopKind.CONTROL_STOPPED, wire.ExecStopKind.CAPTURE_FAILED].includes(frame.output.value.kind)) throw new ConnectError("invalid exec stop kind", Code.DataLoss);
            } else if (frame.output.case !== "exitCode" && ((frame.output.case !== "data" && frame.output.case !== "stderr") || frame.output.value.byteLength < 1 || frame.output.value.byteLength > MAX_OUTPUT_READ_BYTES)) throw new ConnectError("invalid exec output frame", Code.DataLoss);
            yield frame;
          }
          throw new ConnectError("exec stream ended without exit code", Code.DataLoss);
        }, callOptions?.signal, frame => frame.output.case === "exitCode" || frame.output.case === "stopped");
      },
      watchOperation(request: MessageInitShape<typeof wire.OperationRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.OperationRequestSchema, request);
        return ownedStream(async function* (signal) {
          for await (const frame of machines.watchOperation(query, { ...callOptions, signal })) {
            checkIdentity(query.operation, frame.operation, "operation");
            if (![wire.OperationStatus.PENDING, wire.OperationStatus.SUCCEEDED, wire.OperationStatus.FAILED, wire.OperationStatus.CANCELLED, wire.OperationStatus.INDETERMINATE].includes(frame.status)) throw new ConnectError("invalid operation status", Code.DataLoss);
            yield frame;
          }
          throw new ConnectError("operation watch ended without terminal status", Code.DataLoss);
        }, callOptions?.signal, frame => frame.status !== wire.OperationStatus.PENDING);
      },
      async readOutput(request: MessageInitShape<typeof wire.ReadOutputRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ReadOutputRequestSchema, request);
        if (query.offset < 0n || query.offset > MAX_OFFSET) throw new RangeError("offset must be uint64");
        if (!Number.isInteger(query.limit) || query.limit < 1 || query.limit > MAX_OUTPUT_READ_BYTES) throw new RangeError("limit must be 1..65536");
        const result = await machines.readOutput(query, callOptions);
        checkIdentity(query.machine, result.machine, "machine");
        if (result.offset < query.offset || result.data.byteLength > query.limit || result.offset + BigInt(result.data.byteLength) > MAX_OFFSET) throw new ConnectError("invalid console output range", Code.DataLoss);
        return result;
      },
    },
    close: () => session.abort(),
  };
}

function checkIdentity(request: { value: Uint8Array } | undefined, response: { value: Uint8Array } | undefined, kind: string): void {
  if (!request || !response || request.value.length !== response.value.length || request.value.some((byte, index) => byte !== response.value[index])) throw new ConnectError(`response ${kind} identity differs from request`, Code.DataLoss);
}

// Connect's promise-client iterator does not forward return() to the RPC.
// Own cancellation outside the generator so return() also interrupts a pending next().
function ownedStream<T>(open: (signal: AbortSignal) => AsyncIterable<T>, parent: AbortSignal | undefined, terminal: (value: T) => boolean): AsyncIterableIterator<T> {
  let iterator: AsyncIterator<T> | undefined;
  let controller: AbortController | undefined;
  let closed = false;
  const abort = () => controller?.abort(parent?.reason);
  const close = () => {
    closed = true;
    controller?.abort();
    parent?.removeEventListener("abort", abort);
  };
  return {
    [Symbol.asyncIterator]() { return this; },
    async next() {
      if (closed) return { done: true, value: undefined };
      if (!iterator) {
        controller = new AbortController();
        parent?.addEventListener("abort", abort, { once: true });
        if (parent?.aborted) abort();
        iterator = open(controller.signal)[Symbol.asyncIterator]();
      }
      try {
        const result = await iterator.next();
        if (result.done || terminal(result.value)) close();
        return result;
      } catch (cause) {
        close();
        throw cause;
      }
    },
    async return() {
      close();
      await iterator?.return?.();
      return { done: true, value: undefined };
    },
  };
}

function checkFileAccess(access: wire.FileAccess | undefined, length: number): wire.FileAccess {
  if (!access || !access.machine || !access.path.startsWith("/") || access.path.includes("\0") || access.offset < 0n || access.offset > MAX_OFFSET || access.deadlineUnixMs < 1n || access.deadlineUnixMs > MAX_OFFSET) throw new RangeError("invalid absolute guest file access");
  if (!Number.isInteger(length) || length < 0 || length > MAX_OUTPUT_READ_BYTES || access.offset + BigInt(length) > MAX_OFFSET) throw new RangeError("invalid guest file chunk range");
  return access;
}

function fileOptions(access: wire.FileAccess, options?: CallOptions): CallOptions {
  const remaining = access.deadlineUnixMs - BigInt(Date.now());
  if (remaining <= 0n || remaining > BigInt(Number.MAX_SAFE_INTEGER)) throw new RangeError("file deadline expired or not representable by this transport");
  return { ...options, timeoutMs: Math.min(Number(remaining), options?.timeoutMs ?? Infinity) };
}

function checkFileResult(access: wire.FileAccess, result: { machine: wire.MachineId | undefined; offset: bigint }): void {
  checkIdentity(access.machine, result.machine, "machine");
  if (access.offset !== result.offset) throw new ConnectError("file result offset substituted", Code.DataLoss);
}
