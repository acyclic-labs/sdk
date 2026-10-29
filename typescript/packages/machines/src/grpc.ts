import { grpcDeadlines } from "@acyclic-labs/actors/deadline";
import { credentialProvider, type CredentialOptions } from "@acyclic-labs/actors/credential";
import { rootCertificates } from "node:tls";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient, type CallOptions, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { pageSize } from "./wasm-runtime.js";
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
  const maximum = options.maximumMessageBytes;
  if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 1)) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && options.caCertificate.length === 0) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${await credential(request.signal)}`);
    return next(request);
  };
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transport = grpcDeadlines(createGrpcTransport({
    sessionManager: session, baseUrl: endpoint.href,
    acceptCompression: [], interceptors: [authenticate],
    ...(maximum === undefined ? {} : { readMaxBytes: maximum, writeMaxBytes: maximum }),
  }), () => new ConnectError("the operation timed out", Code.DeadlineExceeded));
  const machines = createClient(wire.MachinesService, transport);
  return {
    machines: {
      ...machines,
      async *exec(request: MessageInitShape<typeof wire.ExecRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ExecRequestSchema, request);
        if (query.argv.length < 1 || !query.argv[0] || query.argv.some(value => value.includes("\0"))) throw new RangeError("invalid exec argv");
        if (!Number.isInteger(query.timeoutMs) || query.timeoutMs < 1 || query.timeoutMs > 0xffff_ffff) throw new RangeError("timeoutMs must be a positive uint32");
        const controller = new AbortController();
        const signal = callOptions?.signal;
        const abort = () => controller.abort(signal?.reason);
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) abort();
        try {
          let exited = false;
          for await (const frame of machines.exec(query, { ...callOptions, signal: controller.signal, timeoutMs: callOptions?.timeoutMs ?? query.timeoutMs + 1000 })) {
            checkMachine(query.machine, frame.machine);
            if (exited) throw new ConnectError("exec frame follows terminal exit code", Code.DataLoss);
            if (frame.output.case === "exitCode") exited = true;
            else if (frame.output.case !== "data" || frame.output.value.byteLength < 1 || frame.output.value.byteLength > MAX_OUTPUT_READ_BYTES) throw new ConnectError("invalid exec output frame", Code.DataLoss);
            yield frame;
          }
          if (!exited) throw new ConnectError("exec stream ended without exit code", Code.DataLoss);
        } finally {
          // Connect's promise-client iterator does not forward return() to the RPC.
          controller.abort();
          signal?.removeEventListener("abort", abort);
        }
      },
      async readOutput(request: MessageInitShape<typeof wire.ReadOutputRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ReadOutputRequestSchema, request);
        if (query.offset < 0n || query.offset > MAX_OFFSET) throw new RangeError("offset must be uint64");
        query.limit = pageSize(query.limit, MAX_OUTPUT_READ_BYTES);
        const result = await machines.readOutput(query, callOptions);
        checkMachine(query.machine, result.machine);
        if (result.offset < query.offset || result.data.byteLength > query.limit || result.offset + BigInt(result.data.byteLength) > MAX_OFFSET) throw new ConnectError("invalid console output range", Code.DataLoss);
        return result;
      },
    },
    close: () => session.abort(),
  };
}

function checkMachine(request: wire.MachineId | undefined, response: wire.MachineId | undefined): void {
  if (!request || !response || request.value.length !== response.value.length || request.value.some((byte, index) => byte !== response.value[index])) throw new ConnectError("response machine identity differs from request", Code.DataLoss);
}
