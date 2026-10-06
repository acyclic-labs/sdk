import { rootCertificates } from "node:tls";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { MachinesService } from "../generated/proto/machines/v1/machines_pb.js";
import { MACHINES_REMOTE_POLICY, createMachinesGrpcClient } from "./generated-client.js";
import type { RustOwnedGrpcInvoker, RustOwnedGrpcMethodMetadata } from "./generated-client.js";

/** Native Machines gRPC configuration. Rust owns the RPC and domain mapping. */
export interface MachinesGrpcOptions {
  readonly endpoint: string;
  readonly caCertificate: string;
  readonly certificate: string;
  readonly privateKey: string;
  readonly maximumMessageBytes?: number;
}

/**
 * Creates the generated Machines RPC surface for Node and Bun.
 *
 * The returned methods expose the generated protobuf RPC surface. Use
 * `Machines.fromEnv()` for the public domain provider backed by Rust.
 */
export function createMachinesGrpcInvoker(options: MachinesGrpcOptions): RustOwnedGrpcInvoker {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
    throw new TypeError("Machines gRPC endpoint must be HTTPS without credentials, query, or fragment");
  }
  const caCertificate = validateCertificate(options.caCertificate, "caCertificate");
  const certificate = validateCertificate(options.certificate, "certificate");
  const privateKey = validateCertificate(options.privateKey, "privateKey");
  const maximum = options.maximumMessageBytes ?? MACHINES_REMOTE_POLICY.maximumMessageBytes;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  const client = createClient(MachinesService, createGrpcTransport({
    baseUrl: endpoint.href,
    readMaxBytes: maximum,
    writeMaxBytes: maximum,
    nodeOptions: { ca: [...rootCertificates, caCertificate], cert: certificate, key: privateKey },
  }));
  return {
    invokeGrpc<TRequest, TResponse>(method: RustOwnedGrpcMethodMetadata, request: TRequest) {
      const operation = method.rpcName.charAt(0).toLowerCase() + method.rpcName.slice(1) as keyof typeof client;
      const call = client[operation];
      if (typeof call !== "function" || method.serverStreaming) throw new TypeError(`Machines RPC ${method.rpcName} is not unary`);
      return (call as unknown as (request: TRequest) => Promise<TResponse>)(request);
    },
    invokeGrpcStream<TRequest, TResponse>(method: RustOwnedGrpcMethodMetadata, request: TRequest) {
      const operation = method.rpcName.charAt(0).toLowerCase() + method.rpcName.slice(1) as keyof typeof client;
      const call = client[operation];
      if (typeof call !== "function" || !method.serverStreaming) throw new TypeError(`Machines RPC ${method.rpcName} is not server streaming`);
      return (call as unknown as (request: TRequest) => AsyncIterable<TResponse>)(request);
    },
  };
}

/** Creates the generated protobuf RPC object with the Rust method table. */
export function createMachinesGrpcClientNative(options: MachinesGrpcOptions) {
  return createMachinesGrpcClient(createMachinesGrpcInvoker(options));
}

function validateCertificate(value: string, name: string): string {
  if (typeof value !== "string" || value.length === 0 || new TextEncoder().encode(value).byteLength > 64 * 1024) {
    throw new RangeError(`${name} must be a nonempty certificate under 64 KiB`);
  }
  return value;
}
