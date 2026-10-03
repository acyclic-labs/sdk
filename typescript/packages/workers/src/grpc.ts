import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { WorkersService } from "../generated/proto/workers/v1/workers_pb.js";
import { validateWorkersCredential, validateWorkersGrpcEndpoint, validateWorkersMessageLimit } from "./wasm-runtime.js";

export interface WorkersGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Complete Workers v1 gRPC client for Node and Bun over authenticated HTTP/2. */
export function createWorkersGrpcClient(options: WorkersGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  validateWorkersGrpcEndpoint(options.endpoint);
  validateWorkersCredential(options.token);
  const maximum = options.maximumMessageBytes ?? 16 * 1024 * 1024;
  validateWorkersMessageLimit(maximum);
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    return next(request);
  };
  return createClient(WorkersService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...(options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } }) }));
}
