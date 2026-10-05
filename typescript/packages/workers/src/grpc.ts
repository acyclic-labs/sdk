import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ProtocolService } from "../generated/proto/transport/v1/transport_pb.js";
import { WorkersService } from "../generated/proto/workers/v1/workers_pb.js";
import { WORKERS_HANDSHAKE, WORKERS_REMOTE_POLICY, rustOwnedGrpcHandshakeRequest, validateRustOwnedGrpcHandshake } from "./generated-client.js";
import { validateWorkersCaCertificate, validateWorkersCredential, validateWorkersGrpcEndpoint, validateWorkersMessageLimit } from "./wasm-runtime.js";

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
  if (options.caCertificate !== undefined) validateWorkersCaCertificate(options.caCertificate);
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    request.header.set("acyclic-family", "workers");
    return next(request);
  };
  const tls = options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } };
  const control = createClient(ProtocolService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: 64 * 1024, writeMaxBytes: 64 * 1024, ...tls }));
  let handshake: Promise<void> | undefined;
  const applicationAuthenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    request.header.set("acyclic-family", "workers");
    if (handshake === undefined) {
      const pending = control.handshake(rustOwnedGrpcHandshakeRequest(WORKERS_HANDSHAKE, "workers"), { timeoutMs: WORKERS_REMOTE_POLICY.requestTimeoutMillis })
        .then(response => { validateRustOwnedGrpcHandshake(response, WORKERS_HANDSHAKE, "workers"); });
      const wrapped = pending.catch(error => { if (handshake === wrapped) handshake = undefined; throw error; });
      handshake = wrapped;
    }
    await handshake;
    return next(request);
  };
  return createClient(WorkersService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [applicationAuthenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...tls }));
}
