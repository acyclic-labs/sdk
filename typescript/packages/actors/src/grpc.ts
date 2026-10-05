import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ProtocolService } from "../generated/proto/transport/v1/transport_pb.js";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
import { ACTORS_HANDSHAKE, ACTORS_REMOTE_POLICY, rustOwnedGrpcHandshakeRequest, validateRustOwnedGrpcHandshake } from "./generated-client.js";
import { validateActorsCaCertificate, validateActorsCredential, validateActorsGrpcEndpoint, validateActorsMessageLimit } from "./wasm-runtime.js";

export interface ActorsGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Complete Actors v1 gRPC client for Node and Bun over authenticated HTTP/2. */
export function createActorsGrpcClient(options: ActorsGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  validateActorsGrpcEndpoint(options.endpoint);
  validateActorsCredential(options.token);
  const maximum = options.maximumMessageBytes ?? 16 * 1024 * 1024;
  validateActorsMessageLimit(maximum);
  if (options.caCertificate !== undefined) validateActorsCaCertificate(options.caCertificate);
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    request.header.set("acyclic-family", "actors");
    return next(request);
  };
  const tls = options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } };
  const control = createClient(ProtocolService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: 64 * 1024, writeMaxBytes: 64 * 1024, ...tls }));
  let handshake: Promise<void> | undefined;
  const applicationAuthenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    request.header.set("acyclic-family", "actors");
    if (handshake === undefined) {
      const pending = control.handshake(rustOwnedGrpcHandshakeRequest(ACTORS_HANDSHAKE, "actors"), { timeoutMs: ACTORS_REMOTE_POLICY.requestTimeoutMillis })
        .then(response => { validateRustOwnedGrpcHandshake(response, ACTORS_HANDSHAKE, "actors"); });
      const wrapped = pending.catch(error => { if (handshake === wrapped) handshake = undefined; throw error; });
      handshake = wrapped;
    }
    await handshake;
    return next(request);
  };
  return createClient(ActorsService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [applicationAuthenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...tls }));
}
