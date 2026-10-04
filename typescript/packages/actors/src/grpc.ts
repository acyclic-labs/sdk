import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";
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
    return next(request);
  };
  return createClient(ActorsService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...(options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } }) }));
}
