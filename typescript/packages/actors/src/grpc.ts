import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { ActorsService } from "../generated/proto/actors/v1/actors_pb.js";

export interface ActorsGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Complete Actors v1 gRPC client for Node and Bun over authenticated HTTP/2. */
export function createActorsGrpcClient(options: ActorsGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  if (!options.token.trim() || /[\r\n]/.test(options.token)) throw new TypeError("invalid bearer token");
  const maximum = options.maximumMessageBytes ?? 16 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    return next(request);
  };
  return createClient(ActorsService, createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...(options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } }) }));
}
