import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { BucketsService, ObjectsService, MultipartService, SnapshotsService } from "../generated/proto/objects/v1/objects_pb.js";

export interface ObjectsGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Complete Objects v1 clients, including client-streaming uploads and server-streaming reads. */
export function createObjectsGrpcClients(options: ObjectsGrpcOptions) {
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
  const transport = createGrpcTransport({ baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: maximum, writeMaxBytes: maximum, ...(options.caCertificate === undefined ? {} : { nodeOptions: { ca: [...rootCertificates, options.caCertificate] } }) });
  return {
    buckets: createClient(BucketsService, transport),
    objects: createClient(ObjectsService, transport),
    multipart: createClient(MultipartService, transport),
    snapshots: createClient(SnapshotsService, transport),
  };
}
