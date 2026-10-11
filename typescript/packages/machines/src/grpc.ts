import { rootCertificates } from "node:tls";
import { createClient, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { MachinesService } from "../generated/proto/machines/v1/machines_pb.js";

export interface MachinesGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Node/Bun client for the Cloud product API's protobuf admissions and observations. */
export function createMachinesGrpcClient(options: MachinesGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  if (!options.token.trim() || new TextEncoder().encode(options.token).byteLength > 12 * 1024 || /[\r\n\0]/.test(options.token)) throw new TypeError("invalid bearer token");
  const maximum = options.maximumMessageBytes ?? 64 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    return next(request);
  };
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transport = createGrpcTransport({
    sessionManager: session, baseUrl: endpoint.href, defaultTimeoutMs: 30000,
    acceptCompression: [], interceptors: [authenticate],
    readMaxBytes: maximum, writeMaxBytes: maximum,
  });
  return { machines: createClient(MachinesService, transport), close: () => session.abort() };
}
