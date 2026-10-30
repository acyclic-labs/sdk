import { credentialProvider, type CredentialOptions } from "@acyclic-labs/auth/credential";
import { credentialInterceptor } from "@acyclic-labs/auth/credential-connect";
import { rootCertificates } from "node:tls";
import { createGrpcTransport, Http2SessionManager, type GrpcTransportOptions } from "@connectrpc/connect-node";
import { createMachinesClient } from "./transport.js";
export { MAX_OUTPUT_READ_BYTES, type MachinesExecRequest } from "./transport.js";

export interface MachinesGrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  /** Optional caller message capacity; omission retains the wire encoding maximum. */
  readonly maximumMessageBytes?: number;
}

/** Node/Bun client for the Cloud product API's protobuf admissions and observations. */
export function createMachinesGrpcClient(options: MachinesGrpcOptions) {
  const { transport, close } = createMachinesGrpcTransport(options);
  return { ...createMachinesClient(transport), close };
}

/** Retains the same authenticated HTTP/2 owner for a typed or Rust client. */
export function createMachinesGrpcTransport(options: MachinesGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  const credential = credentialProvider(options);
  const maximum = options.maximumMessageBytes;
  if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 1)) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate = credentialInterceptor(credential);
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transportOptions: GrpcTransportOptions = {
    sessionManager: session, baseUrl: endpoint.href,
    acceptCompression: [], interceptors: [authenticate],
  };
  if (maximum !== undefined) {
    transportOptions.readMaxBytes = maximum;
    transportOptions.writeMaxBytes = maximum;
  }
  const transport = createGrpcTransport(transportOptions);
  return { transport, close: () => session.abort() };
}
