import { credentialProvider, type CredentialOptions } from "@acyclic-labs/actors/credential";
import { rootCertificates } from "node:tls";
import { type MessageInitShape } from "@bufbuild/protobuf";
import { createClient, type CallOptions, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager, type GrpcTransportOptions } from "@connectrpc/connect-node";
import { ExportRequestSchema, FilesystemService } from "../generated/proto/filesystem/v2/filesystem_pb.js";

export interface FilesystemGrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  /** Optional caller message capacity; omission retains the wire encoding maximum. */
  readonly maximumMessageBytes?: number;
}

/** Node/Bun access to the public filesystem RPCs, including streaming import/export. */
export function createFilesystemGrpcClient(options: FilesystemGrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  const credential = credentialProvider(options);
  const maximum = options.maximumMessageBytes;
  if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 1)) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${await credential(request.signal)}`);
    return next(request);
  };
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
  const filesystem = createClient(FilesystemService, transport);
  return {
    filesystem: {
      ...filesystem,
      async *export(request: MessageInitShape<typeof ExportRequestSchema>, callOptions?: CallOptions) {
        const controller = new AbortController();
        const signal = callOptions?.signal;
        const abort = () => controller.abort(signal?.reason);
        signal?.addEventListener("abort", abort, { once: true });
        if (signal?.aborted) abort();
        try {
          yield* filesystem.export(request, { ...callOptions, signal: controller.signal });
        } finally {
          // Connect's promise-client iterator omits return(), so consumer exit needs an explicit abort.
          controller.abort();
          signal?.removeEventListener("abort", abort);
        }
      },
    },
    close: () => session.abort(),
  };
}
