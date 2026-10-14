import { rootCertificates } from "node:tls";
import { type MessageInitShape } from "@bufbuild/protobuf";
import { createClient, type CallOptions, type Interceptor } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { ExportRequestSchema, FilesystemService } from "../generated/proto/filesystem/v2/filesystem_pb.js";

export interface FilesystemGrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

/** Node/Bun access to the public filesystem RPCs, including streaming import/export. */
export function createFilesystemGrpcClient(options: FilesystemGrpcOptions) {
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
