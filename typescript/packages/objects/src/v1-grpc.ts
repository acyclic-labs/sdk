import { credentialProvider, type CredentialOptions } from "@acyclic-labs/actors/credential";
import { rootCertificates } from "node:tls";
import { createClient, Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { observeInterceptors, resolveObserver, type AcyclicObserver } from "@acyclic-labs/actors/observe";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { BlobsService } from "../generated/proto/objects/v1/objects_pb.js";
import { ObjectsV1Error, ObjectsV1Provider, objectsV1Error } from "./v1.js";

/** Node/Bun transport configuration for the Objects service. */
export interface ObjectsV1GrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
  readonly observer?: AcyclicObserver;
  /** The caller's deadline for one call, in milliseconds; defaults to 30 s. */
  readonly timeoutMs?: number;
}

function grpcError(error: unknown): ObjectsV1Error {
  if (!(error instanceof ConnectError)) return objectsV1Error(error);
  const detail = error.findDetails(wire.ErrorDetailSchema).find(value => value.code in wire.ErrorCode && value.code !== wire.ErrorCode.UNSPECIFIED);
  if (detail !== undefined) return new ObjectsV1Error(detail.code);
  // The Rust client's classification of a status without a typed detail.
  const code = error.code === Code.InvalidArgument ? wire.ErrorCode.INVALID_ARGUMENT : error.code === Code.NotFound ? wire.ErrorCode.NOT_FOUND : error.code === Code.PermissionDenied || error.code === Code.Unauthenticated ? wire.ErrorCode.ACCESS_DENIED : error.code === Code.ResourceExhausted ? wire.ErrorCode.QUOTA_EXCEEDED : error.code === Code.OutOfRange ? wire.ErrorCode.RANGE_NOT_SATISFIABLE : error.code === Code.Unimplemented ? wire.ErrorCode.UNSUPPORTED : error.code === Code.DataLoss ? wire.ErrorCode.CORRUPT : error.code === Code.DeadlineExceeded ? wire.ErrorCode.DEADLINE_ELAPSED : wire.ErrorCode.UNAVAILABLE;
  return new ObjectsV1Error(code);
}

/** The generated Blobs service client over one owned HTTP/2 session. */
export function createObjectsV1GrpcClients(options: ObjectsV1GrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  const credential = credentialProvider(options);
  const maximum = options.maximumMessageBytes ?? 64 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  const timeoutMs = options.timeoutMs ?? 30000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 0x7fff_ffff) throw new RangeError("timeoutMs must be a positive int32");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${await credential(request.signal)}`);
    return next(request);
  };
  // Bun on Windows prematurely closes large compressed response streams in the local TLS fixture.
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transport = createGrpcTransport({ sessionManager: session, defaultTimeoutMs: timeoutMs, acceptCompression: [], baseUrl: endpoint.href, interceptors: observeInterceptors([authenticate], resolveObserver(options.observer), "objects"), readMaxBytes: maximum, writeMaxBytes: maximum });
  return { blobs: createClient(BlobsService, transport), close: () => session.abort() };
}

/** Rust-verified provider over the generated gRPC Blobs service. */
export class GrpcObjectsV1 extends ObjectsV1Provider {
  private readonly clients: ReturnType<typeof createObjectsV1GrpcClients>;
  constructor(options: ObjectsV1GrpcOptions) {
    super();
    this.clients = createObjectsV1GrpcClients(options);
  }
  /** Releases the owned HTTP/2 connection and cancels outstanding calls. */
  close(): void { this.clients.close(); }
  protected async invoke(route: string, input: Uint8Array): Promise<Uint8Array> {
    const blobs = this.clients.blobs;
    const unary = async <I extends DescMessage, O extends DescMessage>(request: I, response: O, call: (value: MessageShape<I>) => Promise<MessageShape<O>>) => toBinary(response, await call(fromBinary(request, input)));
    try {
      switch (route) {
        case "blobs/put": return await unary(wire.PutBlobRequestSchema, wire.PutResponseSchema, value => blobs.putBlob(value));
        case "blobs/put-manifest": return await unary(wire.PutManifestRequestSchema, wire.PutResponseSchema, value => blobs.putManifest(value));
        case "blobs/get": return await unary(wire.GetBlobRequestSchema, wire.GetBlobResponseSchema, value => blobs.getBlob(value));
        case "blobs/head": return await unary(wire.HeadBlobRequestSchema, wire.BlobInfoSchema, value => blobs.headBlob(value));
        case "blobs/delete": return await unary(wire.DeleteBlobRequestSchema, wire.DeleteBlobResponseSchema, value => blobs.deleteBlob(value));
        case "blobs/list": return await unary(wire.ListBlobsRequestSchema, wire.ListBlobsResponseSchema, value => blobs.listBlobs(value));
        case "blobs/retain": return await unary(wire.RetainRequestSchema, wire.RetainResponseSchema, value => blobs.retain(value));
        case "blobs/release": return await unary(wire.ReleaseRequestSchema, wire.ReleaseResponseSchema, value => blobs.release(value));
        case "blobs/collect": return await unary(wire.CollectRequestSchema, wire.CollectResponseSchema, value => blobs.collect(value));
        case "blobs/usage": return await unary(wire.UsageRequestSchema, wire.UsageResponseSchema, value => blobs.usage(value));
        default: throw new ObjectsV1Error(wire.ErrorCode.INVALID_ARGUMENT);
      }
    } catch (error) {
      throw grpcError(error);
    }
  }
}
