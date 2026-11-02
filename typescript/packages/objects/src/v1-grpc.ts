import { credentialInterceptor } from "@acyclic-labs/auth/credential-connect";
import { credentialProvider, type CredentialOptions } from "@acyclic-labs/actors/credential";
import { rootCertificates } from "node:tls";
import { createClient, ConnectError } from "@connectrpc/connect";
import { fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { createGrpcTransport, Http2SessionManager, type GrpcTransportOptions } from "@connectrpc/connect-node";
import { observeInterceptors, resolveObserver, type AcyclicObserver } from "@acyclic-labs/actors/observe";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { BlobsService } from "../generated/proto/objects/v1/objects_pb.js";
import { ObjectsV1Request as RustRequest, objects_v1_grpc_error_code } from "../generated/wasm/acyclic_objects_wasm.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";
import { ObjectsV1Error, ObjectsV1Provider, objectsV1Error, objectsRequest, objectsStreamRequest, type ObjectsV1Request } from "./v1.js";

/** Node/Bun transport configuration for the Objects service. */
export interface ObjectsV1GrpcOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly caCertificate?: string;
  /** Optional caller message capacity; omission retains the wire encoding maximum. */
  readonly maximumMessageBytes?: number;
  readonly observer?: AcyclicObserver;
  /** Optional caller deadline in milliseconds; omitted or zero is unbounded. */
  readonly timeoutMs?: number;
}

function grpcError(error: unknown): ObjectsV1Error {
  if (!(error instanceof ConnectError)) return objectsV1Error(error);
  const detail = error.findDetails(wire.ErrorDetailSchema).find(value => value.code in wire.ErrorCode && value.code !== wire.ErrorCode.UNSPECIFIED);
  if (detail !== undefined) return new ObjectsV1Error(detail.code);
  return new ObjectsV1Error(objects_v1_grpc_error_code(error.code));
}

/** The generated Blobs service client over one owned HTTP/2 session. */
export function createObjectsV1GrpcClients(options: ObjectsV1GrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  const credential = credentialProvider(options);
  const maximum = options.maximumMessageBytes;
  if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 1)) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  const timeoutMs = options.timeoutMs;
  if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 0x7fff_ffff)) throw new RangeError("timeoutMs must be a non-negative int32");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate = credentialInterceptor(credential);
  // Bun on Windows prematurely closes large compressed response streams in the local TLS fixture.
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transportOptions: GrpcTransportOptions = { sessionManager: session, acceptCompression: [], baseUrl: endpoint.href, interceptors: observeInterceptors([authenticate], resolveObserver(options.observer), "objects") };
  if (timeoutMs !== undefined) transportOptions.defaultTimeoutMs = timeoutMs;
  if (maximum !== undefined) {
    transportOptions.readMaxBytes = maximum;
    transportOptions.writeMaxBytes = maximum;
  }
  const transport = createGrpcTransport(transportOptions);
  return { blobs: createClient(BlobsService, transport), close: () => session.abort() };
}

/** Rust-verified provider over the generated gRPC Blobs service. */
export class GrpcObjectsV1 extends ObjectsV1Provider {
  private readonly clients: ReturnType<typeof createObjectsV1GrpcClients>;
  private readonly timeoutMs: number | undefined;
  constructor(options: ObjectsV1GrpcOptions) {
    super();
    this.clients = createObjectsV1GrpcClients(options);
    this.timeoutMs = options.timeoutMs;
  }
  /** Releases the owned HTTP/2 connection and cancels outstanding calls. */
  close(): void { this.clients.close(); }
  open(route: string, input: Uint8Array): ObjectsV1Request {
    if (route === "blobs/availability") {
      const blobs = this.clients.blobs;
      return objectsStreamRequest(input, async function* (bytes, controller) {
        const ready = ensureObjectsWasm(controller.signal);
        if (ready !== undefined) await ready;
        try {
          for await (const cursor of blobs.availability(fromBinary(wire.AvailabilityRequestSchema, bytes), { signal: controller.signal, timeoutMs: 0 })) {
            yield RustRequest.frame(toBinary(wire.AvailabilityCursorSchema, cursor));
          }
        } catch (error) { throw controller.signal.aborted ? controller.signal.reason : grpcError(error); }
      }, this.timeoutMs);
    }
    return objectsRequest(input, (bytes, controller) => this.send(route, bytes, controller.signal));
  }
  private async send(route: string, input: Uint8Array, signal: AbortSignal): Promise<Uint8Array> {
    const ready = ensureObjectsWasm(signal);
    if (ready !== undefined) await ready;
    const blobs = this.clients.blobs;
    const unary = async <I extends DescMessage, O extends DescMessage>(request: I, response: O, call: (value: MessageShape<I>) => Promise<MessageShape<O>>) => toBinary(response, await call(fromBinary(request, input)));
    try {
      switch (route) {
        case "blobs/put": return await unary(wire.PutBlobRequestSchema, wire.PutResponseSchema, value => blobs.putBlob(value, { signal }));
        case "blobs/put-batch": return await unary(wire.PutBlobsRequestSchema, wire.PutBlobsResponseSchema, value => blobs.putBlobs(value, { signal }));
        case "blobs/put-manifest": return await unary(wire.PutManifestRequestSchema, wire.PutResponseSchema, value => blobs.putManifest(value, { signal }));
        case "blobs/get": return await unary(wire.GetBlobRequestSchema, wire.GetBlobResponseSchema, value => blobs.getBlob(value, { signal }));
        case "blobs/head": return await unary(wire.HeadBlobRequestSchema, wire.BlobInfoSchema, value => blobs.headBlob(value, { signal }));
        case "blobs/delete": return await unary(wire.DeleteBlobRequestSchema, wire.DeleteBlobResponseSchema, value => blobs.deleteBlob(value, { signal }));
        case "blobs/list": return await unary(wire.ListBlobsRequestSchema, wire.ListBlobsResponseSchema, value => blobs.listBlobs(value, { signal }));
        case "blobs/retain": return await unary(wire.RetainRequestSchema, wire.RetainResponseSchema, value => blobs.retain(value, { signal }));
        case "blobs/release": return await unary(wire.ReleaseRequestSchema, wire.ReleaseResponseSchema, value => blobs.release(value, { signal }));
        case "blobs/collect": return await unary(wire.CollectRequestSchema, wire.CollectResponseSchema, value => blobs.collect(value, { signal }));
        case "blobs/usage": return await unary(wire.UsageRequestSchema, wire.UsageResponseSchema, value => blobs.usage(value, { signal }));
        default: throw new ObjectsV1Error(wire.ErrorCode.INVALID_ARGUMENT);
      }
    } catch (error) {
      throw grpcError(error);
    }
  }
}
