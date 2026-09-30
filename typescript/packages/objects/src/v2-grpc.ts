import { rootCertificates } from "node:tls";
import { createClient, Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { create, fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { BucketsService, ObjectsService, MultipartService } from "../generated/proto/objects/v2/objects_pb.js";
/** Node/Bun transport configuration for the logical Objects service. */
export interface ObjectsV2GrpcOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}
import * as wire from "../generated/proto/objects/v2/objects_pb.js";
import { ObjectsV2Error, ObjectsV2Provider, objectsV2Error } from "./v2.js";
import { validate_objects_v2_get_header, validate_objects_v2_request, validate_objects_v2_response } from "../generated/wasm/acyclic_objects_wasm.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";

function grpcError(error: unknown): ObjectsV2Error {
  if (!(error instanceof ConnectError)) return objectsV2Error(error);
  const detail = error.findDetails(wire.ErrorDetailSchema).find(value => value.code >= wire.ErrorCode.INVALID_ARGUMENT && value.code <= wire.ErrorCode.NOT_MODIFIED);
  if (detail !== undefined) return new ObjectsV2Error(detail.code);
  const code = error.code === Code.InvalidArgument ? wire.ErrorCode.INVALID_ARGUMENT : error.code === Code.NotFound ? wire.ErrorCode.NOT_FOUND : error.code === Code.AlreadyExists ? wire.ErrorCode.ALREADY_EXISTS : error.code === Code.FailedPrecondition ? wire.ErrorCode.PRECONDITION_FAILED : error.code === Code.ResourceExhausted ? wire.ErrorCode.QUOTA_EXCEEDED : error.code === Code.PermissionDenied || error.code === Code.Unauthenticated ? wire.ErrorCode.ACCESS_DENIED : error.code === Code.Unimplemented ? wire.ErrorCode.UNSUPPORTED : wire.ErrorCode.UNAVAILABLE;
  return new ObjectsV2Error(code);
}

/** Complete Node/Bun clients, including client-streaming PUT/parts and server-streaming GET. */
export function createObjectsV2GrpcClients(options: ObjectsV2GrpcOptions) {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("gRPC endpoint must be HTTPS without credentials, query, or fragment");
  if (!options.token.trim() || new TextEncoder().encode(options.token).byteLength > 8192 || /[\r\n\0]/.test(options.token)) throw new TypeError("invalid bearer token");
  const maximum = options.maximumMessageBytes ?? 16 * 1024 * 1024;
  if (!Number.isSafeInteger(maximum) || maximum < 1) throw new RangeError("maximumMessageBytes must be a positive safe integer");
  if (options.caCertificate !== undefined && (options.caCertificate.length === 0 || new TextEncoder().encode(options.caCertificate).byteLength > 64 * 1024)) throw new RangeError("invalid private CA certificate");
  const authenticate: Interceptor = next => async request => {
    request.header.set("authorization", `Bearer ${options.token}`);
    return next(request);
  };
  // Bun on Windows prematurely closes large compressed response streams in the local TLS fixture.
  // Identity encoding preserves gRPC streaming in both supported runtimes.
  const session = new Http2SessionManager(endpoint, {}, options.caCertificate === undefined ? {} : { ca: [...rootCertificates, options.caCertificate] });
  const transport = createGrpcTransport({ sessionManager: session, defaultTimeoutMs: 30000, acceptCompression: [], baseUrl: endpoint.href, interceptors: [authenticate], readMaxBytes: maximum, writeMaxBytes: maximum });
  return { buckets: createClient(BucketsService, transport), objects: createClient(ObjectsService, transport), multipart: createClient(MultipartService, transport), close: () => session.abort() };
}

/** Rust-validated provider with buffered operations and bounded streamed uploads. */
export class GrpcObjectsV2 extends ObjectsV2Provider {
  private readonly clients: ReturnType<typeof createObjectsV2GrpcClients>;
  constructor(options: ObjectsV2GrpcOptions, private readonly maximumResponseBytes = 64 * 1024 * 1024) {
    super();
    if (!Number.isSafeInteger(maximumResponseBytes) || maximumResponseBytes < 1) throw new RangeError("invalid response bound");
    this.clients = createObjectsV2GrpcClients(options);
  }
  /** Releases the owned HTTP/2 connection and cancels outstanding calls. */
  close(): void { this.clients.close(); }
  /** Streams caller chunks with backpressure; source failure or cancellation prevents completion. */
  putStream(header: wire.PutObjectHeader, body: AsyncIterable<Uint8Array>, signal?: AbortSignal): Promise<wire.ObjectInfo> {
    return this.upload("objects/put", wire.PutObjectHeaderSchema, wire.ObjectInfoSchema, header, body, signal,
      (chunks, signal, header) => {
        async function* frames() {
          yield create(wire.PutObjectRequestSchema, { frame: { case: "header", value: header } });
          for await (const chunk of chunks) yield create(wire.PutObjectRequestSchema, { frame: { case: "body", value: chunk } });
          yield create(wire.PutObjectRequestSchema, { frame: { case: "complete", value: true } });
        }
        return this.clients.objects.putObject(frames(), { signal });
      });
  }
  /** Streams one staged part; failure preserves its previously published receipt. */
  uploadPartStream(header: wire.UploadPartHeader, body: AsyncIterable<Uint8Array>, signal?: AbortSignal): Promise<wire.UploadedPart> {
    return this.upload("multipart/upload-part", wire.UploadPartHeaderSchema, wire.UploadedPartSchema, header, body, signal,
      (chunks, signal, header) => {
        async function* frames() {
          yield create(wire.UploadPartRequestSchema, { frame: { case: "header", value: header } });
          for await (const chunk of chunks) yield create(wire.UploadPartRequestSchema, { frame: { case: "body", value: chunk } });
          yield create(wire.UploadPartRequestSchema, { frame: { case: "complete", value: true } });
        }
        return this.clients.multipart.uploadPart(frames(), { signal });
      });
  }
  private async upload<I extends DescMessage, O extends DescMessage>(
    route: string, input: I, output: O, header: MessageShape<I>, body: AsyncIterable<Uint8Array>, signal: AbortSignal | undefined,
    call: (chunks: AsyncIterable<Uint8Array>, signal: AbortSignal, header: MessageShape<I>) => Promise<MessageShape<O>>,
  ): Promise<MessageShape<O>> {
    await ensureObjectsWasm();
    const bytes = toBinary(input, header);
    try { validate_objects_v2_request(route, bytes, 0n); } catch (error) { throw objectsV2Error(error); }
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) controller.abort();
    let length = 0n;
    let complete = false;
    let sourceError: ObjectsV2Error | undefined;
    async function* chunks() {
      const iterator = body[Symbol.asyncIterator]();
      let rejectAbort: (reason: unknown) => void = () => {};
      const aborted = new Promise<never>((_, reject) => { rejectAbort = reject; });
      // A pending caller source must not prevent the RPC from being cancelled.
      const cancelled = () => rejectAbort(new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE));
      controller.signal.addEventListener("abort", cancelled, { once: true });
      if (controller.signal.aborted) cancelled();
      try {
        while (true) {
          const item = await Promise.race([iterator.next(), aborted]);
          if (item.done) { complete = true; return; }
          if (!(item.value instanceof Uint8Array)) throw new ObjectsV2Error(wire.ErrorCode.INVALID_ARGUMENT);
          length += BigInt(item.value.byteLength);
          validate_objects_v2_request(route, bytes, length);
          for (let offset = 0; offset < item.value.byteLength; offset += 65536) {
            yield item.value.subarray(offset, offset + 65536);
          }
        }
      } catch (error) {
        sourceError = objectsV2Error(error);
        controller.abort();
        throw sourceError;
      } finally {
        controller.signal.removeEventListener("abort", cancelled);
        // A caller iterator may still be awaiting its own work; do not delay RPC cancellation.
        if (!complete && iterator.return !== undefined) void Promise.resolve().then(() => iterator.return!()).catch(() => {});
      }
    }
    try {
      const value = await call(chunks(), controller.signal, fromBinary(input, bytes));
      if (!complete) throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
      validate_objects_v2_response(route, bytes, toBinary(output, value), length);
      return value;
    } catch (error) {
      if (sourceError !== undefined) throw sourceError;
      throw grpcError(error);
    } finally {
      controller.abort();
      signal?.removeEventListener("abort", abort);
    }
  }
  protected async invoke(route: string, bytes: Uint8Array, body: Uint8Array, maximum: bigint): Promise<readonly Uint8Array[]> {
    const unary = async <I extends DescMessage, O extends DescMessage>(input: I, output: O, call: (value: MessageShape<I>) => Promise<MessageShape<O>>) => [toBinary(output, await call(fromBinary(input, bytes)))];
    try {
      switch (route) {
        case "buckets/create": return await unary(wire.CreateBucketRequestSchema, wire.BucketSchema, value => this.clients.buckets.createBucket(value));
        case "buckets/head": return await unary(wire.HeadBucketRequestSchema, wire.BucketSchema, value => this.clients.buckets.headBucket(value));
        case "buckets/delete": return await unary(wire.DeleteBucketRequestSchema, wire.DeleteBucketResponseSchema, value => this.clients.buckets.deleteBucket(value));
        case "objects/head": return await unary(wire.HeadObjectRequestSchema, wire.HeadObjectResponseSchema, value => this.clients.objects.headObject(value));
        case "objects/delete": return await unary(wire.DeleteObjectRequestSchema, wire.DeleteObjectResponseSchema, value => this.clients.objects.deleteObject(value));
        case "objects/list": return await unary(wire.ListObjectsRequestSchema, wire.ListObjectsResponseSchema, value => this.clients.objects.listObjects(value));
        case "multipart/create": return await unary(wire.CreateMultipartRequestSchema, wire.MultipartUploadSchema, value => this.clients.multipart.createMultipart(value));
        case "multipart/list-parts": return await unary(wire.ListPartsRequestSchema, wire.ListPartsResponseSchema, value => this.clients.multipart.listParts(value));
        case "multipart/complete": return await unary(wire.CompleteMultipartRequestSchema, wire.ObjectInfoSchema, value => this.clients.multipart.completeMultipart(value));
        case "multipart/abort": return await unary(wire.AbortMultipartRequestSchema, wire.AbortMultipartResponseSchema, value => this.clients.multipart.abortMultipart(value));
        case "objects/put": {
          const header = fromBinary(wire.PutObjectHeaderSchema, bytes);
          async function* source() { yield body; }
          return [toBinary(wire.ObjectInfoSchema, await this.putStream(header, source()))];
        }
        case "multipart/upload-part": {
          const header = fromBinary(wire.UploadPartHeaderSchema, bytes);
          async function* source() { yield body; }
          return [toBinary(wire.UploadedPartSchema, await this.uploadPartStream(header, source()))];
        }
        case "objects/get": {
          const frames: Uint8Array[] = [];
          let size = 0;
          let remaining: bigint | undefined;
          let rejected: ObjectsV2Error | undefined;
          const controller = new AbortController();
          const iterator = this.clients.objects.getObject(fromBinary(wire.GetObjectRequestSchema, bytes), { signal: controller.signal })[Symbol.asyncIterator]();
          try {
          for await (const frame of { [Symbol.asyncIterator]: () => iterator }) {
            if (rejected === undefined) {
              try {
                if (frame.frame.case === "error") throw objectsV2Error({ code: frame.frame.value.code });
                if (remaining === undefined) {
                  if (frame.frame.case !== "header") throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
                  remaining = validate_objects_v2_get_header(bytes, toBinary(wire.GetObjectHeaderSchema, frame.frame.value), maximum);
                } else {
                  if (frame.frame.case !== "body" || frame.frame.value.byteLength > 65536 || BigInt(frame.frame.value.byteLength) > remaining) throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
                  remaining -= BigInt(frame.frame.value.byteLength);
                }
              } catch (error) { rejected = objectsV2Error(error); }
            }
            const encoded = toBinary(wire.GetObjectResponseSchema, frame);
            size += encoded.byteLength;
            if (size > this.maximumResponseBytes) {
              controller.abort();
              throw new ObjectsV2Error(wire.ErrorCode.QUOTA_EXCEEDED);
            }
            if (rejected === undefined) frames.push(encoded);
          }
          if (rejected !== undefined) throw rejected;
          if (remaining !== 0n) throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
          return frames;
          } finally {
            controller.abort();
            // Connect's response iterator omits return(); observe cancellation to release its deadline.
            try { while (!(await iterator.next()).done) { /* discard already queued frames */ } } catch { /* cancellation is expected */ }
          }
        }
        default: throw new ObjectsV2Error(wire.ErrorCode.INVALID_ARGUMENT);
      }
    } catch (error) {
      throw grpcError(error);
    }
  }
}
