/** Logical Objects v1 clients. Public messages are generated from the canonical descriptor. */
import { fromBinary, toBinary, type DescMessage, type MessageShape } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { ObjectsV1Memory, decode_objects_v1_body, validate_objects_v1_request, validate_objects_v1_response, validate_objects_v1_get_header } from "../generated/wasm/acyclic_objects_wasm.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";
export * from "../generated/proto/objects/v1/objects_pb.js";

/** A bounded buffered read; metadata remains the generated public wire type. */
export interface ObjectValue { readonly header: wire.GetObjectHeader; readonly body: Uint8Array }
export class ObjectsV1Error extends Error {
  constructor(readonly code: wire.ErrorCode) { super(`Objects operation failed (${wire.ErrorCode[code]})`); this.name = "ObjectsV1Error"; }
}
export function objectsV1Error(value: unknown): ObjectsV1Error {
  if (value instanceof ObjectsV1Error) return value;
  const code = typeof value === "object" && value !== null && "code" in value ? value.code : undefined;
  return new ObjectsV1Error(typeof code === "number" && code >= wire.ErrorCode.INVALID_ARGUMENT && code <= wire.ErrorCode.NOT_MODIFIED ? code : wire.ErrorCode.UNAVAILABLE);
}

/** Generated messages cross this interface without an independently maintained JSON contract. */
/** Largest bearer credential, in UTF-8 bytes, that an SDK client accepts; matches the Acyclic platform's maximum bearer (12 KiB). */
export const MAX_BEARER_TOKEN_BYTES = 12 * 1024;

/** Largest decoded_length one body frame may declare: 64 KiB plain, one 8 MiB stored block compressed. */
export function bodyDecodedLimit(body: wire.Body): bigint {
  switch (body.codec) {
    case wire.Codec.NONE: return BigInt(wire.ObjectsLimit.MAX_BODY_FRAME_BYTES);
    case wire.Codec.ZSTD: return BigInt(wire.ObjectsLimit.MAX_BODY_DECODED_BYTES);
    default: throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
  }
}

export abstract class ObjectsV1Provider {
  protected abstract invoke(route: string, bytes: Uint8Array, body: Uint8Array, maximum: bigint): Promise<readonly Uint8Array[]>;
  protected async call<I extends DescMessage, O extends DescMessage>(route: string, input: I, output: O, value: MessageShape<I>, body: Uint8Array = new Uint8Array(0)): Promise<MessageShape<O>> {
    await ensureObjectsWasm();
    try {
      const bytes = toBinary(input, value);
      validate_objects_v1_request(route, bytes, BigInt(body.byteLength));
      const frames = await this.invoke(route, bytes, body, 0n);
      const [frame] = frames;
      if (frames.length !== 1 || frame === undefined) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
      validate_objects_v1_response(route, bytes, frame, BigInt(body.byteLength));
      return fromBinary(output, frame);
    } catch (error) { throw objectsV1Error(error); }
  }
  createBucket(value: wire.CreateBucketRequest) { return this.call("buckets/create", wire.CreateBucketRequestSchema, wire.BucketSchema, value); }
  headBucket(value: wire.HeadBucketRequest) { return this.call("buckets/head", wire.HeadBucketRequestSchema, wire.BucketSchema, value); }
  deleteBucket(value: wire.DeleteBucketRequest) { return this.call("buckets/delete", wire.DeleteBucketRequestSchema, wire.DeleteBucketResponseSchema, value); }
  put(value: wire.PutObjectHeader, body: Uint8Array) { return this.call("objects/put", wire.PutObjectHeaderSchema, wire.ObjectInfoSchema, value, body); }
  head(value: wire.HeadObjectRequest) { return this.call("objects/head", wire.HeadObjectRequestSchema, wire.HeadObjectResponseSchema, value); }
  delete(value: wire.DeleteObjectRequest) { return this.call("objects/delete", wire.DeleteObjectRequestSchema, wire.DeleteObjectResponseSchema, value); }
  list(value: wire.ListObjectsRequest) { return this.call("objects/list", wire.ListObjectsRequestSchema, wire.ListObjectsResponseSchema, value); }
  createMultipart(value: wire.CreateMultipartRequest) { return this.call("multipart/create", wire.CreateMultipartRequestSchema, wire.MultipartUploadSchema, value); }
  uploadPart(value: wire.UploadPartHeader, body: Uint8Array) { return this.call("multipart/upload-part", wire.UploadPartHeaderSchema, wire.UploadedPartSchema, value, body); }
  listParts(value: wire.ListPartsRequest) { return this.call("multipart/list-parts", wire.ListPartsRequestSchema, wire.ListPartsResponseSchema, value); }
  completeMultipart(value: wire.CompleteMultipartRequest) { return this.call("multipart/complete", wire.CompleteMultipartRequestSchema, wire.ObjectInfoSchema, value); }
  abortMultipart(value: wire.AbortMultipartRequest) { return this.call("multipart/abort", wire.AbortMultipartRequestSchema, wire.AbortMultipartResponseSchema, value); }
  async get(value: wire.GetObjectRequest, maximumBytes: bigint): Promise<ObjectValue> {
    await ensureObjectsWasm();
    try {
      if (maximumBytes < 0n || maximumBytes > 0xffff_ffffn) throw new ObjectsV1Error(wire.ErrorCode.INVALID_ARGUMENT);
      const bytes = toBinary(wire.GetObjectRequestSchema, value);
      validate_objects_v1_request("objects/get", bytes, 0n);
      const frames = await this.invoke("objects/get", bytes, new Uint8Array(0), maximumBytes);
      let header: wire.GetObjectHeader | undefined;
      let length = 0;
      let expected = 0n;
      const chunks: Uint8Array[] = [];
      for (const frameBytes of frames) {
        const { frame } = fromBinary(wire.GetObjectResponseSchema, frameBytes);
        if (frame.case === "error") throw objectsV1Error({ code: frame.value.code });
        if (header === undefined) {
          if (frame.case !== "header" || frame.value.object === undefined) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
          header = frame.value;
          expected = validate_objects_v1_get_header(bytes, toBinary(wire.GetObjectHeaderSchema, header), maximumBytes);
        } else {
          if (frame.case !== "body") throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
          // Rust bounds the declared length by the frame limit and the bytes still selected before decompressing.
          const chunk = decode_objects_v1_body(toBinary(wire.BodySchema, frame.value), expected - BigInt(length));
          length += chunk.byteLength;
          if (BigInt(length) > maximumBytes) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
          chunks.push(chunk);
        }
      }
      if (header === undefined) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
      if (BigInt(length) !== expected) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
      const body = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
      return { header, body };
    } catch (error) { throw objectsV1Error(error); }
  }
}

/** The bounded Rust reference provider, usable in browsers, Node, and Bun. */
export class MemoryObjectsV1 extends ObjectsV1Provider {
  private constructor(private readonly memory: ObjectsV1Memory) { super(); }
  static async create(maximumBytes = 64n * 1024n * 1024n, maximumEntries = 10000): Promise<MemoryObjectsV1> {
    await ensureObjectsWasm();
    if (!Number.isSafeInteger(maximumEntries) || maximumEntries < 1 || maximumEntries > 0xffff_ffff) throw new ObjectsV1Error(wire.ErrorCode.INVALID_ARGUMENT);
    if (maximumBytes < 0n || maximumBytes > 0xffff_ffffn) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
    try { return new MemoryObjectsV1(new ObjectsV1Memory(maximumBytes, maximumEntries)); }
    catch (error) { throw objectsV1Error(error); }
  }
  protected invoke(route: string, bytes: Uint8Array, body: Uint8Array, maximum: bigint): Promise<readonly Uint8Array[]> {
    return this.memory.invoke(route, bytes, body, maximum);
  }
}
