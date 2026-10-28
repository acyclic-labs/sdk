/** Objects v1 clients: account-scoped, content-addressed blobs. Public messages are generated from the canonical descriptor. */
import { create, fromBinary, toBinary, type DescMessage, type MessageInitShape, type MessageShape } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { ObjectsV1Memory, objects_v1_digest, verify_objects_v1 } from "../generated/wasm/acyclic_objects_wasm.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";
export * from "../generated/proto/objects/v1/objects_pb.js";

export class ObjectsV1Error extends Error {
  constructor(readonly code: wire.ErrorCode) { super(`Objects operation failed (${wire.ErrorCode[code]})`); this.name = "ObjectsV1Error"; }
}
export function objectsV1Error(value: unknown): ObjectsV1Error {
  if (value instanceof ObjectsV1Error) return value;
  const code = typeof value === "object" && value !== null && "code" in value ? value.code : undefined;
  return new ObjectsV1Error(typeof code === "number" && code in wire.ErrorCode && code !== wire.ErrorCode.UNSPECIFIED ? code : wire.ErrorCode.UNAVAILABLE);
}

/** Largest bearer credential, in UTF-8 bytes, that an SDK client accepts; matches the Acyclic platform's maximum bearer (12 KiB). */
export const MAX_BEARER_TOKEN_BYTES = 12 * 1024;

/** The BLAKE3-256 digest naming `bytes` as a plain blob. */
export async function blobDigest(bytes: Uint8Array): Promise<wire.Digest> {
  await ensureObjectsWasm();
  return create(wire.DigestSchema, { blake3: objects_v1_digest(bytes) });
}

/**
 * One canonical route per call. Every reply is checked against its request by
 * the same Rust rules the Rust clients use before it is returned.
 */
export abstract class ObjectsV1Provider {
  /** Sends one route's binary input message and returns its binary output message. */
  protected abstract invoke(route: string, input: Uint8Array): Promise<Uint8Array>;
  protected async call<I extends DescMessage, O extends DescMessage>(route: string, input: I, output: O, value: MessageInitShape<I>): Promise<MessageShape<O>> {
    await ensureObjectsWasm();
    try {
      const bytes = toBinary(input, create(input, value));
      const reply = await this.invoke(route, bytes);
      verify_objects_v1(route, bytes, reply);
      return fromBinary(output, reply);
    } catch (error) { throw objectsV1Error(error); }
  }
  /** Publishes `body` under its own digest; `references` must already exist. */
  async put(body: Uint8Array, references: readonly wire.Digest[] = []): Promise<wire.PutResponse> {
    const digest = await blobDigest(body);
    return this.call("blobs/put", wire.PutBlobRequestSchema, wire.PutResponseSchema, { digest, body, references: [...references] });
  }
  putManifest(manifest: MessageInitShape<typeof wire.ManifestSchema>) { return this.call("blobs/put-manifest", wire.PutManifestRequestSchema, wire.PutResponseSchema, { manifest }); }
  get(value: MessageInitShape<typeof wire.GetBlobRequestSchema>) { return this.call("blobs/get", wire.GetBlobRequestSchema, wire.GetBlobResponseSchema, value); }
  head(digest: wire.Digest) { return this.call("blobs/head", wire.HeadBlobRequestSchema, wire.BlobInfoSchema, { digest }); }
  delete(digest: wire.Digest) { return this.call("blobs/delete", wire.DeleteBlobRequestSchema, wire.DeleteBlobResponseSchema, { digest }); }
  list(value: MessageInitShape<typeof wire.ListBlobsRequestSchema>) { return this.call("blobs/list", wire.ListBlobsRequestSchema, wire.ListBlobsResponseSchema, value); }
  retain(root: Uint8Array, references: readonly wire.Digest[]) { return this.call("blobs/retain", wire.RetainRequestSchema, wire.RetainResponseSchema, { root, references: [...references] }); }
  release(root: Uint8Array) { return this.call("blobs/release", wire.ReleaseRequestSchema, wire.ReleaseResponseSchema, { root }); }
  collect() { return this.call("blobs/collect", wire.CollectRequestSchema, wire.CollectResponseSchema, {}); }
  usage() { return this.call("blobs/usage", wire.UsageRequestSchema, wire.UsageResponseSchema, {}); }
}

/** The verified digest of a durable put, or `undefined` when its outcome is unknown. */
export function verified(put: wire.PutResponse): wire.Verified | undefined {
  return put.outcome.case === "created" || put.outcome.case === "alreadyExists" ? put.outcome.value : undefined;
}

/** The Rust in-memory account, usable in browsers, Node, and Bun. */
export class MemoryObjectsV1 extends ObjectsV1Provider {
  private constructor(private readonly memory: ObjectsV1Memory) { super(); }
  static async create(): Promise<MemoryObjectsV1> {
    await ensureObjectsWasm();
    return new MemoryObjectsV1(new ObjectsV1Memory());
  }
  protected invoke(route: string, input: Uint8Array): Promise<Uint8Array> {
    return this.memory.invoke(route, input);
  }
}
