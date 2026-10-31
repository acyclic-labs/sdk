/** Objects v1 clients: account-scoped, content-addressed blobs. Public messages are generated from the canonical descriptor. */
import { create, fromBinary, toBinary, type DescMessage, type MessageInitShape, type MessageShape } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { ObjectsV1Memory, ObjectsV1Request as RustRequest, objects_v1_digest, verify_objects_v1 } from "../generated/wasm/acyclic_objects_wasm.js";
import type { ObjectsV1BinaryReply } from "../generated/wasm/acyclic_objects_wasm.js";
export type { ObjectsV1BinaryReply } from "../generated/wasm/acyclic_objects_wasm.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";
import { snapshotBytes } from "@acyclic-labs/auth";
import { awaitSignal } from "@acyclic-labs/auth/credential";
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
  const ready = ensureObjectsWasm();
  if (ready !== undefined) await ready;
  return create(wire.DigestSchema, { blake3: objects_v1_digest(bytes) });
}

/** One original operation. Closing it settles a pending pull and releases its owner. */
export interface ObjectsV1Request {
  next(): Promise<ObjectsV1BinaryReply | null>;
  close(): void;
}

/** Canonical binary messages over the caller's existing account and credential owner. */
export interface ObjectsV1BinaryPort {
  /** Own input before returning. Availability replies carry canonical length-delimited
   * Protobuf chunks; unary replies carry one unframed message. */
  open(route: string, input: Uint8Array): ObjectsV1Request;
}

/** Register before verifying bytes. A change requests verification; it never
 * certifies existence, repair, integrity or durability. Close releases the source. */
export interface ObjectsV1Availability {
  cursor(): wire.AvailabilityCursor;
  changed(): Promise<wire.AvailabilityCursor>;
  close(): void;
}

/** One transport's original abort controller, shared by its pull and cancellation. */
export function objectsRequest(input: Uint8Array, start: (input: Uint8Array, controller: AbortController) => Promise<Uint8Array>): ObjectsV1Request {
  return objectsStreamRequest(input, async function* (bytes, controller) { yield await start(bytes, controller); });
}

/** Transport chunks retain one abort owner from opening through the final pull. */
export function objectsStreamRequest(input: Uint8Array, start: (input: Uint8Array, controller: AbortController) => AsyncIterable<Uint8Array>): ObjectsV1Request {
  let bytes: Uint8Array;
  try { bytes = snapshotBytes(input); }
  catch { throw new ObjectsV1Error(wire.ErrorCode.INVALID_ARGUMENT); }
  const controller = new AbortController();
  let closed = false;
  let pulling = false;
  let source: AsyncIterator<Uint8Array> | undefined;
  const close = () => {
    if (closed) return;
    closed = true;
    controller.abort(new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE));
    void source?.return?.().catch(() => {});
  };
  return {
    async next() {
      if (closed) return null;
      if (pulling) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
      pulling = true;
      try {
        source ??= start(bytes, controller)[Symbol.asyncIterator]();
        const reply = await awaitSignal(source.next(), controller.signal);
        if (reply.done) { close(); return null; }
        return { body: reply.value };
      } catch (error) { close(); throw error; }
      finally { pulling = false; }
    },
    close,
  };
}

/** Retire a WASM owner only after its outstanding borrow has returned. */
function rustRequest(request: RustRequest): ObjectsV1Request {
  let closed = false;
  let pending = 0;
  const close = () => {
    if (closed) return;
    closed = true;
    request.close();
    if (pending === 0) request.free();
  };
  return {
    async next() {
      if (closed) return null;
      if (pending !== 0) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
      pending++;
      try {
        const reply = await request.next();
        if (reply === null) close();
        return reply;
      } catch (error) { close(); throw objectsV1Error(error); }
      finally { pending--; if (closed) request.free(); }
    },
    close,
  };
}

/**
 * One canonical route per call. Every reply is checked against its request by
 * the same Rust rules the Rust clients use before it is returned.
 */
export abstract class ObjectsV1Provider implements ObjectsV1BinaryPort {
  /** Own one lazy request through this provider's original transport. */
  abstract open(route: string, input: Uint8Array): ObjectsV1Request;
  /** Cancellation covers initialization, credential resolution, opening and every
   * later wait. Supply a deadline signal when this observation has a deadline. */
  async availability(digest: wire.Digest, signal?: AbortSignal): Promise<ObjectsV1Availability> {
    signal?.throwIfAborted();
    const input = toBinary(wire.AvailabilityRequestSchema, create(wire.AvailabilityRequestSchema, { digest }));
    const ready = ensureObjectsWasm(signal);
    if (ready !== undefined) await ready;
    signal?.throwIfAborted();
    const request = rustRequest(RustRequest.observe(this, input));
    const stop = () => request.close();
    const close = () => { signal?.removeEventListener("abort", stop); stop(); };
    signal?.addEventListener("abort", stop, { once: true });
    if (signal?.aborted) stop();
    let current: Uint8Array;
    const next = async () => {
      try {
        signal?.throwIfAborted();
        const reply = await awaitSignal(request.next(), signal);
        if (reply === null) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
        current = reply.body;
        return fromBinary(wire.AvailabilityCursorSchema, current.slice());
      } catch (error) { close(); throw signal?.aborted ? signal.reason : objectsV1Error(error); }
    };
    await next();
    return { cursor: () => fromBinary(wire.AvailabilityCursorSchema, current.slice()), changed: next, close };
  }
  protected async call<I extends DescMessage, O extends DescMessage>(route: string, input: I, output: O, value: MessageInitShape<I>): Promise<MessageShape<O>> {
    const ready = ensureObjectsWasm();
    if (ready !== undefined) await ready;
    try {
      const bytes = toBinary(input, create(input, value));
      const request = this.open(route, bytes);
      try {
        const reply = await request.next();
        if (reply === null) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
        verify_objects_v1(route, bytes, reply.body);
        return fromBinary(output, reply.body);
      } finally { request.close(); }
    } catch (error) { throw objectsV1Error(error); }
  }
  /** Publishes `body` under its own digest; `references` must already exist. */
  async put(body: Uint8Array, references: readonly wire.Digest[] = []): Promise<wire.PutResponse> {
    const digest = await blobDigest(body);
    return this.call("blobs/put", wire.PutBlobRequestSchema, wire.PutResponseSchema, { digest, body, references: [...references] });
  }
  /**
   * Publishes many blobs in one call. Results answer members in order; a member that references or repeats an
   * earlier member starts once it settled, and a failed member (`error`) leaves every other member as reported.
   */
  async putBlobs(puts: readonly { readonly body: Uint8Array; readonly references?: readonly wire.Digest[] }[]): Promise<wire.PutResult[]> {
    const members = await Promise.all(puts.map(async ({ body, references = [] }) => ({ digest: await blobDigest(body), body, references: [...references] })));
    return (await this.call("blobs/put-batch", wire.PutBlobsRequestSchema, wire.PutBlobsResponseSchema, { puts: members })).results;
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
    const ready = ensureObjectsWasm();
    if (ready !== undefined) await ready;
    return new MemoryObjectsV1(new ObjectsV1Memory());
  }
  open(route: string, input: Uint8Array): ObjectsV1Request {
    return rustRequest(this.memory.open(route, input));
  }
}
