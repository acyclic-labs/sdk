import { wireRequest, validateWireRequest, wireWhenAbsent } from "./contract.js";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { projectMemoryResponse, WasmSession, type WasmFollow, type WasmApplicationDelivery } from "../generated/wasm/acyclic_stream_wasm.js";
import { ensureStreamWasm, validateSequenceValue, streamResource } from "./contract.js";
import { streamError } from "./wasm-error.js";
import { RevisionSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { StreamError, commitId, type EncodedRecord, type FollowOptions, type Revision } from "./types.js";

export interface SessionTransport {
  readonly incoming: AsyncIterable<wire.SessionFrame>;
  /** Abort withdraws a write that has not yet entered the transport queue. */
  send(frame: wire.SessionFrame, signal?: AbortSignal): Promise<void>;
  close(): void;
}
export type SessionConnect = (node: wire.SessionNode | undefined, signal: AbortSignal) => Promise<SessionTransport>;

/** A requested filter is intersected with the selected credential by the node. */
export interface SessionOptions {
  readonly filter?: NonNullable<wire.SessionSubscription["filter"]>;
  readonly credential?: number;
  readonly signal?: AbortSignal;
}

function validateCredentialSlot(slot: number): void {
  if (!Number.isInteger(slot) || slot < 0 || slot > 0xffff_ffff) throw new RangeError("credential slot must be an unsigned 32-bit integer");
}

/** Platform I/O adapter; all session state and recovery belong to Rust. */
export class StreamSessions {
  readonly #inner: WasmSession;
  constructor(connect: SessionConnect, acknowledge = true) {
    this.#inner = new WasmSession(async (bytes: Uint8Array | null, signal: AbortSignal) => {
      const transport = await connect(bytes === null ? undefined : fromBinary(wire.SessionNodeSchema, bytes), signal);
      if (signal.aborted) { transport.close(); throw signal.reason; }
      const incoming = transport.incoming[Symbol.asyncIterator]();
      return {
        async next() {
          const item = await incoming.next();
          return item.done ? null : toBinary(wire.SessionFrameSchema, item.value);
        },
        send(bytes: Uint8Array, writeSignal: AbortSignal) { return transport.send(fromBinary(wire.SessionFrameSchema, bytes), writeSignal); },
        close() { transport.close(); },
      };
    }, acknowledge);
  }

  /** The pool's read-your-writes floor: one revision per observed authority, never a max. */
  revisionFloor(): Revision[] {
    return (this.#inner.revisionFloor() as Uint8Array[]).map(bytes => fromBinary(RevisionSchema, bytes));
  }

  /** Raises the floor with a revision a receipt or response served; lower revisions never lower it. */
  observe(revision: Revision | undefined): void {
    if (revision !== undefined) this.#inner.observeRevision(toBinary(RevisionSchema, create(RevisionSchema, revision)));
  }

  /** Accepts a direct read or tail reply served against `minimum`: its applied revision must prove it, then raises the floor. */
  served(minimum: readonly Revision[], applied: Revision | undefined): void {
    const encode = (revision: Revision) => toBinary(RevisionSchema, create(RevisionSchema, revision));
    try { this.#inner.servedRevision(minimum.map(encode), applied === undefined ? undefined : encode(applied)); }
    catch (error) { throw streamError(error, "session"); }
  }

  /** Drops the floor after an authority refused it as foreign. */
  refused(error: unknown): void {
    if (error instanceof StreamError && error.code === "revision_foreign") this.#inner.revisionRefused();
  }

  async heads(options: import("./types.js").HeadsRequest, credential = 0): Promise<import("./types.js").Heads> {
    validateCredentialSlot(credential);
    const request = { kind: "heads" as const, ...structuredClone(options) };
    await validateWireRequest(request);
    try { return projectMemoryResponse("heads", await this.#inner.heads(wireRequest(request), credential)) as import("./types.js").Heads; }
    catch (error) { throw streamError(error, "heads"); }
  }

  async registerCredential(slot: number, credential: Uint8Array): Promise<void> {
    validateCredentialSlot(slot);
    try { this.#inner.registerCredential(slot, credential); }
    catch (error) { throw streamError(error, "session"); }
  }

  async *follow(path: string, options: FollowOptions & SessionOptions): AsyncGenerator<EncodedRecord> {
    for await (const data of this.followData(create(wire.SessionFollowSchema, { resource: streamResource(path), delivery: wire.SessionDelivery.ALL, whenAbsent: wireWhenAbsent(options.whenAbsent) }), options)) {
      if (data.item.case !== "record") throw new Error("Rust ALL cursor returned a non-record item");
      const record = data.item.value;
      yield { sequence: record.sequence, value: record.value, keys: record.keys, attributes: record.attributes, commitId: commitId(record.commitId), committedAtMicros: record.committedAtMicros };
    }
  }

  followData(follow: wire.SessionFollow, options: FollowOptions & SessionOptions): AsyncGenerator<wire.SessionData> {
    const { from } = options;
    validateSequenceValue(from);
    const subscription = create(wire.SessionSubscriptionSchema, {
      resume: { next: from }, kind: { case: "follow", value: follow },
      ...(options.filter === undefined ? {} : { filter: options.filter }),
      credential: options.credential ?? 0,
    });
    return this.#data(this.subscribe(subscription, options.signal));
  }

  async *#data(frames: AsyncIterable<wire.SessionFrame>): AsyncGenerator<wire.SessionData> {
    for await (const frame of frames) {
      if (frame.body.case === "watermark") continue;
      if (frame.body.case !== "data") throw new Error("Rust follow cursor returned a non-data frame");
      yield frame.body.value;
    }
  }





  subscribe(subscription: wire.SessionSubscription, signal?: AbortSignal): AsyncGenerator<wire.SessionFrame> {
    validateCredentialSlot(subscription.credential);
    const input = toBinary(wire.SessionSubscriptionSchema, subscription);
    return this.#cursor(() => this.#inner.subscribe(input), signal,
      bytes => fromBinary(wire.SessionFrameSchema, bytes));
  }

  /** Explicit application completion; an abandoned delivery never commits its checkpoint. */
  async *deliveries(subscription: wire.SessionSubscription, signal?: AbortSignal): AsyncGenerator<SessionDelivery> {
    validateCredentialSlot(subscription.credential);
    const input = toBinary(wire.SessionSubscriptionSchema, structuredClone(subscription));
    if (signal?.aborted) return;
    await ensureStreamWasm();
    const cursor = this.#inner.deliveries(input);
    const abort = (): void => cursor.close();
    let delivery: SessionDelivery | undefined;
    try {
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) return;
      for (;;) {
        const owned = await cursor.next();
        if (owned === undefined) return;
        delivery = new SessionDelivery(owned);
        if (signal?.aborted) return;
        yield delivery;
      }
    } catch (error) {
      if (!signal?.aborted) throw streamError(error, "session");
    } finally {
      signal?.removeEventListener("abort", abort);
      delivery?.close();
      cursor.close();
      cursor.free();
    }
  }

  async *#cursor<T>(open: () => WasmFollow, signal: AbortSignal | undefined,
    project: (bytes: Uint8Array) => T): AsyncGenerator<T> {
    if (signal?.aborted) return;
    await ensureStreamWasm();
    if (signal?.aborted) return;
    let cursor: WasmFollow | undefined;
    const abort = (): void => cursor?.close();
    try {
      cursor = open();
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) return;
      for (;;) {
        const bytes = await cursor.next();
        if (signal?.aborted || bytes === null) return;
        yield project(bytes);
      }
    } catch (error) {
      if (!signal?.aborted) throw streamError(error, "session");
    } finally {
      signal?.removeEventListener("abort", abort);
      cursor?.close();
      cursor?.free();
    }
  }
}

/** One consumer decision on a Rust-owned delivery, independent of transport credit. */
export class SessionDelivery {
  readonly frame: wire.SessionFrame;
  readonly before: wire.SessionFrame | undefined;
  /** Persist with the consumer's effects or discard decision before completion. */
  readonly checkpoint: wire.SessionSubscription;
  #inner: WasmApplicationDelivery | undefined;
  constructor(inner: WasmApplicationDelivery) {
    this.#inner = inner;
    this.frame = fromBinary(wire.SessionFrameSchema, inner.frame());
    const before = inner.before();
    this.before = before === undefined ? undefined : fromBinary(wire.SessionFrameSchema, before);
    this.checkpoint = fromBinary(wire.SessionSubscriptionSchema, inner.checkpoint());
  }
  /** False means a reconnect fenced this decision and the item must replay. */
  commit(): boolean { return this.#complete(false); }
  discard(): boolean { return this.#complete(true); }
  close(): void { const inner = this.#inner; this.#inner = undefined; inner?.close(); }
  #complete(discard: boolean): boolean {
    const inner = this.#inner;
    if (inner === undefined) throw new StreamError("invalid_argument", "delivery was already decided or abandoned");
    this.#inner = undefined;
    try { return discard ? inner.discard() : inner.commit(); }
    catch (error) { throw streamError(error, "session"); }
  }
}
