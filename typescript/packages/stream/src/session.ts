import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { WasmSession, type WasmFollow } from "../generated/wasm/acyclic_stream_wasm.js";
import { ensureStreamWasm, validateSequenceValue, streamResource } from "./contract.js";
import { streamError } from "./wasm-error.js";
import { commitId, type EncodedRecord, type FollowOptions, type ChildrenRequest, type StreamChild } from "./types.js";

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

  async registerCredential(slot: number, credential: Uint8Array): Promise<void> {
    validateCredentialSlot(slot);
    const captured = credential.slice();
    await ensureStreamWasm();
    try { this.#inner.registerCredential(slot, captured); }
    catch (error) { throw streamError(error, "session"); }
  }

  async *follow(path: string, options: FollowOptions & SessionOptions): AsyncGenerator<EncodedRecord> {
    for await (const data of this.followData(create(wire.SessionFollowSchema, { resource: streamResource(path), delivery: wire.SessionDelivery.ALL }), options)) {
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

  children(request: ChildrenRequest & SessionOptions): AsyncGenerator<StreamChild> {
    const { parent, signal, limit } = request;
    const subscription = create(wire.SessionSubscriptionSchema, {
      resume: { next: 0n }, credential: request.credential ?? 0,
      ...(request.filter === undefined ? {} : { filter: request.filter }),
      kind: { case: "children", value: { limit, ...(parent === undefined ? {} : { parent }) } },
    });
    return this.#children(this.#data(this.subscribe(subscription, signal)));
  }

  async *#children(data: AsyncIterable<wire.SessionData>): AsyncGenerator<StreamChild> {
    for await (const item of data) {
      if (item.item.case !== "child") throw new Error("Rust child cursor omitted a child");
      yield item.item.value;
    }
  }

  subscribe(subscription: wire.SessionSubscription, signal?: AbortSignal): AsyncGenerator<wire.SessionFrame> {
    validateCredentialSlot(subscription.credential);
    const input = toBinary(wire.SessionSubscriptionSchema, subscription);
    return this.#cursor(() => this.#inner.subscribe(input), signal,
      bytes => fromBinary(wire.SessionFrameSchema, bytes));
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
