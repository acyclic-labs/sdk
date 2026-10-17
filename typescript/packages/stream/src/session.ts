import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { ChildrenRequestSchema, ChildrenResponseSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { WasmSession, type WasmFollow } from "../generated/wasm/acyclic_stream_wasm.js";
import { ensureStreamWasm, validateSequenceValue } from "./contract.js";
import { streamError } from "./wasm-error.js";
import { commitId, type EncodedRecord, type FollowOptions, type ChildrenRequest, type StreamChild } from "./types.js";

export interface SessionTransport {
  readonly incoming: AsyncIterable<wire.SessionFrame>;
  /** Abort withdraws a write that has not yet entered the transport queue. */
  send(frame: wire.SessionFrame, signal?: AbortSignal): Promise<void>;
  close(): void;
}
export type SessionConnect = (node: wire.SessionNode | undefined, signal: AbortSignal) => Promise<SessionTransport>;

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

  async *follow(path: string, options: FollowOptions): AsyncGenerator<EncodedRecord> {
    for await (const data of this.followData(create(wire.SessionFollowSchema, { path, delivery: wire.SessionDelivery.ALL }), options)) {
      if (data.item.case !== "record") throw new Error("Rust ALL cursor returned a non-record item");
      const record = data.item.value;
      yield { sequence: record.sequence, value: record.value, commitId: commitId(record.commitId), committedAtMicros: record.committedAtMicros };
    }
  }

  followData(follow: wire.SessionFollow, options: FollowOptions): AsyncGenerator<wire.SessionData> {
    const input = toBinary(wire.SessionFollowSchema, follow);
    const { from, signal } = options;
    validateSequenceValue(from);
    return this.#cursor(() => this.#inner.followData(input, from), signal,
      bytes => fromBinary(wire.SessionDataSchema, bytes));
  }

  children(request: ChildrenRequest): AsyncGenerator<StreamChild> {
    const { parent, signal, limit } = request;
    const input = toBinary(ChildrenRequestSchema, create(ChildrenRequestSchema, {
      limit, ...(parent === undefined ? {} : { parent }),
    }));
    return this.#cursor(() => this.#inner.children(input), signal, bytes => {
      const child = fromBinary(ChildrenResponseSchema, bytes).child;
      if (child === undefined) throw new Error("Rust child cursor omitted a child");
      return child;
    });
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
