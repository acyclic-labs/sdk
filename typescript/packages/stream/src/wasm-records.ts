import { fromBinary } from "@bufbuild/protobuf";
import { WasmOpened, type WasmProviderRequest } from "../generated/wasm/acyclic_stream_wasm.js";
import { SessionOpenedSchema } from "../generated/proto/stream/v1/session_pb.js";
import { Opened, closeAfterFailure, streamOpening } from "./opened.js";
import { recordsFromFrames } from "./record.js";
import { streamError } from "./wasm-error.js";
import { StreamError, type ReadOptions } from "./types.js";

/** Keeps registration and the selected cursor under one cancellation owner. */
export async function openWasmCursor(
  open: (signal: AbortSignal) => Promise<WasmProviderRequest>, signal: AbortSignal,
) {
  let pending: WasmProviderRequest | undefined;
  let handle: WasmOpened | undefined;
  let pulling: Promise<Uint8Array | null> | undefined;
  let closing: Promise<void> | undefined;
  const abort = (): void => { pending?.close(); handle?.close(); };
  const close = (): Promise<void> => {
    if (closing !== undefined) return closing;
    signal.removeEventListener("abort", abort);
    pending?.close(); pending?.free(); pending = undefined;
    handle?.close();
    const retained = handle; handle = undefined;
    closing = Promise.resolve(pulling).catch(() => {}).then(() => retained?.free());
    return closing;
  };
  signal.addEventListener("abort", abort, { once: true });
  try {
    signal.throwIfAborted();
    pending = await open(signal);
    signal.throwIfAborted();
    const result = await pending.next();
    pending.close(); pending.free(); pending = undefined;
    if (!(result instanceof WasmOpened)) {
      signal.throwIfAborted();
      throw new StreamError("invalid_response", "Rust stream open omitted its selected cursor");
    }
    handle = result;
    signal.throwIfAborted();
    const cursor = handle;
    return {
      opened: fromBinary(SessionOpenedSchema, cursor.opened()),
      next: async (): Promise<Uint8Array | null> => {
        if (closing !== undefined) return null;
        if (pulling !== undefined) throw new StreamError("invalid_argument", "stream already has a pending pull");
        const result = cursor.next();
        pulling = result;
        try { return await result; } finally { pulling = undefined; }
      },
      close,
    };
  } catch (error) { await closeAfterFailure(error, close); throw error; }
}

/** Owns the same Rust request through registration, receipt and cursor disposal. */
export function wasmRecords(
  open: (signal: AbortSignal) => Promise<WasmProviderRequest>, path: string,
  operation: "read" | "follow" | "session",
  read?: ReadOptions, signal?: AbortSignal, maximumResponseBytes?: number,
): Opened {
  if (maximumResponseBytes !== undefined && (!Number.isSafeInteger(maximumResponseBytes) || maximumResponseBytes < 1)) {
    throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }
  return new Opened(async signal => {
    let owned: Awaited<ReturnType<typeof openWasmCursor>> | undefined;
    try {
      owned = await openWasmCursor(open, signal);
      const opening = streamOpening(owned.opened);
      const cursor = owned;
      const frames = async function* (): AsyncGenerator<Uint8Array> {
        let remaining = maximumResponseBytes;
        for (;;) {
          let bytes: Uint8Array | null;
          try { bytes = await cursor.next(); }
          catch (error) { throw streamError(error, operation); }
          if (bytes === null || signal.aborted) return;
          if (remaining !== undefined) {
            if (bytes.byteLength > remaining) throw new StreamError("response_too_large", "response exceeds configured bound");
            remaining -= bytes.byteLength;
          }
          yield bytes;
        }
      };
      return { opening, records: recordsFromFrames(frames(), path, opening.selection.from, read, signal), close: owned.close };
    } catch (error) {
      await closeAfterFailure(error, () => owned?.close());
      if (signal.aborted) throw error;
      throw streamError(error, operation);
    }
  }, signal);
}
