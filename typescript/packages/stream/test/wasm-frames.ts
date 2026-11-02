import { openWasmCursor } from "../src/wasm-records.js";
import type { WasmProviderRequest } from "../generated/wasm/acyclic_stream_wasm.js";

/** Consume the actual Rust request owner rather than emulate its selected cut. */
export const openCursor = (request: WasmProviderRequest, signal = new AbortController().signal) =>
  openWasmCursor(async () => request, signal);

export async function readFrames(request: WasmProviderRequest): Promise<Uint8Array[]> {
  const cursor = await openCursor(request);
  const frames: Uint8Array[] = [];
  try {
    for (;;) {
      const frame = await cursor.next();
      if (frame === null) return frames;
      frames.push(frame);
    }
  } finally { await cursor.close(); }
}
