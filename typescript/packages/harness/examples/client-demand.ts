import { DemandLoader, RequestScheduler, type AgentFileRef, type PageWindow } from "../src/index.js";

/** Attach bounded text hydration to the existing Rust-owned page window.
 * The host reader authenticates the immutable reference, verifies its digest,
 * and enforces maximumBytes before buffering. Paging alone never reads bodies.
 */
export function conversationDemand(options: {
  scheduler: RequestScheduler;
  window: PageWindow;
  readBody: (file: AgentFileRef, signal: AbortSignal, maximumBytes: number) => Promise<Uint8Array>;
  demand: number;
  bodyEntries: number;
  bodyBytes: number;
}) {
  if (!Number.isSafeInteger(options.bodyBytes) || options.bodyBytes < 1) throw new RangeError("bodyBytes");
  const maximumBytes = options.bodyBytes;
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const bodies = new DemandLoader<AgentFileRef, string>({
    reserve: file => {
      const length = file.descriptor.byte_length;
      // Check the exact Rust u64 against the finite policy before Number conversion.
      // Input, decoded UTF-16 text and fixed retained bookkeeping coexist.
      if (typeof length !== "bigint" || length < 0n || length * 3n + 96n > BigInt(maximumBytes)) {
        throw new RangeError("body exceeds hydration budget");
      }
      return Number(length * 3n + 96n);
    },
    load: async (file, signal, reservedBytes) => {
      const limit = Math.floor((reservedBytes - 96) / 3);
      const bytes = await options.readBody(file, signal, limit);
      if (BigInt(bytes.byteLength) !== file.descriptor.byte_length || bytes.buffer.byteLength > limit) {
        throw new RangeError("body backing allocation differs from admitted reference");
      }
      const value = decoder.decode(bytes);
      return { value, bytes: value.length * 2 + 96 };
    },
  }, options.scheduler, options.demand, options.bodyEntries, maximumBytes);
  return { window: options.window, bodies };
}
