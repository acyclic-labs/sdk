import type { GuestFetchResponse } from "./generated/guest.js";

/** HTTP body frames in source order. A trailer frame is terminal. */
export type GuestResponseFrame = { readonly data: Uint8Array } | { readonly trailers: Headers };

/** One response lifetime on local and hosted transports. Consume frames lazily,
 * publish trailers after data, and call close on transport failure/disconnect.
 * Neither a Response copy nor initial headers can carry terminal trailers. */
export function openGuestResponse(response: GuestFetchResponse, signal?: AbortSignal): {
  readonly status: number;
  readonly headers: Headers;
  readonly frames: AsyncIterable<GuestResponseFrame>;
  close(): void;
} {
  const terminal = "trailers" in response ? response.trailers : Promise.resolve(new Headers());
  // A producer can fail before its consumer starts pulling. Retain that failure
  // for the terminal await without creating an unhandled rejection meanwhile.
  void terminal.catch(() => undefined);
  let stop!: (cause: unknown) => void;
  const stopped = new Promise<never>((_resolve, reject) => { stop = reject; });
  void stopped.catch(() => undefined);
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let closed = false;
  const close = () => {
    if (closed) return;
    closed = true;
    signal?.removeEventListener("abort", close);
    const cause = signal?.reason ?? new DOMException("fetch response closed", "AbortError");
    stop(cause);
    if (reader) void reader.cancel(cause).catch(() => undefined);
    else if (response.body && !response.body.locked) void response.body.cancel(cause).catch(() => undefined);
    if ("close" in response) response.close();
  };
  signal?.addEventListener("abort", close, { once: true });
  if (signal?.aborted) close();
  let consumed = false;
  const frames = {
    async *[Symbol.asyncIterator](): AsyncGenerator<GuestResponseFrame> {
      if (consumed) throw new TypeError("fetch response already consumed");
      consumed = true;
      try {
        if (closed) await stopped;
        reader = response.body?.getReader();
        if (reader) {
          for (;;) {
            // reader.cancel settles its pending read. Do not attach one more
            // reaction to a long-lived cancellation promise on every chunk.
            const part = await reader.read();
            if (closed) await stopped;
            if (part.done) break;
            yield { data: part.value };
          }
        }
        const trailers = await Promise.race([terminal, stopped]);
        close();
        yield { trailers };
      } finally {
        close();
        reader?.releaseLock();
      }
    },
  };
  return { status: response.status, headers: response.headers, frames, close };
}
