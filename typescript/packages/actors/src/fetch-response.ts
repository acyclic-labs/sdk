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
  let stop!: (cause: unknown) => void;
  const stopped = new Promise<never>((_resolve, reject) => { stop = reject; });
  void stopped.catch(() => undefined);
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let closed = false;
  const terminate = (cause: unknown) => {
    if (closed) return;
    closed = true;
    signal?.removeEventListener("abort", close);
    stop(cause);
    if (reader) void reader.cancel(cause).catch(() => undefined);
    else if (response.body && !response.body.locked) void response.body.cancel(cause).catch(() => undefined);
    if ("close" in response) response.close();
  };
  const close = () => {
    if (!closed) terminate(signal?.reason ?? new DOMException("fetch response closed", "AbortError"));
  };
  // A failed terminal producer must also wake an in-flight body read. One
  // lifetime handler retains the original cause; no reaction is added per chunk.
  void terminal.catch(cause => {
    try { terminate(cause); } catch { /* The producer failure remains primary. */ }
  });
  signal?.addEventListener("abort", close, { once: true });
  if (signal?.aborted) close();
  let consumed = false;
  async function* consume(): AsyncGenerator<GuestResponseFrame> {
    try {
      if (closed) await stopped;
      reader = response.body?.getReader();
      if (reader) {
        for (;;) {
          // reader.cancel settles its pending read. Do not attach one more
          // reaction to a long-lived cancellation promise on every chunk.
          let part: ReadableStreamReadResult<Uint8Array>;
          try {
            part = await reader.read();
          } catch (cause) {
            // A read can already be rejected when the original owner stops.
            // Preserve that owner's first cause instead of the queued error.
            if (closed) await stopped;
            throw cause;
          }
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
  }
  const frames = {
    [Symbol.asyncIterator](): AsyncIterator<GuestResponseFrame> {
      if (consumed) throw new TypeError("fetch response already consumed");
      consumed = true;
      const iterator = consume();
      return {
        next: () => iterator.next(),
        async return(value?: unknown) {
          // Generator return queues behind an outstanding next and never enters
          // finally before the first next. Cancel the original owner first so
          // either a body read or the terminal wait can settle without EOF.
          close();
          return iterator.return(value);
        },
        async throw(cause: unknown) {
          terminate(cause);
          return iterator.throw(cause);
        },
      };
    },
  };
  return { status: response.status, headers: response.headers, frames, close };
}
