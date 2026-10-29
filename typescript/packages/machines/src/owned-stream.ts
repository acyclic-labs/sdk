// Connect's promise-client iterator does not forward return() to the RPC.
// Own cancellation outside the generator so return() also interrupts a pending next().
export function ownedStream<T>(open: (signal: AbortSignal) => AsyncIterable<T>, parent: AbortSignal | undefined, terminal: (value: T) => boolean): AsyncIterableIterator<T> & { return(): Promise<IteratorResult<T>> } {
  let iterator: AsyncIterator<T> | undefined;
  let controller: AbortController | undefined;
  let closed = false;
  const abort = () => controller?.abort(parent?.reason);
  const close = () => {
    closed = true;
    controller?.abort();
    parent?.removeEventListener("abort", abort);
  };
  return {
    [Symbol.asyncIterator]() { return this; },
    async next() {
      if (closed) return { done: true, value: undefined };
      if (!iterator) {
        controller = new AbortController();
        parent?.addEventListener("abort", abort, { once: true });
        if (parent?.aborted) abort();
        iterator = open(controller.signal)[Symbol.asyncIterator]();
      }
      try {
        const result = await iterator.next();
        if (result.done || terminal(result.value)) {
          close();
          await iterator.return?.();
        }
        return result;
      } catch (cause) {
        close();
        throw cause;
      }
    },
    async return() {
      close();
      await iterator?.return?.();
      return { done: true, value: undefined };
    },
  };
}
