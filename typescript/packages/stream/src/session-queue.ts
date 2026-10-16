import { StreamError } from "./types.js";

/** Bounded asynchronous storage; wakeups come only from producers/consumers. */
export class SessionQueue<T> implements AsyncIterable<T> {
  readonly #values: T[] = [];
  readonly #capacity: number;
  readonly #readers = new Set<() => void>();
  readonly #writers = new Set<() => void>();
  #ended = false;
  #error: unknown;
  constructor(capacity: number) { this.#capacity = capacity; }
  get available(): number { return this.#capacity - this.#values.length; }
  push(value: T): void {
    if (this.#ended || this.available === 0) throw new StreamError("capacity", "session queue capacity exceeded");
    this.#values.push(value);
    this.#wake(this.#readers);
  }
  async send(value: T, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (this.#ended) throw this.#error ?? new StreamError("unavailable", "session ended");
    if (this.available > 0) { this.push(value); return; }
    await new Promise<void>((resolve, reject) => {
      const cleanup = (): void => { this.#writers.delete(wake); signal?.removeEventListener("abort", abort); };
      const wake = (): void => {
        cleanup();
        if (this.#ended) reject(this.#error ?? new StreamError("unavailable", "session ended"));
        else { this.push(value); resolve(); }
      };
      const abort = (): void => { cleanup(); reject(signal?.reason); };
      this.#writers.add(wake);
      signal?.addEventListener("abort", abort, { once: true });
    });
  }
  #wakeWriters(): void {
    // Admit older writes synchronously when space opens. A fresh write cannot
    // overtake a waiting interest revision between wakeup and resumption.
    for (const wake of this.#writers) {
      if (!this.#ended && this.available === 0) break;
      wake();
    }
  }
  end(error?: unknown): void {
    if (this.#ended) return;
    this.#ended = true;
    this.#error = error;
    this.#wake(this.#readers);
    this.#wakeWriters();
  }
  discard(dispose: (value: T) => void): void {
    const discarded = this.#values.splice(0);
    this.#wakeWriters();
    for (const value of discarded) dispose(value);
  }
  #wake(waiters: Set<() => void>): void {
    for (const wake of waiters) wake();
    waiters.clear();
  }
  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    while (true) {
      while (this.#values.length === 0 && !this.#ended) await new Promise<void>(resolve => this.#readers.add(resolve));
      if (this.#values.length === 0) {
        if (this.#error !== undefined) throw this.#error;
        return;
      }
      const value = this.#values.shift()!;
      this.#wakeWriters();
      yield value;
    }
  }
}
