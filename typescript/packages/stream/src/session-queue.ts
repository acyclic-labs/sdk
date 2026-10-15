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
  async send(value: T): Promise<void> {
    while (!this.#ended && this.available === 0) await new Promise<void>(resolve => this.#writers.add(resolve));
    if (this.#ended) throw this.#error ?? new StreamError("unavailable", "session ended");
    this.push(value);
  }
  end(error?: unknown): void {
    if (this.#ended) return;
    this.#ended = true;
    this.#error = error;
    this.#wake(this.#readers);
    this.#wake(this.#writers);
  }
  discard(dispose: (value: T) => void): void {
    for (const value of this.#values.splice(0)) dispose(value);
    this.#wake(this.#writers);
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
      this.#wake(this.#writers);
      yield value;
    }
  }
}
