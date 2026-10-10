/** One immutable selected value, shared by every framework adapter. */
export interface ClientSnapshot<Value> {
  getSnapshot(): Value;
  subscribe(listener: () => void): () => void;
}

/** Cached external store. Construction and reads never attach upstream effects. */
export class SnapshotStore<Value> implements ClientSnapshot<Value> {
  #value: Value;
  readonly #listeners = new Set<() => void>();
  #detach: (() => void) | undefined;
  #disposed = false;

  constructor(initial: Value, readonly attach?: (publish: (value: Value) => void) => () => void,
    readonly onError: (error: unknown) => void = error => { throw error; }) {
    this.#value = initial;
  }

  getSnapshot = (): Value => this.#value;

  publish = (value: Value): void => { this.stage(value)?.(); };

  /** Refresh a cache before notifying; callers can stage related stores together. */
  stage(value: Value): (() => void) | undefined {
    if (this.#disposed || Object.is(value, this.#value)) return;
    this.#value = value;
    return () => {
      if (this.#disposed || !Object.is(value, this.#value)) return;
      // A removed callback is never called later in the same publication.
      for (const listener of [...this.#listeners]) {
        if (this.#listeners.has(listener)) {
          try { listener(); }
          catch (error) { queueMicrotask(() => this.onError(error)); }
        }
      }
    };
  }

  subscribe = (listener: () => void): (() => void) => {
    if (this.#disposed) throw new Error("snapshot store is disposed");
    // Each subscription owns a distinct registration, even for the same callback.
    const registration = (): void => listener();
    this.#listeners.add(registration);
    if (this.#listeners.size === 1 && this.attach !== undefined) {
      try { this.#detach = this.attach(this.publish); }
      catch (error) { this.#listeners.delete(registration); throw error; }
    }
    let active = true;
    return () => {
      if (!active) return;
      active = false;
      this.#listeners.delete(registration);
      if (this.#listeners.size === 0) {
        const detach = this.#detach;
        this.#detach = undefined;
        detach?.();
      }
    };
  };

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#listeners.clear();
    const detach = this.#detach;
    this.#detach = undefined;
    detach?.();
  }
}
