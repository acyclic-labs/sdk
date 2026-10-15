/** One immutable selected value, shared by every framework adapter. */
export interface ClientSnapshot<Value> {
  getSnapshot(): Value;
  subscribe(listener: () => void): () => void;
}

type Registration = { readonly listener: () => void; active: boolean };

/** Cached external store. Construction and reads never attach upstream effects. */
export class SnapshotStore<Value> implements ClientSnapshot<Value> {
  #value: Value;
  readonly #listeners = new Set<Registration>();
  #publicationListeners: readonly Registration[] | undefined;
  #detach: (() => void) | undefined;
  #disposed = false;

  constructor(initial: Value, readonly attach?: (publish: (value: Value) => void) => () => void,
    readonly onError: (error: unknown) => void = error => { throw error; }) {
    this.#value = initial;
  }

  getSnapshot = (): Value => this.#value;

  publish = (value: Value): void => {
    if (this.#disposed || Object.is(value, this.#value)) return;
    this.#value = value;
    this.#notify();
  };

  /** Refresh a cache before notifying; callers can stage related stores together. */
  stage(value: Value): (() => void) | undefined {
    if (this.#disposed || Object.is(value, this.#value)) return;
    this.#value = value;
    return () => {
      if (this.#disposed || !Object.is(value, this.#value)) return;
      this.#notify();
    };
  }

  #notify(): void {
    if (this.#listeners.size === 0) return;
    const listeners = this.#publicationListeners ??= [...this.#listeners];
    for (const registration of listeners) {
      if (this.#disposed) break;
      // A removed callback is never called later in the same publication.
      if (registration.active) {
        const listener = registration.listener;
        try { listener(); }
        catch (error) { queueMicrotask(() => this.onError(error)); }
      }
    }
  }

  subscribe = (listener: () => void): (() => void) => {
    if (this.#disposed) throw new Error("snapshot store is disposed");
    // Each subscription owns a distinct registration, even for the same callback.
    const registration = { listener, active: true };
    this.#listeners.add(registration);
    this.#publicationListeners = undefined;
    if (this.#listeners.size === 1 && this.attach !== undefined) {
      try { this.#detach = this.attach(this.publish); }
      catch (error) {
        registration.active = false;
        this.#listeners.delete(registration);
        this.#publicationListeners = undefined;
        throw error;
      }
    }
    return () => {
      if (!this.#listeners.delete(registration)) return;
      registration.active = false;
      this.#publicationListeners = undefined;
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
    this.#publicationListeners = undefined;
    const detach = this.#detach;
    this.#detach = undefined;
    detach?.();
  }
}
