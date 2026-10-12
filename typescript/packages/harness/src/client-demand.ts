import { HydrationCache, type Hydrated } from "./cache.js";
import { RequestScheduler, RequestCapacityError, requestAborted, requestBound } from "./client-requests.js";

/** Loads authenticated immutable references within a byte reservation. */
export interface DemandSource<Key, Value> {
  reserve(key: Key): number;
  load(key: Key, signal: AbortSignal, reservedBytes: number): Promise<Hydrated<Value>>;
}

interface Pending<Value> {
  readonly controller: AbortController;
  readonly readers: Set<(result: ReadResult<Value>) => void>;
}

type ReadResult<Value> = Readonly<{ ok: true; value: Value }> | Readonly<{ ok: false; error: unknown }>;

/** Bounds shared demand and retained immutable values without caching failures. */
export class DemandLoader<Key, Value> {
  readonly #cache: HydrationCache<Key, Value>;
  readonly #pending = new Map<Key, Pending<Value>>();
  readonly maximumDemand: number;
  #users = 0;
  #disposed = false;

  constructor(readonly source: DemandSource<Key, Value>, readonly scheduler: RequestScheduler,
    maximumDemand: number, maximumEntries: number, maximumBytes: number) {
    this.maximumDemand = requestBound(maximumDemand, "maximumDemand");
    this.#cache = new HydrationCache(maximumEntries, maximumBytes);
  }

  get residency(): Readonly<{ demand: number; pending: number; entries: number; bytes: number }> {
    return { demand: this.#users, pending: this.#pending.size, entries: this.#cache.size, bytes: this.#cache.bytes };
  }

  peek(key: Key): Hydrated<Value> | undefined { return this.#cache.get(key); }

  read(key: Key, signal?: AbortSignal): Promise<Value> {
    if (this.#disposed) return Promise.reject(new Error("demand loader disposed"));
    if (signal?.aborted) return Promise.reject(requestAborted());
    const cached = this.#cache.get(key);
    if (cached !== undefined) return Promise.resolve(cached.value);
    if (this.#users >= this.maximumDemand) return Promise.reject(new RequestCapacityError("demand capacity exhausted"));
    let pending = this.#pending.get(key);
    if (pending === undefined) {
      let bytes: number;
      try { bytes = this.source.reserve(key); }
      catch (error) { return Promise.reject(error); }
      if (!Number.isSafeInteger(bytes) || bytes < 0) {
        return Promise.reject(new RangeError("hydration byte reservation"));
      }
      const controller = new AbortController();
      const promise = this.scheduler.schedule(bytes, async (abort, reservation) => {
        const loaded = await this.source.load(key, abort, reservation);
        if (!Number.isSafeInteger(loaded.bytes) || loaded.bytes < 0 || loaded.bytes > reservation) {
          throw new RangeError("hydration exceeded reserved bytes");
        }
        if (loaded.bytes > this.#cache.maxBytes) throw new RangeError("hydration exceeded retained byte budget");
        return Object.freeze({ value: loaded.value, bytes: loaded.bytes });
      }, controller.signal);
      pending = { controller, readers: new Set() };
      this.#pending.set(key, pending);
      const selected = pending;
      // Shared completion keeps cancelled readers removable.
      void promise.then(loaded => {
        if (!controller.signal.aborted && this.#pending.get(key) === selected) {
          this.#cache.set(key, loaded.value, loaded.bytes);
        }
        this.#complete(key, selected, { ok: true, value: loaded.value });
      }, error => this.#complete(key, selected, { ok: false, error }));
    }
    const selected = pending;
    this.#users++;
    return new Promise<Value>((resolve, reject) => {
      const finish = (result: ReadResult<Value>): void => {
        if (!selected.readers.delete(finish)) return;
        signal?.removeEventListener("abort", cancel);
        this.#users--;
        if (selected.readers.size === 0 && this.#pending.get(key) === selected) {
          this.#pending.delete(key);
          selected.controller.abort();
        }
        if (result.ok) resolve(result.value);
        else reject(result.error);
      };
      const cancel = (): void => finish({ ok: false, error: requestAborted() });
      selected.readers.add(finish);
      signal?.addEventListener("abort", cancel, { once: true });
    });
  }

  #complete(key: Key, pending: Pending<Value>, result: ReadResult<Value>): void {
    if (this.#pending.get(key) === pending) this.#pending.delete(key);
    for (const finish of pending.readers) finish(result);
  }

  /** Invalidating a pin cancels current demand; late completion cannot repopulate it. */
  invalidate(key: Key): void {
    this.#cache.delete(key);
    const pending = this.#pending.get(key);
    if (pending === undefined) return;
    pending.controller.abort();
    // Cancel readers even when their scheduler promise has settled.
    this.#complete(key, pending, { ok: false, error: requestAborted() });
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const [key, pending] of this.#pending) {
      pending.controller.abort();
      this.#complete(key, pending, { ok: false, error: requestAborted() });
    }
    this.#cache.clear();
  }
}
