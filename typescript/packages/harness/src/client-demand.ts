import { HydrationCache, type Hydrated } from "./cache.js";
import { RequestScheduler, RequestCapacityError, requestAborted, requestBound } from "./client-requests.js";

/** Keys denote immutable pinned references, including tenant/provider identity.
 * The trusted loader authenticates correspondence and enforces the reservation
 * before IO. Its byte receipt covers retained deep values/backing storage.
 */
export interface DemandSource<Key, Value> {
  reserve(key: Key): number;
  load(key: Key, signal: AbortSignal, reservedBytes: number): Promise<Hydrated<Value>>;
}

interface Pending<Value> {
  readonly controller: AbortController;
  readonly promise: Promise<Hydrated<Value>>;
  users: number;
}

/** Selective hydration, bounded shared demand and existing LRU retention.
 * Construction performs no work. Cached values must be transitively immutable.
 * Absence/errors are not cached; applications retain domain provenance/status
 * in ClientViews rather than interpreting a cache miss as authoritative absence.
 */
export class DemandLoader<Key, Value> {
  readonly #cache: HydrationCache<Key, Hydrated<Value>>;
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
      pending = { controller, promise, users: 0 };
      this.#pending.set(key, pending);
      const selected = pending;
      // Always consume failure, including when every caller has cancelled.
      void promise.then(loaded => {
        if (!controller.signal.aborted && this.#pending.get(key) === selected) {
          this.#cache.set(key, loaded, loaded.bytes);
        }
      }, () => {}).finally(() => {
        if (this.#pending.get(key) === selected) this.#pending.delete(key);
      });
    }
    const selected = pending;
    selected.users++;
    this.#users++;
    return new Promise<Value>((resolve, reject) => {
      let finished = false;
      const finish = (): boolean => {
        if (finished) return false;
        finished = true;
        signal?.removeEventListener("abort", cancel);
        selected.users--;
        this.#users--;
        if (selected.users === 0 && this.#pending.get(key) === selected) {
          this.#pending.delete(key);
          selected.controller.abort();
        }
        return true;
      };
      const cancel = (): void => { if (finish()) reject(requestAborted()); };
      signal?.addEventListener("abort", cancel, { once: true });
      void selected.promise.then(value => { if (finish()) resolve(value.value); }, error => { if (finish()) reject(error); });
    });
  }

  /** Invalidating a pin cancels current demand; late completion cannot repopulate it. */
  invalidate(key: Key): void {
    this.#cache.delete(key);
    this.#pending.get(key)?.controller.abort();
    this.#pending.delete(key);
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const pending of this.#pending.values()) pending.controller.abort();
    this.#pending.clear();
    this.#cache.clear();
    // The shared scheduler belongs to its caller.
  }
}
