/** Finite provider work and residency limits. */
export interface RequestLimits {
  readonly concurrent: number;
  readonly requests: number;
  readonly bytes: number;
}
export class RequestCapacityError extends Error {}

interface Job {
  readonly bytes: number;
  readonly start: () => Promise<void>;
  readonly cancel: () => void;
  running: boolean;
}
export function requestBound(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(name);
  return value;
}
export function requestAborted(): DOMException {
  return new DOMException("Request cancelled", "AbortError");
}

/** Cancellation retains running reservations until provider settlement. */
export class RequestScheduler {
  readonly limits: RequestLimits;
  readonly #jobs = new Set<Job>();
  readonly #queue: Job[] = [];
  #running = 0;
  #bytes = 0;
  #disposed = false;

  constructor(limits: RequestLimits) {
    for (const name of ["concurrent", "requests", "bytes"] as const) requestBound(limits[name], name);
    if (limits.concurrent > limits.requests) throw new RangeError("concurrent exceeds requests");
    this.limits = Object.freeze({ ...limits });
  }
  get residency(): Readonly<{ requests: number; running: number; bytes: number }> {
    return { requests: this.#jobs.size, running: this.#running, bytes: this.#bytes };
  }
  /** Reserve before capturing inputs and invoking provider code. */
  schedule<Value>(bytes: number, work: (signal: AbortSignal) => Promise<Value>,
    signal?: AbortSignal, capture?: () => void): Promise<Value> {
    if (this.#disposed) return Promise.reject(new Error("request scheduler disposed"));
    if (signal?.aborted) return Promise.reject(requestAborted());
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.limits.bytes) return Promise.reject(new RangeError("request byte reservation"));
    if (this.#jobs.size >= this.limits.requests || bytes > this.limits.bytes - this.#bytes) return Promise.reject(new RequestCapacityError("request capacity exhausted"));
    return new Promise<Value>((resolve, reject) => {
      const controller = new AbortController();
      const detach = () => signal?.removeEventListener("abort", job.cancel);
      const job: Job = {
        bytes, running: false,
        cancel: () => {
          reject(requestAborted()); controller.abort(); detach();
          if (!job.running) {
            const index = this.#queue.indexOf(job);
            if (index >= 0) this.#queue.splice(index, 1);
            this.#release(job);
          }
        },
        start: async () => {
          try {
            if (controller.signal.aborted) throw requestAborted();
            resolve(await work(controller.signal));
          } catch (error) { reject(error); }
          finally { detach(); this.#release(job); }
        },
      };
      this.#jobs.add(job); this.#bytes += bytes;
      signal?.addEventListener("abort", job.cancel, { once: true });
      try { capture?.(); }
      catch (error) { reject(error); detach(); this.#release(job); return; }
      if (!this.#jobs.has(job)) return;
      this.#queue.push(job); this.#drain();
    });
  }
  #release(job: Job): void {
    if (!this.#jobs.delete(job)) return;
    this.#bytes -= job.bytes;
    if (job.running) this.#running--;
    this.#drain();
  }
  #drain(): void {
    while (!this.#disposed && this.#running < this.limits.concurrent && this.#queue.length) {
      const job = this.#queue.shift()!;
      job.running = true; this.#running++;
      void Promise.resolve().then(job.start);
    }
  }
  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#queue.length = 0;
    for (const job of this.#jobs) job.cancel();
  }
}

export function abortableWait(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal.aborted) { reject(signal.reason); return; }
    const aborted = () => { clearTimeout(timeout); reject(signal.reason); };
    const timeout = setTimeout(() => { signal.removeEventListener("abort", aborted); resolve(); }, milliseconds);
    signal.addEventListener("abort", aborted, { once: true });
  });
}
