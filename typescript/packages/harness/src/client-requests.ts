/** Finite admission shared by reads and transport sends. No retries or timers. */
export interface RequestLimits {
  readonly concurrent: number;
  /** Includes running requests, even after a caller cancels. */
  readonly requests: number;
  /** Reserved provider work/storage, including queued requests. */
  readonly bytes: number;
}

export class RequestCapacityError extends Error {}

interface Job {
  readonly bytes: number;
  readonly controller: AbortController;
  readonly start: () => Promise<void>;
  readonly reject: (reason: unknown) => void;
  readonly detach: () => void;
  running: boolean;
}

export function requestBound(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(name);
  return value;
}

export function requestAborted(): DOMException {
  return new DOMException("Request cancelled", "AbortError");
}

/** Cancellation releases caller interest immediately. A running provider keeps
 * its reservation until it settles, so ignoring abort cannot bypass admission.
 * Providers must enforce the supplied reservation before allocating/reading.
 */
export class RequestScheduler {
  readonly limits: RequestLimits;
  readonly #jobs = new Set<Job>();
  readonly #queue: Job[] = [];
  #running = 0;
  #bytes = 0;
  #disposed = false;

  constructor(limits: RequestLimits) {
    requestBound(limits.concurrent, "concurrent");
    requestBound(limits.requests, "requests");
    requestBound(limits.bytes, "bytes");
    if (limits.concurrent > limits.requests) throw new RangeError("concurrent exceeds requests");
    this.limits = Object.freeze({ ...limits });
  }

  get residency(): Readonly<{ requests: number; running: number; bytes: number }> {
    return { requests: this.#jobs.size, running: this.#running, bytes: this.#bytes };
  }

  schedule<Value>(bytes: number, work: (signal: AbortSignal, bytes: number) => Promise<Value>,
    signal?: AbortSignal): Promise<Value> {
    if (this.#disposed) return Promise.reject(new Error("request scheduler disposed"));
    if (signal?.aborted) return Promise.reject(requestAborted());
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.limits.bytes) {
      return Promise.reject(new RangeError("request byte reservation"));
    }
    if (this.#jobs.size >= this.limits.requests || bytes > this.limits.bytes - this.#bytes) {
      return Promise.reject(new RequestCapacityError("request capacity exhausted"));
    }
    return new Promise<Value>((resolve, reject) => {
      const controller = new AbortController();
      const cancel = (): void => {
        reject(requestAborted());
        controller.abort();
        job.detach();
        if (!job.running) {
          const index = this.#queue.indexOf(job);
          if (index >= 0) this.#queue.splice(index, 1);
          this.#release(job);
        }
      };
      const job: Job = {
        bytes, controller, reject, running: false,
        detach: () => signal?.removeEventListener("abort", cancel),
        start: async () => {
          try {
            if (controller.signal.aborted) throw requestAborted();
            resolve(await work(controller.signal, bytes));
          }
          catch (error) { reject(error); }
          finally { job.detach(); this.#release(job); }
        },
      };
      this.#jobs.add(job);
      this.#bytes += bytes;
      this.#queue.push(job);
      signal?.addEventListener("abort", cancel, { once: true });
      this.#drain();
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
      job.running = true;
      this.#running++;
      // Admission completes before invoking provider code, including sync throws.
      void Promise.resolve().then(job.start);
    }
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const job of this.#jobs) {
      job.reject(requestAborted());
      job.controller.abort();
      job.detach();
      if (!job.running) this.#release(job);
    }
    this.#queue.length = 0;
  }
}
