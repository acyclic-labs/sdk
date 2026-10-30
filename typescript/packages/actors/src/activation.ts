interface ActivationObserver {
  previous: ActivationObserver | undefined;
  next: ActivationObserver | undefined;
  reject(reason: unknown): void;
}

/** Constructor and exclusive sections gate every new delivery. Failed heaps stay fenced.
 * Ready admission uses retained state with no I/O or deployment lookup. Its work
 * is constant in other activations and completed deliveries, excluding guest work.
 */
export class ActorActivation<State> {
  #initialize: (() => State | Promise<State>) | undefined;
  #value: State | undefined;
  #initialized = false;
  #failure: (Error & { code: "unavailable" }) | undefined;
  #observers: ActivationObserver | undefined;
  #gate: Promise<void> | undefined;
  #admission: Promise<void> | undefined;
  #batch: { active: number; drained?: () => void } = { active: 0 };
  constructor(initialize: () => State | Promise<State>) { this.#initialize = initialize; }
  // Remove settled observers: a terminal promise's reactions would retain history
  // for the entire activation. Only currently pending operations observe stop.
  #observe<Result>(operation: (fail: (error: unknown) => void) => Promise<Result>): Promise<Result> {
    return new Promise<Result>((resolve, reject) => {
      const observer: ActivationObserver = { previous: undefined, next: undefined, reject };
      if (this.#failure) { reject(this.#failure); return; }
      observer.next = this.#observers;
      if (observer.next) observer.next.previous = observer;
      this.#observers = observer;
      const fail = (error: unknown) => {
        this.#unlink(observer);
        reject(error);
        this.fence(error);
      };
      operation(fail).then(value => {
        this.#unlink(observer);
        if (this.#failure) reject(this.#failure); else resolve(value);
      }, fail);
    });
  }
  #unlink(observer: ActivationObserver): void {
    if (observer.previous) observer.previous.next = observer.next;
    else if (this.#observers === observer) this.#observers = observer.next;
    else return;
    if (observer.next) observer.next.previous = observer.previous;
    observer.previous = observer.next = undefined;
  }
  #available(): void { if (this.#failure) throw this.#failure; }
  blockConcurrencyWhile<Result>(callback: (state: State) => Result | Promise<Result>): Promise<Result> {
    if (this.#failure) return Promise.reject(this.#failure);
    return this.#observe(fail => this.#exclusive(callback, fail));
  }
  #exclusive<Result>(callback: (state: State) => Result | Promise<Result>, fail: (error: unknown) => void): Promise<Result> {
    const operation = (this.#gate ?? Promise.resolve()).then(async () => {
      this.#available();
      if (!this.#initialized) {
        const initialize = this.#initialize!;
        this.#initialize = undefined;
        this.#value = await initialize();
        this.#initialized = true;
      }
      this.#available();
      const result = await callback(this.#value as State);
      this.#available();
      return result;
    });
    // Settle this section's original public owner before fencing its peers or
    // opening the queue. An internal section is not another public observer.
    const gate = operation.then(() => {
      if (this.#gate === gate) this.#gate = undefined;
    }, fail);
    this.#gate = gate;
    return operation;
  }
  deliver<Result>(handler: (state: State) => Result | Promise<Result>): Promise<Result> {
    if (this.#failure) return Promise.reject(this.#failure);
    const batch = this.#batch, admission = this.#admission;
    // Registration precedes every await, including initialization and admission.
    batch.active++;
    return this.#observe(fail => (async () => {
      try {
        if (admission) await admission;
        // A ready heap with no exclusive owner needs no queue or handoff.
        // Recheck after capture admission, whose producer may have fenced it.
        const state = this.#initialized && this.#gate === undefined
          ? this.#value as State
          : await this.#exclusive(state => state, fail);
        // Ownership may be revoked after admission resolves but before this continuation.
        this.#available();
        const result = await handler(state);
        this.#available();
        return result;
      } finally { if (--batch.active === 0) batch.drained?.(); }
    })());
  }
  /** Host capture waits for admitted handlers and gates new deliveries until complete.
   * Call outside a handler: a handler cannot wait for its own completion.
   */
  quiesce<Result>(capture: (state: State) => Result | Promise<Result>): Promise<Result> {
    if (this.#failure) return Promise.reject(this.#failure);
    const batch = this.#batch;
    const drained = batch.active === 0 ? Promise.resolve() : new Promise<void>(resolve => { batch.drained = resolve; });
    // Close admission first. Already admitted handlers retain exclusive access
    // while draining; capture acquires it only after their completion event.
    return this.#observe(fail => {
      const operation = (this.#admission?.then(() => drained) ?? drained).then(() => this.#exclusive(capture, fail));
      const released = () => { if (this.#admission === admission) this.#admission = undefined; };
      const admission = operation.then(released, released);
      this.#admission = admission;
      this.#batch = { active: 0 };
      return operation;
    });
  }
  /** Host cancellation/ownership loss permanently invalidates this heap. */
  fence(cause?: unknown): void {
    if (this.#failure) return;
    this.#failure = Object.assign(
      new Error("activation fenced; host must discard heap", { cause }),
      { code: "unavailable" as const },
    );
    // Only terminal notification visits all currently armed observers.
    while (this.#observers) {
      const observer = this.#observers;
      this.#unlink(observer);
      observer.reject(this.#failure);
    }
  }
}
