/** Source-owned freshness, partitioned by account, audience and auth context.
 * Observe performs no IO. A source watcher publishes a new revision before
 * changed credentials may be reused; unavailable sources return no revision. */
export interface CredentialValidity {
  readonly nowUnixSeconds: number;
  readonly revision?: string;
}

/** Non-secret source partition and absolute lifetime; never a permission. */
export interface CredentialBinding {
  readonly revision: string;
  readonly expiresAtUnixSeconds: number;
}

/** Register before reading source-bound data. Abort cancels outstanding work
 * and discards that data. Subscribe again after a change; close unregisters.
 * Absolute expiry still requires the request's check/deadline, without a timer. */
export interface CredentialWatch {
  readonly validity: CredentialValidity;
  readonly changed: AbortSignal;
  close(): void;
}

/** Custodian-supplied absolute validity. Never derive validity from token claims. */
export class CredentialSnapshot {
  readonly #token: string;
  readonly revision: string;
  readonly expiresAtUnixSeconds: number;

  constructor(token: string, revision: string, expiresAtUnixSeconds: number) {
    validateCredential(token);
    if (!revision || !Number.isSafeInteger(expiresAtUnixSeconds) || expiresAtUnixSeconds < 0) {
      throw new TypeError("invalid credential validity");
    }
    this.#token = token;
    this.revision = revision;
    this.expiresAtUnixSeconds = expiresAtUnixSeconds;
    Object.freeze(this);
  }

  /** Borrow only for the admitted request. Do not log or persist bearer bytes. */
  token(): string { return this.#token; }
}

/** Validate at the transport boundary without exposing credential material. */
export function validateCredential(token: unknown): asserts token is string {
  if (typeof token !== "string" || token.length > 12 * 1024 ||
      new TextEncoder().encode(token).byteLength > 12 * 1024 || !token.trim() || /[\r\n\0]/.test(token)) {
    throw new TypeError("credential must be non-empty, at most 12 KiB, and free of CR, LF, or NUL");
  }
}

export type CredentialProvider = ((signal?: AbortSignal) => string | Promise<string>) & {
  /** Retain source custody through dispatch and exact server rejection. */
  readonly resolveRequest?: (signal?: AbortSignal) => Promise<CredentialRequest>;
  /** Present only for a source with trusted validity and push invalidation. */
  readonly watch?: () => CredentialWatch;
};

export interface CredentialOptions {
  readonly token?: string;
  readonly credential?: CredentialProvider;
}

/** One transport attempt. A late rejection cannot invalidate a newer lease. */
export interface CredentialRequest {
  /** Absent for raw callbacks. Retain exact custody and check before reuse. */
  readonly binding?: CredentialBinding;
  token(): string;
  check(): void;
  reject(): boolean;
}

/** Retain exact rejection custody through streaming trailers as well as headers.
 * Policy denials, cancellation and transport failures leave the lease alone. */
export async function* credentialStream<T>(
  stream: AsyncIterable<T>, lease: CredentialRequest, rejected: (error: unknown) => boolean,
): AsyncIterable<T> {
  try {
    lease.check();
    for await (const value of stream) {
      lease.check();
      yield value;
      lease.check();
    }
    lease.check();
  }
  catch (error) { if (rejected(error)) lease.reject(); throw error; }
}

interface Flight {
  readonly controller: AbortController;
  readonly promise: Promise<CredentialSnapshot>;
  waiters: number;
  published: boolean;
}

/** One shared source partition; no timer, detached refresh, retry or fallback.
 * Every caller owns an interest. Cancelling the last interest aborts the source
 * and fences even providers that ignore cancellation from publishing later. */
export class CredentialCache {
  #current: CredentialSnapshot | undefined;
  #rejected: CredentialSnapshot | undefined;
  #flight: Flight | undefined;
  readonly #refresh: (rejected: CredentialSnapshot | undefined, signal: AbortSignal) => Promise<CredentialSnapshot>;
  readonly #observe: () => CredentialValidity;
  readonly #changes = new Set<AbortController>();
  readonly provider: CredentialProvider;

  constructor(
    refresh: (rejected: CredentialSnapshot | undefined, signal: AbortSignal) => Promise<CredentialSnapshot>,
    observe: () => CredentialValidity,
  ) {
    this.#refresh = refresh;
    this.#observe = observe;
    this.provider = Object.assign(
      (signal?: AbortSignal) => this.resolve(signal).then(lease => lease.token()),
      { resolveRequest: async (signal?: AbortSignal): Promise<CredentialRequest> => {
        const lease = await this.resolve(signal);
        return {
          binding: Object.freeze({ revision: lease.revision, expiresAtUnixSeconds: lease.expiresAtUnixSeconds }),
          token: () => lease.token(), check: () => this.check(lease), reject: () => this.reject(lease),
        };
      }, watch: () => this.watch() },
    );
  }

  async resolve(signal?: AbortSignal): Promise<CredentialSnapshot> {
    signal?.throwIfAborted();
    const validity = this.#observe();
    requireRevision(validity);
    const current = this.#current;
    if (current && valid(current, validity) && this.#current === current) {
      signal?.throwIfAborted();
      return current;
    }
    signal?.throwIfAborted();
    if (this.#flight?.published) this.#flight = undefined;
    if (!this.#flight) {
      const controller = new AbortController();
      const rejected = this.#rejected;
      const flight: Flight = {
        controller, waiters: 0, published: false,
        promise: abortable(Promise.resolve().then(() => {
          controller.signal.throwIfAborted();
          return this.#refresh(rejected, controller.signal);
        }).then(snapshot => {
          const acceptable = valid(snapshot, this.#observe());
          controller.signal.throwIfAborted();
          if (this.#flight !== flight || !acceptable) throw new Error("credential source changed or expired");
          if (this.#rejected?.token() === snapshot.token()) throw new Error("credential was rejected by server");
          this.#current = snapshot;
          this.#rejected = undefined;
          flight.published = true;
          return snapshot;
        }), controller.signal),
      };
      this.#flight = flight;
    }
    const flight = this.#flight;
    flight.waiters += 1;
    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      flight.waiters -= 1;
      if (flight.waiters === 0 && this.#flight === flight) {
        this.#flight = undefined;
        if (!flight.published) flight.controller.abort();
      }
    };
    try {
      // Cancellation withdraws interest in the event callback itself, before
      // a reentrant source observation can publish an abandoned refresh.
      const snapshot = await abortable(flight.promise, signal, release);
      this.check(snapshot);
      return snapshot;
    } finally {
      release();
    }
  }

  /** Recheck immediately before dispatch, after channel acquisition or queuing. */
  check(snapshot: CredentialSnapshot): void {
    const acceptable = valid(snapshot, this.#observe());
    if (this.#current !== snapshot || !acceptable) throw new Error("credential source changed or expired");
  }

  /** Only authenticated rejection of this exact request's lease invalidates it. */
  reject(snapshot: CredentialSnapshot): boolean {
    if (this.#current !== snapshot) return false;
    this.#current = undefined;
    this.#rejected = snapshot;
    if (this.#flight?.published) this.#flight = undefined;
    this.#notify();
    return true;
  }

  /** Custodians publish changed/unavailable validity before calling invalidate. */
  watch(): CredentialWatch {
    const controller = new AbortController();
    this.#changes.add(controller);
    try {
      // Subscribe before observation: the source may publish a change while
      // observing, including synchronously reentering invalidate().
      const validity = Object.freeze({ ...this.#observe() });
      return { validity, changed: controller.signal, close: () => { this.#changes.delete(controller); } };
    } catch (error) {
      this.#changes.delete(controller);
      throw error;
    }
  }

  #notify(): void {
    const changes = [...this.#changes];
    this.#changes.clear();
    for (const controller of changes) controller.abort();
  }

  /** Source watcher invalidation. Publish changed/unavailable revision first. */
  invalidate(): void {
    const flight = this.#flight;
    this.#flight = undefined;
    this.#current = undefined;
    this.#rejected = undefined;
    flight?.controller.abort(new Error("credential source changed or unavailable"));
    this.#notify();
  }
}

function requireRevision(validity: CredentialValidity): void {
  if (!validity.revision || !Number.isSafeInteger(validity.nowUnixSeconds) || validity.nowUnixSeconds < 0) {
    throw new Error("credential source unavailable");
  }
}

function valid(snapshot: CredentialSnapshot, validity: CredentialValidity): boolean {
  requireRevision(validity);
  return snapshot.revision === validity.revision && validity.nowUnixSeconds < snapshot.expiresAtUnixSeconds;
}

function abortable<T>(promise: Promise<T>, signal?: AbortSignal, onAbort?: () => void): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    if (signal?.aborted) { promise.catch(() => {}); onAbort?.(); reject(signal.reason); return; }
    const cleanup = () => signal?.removeEventListener("abort", abort);
    const abort = () => { cleanup(); onAbort?.(); reject(signal?.reason); };
    signal?.addEventListener("abort", abort, { once: true });
    promise.then(value => { cleanup(); if (signal?.aborted) reject(signal.reason); else resolve(value); },
      error => { cleanup(); reject(error); });
  });
}

async function rawCredential(provider: CredentialProvider, signal?: AbortSignal): Promise<string> {
  signal?.throwIfAborted();
  let result: string | Promise<string>;
  try { result = provider(signal); }
  catch (error) { signal?.throwIfAborted(); throw error; }
  // Attach rejection custody even when the callback synchronously aborts.
  // Deferring invocation can leave its already-created promise unobserved.
  return abortable(Promise.resolve(result), signal);
}

/** Raw callbacks resolve afresh; snapshot-aware providers retain exact custody. */
export function credentialProvider(options: CredentialOptions): CredentialProvider {
  const provider = options.credential;
  if (provider !== undefined) {
    if (typeof provider !== "function") throw new TypeError("credential must be a function");
    return Object.assign(async (signal?: AbortSignal) => {
      const token = await rawCredential(provider, signal);
      validateCredential(token);
      return token;
    }, {
      resolveRequest: (signal?: AbortSignal) => resolveCredential(provider, signal),
      ...(provider.watch ? { watch: () => provider.watch!() } : {}),
    });
  }
  const token = options.token;
  validateCredential(token);
  return signal => { signal?.throwIfAborted(); return token; };
}

/** A single resolution, with the same cancellation/deadline as its transport. */
export async function resolveCredential(provider: CredentialProvider, signal?: AbortSignal): Promise<CredentialRequest> {
  signal?.throwIfAborted();
  if (provider.resolveRequest) {
    const request = await abortable(provider.resolveRequest(signal), signal);
    signal?.throwIfAborted();
    validateCredential(request.token());
    request.check();
    return {
      ...(request.binding === undefined ? {} : { binding: request.binding }),
      token: () => request.token(),
      check: () => { signal?.throwIfAborted(); request.check(); },
      reject: () => request.reject(),
    };
  }
  const token = await rawCredential(provider, signal);
  validateCredential(token);
  return { token: () => token, check: () => signal?.throwIfAborted(), reject: () => false };
}
