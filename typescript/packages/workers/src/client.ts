import { create, fromBinary, toBinary, type MessageShape } from "@bufbuild/protobuf";
import { observed, resolveObserver, type AcyclicObserver } from "./observe.js";
import { installWorkersMethods, type WorkersClientMethods, type WorkersMethod } from "./generated/workers-service.js";
import type { WorkersClientOptions, WorkersCallOptions as RustCallOptions, WorkersFailure } from "./generated/semantic/workers/readonly.js";
import { loadBinding, type RustClient } from "./binding.js";
import { canonicalSemantic, requireBigIntCodec } from "./generated/readonly.js";
import { workersCancelledFailure } from "./generated/workers-binding.js";

/** Opaque customer bearer and its actual signed expiry; never a SQL session. */
export interface WorkersBearerCredential {
  readonly bearer: string;
  readonly expiresAtUnixMillis: bigint;
}
export interface WorkersCredentialRequest { readonly signal: AbortSignal }
/** Refreshes only the caller's existing leaf, with cancellation propagated. */
export type WorkersCredentialProvider = (request: WorkersCredentialRequest) => Promise<WorkersBearerCredential>;
export type WorkersOptions = Omit<WorkersClientOptions, "token"> & {
  readonly observer?: AcyclicObserver;
} & ({
  readonly token: string;
  readonly credentialProvider?: never;
} | {
  readonly token?: never;
  readonly credentialProvider: WorkersCredentialProvider;
});
export type WorkersCallOptions = Omit<RustCallOptions, "bearerToken"> & { readonly signal?: AbortSignal };
export class WorkersTransportError extends Error {
  constructor(message: string, readonly code: string, readonly metadata?: WorkersFailure) { super(message); }
}

/** Rust-owned admission and transport, with descriptor-installed typed methods. */
export class WorkersClient {
  readonly #config: Omit<WorkersClientOptions, "token">;
  readonly #token: string | undefined;
  readonly #provider: WorkersCredentialProvider | undefined;
  #credential: WorkersBearerCredential | undefined;
  #refresh: Promise<WorkersBearerCredential> | undefined;
  #refreshAbort: AbortController | undefined;
  #refreshWaiters = 0;
  readonly #observer: AcyclicObserver | undefined;
  #client: Promise<RustClient> | undefined;
  #abort: AbortController | undefined;
  #waiters = 0;
  constructor(options: WorkersOptions) {
    const { observer, token, credentialProvider, ...config } = options;
    if ((token === undefined) === (credentialProvider === undefined)) {
      throw new WorkersTransportError("provide one static bearer or credential provider", "configuration");
    }
    this.#config = structuredClone(config);
    this.#token = token;
    this.#provider = credentialProvider;
    this.#observer = resolveObserver(observer);
    installWorkersMethods(this, (method, request, options) => this.#call(method, request, options));
  }
  get transport(): Promise<string> { return this.#bearer().then(token => this.#connection(token)).then(client => client.transport); }
  #call(method: WorkersMethod, request: unknown, options?: WorkersCallOptions): Promise<unknown> {
    const { signal, ...rawOptions } = options ?? {};
    const callOptions = structuredClone(rawOptions);
    return observed(this.#observer, "workers", method.localName, async sizes => {
      if (signal?.aborted) throw cancelled();
      requireBigIntCodec();
      // This synchronous copy/encode precedes observer callbacks, imports and connection awaits.
      const encoded = toBinary(method.input, create(method.input, structuredClone(request) as MessageShape<typeof method.input>));
      if (sizes) sizes.requestBytes = encoded.byteLength;
      const bearerToken = await this.#bearer(signal);
      const client = await this.#connection(bearerToken, signal);
      const response = await client.call(method.name, encoded, { ...callOptions, bearerToken }, signal);
      if (sizes) sizes.responseBytes = response.byteLength;
      return canonicalSemantic(fromBinary(method.output, response));
    });
  }
  async #bearer(signal?: AbortSignal): Promise<string> {
    if (signal?.aborted) throw cancelled();
    if (this.#token !== undefined) return this.#token;
    if (this.#credential !== undefined && this.#credential.expiresAtUnixMillis > BigInt(Date.now()) + 1_000n) {
      return this.#credential.bearer;
    }
    if (this.#refresh === undefined) {
      const controller = new AbortController();
      let pending!: Promise<WorkersBearerCredential>;
      pending = Promise.resolve().then(() => this.#provider!({ signal: controller.signal })).then(value => {
        if (typeof value.expiresAtUnixMillis !== "bigint" || value.expiresAtUnixMillis > 0xffff_ffff_ffff_ffffn
          || value.expiresAtUnixMillis <= BigInt(Date.now())) {
          throw new WorkersTransportError("credential provider returned an invalid or expired expiry", "configuration");
        }
        if (controller.signal.aborted) throw cancelled();
        const credential = { bearer: value.bearer, expiresAtUnixMillis: value.expiresAtUnixMillis };
        if (this.#refresh === pending) this.#credential = credential;
        return credential;
      }).finally(() => {
        if (this.#refresh === pending) { this.#refresh = undefined; this.#refreshAbort = undefined; }
      });
      this.#refresh = pending;
      this.#refreshAbort = controller;
    }
    const pending = this.#refresh;
    this.#refreshWaiters += 1;
    try { return (await abortable(pending, signal)).bearer; }
    finally {
      this.#refreshWaiters -= 1;
      if (this.#refreshWaiters === 0 && this.#refresh === pending && this.#refreshAbort !== undefined) {
        const controller = this.#refreshAbort;
        this.#refresh = undefined;
        this.#refreshAbort = undefined;
        controller.abort();
      }
    }
  }
  #connection(token: string, signal?: AbortSignal): Promise<RustClient> {
    if (signal?.aborted) return Promise.reject(cancelled());
    if (this.#client === undefined) {
      const controller = new AbortController();
      let pending!: Promise<RustClient>;
      pending = loadBinding(this.#config.transport).then(binding => binding.connect({ ...this.#config, token }, controller.signal))
        .then(client => { if (this.#client === pending) this.#abort = undefined; return client; })
        .catch(error => { if (this.#client === pending) { this.#client = undefined; this.#abort = undefined; } throw error; });
      this.#abort = controller;
      this.#client = pending;
    }
    const pending = this.#client;
    this.#waiters += 1;
    return abortable(pending, signal).finally(() => {
      this.#waiters -= 1;
      if (this.#waiters === 0 && this.#client === pending && this.#abort !== undefined) {
        const controller = this.#abort;
        this.#client = undefined;
        this.#abort = undefined;
        controller.abort();
      }
    });
  }
}
export interface WorkersClient extends WorkersClientMethods {}
export { WorkersClient as HttpWorkersClient };
export type HttpWorkersOptions = WorkersOptions;
export const createWorkersGrpcClient = (options: WorkersOptions): WorkersClient => new WorkersClient(options);
function abortable<T>(pending: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return pending;
  return new Promise((resolve, reject) => {
    const abort = () => reject(cancelled());
    if (signal.aborted) { abort(); return; }
    signal.addEventListener("abort", abort, { once: true });
    void pending.then(value => { signal.removeEventListener("abort", abort); resolve(value); }, error => { signal.removeEventListener("abort", abort); reject(error); });
  });
}

function cancelled(): WorkersTransportError {
  const metadata = workersCancelledFailure();
  return new WorkersTransportError(metadata.message, metadata.code, metadata);
}
