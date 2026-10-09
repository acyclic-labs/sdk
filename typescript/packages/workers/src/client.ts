import { create, fromBinary, toBinary, type MessageShape } from "@bufbuild/protobuf";
import { observed, resolveObserver, type AcyclicObserver } from "./observe.js";
import { installWorkersMethods, type WorkersClientMethods, type WorkersMethod } from "./generated/workers-service.js";
import type { WorkersClientOptions, WorkersCallOptions as RustCallOptions, WorkersFailure } from "./generated/semantic/workers/readonly.js";
import { loadBinding, type RustClient } from "./binding.js";
import { canonicalSemantic, requireBigIntCodec } from "./generated/readonly.js";
import { workersCancelledFailure } from "./generated/workers-binding.js";

export type WorkersOptions = WorkersClientOptions & { readonly observer?: AcyclicObserver };
export type WorkersCallOptions = RustCallOptions & { readonly signal?: AbortSignal };
export class WorkersTransportError extends Error {
  constructor(message: string, readonly code: string, readonly metadata?: WorkersFailure) { super(message); }
}

/** Rust-owned admission and transport, with descriptor-installed typed methods. */
export class WorkersClient {
  readonly #config: WorkersClientOptions;
  readonly #observer: AcyclicObserver | undefined;
  #client: Promise<RustClient> | undefined;
  #abort: AbortController | undefined;
  #waiters = 0;
  constructor(options: WorkersOptions) {
    const { observer, ...config } = options;
    this.#config = structuredClone(config);
    this.#observer = resolveObserver(observer);
    installWorkersMethods(this, (method, request, options) => this.#call(method, request, options));
  }
  get transport(): Promise<string> { return this.#connection().then(client => client.transport); }
  #call(method: WorkersMethod, request: unknown, options?: WorkersCallOptions): Promise<unknown> {
    const { signal, ...rawOptions } = options ?? {};
    const callOptions = structuredClone(rawOptions);
    return observed(this.#observer, "workers", method.localName, async sizes => {
      if (signal?.aborted) throw cancelled();
      requireBigIntCodec();
      // This synchronous copy/encode precedes observer callbacks, imports and connection awaits.
      const encoded = toBinary(method.input, create(method.input, structuredClone(request) as MessageShape<typeof method.input>));
      if (sizes) sizes.requestBytes = encoded.byteLength;
      const client = await this.#connection(signal);
      const response = await client.call(method.name, encoded, callOptions, signal);
      if (sizes) sizes.responseBytes = response.byteLength;
      return canonicalSemantic(fromBinary(method.output, response));
    });
  }
  #connection(signal?: AbortSignal): Promise<RustClient> {
    if (signal?.aborted) return Promise.reject(cancelled());
    if (this.#client === undefined) {
      const controller = new AbortController();
      let pending!: Promise<RustClient>;
      pending = loadBinding(this.#config.transport).then(binding => binding.connect(this.#config, controller.signal))
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
