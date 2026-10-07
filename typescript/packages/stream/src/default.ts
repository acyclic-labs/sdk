import { HttpStreamProvider } from "./http.js";
import type { StreamEnvironment, StreamProvider } from "./types.js";

type ProviderSelection = {
  readonly promise: Promise<StreamProvider>;
  waiters: number;
  settled: boolean;
  readonly cancel: () => void;
};

/** Selects the staged Rust native transport in Node, with the existing WASM-backed HTTP adapter elsewhere. */
export class DefaultStreamProvider implements StreamProvider {
  readonly #options: StreamEnvironment;
  #selection: ProviderSelection | undefined;

  constructor(options: StreamEnvironment) {
    this.#options = {
      endpoint: options.endpoint,
      token: options.token,
      ...(options.caCertificate === undefined ? {} : { caCertificate: options.caCertificate.slice() }),
    };
  }

  private async provider(): Promise<StreamProvider> {
    const provider = await this.acquireProvider(undefined);
    if (provider === undefined) throw new Error("stream provider selection was cancelled");
    return provider;
  }

  private acquireProvider(signal: AbortSignal | undefined): Promise<StreamProvider | undefined> {
    if (signal?.aborted) return Promise.resolve(undefined);
    let selection = this.#selection;
    if (selection === undefined) {
      let selectionCancelled = false;
      let cancelSelection = () => { selectionCancelled = true; };
      let created!: ProviderSelection;
      const promise = this.select(cancel => {
        cancelSelection = () => { selectionCancelled = true; cancel(); };
        if (selectionCancelled) cancelSelection();
      }).then(
        provider => { created.settled = true; return provider; },
        error => {
          created.settled = true;
          if (this.#selection === created) this.#selection = undefined;
          throw error;
        },
      );
      created = { promise, waiters: 0, settled: false, cancel: () => cancelSelection() };
      this.#selection = created;
      selection = created;
    }
    if (signal?.aborted) return Promise.resolve(undefined);
    selection.waiters += 1;
    return new Promise<StreamProvider | undefined>((resolve, reject) => {
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        if (signal !== undefined) signal.removeEventListener("abort", abort);
        selection!.waiters -= 1;
        if (!selection!.settled && selection!.waiters === 0 && this.#selection === selection) {
          selection!.cancel();
          this.#selection = undefined;
        }
      };
      const abort = () => { release(); resolve(undefined); };
      if (signal !== undefined) {
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) { abort(); return; }
      }
      selection!.promise.then(
        provider => {
          if (signal?.aborted) abort();
          else { release(); resolve(provider); }
        },
        error => { release(); reject(error); },
      );
    });
  }

  private async select(registerCancel: (cancel: () => void) => void): Promise<StreamProvider> {
    const runtime = globalThis as typeof globalThis & { process?: { readonly versions?: { readonly node?: string; readonly bun?: string } } };
    if (runtime.process?.versions?.node === undefined && runtime.process?.versions?.bun === undefined) {
      return new HttpStreamProvider(this.#options);
    }
    // Keep the Node-only facade out of browser dependency graphs while
    // anchoring resolution to this package rather than the consumer CWD.
    const native = await import(new URL("./native.js", import.meta.url).href);
    if (!await native.nativeStreamAvailable()) return new HttpStreamProvider(this.#options);
    const cancellation = await native.createNativeStreamCancellation();
    registerCancel(() => cancellation.cancel());
    return native.NativeStreamProvider.connect(this.#options.endpoint, this.#options.token, this.#options.caCertificate, cancellation);
  }

  inspectIdempotency(key: Parameters<StreamProvider["inspectIdempotency"]>[0]) {
    const authored = key.slice() as typeof key;
    return this.provider().then(provider => provider.inspectIdempotency(authored));
  }
  tail(path: string) { return this.provider().then(provider => provider.tail(path)); }
  append(path: string, values: Parameters<StreamProvider["append"]>[1], options?: Parameters<StreamProvider["append"]>[2]) {
    const copiedValues = values.map(value => value.slice());
    const authored = options === undefined ? undefined : structuredClone(options);
    return this.provider().then(provider => provider.append(path, copiedValues, authored));
  }
  fork(source: string, destination: string, options?: Parameters<StreamProvider["fork"]>[2]) {
    const authored = options === undefined ? undefined : structuredClone(options);
    return this.provider().then(provider => provider.fork(source, destination, authored));
  }
  read(path: string, options: Parameters<StreamProvider["read"]>[1]) {
    const authored = { from: options.from, limit: options.limit };
    return this.readAfterSelection(path, authored);
  }
  private async *readAfterSelection(path: string, options: Parameters<StreamProvider["read"]>[1]) {
    yield* (await this.provider()).read(path, options);
  }
  follow(path: string, options: Parameters<StreamProvider["follow"]>[1]) {
    const authored = {
      from: options.from,
      ...(options.signal === undefined ? {} : { signal: options.signal }),
    };
    return this.followAfterSelection(path, authored);
  }
  private async *followAfterSelection(path: string, options: Parameters<StreamProvider["follow"]>[1]) {
    const provider = await this.acquireProvider(options.signal);
    if (provider === undefined || options.signal?.aborted) return;
    yield* provider.follow(path, options);
  }
  childrenPage(request: Parameters<StreamProvider["childrenPage"]>[0]) {
    const authored = structuredClone(request);
    return this.provider().then(provider => provider.childrenPage(authored));
  }
  commit(request: Parameters<StreamProvider["commit"]>[0], options: Parameters<StreamProvider["commit"]>[1]) {
    const authoredRequest = structuredClone(request);
    const authoredOptions = structuredClone(options);
    return this.provider().then(provider => provider.commit(authoredRequest, authoredOptions));
  }
  readCommit(id: Parameters<StreamProvider["readCommit"]>[0]) {
    const authored = id.slice() as typeof id;
    return this.provider().then(provider => provider.readCommit(authored));
  }
  createToken(request: Parameters<NonNullable<StreamProvider["createToken"]>>[0]) {
    const authored = structuredClone(request);
    return this.provider().then(provider => {
      return provider.createToken?.(authored) ?? new HttpStreamProvider(this.#options).createToken!(authored);
    });
  }
}
