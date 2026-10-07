import { HttpStreamProvider } from "./http.js";
import type { StreamEnvironment, StreamProvider } from "./types.js";

/** Selects the staged Rust native transport in Node, with the existing WASM-backed HTTP adapter elsewhere. */
export class DefaultStreamProvider implements StreamProvider {
  readonly #options: StreamEnvironment;
  #delegate: Promise<StreamProvider> | undefined;

  constructor(options: StreamEnvironment) { this.#options = options; }

  private async provider(): Promise<StreamProvider> {
    this.#delegate ??= this.select();
    return this.#delegate;
  }

  private async select(): Promise<StreamProvider> {
    const runtime = globalThis as typeof globalThis & { process?: { readonly versions?: { readonly node?: string; readonly bun?: string } } };
    if (runtime.process?.versions?.node === undefined && runtime.process?.versions?.bun === undefined) {
      return new HttpStreamProvider(this.#options);
    }
    // Keep the Node-only facade out of browser dependency graphs. The import
    // specifier is fixed; the indirection only prevents browser bundlers from
    // resolving node:buffer/node:module while compiling the default entry.
    const loadNative = Function("specifier", "return import(specifier)") as (specifier: string) => Promise<typeof import("./native.js")>;
    const native = await loadNative("./native.js");
    if (!await native.nativeStreamAvailable()) return new HttpStreamProvider(this.#options);
    return native.NativeStreamProvider.connect(this.#options.endpoint, this.#options.token, this.#options.caCertificate);
  }

  inspectIdempotency(key: Parameters<StreamProvider["inspectIdempotency"]>[0]) { return this.provider().then(provider => provider.inspectIdempotency(key)); }
  tail(path: string) { return this.provider().then(provider => provider.tail(path)); }
  append(path: string, values: Parameters<StreamProvider["append"]>[1], options?: Parameters<StreamProvider["append"]>[2]) { return this.provider().then(provider => provider.append(path, values, options)); }
  fork(source: string, destination: string, options?: Parameters<StreamProvider["fork"]>[2]) { return this.provider().then(provider => provider.fork(source, destination, options)); }
  async *read(path: string, options: Parameters<StreamProvider["read"]>[1]) { yield* (await this.provider()).read(path, options); }
  async *follow(path: string, options: Parameters<StreamProvider["follow"]>[1]) { yield* (await this.provider()).follow(path, options); }
  childrenPage(request: Parameters<StreamProvider["childrenPage"]>[0]) { return this.provider().then(provider => provider.childrenPage(request)); }
  commit(request: Parameters<StreamProvider["commit"]>[0], options: Parameters<StreamProvider["commit"]>[1]) { return this.provider().then(provider => provider.commit(request, options)); }
  readCommit(id: Parameters<StreamProvider["readCommit"]>[0]) { return this.provider().then(provider => provider.readCommit(id)); }
  createToken(request: Parameters<NonNullable<StreamProvider["createToken"]>>[0]) { return this.provider().then(provider => {
    if (provider.createToken === undefined) throw new Error("provider does not support token creation");
    return provider.createToken(request);
  }); }
}
