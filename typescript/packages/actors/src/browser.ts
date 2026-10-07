import type * as GeneratedBrowser from "../generated/wasm/acyclic_actors_wasm.js";

type BrowserModule = typeof GeneratedBrowser;
type BrowserClient = InstanceType<BrowserModule["ActorsClient"]>;
type BrowserCancellation = InstanceType<BrowserModule["CancellationHandle"]>;

let modulePromise: Promise<BrowserModule> | undefined;

async function loadBrowserModule(): Promise<BrowserModule> {
  const generatedModule = "../generated/wasm/acyclic_actors_wasm.js";
  modulePromise ??= import(generatedModule).then(async (module) => {
    const binding = module as BrowserModule;
    await binding.default?.();
    return binding;
  }).catch((error: unknown) => {
    modulePromise = undefined;
    throw error;
  });
  return modulePromise;
}

export class BrowserActorsClient {
  private constructor(private readonly binding: BrowserClient) {}

  static async connect(endpoint: string, token: string): Promise<BrowserActorsClient> {
    const module = await loadBrowserModule();
    return new BrowserActorsClient(new module.ActorsClient(endpoint, token));
  }

  get transport(): string {
    return "grpc-web";
  }

  async call(operation: string, request: Uint8Array, signal?: AbortSignal): Promise<Uint8Array> {
    const module = await loadBrowserModule();
    const cancellation: BrowserCancellation = new module.CancellationHandle();
    const abort = () => cancellation.cancel();
    if (signal?.aborted) abort();
    else signal?.addEventListener("abort", abort, { once: true });
    try {
      const method = this.binding[operation as keyof BrowserClient] as (
        request: Uint8Array,
        cancellation: BrowserCancellation,
      ) => Promise<Uint8Array>;
      return await method.call(this.binding, request, cancellation);
    } finally {
      signal?.removeEventListener("abort", abort);
    }
  }
}
