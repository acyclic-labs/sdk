import type * as GeneratedBrowser from "../generated/wasm/acyclic_actors_wasm.js";
import { normalizeActorsError } from "./client.js";

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
    try {
      const module = await loadBrowserModule();
      return new BrowserActorsClient(new module.ActorsClient(endpoint, token));
    } catch (error) {
      throw normalizeActorsError(error);
    }
  }

  get transport(): string {
    return "grpc-web";
  }

  async call(operation: string, request: Uint8Array, signal?: AbortSignal): Promise<Uint8Array> {
    let abort: (() => void) | undefined;
    try {
      const module = await loadBrowserModule();
      const cancellation: BrowserCancellation = new module.CancellationHandle();
      const onAbort = () => cancellation.cancel();
      abort = onAbort;
      if (signal?.aborted) onAbort();
      else signal?.addEventListener("abort", onAbort, { once: true });
      const method = this.binding[operation as keyof BrowserClient] as (
        request: Uint8Array,
        cancellation: BrowserCancellation,
      ) => Promise<Uint8Array>;
      return await method.call(this.binding, request, cancellation);
    } catch (error) {
      throw normalizeActorsError(error);
    } finally {
      if (abort !== undefined) signal?.removeEventListener("abort", abort);
    }
  }
}
