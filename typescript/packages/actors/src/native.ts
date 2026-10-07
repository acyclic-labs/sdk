import { Buffer } from "node:buffer";
import { arch, platform } from "node:process";
import type * as GeneratedNative from "../generated/native/binding.js";

type NativeModule = typeof GeneratedNative;
type NativeClient = InstanceType<NativeModule["NativeActorsClient"]>;
type NativeCancellation = InstanceType<NativeModule["NativeActorsCancellation"]>;
type NativeResult = Awaited<ReturnType<NativeClient["createActorResult"]>>;

let modulePromise: Promise<NativeModule> | undefined;

async function loadNativeModule(): Promise<NativeModule> {
  const target = `${platform}-${arch}`;
  modulePromise ??= import(`@acyclic-labs/actors-${target}`).then((module) => {
    const namespace = module as NativeModule & { readonly default?: NativeModule };
    const binding = namespace.NativeActorsClient === undefined ? namespace.default : namespace;
    if (binding?.NativeActorsClient === undefined || binding.NativeActorsCancellation === undefined) {
      throw new Error("Actors native companion did not export the generated Rust N-API binding");
    }
    return binding;
  }).catch((error: unknown) => {
    modulePromise = undefined;
    throw error;
  });
  return modulePromise;
}

export class NativeActorsClient {
  private constructor(
    private readonly binding: NativeClient,
    private readonly module: NativeModule,
  ) {}

  static async connect(endpoint: string, token: string, caCertificate?: Uint8Array): Promise<NativeActorsClient> {
    const module = await loadNativeModule();
    const binding = caCertificate === undefined
      ? await module.NativeActorsClient.connect(endpoint, token)
      : await module.NativeActorsClient.connectWithCa(endpoint, token, Buffer.from(caCertificate));
    return new NativeActorsClient(binding, module);
  }

  get transport(): string {
    return this.binding.transport();
  }

  async call(operation: string, request: Uint8Array, signal?: AbortSignal): Promise<Uint8Array> {
    const cancellation: NativeCancellation = new this.module.NativeActorsCancellation();
    const abort = () => cancellation.cancel();
    if (signal?.aborted) abort();
    else signal?.addEventListener("abort", abort, { once: true });
    try {
      const method = this.binding[`${operation}Result` as keyof NativeClient] as (
        request: Buffer,
        cancellation: NativeCancellation,
      ) => Promise<NativeResult>;
      const result = await method.call(this.binding, Buffer.from(request), cancellation);
      if (result.error !== undefined) throw result.error;
      if (result.value === undefined) throw new Error("Actors native companion returned no result");
      return new Uint8Array(result.value);
    } finally {
      signal?.removeEventListener("abort", abort);
    }
  }
}
