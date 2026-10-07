import { Buffer } from "node:buffer";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import type * as GeneratedNative from "../generated/native/binding.js";
import { normalizeActorsError, type ActorsOperation } from "./client.js";

type NativeModule = typeof GeneratedNative;
type NativeClient = InstanceType<NativeModule["NativeActorsClient"]>;
type NativeCancellation = InstanceType<NativeModule["NativeActorsCancellation"]>;
type NativeResult = Awaited<ReturnType<NativeClient["createActorResult"]>>;
type NativeConnectResult = Awaited<ReturnType<NativeModule["NativeActorsClient"]["connectResult"]>>;

let modulePromise: Promise<NativeModule> | undefined;
const requireNative = createRequire(import.meta.url);
const nativeBindingPath = fileURLToPath(new URL("../generated/native/binding.cjs", import.meta.url));

async function loadNativeModule(): Promise<NativeModule> {
  modulePromise ??= Promise.resolve().then(() => {
    const namespace = requireNative(nativeBindingPath) as NativeModule & { readonly default?: NativeModule };
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

/**
 * Rust-owned semantic constructors from the generated N-API companion.
 *
 * The native companion is optional, so loading it is asynchronous. These
 * pass-throughs deliberately contain no local validation or coercion; their
 * return types come from the generated N-API declaration.
 */
export async function ActorId(value: string): Promise<ReturnType<NativeModule["ActorId"]>> {
  const module = await loadNativeModule();
  return module.ActorId(value);
}

export async function CodeSha256(value: Uint8Array): Promise<ReturnType<NativeModule["CodeSha256"]>> {
  const module = await loadNativeModule();
  return module.CodeSha256(value);
}

export async function PositiveU64(value: bigint): Promise<ReturnType<NativeModule["PositiveU64"]>> {
  const module = await loadNativeModule();
  return module.PositiveU64(value);
}

export class NativeActorsClient {
  private constructor(
    private readonly binding: NativeClient,
    private readonly module: NativeModule,
  ) {}

  static async connect(endpoint: string, token: string, caCertificate?: Uint8Array): Promise<NativeActorsClient> {
    try {
      const module = await loadNativeModule();
      const result: NativeConnectResult = caCertificate === undefined
        ? await module.NativeActorsClient.connectResult(endpoint, token, undefined)
        : await module.NativeActorsClient.connectWithCaResult(
          endpoint,
          token,
          Buffer.from(caCertificate),
          undefined,
        );
      if (result.error !== undefined) throw normalizeActorsError(result.error);
      if (result.client === undefined) {
        throw normalizeActorsError({ code: "internal", message: "Actors native companion returned no client" });
      }
      return new NativeActorsClient(result.client, module);
    } catch (error) {
      throw normalizeActorsError(error);
    }
  }

  get transport(): string {
    return this.binding.transport();
  }

  async call(operation: ActorsOperation, request: Uint8Array, signal?: AbortSignal): Promise<Uint8Array> {
    const cancellation: NativeCancellation = new this.module.NativeActorsCancellation();
    const abort = () => cancellation.cancel();
    if (signal?.aborted) abort();
    else signal?.addEventListener("abort", abort, { once: true });
    try {
      const method = this.binding[`${operation}Result`];
      const result = await method.call(this.binding, Buffer.from(request), cancellation);
      if (result.error !== undefined) throw normalizeActorsError(result.error);
      if (result.value === undefined) {
        throw normalizeActorsError({ code: "internal", message: "Actors native companion returned no result" });
      }
      return new Uint8Array(result.value);
    } finally {
      signal?.removeEventListener("abort", abort);
    }
  }
}
