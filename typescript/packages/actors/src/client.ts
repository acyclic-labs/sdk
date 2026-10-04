import { HttpActorsClient, type HttpActorsOptions } from "./http.js";
import { ensureActorsWasm } from "./wasm-runtime.js";
import {
  isRustOwnedTransportUnavailable,
  selectRustOwnedTransport,
  ACTORS_REMOTE_POLICY,
  type RustOwnedRuntime,
} from "./generated-client.js";

export type ActorsTransport = typeof ACTORS_REMOTE_POLICY.transport.native[number]["kind"];
export interface ActorsEnvironment extends Omit<HttpActorsOptions, "fetcher"> { readonly transport?: ActorsTransport }
type ActorsGrpcClient = ReturnType<typeof import("./grpc.js")["createActorsGrpcClient"]>;
export type ActorsClient = HttpActorsClient | ActorsGrpcClient;

export async function fromEnv(environment: ActorsEnvironment): Promise<ActorsClient> {
  const runtime: RustOwnedRuntime = isNativeRuntime() ? "native" : "browser";
  let selected = selectRustOwnedTransport(ACTORS_REMOTE_POLICY, runtime, environment.transport);
  if (selected === "http") {
    await ensureActorsWasm();
    return new HttpActorsClient(environment);
  }
  if (selected !== "grpc" || runtime !== "native") throw new TypeError("Actors gRPC transport requires a native Node or Bun runtime");
  try {
    const { createActorsGrpcClient } = await import("./grpc.js");
    return createActorsGrpcClient({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
  } catch (error) {
    if (environment.transport !== undefined || !isRustOwnedTransportUnavailable(error)) throw error;
    selected = selectRustOwnedTransport(ACTORS_REMOTE_POLICY, runtime, undefined, { grpc: false });
    if (selected !== "http") throw error;
    await ensureActorsWasm();
    return new HttpActorsClient(environment);
  }
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
