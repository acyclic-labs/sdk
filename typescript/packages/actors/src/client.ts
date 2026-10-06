import { HttpActorsClient, type HttpActorsOptions } from "./http.js";
import { ACTORS_REMOTE_POLICY, isRustOwnedTransportUnavailable, selectRustOwnedTransport } from "./generated-client.js";
import { ensureActorsWasm, validateActorsCredential, validateActorsEndpoint, validateActorsGrpcEndpoint } from "./wasm-runtime.js";

export type ActorsTransport = "grpc" | "http";
export interface ActorsEnvironment extends Omit<HttpActorsOptions, "fetcher"> { readonly transport?: ActorsTransport }
type ActorsGrpcClient = ReturnType<typeof import("./grpc.js")["createActorsGrpcClient"]>;
export type ActorsClient = HttpActorsClient | ActorsGrpcClient;

export async function fromEnv(environment: ActorsEnvironment): Promise<ActorsClient> {
  const runtime = isNativeRuntime() ? "native" : "browser";
  const selected = selectRustOwnedTransport(ACTORS_REMOTE_POLICY, runtime, environment.transport);
  await ensureActorsWasm();
  validateActorsCredential(environment.token);
  if (selected === "http") {
    validateActorsEndpoint(environment.endpoint);
    return new HttpActorsClient(environment);
  }
  if (selected !== "grpc" || runtime !== "native") throw new TypeError("Actors gRPC transport requires a native Node or Bun runtime");
  validateActorsGrpcEndpoint(environment.endpoint);
  let createActorsGrpcClient: typeof import("./grpc.js")["createActorsGrpcClient"];
  try {
    ({ createActorsGrpcClient } = await import("./grpc.js"));
  } catch (error) {
    if (!isRustOwnedTransportUnavailable(error)) throw error;
    validateActorsEndpoint(environment.endpoint);
    return new HttpActorsClient(environment);
  }
  return createActorsGrpcClient({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
