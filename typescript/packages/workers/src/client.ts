import { HttpWorkersClient, type HttpWorkersOptions } from "./http.js";
import { WORKERS_REMOTE_POLICY, isRustOwnedTransportUnavailable, selectRustOwnedTransport } from "./generated-client.js";

export type WorkersTransport = "grpc" | "http";
export interface WorkersEnvironment extends Omit<HttpWorkersOptions, "fetcher"> { readonly transport?: WorkersTransport }
type WorkersGrpcClient = ReturnType<typeof import("./grpc.js")["createWorkersGrpcClient"]>;
export type WorkersClient = HttpWorkersClient | WorkersGrpcClient;

export async function fromEnv(environment: WorkersEnvironment): Promise<WorkersClient> {
  const runtime = isNativeRuntime() ? "native" : "browser";
  const selected = selectRustOwnedTransport(WORKERS_REMOTE_POLICY, runtime, environment.transport);
  if (selected === "http") return new HttpWorkersClient(environment);
  if (selected !== "grpc" || runtime !== "native") throw new TypeError("Workers gRPC transport requires a native Node or Bun runtime");
  let createWorkersGrpcClient: typeof import("./grpc.js")["createWorkersGrpcClient"];
  try {
    ({ createWorkersGrpcClient } = await import("./grpc.js"));
  } catch (error) {
    if (!isRustOwnedTransportUnavailable(error)) throw error;
    return new HttpWorkersClient(environment);
  }
  return createWorkersGrpcClient({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
