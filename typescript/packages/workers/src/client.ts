import { HttpWorkersClient, type HttpWorkersOptions } from "./http.js";
import { ensureWorkersWasm } from "./wasm-runtime.js";
import {
  isRustOwnedTransportUnavailable,
  selectRustOwnedTransport,
  WORKERS_REMOTE_POLICY,
  type RustOwnedRuntime,
} from "./generated-client.js";

export type WorkersTransport = typeof WORKERS_REMOTE_POLICY.transport.native[number]["kind"];
export interface WorkersEnvironment extends Omit<HttpWorkersOptions, "fetcher"> { readonly transport?: WorkersTransport }
type WorkersGrpcClient = ReturnType<typeof import("./grpc.js")["createWorkersGrpcClient"]>;
export type WorkersClient = HttpWorkersClient | WorkersGrpcClient;

export async function fromEnv(environment: WorkersEnvironment): Promise<WorkersClient> {
  const runtime: RustOwnedRuntime = isNativeRuntime() ? "native" : "browser";
  let selected = selectRustOwnedTransport(WORKERS_REMOTE_POLICY, runtime, environment.transport);
  if (selected === "http") {
    // The fallback is initialized before constructing the client, so endpoint
    // and credential validation still happen before the first operation.
    await ensureWorkersWasm();
    return new HttpWorkersClient(environment);
  }
  if (selected !== "grpc" || runtime !== "native") throw new TypeError("Workers gRPC transport requires a native Node or Bun runtime");
  try {
    const { createWorkersGrpcClient } = await import("./grpc.js");
    return createWorkersGrpcClient({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
  } catch (error) {
    // A missing optional adapter is the only condition that may select the
    // next Rust-qualified transport. Construction, endpoint, and credential
    // errors are returned unchanged; no operation has been attempted.
    if (environment.transport !== undefined || !isRustOwnedTransportUnavailable(error)) throw error;
    selected = selectRustOwnedTransport(WORKERS_REMOTE_POLICY, runtime, undefined, { grpc: false });
    if (selected !== "http") throw error;
    await ensureWorkersWasm();
    return new HttpWorkersClient(environment);
  }
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
