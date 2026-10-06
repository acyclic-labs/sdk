import { HttpWorkersClient, type HttpWorkersOptions } from "./http.js";
import { WORKERS_REMOTE_POLICY, selectRustOwnedTransport } from "./generated-client.js";
import { ensureWorkersWasm } from "./wasm-runtime.js";

export type WorkersTransport = "grpc" | "http";
export interface WorkersEnvironment extends Omit<HttpWorkersOptions, "fetcher"> { readonly transport?: WorkersTransport }
export type WorkersClient = HttpWorkersClient;

/** Browser entrypoint: the Rust-qualified browser policy exposes HTTP only. */
export async function fromEnv(environment: WorkersEnvironment): Promise<WorkersClient> {
  const selected = selectRustOwnedTransport(WORKERS_REMOTE_POLICY, "browser", environment.transport);
  if (selected !== "http") throw new TypeError("Workers gRPC transport requires a native Node or Bun runtime");
  await ensureWorkersWasm();
  return new HttpWorkersClient(environment);
}
