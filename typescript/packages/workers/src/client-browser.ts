import { HttpWorkersClient, type HttpWorkersOptions } from "./http.js";
import { WORKERS_REMOTE_POLICY } from "./generated-client.js";
import { ensureWorkersWasm } from "./wasm-runtime.js";

export type WorkersTransport = "grpc" | "http";
export interface WorkersEnvironment extends Omit<HttpWorkersOptions, "fetcher"> { readonly transport?: WorkersTransport }
export type WorkersClient = HttpWorkersClient;

/** Browser entrypoint: the Rust-qualified browser policy exposes HTTP only. */
export async function fromEnv(environment: WorkersEnvironment): Promise<WorkersClient> {
  const selected = environment.transport === undefined
    ? WORKERS_REMOTE_POLICY.transport.browser[0]
    : WORKERS_REMOTE_POLICY.transport.browser.find(option => option.kind === environment.transport);
  if (selected === undefined) throw new TypeError(`Workers transport ${environment.transport ?? "default"} is unavailable in the browser runtime`);
  if (selected.kind !== "http") throw new TypeError("Workers gRPC transport requires a native Node or Bun runtime");
  await ensureWorkersWasm();
  return new HttpWorkersClient(environment);
}
