import { HttpActorsClient, type HttpActorsOptions } from "./http.js";
import { ACTORS_REMOTE_POLICY } from "./generated-client.js";
import { ensureActorsWasm } from "./wasm-runtime.js";

export type ActorsTransport = "grpc" | "http";
export interface ActorsEnvironment extends Omit<HttpActorsOptions, "fetcher"> { readonly transport?: ActorsTransport }
export type ActorsClient = HttpActorsClient;

/** Browser entrypoint: the Rust-qualified browser policy exposes HTTP only. */
export async function fromEnv(environment: ActorsEnvironment): Promise<ActorsClient> {
  const selected = environment.transport === undefined
    ? ACTORS_REMOTE_POLICY.transport.browser[0]
    : ACTORS_REMOTE_POLICY.transport.browser.find(option => option.kind === environment.transport);
  if (selected === undefined) throw new TypeError(`Actors transport ${environment.transport ?? "default"} is unavailable in the browser runtime`);
  if (selected.kind !== "http") throw new TypeError("Actors gRPC transport requires a native Node or Bun runtime");
  await ensureActorsWasm();
  return new HttpActorsClient(environment);
}
