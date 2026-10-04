import { HttpActorsClient, type HttpActorsOptions } from "./http.js";
import { selectRustOwnedTransport, ACTORS_REMOTE_POLICY } from "./generated-client.js";
import { ensureActorsWasm } from "./wasm-runtime.js";

export type ActorsTransport = typeof ACTORS_REMOTE_POLICY.transport.native[number]["kind"];
export interface ActorsEnvironment extends Omit<HttpActorsOptions, "fetcher"> { readonly transport?: ActorsTransport }
export type ActorsClient = HttpActorsClient;

/** Browser entrypoint: the Rust-qualified browser policy exposes HTTP only. */
export async function fromEnv(environment: ActorsEnvironment): Promise<ActorsClient> {
  const selected = selectRustOwnedTransport(ACTORS_REMOTE_POLICY, "browser", environment.transport);
  if (selected !== "http") throw new TypeError("Actors gRPC transport requires a native Node or Bun runtime");
  await ensureActorsWasm();
  return new HttpActorsClient(environment);
}
