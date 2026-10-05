import { HttpActorsClient, type HttpActorsOptions } from "./http.js";
import { ACTORS_REMOTE_POLICY } from "./generated-client.js";

export type ActorsTransport = "grpc" | "http";
export interface ActorsEnvironment extends Omit<HttpActorsOptions, "fetcher"> { readonly transport?: ActorsTransport }
type ActorsGrpcClient = ReturnType<typeof import("./grpc.js")["createActorsGrpcClient"]>;
export type ActorsClient = HttpActorsClient | ActorsGrpcClient;

export async function fromEnv(environment: ActorsEnvironment): Promise<ActorsClient> {
  const runtime = isNativeRuntime() ? "native" : "browser";
  const options = ACTORS_REMOTE_POLICY.transport[runtime];
  const selected = environment.transport === undefined ? options[0] : options.find(option => option.kind === environment.transport);
  if (selected === undefined) throw new TypeError(`Actors transport ${environment.transport ?? "default"} is unavailable in the ${runtime} runtime`);
  if (selected.kind === "http") return new HttpActorsClient(environment);
  if (selected.kind !== "grpc" || runtime !== "native") throw new TypeError("Actors gRPC transport requires a native Node or Bun runtime");
  const { createActorsGrpcClient } = await import("./grpc.js");
  return createActorsGrpcClient({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
