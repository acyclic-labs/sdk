import { HttpWorkersClient, type HttpWorkersOptions } from "./http.js";
import { WORKERS_REMOTE_POLICY } from "./generated-client.js";

export type WorkersTransport = "grpc" | "http";
export interface WorkersEnvironment extends Omit<HttpWorkersOptions, "fetcher"> { readonly transport?: WorkersTransport }
type WorkersGrpcClient = ReturnType<typeof import("./grpc.js")["createWorkersGrpcClient"]>;
export type WorkersClient = HttpWorkersClient | WorkersGrpcClient;

export async function fromEnv(environment: WorkersEnvironment): Promise<WorkersClient> {
  const runtime = isNativeRuntime() ? "native" : "browser";
  const options = WORKERS_REMOTE_POLICY.transport[runtime];
  const selected = environment.transport === undefined ? options[0] : options.find(option => option.kind === environment.transport);
  if (selected === undefined) throw new TypeError(`Workers transport ${environment.transport ?? "default"} is unavailable in the ${runtime} runtime`);
  if (selected.kind === "http") return new HttpWorkersClient(environment);
  if (selected.kind !== "grpc" || runtime !== "native") throw new TypeError("Workers gRPC transport requires a native Node or Bun runtime");
  const { createWorkersGrpcClient } = await import("./grpc.js");
  return createWorkersGrpcClient({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
