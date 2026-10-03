import { OBJECTS_REMOTE_POLICY } from "./generated-client.js";
import { HttpObjectsV2, type ObjectsV2HttpOptions } from "./v2-http.js";
import type { ObjectsV2Provider } from "./v2.js";

export type ObjectsV2Transport = "grpc" | "http";
export interface ObjectsV2Environment extends Omit<ObjectsV2HttpOptions, "fetch"> { readonly transport?: ObjectsV2Transport }

export async function fromEnv(environment: ObjectsV2Environment): Promise<ObjectsV2Provider> {
  const runtime = isNativeRuntime() ? "native" : "browser";
  const options = OBJECTS_REMOTE_POLICY.transport[runtime];
  const selected = environment.transport === undefined ? options[0] : options.find(option => option.kind === environment.transport);
  if (selected === undefined) throw new TypeError(`Objects transport ${environment.transport ?? "default"} is unavailable in the ${runtime} runtime`);
  if (selected.kind === "http") return new HttpObjectsV2(environment);
  if (selected.kind !== "grpc" || runtime !== "native") throw new TypeError("Objects gRPC transport requires a native Node or Bun runtime");
  const { GrpcObjectsV2 } = await import("./v2-grpc.js");
  return new GrpcObjectsV2({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
