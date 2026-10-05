import { isRustOwnedTransportUnavailable, OBJECTS_REMOTE_POLICY, selectRustOwnedTransport } from "./generated-client.js";
import { HttpObjectsV2, type ObjectsV2HttpOptions } from "./v2-http.js";
import type { ObjectsV2Provider } from "./v2.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";

export type ObjectsV2Transport = typeof OBJECTS_REMOTE_POLICY.transport.native[number]["kind"];
export interface ObjectsV2Environment extends Omit<ObjectsV2HttpOptions, "fetch"> { readonly transport?: ObjectsV2Transport }

export async function fromEnv(environment: ObjectsV2Environment): Promise<ObjectsV2Provider> {
  const runtime = isNativeRuntime() ? "native" : "browser";
  let selected = selectRustOwnedTransport(OBJECTS_REMOTE_POLICY, runtime, environment.transport);
  if (selected === "http") {
    await ensureObjectsWasm();
    return new HttpObjectsV2(environment);
  }
  if (selected !== "grpc" || runtime !== "native") throw new TypeError("Objects gRPC transport requires a native Node or Bun runtime");
  try {
    const { GrpcObjectsV2 } = await import("./v2-grpc.js");
    return new GrpcObjectsV2({ endpoint: environment.endpoint, token: environment.token, ...(environment.maximumResponseBytes === undefined ? {} : { maximumMessageBytes: environment.maximumResponseBytes }) });
  } catch (error) {
    // Only absence of the optional native adapter may select the next
    // Rust-qualified transport. Endpoint and credential failures remain
    // terminal and are surfaced before any application operation.
    if (environment.transport !== undefined || !isRustOwnedTransportUnavailable(error)) throw error;
    selected = selectRustOwnedTransport(OBJECTS_REMOTE_POLICY, runtime, undefined, { grpc: false, http: true });
    if (selected !== "http") throw error;
    await ensureObjectsWasm();
    return new HttpObjectsV2(environment);
  }
}

function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
