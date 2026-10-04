import { OBJECTS_REMOTE_POLICY, selectRustOwnedTransport } from "./generated-client.js";
import { HttpObjectsV2, type ObjectsV2HttpOptions } from "./v2-http.js";
import type { ObjectsV2Provider } from "./v2.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";

export type ObjectsV2Transport = typeof OBJECTS_REMOTE_POLICY.transport.native[number]["kind"];
export interface ObjectsV2Environment extends Omit<ObjectsV2HttpOptions, "fetch"> { readonly transport?: ObjectsV2Transport }
export type ObjectsV2Client = ObjectsV2Provider;

/** Browser entrypoint: the Rust-qualified browser policy exposes HTTP only. */
export async function fromEnv(environment: ObjectsV2Environment): Promise<ObjectsV2Client> {
  const selected = selectRustOwnedTransport(OBJECTS_REMOTE_POLICY, "browser", environment.transport);
  if (selected !== "http") throw new TypeError("Objects gRPC transport requires a native Node or Bun runtime");
  await ensureObjectsWasm();
  return new HttpObjectsV2(environment);
}
