import { OBJECTS_REMOTE_POLICY } from "./generated-client.js";
import { HttpObjectsV2, type ObjectsV2HttpOptions } from "./v2-http.js";
import type { ObjectsV2Provider } from "./v2.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";

export type ObjectsV2Transport = "grpc" | "http";
export interface ObjectsV2Environment extends Omit<ObjectsV2HttpOptions, "fetch"> { readonly transport?: ObjectsV2Transport }
export type ObjectsV2Client = ObjectsV2Provider;

/** Browser entrypoint: the Rust-qualified browser policy exposes HTTP only. */
export async function fromEnv(environment: ObjectsV2Environment): Promise<ObjectsV2Client> {
  const selected = environment.transport === undefined
    ? OBJECTS_REMOTE_POLICY.transport.browser[0]
    : OBJECTS_REMOTE_POLICY.transport.browser.find(option => option.kind === environment.transport);
  if (selected === undefined) throw new TypeError(`Objects transport ${environment.transport ?? "default"} is unavailable in the browser runtime`);
  if (selected.kind !== "http") throw new TypeError("Objects gRPC transport requires a native Node or Bun runtime");
  await ensureObjectsWasm();
  return new HttpObjectsV2(environment);
}
