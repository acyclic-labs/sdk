import type { DeploymentManifest, EntryRuntime } from "../src/generated/manifest.js";
import type { Env } from "./generated/env.js";

type StreamHandle = { append(bytes: Uint8Array, retryKey: string): Promise<void> };
type SecretHandle = { reveal(): Promise<string> };
type ActorEnv = Env<StreamHandle, never, never, never, never, never, never, never, never, never, never, SecretHandle>;

export function author(env: ActorEnv): Promise<void> {
  const result = env.INBOX.append(new Uint8Array([1]), "delivery/effect");
  // @ts-expect-error unselected bindings are absent
  env.compute.run();
  // @ts-expect-error secret references are typed handles
  const plaintext: string = env.MODEL_KEY;
  return result;
}
export const actor: EntryRuntime["kind"] = "actor";
// @ts-expect-error unknown execution products are rejected
export const invalid: EntryRuntime["kind"] = "daemon";
export type Manifest = DeploymentManifest;