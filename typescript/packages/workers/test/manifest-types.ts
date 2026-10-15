import type { DeploymentManifest, EntryKind } from "../src/generated/manifest.js";
import type { Env } from "./generated/env.js";

type StreamHandle = { append(bytes: number[], retryKey: string): Promise<void> };
type SecretHandle = { reveal(): Promise<string> };
type CounterEnv = Env<StreamHandle, never, never, never, never, never, SecretHandle>;

export function author(env: CounterEnv): Promise<void> {
  const result = env.inbox.append([1], "delivery/effect");
  // @ts-expect-error unselected bindings are absent
  env.compute.run();
  // @ts-expect-error secret references become typed handles, not plaintext
  const plaintext: string = env.MODEL_KEY;
  return result;
}

export const actor: EntryKind = "actor";
// @ts-expect-error unknown execution products are rejected
export const invalid: EntryKind = "daemon";
export type Manifest = DeploymentManifest;
