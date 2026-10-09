// Generated from Rust nominal semantic declarations. Do not edit.
import type * as Semantic from "./semantic/actors/index.js";
import type { ReadonlyBytes } from "./readonly.js";

export interface ActorsNominalBinding {
  readonly ActorId: (value: string) => Semantic.ActorId;
  readonly CodeSha256: (value: ReadonlyBytes | Uint8Array) => Semantic.CodeSha256;
  readonly PositiveU64: (value: bigint) => Semantic.PositiveU64;
  readonly CurrentHeadMarker: (value: true) => Semantic.CurrentHeadMarker;
}

let resolveBinding: (() => Promise<ActorsNominalBinding>) | undefined;

/** Installs the platform resolver supplied by the package's Rust bridge. */
export function configureActorsNominalBinding(resolve: () => Promise<ActorsNominalBinding>): void {
  resolveBinding = resolve;
}

async function binding(): Promise<ActorsNominalBinding> {
  if (resolveBinding === undefined) throw new Error("Actors nominal binding is not configured");
  return resolveBinding();
}

export async function ActorId(value: string): Promise<Semantic.ActorId> {
  return (await binding()).ActorId(value);
}

export async function CodeSha256(value: ReadonlyBytes | Uint8Array): Promise<Semantic.CodeSha256> {
  return (await binding()).CodeSha256(value);
}

export async function PositiveU64(value: bigint): Promise<Semantic.PositiveU64> {
  return (await binding()).PositiveU64(value);
}

export type CurrentHeadMarker = Semantic.CurrentHeadMarker;

export async function CurrentHeadMarker(value: true): Promise<CurrentHeadMarker> {
  return (await binding()).CurrentHeadMarker(value);
}
