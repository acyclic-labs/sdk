/** Initialize with WASM bytes in Node, then call the Rust-owned validator. */
export { default as initializeManifest, validate } from "../generated/wasm/acyclic_manifest.js";
export * from "./generated/manifest.js";
import { parseSecretReference as parseReference, formatSecretReference as formatReference,
  selectSecretReference as selectReference } from "../generated/wasm/acyclic_manifest.js";
import type { SecretRef } from "./generated/manifest.js";

/** Canonical bytes and arbitrary-depth segments, validated by the Rust owner. */
export function parseSecretReference(reference: string): { reference: string; segments: string[] } {
  const segments = parseReference(reference);
  return { reference: formatReference(segments), segments };
}

/** No URL normalization, aliases, implicit scope or secret resolution. */
export function formatSecretReference(segments: string[]): string {
  return formatReference(segments);
}

/** A separately admitted environment name; syntax grants no resolution rights. */
export function selectSecretReference(name: string, reference: string): SecretRef {
  return JSON.parse(selectReference(name, reference)) as SecretRef;
}
