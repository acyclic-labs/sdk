import {
  resourceAddress as address, resourceAddressPreimage as preimage,
  canonicalResourceHost as canonicalHost,
} from "./wasm.js";

/** Stable wire codes. Names and accounts are exact bytes; no Unicode normalization. */
export const ResourceKind = Object.freeze({
  Bucket: 1, Stream: 2, Actor: 3, Worker: 4, FsWorkspace: 5, Machine: 6,
} as const);
export type ResourceKind = typeof ResourceKind[keyof typeof ResourceKind];

function kindCode(kind: ResourceKind): number {
  if (!Number.isInteger(kind) || kind < 1 || kind > 6) throw new TypeError("unknown resource kind");
  return kind;
}

/** Canonical 256-bit BLAKE3 address, derived by the shared Rust WASM codec. */
export function resourceAddress(account: Uint8Array, kind: ResourceKind, name: Uint8Array): Uint8Array {
  return address(account, kindCode(kind), name);
}

/** Length-prefixed domain, version, account, kind and name transcript. */
export function resourceAddressPreimage(account: Uint8Array, kind: ResourceKind, name: Uint8Array): Uint8Array {
  return preimage(account, kindCode(kind), name);
}

/** One strict lowercase host spelling for URL authority, TLS SNI and signing. */
export function canonicalResourceHost(host: string): string { return canonicalHost(host); }
