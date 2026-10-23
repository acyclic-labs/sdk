import {
  encodeResourceIdentity, resourceIdentityAccount, resourceIdentityId,
  resourceIdentityHostname, canonicalResourceHost as canonicalHost,
} from "./wasm.js";

/** Opaque account and resource id, encoded by the shared Rust codec. */
export class ResourceIdentity {
  readonly #encoded: Uint8Array;

  constructor(account: Uint8Array, id: Uint8Array) {
    this.#encoded = encodeResourceIdentity(account, id);
  }

  /** Decode exactly one canonical wire/storage pair; no normalization. */
  static decode(encoded: Uint8Array): ResourceIdentity {
    const snapshot = encoded.slice();
    return new ResourceIdentity(resourceIdentityAccount(snapshot), resourceIdentityId(snapshot));
  }

  /** Defensive copies preserve value semantics across mutable JS byte arrays. */
  get account(): Uint8Array { return resourceIdentityAccount(this.#encoded); }
  get id(): Uint8Array { return resourceIdentityId(this.#encoded); }
  encode(): Uint8Array { return this.#encoded.slice(); }

  /** Direct-account DNS form, identical for URL authority, TLS SNI and signing. */
  hostname(domain: string): string { return resourceIdentityHostname(this.#encoded, domain); }
}

/** One strict lowercase hostname spelling; rejects aliases instead of folding. */
export function canonicalResourceHost(host: string): string { return canonicalHost(host); }

/** Stable discriminants carried by errors from the Rust codec and DNS validator. */
export interface IdentityError extends Error {
  readonly name: "IdentityError";
  readonly code: "empty_account" | "empty_id" | "malformed_identity";
}
export interface DnsError extends Error {
  readonly name: "DnsError";
  readonly code: "empty_label" | "label_too_long" | "invalid_character" | "edge_hyphen" | "host_too_long" | "numeric_host";
}
