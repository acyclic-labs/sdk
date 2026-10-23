import {
  encodeResourceIdentity, resourceIdentityAccount, resourceIdentityId,
  resourceIdentityHostname, canonicalResourceHost as canonicalHost,
} from "./wasm.js";

/** Check the JS/WASM boundary shape; byte-content validation remains in Rust. */
function requireBytes(value: Uint8Array): void {
  if (!(value instanceof Uint8Array)) throw new TypeError("identity fields must be Uint8Array bytes");
}

function requireText(value: string): void {
  if (typeof value !== "string") throw new TypeError("DNS text must be a string");
}

/** Opaque account and resource id, encoded by the shared Rust codec. */
export class ResourceIdentity {
  readonly #encoded: Uint8Array;

  constructor(account: Uint8Array, id: Uint8Array) {
    requireBytes(account);
    requireBytes(id);
    this.#encoded = encodeResourceIdentity(account, id);
  }

  /** Decode exactly one canonical wire/storage pair; no normalization. */
  static decode(encoded: Uint8Array): ResourceIdentity {
    requireBytes(encoded);
    const snapshot = encoded.slice();
    return new ResourceIdentity(resourceIdentityAccount(snapshot), resourceIdentityId(snapshot));
  }

  /** Wrap existing fields without changing their wire or storage representation. */
  static fromFields(fields: ResourceIdentityFields): ResourceIdentity {
    return new ResourceIdentity(fields.account, fields.id);
  }

  /** Recover independent copies of the original account-scoped fields. */
  toFields(): ResourceIdentityFields { return { account: this.account, id: this.id }; }

  /** Value equality over the canonical encoding produced by Rust. */
  equals(other: ResourceIdentity): boolean {
    const right = other.#encoded;
    return this.#encoded.length === right.length
      && this.#encoded.every((byte, index) => byte === right[index]);
  }

  /** Defensive copies preserve value semantics across mutable JS byte arrays. */
  get account(): Uint8Array { return resourceIdentityAccount(this.#encoded); }
  get id(): Uint8Array { return resourceIdentityId(this.#encoded); }
  encode(): Uint8Array { return this.#encoded.slice(); }

  /** Direct-account DNS form, identical for URL authority, TLS SNI and signing. */
  hostname(domain: string): string {
    requireText(domain);
    return resourceIdentityHostname(this.#encoded, domain);
  }
}

/** The existing account and id fields; wrapping them does not create a resource. */
export interface ResourceIdentityFields {
  readonly account: Uint8Array;
  readonly id: Uint8Array;
}

/** One strict lowercase hostname spelling; rejects aliases instead of folding. */
export function canonicalResourceHost(host: string): string {
  requireText(host);
  return canonicalHost(host);
}

/** Stable discriminants carried by errors from the Rust codec and DNS validator. */
export interface IdentityError extends Error {
  readonly name: "IdentityError";
  readonly code: "empty_account" | "empty_id" | "malformed_identity";
}
export interface DnsError extends Error {
  readonly name: "DnsError";
  readonly code: "empty_label" | "label_too_long" | "invalid_character" | "edge_hyphen" | "host_too_long" | "numeric_host";
}
