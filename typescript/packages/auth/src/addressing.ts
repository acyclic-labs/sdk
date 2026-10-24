import {
  encodeResourceIdentity, resourceIdentityAccount, resourceIdentityId,
  validateResourceLabel as validateLabel,
  resourceIdentityHostname, canonicalResourceHost as canonicalHost,
  resourceIdentityFromHostname, encodeActorReference, actorReferenceField,
  actorLocationHint as locationHint, actorReferenceFromLegacyFields, canonicalWorkerRoute, workerRouteMatches, selectWorkerRoute as selectRoute,
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

  /** Exact configured suffix, two identity labels and byte-exact re-rendering. */
  static fromHostname(host: string, domain: string): ResourceIdentity {
    requireText(host); requireText(domain);
    return ResourceIdentity.decode(resourceIdentityFromHostname(host, domain));
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

/** Exactly one DNS label, with the Rust validator's typed errors. */
export function validateResourceLabel(label: Uint8Array): void {
  requireBytes(label);
  validateLabel(label);
}

/** Stable discriminants carried by errors from the Rust codec and DNS validator. */
export interface IdentityError extends Error {
  readonly name: "IdentityError";
  readonly code: "empty_account" | "empty_id" | "malformed_identity"
    | "invalid_actor_id" | "invalid_namespace" | "reserved_namespace" | "invalid_location_hint";
}

/** An actor address contains three fields and never contains authority. */
export interface ActorReferenceFields {
  readonly account: Uint8Array;
  readonly namespace: Uint8Array;
  readonly id: Uint8Array;
}

/** Immutable, versioned plain triple, encoded and validated only by Rust. */
export class ActorReference {
  readonly #encoded: Uint8Array;

  constructor(account: Uint8Array, namespace: Uint8Array, id: Uint8Array) {
    requireBytes(account); requireBytes(namespace); requireBytes(id);
    this.#encoded = encodeActorReference(account, namespace, id);
  }

  static decode(encoded: Uint8Array): ActorReference {
    requireBytes(encoded);
    const bytes = encoded.slice();
    return new ActorReference(actorReferenceField(bytes, 0), actorReferenceField(bytes, 1), actorReferenceField(bytes, 2));
  }

  static fromFields(fields: ActorReferenceFields): ActorReference {
    return new ActorReference(fields.account, fields.namespace, fields.id);
  }

  /** Only an absent field maps to the reserved default, never an empty string. */
  static fromLegacyFields(account: Uint8Array, namespace: string | undefined, id: string): ActorReference {
    requireBytes(account); requireText(id);
    if (namespace !== undefined) requireText(namespace);
    return ActorReference.decode(actorReferenceFromLegacyFields(account, namespace, id));
  }

  get account(): Uint8Array { return actorReferenceField(this.#encoded, 0); }
  get namespace(): Uint8Array { return actorReferenceField(this.#encoded, 1); }
  get id(): Uint8Array { return actorReferenceField(this.#encoded, 2); }
  encode(): Uint8Array { return this.#encoded.slice(); }
  toFields(): ActorReferenceFields { return { account: this.account, namespace: this.namespace, id: this.id }; }
  equals(other: ActorReference): boolean {
    return this.#encoded.length === other.#encoded.length
      && this.#encoded.every((byte, index) => byte === other.#encoded[index]);
  }
}
export interface DnsError extends Error {
  readonly name: "DnsError";
  readonly code: "empty_label" | "label_too_long" | "invalid_character" | "edge_hyphen" | "host_too_long" | "numeric_host";
}

/** A Worker route parsed and matched by the shared Rust codec. */
export class WorkerRoutePattern {
  readonly #pattern: string;
  constructor(pattern: string) { requireText(pattern); this.#pattern = canonicalWorkerRoute(pattern); }
  toString(): string { return this.#pattern; }
  matches(url: string): boolean { requireText(url); return workerRouteMatches(this.#pattern, url); }
}
export interface WorkerRoute { readonly pattern: WorkerRoutePattern; readonly target?: string; }
/** undefined is no match; null is a matched exclusion, suppressing broader routes. */
export function selectWorkerRoute(routes: readonly WorkerRoute[], url: string): string | null | undefined {
  requireText(url);
  for (const route of routes) {
    if (route.target !== undefined) {
      requireText(route.target);
      validateResourceLabel(new TextEncoder().encode(route.target));
    }
  }
  const target = selectRoute(routes.map(r => r.pattern.toString()), routes.map(r => r.target ?? ""), url);
  return target === "" ? null : target;
}

export function actorLocationHint(value: string): string { requireText(value); return locationHint(value); }
