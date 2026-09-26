import { blake3 } from "@noble/hashes/blake3.js";
import type { ETag, IdempotencyKey, ObjectMetadata } from "./index.js";

declare const replaceableBrand: unique symbol;

/** Stable opaque identity of one replaceable object. Do not parse or derive it. */
export type ObjectId = string & { readonly [replaceableBrand]: "ObjectId" };

/** Validate and brand an object identity received from the service. */
export function objectId(value: string): ObjectId {
  if (!value || value.length > 4_096 || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new TypeError("object id must be non-empty, bounded, and free of control characters");
  }
  return value as ObjectId;
}

export type ReplaceCondition =
  | { readonly kind: "ifAbsent" }
  | { readonly kind: "ifMatch"; readonly etag: ETag };

export interface ReplacePutRequest {
  readonly object: ObjectId;
  readonly body: Uint8Array;
  readonly metadata?: Partial<ObjectMetadata>;
  readonly condition?: ReplaceCondition;
  /** Reuse this exact key when a transport failure leaves the result ambiguous. */
  readonly idempotencyKey: IdempotencyKey;
}

export interface PutReceipt {
  readonly object: ObjectId;
  readonly etag: ETag;
  readonly size: bigint;
}

export interface CurrentGetOptions {
  readonly range?: { readonly start: number; readonly endExclusive: number };
  readonly ifMatch?: ETag;
  readonly ifNoneMatch?: ETag;
  readonly maximumBytes?: number;
  /** Reads may be served from an admitted regional replica. */
  readonly consistency?: "weak";
}

export interface CurrentObject {
  readonly object: ObjectId;
  readonly etag: ETag;
  /** Full current value size, including when a range was requested. */
  readonly size: bigint;
  readonly metadata: ObjectMetadata;
  readonly body: Uint8Array;
  readonly contentRange?: { readonly start: number; readonly endExclusive: number; readonly total: number };
}

export interface Verification {
  readonly object: ObjectId;
  readonly etag: ETag;
  readonly size: bigint;
  readonly valid: boolean;
}

export type ReplaceableObjectErrorCode =
  | "not_found"
  | "precondition_failed"
  | "idempotency_mismatch"
  | "invalid_object_id"
  | "invalid_range"
  | "response_too_large"
  | "invalid_response";

export class ReplaceableObjectError extends Error {
  constructor(readonly code: ReplaceableObjectErrorCode, message: string) {
    super(message);
    this.name = "ReplaceableObjectError";
  }
}

/** An operation may have committed and should be retried with the same key. */
export class AmbiguousMutationError extends Error {
  constructor(message: string, readonly cause?: unknown) {
    super(message);
    this.name = "AmbiguousMutationError";
  }
}

export function isAmbiguousMutationError(value: unknown): value is AmbiguousMutationError {
  return value instanceof AmbiguousMutationError ||
    (value instanceof ReplaceableTransportError && value.ambiguousMutation);
}

export interface ReplaceableObjectsProvider {
  replacePut(request: ReplacePutRequest): Promise<PutReceipt>;
  getCurrent(object: ObjectId, options?: CurrentGetOptions): Promise<CurrentObject>;
  verifyCurrent(object: ObjectId, expectedEtag?: ETag): Promise<Verification>;
}

export interface ReplaceableHttpOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly fetcher?: typeof fetch;
  readonly maximumResponseBytes?: number;
}

/** HTTP adapter for the v2 current-value routes.
 *
 * Routes are intentionally small and stable: `POST v2/objects/put`,
 * `POST v2/objects/get-current`, and `POST v2/objects/verify-current`.
 * The JSON envelope uses the SDK's existing `$bytes`, `$bigint`, and `$map`
 * representations. A failed PUT with no response is always ambiguous.
 */
export class HttpReplaceableObjectsProvider implements ReplaceableObjectsProvider {
  readonly #endpoint: string;
  readonly #token: string;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;

  constructor(options: ReplaceableHttpOptions) {
    const endpoint = new URL(options.endpoint);
    if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) {
      throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment");
    }
    if (!options.token.trim()) throw new TypeError("token is required");
    const maximum = options.maximumResponseBytes ?? 16 * 1024 * 1024;
    if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new RangeError("maximumResponseBytes must be a positive safe integer");
    this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? fetch;
    this.#maximum = maximum;
  }

  replacePut(request: ReplacePutRequest): Promise<PutReceipt> {
    return this.#call("put", request, receipt, true);
  }

  getCurrent(object: ObjectId, options: CurrentGetOptions = {}): Promise<CurrentObject> {
    return this.#call("get-current", { object, ...options }, currentObject, false);
  }

  verifyCurrent(object: ObjectId, expectedEtag?: ETag): Promise<Verification> {
    return this.#call("verify-current", { object, ...(expectedEtag === undefined ? {} : { expectedEtag }) }, verification, false);
  }

  async #call<Value>(route: string, request: unknown, project: Decoder<Value>, mutation: boolean): Promise<Value> {
    let response: Response;
    try {
      response = await this.#fetcher(new URL(`v2/objects/${route}`, this.#endpoint), {
        method: "POST",
        headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" },
        body: encode(request),
      });
    } catch (error) {
      if (mutation) throw new AmbiguousMutationError("Objects PUT outcome is ambiguous; retry the same idempotency key", error);
      throw error;
    }
    let bytes: Uint8Array;
    try {
      bytes = await boundedBytes(response, this.#maximum);
    } catch (error) {
      if (mutation) throw new AmbiguousMutationError("Objects PUT response was incomplete; retry the same idempotency key", error);
      throw error;
    }
    if (!response.ok) {
      if (mutation && (response.status === 408 || response.status === 429 || response.status >= 500)) {
        throw new AmbiguousMutationError(`Objects PUT returned HTTP ${response.status}; retry the same idempotency key`);
      }
      throw new ReplaceableTransportError(`HTTP ${response.status}`, response.status, false);
    }
    try {
      return project(decode(new TextDecoder().decode(bytes)));
    } catch {
      throw new ReplaceableTransportError(`invalid v2 Objects ${route} response`, response.status, false);
    }
  }
}

export class ReplaceableTransportError extends Error {
  constructor(message: string, readonly status: number, readonly ambiguousMutation: boolean) {
    super(message);
    this.name = "ReplaceableTransportError";
  }
}

/** Deterministic in-memory current-value provider for tests and local agents. */
export class MemoryReplaceableObjectsProvider implements ReplaceableObjectsProvider {
  readonly #values = new Map<ObjectId, StoredValue>();
  readonly #idempotency = new Map<IdempotencyKey, { fingerprint: string; receipt: PutReceipt }>();

  async replacePut(request: ReplacePutRequest): Promise<PutReceipt> {
    validateRequest(request);
    const metadata = normalizeMetadata(request.metadata);
    const body = Uint8Array.from(request.body);
    const fingerprint = JSON.stringify({ object: request.object, body: bytes(body), metadata: metadataJson(metadata), condition: request.condition });
    const replay = this.#idempotency.get(request.idempotencyKey);
    if (replay) {
      if (replay.fingerprint !== fingerprint) throw new ReplaceableObjectError("idempotency_mismatch", "idempotency key was reused with different arguments");
      return replay.receipt;
    }
    const current = this.#values.get(request.object);
    if (request.condition?.kind === "ifAbsent" && current) throw new ReplaceableObjectError("precondition_failed", "object already exists");
    if (request.condition?.kind === "ifMatch" && (!current || current.etag !== request.condition.etag)) {
      throw new ReplaceableObjectError("precondition_failed", "object ETag did not match");
    }
    const etag = await digest(body);
    const receipt = Object.freeze({ object: request.object, etag, size: BigInt(body.byteLength) });
    this.#values.set(request.object, { body, metadata, etag });
    this.#idempotency.set(request.idempotencyKey, { fingerprint, receipt });
    return receipt;
  }

  async getCurrent(object: ObjectId, options: CurrentGetOptions = {}): Promise<CurrentObject> {
    validateObjectId(object);
    const value = this.#values.get(object);
    if (!value) throw new ReplaceableObjectError("not_found", "object was not found");
    if (options.ifMatch !== undefined && options.ifMatch !== value.etag) throw new ReplaceableObjectError("precondition_failed", "object ETag did not match");
    if (options.ifNoneMatch !== undefined && options.ifNoneMatch === value.etag) throw new ReplaceableObjectError("precondition_failed", "object ETag matched if-none-match");
    const range = validateRange(options.range, value.body.byteLength);
    const body = range ? value.body.slice(range.start, range.endExclusive) : value.body.slice();
    if (options.maximumBytes !== undefined && (!Number.isSafeInteger(options.maximumBytes) || options.maximumBytes < 0 || body.byteLength > options.maximumBytes)) {
      throw new ReplaceableObjectError("response_too_large", "object exceeds maximum response size");
    }
    return { object, etag: value.etag, size: BigInt(value.body.byteLength), metadata: cloneMetadata(value.metadata), body,
      ...(range ? { contentRange: { start: range.start, endExclusive: range.endExclusive, total: value.body.byteLength } } : {}) };
  }

  async verifyCurrent(object: ObjectId, expectedEtag?: ETag): Promise<Verification> {
    validateObjectId(object);
    const value = this.#values.get(object);
    if (!value) throw new ReplaceableObjectError("not_found", "object was not found");
    const actual = await digest(value.body);
    return { object, etag: actual, size: BigInt(value.body.byteLength), valid: actual === value.etag && (expectedEtag === undefined || expectedEtag === actual) };
  }
}

export interface ReplaceableObjectsEnvironment { readonly endpoint: string; readonly token: string }

/** Typed current-value facade. It deliberately has no version, snapshot, or delete operations. */
export class ReplaceableObjects {
  static memory(): ReplaceableObjects { return new ReplaceableObjects(new MemoryReplaceableObjectsProvider()); }
  static fromEnv(environment?: Partial<ReplaceableObjectsEnvironment>): ReplaceableObjects {
    const runtime = globalThis as typeof globalThis & { process?: { env?: Readonly<Record<string, string | undefined>> } };
    const endpoint = environment?.endpoint ?? runtime.process?.env?.ACYCLIC_OBJECTS_ENDPOINT;
    const token = environment?.token ?? runtime.process?.env?.ACYCLIC_OBJECTS_TOKEN;
    if (!endpoint?.trim()) throw new TypeError("ACYCLIC_OBJECTS_ENDPOINT is required");
    if (!token?.trim()) throw new TypeError("ACYCLIC_OBJECTS_TOKEN is required");
    return new ReplaceableObjects(new HttpReplaceableObjectsProvider({ endpoint, token }));
  }
  constructor(readonly provider: ReplaceableObjectsProvider) {}
  replacePut(request: ReplacePutRequest): Promise<PutReceipt> { return this.provider.replacePut(request); }
  getCurrent(object: ObjectId, options?: CurrentGetOptions): Promise<CurrentObject> { return this.provider.getCurrent(object, options); }
  verifyCurrent(object: ObjectId, expectedEtag?: ETag): Promise<Verification> { return this.provider.verifyCurrent(object, expectedEtag); }
}

type StoredValue = { readonly body: Uint8Array; readonly metadata: ObjectMetadata; readonly etag: ETag };
type Decoder<Value> = (value: unknown) => Value;
const emptyMetadata = (): ObjectMetadata => ({ contentType: "", contentEncoding: "", cacheControl: "", contentDisposition: "", contentLanguage: "", expiresUnixSeconds: undefined, user: new Map() });
function validateRequest(request: ReplacePutRequest): void { validateObjectId(request.object); if (!request.idempotencyKey.trim() || request.idempotencyKey.length > 256 || /[\u0000-\u001f\u007f]/u.test(request.idempotencyKey)) throw new ReplaceableObjectError("invalid_object_id", "idempotency key is invalid"); }
function validateObjectId(value: ObjectId): void { objectId(value); }
function normalizeMetadata(value: Partial<ObjectMetadata> | undefined): ObjectMetadata { return { ...emptyMetadata(), ...value, user: new Map(value?.user ?? []) }; }
function cloneMetadata(value: ObjectMetadata): ObjectMetadata { return { ...value, user: new Map(value.user) }; }
function metadataJson(value: ObjectMetadata): unknown { return { ...value, expiresUnixSeconds: value.expiresUnixSeconds?.toString(), user: [...value.user] }; }
function validateRange(value: CurrentGetOptions["range"], total: number): { start: number; endExclusive: number } | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value.start) || !Number.isSafeInteger(value.endExclusive) || value.start < 0 || value.endExclusive <= value.start || value.endExclusive > total) throw new ReplaceableObjectError("invalid_range", "range is invalid");
  return value;
}
async function digest(value: Uint8Array): Promise<ETag> { return `\"${hex(blake3(value))}\"` as ETag; }
function encode(value: unknown): string { return JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item instanceof Uint8Array ? { $bytes: bytes(item) } : item instanceof Map ? { $map: [...item] } : item); }
function decode(value: string): unknown { return JSON.parse(value, (_key, item: unknown) => { if (item !== null && typeof item === "object" && "$bigint" in item && typeof item.$bigint === "string") return BigInt(item.$bigint); if (item !== null && typeof item === "object" && "$bytes" in item && typeof item.$bytes === "string") return fromBytes(item.$bytes); if (item !== null && typeof item === "object" && "$map" in item && Array.isArray(item.$map)) return new Map(item.$map as readonly (readonly [unknown, unknown])[]); return item; }); }
function bytes(value: Uint8Array): string { let binary = ""; for (const byte of value) binary += String.fromCharCode(byte); return btoa(binary); }
function hex(value: Uint8Array): string { return Array.from(value, byte => byte.toString(16).padStart(2, "0")).join(""); }
function fromBytes(value: string): Uint8Array { return Uint8Array.from(atob(value), character => character.charCodeAt(0)); }
function record(value: unknown): Record<string, unknown> { if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("expected object"); return value as Record<string, unknown>; }
function text(value: unknown, name: string): string { if (typeof value !== "string" || !value) throw new TypeError(`${name} must be a non-empty string`); return value; }
function bigint(value: unknown, name: string): bigint { if (typeof value !== "bigint") throw new TypeError(`${name} must be a bigint`); return value; }
function metadata(value: unknown): ObjectMetadata { const item = record(value); const user = item.user instanceof Map ? item.user : new Map(); return { contentType: typeof item.contentType === "string" ? item.contentType : "", contentEncoding: typeof item.contentEncoding === "string" ? item.contentEncoding : "", cacheControl: typeof item.cacheControl === "string" ? item.cacheControl : "", contentDisposition: typeof item.contentDisposition === "string" ? item.contentDisposition : "", contentLanguage: typeof item.contentLanguage === "string" ? item.contentLanguage : "", expiresUnixSeconds: item.expiresUnixSeconds === undefined ? undefined : bigint(item.expiresUnixSeconds, "expiresUnixSeconds"), user: new Map(user as Map<string, string>) }; }
function receipt(value: unknown): PutReceipt { const item = record(value); return { object: objectId(text(item.object, "object")), etag: text(item.etag, "etag") as ETag, size: bigint(item.size, "size") }; }
function currentObject(value: unknown): CurrentObject { const item = record(value); const body = item.body instanceof Uint8Array ? item.body : (() => { throw new TypeError("body must be bytes"); })(); return { object: objectId(text(item.object, "object")), etag: text(item.etag, "etag") as ETag, size: bigint(item.size, "size"), metadata: metadata(item.metadata), body, ...(item.contentRange === undefined ? {} : { contentRange: range(item.contentRange) }) }; }
function verification(value: unknown): Verification { const item = record(value); return { object: objectId(text(item.object, "object")), etag: text(item.etag, "etag") as ETag, size: bigint(item.size, "size"), valid: item.valid === true }; }
function range(value: unknown): { start: number; endExclusive: number; total: number } { const item = record(value); if (!Number.isSafeInteger(item.start) || !Number.isSafeInteger(item.endExclusive) || !Number.isSafeInteger(item.total)) throw new TypeError("content range is invalid"); return { start: item.start as number, endExclusive: item.endExclusive as number, total: item.total as number }; }
async function boundedBytes(response: Response, maximum: number): Promise<Uint8Array> { const reader = response.body?.getReader(); if (!reader) return new Uint8Array(); const chunks: Uint8Array[] = []; let total = 0; try { for (;;) { const { done, value } = await reader.read(); if (done) break; total += value.byteLength; if (total > maximum) { await reader.cancel().catch(() => undefined); throw new ReplaceableTransportError("response exceeds configured bound", response.status, false); } chunks.push(value); } } finally { reader.releaseLock(); } const result = new Uint8Array(total); let offset = 0; for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.byteLength; } return result; }
