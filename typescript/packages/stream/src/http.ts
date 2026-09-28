import { pathValue, validateAppend } from "./client.js";
import { StreamLimit } from "../generated/proto/stream/v2/stream_pb.js";
import { publicHttpErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import type { AccessToken, AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommittedEnvelope, CommitId, CommitOptions, CommitResult, CreateTokenRequest, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider } from "./types.js";
import { StreamError } from "./types.js";
import { decodeHttpResponseFor } from "./http-contract.js";
import type { HttpResponseFor, HttpRoute } from "./http-contract.js";
import { encodeHttpRequest, ensureStreamWasm, normalizeWireCommitBytes, validateWireRequest, wireAppendRequest, wireCreateTokenRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";

export interface HttpStreamProviderOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Authenticated JSON transport. Mutation retries are deliberately the caller's decision. */
export class HttpStreamProvider implements StreamProvider {
  readonly #endpoint: string;
  readonly #token: string;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;
  constructor(options: HttpStreamProviderOptions) {
    const endpoint = new URL(options.endpoint);
    if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment");
    if (!options.token.trim()) throw new TypeError("token is required");
    this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? fetch;
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }
  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> { const input = wireInspectIdempotencyRequest(key); return this.#request("idempotency/inspect", await encodeHttpRequest("idempotency/inspect", input)); }
  async tail(path: string): Promise<Sequence> { return this.#tail(path); }
  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> { const records = values.map(value => value.slice()); const authored = options === undefined ? undefined : structuredClone(options); await validateAppend(path, records, authored); const input = wireAppendRequest(path, records, authored); return this.#request("append", await encodeHttpRequest("append", input)); }
  async fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> { const authored = options === undefined ? undefined : structuredClone(options); await validateWireRequest({ kind: "fork", source, destination, ...(authored === undefined ? {} : { options: authored }) }); const input = wireRequest({ kind: "fork", source, destination, ...(authored === undefined ? {} : { options: authored }) }); return this.#request("fork", await encodeHttpRequest("fork", input)); }
  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> { for (const item of await this.#read(path, options)) yield item; }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const { from, signal } = options;
    await validateWireRequest({ kind: "follow", path, from });
    if (signal?.aborted) return;
    const tail = await this.#tail(path, signal);
    if (from > tail) throw new StreamError("out_of_range", "follow cursor is beyond the stream tail");
    let next = from;
    while (!signal?.aborted) {
      const records: EncodedRecord[] = [];
      for (const item of await this.#read(path, { from: next, limit: 256 }, signal, false)) records.push(item);
      for (const item of records) { yield item; next = item.sequence + 1n; }
      if (!records.length) await delay(250, signal);
    }
  }
  async *children(parent: string | undefined, limit: number): AsyncIterable<{ readonly path: string }> { await validateWireRequest({ kind: "children", limit, ...(parent === undefined ? {} : { parent }) }); const input = wireRequest({ kind: "children", limit, ...(parent === undefined ? {} : { parent }) }); for (const item of await this.#request("children", await encodeHttpRequest("children", input))) yield item; }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    if (request === null || typeof request !== "object") throw new StreamError("invalid_argument", "children page request must be an object");
    if (request.parent !== undefined) pathValue(request.parent);
    if (request.after !== undefined) {
      pathValue(request.after);
      if (request.hierarchyVersion === undefined || directParent(request.after) !== (request.parent ?? "")) {
        throw new StreamError("invalid_cursor", "child continuation must name a direct child and its hierarchy revision");
      }
    }
    if (request.hierarchyVersion !== undefined && request.hierarchyVersion.byteLength !== 32) {
      throw new StreamError("invalid_cursor", "hierarchy version must be a commit identity");
    }
    if (!Number.isSafeInteger(request.limit) || request.limit < 1 || request.limit > StreamLimit.MAX_ITEMS) {
      throw new StreamError("limit_exceeded", `child page limit must be between 1 and ${StreamLimit.MAX_ITEMS}`);
    }
    await validateWireRequest({
      kind: "children_page",
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    });
    const input = wireRequest({
      kind: "children_page",
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    });
    return this.#request("children/page", await encodeHttpRequest("children/page", input));
  }
  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> { const authored = structuredClone(request); const retained = structuredClone(options); const input = await normalizeWireCommitBytes(authored, retained); return this.#request("commit", await encodeHttpRequest("commit", input)); }
  async readCommit(value: CommitId): Promise<CommittedEnvelope> { const input = wireReadCommitRequest(value); return this.#request("commits/read", await encodeHttpRequest("commits/read", input)); }
  async createToken(request: CreateTokenRequest): Promise<AccessToken> {
    const input = wireCreateTokenRequest(request);
    return this.#request("tokens/create", await encodeHttpRequest("tokens/create", input));
  }
  async #read(path: string, options: ReadOptions, signal?: AbortSignal, checkEmptyCursor = true): Promise<readonly EncodedRecord[]> {
    const { from, limit } = options;
    await validateWireRequest({ kind: "read", path, from, limit });
    const input = wireRequest({ kind: "read", path, from, limit });
    const records = await this.#request("read", await encodeHttpRequest("read", input), signal);
    // A canonical read is a contiguous page beginning at the requested
    // cursor.  Checking this at the transport boundary prevents a malformed
    // hosted response from making follow skip records or move its cursor
    // backwards.
    let expected = from;
    for (const record of records) {
      if (record.sequence !== expected) {
        throw new StreamError("invalid_response", "read response contains a non-contiguous cursor");
      }
      expected += 1n;
    }
    // The Rust provider distinguishes an empty page at the current tail from
    // a cursor beyond the tail.  A hosted response with no records carries no
    // tail, so ask the canonical tail route before accepting an empty page.
    // This keeps direct reads and the polling follow adapter aligned with the
    // in-memory provider even when a hosted implementation returns [] for
    // both cases.
    if (checkEmptyCursor && records.length === 0) {
      const tail = await this.#tail(path, signal);
      if (from > tail) throw new StreamError("out_of_range", "read cursor is beyond the stream tail");
    }
    return records;
  }
  async #tail(path: string, signal?: AbortSignal): Promise<Sequence> {
    await validateWireRequest({ kind: "tail", path });
    const input = wireRequest({ kind: "tail", path });
    return this.#request("tail", await encodeHttpRequest("tail", input), signal);
  }
  async #request<Route extends HttpRoute>(route: Route, body: unknown, signal?: AbortSignal): Promise<HttpResponseFor<Route>> {
    const response = await this.#fetcher(new URL(`v1/stream/${route}`, this.#endpoint), { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: typeof body === "string" ? body : JSON.stringify(body), ...(signal === undefined ? {} : { signal }) });
    let text: string;
    try { text = await boundedText(response, this.#maximum); }
    catch (error) { if (error instanceof StreamError) throw error; throw new StreamError("invalid_response", `invalid ${route} response encoding: ${error instanceof Error ? error.message : String(error)}`, response.status); }
    if (!response.ok) throw await hostedError(route, text, response.status);
    try { await ensureStreamWasm(); return decodeHttpResponseFor(route, text); } catch (error) { throw new StreamError("invalid_response", `invalid ${route} response: ${error instanceof Error ? error.message : String(error)}`, response.status); }
  }
}

/**
 * Decode the small error envelope shared by hosted Stream endpoints. Rust
 * owns the error-code set and its public projection. Unknown or malformed
 * bodies stay transport failures so arbitrary server strings cannot become
 * typed domain errors.
 */
async function hostedError(route: string, text: string, status: number): Promise<StreamError> {
  const fallback = text || `HTTP ${status}`;
  const details = parseHostedError(text);
  if (details.code === undefined) return new StreamError("transport", fallback, status);
  try {
    await ensureStreamWasm();
    const code = publicHttpErrorCode(details.code, route);
    if (code !== undefined) return new StreamError(code, details.message ?? fallback, status);
  } catch { /* Unknown or unavailable contract remains a transport error. */ }
  return new StreamError("transport", fallback, status);
}

function parseHostedError(text: string): { readonly code?: string; readonly message?: string } {
  try {
    const value: unknown = JSON.parse(text);
    if (typeof value === "string") return { code: value };
    if (value === null || typeof value !== "object" || Array.isArray(value)) return {};
    const item = value as Record<string, unknown>;
    const nested = item.error;
    const error = nested !== null && typeof nested === "object" && !Array.isArray(nested)
      ? nested as Record<string, unknown>
      : undefined;
    const code = item.code ?? error?.code;
    const message = item.message ?? error?.message;
    return {
      ...(typeof code === "string" && code.length > 0 ? { code } : {}),
      ...(typeof message === "string" && message.length > 0 ? { message } : {}),
    };
  } catch {
    return {};
  }
}

const decoder = new TextDecoder("utf-8", { fatal: true });
async function boundedText(response: Response, maximum: number): Promise<string> {
  const reader = response.body?.getReader();
  if (reader === undefined) return "";
  const chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maximum) { await reader.cancel().catch(() => undefined); throw new StreamError("response_too_large", "response exceeds configured bound", response.status); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return decoder.decode(bytes);
}
async function delay(milliseconds: number, signal?: AbortSignal): Promise<void> { if (signal?.aborted) return; await new Promise<void>(resolve => { const finish = () => { clearTimeout(timeout); signal?.removeEventListener("abort", finish); resolve(); }; const timeout = setTimeout(finish, milliseconds); signal?.addEventListener("abort", finish, { once: true }); }); }
function directParent(path: string): string { const at = path.lastIndexOf("/"); return at < 0 ? "" : path.slice(0, at); }
