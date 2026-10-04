import { validateAppend } from "./client.js";
import { consumeHttpResponseBytes, nextHttpFollowCursor, publicHttpErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import type { AccessToken, AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommittedEnvelope, CommitId, CommitOptions, CommitResult, CreateTokenRequest, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider } from "./types.js";
import { StreamError } from "./types.js";
import { decodeHttpResponseFor } from "./http-contract.js";
import type { HttpResponseFor, HttpRoute } from "./http-contract.js";
import { encodeHttpRequest, ensureStreamWasm, normalizeWireCommitBytes, validateHttpEndpointValue, validateWireRequest, wireAppendRequest, wireCreateTokenRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { STREAM_HANDSHAKE, negotiateRustOwnedEndpoint, validateRustOwnedCredentialPolicy } from "./generated-client.js";

export interface HttpStreamProviderOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Authenticated JSON transport. Mutation retries are deliberately the caller's decision. */
export class HttpStreamProvider implements StreamProvider {
  readonly #endpoint: string;
  readonly #token: string;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;
  #handshake: Promise<void> | undefined;
  constructor(options: HttpStreamProviderOptions) {
    validateHttpEndpointValue(options.endpoint);
    const endpoint = new URL(options.endpoint);
    validateRustOwnedCredentialPolicy(options.token);
    this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`;
    this.#token = options.token;
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }
  async inspectIdempotency(key: IdempotencyKey, signal?: AbortSignal): Promise<IdempotencyObservation | undefined> { const input = wireInspectIdempotencyRequest(key); return this.#request("idempotency/inspect", await encodeHttpRequest("idempotency/inspect", input), signal); }
  async tail(path: string, signal?: AbortSignal): Promise<Sequence> { return this.#tail(path, signal); }
  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions, signal?: AbortSignal): Promise<AppendResult> { const records = values.map(value => value.slice()); const authored = options === undefined ? undefined : structuredClone(options); await validateAppend(path, records, authored); const input = wireAppendRequest(path, records, authored); return this.#request("append", await encodeHttpRequest("append", input), signal); }
  async fork(source: string, destination: string, options?: ForkOptions, signal?: AbortSignal): Promise<ForkReceipt> { const authored = options === undefined ? undefined : structuredClone(options); await validateWireRequest({ kind: "fork", source, destination, ...(authored === undefined ? {} : { options: authored }) }); const input = wireRequest({ kind: "fork", source, destination, ...(authored === undefined ? {} : { options: authored }) }); return this.#request("fork", await encodeHttpRequest("fork", input), signal); }
  async *read(path: string, options: ReadOptions, signal?: AbortSignal): AsyncIterable<EncodedRecord> {
    const page = await this.#read(path, options, signal);
    for (const item of page.records) yield item;
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const { from, signal } = options;
    await validateWireRequest({ kind: "follow", path, from });
    if (signal?.aborted) return;
    const tail = await this.#tail(path, signal);
    if (from > tail) throw new StreamError("out_of_range", "follow cursor is beyond the stream tail");
    let next = from;
    while (!signal?.aborted) {
      const page = await this.#read(path, { from: next, limit: 256 }, signal, false);
      for (const item of page.records) yield item;
      next = page.next;
      if (!page.records.length) await delay(250, signal);
    }
  }
  async *children(parent: string | undefined, limit: number, signal?: AbortSignal): AsyncIterable<{ readonly path: string }> { await validateWireRequest({ kind: "children", limit, ...(parent === undefined ? {} : { parent }) }); const input = wireRequest({ kind: "children", limit, ...(parent === undefined ? {} : { parent }) }); for (const item of await this.#request("children", await encodeHttpRequest("children", input), signal)) yield item; }
  async childrenPage(request: ChildrenPageRequest, signal?: AbortSignal): Promise<ChildrenPage> {
    if (request === null || typeof request !== "object") throw new StreamError("invalid_argument", "children page request must be an object");
    const authored = {
      kind: "children_page",
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    } as const;
    await validateWireRequest(authored);
    const input = wireRequest(authored);
    return this.#request("children/page", await encodeHttpRequest("children/page", input), signal);
  }
  async commit(request: ProviderCommitRequest, options: CommitOptions, signal?: AbortSignal): Promise<CommitResult> { const authored = structuredClone(request); const retained = structuredClone(options); const input = await normalizeWireCommitBytes(authored, retained); return this.#request("commit", await encodeHttpRequest("commit", input), signal); }
  async readCommit(value: CommitId, signal?: AbortSignal): Promise<CommittedEnvelope> { const input = wireReadCommitRequest(value); return this.#request("commits/read", await encodeHttpRequest("commits/read", input), signal); }
  async createToken(request: CreateTokenRequest, signal?: AbortSignal): Promise<AccessToken> {
    const input = wireCreateTokenRequest(request);
    return this.#request("tokens/create", await encodeHttpRequest("tokens/create", input), signal);
  }
  async #read(path: string, options: ReadOptions, signal?: AbortSignal, checkEmptyCursor = true): Promise<{ readonly records: readonly EncodedRecord[]; readonly next: bigint }> {
    const { from, limit } = options;
    await validateWireRequest({ kind: "read", path, from, limit });
    // Stream tails are monotonic. Validate before reading so a concurrent
    // append cannot turn an invalid empty-read cursor into a valid one.
    if (checkEmptyCursor && from > await this.#tail(path, signal)) throw new StreamError("out_of_range", "read cursor is beyond the stream tail");
    const input = wireRequest({ kind: "read", path, from, limit });
    let next = from;
    const records = await this.#request("read", await encodeHttpRequest("read", input), signal, from, cursor => { next = cursor; });
    return { records, next };
  }
  async #tail(path: string, signal?: AbortSignal): Promise<Sequence> {
    await validateWireRequest({ kind: "tail", path });
    const input = wireRequest({ kind: "tail", path });
    return this.#request("tail", await encodeHttpRequest("tail", input), signal);
  }
  async #request<Route extends HttpRoute>(route: Route, body: unknown, signal?: AbortSignal, readFrom?: bigint, onCursor?: (next: bigint) => void): Promise<HttpResponseFor<Route>> {
    const headers = { authorization: `Bearer ${this.#token}`, "content-type": "application/json" };
    await this.#ensureHandshake(headers, signal);
    const response = await this.#fetcher(new URL(`v1/stream/${route}`, this.#endpoint), { method: "POST", headers, body: typeof body === "string" ? body : JSON.stringify(body), ...(signal === undefined ? {} : { signal }) });
    let text: string;
    try { text = await boundedText(response, this.#maximum); }
    catch (error) { if (error instanceof StreamError) throw error; throw new StreamError("invalid_response", `invalid ${route} response encoding: ${error instanceof Error ? error.message : String(error)}`, response.status); }
    if (!response.ok) throw await hostedError(route, text, response.status);
    try {
      await ensureStreamWasm();
       if (route === "read" && readFrom !== undefined) onCursor?.(nextHttpFollowCursor(text, readFrom));
      return decodeHttpResponseFor(route, text);
    } catch (error) { throw new StreamError("invalid_response", `invalid ${route} response: ${error instanceof Error ? error.message : String(error)}`, response.status); }
  }
  async #ensureHandshake(headers: HeadersInit, signal?: AbortSignal): Promise<void> {
    if (this.#handshake !== undefined) return this.#handshake;
    const pending = negotiateRustOwnedEndpoint(this.#fetcher, this.#endpoint, headers, STREAM_HANDSHAKE, this.#maximum, signal)
      .catch(error => { this.#handshake = undefined; throw error; });
    this.#handshake = pending;
    return pending;
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
      try { total = Number(consumeHttpResponseBytes(BigInt(total), BigInt(value.byteLength), BigInt(maximum))); }
      catch { await reader.cancel().catch(() => undefined); throw new StreamError("response_too_large", "response exceeds configured bound", response.status); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return decoder.decode(bytes);
}
async function delay(milliseconds: number, signal?: AbortSignal): Promise<void> { if (signal?.aborted) return; await new Promise<void>(resolve => { const finish = () => { clearTimeout(timeout); signal?.removeEventListener("abort", finish); resolve(); }; const timeout = setTimeout(finish, milliseconds); signal?.addEventListener("abort", finish, { once: true }); }); }
