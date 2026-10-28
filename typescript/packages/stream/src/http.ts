import { checkedReadSequence } from "./record.js";
import type { SessionFollow, SessionData } from "../generated/proto/stream/v1/session_pb.js";
import { StreamSessions } from "./session.js";
import { webSocketSession, type SessionWebSocketFactory } from "./session-websocket.js";
import { credentialProvider, type CredentialOptions, type CredentialProvider } from "@acyclic-labs/actors/credential";
import { validateAppend } from "./client.js";
import { StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import { is_stream_error_code, publicHttpErrorCode } from "../generated/wasm/acyclic_stream_wasm.js";
import type { AccessToken, AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommittedEnvelope, CommitId, CommitOptions, CommitResult, CreateTokenRequest, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider } from "./types.js";
import { StreamError, type StreamFailureCode } from "./types.js";
import { decodeHttpResponseFor } from "./http-contract.js";
import type { HttpResponseFor, HttpRoute } from "./http-contract.js";
import { observed, resolveObserver, type AcyclicObserver, type OperationSizes } from "@acyclic-labs/actors/observe";
import { encodeHttpRequest, ensureStreamWasm, normalizeWireCommitBytes, validatePathValue, validateWireRequest, wireAppendRequest, wireCreateTokenRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";

export interface HttpStreamProviderOptions extends CredentialOptions { readonly endpoint: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number; readonly observer?: AcyclicObserver; readonly webSocketFactory?: SessionWebSocketFactory }

/** Authenticated JSON transport. Mutation retries are deliberately the caller's decision. */
export class HttpStreamProvider implements StreamProvider {
  readonly #sessions: StreamSessions;
  readonly #endpoint: string;
  readonly #credential: CredentialProvider;
  readonly #fetcher: typeof fetch;
  readonly #maximum: number;
  readonly #observer: AcyclicObserver | undefined;
  constructor(options: HttpStreamProviderOptions) {
    const endpoint = new URL(options.endpoint);
    if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment");
    this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`;
    this.#credential = credentialProvider(options);
    this.#sessions = new StreamSessions(webSocketSession(this.#endpoint, this.#credential, options.webSocketFactory));
    this.#fetcher = options.fetcher ?? globalThis.fetch.bind(globalThis);
    this.#observer = resolveObserver(options.observer);
    this.#maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#maximum) || this.#maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }
  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const input = wireInspectIdempotencyRequest(key);
    const observation: IdempotencyObservation | undefined = await this.#request("idempotency/inspect", await encodeHttpRequest("idempotency/inspect", input));
    // A recovered write floors later reads exactly like its lost reply would have.
    this.#sessions.observe(observation?.revision);
    return observation;
  }
  async tail(path: string): Promise<Sequence> { return this.#tail(path); }
  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> { const records = values.map(value => value.slice()); const authored = options === undefined ? undefined : structuredClone(options); await validateAppend(path, records, authored); const input = wireAppendRequest(path, records, authored); const result = await this.#request("append", await encodeHttpRequest("append", input)); if (result.ok) this.#sessions.observe(result.revision); return result; }
  async fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> { const authored = options === undefined ? undefined : structuredClone(options); await validateWireRequest({ kind: "fork", source, destination, ...(authored === undefined ? {} : { options: authored }) }); const input = wireRequest({ kind: "fork", source, destination, ...(authored === undefined ? {} : { options: authored }) }); const receipt = await this.#request("fork", await encodeHttpRequest("fork", input)); this.#sessions.observe(receipt.revision); return receipt; }
  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> { for (const item of await this.#read(path, options)) yield item; }
  async registerCredential(slot: number, credential: Uint8Array): Promise<void> {
    await this.#sessions.registerCredential(slot, credential);
  }
  async *followData(follow: SessionFollow, options: FollowOptions): AsyncIterable<SessionData> {
    const authored = structuredClone(follow);
    yield* this.#sessions.followData(authored, options);
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const authored = { ...options, ...(options.filter === undefined ? {} : { filter: structuredClone(options.filter) }) };
    await validateWireRequest({ kind: "follow", path, from: authored.from });
    yield* this.#sessions.follow(path, authored);
  }


  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    if (request === null || typeof request !== "object") throw new StreamError("invalid_argument", "children page request must be an object");
    if (request.parent !== undefined) validatePathValue(request.parent);
    if (request.after !== undefined) {
      validatePathValue(request.after);
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
      ...(request.tailStream === undefined ? {} : { tailStream: request.tailStream }),
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    });
    const input = wireRequest({
      kind: "children_page",
      ...(request.tailStream === undefined ? {} : { tailStream: request.tailStream }),
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
      limit: request.limit,
    });
    return this.#request("children/page", await encodeHttpRequest("children/page", input));
  }
  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> { const authored = structuredClone(request); const retained = structuredClone(options); const input = await normalizeWireCommitBytes(authored, retained); const result = await this.#request("commit", await encodeHttpRequest("commit", input)); if (result.ok) this.#sessions.observe(result.revision); return result; }
  async readCommit(value: CommitId): Promise<CommittedEnvelope> { const input = wireReadCommitRequest(value); return this.#request("commits/read", await encodeHttpRequest("commits/read", input)); }
  async createToken(request: CreateTokenRequest): Promise<AccessToken> {
    const input = wireCreateTokenRequest(request);
    return this.#request("tokens/create", await encodeHttpRequest("tokens/create", input));
  }
  async #read(path: string, options: ReadOptions, signal?: AbortSignal): Promise<readonly EncodedRecord[]> {
    const { from, limit, key } = structuredClone(options);
    await validateWireRequest({ kind: "read", path, from, limit, ...(key === undefined ? {} : { key }) });
    const atLeast = this.#sessions.revisionFloor();
    const input = wireRequest({ kind: "read", path, from, limit, atLeast, ...(key === undefined ? {} : { key }) });
    const { records, applied } = await this.#request("read", await encodeHttpRequest("read", input), signal);
    // The page's own served revision must prove the minimum sent, and then
    // floors later reads: a lagging replica cannot serve older data.
    this.#sessions.served(atLeast, applied);
    // A canonical read is a contiguous page beginning at the requested
    // cursor.  Checking this at the transport boundary prevents a malformed
    // hosted response from making follow skip records or move its cursor
    // backwards.
    let expected = from;
    for (const record of records) {
      expected = checkedReadSequence(record, expected, key) + 1n;
    }
    return records;
  }
  async heads(options: import("./types.js").HeadsRequest): Promise<import("./types.js").Heads> {
    return this.#sessions.heads(options);
  }

  async head(path: string): Promise<import("./types.js").PathHead> {
    const request = { kind: "head" as const, path };
    await validateWireRequest(request);
    return this.#request("head", await encodeHttpRequest("head", wireRequest(request)));
  }
  async #tail(path: string, signal?: AbortSignal): Promise<Sequence> {
    await validateWireRequest({ kind: "tail", path });
    const atLeast = this.#sessions.revisionFloor();
    const input = wireRequest({ kind: "tail", path, atLeast });
    const { tail, applied } = await this.#request("tail", await encodeHttpRequest("tail", input), signal);
    this.#sessions.served(atLeast, applied);
    return tail;
  }
  #request<Route extends HttpRoute>(route: Route, body: unknown, signal?: AbortSignal): Promise<HttpResponseFor<Route>> {
    return observed(this.#observer, "stream", route, sizes => this.#send(route, body, signal, sizes));
  }
  async #send<Route extends HttpRoute>(route: Route, body: unknown, signal?: AbortSignal, sizes?: OperationSizes): Promise<HttpResponseFor<Route>> {
    const payload = typeof body === "string" ? body : JSON.stringify(body);
    if (sizes) sizes.requestBytes = new TextEncoder().encode(payload).byteLength;
    const response = await this.#fetcher(new URL(`v1/stream/${route}`, this.#endpoint), { method: "POST", redirect: "error", headers: { authorization: `Bearer ${await this.#credential(signal)}`, "content-type": "application/json" }, body: payload, ...(signal === undefined ? {} : { signal }) });
    let text: string;
    try { text = await boundedText(response, this.#maximum, sizes); }
    catch (error) { if (error instanceof StreamError) throw error; throw new StreamError("invalid_response", `invalid ${route} response encoding: ${error instanceof Error ? error.message : String(error)}`, response.status); }
    if (!response.ok) {
      const error = await hostedError(route, text, response.status);
      this.#sessions.refused(error);
      throw error;
    }
    try { await ensureStreamWasm(); return decodeHttpResponseFor(route, text); } catch (error) { throw new StreamError("invalid_response", `invalid ${route} response: ${error instanceof Error ? error.message : String(error)}`, response.status); }
  }
}

/** Unknown peer error codes remain transport failures. */
async function hostedError(route: string, text: string, status: number): Promise<StreamError> {
  const fallback = text || `HTTP ${status}`;
  const details = parseHostedError(text);
  if (details.code === undefined) return new StreamError("transport", fallback, status);
  try {
    await ensureStreamWasm();
    const code = publicHttpErrorCode(details.code, route);
    if (code !== undefined && isPublicHttpCode(code)) return new StreamError(code, details.message ?? fallback, status);
  } catch { /* Unknown or unavailable contract remains a transport error. */ }
  return new StreamError("transport", fallback, status);
}

const HTTP_ALIASES = ["stream_not_found", "commit_not_found", "destination_exists", "capacity_exhausted"] as const satisfies readonly StreamFailureCode[];
function isPublicHttpCode(code: string): code is StreamFailureCode {
  return is_stream_error_code(code) || (HTTP_ALIASES as readonly string[]).includes(code);
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
async function boundedText(response: Response, maximum: number, sizes?: OperationSizes): Promise<string> {
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
  if (sizes) sizes.responseBytes = total;
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return decoder.decode(bytes);
}
function directParent(path: string): string { const at = path.lastIndexOf("/"); return at < 0 ? "" : path.slice(0, at); }

/** Largest bearer credential, in UTF-8 bytes, that an SDK client accepts; matches the Acyclic platform's maximum bearer (12 KiB). */
export const MAX_BEARER_TOKEN_BYTES = 12 * 1024;
