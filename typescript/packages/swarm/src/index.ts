import type {
  ArtifactFile, CreateSwarmRequest, CreateSwarmResponse, CreateTokenRequest,
  CreateTokenResponse, Json, PullRequestInput, SwarmArtifact, SwarmDetail,
  SwarmEvent, SwarmPullRequest, SwarmSummary, TokenInfo, TraceResponse,
  UploadResponse, Whoami,
} from "./protocol.js";

export type * from "./protocol.js";

export interface SwarmHttpOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly org?: string;
  readonly user?: string;
  readonly fetch?: typeof globalThis.fetch;
  readonly maximumResponseBytes?: number;
}
export interface CallOptions { readonly signal?: AbortSignal; readonly traceparent?: string }
export interface MutationOptions extends CallOptions { readonly idempotencyKey?: string }
export interface EventOptions extends CallOptions { readonly lastEventId?: string }
export interface ServerEvent<T> { readonly id: string; readonly event: string; readonly data: T }

export class SwarmHttpError extends Error {
  constructor(readonly status: number, readonly body: Json, readonly traceId: string | null) {
    super(typeof body === "object" && body !== null && !Array.isArray(body) && typeof body.message === "string" ? body.message : `Swarm HTTP ${status}`);
    this.name = "SwarmHttpError";
  }
}

/** Public Cloud routes; each mutation is sent once, with the caller's original identity. */
export class SwarmClient {
  private readonly endpoint: URL;
  private readonly send: typeof globalThis.fetch;
  private readonly maximum: number;

  constructor(private readonly options: SwarmHttpOptions) {
    this.endpoint = new URL(options.endpoint);
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(this.endpoint.hostname);
    if ((this.endpoint.protocol !== "https:" && !(local && this.endpoint.protocol === "http:")) || this.endpoint.username || this.endpoint.password || this.endpoint.search || this.endpoint.hash || this.endpoint.pathname !== "/") throw new TypeError("Swarm endpoint must be an HTTPS origin or HTTP loopback origin");
    if (!options.token.trim() || new TextEncoder().encode(options.token).byteLength > 12 * 1024 || /[\r\n\0]/.test(options.token)) throw new TypeError("invalid bearer token");
    this.send = options.fetch ?? globalThis.fetch;
    this.maximum = options.maximumResponseBytes ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(this.maximum) || this.maximum < 1) throw new RangeError("maximumResponseBytes must be a positive safe integer");
  }

  health(options?: CallOptions) { return this.json<{ ok: boolean; version: string }>("/health", "GET", undefined, options); }
  healthEvents(options?: CallOptions) { return this.events<{ seq: number; ok: boolean }>("/health/sse", options); }
  create(request: CreateSwarmRequest, options?: MutationOptions) { return this.json<CreateSwarmResponse>("/v1/swarms", "POST", request, options); }
  list(options?: CallOptions) { return this.json<{ swarms: SwarmSummary[] }>("/v1/swarms", "GET", undefined, options); }
  detail(id: string, options?: CallOptions) { return this.json<SwarmDetail>(this.swarm(id), "GET", undefined, options); }
  trace(id: string, options?: CallOptions) { return this.json<TraceResponse>(`${this.swarm(id)}/trace`, "GET", undefined, options); }
  artifact(id: string, options?: CallOptions) { return this.json<SwarmArtifact>(`${this.swarm(id)}/artifact`, "GET", undefined, options); }
  artifactFiles(id: string, options?: CallOptions) { return this.json<{ files: ArtifactFile[] }>(`${this.swarm(id)}/artifact/files`, "GET", undefined, options); }
  artifactTree(id: string, options?: CallOptions) { return this.request(`${this.swarm(id)}/artifact/tree`, "GET", undefined, options); }
  async artifactPatch(id: string, options?: CallOptions) { return this.text(await this.request(`${this.swarm(id)}/artifact/patch`, "GET", undefined, options)); }
  memory(id: string, options?: CallOptions) { return this.json<Json>(`${this.swarm(id)}/memory`, "GET", undefined, options); }
  cancel(id: string, options?: CallOptions) { return this.json<{ swarm_id: string; status: "canceling" }>(`${this.swarm(id)}/cancel`, "POST", undefined, options); }
  openPullRequest(id: string, request: PullRequestInput, options?: CallOptions) { return this.json<SwarmPullRequest>(`${this.swarm(id)}/pull-request`, "POST", request, options); }
  watch(id: string, options?: EventOptions) { return this.events<SwarmEvent>(`${this.swarm(id)}/events`, options); }
  createToken(request: CreateTokenRequest, options?: CallOptions) { return this.json<CreateTokenResponse>("/v1/tokens", "POST", request, options); }
  listTokens(org?: string, options?: CallOptions) { return this.json<{ tokens: TokenInfo[] }>(`/v1/tokens${org === undefined ? "" : `?${new URLSearchParams({ org })}`}`, "GET", undefined, options); }
  async revokeToken(id: string, options?: CallOptions) {
    const response = await this.request(`/v1/tokens/${segment(id)}`, "DELETE", undefined, options);
    await response.body?.cancel();
  }
  whoami(options?: CallOptions) { return this.json<Whoami>("/v1/whoami", "GET", undefined, options); }
  async metrics(options?: CallOptions) { return this.text(await this.request("/v1/metrics", "GET", undefined, options)); }
  async upload(archive: Blob | Uint8Array, options: CallOptions & { readonly idempotencyKey: string }) {
    const body = archive instanceof Uint8Array ? new Uint8Array(archive) : archive;
    const response = await this.request("/v1/swarms/uploads", "POST", body, options, "application/gzip");
    return JSON.parse(await this.text(response)) as UploadResponse;
  }

  private swarm(id: string) { return `/v1/swarms/${segment(id)}`; }

  private async json<T>(path: string, method: string, body: unknown, options?: MutationOptions): Promise<T> {
    return JSON.parse(await this.text(await this.request(path, method, body === undefined ? undefined : JSON.stringify(body), options, "application/json"))) as T;
  }

  private async request(path: string, method: string, body?: BodyInit, options?: MutationOptions, contentType?: string, accept?: string, lastEventId?: string): Promise<Response> {
    const headers = new Headers({ authorization: `Bearer ${this.options.token}` });
    if (this.options.org !== undefined) headers.set("x-acyclic-org", this.options.org);
    if (this.options.user !== undefined) headers.set("x-acyclic-user", this.options.user);
    if (options?.traceparent !== undefined) headers.set("traceparent", options.traceparent);
    if (options?.idempotencyKey !== undefined) headers.set("idempotency-key", options.idempotencyKey);
    if (body !== undefined && contentType !== undefined) headers.set("content-type", contentType);
    if (accept !== undefined) headers.set("accept", accept);
    if (lastEventId !== undefined) headers.set("last-event-id", lastEventId);
    const response = await this.send(new URL(path, this.endpoint), {
      method, headers, redirect: "error",
      ...(body === undefined ? {} : { body }),
      ...(options?.signal === undefined ? {} : { signal: options.signal }),
    });
    if (!response.ok) {
      const text = await this.text(response);
      let error: Json;
      try { error = JSON.parse(text) as Json; } catch { error = text; }
      throw new SwarmHttpError(response.status, error, response.headers.get("x-trace-id"));
    }
    return response;
  }

  private async text(response: Response): Promise<string> {
    if (!response.body) return "";
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8", { fatal: true });
    let size = 0;
    let result = "";
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) return result + decoder.decode();
        size += value.byteLength;
        if (size > this.maximum) throw new RangeError("Swarm response exceeds maximumResponseBytes");
        result += decoder.decode(value, { stream: true });
      }
    } finally { await reader.cancel(); reader.releaseLock(); }
  }

  private async *events<T>(path: string, options?: EventOptions): AsyncGenerator<ServerEvent<T>> {
    const controller = new AbortController();
    const abort = () => controller.abort(options?.signal?.reason);
    options?.signal?.addEventListener("abort", abort, { once: true });
    if (options?.signal?.aborted) abort();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      const requestOptions = { ...options, signal: controller.signal };
      // Last-Event-ID is a decimal u64 string; never round it through a JS number.
      const response = await this.request(path, "GET", undefined, requestOptions, undefined, "text/event-stream", options?.lastEventId);
      if (response.headers.get("content-type")?.split(";")[0]?.trim() !== "text/event-stream") throw new TypeError("Swarm event response is not text/event-stream");
      if (!response.body) throw new TypeError("Swarm event response has no body");
      reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8", { fatal: true });
      let fragments: string[] = [];
      let id = "";
      let event = "message";
      let data: string[] = [];
      let size = 0;
      while (true) {
        const chunk = await reader.read();
        // An incomplete event after disconnect is replayed by the next subscription.
        if (chunk.done) return;
        let offset = 0;
        while (offset < chunk.value.byteLength) {
          const newline = chunk.value.indexOf(10, offset);
          const end = newline < 0 ? chunk.value.byteLength : newline + 1;
          const part = chunk.value.subarray(offset, end);
          size += part.byteLength;
          if (size > this.maximum) throw new RangeError("Swarm event exceeds maximumResponseBytes");
          fragments.push(decoder.decode(part, { stream: true }));
          offset = end;
          if (newline < 0) break;
          const line = fragments.join("").replace(/\r?\n$/, "");
          fragments = [];
          if (line === "") {
            if (data.length) {
              controller.signal.throwIfAborted();
              yield { id, event, data: JSON.parse(data.join("\n")) as T };
            }
            data = []; event = "message"; size = 0;
          } else {
            const colon = line.indexOf(":");
            const field = colon < 0 ? line : line.slice(0, colon);
            const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
            if (field === "data") data.push(value);
            if (field === "event") event = value;
            if (field === "id" && !value.includes("\0")) id = value;
          }
        }
      }
    } finally {
      controller.abort();
      options?.signal?.removeEventListener("abort", abort);
      if (reader) { await reader.cancel().catch(() => {}); reader.releaseLock(); }
    }
  }
}

function segment(value: string): string {
  if (!value || value === "." || value === "..") throw new TypeError("route identity must be non-empty and not a dot segment");
  return encodeURIComponent(value);
}
