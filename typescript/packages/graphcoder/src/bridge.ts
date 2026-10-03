import {
  GraphCoderError,
  checkedPublicText,
  MAX_MESSAGE_BODY_BYTES,
  MAX_OPERATION_ID_BYTES,
  MAX_PROMPT_BYTES,
  agentId,
  approvalId,
  messageId,
  sessionId,
  type ActivityEvent,
  type AgentSummary,
  type ApprovalRequest,
  type ChangeBody,
  type ChangeSummary,
  type FileBody,
  type GraphCoderTransport,
  type GraphMessage,
  type PageQuery,
  type SessionPage,
  type SessionId,
  type SessionSnapshot,
  type SessionSummary,
  type StartSessionInput,
  type WritebackApproval,
  type WritebackReceipt,
} from "./api.js";

/**
 * Small request/response protocol for native and terminal hosts.
 *
 * The host owns the durable Harness. This package only turns the public UI
 * transport calls into explicit messages. BigInts use decimal strings on the
 * wire so the same protocol works over JSON lines, N-API, and WASM bridges.
 */
export type GraphCoderWireMethod =
  | "list_sessions"
  | "start_session"
  | "open_session"
  | "resume_session"
  | "read_activity"
  | "read_messages"
  | "send_message"
  | "list_approvals"
  | "resolve_approval"
  | "cancel_session"
  | "list_changes"
  | "read_change"
  | "read_file"
  | "approve_writeback";

export interface GraphCoderWirePageQuery {
  readonly after?: string;
  readonly limit?: number;
}

/**
 * Method-specific request parameters. Keeping this map public gives native
 * and process hosts one source of truth for the dispatcher boundary instead
 * of making each host recreate stringly typed payloads.
 */
export interface GraphCoderWireParamsByMethod {
  readonly list_sessions: { readonly query?: GraphCoderWirePageQuery };
  readonly start_session: { readonly prompt: string; readonly operation_id: string; readonly model_fixture?: string };
  readonly open_session: { readonly session_id: string };
  readonly resume_session: { readonly session_id: string };
  readonly read_activity: { readonly session_id: string; readonly query?: GraphCoderWirePageQuery };
  readonly read_messages: { readonly session_id: string; readonly query?: GraphCoderWirePageQuery };
  readonly send_message: { readonly session_id: string; readonly sender_id: string; readonly recipient_id: string; readonly body: string };
  readonly list_approvals: { readonly session_id: string; readonly query?: GraphCoderWirePageQuery };
  readonly resolve_approval: { readonly approval_id: string; readonly approved: boolean; readonly session_id: string };
  readonly cancel_session: { readonly session_id: string };
  readonly list_changes: { readonly session_id: string };
  readonly read_change: { readonly session_id: string; readonly path: string; readonly generation: string };
  readonly read_file: { readonly session_id: string; readonly path: string; readonly generation: string };
  readonly approve_writeback: { readonly session_id: string; readonly operation_id: string; readonly expected_generation: string; readonly approved: boolean };
}

export type GraphCoderWireParams<M extends GraphCoderWireMethod = GraphCoderWireMethod> = Readonly<GraphCoderWireParamsByMethod[M]>;

export interface GraphCoderWireRequest<M extends GraphCoderWireMethod = GraphCoderWireMethod> {
  readonly request_id: string;
  readonly method: M;
  readonly params: GraphCoderWireParams<M>;
}

export interface GraphCoderWireError {
  readonly code: "invalid_input" | "not_found" | "stale" | "denied" | "unsupported" | "transport";
  readonly message: string;
}

export type GraphCoderWireResponse =
  | { readonly request_id: string; readonly ok: true; readonly result: unknown }
  | { readonly request_id: string; readonly ok: false; readonly error: GraphCoderWireError };

/** A native, terminal, JSON-lines, or WASM host can implement this one call. */
export interface GraphCoderBridge {
  request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse>;
}

/** Shared wire limit. Rust hosts validate UTF-8 bytes, so JS hosts must too. */
export const MAX_REQUEST_ID_BYTES = 256;

export function checkedRequestId(value: unknown, label = "request_id"): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new GraphCoderError("invalid_input", `${label} must be nonempty text`);
  }
  if (new TextEncoder().encode(value).byteLength > MAX_REQUEST_ID_BYTES) {
    throw new GraphCoderError("invalid_input", `${label} must be at most 256 UTF-8 bytes`);
  }
  return value;
}

/** Default upper bound for one bridge response envelope, in UTF-8 bytes. */
export const DEFAULT_MAX_BRIDGE_RESPONSE_BYTES = 16 * 1024 * 1024;
/** Protocol ceiling for one file body after a host selects a larger/chunked envelope. */
export const MAX_BRIDGE_FILE_BYTES = 64 * 1024 * 1024;

export type GraphCoderWireSessionSummary = Omit<SessionSummary, "id" | "rootAgentId"> & { readonly id: string; readonly root_agent_id: string };
export type GraphCoderWireAgentSummary = Omit<AgentSummary, "id" | "parentId" | "children"> & { readonly id: string; readonly parent_id: string | null; readonly children: readonly string[] };
export type GraphCoderWireActivityEvent = Omit<ActivityEvent, "sequence" | "actorId"> & { readonly sequence: string; readonly actor_id: string | null };
export type GraphCoderWireMessage = Omit<GraphMessage, "id" | "sessionId" | "senderId" | "recipientId"> & { readonly id: string; readonly session_id: string; readonly sender_id: string; readonly recipient_id: string };
export type GraphCoderWireApproval = Omit<ApprovalRequest, "id" | "sessionId" | "agentId"> & { readonly id: string; readonly session_id: string; readonly agent_id: string };
export type GraphCoderWireChangeSummary = Omit<ChangeSummary, "oldPath"> & { readonly old_path?: string };
export type GraphCoderWireChangeBody = Omit<ChangeBody, "generation" | "unifiedDiff"> & { readonly session_id: string; readonly generation: string; readonly unified_diff: string };
export type GraphCoderWireFileBody = Omit<FileBody, "generation" | "bytes" | "mediaType"> & { readonly session_id: string; readonly generation: string; readonly media_type: string; readonly bytes: readonly number[] };
export type GraphCoderWireSnapshot = Omit<SessionSnapshot, "summary" | "agents" | "workspaceGeneration"> & { readonly summary: GraphCoderWireSessionSummary; readonly agents: readonly GraphCoderWireAgentSummary[]; readonly workspace_generation: string };

export interface GraphCoderWireResultByMethod {
  readonly list_sessions: { readonly items: readonly GraphCoderWireSessionSummary[]; readonly next?: string };
  readonly start_session: GraphCoderWireSnapshot;
  readonly open_session: GraphCoderWireSnapshot;
  readonly resume_session: GraphCoderWireSnapshot;
  readonly read_activity: { readonly session_id: string; readonly items: readonly GraphCoderWireActivityEvent[]; readonly next?: string };
  readonly read_messages: { readonly session_id: string; readonly items: readonly GraphCoderWireMessage[]; readonly next?: string };
  readonly send_message: GraphCoderWireMessage;
  readonly list_approvals: { readonly session_id: string; readonly items: readonly GraphCoderWireApproval[]; readonly next?: string };
  readonly resolve_approval: GraphCoderWireApproval;
  readonly cancel_session: GraphCoderWireSnapshot;
  readonly list_changes: { readonly session_id: string; readonly generation: string; readonly items: readonly GraphCoderWireChangeSummary[] };
  readonly read_change: GraphCoderWireChangeBody;
  readonly read_file: GraphCoderWireFileBody;
  readonly approve_writeback: { readonly operation_id: string; readonly session_id: string; readonly generation: string; readonly applied: boolean };
}

export type GraphCoderWireResult<M extends GraphCoderWireMethod> = GraphCoderWireResultByMethod[M];

/**
 * Production transport adapter. It has no filesystem, process, model, or
 * storage behavior; those stay behind the injected durable Harness bridge.
 */
export class BridgeGraphCoderTransport implements GraphCoderTransport {
  #nextRequest = 1;

  constructor(
    readonly bridge: GraphCoderBridge,
    readonly requestPrefix = "graphcoder",
    readonly maximumResponseBytes = DEFAULT_MAX_BRIDGE_RESPONSE_BYTES,
    readonly maximumFileBytes = MAX_BRIDGE_FILE_BYTES,
  ) {
    if (!Number.isSafeInteger(maximumResponseBytes) || maximumResponseBytes < 1) {
      throw new GraphCoderError("invalid_input", "maximum bridge response bytes must be positive");
    }
    if (!Number.isSafeInteger(maximumFileBytes) || maximumFileBytes < 1 || maximumFileBytes > MAX_BRIDGE_FILE_BYTES) {
      throw new GraphCoderError("invalid_input", "maximum bridge file bytes must be between 1 and 64 MiB");
    }
  }

  async listSessions(query?: PageQuery): Promise<SessionPage> {
    return this.#page("list_sessions", queryParams(query), decodeSessionSummary);
  }

  async startSession(input: StartSessionInput): Promise<SessionSnapshot> {
    const prompt = checkedPublicText(input.prompt, "session prompt", MAX_PROMPT_BYTES);
    const requestedOperationId = checkedPublicText(input.operationId, "operation id", MAX_OPERATION_ID_BYTES);
    const params: GraphCoderWireParams<"start_session"> = input.modelFixture === undefined
      ? { prompt, operation_id: requestedOperationId }
      : { prompt, operation_id: requestedOperationId, model_fixture: input.modelFixture };
    return decodeSnapshot(await this.#call("start_session", params));
  }

  async openSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return boundSnapshot(decodeSnapshot(await this.#call("open_session", { session_id: id })), id, "open session");
  }

  async resumeSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return boundSnapshot(decodeSnapshot(await this.#call("resume_session", { session_id: id })), id, "resume session");
  }

  async readActivity(id: SessionSnapshot["summary"]["id"], query?: PageQuery): Promise<{ readonly items: readonly ActivityEvent[]; readonly next?: string }> {
    return this.#page("read_activity", { session_id: id, ...queryParams(query) }, decodeActivity, id);
  }

  async readMessages(id: SessionSnapshot["summary"]["id"], query?: PageQuery): Promise<{ readonly items: readonly GraphMessage[]; readonly next?: string }> {
    return this.#page("read_messages", { session_id: id, ...queryParams(query) }, decodeMessage, id);
  }

  async sendMessage(input: { readonly sessionId: SessionSnapshot["summary"]["id"]; readonly senderId: AgentSummary["id"]; readonly recipientId: AgentSummary["id"]; readonly body: string }): Promise<GraphMessage> {
    const body = checkedPublicText(input.body, "message body", MAX_MESSAGE_BODY_BYTES);
    const message = decodeMessage(await this.#call("send_message", { session_id: input.sessionId, sender_id: input.senderId, recipient_id: input.recipientId, body }));
    if (message.sessionId !== input.sessionId || message.senderId !== input.senderId || message.recipientId !== input.recipientId || message.body !== input.body) {
      throw new GraphCoderError("transport", "send message response is not bound to its request");
    }
    return message;
  }

  async listApprovals(id: SessionSnapshot["summary"]["id"], query?: PageQuery): Promise<{ readonly items: readonly ApprovalRequest[]; readonly next?: string }> {
    return this.#page("list_approvals", { session_id: id, ...queryParams(query) }, decodeApproval, id);
  }

  async resolveApproval(input: { readonly approvalId: ApprovalRequest["id"]; readonly approved: boolean; readonly sessionId: SessionSummary["id"] }): Promise<ApprovalRequest> {
    const params: GraphCoderWireParams<"resolve_approval"> = { approval_id: input.approvalId, approved: input.approved, session_id: input.sessionId };
    const approval = decodeApproval(await this.#call("resolve_approval", params));
    if (approval.id !== input.approvalId || approval.sessionId !== input.sessionId) throw new GraphCoderError("transport", "approval response is not bound to its request");
    return approval;
  }

  async cancelSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return boundSnapshot(decodeSnapshot(await this.#call("cancel_session", { session_id: id })), id, "cancel session");
  }

  async listChanges(id: SessionSnapshot["summary"]["id"]): Promise<{ readonly generation: bigint; readonly items: readonly ChangeSummary[] }> {
    const result = await this.#call("list_changes", { session_id: id });
    const raw = record(result, "changes result");
    const responseSession = sessionId(text(raw.session_id, "changes session id"));
    if (responseSession !== id) throw new GraphCoderError("transport", "changes response is not bound to its session");
    return { generation: wireBigInt(raw.generation, "changes generation"), items: array(raw.items, "change list").map(decodeChangeSummary) };
  }

  async readChange(id: SessionSnapshot["summary"]["id"], path: string, generation: bigint): Promise<ChangeBody> {
    const requestedPath = checkedPath(path);
    const requestedGeneration = checkedGeneration(generation, "change generation");
    const body = decodeChangeBody(await this.#call("read_change", { session_id: id, path: requestedPath, generation: requestedGeneration }), id);
    if (body.path !== requestedPath || body.generation !== generation) throw new GraphCoderError("transport", "change response is not bound to its request");
    return body;
  }

  async readFile(id: SessionSnapshot["summary"]["id"], path: string, generation: bigint): Promise<FileBody> {
    const requestedPath = checkedPath(path);
    const requestedGeneration = checkedGeneration(generation, "file generation");
    const body = decodeFileBody(await this.#call("read_file", { session_id: id, path: requestedPath, generation: requestedGeneration }), this.maximumFileBytes, id);
    if (body.path !== requestedPath || body.generation !== generation) throw new GraphCoderError("transport", "file response is not bound to its request");
    return body;
  }

  async approveWriteback(input: WritebackApproval): Promise<WritebackReceipt> {
    const requestedOperationId = checkedPublicText(input.operationId, "operation id", MAX_OPERATION_ID_BYTES);
    const result = await this.#call("approve_writeback", {
      session_id: input.sessionId,
      operation_id: requestedOperationId,
      expected_generation: checkedGeneration(input.expectedGeneration, "writeback generation"),
      approved: input.approved,
    });
    const operationId = text(result.operation_id, "writeback operation id");
    const responseSession = sessionId(text(result.session_id, "writeback session id"));
    const responseGeneration = wireBigInt(result.generation, "writeback generation");
    if (operationId !== requestedOperationId || responseSession !== input.sessionId || responseGeneration !== input.expectedGeneration || typeof result.applied !== "boolean") {
      throw new GraphCoderError("transport", "writeback response is not bound to its request");
    }
    return { operationId, sessionId: responseSession, generation: responseGeneration, applied: result.applied };
  }

  async #call<M extends GraphCoderWireMethod>(method: M, params: GraphCoderWireParams<M>): Promise<GraphCoderWireResult<M>> {
    const requestId = checkedRequestId(`${this.requestPrefix}-${this.#nextRequest++}`);
    const request = { request_id: requestId, method, params } as GraphCoderWireRequest<M>;
    let response: unknown;
    try {
      response = await this.bridge.request(request);
    } catch (error) {
      throw new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
    }
    const result = decodeResponse(response, requestId, this.maximumResponseBytes);
    return result as GraphCoderWireResult<M>;
  }

  async #page<M extends "list_sessions" | "read_activity" | "read_messages" | "list_approvals", T>(method: M, params: GraphCoderWireParams<M>, decode: (value: unknown) => T, expectedSession?: SessionSnapshot["summary"]["id"]): Promise<{ readonly items: readonly T[]; readonly next?: string }> {
    const result = await this.#call(method, params);
    const raw = record(result, `${method} result`);
    if (expectedSession !== undefined && sessionId(text(raw.session_id, `${method} session id`)) !== expectedSession) {
      throw new GraphCoderError("transport", `${method} response is not bound to its session`);
    }
    const items = array(raw.items, `${method} items`).map(decode);
    const next = raw.next === undefined ? undefined : text(raw.next, `${method} next cursor`);
    return { items, ...(next === undefined ? {} : { next }) };
  }
}

/** Semantic alias for hosts backed by PersistentLocalSwarm. */
export { BridgeGraphCoderTransport as HarnessGraphCoderTransport };

function wireQuery(query: PageQuery | undefined): GraphCoderWirePageQuery | undefined {
  if (query === undefined) return undefined;
  if (typeof query !== "object" || query === null || Array.isArray(query)) throw new GraphCoderError("invalid_input", "page query must be an object");
  if (query.after !== undefined && (typeof query.after !== "string" || query.after.trim() === "")) throw new GraphCoderError("invalid_input", "page cursor must be nonempty text");
  if (query.limit !== undefined && (!Number.isSafeInteger(query.limit) || query.limit < 1 || query.limit > 1_024)) throw new GraphCoderError("invalid_input", "page limit must be between 1 and 1024");
  const value: { after?: string; limit?: number } = {};
  if (query.after !== undefined) value.after = query.after;
  if (query.limit !== undefined) value.limit = query.limit;
  return value;
}

function queryParams(query: PageQuery | undefined): { readonly query?: GraphCoderWirePageQuery } {
  const value = wireQuery(query);
  return value === undefined ? {} : { query: value };
}

function checkedPath(path: unknown): string {
  if (typeof path !== "string") throw new GraphCoderError("invalid_input", "path must be text");
  const bytes = new TextEncoder().encode(path);
  if (bytes.byteLength === 0 || bytes.byteLength > 4_096) throw new GraphCoderError("invalid_input", "path must be between 1 and 4096 UTF-8 bytes");
  if (path.includes("\\") || path.startsWith("/") || /^[A-Za-z]:/u.test(path) || /[\u0000-\u001f\u007f]/u.test(path)) {
    throw new GraphCoderError("invalid_input", "path must be a relative slash-separated path");
  }
  if (path.split("/").some(segment => segment === "" || segment === "." || segment === "..")) {
    throw new GraphCoderError("invalid_input", "path contains an empty or traversal segment");
  }
  return path;
}

function checkedGeneration(generation: bigint, label: string): string {
  if (typeof generation !== "bigint" || generation < 0n) throw new GraphCoderError("invalid_input", `${label} must be a nonnegative bigint`);
  return generation.toString();
}

function decodeResponse(value: unknown, requestId: string, maximumBytes: number): unknown {
  let encoded: string;
  try {
    encoded = JSON.stringify(value);
  } catch (error) {
    throw new GraphCoderError("transport", `bridge response is not serializable: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (encoded === undefined) throw new GraphCoderError("transport", "bridge response is empty");
  if (new TextEncoder().encode(encoded).byteLength > maximumBytes) throw new GraphCoderError("transport", "bridge response exceeds the configured size limit");
  const raw = record(value, "bridge response");
  if (text(raw.request_id, "bridge response id") !== requestId) throw new GraphCoderError("transport", `bridge response id ${String(raw.request_id)} does not match ${requestId}`);
  if (raw.ok === true) {
    if (!("result" in raw)) throw new GraphCoderError("transport", "successful bridge response has no result");
    return raw.result;
  }
  if (raw.ok !== false) throw new GraphCoderError("transport", "bridge response ok must be boolean");
  const error = record(raw.error, "bridge error");
  const code = error.code;
  if (code !== "invalid_input" && code !== "not_found" && code !== "stale" && code !== "denied" && code !== "unsupported" && code !== "transport") throw new GraphCoderError("transport", "bridge error code is invalid");
  throw new GraphCoderError(code, text(error.message, "bridge error message"));
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new GraphCoderError("transport", `${label} is not an object`);
  return value as Record<string, unknown>;
}

function array(value: unknown, label: string): readonly unknown[] {
  if (!Array.isArray(value)) throw new GraphCoderError("transport", `${label} is not an array`);
  return value;
}

function text(value: unknown, label: string): string {
  if (typeof value !== "string") throw new GraphCoderError("transport", `${label} is not text`);
  return value;
}

function oneOf<T extends string>(value: unknown, choices: readonly T[], label: string): T {
  if (typeof value !== "string" || !choices.includes(value as T)) throw new GraphCoderError("transport", `${label} is invalid`);
  return value as T;
}

function wireBigInt(value: unknown, label: string): bigint {
  const raw = text(value, label);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) throw new GraphCoderError("transport", `${label} is not a decimal unsigned integer`);
  try { return BigInt(raw); } catch { throw new GraphCoderError("transport", `${label} is not a valid integer`); }
}

function decodeSessionSummary(value: unknown): SessionSummary {
  const raw = record(value, "session summary");
  return { id: sessionId(text(raw.id, "session id")), title: text(raw.title, "session title"), state: oneOf(raw.state, ["idle", "running", "completed", "failed", "cancelled"], "session state"), updatedAt: text(raw.updated_at, "session updated_at"), rootAgentId: agentId(text(raw.root_agent_id, "root agent id")) };
}

function boundSnapshot(snapshot: SessionSnapshot, expectedSession: SessionSnapshot["summary"]["id"], operation: string): SessionSnapshot {
  if (snapshot.summary.id !== expectedSession) throw new GraphCoderError("transport", `${operation} response is not bound to its session`);
  return snapshot;
}

function decodeAgent(value: unknown): AgentSummary {
  const raw = record(value, "agent summary");
  if (!Number.isSafeInteger(raw.depth) || (raw.depth as number) < 0) throw new GraphCoderError("transport", "agent depth is invalid");
  return { id: agentId(text(raw.id, "agent id")), parentId: raw.parent_id === null ? null : agentId(text(raw.parent_id, "agent parent id")), task: text(raw.task, "agent task"), state: oneOf(raw.state, ["queued", "running", "waiting", "completed", "failed", "cancelled"], "agent state"), depth: raw.depth as number, children: array(raw.children, "agent children").map(value => agentId(text(value, "agent child id"))) };
}

function decodeSnapshot(value: unknown): SessionSnapshot {
  const raw = record(value, "session snapshot");
  return { summary: decodeSessionSummary(raw.summary), agents: array(raw.agents, "session agents").map(decodeAgent), workspaceGeneration: wireBigInt(raw.workspace_generation, "workspace generation") };
}

function decodeActivity(value: unknown): ActivityEvent {
  const raw = record(value, "activity event");
  return { sequence: wireBigInt(raw.sequence, "activity sequence"), id: text(raw.id, "activity id"), kind: oneOf(raw.kind, ["session", "agent", "message", "approval", "workspace", "model"], "activity kind"), actorId: raw.actor_id === null ? null : agentId(text(raw.actor_id, "activity actor id")), text: text(raw.text, "activity text"), at: text(raw.at, "activity timestamp") };
}

function decodeMessage(value: unknown): GraphMessage {
  const raw = record(value, "message");
  return { id: messageId(text(raw.id, "message id")), sessionId: sessionId(text(raw.session_id, "message session id")), senderId: agentId(text(raw.sender_id, "message sender id")), recipientId: agentId(text(raw.recipient_id, "message recipient id")), body: checkedPublicText(raw.body, "message body", MAX_MESSAGE_BODY_BYTES), deliveredAt: raw.delivered_at === null ? null : text(raw.delivered_at, "message delivery timestamp") };
}

function decodeApproval(value: unknown): ApprovalRequest {
  const raw = record(value, "approval");
  return { id: approvalId(text(raw.id, "approval id")), sessionId: sessionId(text(raw.session_id, "approval session id")), agentId: agentId(text(raw.agent_id, "approval agent id")), operationId: text(raw.operation_id, "approval operation id"), actionDigest: text(raw.action_digest, "approval action digest"), description: text(raw.description, "approval description"), state: oneOf(raw.state, ["pending", "approved", "declined", "cancelled", "expired", "denied"], "approval state"), createdAt: text(raw.created_at, "approval created_at") };
}

function decodeChangeSummary(value: unknown): ChangeSummary {
  const raw = record(value, "change summary");
  const result: ChangeSummary = { path: text(raw.path, "change path"), kind: oneOf(raw.kind, ["added", "modified", "deleted", "renamed"], "change kind"), additions: checkedCount(raw.additions, "change additions"), deletions: checkedCount(raw.deletions, "change deletions") };
  if (raw.old_path !== undefined) return { ...result, oldPath: text(raw.old_path, "change old path") };
  return result;
}

function checkedCount(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new GraphCoderError("transport", `${label} is invalid`);
  return value as number;
}

function decodeChangeBody(value: unknown, expectedSession: SessionId): ChangeBody {
  const raw = record(value, "change body");
  if (sessionId(text(raw.session_id, "change session id")) !== expectedSession) throw new GraphCoderError("transport", "change response is not bound to its session");
  return { path: text(raw.path, "change path"), unifiedDiff: text(raw.unified_diff, "unified diff"), generation: wireBigInt(raw.generation, "change generation") };
}

function decodeFileBody(value: unknown, maximumBytes = MAX_BRIDGE_FILE_BYTES, expectedSession?: SessionId): FileBody {
  const raw = record(value, "file body");
  if (expectedSession !== undefined && sessionId(text(raw.session_id, "file session id")) !== expectedSession) throw new GraphCoderError("transport", "file response is not bound to its session");
  const bytes = raw.bytes instanceof Uint8Array ? raw.bytes : array(raw.bytes, "file bytes");
  if (bytes.length > maximumBytes) throw new GraphCoderError("transport", "file body exceeds the configured file size limit");
  if (!bytes.every(byte => Number.isInteger(byte) && (byte as number) >= 0 && (byte as number) <= 255)) throw new GraphCoderError("transport", "file bytes contain an invalid octet");
  return { path: text(raw.path, "file path"), mediaType: text(raw.media_type, "file media type"), bytes: Uint8Array.from(bytes as ArrayLike<number>), generation: wireBigInt(raw.generation, "file generation") };
}
