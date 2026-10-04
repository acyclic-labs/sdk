import {
  GraphCoderError,
  checkedPath,
  checkedPublicText,
  MAX_MESSAGE_BODY_BYTES,
  MAX_OPERATION_ID_BYTES,
  MAX_PROMPT_BYTES,
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
  type SessionSnapshot,
  type SessionSummary,
  type StartSessionInput,
  type WritebackApproval,
  type WritebackReceipt,
} from "./api.js";
import {
  checkedRequestId,
  decodeActivity,
  decodeAgent,
  decodeApproval,
  decodeChangeBody,
  decodeChangeSummary,
  decodeFileBody,
  decodeGeneration,
  decodeMessage,
  decodeSessionSummary,
  decodeSnapshot,
  encodeGeneration,
  encodePageQuery,
  MAX_REQUEST_ID_BYTES,
  array,
  record,
  text,
} from "./wire-codec.js";
export { checkedRequestId, MAX_REQUEST_ID_BYTES } from "./wire-codec.js";

/**
 * Small request/response protocol for native and terminal hosts.
 *
 * The host owns the durable Harness. This package only turns the public UI
 * transport calls into explicit messages. BigInts use decimal strings on the
 * wire so the same protocol works over JSON lines, N-API, and WASM bridges.
 * GRAPH_CODER_WIRE_METHODS is the complete method list at runtime and in the type system.
 */
export const GRAPH_CODER_WIRE_METHODS = Object.freeze([
  "list_sessions",
  "start_session",
  "open_session",
  "resume_session",
  "read_activity",
  "read_messages",
  "send_message",
  "list_approvals",
  "resolve_approval",
  "cancel_session",
  "list_changes",
  "read_change",
  "read_file",
  "approve_writeback",
] as const);

export type GraphCoderWireMethod = (typeof GRAPH_CODER_WIRE_METHODS)[number];

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
  /** Host-only operator control; never encoded as a public GraphCoder request. */
  operatorApprove?(input: {
    readonly approvalId: string;
    readonly approved: boolean;
    readonly sessionId: string;
  }): Promise<void>;
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

  async operatorApprove(input: { readonly approvalId: ApprovalRequest["id"]; readonly approved: boolean; readonly sessionId: SessionSummary["id"] }): Promise<void> {
    await this.bridge.operatorApprove?.(input);
  }

  async cancelSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return boundSnapshot(decodeSnapshot(await this.#call("cancel_session", { session_id: id })), id, "cancel session");
  }

  async listChanges(id: SessionSnapshot["summary"]["id"]): Promise<{ readonly generation: bigint; readonly items: readonly ChangeSummary[] }> {
    const result = await this.#call("list_changes", { session_id: id });
    const raw = record(result, "changes result");
    const responseSession = sessionId(text(raw.session_id, "changes session id"));
    if (responseSession !== id) throw new GraphCoderError("transport", "changes response is not bound to its session");
    return { generation: decodeGeneration(raw.generation, "changes generation"), items: array(raw.items, "change list").map(decodeChangeSummary) };
  }

  async readChange(id: SessionSnapshot["summary"]["id"], path: string, generation: bigint): Promise<ChangeBody> {
    const requestedPath = checkedPath(path);
    const requestedGeneration = encodeGeneration(generation, "change generation");
    const body = decodeChangeBody(await this.#call("read_change", { session_id: id, path: requestedPath, generation: requestedGeneration }), id);
    if (body.path !== requestedPath || body.generation !== generation) throw new GraphCoderError("transport", "change response is not bound to its request");
    return body;
  }

  async readFile(id: SessionSnapshot["summary"]["id"], path: string, generation: bigint): Promise<FileBody> {
    const requestedPath = checkedPath(path);
    const requestedGeneration = encodeGeneration(generation, "file generation");
    const body = decodeFileBody(await this.#call("read_file", { session_id: id, path: requestedPath, generation: requestedGeneration }), this.maximumFileBytes, id);
    if (body.path !== requestedPath || body.generation !== generation) throw new GraphCoderError("transport", "file response is not bound to its request");
    return body;
  }

  async approveWriteback(input: WritebackApproval): Promise<WritebackReceipt> {
    const requestedOperationId = checkedPublicText(input.operationId, "operation id", MAX_OPERATION_ID_BYTES);
    const result = await this.#call("approve_writeback", {
      session_id: input.sessionId,
      operation_id: requestedOperationId,
      expected_generation: encodeGeneration(input.expectedGeneration, "writeback generation"),
      approved: input.approved,
    });
    const operationId = text(result.operation_id, "writeback operation id");
    const responseSession = sessionId(text(result.session_id, "writeback session id"));
    const responseGeneration = decodeGeneration(result.generation, "writeback generation");
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

function queryParams(query: PageQuery | undefined): { readonly query?: GraphCoderWirePageQuery } {
  const value = encodePageQuery(query);
  return value === undefined ? {} : { query: value };
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


function boundSnapshot(snapshot: SessionSnapshot, expectedSession: SessionSnapshot["summary"]["id"], operation: string): SessionSnapshot {
  if (snapshot.summary.id !== expectedSession) throw new GraphCoderError("transport", `${operation} response is not bound to its session`);
  return snapshot;
}
