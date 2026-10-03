import {
  GraphCoderError,
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

export type GraphCoderWireParams = Readonly<Record<string, unknown>>;

export interface GraphCoderWireRequest<M extends GraphCoderWireMethod = GraphCoderWireMethod> {
  readonly request_id: string;
  readonly method: M;
  readonly params: GraphCoderWireParams;
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

type WireSessionSummary = Omit<SessionSummary, "id" | "rootAgentId"> & { readonly id: string; readonly root_agent_id: string };
type WireAgentSummary = Omit<AgentSummary, "id" | "parentId" | "children"> & { readonly id: string; readonly parent_id: string | null; readonly children: readonly string[] };
type WireActivityEvent = Omit<ActivityEvent, "sequence" | "actorId"> & { readonly sequence: string; readonly actor_id: string | null };
type WireMessage = Omit<GraphMessage, "id" | "sessionId" | "senderId" | "recipientId"> & { readonly id: string; readonly session_id: string; readonly sender_id: string; readonly recipient_id: string };
type WireApproval = Omit<ApprovalRequest, "id" | "sessionId" | "agentId"> & { readonly id: string; readonly session_id: string; readonly agent_id: string };
type WireChangeBody = Omit<ChangeBody, "generation"> & { readonly generation: string };
type WireFileBody = Omit<FileBody, "generation" | "bytes"> & { readonly generation: string; readonly bytes: readonly number[] };
type WireSnapshot = Omit<SessionSnapshot, "summary" | "agents" | "workspaceGeneration"> & { readonly summary: WireSessionSummary; readonly agents: readonly WireAgentSummary[]; readonly workspace_generation: string };

interface WireResultMap {
  list_sessions: { readonly items: readonly WireSessionSummary[]; readonly next?: string };
  start_session: WireSnapshot;
  open_session: WireSnapshot;
  resume_session: WireSnapshot;
  read_activity: { readonly items: readonly WireActivityEvent[]; readonly next?: string };
  read_messages: { readonly items: readonly WireMessage[]; readonly next?: string };
  send_message: WireMessage;
  list_approvals: { readonly items: readonly WireApproval[]; readonly next?: string };
  resolve_approval: WireApproval;
  cancel_session: WireSnapshot;
  list_changes: { readonly generation: string; readonly items: readonly ChangeSummary[] };
  read_change: WireChangeBody;
  read_file: WireFileBody;
  approve_writeback: { readonly operation_id: string; readonly session_id: string; readonly generation: string; readonly applied: boolean };
}

type WireParams<M extends GraphCoderWireMethod> = GraphCoderWireParams & { readonly method?: M };

/**
 * Production transport adapter. It has no filesystem, process, model, or
 * storage behavior; those stay behind the injected durable Harness bridge.
 */
export class BridgeGraphCoderTransport implements GraphCoderTransport {
  #nextRequest = 1;

  constructor(readonly bridge: GraphCoderBridge, readonly requestPrefix = "graphcoder") {}

  async listSessions(query?: PageQuery): Promise<SessionPage> {
    return this.#page("list_sessions", queryParams(query), decodeSessionSummary);
  }

  async startSession(input: StartSessionInput): Promise<SessionSnapshot> {
    const params: GraphCoderWireParams = input.modelFixture === undefined
      ? { prompt: input.prompt }
      : { prompt: input.prompt, model_fixture: input.modelFixture };
    return decodeSnapshot(await this.#call("start_session", params));
  }

  async openSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return decodeSnapshot(await this.#call("open_session", { session_id: id }));
  }

  async resumeSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return decodeSnapshot(await this.#call("resume_session", { session_id: id }));
  }

  async readActivity(id: SessionSnapshot["summary"]["id"], query?: PageQuery): Promise<{ readonly items: readonly ActivityEvent[]; readonly next?: string }> {
    return this.#page("read_activity", { session_id: id, ...queryParams(query) }, decodeActivity);
  }

  async readMessages(id: SessionSnapshot["summary"]["id"], query?: PageQuery): Promise<{ readonly items: readonly GraphMessage[]; readonly next?: string }> {
    return this.#page("read_messages", { session_id: id, ...queryParams(query) }, decodeMessage);
  }

  async sendMessage(input: { readonly sessionId: SessionSnapshot["summary"]["id"]; readonly senderId: AgentSummary["id"]; readonly recipientId: AgentSummary["id"]; readonly body: string }): Promise<GraphMessage> {
    return decodeMessage(await this.#call("send_message", { session_id: input.sessionId, sender_id: input.senderId, recipient_id: input.recipientId, body: input.body }));
  }

  async listApprovals(id: SessionSnapshot["summary"]["id"], query?: PageQuery): Promise<{ readonly items: readonly ApprovalRequest[]; readonly next?: string }> {
    return this.#page("list_approvals", { session_id: id, ...queryParams(query) }, decodeApproval);
  }

  async resolveApproval(input: { readonly approvalId: ApprovalRequest["id"]; readonly approved: boolean }): Promise<ApprovalRequest> {
    return decodeApproval(await this.#call("resolve_approval", { approval_id: input.approvalId, approved: input.approved }));
  }

  async cancelSession(id: SessionSnapshot["summary"]["id"]): Promise<SessionSnapshot> {
    return decodeSnapshot(await this.#call("cancel_session", { session_id: id }));
  }

  async listChanges(id: SessionSnapshot["summary"]["id"]): Promise<{ readonly generation: bigint; readonly items: readonly ChangeSummary[] }> {
    const result = await this.#call("list_changes", { session_id: id });
    return { generation: wireBigInt(result.generation, "changes generation"), items: result.items };
  }

  async readChange(id: SessionSnapshot["summary"]["id"], path: string, generation: bigint): Promise<ChangeBody> {
    return decodeChangeBody(await this.#call("read_change", { session_id: id, path, generation: generation.toString() }));
  }

  async readFile(id: SessionSnapshot["summary"]["id"], path: string, generation: bigint): Promise<FileBody> {
    return decodeFileBody(await this.#call("read_file", { session_id: id, path, generation: generation.toString() }));
  }

  async approveWriteback(input: WritebackApproval): Promise<WritebackReceipt> {
    const result = await this.#call("approve_writeback", {
      session_id: input.sessionId,
      operation_id: input.operationId,
      expected_generation: input.expectedGeneration.toString(),
      approved: input.approved,
    });
    return { operationId: result.operation_id, sessionId: sessionId(result.session_id), generation: wireBigInt(result.generation, "writeback generation"), applied: result.applied };
  }

  async #call<M extends GraphCoderWireMethod>(method: M, params: WireParams<M>): Promise<WireResultMap[M]> {
    const requestId = `${this.requestPrefix}-${this.#nextRequest++}`;
    const request = { request_id: requestId, method, params } as GraphCoderWireRequest<M>;
    let response: GraphCoderWireResponse;
    try {
      response = await this.bridge.request(request);
    } catch (error) {
      throw new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
    }
    if (response.request_id !== requestId) throw new GraphCoderError("transport", `bridge response id ${response.request_id} does not match ${requestId}`);
    if (!response.ok) throw new GraphCoderError(response.error.code, response.error.message);
    return response.result as WireResultMap[M];
  }

  async #page<M extends "list_sessions" | "read_activity" | "read_messages" | "list_approvals", T>(method: M, params: WireParams<M>, decode: (value: unknown) => T): Promise<{ readonly items: readonly T[]; readonly next?: string }> {
    const result = await this.#call(method, params);
    return { items: (result.items as readonly unknown[]).map(decode), ...(result.next === undefined ? {} : { next: result.next }) };
  }
}

/** Semantic alias for hosts backed by PersistentLocalSwarm. */
export { BridgeGraphCoderTransport as HarnessGraphCoderTransport };

function wireQuery(query: PageQuery | undefined): GraphCoderWirePageQuery | undefined {
  if (query === undefined) return undefined;
  const value: { after?: string; limit?: number } = {};
  if (query.after !== undefined) value.after = query.after;
  if (query.limit !== undefined) value.limit = query.limit;
  return value;
}

function queryParams(query: PageQuery | undefined): GraphCoderWireParams {
  const value = wireQuery(query);
  return value === undefined ? {} : { query: value };
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new GraphCoderError("transport", `${label} is not an object`);
  return value as Record<string, unknown>;
}

function text(value: unknown, label: string): string {
  if (typeof value !== "string") throw new GraphCoderError("transport", `${label} is not text`);
  return value;
}

function wireBigInt(value: unknown, label: string): bigint {
  const raw = text(value, label);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) throw new GraphCoderError("transport", `${label} is not a decimal unsigned integer`);
  try { return BigInt(raw); } catch { throw new GraphCoderError("transport", `${label} is not a valid integer`); }
}

function decodeSessionSummary(value: unknown): SessionSummary {
  const raw = record(value, "session summary");
  return { id: sessionId(text(raw.id, "session id")), title: text(raw.title, "session title"), state: raw.state as SessionSummary["state"], updatedAt: text(raw.updated_at, "session updated_at"), rootAgentId: agentId(text(raw.root_agent_id, "root agent id")) };
}

function decodeAgent(value: WireAgentSummary): AgentSummary {
  return { id: agentId(value.id), parentId: value.parent_id === null ? null : agentId(value.parent_id), task: value.task, state: value.state, depth: value.depth, children: value.children.map(agentId) };
}

function decodeSnapshot(value: WireSnapshot): SessionSnapshot {
  return { summary: decodeSessionSummary(value.summary as never), agents: value.agents.map(decodeAgent), workspaceGeneration: wireBigInt(value.workspace_generation, "workspace generation") };
}

function decodeActivity(value: unknown): ActivityEvent {
  const raw = record(value, "activity event");
  return { sequence: wireBigInt(raw.sequence, "activity sequence"), id: text(raw.id, "activity id"), kind: raw.kind as ActivityEvent["kind"], actorId: raw.actor_id === null ? null : agentId(text(raw.actor_id, "activity actor id")), text: text(raw.text, "activity text"), at: text(raw.at, "activity timestamp") };
}

function decodeMessage(value: unknown): GraphMessage {
  const raw = record(value, "message");
  return { id: messageId(text(raw.id, "message id")), sessionId: sessionId(text(raw.session_id, "message session id")), senderId: agentId(text(raw.sender_id, "message sender id")), recipientId: agentId(text(raw.recipient_id, "message recipient id")), body: text(raw.body, "message body"), deliveredAt: raw.delivered_at === null ? null : text(raw.delivered_at, "message delivery timestamp") };
}

function decodeApproval(value: unknown): ApprovalRequest {
  const raw = record(value, "approval");
  return { id: approvalId(text(raw.id, "approval id")), sessionId: sessionId(text(raw.session_id, "approval session id")), agentId: agentId(text(raw.agent_id, "approval agent id")), operationId: text(raw.operation_id, "approval operation id"), actionDigest: text(raw.action_digest, "approval action digest"), description: text(raw.description, "approval description"), state: raw.state as ApprovalRequest["state"], createdAt: text(raw.created_at, "approval created_at") };
}

function decodeChangeBody(value: WireChangeBody): ChangeBody {
  return { path: value.path, unifiedDiff: value.unifiedDiff, generation: wireBigInt(value.generation, "change generation") };
}

function decodeFileBody(value: WireFileBody): FileBody {
  if (!value.bytes.every(byte => Number.isInteger(byte) && byte >= 0 && byte <= 255)) throw new GraphCoderError("transport", "file bytes contain an invalid octet");
  return { path: value.path, mediaType: value.mediaType, bytes: Uint8Array.from(value.bytes), generation: wireBigInt(value.generation, "file generation") };
}
