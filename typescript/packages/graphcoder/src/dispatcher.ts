import {
  GraphCoderError,
  agentId,
  approvalId,
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
  type SessionSnapshot,
  type SessionSummary,
} from "./api.js";
import type { GraphCoderWireMethod, GraphCoderWireRequest, GraphCoderWireResponse } from "./bridge.js";

/** Native-side JSON-lines dispatcher over an injected durable transport. */
export class GraphCoderWireDispatcher {
  constructor(readonly transport: GraphCoderTransport) {}

  async dispatch(value: unknown): Promise<GraphCoderWireResponse> {
    const requestId = requestIdentity(value);
    try {
      const request = decodeRequest(value);
      const result = await this.#dispatch(request);
      return { request_id: request.request_id, ok: true, result };
    } catch (error) {
      const typed = error instanceof GraphCoderError ? error : new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
      return { request_id: requestId, ok: false, error: { code: typed.code, message: typed.message } };
    }
  }

  async dispatchLine(line: string): Promise<string> {
    let value: unknown;
    try { value = JSON.parse(line) as unknown; }
    catch (error) { return JSON.stringify({ request_id: "unknown", ok: false, error: { code: "invalid_input", message: `request is not JSON: ${error instanceof Error ? error.message : String(error)}` } }); }
    return JSON.stringify(await this.dispatch(value), jsonReplacer);
  }

  async #dispatch(request: GraphCoderWireRequest): Promise<unknown> {
    const params = record(request.params, "request params");
    switch (request.method) {
      case "list_sessions": return wirePage(await this.transport.listSessions(page(params.query)), wireSessionSummary);
      case "start_session": return wireSnapshot(await this.transport.startSession({ prompt: requiredText(params.prompt, "prompt"), operationId: requiredText(params.operation_id, "operation_id"), ...(params.model_fixture === undefined ? {} : { modelFixture: requiredText(params.model_fixture, "model_fixture") }) }));
      case "open_session": return wireSnapshot(await this.transport.openSession(sessionId(requiredText(params.session_id, "session_id"))));
      case "resume_session": return wireSnapshot(await this.transport.resumeSession(sessionId(requiredText(params.session_id, "session_id"))));
      case "read_activity": { const id = sessionId(requiredText(params.session_id, "session_id")); return wirePage(await this.transport.readActivity(id, page(params.query)), wireActivity); }
      case "read_messages": { const id = sessionId(requiredText(params.session_id, "session_id")); return wirePage(await this.transport.readMessages(id, page(params.query)), wireMessage); }
      case "send_message": { const id = sessionId(requiredText(params.session_id, "session_id")); return wireMessage(await this.transport.sendMessage({ sessionId: id, senderId: agentId(requiredText(params.sender_id, "sender_id")), recipientId: agentId(requiredText(params.recipient_id, "recipient_id")), body: requiredText(params.body, "body") })); }
      case "list_approvals": { const id = sessionId(requiredText(params.session_id, "session_id")); return wirePage(await this.transport.listApprovals(id, page(params.query)), wireApproval); }
      case "resolve_approval": { const id = sessionId(requiredText(params.session_id, "session_id")); return wireApproval(await this.transport.resolveApproval({ approvalId: approvalId(requiredText(params.approval_id, "approval_id")), approved: requiredBoolean(params.approved, "approved"), sessionId: id })); }
      case "cancel_session": return wireSnapshot(await this.transport.cancelSession(sessionId(requiredText(params.session_id, "session_id"))));
      case "list_changes": { const id = sessionId(requiredText(params.session_id, "session_id")); const changes = await this.transport.listChanges(id); return { session_id: id, generation: changes.generation, items: changes.items.map(wireChangeSummary) }; }
      case "read_change": { const id = sessionId(requiredText(params.session_id, "session_id")); return wireChange(await this.transport.readChange(id, requiredText(params.path, "path"), generation(params.generation)), id); }
      case "read_file": { const id = sessionId(requiredText(params.session_id, "session_id")); return wireFile(await this.transport.readFile(id, requiredText(params.path, "path"), generation(params.generation)), id); }
      case "approve_writeback": { const id = sessionId(requiredText(params.session_id, "session_id")); const receipt = await this.transport.approveWriteback({ sessionId: id, operationId: requiredText(params.operation_id, "operation_id"), expectedGeneration: generation(params.expected_generation), approved: requiredBoolean(params.approved, "approved") }); return { operation_id: receipt.operationId, session_id: receipt.sessionId, generation: receipt.generation, applied: receipt.applied }; }
    }
  }
}

function decodeRequest(value: unknown): GraphCoderWireRequest {
  const raw = record(value, "request");
  const requestId = requiredText(raw.request_id, "request_id");
  const method = requiredText(raw.method, "method");
  if (!METHODS.has(method as GraphCoderWireMethod)) throw new GraphCoderError("invalid_input", `unsupported method ${method}`);
  return { request_id: requestId, method: method as GraphCoderWireMethod, params: record(raw.params, "request params") } as GraphCoderWireRequest;
}
function requestIdentity(value: unknown): string { return typeof value === "object" && value !== null && !Array.isArray(value) && typeof (value as Record<string, unknown>).request_id === "string" ? (value as Record<string, unknown>).request_id as string : "unknown"; }
const METHODS = new Set<GraphCoderWireMethod>(["list_sessions", "start_session", "open_session", "resume_session", "read_activity", "read_messages", "send_message", "list_approvals", "resolve_approval", "cancel_session", "list_changes", "read_change", "read_file", "approve_writeback"]);
function record(value: unknown, label: string): Record<string, unknown> { if (typeof value !== "object" || value === null || Array.isArray(value)) throw new GraphCoderError("invalid_input", `${label} must be an object`); return value as Record<string, unknown>; }
function requiredText(value: unknown, label: string): string { if (typeof value !== "string" || value.trim() === "") throw new GraphCoderError("invalid_input", `${label} must be nonempty text`); return value; }
function requiredBoolean(value: unknown, label: string): boolean { if (typeof value !== "boolean") throw new GraphCoderError("invalid_input", `${label} must be boolean`); return value; }
function generation(value: unknown): bigint { const text = requiredText(value, "generation"); if (!/^(0|[1-9][0-9]*)$/u.test(text)) throw new GraphCoderError("invalid_input", "generation must be an unsigned decimal string"); return BigInt(text); }
function page(value: unknown): PageQuery | undefined { if (value === undefined) return undefined; const raw = record(value, "query"); if (raw.after !== undefined && (typeof raw.after !== "string" || raw.after.trim() === "")) throw new GraphCoderError("invalid_input", "page cursor must be nonempty text"); if (raw.limit !== undefined && (!Number.isSafeInteger(raw.limit) || (raw.limit as number) < 1 || (raw.limit as number) > 1_024)) throw new GraphCoderError("invalid_input", "page limit must be between 1 and 1024"); return { ...(raw.after === undefined ? {} : { after: raw.after as string }), ...(raw.limit === undefined ? {} : { limit: raw.limit as number }) }; }
function wirePage<T>(value: { readonly items: readonly T[]; readonly next?: string }, map: (value: T) => unknown): unknown { return { items: value.items.map(map), ...(value.next === undefined ? {} : { next: value.next }) }; }
function wireSessionSummary(value: SessionSummary): unknown { return { ...value, id: value.id as string, root_agent_id: value.rootAgentId as string, updated_at: value.updatedAt }; }
function wireSnapshot(value: SessionSnapshot): unknown { return { summary: wireSessionSummary(value.summary), agents: value.agents.map(wireAgent), workspace_generation: value.workspaceGeneration }; }
function wireAgent(value: AgentSummary): unknown { return { ...value, id: value.id as string, parent_id: value.parentId as string | null, children: value.children.map(child => child as string) }; }
function wireActivity(value: ActivityEvent): unknown { return { ...value, sequence: value.sequence, actor_id: value.actorId as string | null }; }
function wireMessage(value: GraphMessage): unknown { return { ...value, id: value.id as string, session_id: value.sessionId as string, sender_id: value.senderId as string, recipient_id: value.recipientId as string, delivered_at: value.deliveredAt }; }
function wireApproval(value: ApprovalRequest): unknown { return { ...value, id: value.id as string, session_id: value.sessionId as string, agent_id: value.agentId as string, operation_id: value.operationId, action_digest: value.actionDigest, created_at: value.createdAt }; }
function wireChangeSummary(value: ChangeSummary): unknown { return { ...value, old_path: value.oldPath }; }
function wireChange(value: ChangeBody, session: SessionSummary["id"]): unknown { return { session_id: session as string, path: value.path, unified_diff: value.unifiedDiff, generation: value.generation }; }
function wireFile(value: FileBody, session: SessionSummary["id"]): unknown { return { session_id: session as string, path: value.path, media_type: value.mediaType, bytes: [...value.bytes], generation: value.generation }; }
function jsonReplacer(_key: string, value: unknown): unknown { return typeof value === "bigint" ? value.toString() : value; }
