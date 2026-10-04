import {
  GraphCoderError,
  checkedPath,
  checkedPublicText,
  MAX_MESSAGE_BODY_BYTES,
  MAX_OPERATION_ID_BYTES,
  MAX_PROMPT_BYTES,
  agentId,
  approvalId,
  sessionId,
  type GraphCoderTransport,
} from "./api.js";
import { checkedRequestId, GRAPH_CODER_WIRE_METHODS, type GraphCoderWireMethod, type GraphCoderWireRequest, type GraphCoderWireResponse } from "./bridge.js";
import { decodeGeneration, decodePageQuery, encodeGeneration, wireActivity, wireApproval, wireChange, wireChangeSummary, wireFile, wireMessage, wirePage, wireSessionSummary, wireSnapshot } from "./wire-codec.js";

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
    return JSON.stringify(await this.dispatch(value));
  }

  async #dispatch(request: GraphCoderWireRequest): Promise<unknown> {
    const params = record(request.params, "request params");
    switch (request.method) {
      case "list_sessions": return wirePage(await this.transport.listSessions(decodePageQuery(params.query)), wireSessionSummary);
      case "start_session": return wireSnapshot(await this.transport.startSession({ prompt: checkedPublicText(params.prompt, "prompt", MAX_PROMPT_BYTES), operationId: checkedPublicText(params.operation_id, "operation_id", MAX_OPERATION_ID_BYTES), ...(params.model_fixture === undefined ? {} : { modelFixture: checkedPublicText(params.model_fixture, "model_fixture", MAX_OPERATION_ID_BYTES) }) }));
      case "open_session": return wireSnapshot(await this.transport.openSession(sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES))));
      case "resume_session": return wireSnapshot(await this.transport.resumeSession(sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES))));
      case "read_activity": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wirePage(await this.transport.readActivity(id, decodePageQuery(params.query)), wireActivity); }
      case "read_messages": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wirePage(await this.transport.readMessages(id, decodePageQuery(params.query)), wireMessage); }
      case "send_message": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wireMessage(await this.transport.sendMessage({ sessionId: id, senderId: agentId(checkedPublicText(params.sender_id, "sender_id", MAX_OPERATION_ID_BYTES)), recipientId: agentId(checkedPublicText(params.recipient_id, "recipient_id", MAX_OPERATION_ID_BYTES)), body: checkedPublicText(params.body, "body", MAX_MESSAGE_BODY_BYTES) })); }
      case "list_approvals": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wirePage(await this.transport.listApprovals(id, decodePageQuery(params.query)), wireApproval); }
      case "resolve_approval": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wireApproval(await this.transport.resolveApproval({ approvalId: approvalId(checkedPublicText(params.approval_id, "approval_id", MAX_OPERATION_ID_BYTES)), approved: requiredBoolean(params.approved, "approved"), sessionId: id })); }
      case "cancel_session": return wireSnapshot(await this.transport.cancelSession(sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES))));
      case "list_changes": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); const changes = await this.transport.listChanges(id); return { session_id: id, generation: encodeGeneration(changes.generation, "changes generation", "transport"), items: changes.items.map(wireChangeSummary) }; }
      case "read_change": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wireChange(await this.transport.readChange(id, checkedPath(params.path), decodeGeneration(params.generation, "generation", "invalid_input")), id); }
      case "read_file": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); return wireFile(await this.transport.readFile(id, checkedPath(params.path), decodeGeneration(params.generation, "generation", "invalid_input")), id); }
      case "approve_writeback": { const id = sessionId(checkedPublicText(params.session_id, "session_id", MAX_OPERATION_ID_BYTES)); const receipt = await this.transport.approveWriteback({ sessionId: id, operationId: checkedPublicText(params.operation_id, "operation_id", MAX_OPERATION_ID_BYTES), expectedGeneration: decodeGeneration(params.expected_generation, "expected_generation", "invalid_input"), approved: requiredBoolean(params.approved, "approved") }); return { operation_id: receipt.operationId, session_id: receipt.sessionId, generation: encodeGeneration(receipt.generation, "writeback generation", "transport"), applied: receipt.applied }; }
    }
  }
}

function decodeRequest(value: unknown): GraphCoderWireRequest {
  const raw = record(value, "request");
  const requestId = checkedRequestId(raw.request_id);
  const method = checkedPublicText(raw.method, "method", MAX_OPERATION_ID_BYTES);
  if (!GRAPH_CODER_WIRE_METHODS.includes(method as GraphCoderWireMethod)) throw new GraphCoderError("invalid_input", `unsupported method ${method}`);
  return { request_id: requestId, method: method as GraphCoderWireMethod, params: record(raw.params, "request params") } as GraphCoderWireRequest;
}
function requestIdentity(value: unknown): string {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return "unknown";
  const candidate = (value as Record<string, unknown>).request_id;
  if (typeof candidate !== "string") return "unknown";
  try { return checkedRequestId(candidate); }
  catch { return "unknown"; }
}
function record(value: unknown, label: string): Record<string, unknown> { if (typeof value !== "object" || value === null || Array.isArray(value)) throw new GraphCoderError("invalid_input", `${label} must be an object`); return value as Record<string, unknown>; }
function requiredBoolean(value: unknown, label: string): boolean { if (typeof value !== "boolean") throw new GraphCoderError("invalid_input", `${label} must be boolean`); return value; }
