import {
  GraphCoderError,
  MAX_MESSAGE_BODY_BYTES,
  MAX_OPERATION_ID_BYTES,
  agentId,
  approvalId,
  checkedPublicText,
  messageId,
  sessionId,
  type ActivityEvent,
  type AgentSummary,
  type ApprovalRequest,
  type ChangeBody,
  type ChangeSummary,
  type FileBody,
  type GraphMessage,
  type PageQuery,
  type SessionSnapshot,
  type SessionSummary,
  type SessionId,
} from "./api.js";

/**
 * Private codec shared by the bridge and its native dispatcher.
 *
 * Keeping the wire checks and JSON shape in one place is deliberate: the
 * browser/native side and the host side must reject and encode the same bytes
 * before a request reaches a transport.
 */
export const MAX_REQUEST_ID_BYTES = MAX_OPERATION_ID_BYTES;

export function checkedRequestId(value: unknown, label = "request_id"): string {
  return checkedPublicText(value, label, MAX_REQUEST_ID_BYTES);
}

export function encodePageQuery(query: PageQuery | undefined): { readonly after?: string; readonly limit?: number } | undefined {
  if (query === undefined) return undefined;
  return parsePageQuery(query);
}

export function decodePageQuery(value: unknown): PageQuery | undefined {
  if (value === undefined) return undefined;
  return parsePageQuery(value);
}

function parsePageQuery(value: unknown): { readonly after?: string; readonly limit?: number } {
  const raw = record(value, "page query", "invalid_input");
  if (raw.after !== undefined && (typeof raw.after !== "string" || raw.after.trim() === "")) {
    throw new GraphCoderError("invalid_input", "page cursor must be nonempty text");
  }
  if (raw.limit !== undefined && (!Number.isSafeInteger(raw.limit) || (raw.limit as number) < 1 || (raw.limit as number) > 1_024)) {
    throw new GraphCoderError("invalid_input", "page limit must be between 1 and 1024");
  }
  const result: { after?: string; limit?: number } = {};
  if (raw.after !== undefined) result.after = raw.after as string;
  if (raw.limit !== undefined) result.limit = raw.limit as number;
  return result;
}

export function encodeGeneration(generation: bigint, label: string, code: "invalid_input" | "transport" = "invalid_input"): string {
  if (typeof generation !== "bigint" || generation < 0n) {
    throw new GraphCoderError(code, `${label} must be a nonnegative bigint`);
  }
  return generation.toString();
}

export function decodeGeneration(value: unknown, label: string, code: "invalid_input" | "transport" = "transport"): bigint {
  const decimal = decimalText(value, label, code);
  if (!/^(0|[1-9][0-9]*)$/u.test(decimal)) {
    throw new GraphCoderError(code, `${label} must be an unsigned decimal string`);
  }
  try {
    return BigInt(decimal);
  } catch {
    throw new GraphCoderError(code, `${label} is not a valid integer`);
  }
}

export function wirePage<T>(value: { readonly items: readonly T[]; readonly next?: string }, map: (value: T) => unknown): unknown {
  return { items: value.items.map(map), ...(value.next === undefined ? {} : { next: value.next }) };
}

export function wireSessionSummary(value: SessionSummary): unknown {
  return { ...value, id: value.id as string, root_agent_id: value.rootAgentId as string, updated_at: value.updatedAt };
}

export function wireSnapshot(value: SessionSnapshot): unknown {
  return {
    summary: wireSessionSummary(value.summary),
    agents: value.agents.map(wireAgent),
    workspace_generation: value.workspaceGeneration === undefined
      ? null
      : encodeGeneration(value.workspaceGeneration, "workspace generation", "transport"),
  };
}

export function wireAgent(value: AgentSummary): unknown {
  return { ...value, id: value.id as string, parent_id: value.parentId as string | null, children: value.children.map(child => child as string) };
}

export function wireActivity(value: ActivityEvent): unknown {
  return { ...value, sequence: encodeGeneration(value.sequence, "activity sequence", "transport"), actor_id: value.actorId as string | null };
}

export function wireMessage(value: GraphMessage): unknown {
  return { ...value, id: value.id as string, session_id: value.sessionId as string, sender_id: value.senderId as string, recipient_id: value.recipientId as string, delivered_at: value.deliveredAt };
}

export function wireApproval(value: ApprovalRequest): unknown {
  return { ...value, id: value.id as string, session_id: value.sessionId as string, agent_id: value.agentId as string, operation_id: value.operationId, action_digest: value.actionDigest, created_at: value.createdAt };
}

export function wireChangeSummary(value: ChangeSummary): unknown {
  return { ...value, old_path: value.oldPath };
}

export function wireChange(value: ChangeBody, session: SessionSummary["id"]): unknown {
  return { session_id: session as string, path: value.path, unified_diff: value.unifiedDiff, generation: encodeGeneration(value.generation, "change generation", "transport") };
}

export function wireFile(value: FileBody, session: SessionSummary["id"]): unknown {
  return { session_id: session as string, path: value.path, media_type: value.mediaType, bytes: [...value.bytes], generation: encodeGeneration(value.generation, "file generation", "transport") };
}

export function decodeSessionSummary(value: unknown): SessionSummary {
  const raw = record(value, "session summary");
  return { id: sessionId(text(raw.id, "session id")), title: text(raw.title, "session title"), state: oneOf(raw.state, ["idle", "running", "completed", "failed", "cancelled"], "session state"), updatedAt: text(raw.updated_at, "session updated_at"), rootAgentId: agentId(text(raw.root_agent_id, "root agent id")) };
}

export function decodeAgent(value: unknown): AgentSummary {
  const raw = record(value, "agent summary");
  if (!Number.isSafeInteger(raw.depth) || (raw.depth as number) < 0) throw new GraphCoderError("transport", "agent depth is invalid");
  return { id: agentId(text(raw.id, "agent id")), parentId: raw.parent_id === null ? null : agentId(text(raw.parent_id, "agent parent id")), task: text(raw.task, "agent task"), state: oneOf(raw.state, ["queued", "running", "waiting", "completed", "failed", "cancelled"], "agent state"), depth: raw.depth as number, children: array(raw.children, "agent children").map(value => agentId(text(value, "agent child id"))) };
}

export function decodeSnapshot(value: unknown): SessionSnapshot {
  const raw = record(value, "session snapshot");
  return {
    summary: decodeSessionSummary(raw.summary),
    agents: array(raw.agents, "session agents").map(decodeAgent),
    workspaceGeneration: raw.workspace_generation === null
      ? undefined
      : decodeGeneration(raw.workspace_generation, "workspace generation"),
  };
}

export function decodeActivity(value: unknown): ActivityEvent {
  const raw = record(value, "activity event");
  return { sequence: decodeGeneration(raw.sequence, "activity sequence"), id: text(raw.id, "activity id"), kind: oneOf(raw.kind, ["session", "agent", "message", "approval", "workspace", "model"], "activity kind"), actorId: raw.actor_id === null ? null : agentId(text(raw.actor_id, "activity actor id")), text: text(raw.text, "activity text"), at: text(raw.at, "activity timestamp") };
}

export function decodeMessage(value: unknown): GraphMessage {
  const raw = record(value, "message");
  return { id: messageId(text(raw.id, "message id")), sessionId: sessionId(text(raw.session_id, "message session id")), senderId: agentId(text(raw.sender_id, "message sender id")), recipientId: agentId(text(raw.recipient_id, "message recipient id")), body: checkedPublicText(raw.body, "message body", MAX_MESSAGE_BODY_BYTES), deliveredAt: raw.delivered_at === null ? null : text(raw.delivered_at, "message delivery timestamp") };
}

export function decodeApproval(value: unknown): ApprovalRequest {
  const raw = record(value, "approval");
  return { id: approvalId(text(raw.id, "approval id")), sessionId: sessionId(text(raw.session_id, "approval session id")), agentId: agentId(text(raw.agent_id, "approval agent id")), operationId: checkedPublicText(raw.operation_id, "approval operation id", MAX_OPERATION_ID_BYTES), actionDigest: text(raw.action_digest, "approval action digest"), description: text(raw.description, "approval description"), state: oneOf(raw.state, ["pending", "approved", "declined", "cancelled", "expired", "denied"], "approval state"), createdAt: text(raw.created_at, "approval created_at") };
}

export function decodeChangeSummary(value: unknown): ChangeSummary {
  const raw = record(value, "change summary");
  const result: ChangeSummary = { path: text(raw.path, "change path"), kind: oneOf(raw.kind, ["added", "modified", "deleted", "renamed"], "change kind"), additions: checkedCount(raw.additions, "change additions"), deletions: checkedCount(raw.deletions, "change deletions") };
  if (raw.old_path !== undefined) return { ...result, oldPath: text(raw.old_path, "change old path") };
  return result;
}

export function decodeChangeBody(value: unknown, expectedSession: SessionId): ChangeBody {
  const raw = record(value, "change body");
  if (sessionId(text(raw.session_id, "change session id")) !== expectedSession) throw new GraphCoderError("transport", "change response is not bound to its session");
  return { path: text(raw.path, "change path"), unifiedDiff: text(raw.unified_diff, "unified diff"), generation: decodeGeneration(raw.generation, "change generation") };
}

export function decodeFileBody(value: unknown, maximumBytes = 64 * 1024 * 1024, expectedSession?: SessionId): FileBody {
  const raw = record(value, "file body");
  if (expectedSession !== undefined && sessionId(text(raw.session_id, "file session id")) !== expectedSession) throw new GraphCoderError("transport", "file response is not bound to its session");
  const bytes = raw.bytes instanceof Uint8Array ? raw.bytes : array(raw.bytes, "file bytes");
  if (bytes.length > maximumBytes) throw new GraphCoderError("transport", "file body exceeds the configured file size limit");
  if (!bytes.every(byte => Number.isInteger(byte) && (byte as number) >= 0 && (byte as number) <= 255)) throw new GraphCoderError("transport", "file bytes contain an invalid octet");
  return { path: text(raw.path, "file path"), mediaType: text(raw.media_type, "file media type"), bytes: Uint8Array.from(bytes as ArrayLike<number>), generation: decodeGeneration(raw.generation, "file generation") };
}

export function record(value: unknown, label: string, code: "invalid_input" | "transport" = "transport"): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new GraphCoderError(code, `${label} is not an object`);
  return value as Record<string, unknown>;
}

export function array(value: unknown, label: string, code: "invalid_input" | "transport" = "transport"): readonly unknown[] {
  if (!Array.isArray(value)) throw new GraphCoderError(code, `${label} is not an array`);
  return value;
}

export function text(value: unknown, label: string, code: "invalid_input" | "transport" = "transport"): string {
  if (typeof value !== "string") throw new GraphCoderError(code, `${label} is not text`);
  return value;
}

function decimalText(value: unknown, label: string, code: "invalid_input" | "transport"): string {
  if (typeof value !== "string" || value.trim() === "") throw new GraphCoderError(code, `${label} must be nonempty text`);
  return value;
}

function oneOf<T extends string>(value: unknown, choices: readonly T[], label: string): T {
  if (typeof value !== "string" || !choices.includes(value as T)) throw new GraphCoderError("transport", `${label} is invalid`);
  return value as T;
}

function checkedCount(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new GraphCoderError("transport", `${label} is invalid`);
  return value as number;
}
