/**
 * Public, transport-neutral GraphCoder application contracts.
 *
 * The application owns presentation state only. A transport owns durable
 * sessions, agents, messages, approvals, workspaces, and recovery. This
 * boundary is intentionally usable by a terminal, native UI, or another
 * host without importing either presentation toolkit.
 */

export type SessionId = string & { readonly __graphcoderSessionId: unique symbol };
export type AgentId = string & { readonly __graphcoderAgentId: unique symbol };
export type MessageId = string & { readonly __graphcoderMessageId: unique symbol };
export type ApprovalId = string & { readonly __graphcoderApprovalId: unique symbol };

export type SessionState = "idle" | "running" | "completed" | "failed" | "cancelled";
export type AgentState = "queued" | "running" | "waiting" | "completed" | "failed" | "cancelled";
export type ApprovalState = "pending" | "approved" | "declined" | "cancelled" | "expired" | "denied";

export interface SessionSummary {
  readonly id: SessionId;
  readonly title: string;
  readonly state: SessionState;
  readonly updatedAt: string;
  readonly rootAgentId: AgentId;
}

export interface AgentSummary {
  readonly id: AgentId;
  readonly parentId: AgentId | null;
  readonly task: string;
  readonly state: AgentState;
  readonly depth: number;
  readonly children: readonly AgentId[];
}

export interface ActivityEvent {
  readonly sequence: bigint;
  readonly id: string;
  readonly kind: "session" | "agent" | "message" | "approval" | "workspace" | "model";
  readonly actorId: AgentId | null;
  readonly text: string;
  readonly at: string;
}

export interface GraphMessage {
  readonly id: MessageId;
  readonly sessionId: SessionId;
  readonly senderId: AgentId;
  readonly recipientId: AgentId;
  readonly body: string;
  readonly deliveredAt: string | null;
}

export interface ApprovalRequest {
  readonly id: ApprovalId;
  readonly sessionId: SessionId;
  readonly agentId: AgentId;
  readonly operationId: string;
  readonly actionDigest: string;
  readonly description: string;
  readonly state: ApprovalState;
  readonly createdAt: string;
}

export interface ChangeSummary {
  readonly path: string;
  readonly kind: "added" | "modified" | "deleted" | "renamed";
  readonly additions: number;
  readonly deletions: number;
  readonly oldPath?: string;
}

export interface ChangeBody {
  readonly path: string;
  readonly unifiedDiff: string;
  readonly generation: bigint;
}

export interface SessionSnapshot {
  readonly summary: SessionSummary;
  readonly agents: readonly AgentSummary[];
  readonly workspaceGeneration: bigint;
}

export interface PageQuery {
  readonly after?: string;
  readonly limit?: number;
}

export interface SessionPage {
  readonly items: readonly SessionSummary[];
  readonly next?: string;
}

export interface ActivityPage {
  readonly items: readonly ActivityEvent[];
  readonly next?: string;
}

export interface MessagePage {
  readonly items: readonly GraphMessage[];
  readonly next?: string;
}

export interface ApprovalPage {
  readonly items: readonly ApprovalRequest[];
  readonly next?: string;
}

export interface StartSessionInput {
  readonly prompt: string;
  /** A fixture name is explicit so a host cannot silently use a mock model. */
  readonly modelFixture?: string;
}

export interface WritebackApproval {
  readonly sessionId: SessionId;
  readonly operationId: string;
  readonly expectedGeneration: bigint;
  readonly approved: boolean;
}

export interface WritebackReceipt {
  readonly operationId: string;
  readonly sessionId: SessionId;
  readonly generation: bigint;
  readonly applied: boolean;
}

export interface GraphCoderTransport {
  /** Returns summaries only; it must not start workers or hydrate workspaces. */
  listSessions(query?: PageQuery): Promise<SessionPage>;
  startSession(input: StartSessionInput): Promise<SessionSnapshot>;
  openSession(sessionId: SessionId): Promise<SessionSnapshot>;
  resumeSession(sessionId: SessionId): Promise<SessionSnapshot>;
  readActivity(sessionId: SessionId, query?: PageQuery): Promise<ActivityPage>;
  readMessages(sessionId: SessionId, query?: PageQuery): Promise<MessagePage>;
  sendMessage(input: { readonly sessionId: SessionId; readonly senderId: AgentId; readonly recipientId: AgentId; readonly body: string }): Promise<GraphMessage>;
  listApprovals(sessionId: SessionId, query?: PageQuery): Promise<ApprovalPage>;
  resolveApproval(input: { readonly approvalId: ApprovalId; readonly approved: boolean }): Promise<ApprovalRequest>;
  cancelSession(sessionId: SessionId): Promise<SessionSnapshot>;
  listChanges(sessionId: SessionId): Promise<{ readonly generation: bigint; readonly items: readonly ChangeSummary[] }>;
  readChange(sessionId: SessionId, path: string, generation: bigint): Promise<ChangeBody>;
  approveWriteback(input: WritebackApproval): Promise<WritebackReceipt>;
}

export class GraphCoderError extends Error {
  override readonly name = "GraphCoderError";

  constructor(readonly code: "invalid_input" | "not_found" | "stale" | "denied" | "unsupported" | "transport", message: string) {
    super(message);
  }
}

export interface GraphCoderUiState {
  readonly sessions: readonly SessionSummary[];
  readonly sessionsNext: string | undefined;
  readonly selectedSession: SessionSnapshot | undefined;
  readonly activity: readonly ActivityEvent[];
  readonly activityNext: string | undefined;
  readonly messages: readonly GraphMessage[];
  readonly messagesNext: string | undefined;
  readonly approvals: readonly ApprovalRequest[];
  readonly approvalsNext: string | undefined;
  readonly changes: readonly ChangeSummary[];
  readonly changesGeneration: bigint | undefined;
  readonly changeBody: ChangeBody | undefined;
  readonly writeback: WritebackReceipt | undefined;
  readonly pending: boolean;
  readonly error: GraphCoderError | undefined;
}

export type GraphCoderUiCommand =
  | { readonly kind: "list_sessions"; readonly after?: string; readonly limit?: number }
  | { readonly kind: "start_session"; readonly prompt: string; readonly modelFixture?: string }
  | { readonly kind: "open_session"; readonly sessionId: SessionId }
  | { readonly kind: "resume_session"; readonly sessionId: SessionId }
  | { readonly kind: "load_activity"; readonly after?: string; readonly limit?: number }
  | { readonly kind: "load_messages"; readonly after?: string; readonly limit?: number }
  | { readonly kind: "send_message"; readonly senderId: AgentId; readonly recipientId: AgentId; readonly body: string }
  | { readonly kind: "load_approvals"; readonly after?: string; readonly limit?: number }
  | { readonly kind: "resolve_approval"; readonly approvalId: ApprovalId; readonly approved: boolean }
  | { readonly kind: "cancel_session" }
  | { readonly kind: "list_changes" }
  | { readonly kind: "read_change"; readonly path: string; readonly generation?: bigint }
  | { readonly kind: "approve_writeback"; readonly operationId: string; readonly expectedGeneration: bigint; readonly approved: boolean };

const initialState: GraphCoderUiState = {
  sessions: [], sessionsNext: undefined, selectedSession: undefined,
  activity: [], activityNext: undefined, messages: [], messagesNext: undefined,
  approvals: [], approvalsNext: undefined, changes: [], changesGeneration: undefined, changeBody: undefined, writeback: undefined, pending: false, error: undefined,
};

function checkedId(value: string, label: string): string {
  if (value.trim() === "") throw new GraphCoderError("invalid_input", `${label} must not be empty`);
  return value;
}

function checkedPageLimit(limit: number | undefined): number | undefined {
  if (limit === undefined) return undefined;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_024) {
    throw new GraphCoderError("invalid_input", "page limit must be between 1 and 1024");
  }
  return limit;
}

function pageQuery(after: string | undefined, limit: number | undefined): PageQuery {
  const query: { after?: string; limit?: number } = {};
  if (after !== undefined) query.after = after;
  const checked = checkedPageLimit(limit);
  if (checked !== undefined) query.limit = checked;
  return query;
}

/**
 * Generic application state machine. It contains only the currently visible
 * projections and delegates all durable behavior to the injected transport.
 */
export class GraphCoderUi {
  #state: GraphCoderUiState = initialState;
  #queue: Promise<void> = Promise.resolve();

  constructor(readonly transport: GraphCoderTransport) {}

  state(): GraphCoderUiState { return this.#state; }

  async dispatch(command: GraphCoderUiCommand): Promise<GraphCoderUiState> {
    const run = this.#queue.then(() => this.#dispatchOne(command), () => this.#dispatchOne(command));
    this.#queue = run.then(() => undefined, () => undefined);
    await run;
    return this.#state;
  }

  async #dispatchOne(command: GraphCoderUiCommand): Promise<void> {
    this.#state = { ...this.#state, pending: true, error: undefined };
    try {
      await this.#dispatch(command);
      this.#state = { ...this.#state, pending: false };
    } catch (error) {
      const normalized = error instanceof GraphCoderError
        ? error
        : new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
      this.#state = { ...this.#state, pending: false, error: normalized };
      throw normalized;
    }
  }

  async #dispatch(command: GraphCoderUiCommand): Promise<void> {
    switch (command.kind) {
      case "list_sessions": {
        const page = await this.transport.listSessions(pageQuery(command.after, command.limit));
        this.#state = { ...this.#state, sessions: command.after ? [...this.#state.sessions, ...page.items] : page.items, sessionsNext: page.next };
        return;
      }
      case "start_session": {
        if (command.prompt.trim() === "") throw new GraphCoderError("invalid_input", "session prompt must not be empty");
        const input: { prompt: string; modelFixture?: string } = { prompt: command.prompt };
        if (command.modelFixture !== undefined) input.modelFixture = command.modelFixture;
        const snapshot = await this.transport.startSession(input);
        this.#select(snapshot);
        return;
      }
      case "open_session":
        this.#select(await this.transport.openSession(checkedId(command.sessionId, "session id") as SessionId));
        return;
      case "resume_session":
        this.#select(await this.transport.resumeSession(checkedId(command.sessionId, "session id") as SessionId));
        return;
      case "load_activity": {
        const session = this.#requireSelected();
        const page = await this.transport.readActivity(session.summary.id, pageQuery(command.after, command.limit));
        this.#state = { ...this.#state, activity: command.after ? [...this.#state.activity, ...page.items] : page.items, activityNext: page.next };
        return;
      }
      case "load_messages": {
        const session = this.#requireSelected();
        const page = await this.transport.readMessages(session.summary.id, pageQuery(command.after, command.limit));
        this.#state = { ...this.#state, messages: command.after ? [...this.#state.messages, ...page.items] : page.items, messagesNext: page.next };
        return;
      }
      case "send_message": {
        const session = this.#requireSelected();
        if (command.body.trim() === "") throw new GraphCoderError("invalid_input", "message body must not be empty");
        const message = await this.transport.sendMessage({ sessionId: session.summary.id, senderId: command.senderId, recipientId: command.recipientId, body: command.body });
        this.#state = { ...this.#state, messages: [...this.#state.messages, message] };
        return;
      }
      case "load_approvals": {
        const session = this.#requireSelected();
        const page = await this.transport.listApprovals(session.summary.id, pageQuery(command.after, command.limit));
        this.#state = { ...this.#state, approvals: command.after ? [...this.#state.approvals, ...page.items] : page.items, approvalsNext: page.next };
        return;
      }
      case "resolve_approval": {
        const approval = await this.transport.resolveApproval({ approvalId: checkedId(command.approvalId, "approval id") as ApprovalId, approved: command.approved });
        this.#state = { ...this.#state, approvals: this.#state.approvals.map(item => item.id === approval.id ? approval : item) };
        return;
      }
      case "cancel_session": {
        const session = this.#requireSelected();
        this.#select(await this.transport.cancelSession(session.summary.id));
        return;
      }
      case "list_changes": {
        const session = this.#requireSelected();
        const changes = await this.transport.listChanges(session.summary.id);
        this.#state = { ...this.#state, changes: changes.items, changesGeneration: changes.generation, changeBody: undefined };
        return;
      }
      case "read_change": {
        const session = this.#requireSelected();
        const generation = command.generation ?? this.#state.changesGeneration;
        if (generation === undefined) throw new GraphCoderError("invalid_input", "load changes before reading a diff");
        const changeBody = await this.transport.readChange(session.summary.id, checkedId(command.path, "change path"), generation);
        this.#state = { ...this.#state, changeBody };
        return;
      }
      case "approve_writeback": {
        const session = this.#requireSelected();
        const receipt = await this.transport.approveWriteback({ sessionId: session.summary.id, operationId: checkedId(command.operationId, "operation id"), expectedGeneration: command.expectedGeneration, approved: command.approved });
        this.#state = { ...this.#state, writeback: receipt };
        return;
      }
      default: return assertNever(command);
    }
  }

  #requireSelected(): SessionSnapshot {
    if (this.#state.selectedSession === undefined) throw new GraphCoderError("invalid_input", "select a session first");
    return this.#state.selectedSession;
  }

  #select(snapshot: SessionSnapshot): void {
    this.#state = { ...this.#state, selectedSession: snapshot, activity: [], activityNext: undefined, messages: [], messagesNext: undefined, approvals: [], approvalsNext: undefined, changes: [], changesGeneration: undefined, changeBody: undefined, writeback: undefined };
  }
}

function assertNever(value: never): never { throw new GraphCoderError("invalid_input", `unknown command ${(value as { kind?: string }).kind ?? ""}`); }

export function sessionId(value: string): SessionId { return checkedId(value, "session id") as SessionId; }
export function agentId(value: string): AgentId { return checkedId(value, "agent id") as AgentId; }
export function messageId(value: string): MessageId { return checkedId(value, "message id") as MessageId; }
export function approvalId(value: string): ApprovalId { return checkedId(value, "approval id") as ApprovalId; }
