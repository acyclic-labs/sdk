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

export interface FileBody {
  readonly path: string;
  readonly mediaType: string;
  readonly bytes: Uint8Array;
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
  /** Stable caller-owned identity used to recover a committed turn after reconnect. */
  readonly operationId: string;
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
  resolveApproval(input: { readonly approvalId: ApprovalId; readonly approved: boolean; readonly sessionId: SessionId }): Promise<ApprovalRequest>;
  cancelSession(sessionId: SessionId): Promise<SessionSnapshot>;
  listChanges(sessionId: SessionId): Promise<{ readonly generation: bigint; readonly items: readonly ChangeSummary[] }>;
  readChange(sessionId: SessionId, path: string, generation: bigint): Promise<ChangeBody>;
  /** Reads one body on demand; implementations must not hydrate a workspace page eagerly. */
  readFile(sessionId: SessionId, path: string, generation: bigint): Promise<FileBody>;
  approveWriteback(input: WritebackApproval): Promise<WritebackReceipt>;
}

/** Public text ceilings keep direct native and terminal callers bounded. */
export const MAX_PROMPT_BYTES = 64 * 1024;
export const MAX_MESSAGE_BODY_BYTES = 64 * 1024;
export const MAX_OPERATION_ID_BYTES = 256;

export function checkedPublicText(value: unknown, label: string, maximumBytes: number): string {
  if (typeof value !== "string" || value.trim() === "") throw new GraphCoderError("invalid_input", `${label} must be nonempty text`);
  if (new TextEncoder().encode(value).byteLength > maximumBytes) throw new GraphCoderError("invalid_input", `${label} exceeds its UTF-8 byte limit`);
  return value;
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
  readonly fileBody: FileBody | undefined;
  readonly writeback: WritebackReceipt | undefined;
  readonly pending: boolean;
  readonly error: GraphCoderError | undefined;
}

export type GraphCoderUiCommand =
  | { readonly kind: "list_sessions"; readonly after?: string; readonly limit?: number }
  | { readonly kind: "start_session"; readonly prompt: string; readonly operationId: string; readonly modelFixture?: string }
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
  | { readonly kind: "read_file"; readonly path: string; readonly generation?: bigint }
  | { readonly kind: "approve_writeback"; readonly operationId: string; readonly expectedGeneration: bigint; readonly approved: boolean };

const initialState: GraphCoderUiState = {
  sessions: [], sessionsNext: undefined, selectedSession: undefined,
  activity: [], activityNext: undefined, messages: [], messagesNext: undefined,
  approvals: [], approvalsNext: undefined, changes: [], changesGeneration: undefined, changeBody: undefined, fileBody: undefined, writeback: undefined, pending: false, error: undefined,
};

function checkedId(value: string, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new GraphCoderError("invalid_input", `${label} must not be empty`);
  if (new TextEncoder().encode(value).byteLength > MAX_OPERATION_ID_BYTES) throw new GraphCoderError("invalid_input", `${label} must be at most 256 UTF-8 bytes`);
  return value;
}

export function checkedPath(value: string): string {
  const bytes = new TextEncoder().encode(value);
  if (bytes.byteLength === 0 || bytes.byteLength > 4_096) throw new GraphCoderError("invalid_input", "path must be between 1 and 4096 UTF-8 bytes");
  if (value.includes("\\") || value.startsWith("/") || /^[A-Za-z]:/u.test(value) || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new GraphCoderError("invalid_input", "path must be a relative slash-separated path");
  }
  if (value.split("/").some(segment => segment === "" || segment === "." || segment === "..")) {
    throw new GraphCoderError("invalid_input", "path contains an empty or traversal segment");
  }
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
  #queueGeneration = 0;
  #commandEpoch = 0;

  constructor(readonly transport: GraphCoderTransport) {}

  state(): GraphCoderUiState { return snapshotState(this.#state); }

  async dispatch(command: GraphCoderUiCommand): Promise<GraphCoderUiState> {
    // Cancellation must be able to reach the durable owner while a history or
    // model request is waiting. It is deliberately the only command that may
    // bypass the presentation queue; stale work is fenced below.
    if (command.kind === "cancel_session") return this.#cancelNow();
    const queueGeneration = this.#queueGeneration;
    const run = this.#queue.then(() => {
      if (queueGeneration !== this.#queueGeneration) return;
      return this.#dispatchOne(command);
    }, () => {
      if (queueGeneration !== this.#queueGeneration) return;
      return this.#dispatchOne(command);
    });
    this.#queue = run.then(() => undefined, () => undefined);
    await run;
    // Never hand the private projection graph to a host.  In particular,
    // dispatch callers must receive the same detached snapshot as state().
    return this.state();
  }

  async #cancelNow(): Promise<GraphCoderUiState> {
    const session = this.#requireSelected();
    const epoch = ++this.#commandEpoch;
    // Detach queued presentation work immediately. The durable owner still
    // receives the cancellation below, while commands submitted after the
    // cancellation are never chained behind a request that may not resolve.
    this.#queueGeneration += 1;
    this.#queue = Promise.resolve();
    this.#state = { ...this.#state, pending: true, error: undefined };
    try {
      const snapshot = await this.transport.cancelSession(session.summary.id);
      if (epoch !== this.#commandEpoch) return this.state();
      this.#select(snapshot);
      this.#state = { ...this.#state, pending: false };
      return this.state();
    } catch (error) {
      const normalized = error instanceof GraphCoderError
        ? error
        : new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
      if (epoch !== this.#commandEpoch) return this.state();
      this.#state = { ...this.#state, pending: false, error: normalized };
      throw normalized;
    }
  }

  async #dispatchOne(command: GraphCoderUiCommand): Promise<void> {
    const epoch = ++this.#commandEpoch;
    this.#state = { ...this.#state, pending: true, error: undefined };
    try {
      await this.#dispatch(command, epoch);
      if (epoch !== this.#commandEpoch) {
        // A cancelled request may finish later. Its projection is stale and
        // must never overwrite the cancellation or a newer command.
        return;
      }
      this.#state = { ...this.#state, pending: false };
    } catch (error) {
      if (epoch !== this.#commandEpoch) {
        return;
      }
      const normalized = error instanceof GraphCoderError
        ? error
        : new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
      this.#state = { ...this.#state, pending: false, error: normalized };
      throw normalized;
    }
  }

  async #dispatch(command: GraphCoderUiCommand, epoch: number): Promise<void> {
    switch (command.kind) {
      case "list_sessions": {
        const page = await this.transport.listSessions(pageQuery(command.after, command.limit));
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, sessions: command.after ? [...this.#state.sessions, ...page.items] : page.items, sessionsNext: page.next };
        return;
      }
      case "start_session": {
        const input: { prompt: string; operationId: string; modelFixture?: string } = { prompt: checkedPublicText(command.prompt, "session prompt", MAX_PROMPT_BYTES), operationId: checkedPublicText(command.operationId, "operation id", MAX_OPERATION_ID_BYTES) };
        if (command.modelFixture !== undefined) input.modelFixture = command.modelFixture;
        const snapshot = await this.transport.startSession(input);
        if (epoch !== this.#commandEpoch) return;
        this.#select(snapshot);
        return;
      }
      case "open_session": {
        const snapshot = await this.transport.openSession(checkedId(command.sessionId, "session id") as SessionId);
        if (epoch !== this.#commandEpoch) return;
        this.#select(snapshot);
        return;
      }
      case "resume_session": {
        const snapshot = await this.transport.resumeSession(checkedId(command.sessionId, "session id") as SessionId);
        if (epoch !== this.#commandEpoch) return;
        this.#select(snapshot);
        return;
      }
      case "load_activity": {
        const session = this.#requireSelected();
        const page = await this.transport.readActivity(session.summary.id, pageQuery(command.after, command.limit));
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, activity: command.after ? [...this.#state.activity, ...page.items] : page.items, activityNext: page.next };
        return;
      }
      case "load_messages": {
        const session = this.#requireSelected();
        const page = await this.transport.readMessages(session.summary.id, pageQuery(command.after, command.limit));
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, messages: command.after ? [...this.#state.messages, ...page.items] : page.items, messagesNext: page.next };
        return;
      }
      case "send_message": {
        const session = this.#requireSelected();
        checkedPublicText(command.body, "message body", MAX_MESSAGE_BODY_BYTES);
        const message = await this.transport.sendMessage({ sessionId: session.summary.id, senderId: command.senderId, recipientId: command.recipientId, body: command.body });
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, messages: [...this.#state.messages, message] };
        return;
      }
      case "load_approvals": {
        const session = this.#requireSelected();
        const page = await this.transport.listApprovals(session.summary.id, pageQuery(command.after, command.limit));
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, approvals: command.after ? [...this.#state.approvals, ...page.items] : page.items, approvalsNext: page.next };
        return;
      }
      case "resolve_approval": {
        const session = this.#requireSelected();
        const approval = await this.transport.resolveApproval({ approvalId: checkedId(command.approvalId, "approval id") as ApprovalId, approved: command.approved, sessionId: session.summary.id });
        if (epoch !== this.#commandEpoch) return;
        if (approval.sessionId !== session.summary.id) throw new GraphCoderError("transport", "approval response is not bound to the selected session");
        this.#state = { ...this.#state, approvals: this.#state.approvals.map(item => item.id === approval.id ? approval : item) };
        return;
      }
      case "cancel_session": {
        const session = this.#requireSelected();
        const snapshot = await this.transport.cancelSession(session.summary.id);
        if (epoch !== this.#commandEpoch) return;
        this.#select(snapshot);
        return;
      }
      case "list_changes": {
        const session = this.#requireSelected();
        const changes = await this.transport.listChanges(session.summary.id);
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, changes: changes.items, changesGeneration: changes.generation, changeBody: undefined, fileBody: undefined };
        return;
      }
      case "read_change": {
        const session = this.#requireSelected();
        const generation = command.generation ?? this.#state.changesGeneration;
        if (generation === undefined) throw new GraphCoderError("invalid_input", "load changes before reading a diff");
        const changeBody = await this.transport.readChange(session.summary.id, checkedPath(command.path), generation);
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, changeBody };
        return;
      }
      case "read_file": {
        const session = this.#requireSelected();
        const generation = command.generation ?? this.#state.changesGeneration;
        if (generation === undefined) throw new GraphCoderError("invalid_input", "load changes before reading a file");
        const fileBody = await this.transport.readFile(session.summary.id, checkedPath(command.path), generation);
        if (epoch !== this.#commandEpoch) return;
        this.#state = { ...this.#state, fileBody };
        return;
      }
      case "approve_writeback": {
        const session = this.#requireSelected();
        if (typeof command.expectedGeneration !== "bigint" || command.expectedGeneration < 0n) {
          throw new GraphCoderError("invalid_input", "writeback generation must be a nonnegative bigint");
        }
        const receipt = await this.transport.approveWriteback({ sessionId: session.summary.id, operationId: checkedPublicText(command.operationId, "operation id", MAX_OPERATION_ID_BYTES), expectedGeneration: command.expectedGeneration, approved: command.approved });
        if (epoch !== this.#commandEpoch) return;
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
    this.#state = { ...this.#state, selectedSession: snapshot, activity: [], activityNext: undefined, messages: [], messagesNext: undefined, approvals: [], approvalsNext: undefined, changes: [], changesGeneration: undefined, changeBody: undefined, fileBody: undefined, writeback: undefined };
  }
}

function assertNever(value: never): never { throw new GraphCoderError("invalid_input", `unknown command ${(value as { kind?: string }).kind ?? ""}`); }

function snapshotState(state: GraphCoderUiState): GraphCoderUiState {
  return Object.freeze({
    sessions: Object.freeze(state.sessions.map(cloneSessionSummary)),
    sessionsNext: state.sessionsNext,
    selectedSession: state.selectedSession === undefined ? undefined : cloneSessionSnapshot(state.selectedSession),
    activity: Object.freeze(state.activity.map(cloneActivityEvent)),
    activityNext: state.activityNext,
    messages: Object.freeze(state.messages.map(cloneMessage)),
    messagesNext: state.messagesNext,
    approvals: Object.freeze(state.approvals.map(cloneApproval)),
    approvalsNext: state.approvalsNext,
    changes: Object.freeze(state.changes.map(cloneChangeSummary)),
    changesGeneration: state.changesGeneration,
    changeBody: state.changeBody === undefined ? undefined : cloneChangeBody(state.changeBody),
    fileBody: state.fileBody === undefined ? undefined : cloneFileBody(state.fileBody),
    writeback: state.writeback === undefined ? undefined : cloneWritebackReceipt(state.writeback),
    pending: state.pending,
    error: state.error === undefined ? undefined : new GraphCoderError(state.error.code, state.error.message),
  });
}

function cloneSessionSummary(value: SessionSummary): SessionSummary {
  return Object.freeze({ ...value });
}

function cloneAgentSummary(value: AgentSummary): AgentSummary {
  return Object.freeze({ ...value, children: Object.freeze([...value.children]) });
}

function cloneSessionSnapshot(value: SessionSnapshot): SessionSnapshot {
  return Object.freeze({
    summary: cloneSessionSummary(value.summary),
    agents: Object.freeze(value.agents.map(cloneAgentSummary)),
    workspaceGeneration: value.workspaceGeneration,
  });
}

function cloneActivityEvent(value: ActivityEvent): ActivityEvent {
  return Object.freeze({ ...value });
}

function cloneMessage(value: GraphMessage): GraphMessage {
  return Object.freeze({ ...value });
}

function cloneApproval(value: ApprovalRequest): ApprovalRequest {
  return Object.freeze({ ...value });
}

function cloneChangeSummary(value: ChangeSummary): ChangeSummary {
  return Object.freeze({ ...value });
}

function cloneChangeBody(value: ChangeBody): ChangeBody {
  return Object.freeze({ ...value });
}

function cloneFileBody(value: FileBody): FileBody {
  // Uint8Array instances cannot be frozen on all supported runtimes.  A
  // detached copy still prevents mutation of a returned snapshot from
  // changing the UI's private projection or a later snapshot.
  return Object.freeze({ ...value, bytes: Uint8Array.from(value.bytes) });
}

function cloneWritebackReceipt(value: WritebackReceipt): WritebackReceipt {
  return Object.freeze({ ...value });
}

export function sessionId(value: string): SessionId { return checkedId(value, "session id") as SessionId; }
export function agentId(value: string): AgentId { return checkedId(value, "agent id") as AgentId; }
export function messageId(value: string): MessageId { return checkedId(value, "message id") as MessageId; }
export function approvalId(value: string): ApprovalId { return checkedId(value, "approval id") as ApprovalId; }
