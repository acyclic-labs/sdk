import {
  GraphCoderError,
  checkedPublicText,
  MAX_MESSAGE_BODY_BYTES,
  MAX_OPERATION_ID_BYTES,
  MAX_PROMPT_BYTES,
  type ActivityEvent,
  type AgentId,
  type AgentSummary,
  type ApprovalId,
  type ApprovalRequest,
  type ChangeBody,
  type ChangeSummary,
  type FileBody,
  type GraphCoderTransport,
  type GraphMessage,
  type MessageId,
  type PageQuery,
  type SessionId,
  type SessionPage,
  type SessionSnapshot,
  type SessionSummary,
  type StartSessionInput,
  type WritebackApproval,
  type WritebackReceipt,
} from "./api.js";

export const MOCK_FIXTURES = ["deterministic"] as const;
export type MockFixture = (typeof MOCK_FIXTURES)[number];

export interface MockCall {
  readonly method: string;
  readonly sessionId?: SessionId;
}

interface MockSession {
  summary: SessionSummary;
  snapshot: SessionSnapshot & { readonly workspaceGeneration: bigint };
  activity: ActivityEvent[];
  messages: GraphMessage[];
  approvals: ApprovalRequest[];
  changes: ChangeSummary[];
  bodies: Map<string, ChangeBody>;
  files: Map<string, FileBody>;
}

export interface MockTransportOptions {
  readonly fixture?: MockFixture;
  readonly now?: () => string;
}

function page<T>(items: readonly T[], query: PageQuery | undefined): { readonly items: readonly T[]; readonly next?: string } {
  const limit = query?.limit ?? 64;
  const start = query?.after === undefined ? 0 : Number(query.after);
  if (!Number.isSafeInteger(start) || start < 0) throw new GraphCoderError("invalid_input", "page cursor is invalid");
  const selected = items.slice(start, start + limit);
  const nextIndex = start + selected.length;
  return nextIndex < items.length ? { items: selected, next: String(nextIndex) } : { items: selected };
}

function nowIso(now: () => string): string {
  const value = now();
  if (Number.isNaN(Date.parse(value))) throw new GraphCoderError("transport", "mock clock returned an invalid timestamp");
  return value;
}

/**
 * Deterministic transport used by terminal tests and examples. It is an
 * explicit fixture; production code must inject a real Harness transport.
 */
export class MockGraphCoderTransport implements GraphCoderTransport {
  readonly calls: MockCall[] = [];
  readonly #fixture: MockFixture;
  readonly #now: () => string;
  readonly #sessions = new Map<SessionId, MockSession>();
  #nextSession = 1;
  #nextMessage = 1;

  constructor(options: MockTransportOptions = {}) {
    this.#fixture = options.fixture ?? "deterministic";
    if (!MOCK_FIXTURES.includes(this.#fixture)) throw new GraphCoderError("invalid_input", `unknown mock fixture ${this.#fixture}`);
    this.#now = options.now ?? (() => "2026-01-01T00:00:00.000Z");
  }

  listSessions(query?: PageQuery): Promise<SessionPage> {
    this.calls.push({ method: "listSessions" });
    return Promise.resolve(page([...this.#sessions.values()].map(session => session.summary), query));
  }

  startSession(input: StartSessionInput): Promise<SessionSnapshot> {
    this.calls.push({ method: "startSession" });
    try { checkedPublicText(input.operationId, "operation id", MAX_OPERATION_ID_BYTES); } catch (error) { return Promise.reject(error); }
    if (input.modelFixture !== undefined && input.modelFixture !== this.#fixture) {
      return Promise.reject(new GraphCoderError("unsupported", `mock fixture ${input.modelFixture} is unavailable`));
    }
    try { checkedPublicText(input.prompt, "session prompt", MAX_PROMPT_BYTES); } catch (error) { return Promise.reject(error); }
    const number = this.#nextSession++;
    const sessionId = `session-${number}` as SessionId;
    const rootAgentId = `agent-${number}-root` as AgentId;
    const at = nowIso(this.#now);
    const summary: SessionSummary = { id: sessionId, title: input.prompt.slice(0, 80), state: "running", updatedAt: at, rootAgentId };
    const root: AgentSummary = { id: rootAgentId, parentId: null, task: input.prompt, state: "running", depth: 0, children: [] };
    const operationId = `mock-writeback-${number}`;
    const approval: ApprovalRequest = {
      id: `approval-${number}` as ApprovalId,
      sessionId,
      agentId: rootAgentId,
      operationId,
      actionDigest: `mock-digest-${number}`,
      description: "Apply the root workspace changes to the source checkout",
      state: "pending",
      createdAt: at,
    };
    const changes: ChangeSummary[] = [{ path: "README.md", kind: "modified", additions: 1, deletions: 0 }];
    const generation = 1n;
    const snapshot: MockSession["snapshot"] = { summary, agents: [root], workspaceGeneration: generation };
    const activity: ActivityEvent[] = [
      { sequence: 1n, id: `activity-${number}-1`, kind: "session", actorId: rootAgentId, text: "session started", at },
      { sequence: 2n, id: `activity-${number}-2`, kind: "model", actorId: rootAgentId, text: `fixture ${this.#fixture} accepted the prompt`, at },
    ];
    const messages: GraphMessage[] = [];
    const bodies = new Map<string, ChangeBody>([["README.md", { path: "README.md", unifiedDiff: "+GraphCoder fixture output\n", generation }]]);
    const files = new Map<string, FileBody>([["README.md", { path: "README.md", mediaType: "text/markdown", bytes: new TextEncoder().encode("# GraphCoder fixture\n"), generation }]]);
    this.#sessions.set(sessionId, { summary, snapshot, activity, messages, approvals: [approval], changes, bodies, files });
    return Promise.resolve(snapshot);
  }

  openSession(sessionId: SessionId): Promise<SessionSnapshot> {
    this.calls.push({ method: "openSession", sessionId });
    return Promise.resolve(this.#session(sessionId).snapshot);
  }

  resumeSession(sessionId: SessionId): Promise<SessionSnapshot> {
    this.calls.push({ method: "resumeSession", sessionId });
    const session = this.#session(sessionId);
    session.summary = { ...session.summary, state: "running", updatedAt: nowIso(this.#now) };
    session.snapshot = { ...session.snapshot, summary: session.summary };
    session.activity.push({ sequence: BigInt(session.activity.length + 1), id: `resume-${session.activity.length + 1}`, kind: "session", actorId: session.summary.rootAgentId, text: "session resumed", at: session.summary.updatedAt });
    return Promise.resolve(session.snapshot);
  }

  readActivity(sessionId: SessionId, query?: PageQuery) {
    this.calls.push({ method: "readActivity", sessionId });
    return Promise.resolve(page(this.#session(sessionId).activity, query));
  }

  readMessages(sessionId: SessionId, query?: PageQuery) {
    this.calls.push({ method: "readMessages", sessionId });
    return Promise.resolve(page(this.#session(sessionId).messages, query));
  }

  sendMessage(input: { readonly sessionId: SessionId; readonly senderId: AgentId; readonly recipientId: AgentId; readonly body: string }): Promise<GraphMessage> {
    this.calls.push({ method: "sendMessage", sessionId: input.sessionId });
    try { checkedPublicText(input.body, "message body", MAX_MESSAGE_BODY_BYTES); } catch (error) { return Promise.reject(error); }
    const session = this.#session(input.sessionId);
    const message: GraphMessage = { id: `message-${this.#nextMessage++}` as MessageId, sessionId: input.sessionId, senderId: input.senderId, recipientId: input.recipientId, body: input.body, deliveredAt: nowIso(this.#now) };
    session.messages.push(message);
    return Promise.resolve(message);
  }

  listApprovals(sessionId: SessionId, query?: PageQuery) {
    this.calls.push({ method: "listApprovals", sessionId });
    return Promise.resolve(page(this.#session(sessionId).approvals, query));
  }

  resolveApproval(input: { readonly approvalId: ApprovalId; readonly approved: boolean; readonly sessionId: SessionId }): Promise<ApprovalRequest> {
    this.calls.push({ method: "resolveApproval" });
    for (const session of this.#sessions.values()) {
      const index = session.approvals.findIndex(approval => approval.id === input.approvalId);
      if (index >= 0) {
        const current = session.approvals[index]!;
        if (input.sessionId !== undefined && input.sessionId !== current.sessionId) return Promise.reject(new GraphCoderError("denied", "approval belongs to another session"));
        const next: ApprovalRequest = { ...current, state: input.approved ? "approved" : "declined" };
        session.approvals[index] = next;
        return Promise.resolve(next);
      }
    }
    return Promise.reject(new GraphCoderError("not_found", "approval was not found"));
  }

  cancelSession(sessionId: SessionId): Promise<SessionSnapshot> {
    this.calls.push({ method: "cancelSession", sessionId });
    const session = this.#session(sessionId);
    session.summary = { ...session.summary, state: "cancelled", updatedAt: nowIso(this.#now) };
    session.snapshot = { ...session.snapshot, summary: session.summary };
    return Promise.resolve(session.snapshot);
  }

  listChanges(sessionId: SessionId) {
    this.calls.push({ method: "listChanges", sessionId });
    const session = this.#session(sessionId);
    return Promise.resolve({ generation: session.snapshot.workspaceGeneration, items: session.changes });
  }

  readChange(sessionId: SessionId, path: string, generation: bigint): Promise<ChangeBody> {
    this.calls.push({ method: "readChange", sessionId });
    const session = this.#session(sessionId);
    if (generation !== session.snapshot.workspaceGeneration) return Promise.reject(new GraphCoderError("stale", "workspace generation changed"));
    const body = session.bodies.get(path);
    if (body === undefined) return Promise.reject(new GraphCoderError("not_found", "change was not found"));
    return Promise.resolve(body);
  }

  readFile(sessionId: SessionId, path: string, generation: bigint): Promise<FileBody> {
    this.calls.push({ method: "readFile", sessionId });
    const session = this.#session(sessionId);
    if (generation !== session.snapshot.workspaceGeneration) return Promise.reject(new GraphCoderError("stale", "workspace generation changed"));
    const body = session.files.get(path);
    if (body === undefined) return Promise.reject(new GraphCoderError("not_found", "file was not found"));
    return Promise.resolve({ ...body, bytes: Uint8Array.from(body.bytes) });
  }

  approveWriteback(input: WritebackApproval): Promise<WritebackReceipt> {
    this.calls.push({ method: "approveWriteback", sessionId: input.sessionId });
    const session = this.#session(input.sessionId);
    if (input.expectedGeneration !== session.snapshot.workspaceGeneration) return Promise.reject(new GraphCoderError("stale", "workspace generation changed"));
    const approval = session.approvals.find(item => item.operationId === input.operationId);
    if (approval === undefined) return Promise.reject(new GraphCoderError("denied", "writeback operation is not pending"));
    if (input.approved && approval.state !== "approved") return Promise.reject(new GraphCoderError("denied", "writeback requires the matching approval"));
    return Promise.resolve({ operationId: input.operationId, sessionId: input.sessionId, generation: input.expectedGeneration, applied: input.approved });
  }

  #session(id: SessionId): MockSession {
    const session = this.#sessions.get(id);
    if (session === undefined) throw new GraphCoderError("not_found", `session ${id} was not found`);
    return session;
  }
}

export function createMockTransport(options: MockTransportOptions = {}): MockGraphCoderTransport {
  return new MockGraphCoderTransport(options);
}
