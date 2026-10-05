import { describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import { GraphCoderError, GraphCoderUi, agentId, approvalId, sessionId, type SessionId } from "../src/api.js";
import { BridgeGraphCoderTransport, checkedRequestId, type GraphCoderWireRequest, type GraphCoderWireResponse } from "../src/bridge.js";
import { createMockTransport } from "../src/mock.js";
import { GraphCoderTerminal, runCli, runCliWithTransport } from "../src/terminal.js";

function writable(): { readonly stream: NodeJS.WritableStream; readonly lines: () => Array<{ readonly ok?: boolean; readonly [key: string]: unknown }> } {
  const values: string[] = [];
  return {
    stream: { write(value: string): boolean { values.push(value); return true; } } as unknown as NodeJS.WritableStream,
    lines: () => values.filter(value => value.trim() !== "").map(value => JSON.parse(value) as { readonly ok?: boolean; readonly [key: string]: unknown }),
  };
}

describe("GraphCoder UI transport boundary", () => {
  test("durable bridge preserves explicit operations and decodes pinned generations", async () => {
    const requests: GraphCoderWireRequest[] = [];
    const bridge = {
      request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> {
        requests.push(request);
        const response = (result: unknown): GraphCoderWireResponse => ({ request_id: request.request_id, ok: true, result });
        switch (request.method) {
          case "list_sessions": return Promise.resolve(response({ items: [{ id: "session-1", title: "inspect", state: "running", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" }] }));
          case "list_changes": return Promise.resolve(response({ session_id: "session-1", generation: "7", items: [{ path: "README.md", kind: "modified", additions: 1, deletions: 0 }] }));
          case "read_change": return Promise.resolve(response({ session_id: "session-1", path: "README.md", unified_diff: "@@ -1 +1 @@", generation: "7" }));
          case "read_file": return Promise.resolve(response({ session_id: "session-1", path: "README.md", media_type: "text/markdown", bytes: new Uint8Array([72, 105]), generation: "7" }));
          default: return Promise.resolve({ request_id: request.request_id, ok: false, error: { code: "unsupported", message: `fixture does not implement ${request.method}` } });
        }
      },
    };
    const transport = new BridgeGraphCoderTransport(bridge);
    const page = await transport.listSessions({ limit: 5 });
    expect(page.items[0]?.id).toBe(sessionId("session-1"));
    const changes = await transport.listChanges(sessionId("session-1"));
    expect(changes.generation).toBe(7n);
    expect((await transport.readChange(sessionId("session-1"), "README.md", 7n)).generation).toBe(7n);
    expect((await transport.readFile(sessionId("session-1"), "README.md", 7n)).bytes).toEqual(new Uint8Array([72, 105]));
    expect(requests.map(request => request.method)).toEqual(["list_sessions", "list_changes", "read_change", "read_file"]);
    expect(requests[0]?.params).toEqual({ query: { limit: 5 } });
    expect(requests[2]?.params).toEqual({ session_id: "session-1", path: "README.md", generation: "7" });
  });

  test("start requests carry a caller-owned stable operation identity", async () => {
    const requests: GraphCoderWireRequest[] = [];
    const bridge = {
      request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> {
        requests.push(request);
        return Promise.resolve({
          request_id: request.request_id,
          ok: true,
          result: {
            summary: { id: "session-1", title: "inspect", state: "completed", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" },
            agents: [],
            workspace_generation: null,
          },
        });
      },
    };
    const transport = new BridgeGraphCoderTransport(bridge);
    const snapshot = await transport.startSession({ prompt: "inspect", operationId: "op-stable-1", modelFixture: "stage" });
    expect(snapshot.workspaceGeneration).toBeUndefined();
    expect(requests[0]?.params).toEqual({ prompt: "inspect", operation_id: "op-stable-1", model_fixture: "stage" });
  });

  test("input requests target the selected session and carry a distinct operation identity", async () => {
    const requests: GraphCoderWireRequest[] = [];
    const bridge = {
      request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> {
        requests.push(request);
        return Promise.resolve({
          request_id: request.request_id,
          ok: true,
          result: {
            summary: { id: "session-1", title: "inspect", state: "completed", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" },
            agents: [],
            workspace_generation: null,
          },
        });
      },
    };
    const transport = new BridgeGraphCoderTransport(bridge);
    const snapshot = await transport.inputSession({ sessionId: sessionId("session-1"), prompt: "follow-up  with exact bytes", operationId: "op-stable-2" });
    expect(snapshot.summary.id).toBe(sessionId("session-1"));
    expect(requests[0]?.method).toBe("input_session");
    expect(requests[0]?.params).toEqual({ session_id: "session-1", prompt: "follow-up  with exact bytes", operation_id: "op-stable-2" });
  });

  test("preserves unknown activity and approval metadata as null", async () => {
    const bridge = {
      request: async (request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> => {
        const result = request.method === "read_activity"
          ? { session_id: "session-1", items: [{ sequence: "1", id: "event-1", kind: "model", actor_id: null, text: "model event", at: null }] }
          : { session_id: "session-1", items: [{ id: "approval-1", session_id: "session-1", agent_id: "agent-1", operation_id: "op-1", action_digest: "digest", description: null, state: "pending", created_at: null }] };
        return { request_id: request.request_id, ok: true, result };
      },
    };
    const transport = new BridgeGraphCoderTransport(bridge);
    const activity = await transport.readActivity(sessionId("session-1"));
    const approvals = await transport.listApprovals(sessionId("session-1"));
    expect(activity.items[0]?.at).toBeNull();
    expect(approvals.items[0]?.description).toBeNull();
    expect(approvals.items[0]?.createdAt).toBeNull();
  });

  test("does not treat absent workspace generation as generation zero", async () => {
    let reads = 0;
    const transport = {
      startSession: async () => ({
        summary: { id: sessionId("session-1"), title: "inspect", state: "running", updatedAt: "2026-01-01T00:00:00.000Z", rootAgentId: agentId("agent-1") },
        agents: [],
        workspaceGeneration: undefined,
      }),
      inputSession: async input => ({
        summary: { id: input.sessionId, title: "inspect", state: "completed", updatedAt: "2026-01-01T00:00:00.000Z", rootAgentId: agentId("agent-1") },
        agents: [],
        workspaceGeneration: undefined,
      }),
      readFile: async () => {
        reads += 1;
        throw new Error("read_file should not be dispatched without a generation");
      },
    } as unknown as import("../src/api.js").GraphCoderTransport;
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-no-generation", prompt: "inspect" });
    await expect(ui.dispatch({ kind: "read_file", path: "README.md" })).rejects.toMatchObject({ code: "invalid_input" });
    expect(reads).toBe(0);
  });

  test("public prompt, message, and operation inputs enforce UTF-8 byte ceilings", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await expect(ui.dispatch({ kind: "start_session", operationId: "op-bounds", prompt: "x".repeat(64 * 1024 + 1) })).rejects.toMatchObject({ code: "invalid_input" });
    await expect(ui.dispatch({ kind: "start_session", operationId: "é".repeat(129), prompt: "inspect" })).rejects.toMatchObject({ code: "invalid_input" });
    await ui.dispatch({ kind: "start_session", operationId: "op-bounds-2", prompt: "inspect" });
    await expect(ui.dispatch({ kind: "send_message", senderId: agentId("a"), recipientId: agentId("b"), body: "x".repeat(64 * 1024 + 1) })).rejects.toMatchObject({ code: "invalid_input" });
  });

  test("bridge rejects mismatched response identity and preserves typed backend errors", async () => {
    const mismatched = new BridgeGraphCoderTransport({ request: async () => ({ request_id: "wrong", ok: true, result: { items: [] } }) });
    await expect(mismatched.listSessions()).rejects.toMatchObject({ code: "transport" });
    const denied = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: false, error: { code: "denied", message: "root approval required" } }) });
    await expect(denied.listSessions()).rejects.toMatchObject({ code: "denied", message: "root approval required" });
  });

  test("bridge rejects malformed pages, invalid bounds, bad bytes, and oversized envelopes", async () => {
    const malformedPage = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: {} }) });
    await expect(malformedPage.listSessions()).rejects.toMatchObject({ code: "transport" });
    await expect(malformedPage.listSessions({ limit: 0 })).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), "README.md", -1n)).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), 7 as unknown as string, 1n)).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), "../secret", 1n)).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), "README.md", 1 as unknown as bigint)).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), "C:\\secret", 1n)).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), "a//b", 1n)).rejects.toMatchObject({ code: "invalid_input" });
    await expect(malformedPage.readFile(sessionId("session-1"), "x".repeat(4_097), 1n)).rejects.toMatchObject({ code: "invalid_input" });
    const malformedFile = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { path: "README.md", media_type: "text/markdown", bytes: [256], generation: "1" } }) });
    await expect(malformedFile.readFile(sessionId("session-1"), "README.md", 1n)).rejects.toMatchObject({ code: "transport" });
    const oversized = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { items: [{ id: "session-1", title: "x".repeat(128), state: "running", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" }] } }) }, "test", 256);
    await expect(oversized.listSessions()).rejects.toMatchObject({ code: "transport" });
  });

  test("bridge uses the shared UTF-8 identifier and decimal generation checks", async () => {
    const oversizedId = "é".repeat(129);
    expect(() => checkedRequestId(oversizedId)).toThrow(/UTF-8 byte limit/u);

    const malformedSummary = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { items: [{ id: oversizedId, title: "inspect", state: "running", updated_at: "0", root_agent_id: "agent-1" }] } }) });
    await expect(malformedSummary.listSessions()).rejects.toMatchObject({ code: "invalid_input" });

    const malformedGeneration = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { session_id: "session-1", generation: "01", items: [] } }) });
    await expect(malformedGeneration.listChanges(sessionId("session-1"))).rejects.toMatchObject({ code: "transport" });
  });

  test("bridge rejects response bindings that do not match the requested session, path, generation, or receipt", async () => {
    const snapshot = { summary: { id: "other", title: "inspect", state: "running", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" }, agents: [], workspace_generation: "3" };
    const wrongSession = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: snapshot }) });
    await expect(wrongSession.openSession(sessionId("session-1"))).rejects.toMatchObject({ code: "transport" });

    const wrongBody = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { session_id: "other", path: "README.md", unified_diff: "", generation: "7" } }) });
    await expect(wrongBody.readChange(sessionId("session-1"), "README.md", 7n)).rejects.toMatchObject({ code: "transport" });

    const wrongApproval = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { id: "approval-1", session_id: "other", agent_id: "agent-1", operation_id: "op-1", action_digest: "digest", description: "approve", state: "approved", created_at: "2026-01-01T00:00:00.000Z" } }) });
    await expect(wrongApproval.resolveApproval({ approvalId: approvalId("approval-1"), sessionId: sessionId("session-1"), approved: true })).rejects.toMatchObject({ code: "transport" });

    const wrongReceipt = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { operation_id: "other", session_id: "session-1", generation: "7", applied: "yes" } }) });
    await expect(wrongReceipt.approveWriteback({ sessionId: sessionId("session-1"), operationId: "op-1", expectedGeneration: 7n, approved: true })).rejects.toMatchObject({ code: "transport" });
  });

  test("bridge binds approval and cancellation arguments without local side effects", async () => {
    const requests: GraphCoderWireRequest[] = [];
    const bridge = {
      request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> {
        requests.push(request);
        if (request.method === "approve_writeback") return Promise.resolve({ request_id: request.request_id, ok: true, result: { operation_id: "op-7", session_id: "session-1", generation: "3", applied: true } });
        if (request.method === "resolve_approval") return Promise.resolve({ request_id: request.request_id, ok: true, result: { id: "approval-1", session_id: "session-1", agent_id: "agent-1", operation_id: "op-7", action_digest: "digest", description: "approve", state: "approved", created_at: "2026-01-01T00:00:00.000Z" } });
        return Promise.resolve({ request_id: request.request_id, ok: true, result: { summary: { id: "session-1", title: "inspect", state: "cancelled", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" }, agents: [], workspace_generation: "3" } });
      },
    };
    const transport = new BridgeGraphCoderTransport(bridge);
    const resolved = await transport.resolveApproval({ approvalId: approvalId("approval-1"), sessionId: sessionId("session-1"), approved: true });
    expect(resolved.id).toBe(approvalId("approval-1"));
    const receipt = await transport.approveWriteback({ sessionId: sessionId("session-1"), operationId: "op-7", expectedGeneration: 3n, approved: true });
    expect(receipt).toMatchObject({ operationId: "op-7", sessionId: sessionId("session-1"), generation: 3n, applied: true });
    const cancelled = await transport.cancelSession(sessionId("session-1"));
    expect(cancelled.summary.state).toBe("cancelled");
    expect(requests[0]?.params).toEqual({ approval_id: "approval-1", session_id: "session-1", approved: true });
    expect(requests[1]?.params).toEqual({ session_id: "session-1", operation_id: "op-7", expected_generation: "3", approved: true });
    expect(requests[2]?.params).toEqual({ session_id: "session-1" });
  });

  test("session listing is summary-only and does not start workers or hydrate workspace", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);

    await ui.dispatch({ kind: "list_sessions", limit: 10 });

    expect(transport.calls.map(call => call.method)).toEqual(["listSessions"]);
    expect(ui.state().selectedSession).toBeUndefined();
    expect(ui.state().activity).toEqual([]);
    expect(Object.isFrozen(ui.state())).toBe(true);
    expect(Object.isFrozen(ui.state().sessions)).toBe(true);
  });

  test("loads history, messages, approvals, and diff bodies only when requested", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-inspect-1", prompt: "inspect the repository" });
    const id = ui.state().selectedSession!.summary.id;

    expect(transport.calls.map(call => call.method)).toEqual(["startSession"]);
    await ui.dispatch({ kind: "read_file", path: "README.md" });
    expect(ui.state().fileBody?.mediaType).toBe("text/markdown");
    expect(ui.state().fileBody?.generation).toBe(ui.state().selectedSession!.workspaceGeneration);
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "readFile"]);
    await ui.dispatch({ kind: "load_activity" });
    await ui.dispatch({ kind: "load_messages" });
    await ui.dispatch({ kind: "load_approvals" });
    await ui.dispatch({ kind: "list_changes" });
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "readFile", "readActivity", "readMessages", "listApprovals", "listChanges"]);
    expect(ui.state().changeBody).toBeUndefined();

    await ui.dispatch({ kind: "read_change", path: "README.md" });
    expect(ui.state().changeBody?.path).toBe("README.md");
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "readFile", "readActivity", "readMessages", "listApprovals", "listChanges", "readChange"]);
    expect(id as string).toBe("session-1");

    await ui.dispatch({ kind: "read_file", path: "README.md" });
    expect(ui.state().fileBody?.mediaType).toBe("text/markdown");
    expect(transport.calls.map(call => call.method).at(-1)).toBe("readFile");
  });

  test("requires an explicit matching approval before root writeback", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-edit-1", prompt: "edit the README" });
    await ui.dispatch({ kind: "load_approvals" });
    const approval = ui.state().approvals[0]!;
    await expect(ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 1n, approved: true })).rejects.toMatchObject({ code: "denied" });
    await ui.dispatch({ kind: "resolve_approval", approvalId: approval.id, approved: true });
    await expect(ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 2n, approved: true })).rejects.toMatchObject({ code: "stale" });
    await ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 1n, approved: true });
    expect(transport.calls.at(-1)?.method).toBe("approveWriteback");
    expect(ui.state().writeback).toMatchObject({ applied: true, generation: 1n });
  });

  test("sends host operator approval before the public resolution", async () => {
    const transport = createMockTransport();
    const order: string[] = [];
    const resolveApproval = transport.resolveApproval.bind(transport);
    const operatorTransport = transport as typeof transport & {
      operatorApprove: (input: { readonly approvalId: ReturnType<typeof approvalId>; readonly approved: boolean; readonly sessionId: ReturnType<typeof sessionId> }) => Promise<void>;
    };
    operatorTransport.operatorApprove = async () => { order.push("operator"); };
    transport.resolveApproval = async input => {
      order.push("resolve");
      return await resolveApproval(input);
    };
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-operator-1", prompt: "edit the README" });
    await ui.dispatch({ kind: "load_approvals" });
    const approval = ui.state().approvals[0]!;
    await ui.dispatch({ kind: "resolve_approval", approvalId: approval.id, approved: true });
    expect(order).toEqual(["operator", "resolve"]);
  });

  test("rejects invalid targets and stale selection without fabricating UI state", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await expect(ui.dispatch({ kind: "open_session", sessionId: sessionId("missing") })).rejects.toMatchObject({ code: "not_found" });
    await expect(ui.dispatch({ kind: "open_session", sessionId: 1 as unknown as ReturnType<typeof sessionId> })).rejects.toMatchObject({ code: "invalid_input" });
    expect(ui.state().selectedSession).toBeUndefined();
    await expect(ui.dispatch({ kind: "send_message", senderId: agentId("a"), recipientId: agentId("b"), body: "hello" })).rejects.toMatchObject({ code: "invalid_input" });
  });

  test("resume and cancellation remain explicit transport operations", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-tests-1", prompt: "run tests" });
    const id = ui.state().selectedSession!.summary.id;
    await ui.dispatch({ kind: "cancel_session" });
    expect(ui.state().selectedSession?.summary.state).toBe("cancelled");
    await ui.dispatch({ kind: "resume_session", sessionId: id });
    expect(ui.state().selectedSession?.summary.state).toBe("running");
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "cancelSession", "resumeSession"]);
  });

  test("serializes concurrent UI commands so projections cannot overtake durable operations", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await Promise.all([
      ui.dispatch({ kind: "start_session", operationId: "op-first", prompt: "first" }),
      ui.dispatch({ kind: "list_sessions" }),
    ]);
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "listSessions"]);
    expect(ui.state().sessions.map(session => session.id as string)).toEqual(["session-1"]);
  });

  test("dispatch and nested snapshots are detached from private UI state", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    const dispatched = await ui.dispatch({ kind: "start_session", operationId: "op-inspect-2", prompt: "inspect" });
    expect(Object.isFrozen(dispatched.sessions)).toBe(true);
    expect(Object.isFrozen(dispatched.selectedSession!.agents)).toBe(true);
    expect(ui.state().sessions).toHaveLength(0);
    expect(ui.state().selectedSession!.agents).toHaveLength(1);

    await ui.dispatch({ kind: "list_changes" });
    await ui.dispatch({ kind: "read_file", path: "README.md" });
    const first = ui.state();
    first.fileBody!.bytes[0] = 0;
    expect(Object.isFrozen(first.changes)).toBe(true);
    const second = ui.state();
    expect(second.fileBody!.bytes[0]).toBe(new TextEncoder().encode("# GraphCoder fixture\n")[0]);
    expect(second.changes).toHaveLength(1);
  });

  test("cancellation reaches the owner while a history request is pending", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-inspect-3", prompt: "inspect" });
    let release!: () => void;
    const original = transport.readActivity.bind(transport);
    transport.readActivity = async (id, query) => {
      await new Promise<void>(resolve => { release = resolve; });
      return original(id, query);
    };
    const pendingRead = ui.dispatch({ kind: "load_activity" });
    await Promise.resolve();
    await Promise.resolve();
    const cancelled = await ui.dispatch({ kind: "cancel_session" });
    expect(cancelled.selectedSession?.summary.state).toBe("cancelled");
    release();
    await pendingRead;
    expect(ui.state().selectedSession?.summary.state).toBe("cancelled");
  });

  test("cancellation detaches later commands from a request that never resolves", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-stuck", prompt: "inspect" });
    transport.readActivity = async () => await new Promise<never>(() => undefined);
    const stuck = ui.dispatch({ kind: "load_activity" });
    await Promise.resolve();
    await Promise.resolve();
    await ui.dispatch({ kind: "cancel_session" });

    const following = await Promise.race([
      ui.dispatch({ kind: "load_messages" }),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("command remained queued behind cancellation")), 500)),
    ]);
    expect(following.selectedSession?.summary.state).toBe("cancelled");
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "cancelSession", "readMessages"]);
    void stuck;
  });

  test("cancellation fences commands that were already queued behind a stuck request", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-queued", prompt: "inspect" });
    let release!: () => void;
    transport.readActivity = async () => await new Promise<void>(resolve => { release = resolve; });
    const stuck = ui.dispatch({ kind: "load_activity" });
    await Promise.resolve();
    const queued = ui.dispatch({ kind: "load_messages" });
    await Promise.resolve();
    await ui.dispatch({ kind: "cancel_session" });
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "cancelSession"]);
    release();
    await stuck;
    await queued;
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "cancelSession"]);
  });

  test("a newer open wins if cancellation finishes later", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", operationId: "op-race", prompt: "inspect" });
    let finishCancel!: () => void;
    const originalCancel = transport.cancelSession.bind(transport);
    transport.cancelSession = async id => {
      await new Promise<void>(resolve => { finishCancel = resolve; });
      return originalCancel(id);
    };
    const cancelling = ui.dispatch({ kind: "cancel_session" });
    await Promise.resolve();
    const opened = await ui.dispatch({ kind: "open_session", sessionId: sessionId("session-1") });
    expect(opened.selectedSession?.summary.state).toBe("running");
    finishCancel();
    await cancelling;
    expect(ui.state().selectedSession?.summary.state).toBe("running");
  });
});

describe("GraphCoder terminal adapter", () => {
  test("headless commands share the same UI state machine", async () => {
    const output = writable();
    const terminal = new GraphCoderTerminal(createMockTransport(), { output: output.stream });
    await terminal.headless(["start op-terminal-1 inspect repository", "activity", "changes", "diff README.md"]);
    const lines = output.lines();
    expect(lines).toHaveLength(4);
    expect(lines[0]).toMatchObject({ ok: true });
    expect(lines[3]).toMatchObject({ ok: true, value: { path: "README.md" } });
  });

  test("input starts a distinct root turn after reopen and preserves prompt bytes", async () => {
    const output = writable();
    const transport = createMockTransport();
    const prompts: string[] = [];
    const original = transport.inputSession.bind(transport);
    transport.inputSession = async input => {
      prompts.push(input.prompt);
      return original(input);
    };
    const terminal = new GraphCoderTerminal(transport, { output: output.stream });
    await terminal.headless([
      "start op-terminal-first inspect repository",
      "open session-1",
      "input op-terminal-follow-up follow-up  with exact bytes",
    ]);
    expect(prompts).toEqual(["follow-up  with exact bytes"]);
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "openSession", "inputSession"]);
    expect(output.lines().every(line => line.ok === true)).toBe(true);
  });

  test("interactive commands keep accepting cancellation while history is waiting", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const transport = createMockTransport();
    let releaseActivity!: () => void;
    transport.readActivity = async () => {
      await new Promise<void>(resolve => { releaseActivity = resolve; });
      return { items: [] };
    };
    const terminal = new GraphCoderTerminal(transport, { input, output });
    const running = terminal.interactive();
    input.write("start op-interactive-cancel inspect\n");
    await new Promise<void>(resolve => setTimeout(resolve, 20));
    input.write("activity\n");
    await new Promise<void>(resolve => setTimeout(resolve, 20));
    input.write("cancel\n");
    const deadline = Date.now() + 500;
    while (!transport.calls.some(call => call.method === "cancelSession") && Date.now() < deadline) {
      await new Promise<void>(resolve => setTimeout(resolve, 10));
    }
    expect(transport.calls.some(call => call.method === "cancelSession")).toBe(true);
    releaseActivity();
    input.write("quit\n");
    input.end();
    await running;
  });

  test("CLI requires an explicit fixture and supports deterministic headless execution", async () => {
    const missing = writable();
    expect(await runCli(["list"], { output: missing.stream })).toBe(2);
    expect(missing.lines()[0]).toMatchObject({ ok: false });

    const output = writable();
    expect(await runCli(["--fixture=deterministic", "start op-cli-1 hello", "list"], { output: output.stream })).toBe(0);
    expect(output.lines()).toHaveLength(2);
  });

  test("injected production transports use the same headless runner without a fixture", async () => {
    const output = writable();
    const transport = createMockTransport();
    expect(await runCliWithTransport(["start op-production-1 hello", "list"], transport, { output: output.stream })).toBe(0);
    expect(output.lines()).toHaveLength(2);
    const rejected = writable();
    expect(await runCliWithTransport(["--fixture=deterministic"], transport, { output: rejected.stream })).toBe(2);
    expect(rejected.lines()[0]).toMatchObject({ ok: false });
  });

  test("headless runner returns failure when any command is rejected", async () => {
    const output = writable();
    const status = await runCliWithTransport(["open missing"], createMockTransport(), { output: output.stream });
    expect(status).toBe(1);
    expect(output.lines()[0]).toMatchObject({ ok: false });
  });

  test("headless and command-loop adapters issue the same lazy transport sequence", async () => {
    const commands = [
      "start op-loop-1 inspect",
      "activity",
      "messages",
      "approvals",
      "approve approval-1 yes",
      "changes",
      "diff README.md",
      "file README.md",
      "writeback mock-writeback-1 1 yes",
      "cancel",
      "resume session-1",
      "cancel",
    ];
    const headlessTransport = createMockTransport();
    const headless = new GraphCoderTerminal(headlessTransport, { output: writable().stream });
    await headless.headless(commands);
    const commandTransport = createMockTransport();
    const commandLoop = new GraphCoderTerminal(commandTransport, { output: writable().stream });
    for (const command of commands) await commandLoop.command(command);
    expect(commandTransport.calls.map(call => call.method)).toEqual(headlessTransport.calls.map(call => call.method));
    expect(commandTransport.calls.map(call => call.method).filter(method => method === "listChanges")).toHaveLength(1);
    expect(commandTransport.calls.map(call => call.method).filter(method => method === "readChange")).toHaveLength(1);
    expect(commandTransport.calls.map(call => call.method).filter(method => method === "readFile")).toHaveLength(1);
  });

  test("malformed terminal commands become typed errors in headless mode", async () => {
    const output = writable();
    const terminal = new GraphCoderTerminal(createMockTransport(), { output: output.stream });
    await terminal.headless(["unknown", "open", "message a b"]);
    expect(output.lines().every(line => line.ok === false)).toBe(true);
  });

});

void (GraphCoderError satisfies new (...args: never[]) => Error);
void (approvalId satisfies (value: string) => string);
void (undefined as SessionId | undefined);
