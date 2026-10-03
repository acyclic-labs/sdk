import { describe, expect, test } from "bun:test";
import { GraphCoderError, GraphCoderUi, agentId, approvalId, sessionId, type SessionId } from "../src/api.js";
import { BridgeGraphCoderTransport, type GraphCoderWireRequest, type GraphCoderWireResponse } from "../src/bridge.js";
import { createMockTransport } from "../src/mock.js";
import { GraphCoderTerminal, runCli } from "../src/terminal.js";

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
          case "list_changes": return Promise.resolve(response({ generation: "7", items: [{ path: "README.md", kind: "modified", additions: 1, deletions: 0 }] }));
          case "read_change": return Promise.resolve(response({ path: "README.md", unifiedDiff: "@@ -1 +1 @@", generation: "7" }));
          case "read_file": return Promise.resolve(response({ path: "README.md", mediaType: "text/markdown", bytes: [72, 105], generation: "7" }));
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
    const malformedFile = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { path: "README.md", mediaType: "text/markdown", bytes: [256], generation: "1" } }) });
    await expect(malformedFile.readFile(sessionId("session-1"), "README.md", 1n)).rejects.toMatchObject({ code: "transport" });
    const oversized = new BridgeGraphCoderTransport({ request: async request => ({ request_id: request.request_id, ok: true, result: { items: [{ id: "session-1", title: "x".repeat(128), state: "running", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" }] } }) }, "test", 256);
    await expect(oversized.listSessions()).rejects.toMatchObject({ code: "transport" });
  });

  test("bridge binds approval and cancellation arguments without local side effects", async () => {
    const requests: GraphCoderWireRequest[] = [];
    const bridge = {
      request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> {
        requests.push(request);
        if (request.method === "approve_writeback") return Promise.resolve({ request_id: request.request_id, ok: true, result: { operation_id: "op-7", session_id: "session-1", generation: "3", applied: true } });
        return Promise.resolve({ request_id: request.request_id, ok: true, result: { summary: { id: "session-1", title: "inspect", state: "cancelled", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" }, agents: [], workspace_generation: "3" } });
      },
    };
    const transport = new BridgeGraphCoderTransport(bridge);
    const receipt = await transport.approveWriteback({ sessionId: sessionId("session-1"), operationId: "op-7", expectedGeneration: 3n, approved: true });
    expect(receipt).toMatchObject({ operationId: "op-7", sessionId: sessionId("session-1"), generation: 3n, applied: true });
    const cancelled = await transport.cancelSession(sessionId("session-1"));
    expect(cancelled.summary.state).toBe("cancelled");
    expect(requests[0]?.params).toEqual({ session_id: "session-1", operation_id: "op-7", expected_generation: "3", approved: true });
    expect(requests[1]?.params).toEqual({ session_id: "session-1" });
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
    await ui.dispatch({ kind: "start_session", prompt: "inspect the repository" });
    const id = ui.state().selectedSession!.summary.id;

    expect(transport.calls.map(call => call.method)).toEqual(["startSession"]);
    await ui.dispatch({ kind: "load_activity" });
    await ui.dispatch({ kind: "load_messages" });
    await ui.dispatch({ kind: "load_approvals" });
    await ui.dispatch({ kind: "list_changes" });
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "readActivity", "readMessages", "listApprovals", "listChanges"]);
    expect(ui.state().changeBody).toBeUndefined();

    await ui.dispatch({ kind: "read_change", path: "README.md" });
    expect(ui.state().changeBody?.path).toBe("README.md");
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "readActivity", "readMessages", "listApprovals", "listChanges", "readChange"]);
    expect(id as string).toBe("session-1");

    await ui.dispatch({ kind: "read_file", path: "README.md" });
    expect(ui.state().fileBody?.mediaType).toBe("text/markdown");
    expect(transport.calls.map(call => call.method).at(-1)).toBe("readFile");
  });

  test("requires an explicit matching approval before root writeback", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", prompt: "edit the README" });
    await ui.dispatch({ kind: "load_approvals" });
    const approval = ui.state().approvals[0]!;
    await expect(ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 1n, approved: true })).rejects.toMatchObject({ code: "denied" });
    await ui.dispatch({ kind: "resolve_approval", approvalId: approval.id, approved: true });
    await expect(ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 2n, approved: true })).rejects.toMatchObject({ code: "stale" });
    await ui.dispatch({ kind: "approve_writeback", operationId: approval.operationId, expectedGeneration: 1n, approved: true });
    expect(transport.calls.at(-1)?.method).toBe("approveWriteback");
    expect(ui.state().writeback).toMatchObject({ applied: true, generation: 1n });
  });

  test("rejects invalid targets and stale selection without fabricating UI state", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await expect(ui.dispatch({ kind: "open_session", sessionId: sessionId("missing") })).rejects.toMatchObject({ code: "not_found" });
    expect(ui.state().selectedSession).toBeUndefined();
    await expect(ui.dispatch({ kind: "send_message", senderId: agentId("a"), recipientId: agentId("b"), body: "hello" })).rejects.toMatchObject({ code: "invalid_input" });
  });

  test("resume and cancellation remain explicit transport operations", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);
    await ui.dispatch({ kind: "start_session", prompt: "run tests" });
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
      ui.dispatch({ kind: "start_session", prompt: "first" }),
      ui.dispatch({ kind: "list_sessions" }),
    ]);
    expect(transport.calls.map(call => call.method)).toEqual(["startSession", "listSessions"]);
    expect(ui.state().sessions.map(session => session.id as string)).toEqual(["session-1"]);
  });
});

describe("GraphCoder terminal adapter", () => {
  test("headless commands share the same UI state machine", async () => {
    const output = writable();
    const terminal = new GraphCoderTerminal(createMockTransport(), { output: output.stream });
    await terminal.headless(["start inspect repository", "activity", "changes", "diff README.md"]);
    const lines = output.lines();
    expect(lines).toHaveLength(4);
    expect(lines[0]).toMatchObject({ ok: true });
    expect(lines[3]).toMatchObject({ ok: true, value: { path: "README.md" } });
  });

  test("CLI requires an explicit fixture and supports deterministic headless execution", async () => {
    const missing = writable();
    expect(await runCli(["list"], { output: missing.stream })).toBe(2);
    expect(missing.lines()[0]).toMatchObject({ ok: false });

    const output = writable();
    expect(await runCli(["--fixture=deterministic", "start hello", "list"], { output: output.stream })).toBe(0);
    expect(output.lines()).toHaveLength(2);
  });

  test("headless and command-loop adapters issue the same lazy transport sequence", async () => {
    const commands = [
      "start inspect",
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
