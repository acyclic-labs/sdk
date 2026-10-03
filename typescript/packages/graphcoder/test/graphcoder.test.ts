import { describe, expect, test } from "bun:test";
import { GraphCoderError, GraphCoderUi, agentId, approvalId, sessionId, type SessionId } from "../src/api.js";
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
  test("session listing is summary-only and does not start workers or hydrate workspace", async () => {
    const transport = createMockTransport();
    const ui = new GraphCoderUi(transport);

    await ui.dispatch({ kind: "list_sessions", limit: 10 });

    expect(transport.calls.map(call => call.method)).toEqual(["listSessions"]);
    expect(ui.state().selectedSession).toBeUndefined();
    expect(ui.state().activity).toEqual([]);
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
