import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { createNodeGraphCoderConnection, JsonLineGraphCoderBridge, type GraphCoderProcessDiagnostic } from "../src/node.js";
import type { GraphCoderWireRequest } from "../src/bridge.js";

// Native process startup can take several seconds on the Windows qualification
// lane while other SDK workers are compiling; keep the assertions bounded but
// avoid mistaking host scheduling pressure for a protocol failure.
setDefaultTimeout(30_000);

const childScript = `
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk.toString();
  for (;;) {
    const newline = buffer.indexOf("\\n");
    if (newline < 0) break;
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line.trim()) continue;
    const request = JSON.parse(line);
    const delay = request.params && request.params.delay ? request.params.delay : 0;
    setTimeout(() => process.stdout.write(JSON.stringify({ request_id: request.request_id, ok: true, result: request.params }) + "\\n"), delay);
  }
});
`;

function request(id: string, delay = 0): GraphCoderWireRequest {
  return { request_id: id, method: "list_sessions", params: { delay } };
}

function env(): NodeJS.ProcessEnv {
  return { PATH: process.env.PATH ?? "" };
}

describe("JSON-lines process bridge", () => {
  test("composes the process bridge with the public transport adapter", async () => {
    const script = `let buffer = ""; process.stdin.on("data", chunk => { buffer += chunk.toString(); for (;;) { const newline = buffer.indexOf("\\n"); if (newline < 0) break; const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1); if (!line.trim()) continue; const request = JSON.parse(line); process.stdout.write(JSON.stringify({ request_id: request.request_id, ok: true, result: { items: [] } }) + "\\n"); } });`;
    const connection = createNodeGraphCoderConnection({ executable: process.execPath, args: ["-e", script], env: env() });
    const page = await connection.transport.listSessions();
    expect(page.items).toEqual([]);
    connection.bridge.close();
  });

  test("correlates concurrent responses and preserves explicit parameters", async () => {
    const bridge = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", childScript], env: env() });
    const [slow, fast] = await Promise.all([bridge.request(request("slow", 40)), bridge.request(request("fast"))]);
    expect(slow).toMatchObject({ request_id: "slow", ok: true, result: { delay: 40 } });
    expect(fast).toMatchObject({ request_id: "fast", ok: true, result: { delay: 0 } });
    bridge.close();
  });

  test("cancels one pending request while keeping the process available", async () => {
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const bridge = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", childScript], env: env(), onDiagnostic: event => diagnostics.push(event), cancelMessage: requestId => ({ request_id: `${requestId}:cancel`, method: "cancel_session", params: {} }) });
    const pending = bridge.request(request("cancel", 100));
    expect(bridge.cancel("cancel", "user cancelled")).toBe(true);
    await expect(pending).rejects.toMatchObject({ code: "transport", message: "user cancelled" });
    const response = await bridge.request(request("after-cancel"));
    expect(response).toMatchObject({ request_id: "after-cancel", ok: true });
    const deadline = Date.now() + 2_000;
    while (!diagnostics.some(event => event.kind === "cancelled_response" && event.requestId === "cancel") && Date.now() < deadline) {
      await new Promise<void>(resolve => setTimeout(resolve, 20));
    }
    expect(diagnostics.some(event => event.kind === "cancelled_response" && event.requestId === "cancel")).toBe(true);
    expect(bridge.cancel("missing")).toBe(false);
    bridge.close();
  });

  test("retires a cancelled request id until its late response is consumed", async () => {
    const bridge = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", childScript], env: env() });
    const pending = bridge.request(request("reused", 50));
    expect(bridge.cancel("reused")).toBe(true);
    await expect(pending).rejects.toMatchObject({ code: "transport" });
    await expect(bridge.request(request("reused"))).rejects.toMatchObject({ code: "invalid_input" });
    await new Promise<void>(resolve => setTimeout(resolve, 80));
    bridge.close();
  });

  test("rejects pending calls on clean EOF and reports malformed output", async () => {
    const eof = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", "process.exit(0)"], env: env() });
    await expect(eof.request(request("eof"))).rejects.toMatchObject({ code: "transport" });
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const malformed = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", "console.log('malformed')"], env: env(), onDiagnostic: event => diagnostics.push(event), maximumLineBytes: 128 });
    await expect(malformed.request(request("malformed"))).rejects.toMatchObject({ code: "transport" });
    expect(diagnostics.some(event => event.kind === "malformed_line")).toBe(true);
  });

  test("bounds a response line before parsing it", async () => {
    const oversized = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", "process.stdout.write('x'.repeat(512) + '\\n')"], env: env(), maximumLineBytes: 256 });
    await expect(oversized.request(request("oversized"))).rejects.toMatchObject({ code: "transport" });
  });

  test("bounds pending requests and applies the 256 UTF-8 byte request-id limit", async () => {
    const bridge = new JsonLineGraphCoderBridge({ executable: process.execPath, args: ["-e", childScript], env: env(), maximumPendingRequests: 1 });
    const first = bridge.request(request("first", 30));
    await expect(bridge.request(request("second"))).rejects.toMatchObject({ code: "transport" });
    await expect(first).resolves.toMatchObject({ request_id: "first", ok: true });
    await expect(bridge.request(request("é".repeat(129)))).rejects.toMatchObject({ code: "invalid_input" });
    await expect(bridge.request(request("é".repeat(128)))).resolves.toMatchObject({ ok: true });
    bridge.close();
  });

  test("rejects every pending call when an otherwise valid response has no matching request", async () => {
    const diagnostics: GraphCoderProcessDiagnostic[] = [];
    const bridge = new JsonLineGraphCoderBridge({
      executable: process.execPath,
      args: ["-e", "process.stdout.write(JSON.stringify({ request_id: 'wrong', ok: true, result: {} }) + '\\n')"],
      env: env(),
      onDiagnostic: event => diagnostics.push(event),
    });
    await expect(bridge.request(request("expected"))).rejects.toMatchObject({ code: "transport" });
    expect(diagnostics).toContainEqual({ kind: "unmatched_response", requestId: "wrong" });
  });
});
