import { describe, expect, test } from "bun:test";
import { PassThrough } from "node:stream";
import { createMockTransport } from "../src/mock.js";
import { GraphCoderWireDispatcher } from "../src/dispatcher.js";
import { runNodeGraphCoderDispatcher } from "../src/node-dispatcher.js";

describe("native GraphCoder JSON-lines dispatcher", () => {
  test("maps the public wire envelope to the injected transport and preserves lazy fields", async () => {
    const dispatcher = new GraphCoderWireDispatcher(createMockTransport());
    const listed = JSON.parse(await dispatcher.dispatchLine(JSON.stringify({ request_id: "r1", method: "list_sessions", params: {} }))) as Record<string, unknown>;
    expect(listed).toEqual({ request_id: "r1", ok: true, result: { items: [] } });

  const started = JSON.parse(await dispatcher.dispatchLine(JSON.stringify({ request_id: "r2", method: "start_session", params: { prompt: "inspect", operation_id: "op-dispatch-1" } }))) as { result: { summary: { id: string }; workspace_generation: string } };
    expect(started.result.summary.id).toBe("session-1");
    expect(started.result.workspace_generation).toBe("1");
    const missingOperation = await dispatcher.dispatch({ request_id: "r2-missing", method: "start_session", params: { prompt: "inspect" } } as unknown as Parameters<typeof dispatcher.dispatch>[0]);
    expect(missingOperation).toMatchObject({ request_id: "r2-missing", ok: false, error: { code: "invalid_input" } });
    const changes = JSON.parse(await dispatcher.dispatchLine(JSON.stringify({ request_id: "r3", method: "list_changes", params: { session_id: "session-1" } }))) as { result: { session_id: string; generation: string } };
    expect(changes.result).toMatchObject({ session_id: "session-1", generation: "1" });
    const file = JSON.parse(await dispatcher.dispatchLine(JSON.stringify({ request_id: "r4", method: "read_file", params: { session_id: "session-1", path: "README.md", generation: "1" } }))) as { result: { session_id: string; bytes: number[] } };
    expect(file.result.session_id).toBe("session-1");
    expect(file.result.bytes).toEqual([35, 32, 71, 114, 97, 112, 104, 67, 111, 100, 101, 114, 32, 102, 105, 120, 116, 117, 114, 101, 10]);
  });

  test("requires scoped approval and turns malformed JSON or parameters into typed errors", async () => {
    const dispatcher = new GraphCoderWireDispatcher(createMockTransport());
    const malformed = JSON.parse(await dispatcher.dispatchLine("not-json")) as { ok: boolean; error: { code: string } };
    expect(malformed).toMatchObject({ ok: false, error: { code: "invalid_input" } });
    const missingScope = await dispatcher.dispatch({ request_id: "r5", method: "resolve_approval", params: { approval_id: "approval-1", approved: true } });
    expect(missingScope).toMatchObject({ request_id: "r5", ok: false, error: { code: "invalid_input" } });
    const badPage = await dispatcher.dispatch({ request_id: "r6", method: "list_sessions", params: { query: { after: 4 } } });
    expect(badPage).toMatchObject({ request_id: "r6", ok: false, error: { code: "invalid_input" } });
  });

  test("node entrypoint serves newline-delimited requests until clean EOF", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const lines: string[] = [];
    output.on("data", chunk => lines.push(String(chunk)));
    const serving = runNodeGraphCoderDispatcher({ transport: createMockTransport(), input, output });
    input.end(JSON.stringify({ request_id: "r7", method: "list_sessions", params: {} }) + "\n");
    await serving;
    expect(JSON.parse(lines.join(""))).toEqual({ request_id: "r7", ok: true, result: { items: [] } });
  });

  test("node entrypoint bounds a frame and continues with the following request", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const lines: string[] = [];
    output.on("data", chunk => lines.push(String(chunk)));
    const serving = runNodeGraphCoderDispatcher({ transport: createMockTransport(), input, output, maximumLineBytes: 128 });
    input.end("x".repeat(256) + "\n" + JSON.stringify({ request_id: "r8", method: "list_sessions", params: {} }) + "\n");
    await serving;
    const responses = lines.join("").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
    expect(responses[0]).toMatchObject({ request_id: "", ok: false, error: { code: "invalid_input" } });
    expect(responses[1]).toEqual({ request_id: "r8", ok: true, result: { items: [] } });
  });

  test("node entrypoint rejects invalid UTF-8 before JSON decoding", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const lines: string[] = [];
    output.on("data", chunk => lines.push(String(chunk)));
    const serving = runNodeGraphCoderDispatcher({ transport: createMockTransport(), input, output });
    input.end(Buffer.from([0xc3, 0x28, 0x0a]));
    await serving;
    expect(JSON.parse(lines.join("").trim())).toMatchObject({ request_id: "", ok: false, error: { code: "invalid_input" } });
  });

  test("continues after an invalid UTF-8 chunk and accepts a following Uint8Array request", async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const lines: string[] = [];
    output.on("data", chunk => lines.push(String(chunk)));
    const serving = runNodeGraphCoderDispatcher({ transport: createMockTransport(), input, output });
    input.write(Uint8Array.from([0xc3, 0x28, 0x0a]));
    input.write(new TextEncoder().encode(JSON.stringify({ request_id: "r9", method: "list_sessions", params: {} }) + "\n"));
    input.end();
    await serving;
    const responses = lines.join("").trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);
    expect(responses[0]).toMatchObject({ request_id: "", ok: false, error: { code: "invalid_input" } });
    expect(responses[1]).toEqual({ request_id: "r9", ok: true, result: { items: [] } });
  });

  test("node entrypoint keeps reading control requests while a model request waits", async () => {
    const input = new PassThrough();
    const lines: string[] = [];
    const output = { write(value: string, callback?: (error?: Error | null) => void): boolean { lines.push(value); callback?.(); return true; } } as unknown as NodeJS.WritableStream;
    const transport = createMockTransport();
    let releaseList!: () => void;
    transport.listSessions = async () => {
      await new Promise<void>(resolve => { releaseList = resolve; });
      return { items: [] };
    };
    transport.cancelSession = async id => ({ summary: { id, title: "cancelled", state: "cancelled", updatedAt: "0", rootAgentId: "agent-1" }, agents: [], workspaceGeneration: 0n });
    const serving = runNodeGraphCoderDispatcher({ transport, input, output, maximumInFlight: 1 });
    input.write(JSON.stringify({ request_id: "model-1", method: "list_sessions", params: {} }) + "\n");
    input.write(JSON.stringify({ request_id: "cancel-1", method: "cancel_session", params: { session_id: "session-1" } }) + "\n");
    const deadline = Date.now() + 500;
    while (!lines.join("").includes('"request_id":"cancel-1"') && Date.now() < deadline) await new Promise<void>(resolve => setTimeout(resolve, 10));
    if (!lines.join("").includes('"request_id":"cancel-1"')) throw new Error("control request remained blocked");
    expect(lines.join("")).toContain('"request_id":"cancel-1"');
    releaseList();
    input.end();
    await serving;
  });

  test("node entrypoint admits cancellation with all 64 ordinary slots occupied", async () => {
    const input = new PassThrough();
    const lines: string[] = [];
    const output = { write(value: string, callback?: (error?: Error | null) => void): boolean { lines.push(value); callback?.(); return true; } } as unknown as NodeJS.WritableStream;
    const transport = createMockTransport();
    const releases: Array<() => void> = [];
    transport.listSessions = async () => {
      await new Promise<void>(resolve => releases.push(resolve));
      return { items: [] };
    };
    transport.cancelSession = async id => ({ summary: { id, title: "cancelled", state: "cancelled", updatedAt: "0", rootAgentId: "agent-1" }, agents: [], workspaceGeneration: 0n });
    const serving = runNodeGraphCoderDispatcher({ transport, input, output, maximumInFlight: 64 });
    for (let index = 0; index < 64; index += 1) {
      input.write(JSON.stringify({ request_id: `model-${index}`, method: "list_sessions", params: {} }) + "\n");
    }
    input.write(JSON.stringify({ request_id: "cancel-64", method: "cancel_session", params: { session_id: "session-1" } }) + "\n");
    const deadline = Date.now() + 500;
    while (!lines.join("").includes('"request_id":"cancel-64"') && Date.now() < deadline) await new Promise<void>(resolve => setTimeout(resolve, 10));
    expect(lines.join("")).toContain('"request_id":"cancel-64"');
    for (const release of releases) release();
    input.end();
    await serving;
  });

  test("node entrypoint bounds the reserved cancellation lane", async () => {
    const input = new PassThrough();
    const lines: string[] = [];
    const output = { write(value: string, callback?: (error?: Error | null) => void): boolean { lines.push(value); callback?.(); return true; } } as unknown as NodeJS.WritableStream;
    const transport = createMockTransport();
    const releases: Array<() => void> = [];
    transport.cancelSession = async id => {
      await new Promise<void>(resolve => releases.push(resolve));
      return { summary: { id, title: "cancelled", state: "cancelled", updatedAt: "0", rootAgentId: "agent-1" }, agents: [], workspaceGeneration: 0n };
    };
    const serving = runNodeGraphCoderDispatcher({ transport, input, output });
    for (let index = 0; index < 8; index += 1) {
      input.write(JSON.stringify({ request_id: `control-${index}`, method: "cancel_session", params: { session_id: "session-1" } }) + "\n");
    }
    input.write(JSON.stringify({ request_id: "control-overflow", method: "cancel_session", params: { session_id: "session-1" } }) + "\n");
    const deadline = Date.now() + 500;
    while (!lines.join("").includes('"request_id":"control-overflow"') && Date.now() < deadline) await new Promise<void>(resolve => setTimeout(resolve, 10));
    expect(lines.join("")).toContain('"request_id":"control-overflow"');
    expect(lines.join("")).toContain('"code":"invalid_input"');
    for (const release of releases) release();
    input.end();
    await serving;
  });

  test("node entrypoint converts an oversized producer response into a bounded transport error", async () => {
    const input = new PassThrough();
    const lines: string[] = [];
    const output = { write(value: string, callback?: (error?: Error | null) => void): boolean { lines.push(value); callback?.(); return true; } } as unknown as NodeJS.WritableStream;
    const transport = createMockTransport();
    transport.listSessions = async () => ({ items: [{ id: "session-1", title: "x".repeat(256), state: "running", updatedAt: "0", rootAgentId: "agent-1" }] });
    const serving = runNodeGraphCoderDispatcher({ transport, input, output, maximumLineBytes: 128 });
    input.end(JSON.stringify({ request_id: "large-response", method: "list_sessions", params: {} }) + "\n");
    await serving;
    const response = JSON.parse(lines.join("")) as { request_id: string; ok: boolean; error: { code: string } };
    expect(response).toMatchObject({ request_id: "large-response", ok: false, error: { code: "transport" } });
    expect(Buffer.byteLength(lines[0]!, "utf8")).toBeLessThanOrEqual(128);
  });

  test("uses a neutral request id when an invalid id cannot be echoed safely", async () => {
    const dispatcher = new GraphCoderWireDispatcher(createMockTransport());
    const response = JSON.parse(await dispatcher.dispatchLine(JSON.stringify({ request_id: "é".repeat(129), method: "unknown", params: {} }))) as { request_id: string; ok: boolean; error: { code: string } };
    expect(response).toMatchObject({ request_id: "unknown", ok: false, error: { code: "invalid_input" } });
  });

  test("enforces public text and path bounds before calling the transport", async () => {
    const transport = createMockTransport();
    const dispatcher = new GraphCoderWireDispatcher(transport);
    const hugePrompt = await dispatcher.dispatch({ request_id: "r10", method: "start_session", params: { prompt: "x".repeat(64 * 1024 + 1), operation_id: "op" } });
    expect(hugePrompt).toMatchObject({ request_id: "r10", ok: false, error: { code: "invalid_input" } });
    const hugeBody = await dispatcher.dispatch({ request_id: "r11", method: "send_message", params: { session_id: "session-1", sender_id: "agent-1", recipient_id: "agent-1", body: "x".repeat(64 * 1024 + 1) } });
    expect(hugeBody).toMatchObject({ request_id: "r11", ok: false, error: { code: "invalid_input" } });
    const unsafePath = await dispatcher.dispatch({ request_id: "r12", method: "read_file", params: { session_id: "session-1", path: "../secret", generation: "1" } });
    expect(unsafePath).toMatchObject({ request_id: "r12", ok: false, error: { code: "invalid_input" } });
    expect(transport.calls.some(call => call.method === "readFile")).toBe(false);
  });
});
