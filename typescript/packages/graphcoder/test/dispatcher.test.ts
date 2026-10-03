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
});
