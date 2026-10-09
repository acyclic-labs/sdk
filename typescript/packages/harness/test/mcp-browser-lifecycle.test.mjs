import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import init, { WasmMcpHttpTransport } from "../generated/wasm/acyclic_harness_wasm.js";
import { createBrowserMcpHttpProvider } from "../src/mcp-http.ts";

await init({ module_or_path: readFileSync(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });
const operation = suffix => `00000000-0000-0000-0000-${suffix.toString().padStart(12, "0")}`;
const encode = value => new TextEncoder().encode(value);
const decode = value => new TextDecoder().decode(value);
const result = (id, version = "2025-11-25") => JSON.stringify({ jsonrpc: "2.0", id, result: {
  protocolVersion: version, capabilities: { tools: {} }, serverInfo: { name: "fixture", version: "1" },
} });
const head = { status: 200, headers: { "content-type": "application/json", "mcp-session-id": "negotiated" } };

test("browser hosts explicitly admit initialization, readiness and complete paged discovery", async () => {
  for (const sse of [false, true]) {
    const requests = [];
    const provider = createBrowserMcpHttpProvider(async (_endpoint, options) => {
      const input = JSON.parse(decode(options.body));
      requests.push(input);
      assert.equal(options.headers.get("mcp-protocol-version"), "2025-11-25");
      if (input.method === "initialize") {
        assert.equal(options.headers.get("mcp-session-id"), null);
        assert.deepEqual(input.params.capabilities, {});
        const message = result(input.id);
        return new Response(sse ? `data: ${message}\r\n\r\n` : message,
          { headers: { "content-type": sse ? "text/event-stream" : "application/json", "mcp-session-id": "negotiated" } });
      }
      assert.equal(options.headers.get("mcp-session-id"), "negotiated");
      if (input.method === "notifications/initialized") {
        assert.equal(input.id, undefined);
        return new Response(null, { status: 202 });
      }
      assert.equal(input.method, "tools/list");
      assert.equal(input.params.cursor, requests.length === 3 ? undefined : "next");
      const page = requests.length === 3
        ? `{"tools":[{"name":"echo","description":"héllo","inputSchema":{"type":"object","properties":{"sequence":{"type":"integer","maximum":18446744073709551615}}}}],"nextCursor":"next"}`
        : `{"tools":[]}`;
      const message = `{"jsonrpc":"2.0","id":"${input.id}","result":${page}}`;
      return new Response(sse ? `data: ${message}\r\n\r\n` : message,
        { headers: { "content-type": sse ? "text/event-stream" : "application/json" } });
    });
    const setup = new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, 4096, 1000);
    let accepted;
    let transport;
    try {
      const staged = setup.initializationRequest(operation(1), "client", "1");
      const initialize = JSON.parse(decode(new Uint8Array(staged.body)));
      assert.equal(initialize.method, "initialize");
      assert.deepEqual(initialize.params.capabilities, {});
      assert.equal(staged.headers.get("mcp-session-id"), undefined);
      assert.equal(requests.length, 0);
      // The fixture host explicitly dispatches the staged request and retains
      // its bounded response before handing acceptance to the Rust facade.
      const initializeExchange = provider(operation(1), staged);
      const initializationHead = await initializeExchange.response;
      const retained = await initializeExchange.read();
      assert.ok(retained instanceof Uint8Array);
      assert.ok(retained.length <= staged.maximum_response_bytes);
      assert.equal(await initializeExchange.read(), null);
      initializeExchange.cancel();
      assert.equal(requests.length, 1);
      accepted = await setup.acceptInitialization(operation(1), initializationHead, retained);
      assert.equal(JSON.parse(decode(accepted.resultJson())).serverInfo.name, "fixture");
      const ready = accepted.initializedRequest();
      assert.equal(ready.headers.get("mcp-session-id"), "negotiated");
      assert.equal(requests.length, 1);
      const exchange = provider(operation(2), ready);
      assert.equal((await exchange.response).status, 202);
      assert.equal(await exchange.read(), null);
      exchange.cancel();
      transport = accepted.intoTransport();
      accepted = undefined;
      assert.throws(() => transport.initializationRequest(operation(3), "client", "1"));
      assert.equal(setup.initializationRequest(operation(3), "client", "1").headers.get("mcp-session-id"), undefined);
      const firstBytes = await transport.listToolsJson(operation(4), undefined);
      assert.match(decode(firstBytes), /18446744073709551615/);
      const first = JSON.parse(decode(firstBytes));
      assert.equal(first.tools[0].description, "héllo");
      const last = JSON.parse(decode(await transport.listToolsJson(operation(5), first.nextCursor)));
      assert.deepEqual(last.tools, []);
      assert.equal(last.nextCursor, undefined);
      assert.equal(requests.length, 4);
      assert.equal(await transport.reconcileJson(operation(4)), undefined);
      assert.equal(requests.length, 4);
    } finally {
      accepted?.free(); transport?.free(); setup.free();
    }
  }
});

test("initialization acceptance rejects foreign identity, version, bounds and invalid sessions without I/O", async () => {
  let calls = 0;
  const provider = () => { calls++; throw new Error("acceptance must not dispatch"); };
  const setup = new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, 4096, 1000);
  const bound = new WasmMcpHttpTransport(provider, "http://localhost/mcp", "old", 4096, 1000);
  try {
    for (const [headers, body] of [
      [head, encode(result(operation(2)))],
      [head, encode(result(operation(1), "unknown"))],
      [head, new Uint8Array(4097)],
      [{ ...head, headers: { ...head.headers, "mcp-session-id": "bad\r\nheader" } }, encode(result(operation(1)))],
      [head, encode(`{"jsonrpc":"2.0","id":"${operation(1)}","method":"sampling/createMessage"}`)],
    ]) await assert.rejects(setup.acceptInitialization(operation(1), headers, body));
    await assert.rejects(bound.acceptInitialization(operation(1), head, encode(result(operation(1)))));
    assert.equal(calls, 0);
    // Detach retained bytes before reading host head properties with getters.
    const body = encode(result(operation(1)));
    const mutableHead = { headers: head.headers, get status() { body.fill(0); return 200; } };
    const accepted = await setup.acceptInitialization(operation(1), mutableHead, body);
    try { assert.equal(JSON.parse(decode(accepted.resultJson())).protocolVersion, "2025-11-25"); }
    finally { accepted.free(); }
    assert.equal(calls, 0);
  } finally { bound.free(); setup.free(); }
});
