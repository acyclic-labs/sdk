import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import init, { WasmMcpHttpTransport } from "../generated/wasm/acyclic_harness_wasm.js";
import { createBrowserMcpHttpProvider } from "../src/mcp-http.ts";

await init({ module_or_path: readFileSync(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });
const operation = "00000000-0000-0000-0000-000000000007";
const message = `{"jsonrpc":"2.0","id":"${operation}","result":{"content":[{"type":"text","text":"hello 🦀"}],"structuredContent":{"value":18446744073709551615},"isError":false}}`;

test("browser I/O feeds the Rust JSON/SSE decoder at every chunk width", async () => {
  for (const sse of [false, true]) {
    const bytes = new TextEncoder().encode(sse ? `data: ${message}\r\n\r\n` : message);
    for (let width = 1; width <= bytes.length; width++) {
      let calls = 0;
      let captured;
      const fetcher = async (_endpoint, options) => {
        calls++; captured = options;
        let offset = 0;
        return new Response(new ReadableStream({ pull(controller) {
          if (offset === bytes.length) { controller.close(); return; }
          const next = Math.min(offset + width, bytes.length);
          controller.enqueue(bytes.slice(offset, next)); offset = next;
        } }), { headers: { "content-type": sse ? "text/event-stream" : "application/json" } });
      };
      const transport = new WasmMcpHttpTransport(createBrowserMcpHttpProvider(fetcher),
        "http://localhost/mcp", "session", 4096, 1000);
      try {
        assert.equal(calls, 0);
        const result = new TextDecoder().decode(await transport.callJson(operation, "echo", {}));
        assert.ok(result.includes('"value":18446744073709551615'));
        assert.equal(JSON.parse(result).content[0].text, "hello 🦀");
        assert.equal(captured.headers.get("mcp-session-id"), "session");
        assert.equal(captured.credentials, "omit");
        assert.equal(captured.redirect, "manual");
        assert.equal(captured.signal.aborted, true);
        assert.equal(await transport.reconcileJson(operation), undefined);
        assert.equal(calls, 1);
      } finally { transport.free(); }
    }
  }
});

test("task cancellation and finite deadline abort the same exchange without retry", async () => {
  for (const mode of ["before", "during", "timeout"]) {
    const controller = new AbortController();
    let calls = 0;
    let started;
    const didStart = new Promise(resolve => { started = resolve; });
    if (mode === "before") controller.abort();
    const fetcher = (_endpoint, options) => {
      calls++; started();
      return new Promise((_resolve, reject) => {
        options.signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
      });
    };
    const transport = new WasmMcpHttpTransport(createBrowserMcpHttpProvider(fetcher, controller.signal),
      "http://localhost/mcp", "session", 4096, mode === "timeout" ? 25 : 1000);
    try {
      const call = transport.callJson(operation, "echo", {});
      const rejection = assert.rejects(call);
      if (mode === "during") { await didStart; controller.abort(); }
      await rejection;
      assert.equal(calls, mode === "before" ? 0 : 1);
      assert.equal(await transport.reconcileJson(operation), undefined);
      assert.equal(calls, mode === "before" ? 0 : 1);
    } finally { transport.free(); }
  }
});

test("truncation, expired session and overflow retain explicit failure with no repost", async () => {
  for (const mode of ["truncated", "expired", "overflow"]) {
    let calls = 0;
    const fetcher = async () => {
      calls++;
      return new Response(mode === "truncated" ? `data: ${message}` : message,
        { status: mode === "expired" ? 404 : 200, headers: { "content-type": "text/event-stream" } });
    };
    const transport = new WasmMcpHttpTransport(createBrowserMcpHttpProvider(fetcher),
      "http://localhost/mcp", "session", mode === "overflow" ? 128 : 4096, 1000);
    try {
      await assert.rejects(transport.callJson(operation, "echo", {}));
      assert.equal(await transport.reconcileJson(operation), undefined);
      assert.equal(calls, 1);
    } finally { transport.free(); }
  }
});
