import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";
import init, { WasmMcpHttpTransport } from "../generated/wasm/acyclic_harness_wasm.js";
import { createBrowserMcpHttpProvider } from "../src/mcp-http.ts";

await init({ module_or_path: readFileSync(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });
const operation = "00000000-0000-0000-0000-000000000007";
const message = `{"jsonrpc":"2.0","id":"${operation}","result":{"content":[{"type":"text","text":"hello 🦀"}],"structuredContent":{"value":18446744073709551615},"isError":false}}`;

test("browser transport rejects lossy byte and deadline bounds before I/O", () => {
  let calls = 0;
  const provider = () => { calls++; throw new Error("unexpected I/O"); };
  for (const invalid of [-1, 0.5, 1.5, 2 ** 32, 2 ** 32 + 1, NaN, Infinity, -Infinity, "4096", undefined, null, true]) {
    assert.throws(() => new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, invalid, 1000));
    assert.throws(() => new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, 4096, invalid));
  }
  const maximum = 2 ** 32 - 1;
  const transport = new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, maximum, maximum);
  try {
    const request = transport.initializationRequest(operation, "client", "1");
    assert.equal(request.maximum_response_bytes, maximum);
    assert.equal(request.timeout_ms, maximum);
  } finally { transport.free(); }
  assert.equal(calls, 0);
});

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

test("captured browser responses preserve the admitted prefix before overflow", async () => {
  const corpus = new URL("../../../../rust/codec-fuzz/", import.meta.url);
  const capture = JSON.parse(readFileSync(new URL("captures/mcp-everything-2.1.0-http.json", corpus), "utf8"));
  const requests = readFileSync(new URL("captures/mcp-everything-2.1.0-http.requests.jsonl", corpus), "utf8")
    .trim().split("\n").map(line => JSON.parse(line));
  const recorded = method => {
    const output = capture.outputs.find(output => output.method === method);
    const bytes = new Uint8Array(readFileSync(new URL(`corpus/${output.path}`, corpus)));
    assert.equal(bytes.length, output.bytes);
    assert.equal(createHash("sha256").update(bytes).digest("hex"), output.sha256);
    const exchange = capture.exchanges.find(exchange => exchange.method === method);
    return { bytes, head: { status: exchange.status, headers: exchange.headers },
      request: requests.find(request => request.method === method) };
  };
  const initialization = recorded("initialize");
  const discovery = recorded("tools/list");
  const originalOperation = initialization.request.id;
  const observation = async promise => {
    try {
      const result = await promise;
      result?.free?.();
      assert.fail("malformed or overflowing response was admitted");
    } catch (error) {
      assert.equal(typeof error.code, "number", "refusal lost its typed code");
      return { code: error.code, message: error.message };
    }
  };
  // Mutations derive from the recorded HTTP/SSE bytes; no new capture is claimed.
  const malformed = initialization.bytes.slice();
  malformed[0] = 0xff;
  let expected;
  for (const width of [1, 17, malformed.length]) {
    let reads = 0;
    let cancellations = 0;
    let requests = 0;
    const io = createBrowserMcpHttpProvider(async () => {
      requests++;
      let offset = 0;
      return new Response(new ReadableStream({ pull(controller) {
        if (offset === malformed.length) { controller.close(); return; }
        const next = Math.min(offset + width, malformed.length);
        const chunk = malformed.slice(offset, next);
        // The foreign view's own properties cannot control snapshot extent.
        for (const name of ["length", "byteLength", "byteOffset", "buffer", "subarray"]) {
          Object.defineProperty(chunk, name, { get() { throw new Error(`read foreign ${name}`); } });
        }
        controller.enqueue(chunk);
        offset = next;
      } }), initialization.head);
    });
    const provider = (operation, request) => {
      const exchange = io(operation, request);
      return { response: exchange.response,
        read() { reads++; return exchange.read(); },
        cancel() { cancellations++; exchange.cancel(); } };
    };
    const transport = new WasmMcpHttpTransport(provider, "http://localhost/mcp", "session", 1, 1000);
    try {
      const actual = await observation(transport.listToolsJson(originalOperation, undefined));
      assert.match(actual.message, /SSE is not UTF-8/);
      if (expected) assert.deepEqual(actual, expected, "browser chunking changed the refusal");
      expected = actual;
      assert.equal(reads, 1, "decoder polled beyond the malformed prefix");
      assert.equal(cancellations, 1, "original browser exchange was not cancelled once");
      assert.equal(requests, 1);
      assert.deepEqual(await observation(transport.acceptInitialization(originalOperation,
        initialization.head, malformed)), expected, "retained prefix changed the refusal");
      assert.equal(requests, 1, "retained acceptance dispatched I/O");
      const overflow = await observation(transport.acceptInitialization(originalOperation,
        initialization.head, initialization.bytes));
      assert.notEqual(overflow.code, expected.code);
      assert.ok(overflow.message.includes(originalOperation), "overflow lost the original operation");
    } finally { transport.free(); }
  }
  let overflowReads = 0;
  let overflowCancellations = 0;
  const overflowing = new WasmMcpHttpTransport(() => ({
    response: Promise.resolve(initialization.head),
    read() {
      assert.equal(++overflowReads, 1, "overflow read the browser source again");
      return Promise.resolve(initialization.bytes);
    },
    cancel() { overflowCancellations++; },
  }), "http://localhost/mcp", "session", 1, 1000);
  try {
    const overflow = await observation(overflowing.listToolsJson(originalOperation, undefined));
    assert.ok(overflow.message.includes(originalOperation), "browser overflow lost its operation");
    assert.equal(overflowReads, 1, "overflow read the browser source again");
    assert.equal(overflowCancellations, 1);
  } finally { overflowing.free(); }
  const setup = new WasmMcpHttpTransport(() => { throw new Error("retained response dispatched I/O"); },
    "http://localhost/mcp", undefined, initialization.bytes.length, 1000);
  try {
    const accepted = await setup.acceptInitialization(originalOperation, initialization.head, initialization.bytes);
    try { assert.equal(JSON.parse(new TextDecoder().decode(accepted.resultJson())).serverInfo.name, "mcp-servers/everything"); }
    finally { accepted.free(); }
  } finally { setup.free(); }
  const transport = new WasmMcpHttpTransport(createBrowserMcpHttpProvider(async () =>
    new Response(discovery.bytes, discovery.head)), "http://localhost/mcp", "session", discovery.bytes.length, 1000);
  try {
    assert.ok(JSON.parse(new TextDecoder().decode(await transport.listToolsJson(discovery.request.id, undefined))).tools.length > 0);
  } finally { transport.free(); }
});
