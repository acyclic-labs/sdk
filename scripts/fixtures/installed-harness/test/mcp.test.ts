import { expect, test } from "bun:test";
import {
  NativeContracts, WasmMcpHttpTransport, WasmMcpHttpInitialization, createBrowserMcpHttpProvider, jsonToolProjection,
  type McpCatalog, type McpCallbackRequest,
} from "@acyclic-labs/harness";

const initialization = "01010101-0101-0101-0101-010101010101";
const operation = "02020202-0202-0202-0202-020202020202";
const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

test("installed MCP exports preserve explicit schema exposure, search and stdio contracts", async () => {
  const contracts = await NativeContracts.create();
  const catalog: McpCatalog = { server: "installed", revision: "1",
    schema_exposure: { kind: "selected", names: ["echo"] }, discovery: "search",
    projection_schema: (await jsonToolProjection({})).schema,
    tools: ["echo", "hidden"].map(name => ({ name, description: `Find ${name}`, inputSchema: { type: "object" } })) };
  contracts.validateMcpCatalog(catalog, 2, 8192);
  expect(contracts.mcpModelDefinitions(catalog, 2, 8192).map(tool => tool.name)).toEqual(["mcp.installed.echo"]);
  expect(contracts.searchMcpCatalog(catalog, "find", undefined, 2, 2, 8192).map(tool => tool.name))
    .toEqual(["mcp.installed.echo", "mcp.installed.hidden"]);
  expect(() => contracts.searchMcpCatalog({ ...catalog, discovery: "disabled" }, "", undefined, 2, 2, 8192)).toThrow();
  expect(() => contracts.validateMcpCatalog(catalog, 1, 8192)).toThrow();
  for (const bound of [-1, 0.5, 1.5, 2 ** 32, 2 ** 32 + 1, NaN, Infinity, -Infinity]) {
    expect(() => contracts.validateMcpCatalog(catalog, bound, 8192)).toThrow();
    expect(() => contracts.validateMcpCatalog(catalog, 2, bound)).toThrow();
    expect(() => contracts.mcpModelDefinitions(catalog, bound, 8192)).toThrow();
    expect(() => contracts.mcpModelDefinitions(catalog, 2, bound)).toThrow();
    expect(() => contracts.searchMcpCatalog(catalog, "", undefined, bound, 2, 8192)).toThrow();
    expect(() => contracts.searchMcpCatalog(catalog, "", undefined, 2, bound, 8192)).toThrow();
    expect(() => contracts.searchMcpCatalog(catalog, "", undefined, 2, 2, bound)).toThrow();
    const provider = () => { throw new Error("invalid bounds must not invoke the provider"); };
    expect(() => new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, bound, 1000)).toThrow();
    expect(() => new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, 4096, bound)).toThrow();
  }
  contracts.validateMcpStdioRequest({ initialization, operation, method: "tools/list", params: { cursor: "next" }, maximum_bytes: 4096 });
  expect(() => contracts.validateMcpStdioRequest({ initialization, operation,
    method: "tools/list", params: {}, maximum_bytes: 0 })).toThrow();
  expect(() => contracts.validateMcpStdioRequest({ initialization, operation: initialization,
    method: "tools/list", params: {}, maximum_bytes: 4096 })).toThrow();
});

test("installed MCP HTTP lifecycle classes use shipped WASM and preserve result bytes", async () => {
  await NativeContracts.create();
  for (const sse of [false, true]) {
    let calls = 0;
    const provider = createBrowserMcpHttpProvider(async (_input, options) => {
      calls++;
      const request = JSON.parse(decode(options?.body as Uint8Array));
      expect(new Headers(options?.headers).get("mcp-session-id")).toBe("installed-session");
      expect(options?.credentials).toBe("omit");
      expect(options?.redirect).toBe("manual");
      if (request.method === "notifications/initialized") return new Response(null, { status: 202 });
      expect(request.method).toBe("tools/call");
      expect(request.id).toBe(operation);
      const message = `{"jsonrpc":"2.0","id":"${request.id}","result":{"content":[{"type":"text","text":"héllo 🦀"}],"structuredContent":{"value":18446744073709551615}}}`;
      return new Response(sse ? `data: ${message}\r\n\r\n` : message,
        { headers: { "content-type": sse ? "text/event-stream" : "application/json" } });
    });
    const setup = new WasmMcpHttpTransport(provider, "http://localhost/mcp", undefined, 4096, 1000);
    let accepted: WasmMcpHttpInitialization | undefined;
    let transport: WasmMcpHttpTransport | undefined;
    try {
      expect(setup.initializationRequest(initialization, "installed", "1").headers.get("mcp-session-id")).toBeUndefined();
      accepted = await setup.acceptInitialization(initialization,
        { status: 200, headers: { "content-type": "application/json", "mcp-session-id": "installed-session" } },
        new TextEncoder().encode(JSON.stringify({ jsonrpc: "2.0", id: initialization, result: {
          protocolVersion: "2025-11-25", capabilities: { tools: {} }, serverInfo: { name: "installed", version: "1" },
        } })));
      expect(accepted).toBeInstanceOf(WasmMcpHttpInitialization);
      expect(JSON.parse(decode(accepted.resultJson())).serverInfo.name).toBe("installed");
      expect(calls).toBe(0);
      const ready = provider(operation, accepted.initializedRequest());
      try { expect((await ready.response).status).toBe(202); expect(await ready.read()).toBeNull(); }
      finally { ready.cancel(); }
      transport = accepted.intoTransport(); accepted = undefined;
      const result = decode(await transport.callJson(operation, "echo", {}));
      expect(result).toContain("18446744073709551615");
      expect(JSON.parse(result).content[0].text).toBe("héllo 🦀");
      expect(await transport.reconcileJson(operation)).toBeUndefined();
      expect(calls).toBe(2);
    } finally { accepted?.free(); transport?.free(); setup.free(); }
  }
});

test("installed MCP callback primitives and new stdio services use runtime contracts", async () => {
  const contracts = await NativeContracts.create();
  const message = { jsonrpc: "2.0", id: "sample", method: "sampling/createMessage",
    params: { messages: [{ role: "user", content: { type: "text", text: "input" } }], maxTokens: 128 } };
  const request: McpCallbackRequest = {
    parent: operation,
    policy: { server: "installed", revision: "1", sampling: true, elicitation: false, maximum_requests: 1 },
    message,
  };
  contracts.validateMcpCallbackRequest(request);
  const pinned = contracts.mcpCallbackOperation(request);
  expect(pinned).not.toBe(operation);
  expect(contracts.mcpCallbackOperation({ ...request, message: { ...message,
    params: { messages: [{ role: "user", content: { type: "text", text: "changed" } }], maxTokens: 128 } } })).toBe(pinned);
  for (const params of [{ messages: [], maxTokens: 1.5 }, { messages: [{ role: "system", content: { type: "text", text: "untrusted" } }], maxTokens: 1 }]) {
    expect(() => contracts.validateMcpCallbackRequest({ ...request, message: { ...message, params } })).toThrow();
  }
  expect(() => contracts.validateMcpCallbackRequest({ ...request, policy: { ...request.policy, sampling: false } })).toThrow();
  for (const [method, params] of [
    ["resources/list", {}], ["resources/templates/list", {}], ["resources/read", { uri: "installed://resource" }],
    ["prompts/list", {}], ["prompts/get", { name: "prompt", arguments: {} }],
  ] as const) contracts.validateMcpStdioRequest({ initialization, operation, method, params, maximum_bytes: 4096 });
});

test("installed shipped WASM uses real Fetch resource endpoints and rejects unselected methods before I/O", async () => {
  await NativeContracts.create();
  let calls = 0;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch(request) {
    calls++;
    expect(request.headers.get("authorization")).toBeNull();
    expect(request.headers.get("cookie")).toBeNull();
    expect(request.headers.get("mcp-session-id")).toBe("installed");
    const input = await request.json() as { id: string; method: string; params: { uri: string } };
    expect(input.method).toBe("resources/read");
    const result = input.params.uri === "installed://resource"
      ? { contents: [{ uri: input.params.uri, text: "installed actual resource" }] }
      : { contents: [{ uri: input.params.uri }] };
    return Response.json({ jsonrpc: "2.0", id: input.id, result });
  } });
  const transport = new WasmMcpHttpTransport(createBrowserMcpHttpProvider(), `http://127.0.0.1:${server.port}/mcp`, "installed", 4096, 1000);
  try {
    const result = JSON.parse(decode(await transport.serviceJson(operation, "resources/read", { uri: "installed://resource" })));
    expect(result.contents[0].text).toBe("installed actual resource");
    await expect(transport.serviceJson(operation, "unknown/arbitrary", {})).rejects.toThrow();
    await expect(transport.serviceJson(operation, "resources/read", { uri: 7 })).rejects.toThrow();
    expect(calls).toBe(1);
    await expect(transport.serviceJson(operation, "resources/read", { uri: "installed://invalid" })).rejects.toThrow();
    expect(calls).toBe(2);
  } finally { transport.free(); await server.stop(true); }
});
