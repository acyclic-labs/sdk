import { expect, test } from "bun:test";
import {
  NativeContracts, WasmMcpHttpTransport, WasmMcpHttpInitialization, createBrowserMcpHttpProvider,
  type McpCatalog,
} from "@acyclic-labs/harness";

const initialization = "01010101-0101-0101-0101-010101010101";
const operation = "02020202-0202-0202-0202-020202020202";
const decode = (bytes: Uint8Array) => new TextDecoder().decode(bytes);

test("installed MCP exports preserve explicit schema exposure, search and stdio contracts", async () => {
  const contracts = await NativeContracts.create();
  const catalog: McpCatalog = { server: "installed", revision: "1",
    schema_exposure: { kind: "selected", names: ["echo"] }, discovery: "search",
    tools: ["echo", "hidden"].map(name => ({ name, description: `Find ${name}`, inputSchema: { type: "object" } })) };
  contracts.validateMcpCatalog(catalog, 2, 8192);
  expect(contracts.mcpModelDefinitions(catalog, 2, 8192).map(tool => tool.name)).toEqual(["mcp.installed.echo"]);
  expect(contracts.searchMcpCatalog(catalog, "find", undefined, 2, 2, 8192).map(tool => tool.name))
    .toEqual(["mcp.installed.echo", "mcp.installed.hidden"]);
  expect(() => contracts.searchMcpCatalog({ ...catalog, discovery: "disabled" }, "", undefined, 2, 2, 8192)).toThrow();
  expect(() => contracts.validateMcpCatalog(catalog, 1, 8192)).toThrow();
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
