import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import init, { validateMcpCatalog, searchMcpCatalog, mcpModelDefinitions, validateMcpStdioRequest }
  from "../generated/wasm/acyclic_harness_wasm.js";

await init({ module_or_path: readFileSync(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });

const tool = name => ({ name, description: `Find ${name}`, inputSchema: { type: "object" },
  outputSchema: { type: "object", minProperties: 1, required: ["value"], properties: { value: { type: "integer" } } } });
const catalog = { server: "fixture", revision: "1", schema_exposure: { kind: "eager" },
  discovery: "search", tools: [tool("c"), tool("a"), tool("b")] };

test("MCP WASM validates exact native stdio descriptors without selecting a process", () => {
  const request = { initialization: "01010101-0101-0101-0101-010101010101",
    operation: "02020202-0202-0202-0202-020202020202", method: "tools/call",
    params: { name: "echo", arguments: {} }, maximum_bytes: 4096 };
  validateMcpStdioRequest(request);
  validateMcpStdioRequest({ ...request, method: "tools/list", params: { cursor: "next" } });
  for (const change of [
    { operation: request.initialization }, { operation: "invalid" }, { maximum_bytes: 0 },
    { maximum_bytes: 32 }, { method: "sampling/createMessage" }, { params: [] },
    { params: {} }, { params: { name: "echo", arguments: [] } },
    { params: { name: "echo", task: {} } }, { params: { name: "echo", request_state: "retry" } },
    { params: { name: "echo", requestState: "retry" } },
    { method: "tools/list", params: { cursor: 1 } }, { unapproved_field: true },
  ]) assert.throws(() => validateMcpStdioRequest({ ...request, ...change }));
});

test("MCP WASM uses the native schema and bounded catalog rules", () => {
  validateMcpCatalog(catalog, 3, 8192);
  for (const invalid of [
    { ...catalog, tools: [tool("a"), tool("a")] },
    { ...catalog, tools: [{ ...tool("a"), inputSchema: { type: "unknown" } }] },
    { ...catalog, server: "ambiguous.namespace" },
  ]) assert.throws(() => validateMcpCatalog(invalid, 3, 8192));
  assert.throws(() => validateMcpCatalog(catalog, 2, 8192));
  assert.throws(() => validateMcpCatalog(catalog, 3, 1));
});

test("MCP WASM exposure and discovery are explicit independent host policies", () => {
  assert.deepEqual(mcpModelDefinitions(catalog, 3, 8192).map(tool => tool.name),
    ["mcp.fixture.a", "mcp.fixture.b", "mcp.fixture.c"]);
  const selected = { ...catalog, schema_exposure: { kind: "selected", names: ["b"] } };
  assert.deepEqual(mcpModelDefinitions(selected, 3, 8192).map(tool => tool.name), ["mcp.fixture.b"]);
  assert.equal(searchMcpCatalog(selected, "", undefined, 3, 3, 8192).length, 3);
  assert.deepEqual(mcpModelDefinitions({ ...selected, schema_exposure: { kind: "selected", names: [] } }, 3, 8192), []);
  assert.throws(() => searchMcpCatalog({ ...selected, discovery: "disabled" }, "", undefined, 3, 3, 8192));
  for (const names of [["absent"], ["a", "a"]]) {
    assert.throws(() => validateMcpCatalog({ ...selected, schema_exposure: { kind: "selected", names } }, 3, 8192));
  }
});

test("MCP WASM search is ordered and retains the canonical remote output contract", () => {
  const page = searchMcpCatalog(catalog, "find", undefined, 1, 3, 8192);
  assert.equal(page.length, 1);
  assert.equal(page[0].name, "mcp.fixture.a");
  assert.equal(page[0].revision, "1");
  assert.deepEqual(page[0].output_schema.then.properties.structuredContent, tool("a").outputSchema);
  assert.equal(searchMcpCatalog(catalog, "find", "a", 1, 3, 8192)[0].name, "mcp.fixture.b");
  assert.throws(() => searchMcpCatalog(catalog, "", undefined, 0, 3, 8192));
});

test("MCP WASM rejects lossy unsigned catalog and search allowances", () => {
  for (const invalid of [-1, 0.5, 1.5, 2 ** 32, 2 ** 32 + 1, NaN, Infinity, -Infinity, "3", undefined, null, true]) {
    for (const run of [
      () => validateMcpCatalog(catalog, invalid, 8192),
      () => validateMcpCatalog(catalog, 3, invalid),
      () => mcpModelDefinitions(catalog, invalid, 8192),
      () => mcpModelDefinitions(catalog, 3, invalid),
      () => searchMcpCatalog(catalog, "", undefined, invalid, 3, 8192),
      () => searchMcpCatalog(catalog, "", undefined, 3, invalid, 8192),
      () => searchMcpCatalog(catalog, "", undefined, 3, 3, invalid),
    ]) assert.throws(run, `accepted ${String(invalid)}`);
  }
  const maximum = 2 ** 32 - 1;
  validateMcpCatalog(catalog, maximum, maximum);
  assert.equal(mcpModelDefinitions(catalog, maximum, maximum).length, 3);
  assert.equal(searchMcpCatalog(catalog, "", undefined, maximum, maximum, maximum).length, 3);
});
