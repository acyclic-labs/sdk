import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import init, { validateMcpCatalog, mcpModelDefinitions, searchMcpCatalog, jsonToolProjectionSchema }
  from "../generated/wasm/acyclic_harness_wasm.js";

await init({ module_or_path: readFileSync(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });

test("catalog reservations remain independent across validation, exposure and search", () => {
  const catalog = { server: "catalog", revision: "1", discovery: "search",
    schema_exposure: { kind: "selected", names: ["tool0"] },
    projection_schema: jsonToolProjectionSchema({}),
    tools: Array.from({ length: 257 }, (_, index) => ({ name: `tool${index}`,
      description: "Searchable tool", inputSchema: { type: "object" } })) };
  for (const [tools, bytes] of [[undefined, undefined], [257, undefined],
    [undefined, 1_000_000], [2 ** 32 - 1, 2 ** 32 - 1]]) {
    validateMcpCatalog(catalog, tools, bytes);
    assert.deepEqual(mcpModelDefinitions(catalog, tools, bytes).map(tool => tool.name), ["mcp.catalog.tool0"]);
    assert.equal(searchMcpCatalog(catalog, "searchable", undefined, 2, tools, bytes).length, 2);
  }
  for (const [tools, bytes] of [[256, undefined], [undefined, 1]]) {
    assert.throws(() => validateMcpCatalog(catalog, tools, bytes));
    assert.throws(() => mcpModelDefinitions(catalog, tools, bytes));
    assert.throws(() => searchMcpCatalog(catalog, "", undefined, 2, tools, bytes));
  }
  for (const invalid of [-1, 0, 0.5, 1.5, 2 ** 32, 2 ** 32 + 1, NaN, Infinity, -Infinity, "257", true]) {
    for (const [tools, bytes] of [[invalid, undefined], [undefined, invalid]]) {
      assert.throws(() => validateMcpCatalog(catalog, tools, bytes));
      assert.throws(() => mcpModelDefinitions(catalog, tools, bytes));
      assert.throws(() => searchMcpCatalog(catalog, "", undefined, 2, tools, bytes));
    }
  }
});
