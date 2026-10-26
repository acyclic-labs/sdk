import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import type { WasmToolDefinitionInput } from "../generated/wasm/acyclic_harness_wasm.js";
import { initializeModelProviders } from "../src/providers.js";
import { MODEL_TEST_LIMITS as limits } from "./support/model-limits.mjs";

await initializeModelProviders();
// Generated only by the native file-tools-contract executable. A missing file
// fails this qualification instead of substituting a handwritten contract.
const fixture = JSON.parse(await readFile(
  new URL("../../../../fixtures/harness/v2/file-tools-contract.json", import.meta.url), "utf8",
));
type Definition = WasmToolDefinitionInput;
const definitions = new Map<string, Definition>();

test("native file-tool definitions retain their exact identities through WASM", () => {
  for (const entry of fixture.tool_definitions) {
    const wire = entry.definition;
    const definition = {
      name: wire.name, revision: wire.revision, description: wire.description,
      inputSchema: wire.input_schema, outputSchema: wire.output_schema,
      projectionSchema: wire.projection_schema,
    };
    contracts.validateToolDefinition(definition);
    expect(Array.from(contracts.digestCanonicalJson(wire))).toEqual(entry.definition_digest);
    definitions.set(entry.variant, definition);
  }
  expect(definitions.size).toBe(fixture.tool_definitions.length);
  for (const variant of ["exact_read_full", "exact_read_reference", "range_read_full", "range_read_reference"]) {
    expect(definitions.get(variant)?.name).toBe("read");
  }
  expect(definitions.get("write_full")?.name).toBe("write");
  expect(definitions.get("exact_edit_full")?.name).toBe("edit");
  expect([...definitions.values()].some(tool => tool.name === "acyclic.read_file_range")).toBe(false);
});

function definition(variant: string): Definition {
  // Decode independently so the tests do not depend on execution order.
  const entry = fixture.tool_definitions.find((item: { variant: string }) => item.variant === variant);
  if (!entry) throw new Error(`missing native file-tool variant: ${variant}`);
  const wire = entry.definition;
  return {
    name: wire.name, revision: wire.revision, description: wire.description,
    inputSchema: wire.input_schema, outputSchema: wire.output_schema,
    projectionSchema: wire.projection_schema,
  };
}

test("native file results and Full/Reference projections pass actual WASM contracts", () => {
  for (const [key, prefix] of [
    ["range_read", "range_read"], ["literal_search", "literal_search"], ["exact_read", "exact_read"],
  ] as const) {
    const value = fixture[key];
    const canonical = JSON.stringify(value.canonical);
    for (const mode of ["full", "reference"]) {
      const tool = definition(`${prefix}_${mode}`);
      contracts.validateToolInvocation(tool, { callId: "contract-1", name: tool.name, arguments: value.input });
      contracts.validateToolResult(tool, { value: value.canonical });
      contracts.validateToolProjection(tool, value[mode], limits);
      expect(() => contracts.validateToolInvocation(tool, {
        callId: "contract-1", name: tool.name, arguments: { ...value.input, unknown: true },
      })).toThrow();
      expect(() => contracts.validateToolProjection(tool, value.canonical, limits)).toThrow();
    }
    expect(JSON.stringify(value.canonical)).toBe(canonical);
    expect(value.reference.parts[1].file).toEqual(fixture.source.file);
    expect(value.reference.parts[1].policy).toBe("reference");
  }
  for (const mode of ["full", "reference"]) {
    const tool = definition(`write_${mode}`);
    contracts.validateToolResult(tool, { value: fixture.file_result.canonical });
    contracts.validateToolProjection(tool, fixture.file_result[mode], limits);
    expect(() => contracts.validateToolResult(tool, { value: { file: {} } })).toThrow();
  }
  expect(definition("range_read_full")).toMatchObject({
    name: "read", revision: definition("exact_read_full").revision,
    inputSchema: definition("exact_read_full").inputSchema,
    outputSchema: definition("exact_read_full").outputSchema,
  });
  const wrongReference = structuredClone(fixture.range_read.reference);
  wrongReference.parts[1].policy = "bounded_full";
  expect(() => contracts.validateToolProjection(definition("range_read_reference"), wrongReference, limits)).toThrow();
});
