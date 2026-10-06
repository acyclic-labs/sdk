import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { WasmModelToolDefinitionWire } from "../generated/wasm/acyclic_harness_wasm.js";
import type { ModelToolDefinition, ToolJsonSchema } from "../src/model.js";

type ForkToolFixture = {
  readonly version: number;
  readonly tool: {
    readonly name: string;
    readonly revision: string;
    readonly description: string;
    readonly input_schema: ToolJsonSchema & {
      readonly required: readonly string[];
      readonly properties: Readonly<Record<string, ToolJsonSchema>>;
    };
  };
  readonly cases: readonly { readonly name: string; readonly value: unknown; readonly expected: "accept" | "reject" }[];
};

const fixture = JSON.parse(readFileSync(
  new URL("../../../../conformance/vectors/harness/fork-tool-v3.json", import.meta.url),
  "utf8",
)) as ForkToolFixture;

test("fork tool v3 schema is consumable through generated and public model types", () => {
  expect(fixture.version).toBe(3);
  expect(fixture.tool.name).toBe("acyclic.fork_child");
  expect(fixture.tool.revision).toBe("3");
  expect(fixture.tool.input_schema.required).toEqual(["child_operation", "task", "prompt"]);

  const generatedWire: WasmModelToolDefinitionWire = {
    name: fixture.tool.name,
    revision: fixture.tool.revision,
    description: fixture.tool.description,
    input_schema: fixture.tool.input_schema,
    output_schema: { type: "object" },
    model_output_schema: { type: "object" },
  };
  const publicDefinition: ModelToolDefinition = {
    name: generatedWire.name,
    revision: generatedWire.revision,
    description: generatedWire.description,
    inputSchema: fixture.tool.input_schema,
    outputSchema: generatedWire.output_schema,
    modelOutputSchema: generatedWire.model_output_schema,
  };
  expect(publicDefinition.inputSchema).toEqual(fixture.tool.input_schema);
  // Runtime acceptance/rejection belongs to Rust's schema validator. This
  // consumer test only proves that the pinned vector is consumable through
  // the generated and public model-definition surfaces.
  expect(fixture.cases.map(entry => [entry.name, entry.expected])).toEqual([
    ["bounded request", "accept"],
    ["zero model steps", "reject"],
    ["unknown resource field", "reject"],
  ]);
});
