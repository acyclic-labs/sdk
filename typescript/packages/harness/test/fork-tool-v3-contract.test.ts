import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { WasmModelToolDefinitionWire } from "../generated/wasm/acyclic_harness_wasm.js";
import type { ModelToolDefinition, ToolJsonSchema } from "../src/model.js";

type RequestedResources = {
  readonly model_steps: number;
  readonly output_bytes: number;
  readonly execution_time_ms: number;
};
type ForkChildInput = {
  readonly fork_operation?: string;
  readonly child_operation: string;
  readonly task: string;
  readonly prompt: string;
  readonly requested_resources?: RequestedResources;
};
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

const isPositiveInteger = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 1;

function parseRequestedResources(value: unknown): RequestedResources {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("requested_resources must be an object");
  const record = value as Record<string, unknown>;
  if (Object.keys(record).some(key => !["model_steps", "output_bytes", "execution_time_ms"].includes(key))) {
    throw new TypeError("requested_resources contains an unknown field");
  }
  if (!isPositiveInteger(record.model_steps) || !isPositiveInteger(record.output_bytes) || !isPositiveInteger(record.execution_time_ms)) {
    throw new TypeError("requested_resources limits must be positive safe integers");
  }
  return record as RequestedResources;
}

function parseForkChildInput(value: unknown): ForkChildInput {
  if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("fork input must be an object");
  const record = value as Record<string, unknown>;
  const allowed = ["fork_operation", "child_operation", "task", "prompt", "requested_resources"];
  if (Object.keys(record).some(key => !allowed.includes(key))) throw new TypeError("fork input contains an unknown field");
  if (typeof record.child_operation !== "string" || typeof record.task !== "string" || typeof record.prompt !== "string") {
    throw new TypeError("fork input is missing required strings");
  }
  if (record.task.length < 1 || record.task.length > 4096 || record.prompt.length > 65536) {
    throw new TypeError("fork input text is outside its bounds");
  }
  if (record.fork_operation !== undefined && typeof record.fork_operation !== "string") throw new TypeError("fork_operation must be a string");
  return {
    ...(record.fork_operation === undefined ? {} : { fork_operation: record.fork_operation }),
    child_operation: record.child_operation,
    task: record.task,
    prompt: record.prompt,
    ...(record.requested_resources === undefined ? {} : { requested_resources: parseRequestedResources(record.requested_resources) }),
  };
}

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

  for (const entry of fixture.cases) {
    let accepted = true;
    try {
      parseForkChildInput(entry.value);
    } catch {
      accepted = false;
    }
    expect(accepted, entry.name).toBe(entry.expected === "accept");
  }
});
