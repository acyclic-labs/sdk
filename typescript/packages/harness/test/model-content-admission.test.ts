import { expect, test } from "bun:test";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { validateModelContent, validateUserInput,
  type WasmFileRefWire } from "../generated/wasm/acyclic_harness_wasm.js";
import { initializeModelProviders } from "../src/providers.js";
import { MODEL_TEST_LIMITS as fixtureLimits } from "./support/model-limits.mjs";

await initializeModelProviders();

// Metadata admission only: no read capability or filesystem IO is fabricated.
async function file(): Promise<WasmFileRefWire> {
  return contracts.validateContract("file_ref", {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private",
      owner: { kind: "agent", id: "07070707-0707-0707-0707-070707070707" } },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode("input"), "text/plain"),
    display_name: "input.txt",
  }, undefined) as WasmFileRefWire;
}

test("tool results retain their pinned string schema and enforce the exact render bound", async () => {
  const definition = { name: "read", revision: "1", description: "read", inputSchema: { type: "string" },
    outputSchema: { type: "string" }, projectionSchema: { type: "string" } };
  const limits = { ...fixtureLimits, render_bytes: 64 };
  const accepted = { kind: "json" as const, value: "x".repeat(38) };
  expect(contracts.encodeCanonicalJson(accepted).length).toBe(64);
  expect(() => contracts.validateToolProjection(definition, accepted, limits)).not.toThrow();
  const rejected = { kind: "json" as const, value: "x".repeat(39) };
  expect(contracts.encodeCanonicalJson(rejected).length).toBe(65);
  expect(() => contracts.validateToolProjection(definition, rejected, limits)).toThrow();
  expect(() => contracts.validateToolProjection(definition, { kind: "json", value: 42 }, limits)).toThrow();
});

test("WASM model validators preserve Rust limits and tool-name checks", async () => {
  const content = await file();
  expect(() => validateModelContent("hello", fixtureLimits)).not.toThrow();
  expect(() => validateModelContent({ kind: "file", file: content, policy: "reference" }, fixtureLimits)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "lookup", arguments: { value: 1 } }, fixtureLimits)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "bad name", arguments: {} }, fixtureLimits)).toThrow();
  expect(() => validateModelContent({ kind: "text", text: "x".repeat(fixtureLimits.render_bytes + 1) }, fixtureLimits)).toThrow();
  expect(() => validateModelContent({ kind: "unknown", value: true } as never, fixtureLimits)).toThrow();
});

test("WASM user-input validator rejects empty, tool, and malformed content", async () => {
  const content = await file();
  expect(() => validateUserInput("hello")).not.toThrow();
  expect(() => validateUserInput({ kind: "file", file: content, policy: { native: {
    intent: { kind: "image", detail: "auto" }, maximum_bytes: 64, maximum_work: 8, configuration: null,
  } } })).not.toThrow();
  expect(() => validateUserInput("")).toThrow();
  expect(() => validateUserInput([])).toThrow();
  expect(() => validateUserInput({ kind: "tool_result", callId: "call", name: "lookup", content: { kind: "json", value: {} } })).toThrow();
  expect(() => validateUserInput({ kind: "file", file: { ...content, path: "../escape" }, policy: "reference" })).toThrow();
});

async function nativeOptionPart() {
  const media = await file();
  const options = { ...media, path: "messages/options.json", display_name: "options.json",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode("{}"), "application/json") };
  const configuration = {
    source: { authority: { kind: "agent" as const, id: "07070707-0707-0707-0707-070707070707" }, revision: 9_007_199_254_740_993n },
    configuration: { extension: { name: "fixture.options", version: 1 },
      schema_digest: Array(32).fill(1), content: options },
    implementation_digest: Array(32).fill(2),
  };
  return { kind: "file" as const, file: media, policy: { native: {
    intent: { kind: "image" as const, detail: "auto" as const },
    maximum_bytes: 64, maximum_work: 8, configuration,
  } } };
}

test("Rust inventory includes nested tool media and options while preserving original revisions", async () => {
  const part = await nativeOptionPart();
  const inventory = contracts.modelContentInventory({ kind: "tool_result", callId: "call", name: "inspect",
    content: { kind: "parts", parts: [{ kind: "text", text: "attachment" }, part] } }, fixtureLimits);
  expect(inventory.files).toEqual([part.file, part.policy.native.configuration.configuration.content]);
  expect(inventory.nativeConfigurations).toEqual([part.policy.native.configuration]);
  expect(inventory.nativeConfigurations[0]?.source.revision).toBe(9_007_199_254_740_993n);
  expect(contracts.modelContentInventory("plain text", fixtureLimits)).toEqual({ files: [], nativeConfigurations: [] });
});
