import { expect, test } from "bun:test";
import fc from "fast-check";
import {
  Harness, MemoryConversation, NativeContracts, jsonToolProjection,
  type AgentId, type ModelContentPart, type ModelEvent,
} from "../src/index.js";
import { syntheticAccounting, scriptedPreparation } from "./support/model-accounting.mjs";

const contracts = await NativeContracts.create();
const model = { provider: "fixture", name: "history", revision: "1", options: {} };
const projection = await jsonToolProjection({});
type Block = Extract<ModelEvent, { kind: "reasoning_block" }>["block"];

function assertOrder(parts: readonly ModelContentPart[], blocks: readonly Block[]) {
  const assistant = parts.filter(part => part.kind === "reasoning" || part.kind === "tool_call");
  expect(assistant.map(part => part.kind)).toEqual(blocks.flatMap((_, index) =>
    index + 1 === blocks.length ? ["reasoning"] : ["reasoning", "tool_call"]));
  expect(assistant.filter(part => part.kind === "reasoning").map(part => part.block)).toEqual([...blocks]);
}

function block(text: string): Block {
  return { kind: "encrypted", text, origin: { provider: "responses", model: "fixture", credential_scope: null },
    continuity: [...new TextEncoder().encode(JSON.stringify({ type: "reasoning", id: "rs_saved", summary: [], encrypted_content: text }))],
    outcome: "preserved-opaque" };
}

test("ordered reasoning survives tool batches, final answers and the next conversation turn", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.string({ maxLength: 24 }), { minLength: 2, maxLength: 4 }), async texts => {
    const blocks = [...texts.map(text => block(`signed\n${text}`)), block("tail without a call")];
    const host = await MemoryConversation.create({ agent: "08080808-0808-0808-0808-080808080808" as AgentId });
    let step = 0;
    const runtime = await Harness.builder(contracts).tool({
      name: "echo", revision: "1", description: "Echo", inputSchema: {}, outputSchema: {}, projection,
      parseInput: value => value, parseOutput: value => value, handler: async (_, value) => value,
    }).grant("tool:call:echo").model(model, {
      ...syntheticAccounting(contracts),
      prepare: scriptedPreparation(contracts, async function* (request) {
        if (step++ === 0) {
          for (const [index, item] of blocks.entries()) {
            yield { kind: "reasoning_block" as const, block: item };
            if (index + 1 < blocks.length) yield { kind: "tool_call" as const, callId: `call_${index}`, name: "echo",
              arguments: {}, metadata: { signature: "opaque", ["__proto__"]: { literal: true } } };
          }
        } else {
          const parts = request.messages.flatMap(message => typeof message.content === "string" ? []
            : Array.isArray(message.content) ? message.content : [message.content]);
          assertOrder(parts, blocks);
          yield { kind: "content" as const, delta: "done" };
        }
        yield { kind: "completed" as const, metadata: {} };
      }), async reconcile() { return undefined; },
    }).build();
    try {
      expect((await host.runPrompt(runtime, "first")).text).toBe("done");
      const saved = host.conversation().messages.filter(message => message.extensions["acyclic.model.reasoning"]);
      expect(saved).toHaveLength(blocks.length);
      for (const [index, message] of saved.entries()) {
        expect(contracts.canonicalEqual(contracts.decodeCanonicalJson(await host.read(message.content)),
          { kind: "reasoning_block", block: blocks[index] })).toBe(true);
      }
      expect((await host.runPrompt(runtime, "next")).text).toBe("done");
      expect(step).toBe(3);
    } finally { host.free(); }
  }), { numRuns: 16 });
});

test("negative control detects lost opaque reasoning in the same history oracle", () => {
  expect(() => assertOrder([{ kind: "tool_call", callId: "call", name: "echo", arguments: {} }],
    [block("first"), block("tail")])).toThrow();
});

async function boundedHandler(stripMetadata: boolean, metadata: Record<string, string> = { signature: "x".repeat(1000) }) {
  let executions = 0;
  let step = 0;
  const runtime = await Harness.builder(contracts).limits({ file_bytes: 1024, render_bytes: 1024 }).tool({
    name: "echo", revision: "1", description: "Echo", inputSchema: {}, outputSchema: {}, projection,
    parseInput: value => value, parseOutput: value => value, handler: async () => { executions++; return null; },
  }).grant("tool:call:echo").model(model, {
    ...syntheticAccounting(contracts),
    prepare: scriptedPreparation(contracts, async function* () {
      if (step++ === 0) yield { kind: "tool_call" as const, callId: "call", name: "echo", arguments: {},
        metadata: stripMetadata ? null : metadata };
      yield { kind: "completed" as const, metadata: {} };
    }), async reconcile() { return undefined; },
  }).build();
  try { await runtime.run("go"); } catch { /* Admission rejection is expected. */ }
  return executions;
}

test("the complete saved invocation is admitted before invoking the tool handler", async () => {
  expect(await boundedHandler(false)).toBe(0);
});

test("negative control stripping metadata bypasses the full envelope bound", async () => {
  expect(await boundedHandler(true)).toBe(1);
});

test("reserved tool artifact markers are rejected before the handler", async () => {
  const metadata = { signature: "opaque", "$serde_json::private::RawValue": "null" };
  expect(await boundedHandler(false, metadata)).toBe(0);
  // Negative control: removing the rejected metadata permits the handler.
  expect(await boundedHandler(true, metadata)).toBe(1);
});
