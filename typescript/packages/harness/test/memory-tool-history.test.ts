import { expect, test } from "bun:test";
import fc from "fast-check";
import {
  Harness, MemoryConversation, NativeContracts, defineTool, jsonToolProjection, type AgentId,
} from "../src/index.js";
import { syntheticAccounting, scriptedPreparation } from "./support/model-accounting.mjs";

const contracts = await NativeContracts.create();
const agent = "08080808-0808-0808-0808-080808080808" as AgentId;
const model = { provider: "fixture", name: "fixture", revision: "1", options: {} } as const;
const schema = { type: "object", properties: { value: { type: "integer" } }, required: ["value"], additionalProperties: false };
const projection = await jsonToolProjection(schema);

test("saved parallel calls preserve invocation authority and provider metadata in the next turn", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.string({ maxLength: 48 }), { minLength: 2, maxLength: 5 }), async signatures => {
    const host = await MemoryConversation.create({ agent });
    const metadata = signatures.map(signature => ({
      gemini_thought_signature: `簽名🙂\u0000${signature}`,
      openai_reasoning: [{ type: "reasoning", id: `rs_${signature}`, encrypted_content: signature, summary: [] }],
      anthropic_thinking: [{ type: "thinking", thinking: "inspect", signature }],
    }));
    let modelStep = 0;
    const tool = await defineTool<{ value: number }, { value: number }>({
      name: "echo_saved", revision: "1", description: "Echo", inputSchema: schema, outputSchema: schema,
      parseInput: value => value as { value: number }, parseOutput: value => value as { value: number }, projection,
    }, async (_context, value) => value);
    const runtime = await Harness.builder(contracts).tool(tool).grant("tool:call:echo_saved").model(model, {
      ...syntheticAccounting(contracts),
      prepare: scriptedPreparation(contracts, async function* (request) {
        if (modelStep++ === 0) {
          for (const [index, value] of metadata.entries()) {
            yield { kind: "tool_call" as const, callId: `call_${index}`, name: "echo_saved", arguments: { value: index }, metadata: value };
          }
        } else {
          const parts = request.messages.flatMap(message => typeof message.content === "string" ? []
            : Array.isArray(message.content) ? message.content : [message.content]);
          const exchange = parts.filter(part => part.kind === "tool_call" || part.kind === "tool_result");
          expect(exchange.map(part => part.kind)).toEqual([
            ...metadata.map(() => "tool_call"), ...metadata.map(() => "tool_result"),
          ]);
          expect(exchange.filter(part => part.kind === "tool_call").map(part => part.metadata)).toEqual(metadata);
          yield { kind: "content" as const, delta: "done" };
        }
        yield { kind: "completed" as const, metadata: {} };
      }),
      async reconcile() { return undefined; },
    }).build();
    try {
      const output = await host.runPrompt(runtime, "first");
      const receipts = output.receipts.filter(receipt => receipt.kind === "tool");
      const calls = host.conversation().messages.filter(message => message.kind === "tool_call");
      expect(receipts.map(receipt => receipt.metadata)).toEqual(metadata);
      for (const [index, call] of calls.entries()) {
        const saved = contracts.decodeCanonicalJson(await host.read(call.content));
        expect(saved).toEqual({ invocation: {
          operation_id: receipts[index]!.operationId, call_id: `call_${index}`, name: "echo_saved", arguments: { value: BigInt(index) },
        }, metadata: metadata[index] });
      }
      expect((await host.runPrompt(runtime, "second")).text).toBe("done");
      expect(modelStep).toBe(3);
    } finally {
      host.free();
    }
  }), { numRuns: 16 });
});
