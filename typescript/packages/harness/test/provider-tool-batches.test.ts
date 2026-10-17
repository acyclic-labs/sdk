import { expect, test } from "bun:test";
import fc from "fast-check";
import {
  DEFAULT_LIMITS, Harness, ModelRegistry, NativeContracts, initializeModelProviders, jsonToolProjection,
} from "../src/index.js";

const contracts = await NativeContracts.create();
await initializeModelProviders();
const schema = { type: "object", properties: { value: { type: "integer" } }, required: ["value"], additionalProperties: false };
const projection = await jsonToolProjection(schema);

for (const adapter of ["chat", "gemini"] as const) {
  test(`local ${adapter} tool batches project all calls before all results`, async () => {
    await fc.assert(fc.asyncProperty(
      fc.array(fc.integer({ min: 1, max: 1024 }), { minLength: 2, maxLength: 8 }),
      async values => {
        let requests = 0;
        const calls = values.map((value, index) => ({ id: `call_${index}`, name: "echo", args: { value } }));
        const event = (value: unknown) => `data: ${JSON.stringify(value)}\n\n`;
        const registry = new ModelRegistry({}, DEFAULT_LIMITS, async (_url, init) => {
          if (!(init?.body instanceof Uint8Array)) throw new Error("expected Rust request bytes");
          const body = JSON.parse(new TextDecoder().decode(init.body));
          requests++;
          if (requests === 1) {
            return new Response(adapter === "chat"
              ? event({ choices: [{ delta: { tool_calls: calls.map((call, index) => ({ index, id: call.id,
                function: { name: call.name, arguments: JSON.stringify(call.args) } })) }, finish_reason: "tool_calls" }] }) + "data: [DONE]\n\n"
              : event({ candidates: [{ content: { parts: calls.map(call => ({ functionCall: call,
                thoughtSignature: `signature-${call.id}` })) }, finishReason: "STOP" }] }));
          }
          expect(requests).toBe(2);
          if (adapter === "chat") {
            expect(body.messages.map((message: { role: string }) => message.role))
              .toEqual(["user", "assistant", ...values.map(() => "tool")]);
            expect(body.messages[1].tool_calls.map((call: { id: string }) => call.id)).toEqual(calls.map(call => call.id));
            expect(body.messages.slice(2).map((result: { tool_call_id: string }) => result.tool_call_id)).toEqual(calls.map(call => call.id));
          } else {
            expect(body.contents.map((message: { role: string }) => message.role)).toEqual(["user", "model", "user"]);
            expect(body.contents[1].parts.map((part: { functionCall: { id: string } }) => part.functionCall.id)).toEqual(calls.map(call => call.id));
            expect(body.contents[2].parts.map((part: { functionResponse: { id: string } }) => part.functionResponse.id)).toEqual(calls.map(call => call.id));
            expect(body.contents[1].parts.map((part: { thoughtSignature: string }) => part.thoughtSignature)).toEqual(calls.map(call => `signature-${call.id}`));
          }
          return new Response(adapter === "chat"
            ? event({ choices: [{ delta: { content: "done" }, finish_reason: "stop" }] }) + "data: [DONE]\n\n"
            : event({ candidates: [{ content: { parts: [{ text: "done" }] }, finishReason: "STOP" }] }));
        });
        registry.register("fixture", {
          adapter, authentication: "optional", base_url: "https://example.test/v1", max_event_bytes: 4096,
          models: { model: { capacity: { context_tokens: 8192, output_tokens: 512 }, features: { tools: true, images: true } } },
          token_counter: { path: "count_tokens", adapter, response_field: "input_tokens", framing_tokens: 0 },
        });
        const selected = registry.resolve("fixture/model");
        try {
          const runtime = await Harness.builder(contracts).model(selected.model, selected)
            .tool({ name: "echo", revision: "1", description: "Echo", inputSchema: schema, outputSchema: schema,
              parseInput: value => value, parseOutput: value => value, projection,
              handler: async (_context, value) => value }).grant("tool:call:echo").build();
          expect((await runtime.run("echo the values")).text).toBe("done");
          expect(requests).toBe(2);
        } finally {
          selected.free();
          registry.free();
        }
      },
    ), { numRuns: 16 });
  });
}
