import { expect, test } from "bun:test";
import { aiSdkProvider, type AiSdkStreamPart } from "../src/index.ts";
import type { ModelRequest } from "@acyclic-labs/harness";

const request: ModelRequest = { model: { provider: "test", name: "typed", revision: "1", options: {} }, messages: [], tools: [] };
test("AI SDK projects every supported event without discarding completion metadata", async () => {
  const parts: readonly AiSdkStreamPart[] = [{ type: "text-delta", text: "hello" }, { type: "reasoning-delta", text: "why" }, { type: "tool-call", toolCallId: "1", toolName: "read", input: { path: "x" } }, { type: "finish", finishReason: "stop", usage: { totalTokens: 3 } }];
  const provider = aiSdkProvider({ model: () => "model", async streamText() { return { fullStream: (async function* () { yield* parts; })() }; }, async reconcile() { return undefined; } });
  const events = []; for await (const event of provider.generate(request)) events.push(event);
  expect(events.map(event => event.kind)).toEqual(["content", "reasoning", "tool_call", "completed"]);
  expect(events.at(-1)).toEqual({ kind: "completed", metadata: { finishReason: "stop", usage: { totalTokens: 3 }, providerMetadata: undefined } });
});

test("AI SDK forwards and observes cancellation", async () => {
  const controller = new AbortController(); let forwarded: AbortSignal | undefined;
  const provider = aiSdkProvider({ model: () => "model", streamText(input) { forwarded = input.abortSignal; return { fullStream: (async function* () { yield { type: "text-delta", text: "late" } as const; })() }; }, async reconcile() { return undefined; } });
  controller.abort(); await expect(async () => { for await (const _event of provider.generate({ ...request, signal: controller.signal })) { /* exhaust */ } }).toThrow(); expect(forwarded).toBeUndefined();
});
