import { expect, test } from "bun:test";
import { openCodeProvider, type OpenCodePart } from "../src/index.ts";
import type { ModelRequest } from "@acyclic-labs/harness";

const request: ModelRequest = { model: { provider: "opencode", name: "x", revision: "1", options: {} }, messages: [], tools: [] };
test("OpenCode adapter maps its complete discriminated stream", async () => { const parts: readonly OpenCodePart[] = [{ type: "reasoning", delta: "r" }, { type: "tool", callId: "c", tool: "t", input: 1 }, { type: "done", metadata: { session: 2 } }]; const provider = openCodeProvider({ request: value => value, stream: async function* () { yield* parts; }, async reconcile() { return undefined; } }); const values = []; for await (const event of provider.generate(request)) values.push(event); expect(values.map(value => value.kind)).toEqual(["reasoning", "tool_call", "completed"]); });
