import { expect, test } from "bun:test";
import { piProvider, type PiEvent } from "../src/index.ts";
import type { ModelRequest } from "@acyclic-labs/harness";

const request: ModelRequest = { model: { provider: "pi", name: "x", revision: "1", options: {} }, messages: [], tools: [] };
test("Pi adapter preserves typed source metadata in terminal events", async () => { const parts: readonly PiEvent<{ readonly session: string }>[] = [{ type: "text_delta", text: "ok" }, { type: "complete", metadata: { session: "s" } }]; const provider = piProvider({ request: value => value, run: async function* () { yield* parts; }, async reconcile() { return parts; } }); const values = []; for await (const event of provider.generate(request)) values.push(event); expect(values.at(-1)).toEqual({ kind: "completed", metadata: { session: "s" } }); expect(await provider.reconcile({ operationId: "o", step: 1, requestDigest: new Uint8Array(), observed: [] })).toEqual(values); });
