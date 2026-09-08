import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Harness, type OperationId } from "../src/index.js";

const wasm = readFileSync(
  fileURLToPath(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)),
);

test("native Rust semantics execute through WASM", async () => {
  const harness = await Harness.create({
    authority: { kind: "conversation", id: "conversation-1" },
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
    wasm,
    schemas: [
      {
        name: "example.message",
        version: 1,
        schema: {
          type: "object",
          required: ["text"],
          properties: { text: { type: "string" } },
        },
      },
    ],
  });
  const scope = harness.issueScope("root", ["event:append"]);
  const policyScope = harness.issueScopeWithPolicies("policy", [
    { level: "runtime", policy: { grants: ["event:append", "effect:run"], denies: [] } },
    { level: "invocation", policy: { grants: ["event:append"], denies: ["effect:run"] } },
  ]);
  expect(policyScope.capabilities).toEqual(["event:append"]);
  const first = harness.apply({
    authority: { kind: "conversation", id: "conversation-1" },
    operation_id: "01010101-0101-0101-0101-010101010101" as OperationId,
    idempotency_key: "append-1",
    expected_revision: 0n,
    scope,
    causal_parent: null,
    action: {
      kind: "append_custom",
      schema: "example.message",
      version: 1,
      value: { text: "hello" },
    },
  });
  expect(first.result).toBe("applied");
  const snapshot = harness.snapshot();
  expect(snapshot.revision).toBe(1n);
  expect(() => harness.attenuate(scope, "forged", ["effect:run"])).toThrow();
  const restored = await Harness.restore(snapshot, {
    authority: { kind: "conversation", id: "conversation-1" },
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
    wasm,
    schemas: [
      {
        name: "example.message",
        version: 1,
        schema: {
          type: "object",
          required: ["text"],
          properties: { text: { type: "string" } },
        },
      },
    ],
  });
  expect(restored.snapshot()).toEqual(snapshot);
  restored.free();
  harness.free();
});
