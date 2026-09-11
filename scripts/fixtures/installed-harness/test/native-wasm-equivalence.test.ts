import { expect, test } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import { readFileSync } from "node:fs";
import { Harness, type Event, type OperationId } from "@acyclic-labs/harness";
import { AggregateKind, EventEnvelopeSchema } from "@acyclic-labs/harness/proto";

const fixture = JSON.parse(readFileSync(new URL("../native-wasm-event-v1.json", import.meta.url), "utf8")) as {
  event_wire_hex: string;
};

test("WASM event bytes match the native cross-language fixture", async () => {
  const harness = await Harness.create({
    authority: { kind: "conversation", id: "conversation-1" },
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
    schemas: [{
      name: "example.message",
      version: 1,
      schema: {
        type: "object",
        required: ["text"],
        properties: { text: { type: "string" } },
      },
    }],
  });
  const scope = harness.issueScope("root", ["event:append"]);
  const command = {
    authority: { kind: "conversation" as const, id: "conversation-1" },
    operation_id: "01010101-0101-0101-0101-010101010101" as OperationId,
    idempotency_key: "append-1",
    expected_revision: 0n,
    scope,
    causal_parent: null,
    action: {
      kind: "append_custom" as const,
      schema: "example.message",
      version: 1,
      value: { text: "hello" },
    },
  };
  const first = harness.apply(command);
  const replayed = harness.apply(command);
  expect(first.result).toBe("applied");
  expect(replayed.result).toBe("replayed");
  const encode = (event: Event) => toBinary(EventEnvelopeSchema, create(EventEnvelopeSchema, {
    protocol: harness.protocolIdentity(),
    authority: { kind: AggregateKind.CONVERSATION, id: event.authority.id },
    revision: event.revision,
    operationId: event.operationId,
    intentDigest: event.intentDigest,
    scope: {
      id: event.scope.id,
      capabilities: [...event.scope.capabilities],
      issuer: event.scope.issuer,
      parentProof: new Uint8Array(event.scope.parent_proof ?? []),
      proof: new Uint8Array(event.scope.proof),
    },
    eventType: "custom",
    canonicalPayloadJson: new TextEncoder().encode(JSON.stringify(event.payload)),
  }));
  const firstBytes = encode(first.event);
  expect(firstBytes).toEqual(encode(replayed.event));
  expect([...firstBytes].map(byte => byte.toString(16).padStart(2, "0")).join("")).toBe(fixture.event_wire_hex);
  harness.free();
});
