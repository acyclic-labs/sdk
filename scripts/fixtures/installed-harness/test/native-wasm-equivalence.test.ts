import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { Harness } from "@acyclic-labs/harness";

const fixture = JSON.parse(readFileSync(new URL("../native-wasm-event-v2.json", import.meta.url), "utf8")) as {
  event_wire_hex: string;
};

test("WASM event bytes match the native cross-language fixture", async () => {
  const authority = { kind: "conversation", id: "conversation-1" } as const;
  const harness = await Harness.create({
    authority,
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
  });
  const scope = harness.issueScope("root", ["conversation:bind", "conversation:append"]);
  const agent = harness.identity("agent", "04040404-0404-0404-0404-040404040404");
  const content = harness.validateFileRef({
    volume: { provider: { namespace: "fixture", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: agent } },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: harness.fileDescriptor(new TextEncoder().encode("fixture text"), "text/plain"),
    display_name: "input.txt",
  });
  harness.apply({
    authority,
    operation_id: harness.identity("operation", "01010101-0101-0101-0101-010101010101"),
    idempotency_key: "bind-1", expected_revision: 0n, scope, causal_parent: null,
    action: { kind: "bind_conversation", agent },
  });
  const command = {
    authority,
    operation_id: harness.identity("operation", "02020202-0202-0202-0202-020202020202"),
    idempotency_key: "append-2",
    expected_revision: 1n,
    scope,
    causal_parent: null,
    action: {
      kind: "append_conversation_message" as const,
      message: { id: harness.conversationMessageId("03030303-0303-0303-0303-030303030303"), sequence: 1,
        kind: "user", content, attachments: { kind: "inline", items: [] },
        reply_to: null, tool_call_id: null, extensions: {} },
    },
  };
  const first = harness.apply(command);
  const replayed = harness.apply(command);
  expect(first.result).toBe("applied");
  expect(replayed.result).toBe("replayed");
  expect(first.eventWire).toEqual(replayed.eventWire);
  expect(Buffer.from(first.eventWire).toString("hex")).toBe(fixture.event_wire_hex);
  harness.free();
});
