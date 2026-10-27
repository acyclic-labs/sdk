import { expect, test } from "bun:test";
import { initializeClientQueries, PreparedAgentCommand,
  encodeCanonicalJson, type AgentCommand } from "@acyclic-labs/harness";

test("installed package resolves the public client and default WASM artifacts", async () => {
  await initializeClientQueries();
  const command: AgentCommand = { kind: "root" };
  const encoded = encodeCanonicalJson(command);
  using prepared = new PreparedAgentCommand(encoded);
  expect(prepared.value()).toEqual(command);
  expect(prepared.bytes()).toEqual(encoded);
  const original = prepared.bytes();
  encoded.fill(0);
  expect(prepared.bytes()).toEqual(original);
});

test("installed command admission refuses asserted identity and oversized input", async () => {
  await initializeClientQueries();
  expect(() => new PreparedAgentCommand(encodeCanonicalJson({ kind: "root", principal: "admin" }))).toThrow();
  expect(() => new PreparedAgentCommand(new Uint8Array(PreparedAgentCommand.maximumCommandBytes() + 1))).toThrow();
});
