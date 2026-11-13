import { expect, test } from "bun:test";
import { initializeClientQueries, PreparedAgentCommand,
  encodeCanonicalJson, type AgentCommand } from "@acyclic-labs/harness";

test("installed package resolves the public client and default WASM artifacts", async () => {
  await initializeClientQueries();
  const commands: AgentCommand[] = [
    { kind: "root" },
    { kind: "prompt", agent: "harness/agents/00000000000000000000000000000001",
      id: "original-prompt", body: "é".repeat(PreparedAgentCommand.maximumInvocationBytes()) },
  ];
  for (const command of commands) {
    const encoded = encodeCanonicalJson(command);
    if (command.kind === "prompt") {
      expect(encoded.length).toBeGreaterThan(PreparedAgentCommand.maximumInvocationBytes());
    }
    using prepared = new PreparedAgentCommand(encoded);
    expect(prepared.value()).toEqual(command);
    expect(prepared.bytes()).toEqual(encoded);
    const original = prepared.bytes();
    encoded.fill(0);
    expect(prepared.bytes()).toEqual(original);
  }
});

test("installed command admission refuses asserted identity and malformed input", async () => {
  await initializeClientQueries();
  expect(() => new PreparedAgentCommand(encodeCanonicalJson({ kind: "root", principal: "admin" }))).toThrow();
  expect(() => new PreparedAgentCommand(new Uint8Array([0]))).toThrow();
});
