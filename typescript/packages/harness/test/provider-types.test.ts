import { test, expect } from "bun:test";
import type { AgentFileRef } from "../src/index.js";
import type { WasmAgentFileRef } from "../generated/wasm/acyclic_harness_wasm.js";

// The public client and its host byte reader share the generated exact reference.
function readers(owner: {read(file: AgentFileRef): Promise<Uint8Array>}) {
  const read = (file: AgentFileRef) => owner.read(file);
  const wireRead: (file: WasmAgentFileRef) => Promise<Uint8Array> = read;
  return {wireRead};
}

function invalidOwnerShape(file: AgentFileRef, agent: string) {
  // @ts-expect-error A project volume cannot acquire an agent owner.
  const wrong: WasmAgentFileRef = {...file, volume: {...file.volume, class: "project", owner: {kind: "agent", id: agent}}};
  // Control: the former uncorrelated shape accepts precisely the invalid combination.
  const legacy: {class: "project" | "agent_private"; owner: {kind: "project" | "agent"; id: string}} =
    {class: "project", owner: {kind: "agent", id: agent}};
  return {wrong, legacy};
}

test("provider reader type probes remain in the strict type suite", () => {
  expect(typeof readers).toBe("function");
  expect(typeof invalidOwnerShape).toBe("function");
});
