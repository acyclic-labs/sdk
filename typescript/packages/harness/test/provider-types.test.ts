import { test, expect } from "bun:test";
import { ModelRegistry, DEFAULT_LIMITS, type FileRef, type AgentId } from "../src/index.js";
import type { WasmFileRefWire } from "../generated/wasm/acyclic_harness_wasm.js";

// Compiled by tsconfig.type-tests: public and generated references share one contract.
function readers(owner: {read(file: FileRef): Promise<Uint8Array>}) {
  const read = (file: FileRef) => owner.read(file);
  const registry = new ModelRegistry({}, DEFAULT_LIMITS, undefined, read);
  const wireRead: (file: WasmFileRefWire) => Promise<Uint8Array> = read;
  return {registry, wireRead};
}

function invalidOwnerShape(file: FileRef, agent: AgentId) {
  // @ts-expect-error A project volume cannot acquire an agent owner.
  const wrong: WasmFileRefWire = {...file, volume: {...file.volume, class: "project", owner: {kind: "agent", id: agent}}};
  // Control: the former uncorrelated shape accepts precisely the invalid combination.
  const legacy: {class: "project" | "agent_private"; owner: {kind: "project" | "agent"; id: string}} =
    {class: "project", owner: {kind: "agent", id: agent}};
  return {wrong, legacy};
}

test("provider reader type probes remain in the strict type suite", () => {
  expect(typeof readers).toBe("function");
  expect(typeof invalidOwnerShape).toBe("function");
});
