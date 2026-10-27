import type { AgentGuest, WasmAgents } from "../generated/wasm/acyclic_harness_wasm.js";

// A caller must be able to forward both host signals to the public guest entry.
// Compile against the real wasm-bindgen declarations, without an untyped fixture.
export function guestEntry(guest: AgentGuest): Parameters<typeof WasmAgents.openHosting>[4] {
  return (delivery: Uint8Array, signal: AbortSignal, interrupt: AbortSignal) =>
    guest.onRecord(delivery, signal, interrupt);
}
