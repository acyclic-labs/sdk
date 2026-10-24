import { validateComponentLabel as validateComponentLabelWasm } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";

export async function validateComponentLabel(value: string, field: string): Promise<void> {
  await ensureHarnessWasm();
  try {
    validateComponentLabelWasm(value);
  } catch {
    throw new TypeError(`${field} is invalid`);
  }
}
