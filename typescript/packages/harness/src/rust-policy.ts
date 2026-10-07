import {
  validateAuthorityPathSegment as validateAuthorityPathSegmentWasm,
  validateComponentLabel as validateComponentLabelWasm,
} from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";

// Public synchronous constructors are usable immediately after importing the
// package. ES module initialization completes the canonical Rust/WASM load
// before any caller can invoke these wrappers.
await ensureHarnessWasm();

export function validateAuthorityPathSegment(value: string): string {
  try {
    return validateAuthorityPathSegmentWasm(value);
  } catch {
    throw new TypeError("aggregate identity is not a safe path segment");
  }
}

export function isSafeAuthorityId(value: string): boolean {
  try {
    validateAuthorityPathSegmentWasm(value);
    return true;
  } catch {
    return false;
  }
}

export function validateComponentLabel(value: string, field: string): void {
  try {
    validateComponentLabelWasm(value);
  } catch {
    throw new TypeError(`${field} is invalid`);
  }
}
