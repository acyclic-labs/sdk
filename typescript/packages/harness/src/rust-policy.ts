import {
  validateAuthorityPathSegment as validateAuthorityPathSegmentWasm,
  validateComponentLabel as validateComponentLabelWasm,
} from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureHarnessWasm } from "./wasm-runtime.js";

// Start package-owned initialization without turning one transient fetch
// failure into a permanently rejected ESM module. Async public factories call
// `ensureHarnessWasm()` again and surface their own failure for retry.
try {
  await ensureHarnessWasm();
} catch {
  // The next Rust-backed async operation owns retry and error reporting.
}

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
