import type { Harness, HarnessOptions, NativeContracts } from "../src/index.js";
export function exerciseCheckpoint(HarnessClass: typeof Harness, harness: Harness,
  options: HarnessOptions, contracts: NativeContracts): Promise<void>;
