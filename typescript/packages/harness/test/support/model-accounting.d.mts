import type { NativeContracts } from "../../src/native-contracts.js";
import type { ModelProvider } from "../../src/model.js";

/** Synthetic test accounting shared by TypeScript and actual browser fixtures. */
export declare function syntheticAccounting(contracts: NativeContracts): Pick<ModelProvider, "contextCapacity" | "countTokens">;
