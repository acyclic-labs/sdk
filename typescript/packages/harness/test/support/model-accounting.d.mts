import type { NativeContracts } from "../../src/native-contracts.js";
import type { ModelProvider, ModelRequest, ModelTokenCount, ModelGeneration, ModelEvent } from "../../src/model.js";

/** Synthetic test accounting shared by TypeScript and actual browser fixtures. */
export declare function syntheticAccounting(contracts: NativeContracts): { contextCapacity: ModelProvider["contextCapacity"]; countTokens(request: ModelRequest & {serializedInput: Uint8Array}): Promise<ModelTokenCount> };

export declare function scriptedPreparation(contracts: NativeContracts, generate: (request: ModelRequest & {serializedInput: Uint8Array}, dispatch: ModelGeneration) => AsyncIterable<ModelEvent>): ModelProvider["prepare"];
