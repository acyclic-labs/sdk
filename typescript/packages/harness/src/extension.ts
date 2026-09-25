/** Version-pinned, ref-only extension state shared with the Rust reducer. */
import type { FileRef } from "./conversation.js";
import { NativeContracts } from "./native-contracts.js";

export type ExtensionForkPolicy = "inherit" | "reset" | "reject";

export interface ExtensionRecord {
  readonly name: string;
  readonly version: number;
  readonly schema_digest: readonly number[];
  readonly implementation_digest: readonly number[];
  readonly fork_policy: ExtensionForkPolicy;
  readonly content: FileRef;
}

export async function extensionRecord(value: ExtensionRecord): Promise<ExtensionRecord> {
  return (await NativeContracts.create()).validate("extension_record", value);
}
