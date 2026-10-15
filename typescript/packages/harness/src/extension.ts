/** Version-pinned, ref-only extension state shared with the Rust reducer. */
import type { FileRef } from "./conversation.js";
import type { Authority, EventReference } from "./index.js";
import { NativeContracts } from "./native-contracts.js";
import type { ExtensionForkPolicy } from "./enums.js";
export type { ExtensionForkPolicy } from "./enums.js";

/** Exact installed revision selected for new admissions or required by another extension. */
export interface ExtensionDependency {
  readonly name: string;
  readonly version: number;
}

/** Exact process-local executable identity, matching Rust's `ExtensionIdentity`. */
export interface ExtensionIdentity extends ExtensionDependency {
  readonly digest: readonly number[];
}

/** Validates an executable identity through the native dependency contract. */
export async function extensionIdentity(value: ExtensionIdentity): Promise<ExtensionIdentity> {
  const dependency = (await NativeContracts.create()).validate("extension_dependency", { name: value.name, version: value.version });
  if (!Array.isArray(value.digest) || value.digest.length !== 32
    || value.digest.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)
    || value.digest.every(byte => byte === 0)) {
    throw new TypeError("extension implementation digest must be a nonzero 32-byte value");
  }
  return Object.freeze({ ...dependency, digest: Object.freeze([...value.digest]) });
}

/** Validated, immutable configuration revision for one installed version. */
export interface ExtensionConfiguration {
  readonly extension: ExtensionDependency;
  readonly schema_digest: readonly number[];
  readonly content: FileRef;
}

/** Committed selection snapshot retained by a task across later activations. */
export interface ExtensionAdmission {
  readonly source: Readonly<{ authority: Authority<"agent">; revision: bigint }>;
  readonly selected: readonly ExtensionDependency[];
  readonly configurations: readonly ExtensionConfiguration[];
}

export interface ExtensionRecord {
  readonly name: string;
  readonly version: number;
  readonly schema_digest: readonly number[];
  readonly implementation_digest: readonly number[];
  readonly fork_policy: ExtensionForkPolicy;
  readonly content: FileRef;
}

/** Retained source and new state revision; admission verifies the exact latest source. */
export interface ExtensionStateMigration {
  readonly previous: EventReference;
  readonly record: ExtensionRecord;
}
