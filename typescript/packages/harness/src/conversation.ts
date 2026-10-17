/** Payload-free v2 conversation values. Rust owns admission and replay rules. */
import { NativeContracts } from "./native-contracts.js";
import type { MessageKind, VolumeClass, VolumeOperation } from "./enums.js";
export type { MessageKind, VolumeClass, VolumeOperation } from "./enums.js";
import type { AgentId, OperationId } from "./index.js";
import {
  HARNESS_LIMITS_DEFAULT,
  HARNESS_LIMITS_MAXIMUM,
  HARNESS_MAX_EXACT_JS_INTEGER,
  HARNESS_MAX_LABEL_BYTES,
  HARNESS_MAX_PATH_BYTES,
} from "./limits-contract.js";

export type VolumeOwner = import("../generated/wasm/acyclic_harness_wasm.js").WasmVolumeOwnerWire;
export type ProviderRef<Family extends string = string> =
  import("../generated/wasm/acyclic_harness_wasm.js").WasmProviderRefWire & Readonly<{family: Family}>;
export type VolumeRef<Class extends VolumeClass = VolumeClass, Family extends string = string> =
  import("../generated/wasm/acyclic_harness_wasm.js").WasmVolumeRefWire<Class, Family>;
export type FileDescriptor = import("../generated/wasm/acyclic_harness_wasm.js").WasmFileDescriptorWire;
export type FileRef<Class extends VolumeClass = VolumeClass, Family extends string = string> =
  import("../generated/wasm/acyclic_harness_wasm.js").WasmFileRefWire<Class, Family>;


export type TaskOutcomeRecord =
  | Readonly<{ kind: "succeeded"; result: FileRef }>
  | Readonly<{ kind: "failed"; message: string }>
  | Readonly<{ kind: "cancelled" }>
  | Readonly<{ kind: "indeterminate"; operation_id: OperationId }>;

/** Runtime-configurable bounds below the Rust-owned v2 wire ceilings. */
export interface Limits extends Readonly<typeof HARNESS_LIMITS_DEFAULT> {}

/** Rust-owned defaults for runtime configurable bounds. */
export const DEFAULT_LIMITS: Limits = HARNESS_LIMITS_DEFAULT;
/** Rust-owned protocol ceilings for each runtime configurable bound. */
export const MAX_LIMITS: Limits = HARNESS_LIMITS_MAXIMUM;
export const MAX_EXACT_JS_INTEGER = HARNESS_MAX_EXACT_JS_INTEGER;
export const MAX_LABEL_BYTES = HARNESS_MAX_LABEL_BYTES;
export const MAX_PATH_BYTES = HARNESS_MAX_PATH_BYTES;

export interface Attachment {
  readonly file: FileRef;
  readonly label: string | null;
}

export type ReferencedAttachments =
  | Readonly<{ kind: "inline"; items: readonly Attachment[] }>
  | Readonly<{ kind: "manifest"; manifest: FileRef; item_count: number }>;


declare const conversationMessageIdBrand: unique symbol;
export type ConversationMessageId = string & { readonly [conversationMessageIdBrand]: true };

interface ConversationMessageBase {
  readonly id: ConversationMessageId;
  /** Exact Rust u64 position; never round-tripped through a JavaScript number. */
  readonly sequence: bigint;
  readonly content: FileRef;
  readonly attachments: ReferencedAttachments;
  readonly extensions: Readonly<Record<string, FileRef>>;
}

/** Kind-specific linkage is checked statically before Rust validates the record. */
export type ConversationMessage<Kind extends MessageKind = MessageKind> = {
  readonly [Current in Kind]: ConversationMessageBase & Readonly<{
    kind: Current;
    reply_to: Current extends "tool_result" ? ConversationMessageId : ConversationMessageId | null;
    tool_call_id: Current extends "tool_call" | "tool_result" ? string : null;
  }>;
}[Kind];

export interface ConversationState {
  readonly agent: AgentId | null;
  readonly messages: readonly ConversationMessage[];
}

/** Bounded Rust reducer page; total_messages is the model-context revision. */
export interface ConversationPage extends ConversationState {
  readonly event_revision: bigint;
  readonly total_messages: bigint;
  /** Exclusive message-sequence cursor; null marks the final page. */
  readonly next_sequence: bigint | null;
}

export async function descriptorFor(bytes: Uint8Array, mediaType: string): Promise<FileDescriptor> {
  return (await NativeContracts.create()).fileDescriptor(bytes, mediaType);
}
