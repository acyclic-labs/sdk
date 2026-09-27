/** Payload-free v2 conversation values. Rust owns admission and replay rules. */
import { NativeContracts } from "./native-contracts.js";
import type { AgentId, OperationId } from "./index.js";
import {
  HARNESS_LIMITS_DEFAULT,
  HARNESS_LIMITS_MAXIMUM,
  HARNESS_MAX_EXACT_JS_INTEGER,
  HARNESS_MAX_LABEL_BYTES,
  HARNESS_MAX_PATH_BYTES,
} from "./limits-contract.js";

export type VolumeClass = "project" | "agent_private" | "session_shared";
export type VolumeOwner =
  | Readonly<{ kind: "project"; id: string }>
  | Readonly<{ kind: "agent"; id: AgentId }>
  | Readonly<{ kind: "session"; id: string }>;

export interface ProviderRef<Family extends string = string> {
  readonly namespace: string;
  readonly family: Family;
  readonly version: string;
}

interface VolumeBase<Family extends string> {
  readonly provider: ProviderRef<Family>;
  readonly id: string;
}

/** The volume class fixes its owner class at compile time as well as admission. */
export type VolumeRef<Class extends VolumeClass = VolumeClass, Family extends string = string> =
  Class extends "project" ? VolumeBase<Family> & Readonly<{ class: Class; owner: Extract<VolumeOwner, { kind: "project" }> }>
  : Class extends "agent_private" ? VolumeBase<Family> & Readonly<{ class: Class; owner: Extract<VolumeOwner, { kind: "agent" }> }>
  : VolumeBase<Family> & Readonly<{ class: Class; owner: Extract<VolumeOwner, { kind: "session" }> }>;

export interface FileDescriptor {
  readonly sha256: readonly number[];
  readonly byte_length: number;
  readonly media_type: string;
}

export interface FileRef<Class extends VolumeClass = VolumeClass, Family extends string = string> {
  readonly volume: VolumeRef<Class, Family>;
  readonly path: string;
  readonly version: string;
  readonly descriptor: FileDescriptor;
  readonly display_name: string;
}

export type ContentRef<Class extends VolumeClass = VolumeClass, Family extends string = string> = FileRef<Class, Family>;

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

export type MessageKind =
  | "user" | "assistant" | "system" | "tool_call" | "tool_result"
  | "interaction" | "permission" | "fork" | "merge";

declare const conversationMessageIdBrand: unique symbol;
export type ConversationMessageId = string & Readonly<{ readonly [conversationMessageIdBrand]: true }>;

/** Native UUID validation is the only public constructor for canonical message identities. */
export async function conversationMessageId(value: string): Promise<ConversationMessageId> {
  return (await NativeContracts.create()).validateConversationMessageId(value);
}

interface ConversationMessageBase {
  readonly id: ConversationMessageId;
  /** Exact Rust u64 position; never round-tripped through a JavaScript number. */
  readonly sequence: bigint;
  readonly content: ContentRef;
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

export type VolumeOperation = "read" | "write";

export async function descriptorFor(bytes: Uint8Array, mediaType: string): Promise<FileDescriptor> {
  return (await NativeContracts.create()).fileDescriptor(bytes, mediaType);
}

export async function verifyFileBytes(reference: FileRef, bytes: Uint8Array): Promise<void> {
  (await NativeContracts.create()).verifyFileBytes(reference, bytes);
}

/** Resolves a complete pinned attachment list, rejecting reordered fields and partial pages. */
export async function decodeAttachmentManifest(reference: FileRef, bytes: Uint8Array,
  itemCount: number): Promise<readonly Attachment[]> {
  return (await NativeContracts.create()).decodeAttachmentManifest(reference, bytes, itemCount);
}
