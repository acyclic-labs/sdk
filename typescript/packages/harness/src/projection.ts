import type * as NativeModel from "../generated/wasm/acyclic_harness_wasm.js";
/** Explicit, ref-only selection from canonical conversation history into model context. */
import { DEFAULT_LIMITS,
  type Attachment, type ConversationMessageId, type ConversationState, type FileRef, type Limits } from "./conversation.js";
import type { ModelContent, ModelContentPart, ModelDataPart, ModelMessage } from "./model.js";
import { NativeContracts, type NativeFileRef } from "./native-contracts.js";
import type { ContentBindings } from "./content.js";
import {
  HARNESS_PROJECTION_DEFAULT_MAX_ATTACHMENTS,
  HARNESS_PROJECTION_DEFAULT_MAX_MANIFEST_BYTES,
  HARNESS_PROJECTION_DEFAULT_MAX_MESSAGES,
  HARNESS_PROJECTION_DEFAULT_MAX_RENDER_BYTES,
  HARNESS_PROJECTION_DEFAULT_MAX_RESOLVED_BYTES,
  HARNESS_PROJECTION_MAX_JSON_BYTES,
  HARNESS_PROJECTION_MAX_PROJECTED_ATTACHMENTS,
} from "./limits-contract.js";

export interface ModelContextSelection {
  /** Exact conversation tail observed when this selection was made. */
  readonly conversationRevision: bigint;
  /** Ordered subset of canonical message IDs. */
  readonly messageIds: readonly ConversationMessageId[];
  /** Pinned owner-published canonical base; stage contributions are separate. */
  readonly checkpoint?: FileRef;
}

/** A provider-proven view may contain less than the complete durable history. */
export interface ProjectableConversation extends ConversationState {
  readonly revision: bigint;
}

export interface SelectedModelContext {
  readonly selection: ModelContextSelection;
  /** Volatile provider-neutral projection; never append this to conversation history. */
  readonly messages: readonly ModelMessage[];
}

export interface ConversationProjectionOptions {
  /** Owner-mediated read; possession of a manifest ref is not authorization. */
  readonly resolveManifest?: (file: FileRef) => Promise<Uint8Array>;
  readonly maxManifestBytes?: number;
  readonly maxAttachments?: number;
  readonly maxMessages?: number;
  /** Bound the actual model request even when canonical history has a large referenced list. */
  readonly maxProjectedAttachments?: number;
  /** Owner-mediated exact reads for structured tool artifacts. */
  readonly resolveFile?: (file: FileRef) => Promise<Uint8Array>;
  readonly maxRenderBytes?: number;
  /** Additional message bounds from the owning conversation host. */
  readonly limits?: Limits;
}

export interface FileProjectionOptions {
  /** Must enforce the owner's read grant before returning exact bytes. */
  readonly resolveFile?: (file: FileRef) => Promise<Uint8Array>;
  readonly maxResolvedBytes?: number;
}

/** Adapts an exact-volume content binding into a verified model resolver. */
export function verifiedContentResolver(
  content: Pick<ContentBindings, "validate" | "read">, limits: Limits,
): (file: FileRef) => Promise<Uint8Array> {
  const validate = content.validate.bind(content);
  const read = content.read.bind(content);
  return async file => {
    const contracts = await NativeContracts.create();
    const reference = contracts.validate("file_ref", file);
    validate(reference, limits);
    const bytes = await read(reference);
    if (!(bytes instanceof Uint8Array)) throw new TypeError("content provider returned invalid bytes");
    contracts.verifyFileBytes(reference, bytes);
    return Uint8Array.from(bytes);
  };
}

/** Shared fail-closed file boundary for default model adapters. */
export async function projectModelFile(
  part: Extract<ModelContentPart, { kind: "file" }>, options: FileProjectionOptions,
): Promise<string> {
  const contracts = await NativeContracts.create();
  const file = contracts.validate("file_ref", part.file);
  if (part.policy === "reference") {
    const hash = file.descriptor.sha256.map(byte => byte.toString(16).padStart(2, "0")).join("");
    return `[file: ${file.display_name}; ${file.descriptor.media_type}; sha256=${hash}; bytes=${file.descriptor.byte_length}]`;
  }
  if (part.policy !== "bounded_full") {
    throw new TypeError("unsupported file projection policy");
  }
  if (!file.descriptor.media_type.startsWith("text/")) {
    throw new TypeError("unsupported file content for selected projection policy");
  }
  const limit = options.maxResolvedBytes ?? HARNESS_PROJECTION_DEFAULT_MAX_RESOLVED_BYTES;
  if (!Number.isSafeInteger(limit) || limit < 0) throw new TypeError("maxResolvedBytes must be a non-negative safe integer");
  if (options.resolveFile === undefined) throw new TypeError("file byte resolution is unavailable");
  if (file.descriptor.byte_length > limit) throw new TypeError("file exceeds projection byte limit");
  const bytes = await options.resolveFile(file);
  if (!(bytes instanceof Uint8Array) || bytes.byteLength > limit) {
    throw new TypeError("resolved file exceeds projection byte limit");
  }
  contracts.verifyFileBytes(file, bytes);
  return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
}

export async function selectModelContext(
  conversation: ProjectableConversation,
  selection: ModelContextSelection,
  options: ConversationProjectionOptions,
): Promise<SelectedModelContext> {
  if (typeof conversation.revision !== "bigint" || conversation.revision < 0n
    || typeof selection.conversationRevision !== "bigint"
    || selection.conversationRevision !== conversation.revision) {
    throw new TypeError("model context selection has a stale conversation revision");
  }
  const maxManifestBytes = options.maxManifestBytes ?? HARNESS_PROJECTION_DEFAULT_MAX_MANIFEST_BYTES;
  const maxAttachments = options.maxAttachments ?? HARNESS_PROJECTION_DEFAULT_MAX_ATTACHMENTS;
  const maxMessages = options.maxMessages ?? HARNESS_PROJECTION_DEFAULT_MAX_MESSAGES;
  const maxProjectedAttachments = options.maxProjectedAttachments ?? maxAttachments;
  const maxRenderBytes = options.maxRenderBytes ?? HARNESS_PROJECTION_DEFAULT_MAX_RENDER_BYTES;
  if (!Number.isSafeInteger(maxManifestBytes) || maxManifestBytes < 0
    || !Number.isSafeInteger(maxAttachments) || maxAttachments < 0
    || !Number.isSafeInteger(maxMessages) || maxMessages <= 0
    || !Number.isSafeInteger(maxProjectedAttachments) || maxProjectedAttachments < 0
      || maxProjectedAttachments > HARNESS_PROJECTION_MAX_PROJECTED_ATTACHMENTS
    || !Number.isSafeInteger(maxRenderBytes) || maxRenderBytes <= 0) {
    throw new TypeError("invalid projection limits");
  }
  const contracts = await NativeContracts.create();
  if (selection.messageIds.length > maxMessages) throw new TypeError("selected message count exceeds projection limit");
  const byId = new Map(conversation.messages.map(message => [message.id, message]));
  if (byId.size !== conversation.messages.length) throw new TypeError("conversation has duplicate message identities");
  const validated = new Map<ConversationMessageId, ConversationState["messages"][number]>();
  for (const id of selection.messageIds) {
    const raw = byId.get(id);
    if (raw === undefined) throw new TypeError("selected conversation message is missing");
    const canonical = contracts.validate("conversation_message", raw, DEFAULT_LIMITS);
    validated.set(id, options.limits === undefined ? canonical
      : contracts.validate("conversation_message", canonical, options.limits));
  }
  const projectedMessages = conversation.messages.map(message => validated.get(message.id) ?? message);
  const nativeConversation = { agent: conversation.agent, messages: projectedMessages };
  const nativeSelection = { conversation_revision: selection.conversationRevision, message_ids: selection.messageIds,
    ...(selection.checkpoint === undefined ? {} : { checkpoint: selection.checkpoint }),
  };
  contracts.validateModelContextSelection(nativeConversation, nativeSelection);
  const files = await captureProjectionFiles(projectedMessages,
    selection.messageIds, options, contracts, maxManifestBytes, maxAttachments, maxRenderBytes);
  const projected = await contracts.selectModelContext(
    nativeConversation,
    nativeSelection,
    files, Math.min(maxMessages, 0xffff_ffff), Math.min(maxAttachments, 0xffff_ffff), maxRenderBytes, maxProjectedAttachments,
  );
  return Object.freeze({
    selection: Object.freeze({
      conversationRevision: projected.selection.conversation_revision,
      messageIds: Object.freeze([...projected.selection.message_ids]),
      ...(projected.selection.checkpoint === undefined ? {} : { checkpoint: publicFile(projected.selection.checkpoint) }),
    }),
    messages: Object.freeze(projected.messages.map(message => Object.freeze({ role: message.role, content: publicModelContent(message.content) }))),
  });
}

async function captureProjectionFiles(
  messages: readonly ConversationState["messages"][number][],
  selectedIds: readonly ConversationMessageId[],
  options: ConversationProjectionOptions,
  contracts: NativeContracts,
  maxManifestBytes: number,
  maxAttachments: number,
  maxRenderBytes: number,
): Promise<Map<string, Uint8Array>> {
  const files = new Map<string, Uint8Array>();
  const byId = new Map(messages.map(message => [message.id, message]));
  const fileKey = (file: FileRef): string => new TextDecoder().decode(contracts.encodeCanonicalJson(file));
  const capture = async (
    file: FileRef,
    resolver: ConversationProjectionOptions["resolveFile"],
    label: string,
    limit: number,
    ceiling?: number,
  ) => {
    if (resolver === undefined) throw new TypeError(`${label} resolution is unavailable`);
    const reference = contracts.validate("file_ref", file);
    if (ceiling !== undefined && reference.descriptor.byte_length > ceiling) {
      throw new TypeError(`${label} exceeds JSON byte limit`);
    }
    if (reference.descriptor.byte_length > limit) throw new TypeError(`${label} exceeds its rendering limit`);
    const bytes = await resolver(reference);
    if (!(bytes instanceof Uint8Array)) throw new TypeError(`${label} resolver returned invalid bytes`);
    if (ceiling !== undefined && bytes.byteLength > ceiling) {
      throw new TypeError(`${label} exceeds JSON byte limit`);
    }
    if (bytes.byteLength > limit) throw new TypeError(`${label} exceeds its rendering limit`);
    contracts.verifyFileBytes(reference, bytes);
    const owned = Uint8Array.from(bytes);
    files.set(fileKey(reference), owned);
    return owned;
  };
  const manifestResolver = options.resolveManifest ?? options.resolveFile;
  const manifest = async (reference: FileRef, itemCount: number): Promise<readonly Attachment[]> => {
    if (itemCount > maxAttachments) throw new TypeError("selected message exceeds attachment projection limit");
    if (reference.descriptor.byte_length > maxManifestBytes) throw new TypeError("attachment manifest exceeds projection limit");
    const bytes = await capture(reference, manifestResolver, "attachment manifest", maxManifestBytes);
    return contracts.decodeAttachmentManifest(reference, bytes, itemCount);
  };
  for (const id of selectedIds) {
    const message = byId.get(id);
    if (message === undefined) throw new TypeError("selected conversation message is missing");
    if (message.kind === "tool_call") {
      await capture(message.content, options.resolveFile, "tool artifact", maxRenderBytes, HARNESS_PROJECTION_MAX_JSON_BYTES);
      continue;
    }
    let attachments: readonly Attachment[] | undefined;
    if (message.attachments.kind === "manifest") attachments = await manifest(message.attachments.manifest, message.attachments.item_count);
    else attachments = message.attachments.items;
    if (message.kind === "tool_result") {
      const projection = attachments.find(attachment => attachment.label === "model_projection");
      if (projection === undefined) throw new TypeError("tool result lacks its model projection");
      await capture(projection.file, options.resolveFile, "tool artifact", maxRenderBytes, HARNESS_PROJECTION_MAX_JSON_BYTES);
    }
  }
  return files;
}

function publicFile(file: NativeFileRef | Extract<NativeModel.ModelContentPart, { kind: "file" }>["file"]): FileRef {
  return { ...file,
    descriptor: { ...file.descriptor, byte_length: normalizeModelInteger(file.descriptor.byte_length) },
  } as FileRef;
}

/** Convert Rust-admitted content without reinterpreting option/event identities. */
export function publicModelContent(content: NativeModel.ModelContent): ModelContent {
  const publicFilePart = (part: Extract<NativeModel.ModelContentPart, { kind: "file" }>): Extract<ModelDataPart, { kind: "file" }> => {
    const file = publicFile(part.file);
    if (typeof part.policy === "string") return { ...part, file, policy: part.policy };
    const native = part.policy.native;
    const intent = "maximum_duration_ms" in native.intent
      ? { ...native.intent, maximum_duration_ms: normalizeModelInteger(native.intent.maximum_duration_ms) }
      : native.intent;
    const configuration = native.configuration === null ? null : { ...native.configuration,
      configuration: { ...native.configuration.configuration,
        content: publicFile(native.configuration.configuration.content) } };
    return { ...part, file, policy: { native: { ...native, intent, configuration,
      maximum_bytes: normalizeModelInteger(native.maximum_bytes),
      maximum_work: normalizeModelInteger(native.maximum_work) } } };
  };
  const mapContentPart = (part: NativeModel.ModelContentPart): ModelContentPart => {
    if (part.kind === "tool_call") {
      return { kind: part.kind, callId: part.call_id, name: part.name, arguments: normalizeModelJson(part.arguments) };
    }
    if (part.kind === "tool_result") {
      const content = part.content.kind === "json"
        ? { kind: "json" as const, value: normalizeModelJson(part.content.value) }
        : { kind: "parts" as const, parts: part.content.parts.map(data => data.kind === "file"
          ? publicFilePart(data) : data) as readonly ModelDataPart[] };
      return { kind: part.kind, callId: part.call_id, name: part.name, content };
    }
    if (part.kind === "file") {
      return publicFilePart(part);
    }
    return part;
  };
  if (typeof content === "string") return content;
  return Array.isArray(content) ? content.map(part => mapContentPart(part as NativeModel.ModelContentPart))
    : mapContentPart(content as NativeModel.ModelContentPart);
}

function normalizeModelJson(value: unknown): unknown {
  if (typeof value === "bigint") return normalizeModelInteger(value);
  if (typeof value === "number" && (!Number.isFinite(value) || Object.is(value, -0)
    || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
    throw new TypeError("model projection contains an inexact number");
  }
  if (Array.isArray(value)) return value.map(normalizeModelJson);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, normalizeModelJson(child)]));
  }
  return value;
}

function normalizeModelInteger(value: number | bigint): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) throw new TypeError("model projection integer exceeds JavaScript precision");
  return number;
}
