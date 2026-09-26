/** Explicit, ref-only selection from canonical conversation history into model context. */
import { DEFAULT_LIMITS, decodeAttachmentManifest,
  verifyFileBytes, type Attachment, type ConversationMessageId, type ConversationState, type FileRef, type Limits } from "./conversation.js";
import { validateToolName, type ModelContentPart, type ModelMessage } from "./model.js";
import { NativeContracts } from "./native-contracts.js";
import type { ContentBindings } from "./runtime.js";

export interface ModelContextSelection {
  /** Exact conversation tail observed when this selection was made. */
  readonly conversationRevision: bigint;
  /** Ordered subset of canonical message IDs. */
  readonly messageIds: readonly ConversationMessageId[];
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
  /** Optional owner decoder; its result must match Rust's canonical decoding exactly. */
  readonly decodeManifest?: (manifest: FileRef, bytes: Uint8Array, count: number) => readonly Attachment[] | Promise<readonly Attachment[]>;
  /** Optional additional owning-provider validation; never bypasses Rust. */
  readonly validateMessage?: (message: ConversationState["messages"][number]) => ConversationState["messages"][number] | Promise<ConversationState["messages"][number]>;
}

export interface FileProjectionOptions {
  /** Must enforce the owner's read grant before returning exact bytes. */
  readonly resolveFile?: (file: FileRef) => Promise<Uint8Array>;
  /** Optional additional owner verification; Rust always verifies the descriptor. */
  readonly verifyFile?: (file: FileRef, bytes: Uint8Array) => void | Promise<void>;
  readonly maxResolvedBytes?: number;
}

/** Adapts an exact-volume content binding into a verified model resolver. */
export function verifiedContentResolver(
  content: Pick<ContentBindings, "validate" | "verify" | "read">, limits: Limits,
): (file: FileRef) => Promise<Uint8Array> {
  const validate = content.validate.bind(content);
  const verify = content.verify.bind(content);
  const read = content.read.bind(content);
  return async file => {
    const reference = (await NativeContracts.create()).validate("file_ref", file);
    validate(reference, limits);
    const bytes = await read(reference);
    if (!(bytes instanceof Uint8Array)) throw new TypeError("content provider returned invalid bytes");
    verify(reference, bytes);
    await verifyFileBytes(reference, bytes);
    return Uint8Array.from(bytes);
  };
}

export type ProjectedFile =
  | Readonly<{ kind: "reference"; text: string }>
  | Readonly<{ kind: "text"; text: string }>
  | Readonly<{ kind: "image"; mediaType: string; bytes: Uint8Array }>;

/** Shared fail-closed file boundary for default model adapters. */
export async function projectModelFile(
  part: Extract<ModelContentPart, { kind: "file" }>, options: FileProjectionOptions,
): Promise<ProjectedFile> {
  const file = (await NativeContracts.create()).validate("file_ref", part.file);
  if (part.policy === "reference") {
    const hash = file.descriptor.sha256.map(byte => byte.toString(16).padStart(2, "0")).join("");
    return { kind: "reference", text: `[file: ${file.display_name}; ${file.descriptor.media_type}; sha256=${hash}; bytes=${file.descriptor.byte_length}]` };
  }
  if (part.policy !== "native" && part.policy !== "bounded_full") {
    throw new TypeError("unsupported file projection policy");
  }
  const nativeImage = part.policy === "native"
    && ["image/png", "image/jpeg", "image/gif", "image/webp"].includes(file.descriptor.media_type);
  const boundedText = part.policy === "bounded_full" && file.descriptor.media_type.startsWith("text/");
  if (!nativeImage && !boundedText) {
    throw new TypeError("unsupported file content for selected projection policy");
  }
  const limit = options.maxResolvedBytes ?? 1_048_576;
  if (!Number.isSafeInteger(limit) || limit < 0) throw new TypeError("maxResolvedBytes must be a non-negative safe integer");
  if (options.resolveFile === undefined) throw new TypeError("file byte resolution is unavailable");
  if (file.descriptor.byte_length > limit) throw new TypeError("file exceeds projection byte limit");
  const bytes = await options.resolveFile(file);
  if (!(bytes instanceof Uint8Array) || bytes.byteLength > limit) {
    throw new TypeError("resolved file exceeds projection byte limit");
  }
  await options.verifyFile?.(file, bytes);
  await verifyFileBytes(file, bytes);
  if (nativeImage) {
    return { kind: "image", mediaType: file.descriptor.media_type, bytes: Uint8Array.from(bytes) };
  }
  if (boundedText) {
    return { kind: "text", text: new TextDecoder("utf-8", { fatal: true }).decode(bytes) };
  }
  throw new TypeError("unsupported file content for selected projection policy");
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
  const maxManifestBytes = options.maxManifestBytes ?? 1_048_576;
  const maxAttachments = options.maxAttachments ?? 256;
  const maxMessages = options.maxMessages ?? 256;
  const maxProjectedAttachments = options.maxProjectedAttachments ?? 1_022;
  const maxRenderBytes = options.maxRenderBytes ?? 128 * 1024;
  if (!Number.isSafeInteger(maxManifestBytes) || maxManifestBytes < 0
    || !Number.isSafeInteger(maxAttachments) || maxAttachments < 0
    || !Number.isSafeInteger(maxMessages) || maxMessages <= 0
    || !Number.isSafeInteger(maxProjectedAttachments) || maxProjectedAttachments < 0 || maxProjectedAttachments > 1_022
    || !Number.isSafeInteger(maxRenderBytes) || maxRenderBytes <= 0) {
    throw new TypeError("invalid projection limits");
  }
  if (selection.messageIds.length > maxMessages) throw new TypeError("selected message count exceeds projection limit");
  const byId = new Map(conversation.messages.map(message => [message.id, message]));
  if (byId.size !== conversation.messages.length) throw new TypeError("conversation has duplicate message identities");
  const selected: ModelMessage[] = [];
  const toolCalls = new Map<ConversationMessageId, { callId: string; name: string }>();
  const contracts = await NativeContracts.create();
  let previousSequence = 0n;
  for (const id of selection.messageIds) {
    const raw = byId.get(id);
    if (raw === undefined) throw new TypeError("selected conversation message is missing");
    const canonical = contracts.validate("conversation_message", raw, DEFAULT_LIMITS);
    const message = options.validateMessage === undefined ? canonical
      : contracts.validate("conversation_message", await options.validateMessage(canonical), DEFAULT_LIMITS);
    if (message.sequence <= previousSequence || message.sequence > conversation.revision) {
      throw new TypeError("model context selection is not ordered and unique");
    }
    previousSequence = message.sequence;
    if (message.kind === "tool_call") {
      const invocation = await readJsonArtifact(message.content, options.resolveFile, maxRenderBytes);
      if (!isRecord(invocation) || typeof invocation.call_id !== "string"
        || typeof invocation.name !== "string" || invocation.call_id !== message.tool_call_id
        || !invocation.name) {
        throw new TypeError("tool call artifact identity does not match its record");
      }
      validateToolName(invocation.name);
      toolCalls.set(message.id, { callId: invocation.call_id, name: invocation.name });
      selected.push({ role: "assistant", content: { kind: "tool_call", callId: invocation.call_id,
        name: invocation.name, arguments: invocation.arguments } });
      continue;
    }
    if (message.kind === "tool_result") {
      // The complete output remains an artifact; the model sees only the
      // separately bounded projection attachment.
      if (message.content.descriptor.media_type !== "application/json") {
        throw new TypeError("tool result artifact type is invalid");
      }
      const callId = message.tool_call_id;
      const linked = message.reply_to === null ? undefined : toolCalls.get(message.reply_to);
      if (callId === null || linked === undefined || linked.callId !== callId) {
        throw new TypeError("selected tool result lacks its call");
      }
      const artifacts = await resolveAttachments(message.attachments, options.resolveManifest ?? options.resolveFile,
        maxManifestBytes, maxAttachments, options.decodeManifest);
      const projection = artifacts.find(artifact => artifact.label === "model_projection");
      if (projection === undefined) throw new TypeError("tool result lacks its model projection");
      const value = await readJsonArtifact(projection.file, options.resolveFile, maxRenderBytes);
      selected.push({ role: "tool", content: { kind: "tool_result", callId, name: linked.name, value } });
      continue;
    }
    if (message.kind !== "system" && message.kind !== "user" && message.kind !== "assistant") {
      throw new TypeError(`message kind ${message.kind} requires a specialized model projection`);
    }
    const primaryType = message.content.descriptor.media_type;
    const parts: ModelContentPart[] = [{
      kind: "file", file: message.content,
      policy: ["image/png", "image/jpeg", "image/gif", "image/webp"].includes(primaryType) ? "native"
        : primaryType.startsWith("text/") && message.content.descriptor.byte_length <= maxRenderBytes
          ? "bounded_full" : "reference",
    }];
    const attachments = await resolveAttachments(message.attachments, options.resolveManifest ?? options.resolveFile,
      maxManifestBytes, maxAttachments, options.decodeManifest);
    if (attachments.length > maxAttachments) throw new TypeError("selected message exceeds attachment projection limit");
    for (const attachment of attachments.slice(0, maxProjectedAttachments)) {
      const type = attachment.file.descriptor.media_type;
      parts.push({ kind: "file", file: attachment.file, policy: ["image/png", "image/jpeg", "image/gif", "image/webp"].includes(type) ? "native" : "reference" });
    }
    if (attachments.length > maxProjectedAttachments) {
      parts.push({ kind: "text", text: `[${attachments.length - maxProjectedAttachments} additional attachments omitted from this bounded model context]` });
    }
    selected.push({ role: message.kind, content: parts });
  }
  return Object.freeze({
    selection: Object.freeze({ conversationRevision: selection.conversationRevision, messageIds: Object.freeze([...selection.messageIds]) }),
    messages: Object.freeze(selected),
  });
}

async function readJsonArtifact(file: FileRef,
  resolver: ConversationProjectionOptions["resolveFile"], maxBytes: number): Promise<unknown> {
  const reference = (await NativeContracts.create()).validate("file_ref", file);
  if (reference.descriptor.media_type !== "application/json"
    || reference.descriptor.byte_length > maxBytes) {
    throw new TypeError("tool artifact type or rendering limit is invalid");
  }
  if (resolver === undefined) throw new TypeError("tool artifact resolution is unavailable");
  const bytes = await resolver(reference);
  if (!(bytes instanceof Uint8Array) || bytes.byteLength > maxBytes) {
    throw new TypeError("resolved tool artifact exceeds rendering limit");
  }
  await verifyFileBytes(reference, bytes);
  return (await NativeContracts.create()).decodeModelJson(bytes);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

async function resolveAttachments(
  value: ConversationState["messages"][number]["attachments"],
  resolver: ((file: FileRef) => Promise<Uint8Array>) | undefined,
  maxBytes: number,
  maxAttachments: number,
  decode: ConversationProjectionOptions["decodeManifest"],
): Promise<readonly Attachment[]> {
  const validated = (await NativeContracts.create()).validate("attachments", value);
  if (validated.kind === "inline") return validated.items;
  if (validated.item_count > maxAttachments) throw new TypeError("selected message exceeds attachment projection limit");
  if (validated.manifest.descriptor.byte_length > maxBytes) throw new TypeError("attachment manifest exceeds projection limit");
  if (resolver === undefined) throw new TypeError("attachment manifest resolution is unavailable");
  const bytes = await resolver(validated.manifest);
  if (!(bytes instanceof Uint8Array) || bytes.byteLength > maxBytes) {
    throw new TypeError("resolved attachment manifest exceeds projection limit");
  }
  const canonical = await decodeAttachmentManifest(validated.manifest, bytes, validated.item_count);
  if (decode !== undefined) {
    const ownerDecoded = await decode(validated.manifest, bytes, validated.item_count);
    const contracts = await NativeContracts.create();
    const canonicalBytes = contracts.encodeAttachmentManifest(canonical);
    const ownerBytes = contracts.encodeAttachmentManifest(ownerDecoded);
    if (canonicalBytes.length !== ownerBytes.length
      || canonicalBytes.some((byte, index) => byte !== ownerBytes[index])) {
      throw new TypeError("owner attachment decoder disagrees with canonical manifest");
    }
  }
  return canonical;
}
