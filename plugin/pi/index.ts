import { VERSION, SessionManager, parseSessionEntries, createReadToolDefinition, createWriteToolDefinition,
  createEditToolDefinition, truncateHead, formatSize, DEFAULT_MAX_BYTES, convertToPng, resizeImage,
  formatDimensionNote, type ReadToolInput, type ReadToolDetails, type ExtensionAPI,
  type ExtensionContext, type SessionEntry } from "@earendil-works/pi-coding-agent";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { closeSync, fstatSync, openSync, readSync } from "node:fs";
import { promisify } from "node:util";
import { setTimeout as delay } from "node:timers/promises";
import { isAbsolute, join, resolve } from "node:path";
import { homedir } from "node:os";
import { fileURLToPath } from "node:url";
import { Type } from "typebox";

// The installer replaces this JSON string; no shell resolves it.
const executable = "__ACYCLIC_EXECUTABLE__";
const execute = promisify(execFile);
const maximumContextEntries = 4096, maximumContextBytes = 512 * 1024, maximumBuiltinBytes = 262144;
const scopedPiControls = new Set([
  "SpawnCommit", "TaskInspect", "TaskDiff", "TaskImport", "TaskWriteback", "TaskWritebackStatus",
  "TaskSend", "TaskSteer", "TaskReceive", "TaskWait", "NativeBuiltinRead", "NativeBuiltinWrite", "NativeBuiltinEdit",
]);

/** Capture the actual pre-tool native branch under finite bounds. */
export function captureParentCut(
  session: ExtensionContext["sessionManager"],
  toolCallId: string,
  maximumEntries: number,
  maximumBytes: number,
) {
  if (!toolCallId || !Number.isSafeInteger(maximumEntries) || maximumEntries <= 0 ||
      !Number.isSafeInteger(maximumBytes) || maximumBytes <= 0) {
    throw new Error("Pi context capture requires an identity and finite positive bounds");
  }
  const entries: string[] = [];
  const seen = new Set<string>();
  let next = session.getLeafId();
  const sourceLeafId = next;
  let owner: { id: string; timestamp: string; parentId: string | null } | undefined;
  let bytes = 2; // JSON array delimiters.
  while (next !== null) {
    if (seen.size >= maximumEntries || seen.has(next)) {
      throw new Error("Pi context parent chain is cyclic or exceeds its entry bound");
    }
    seen.add(next);
    const entry = session.getEntry(next);
    if (!entry || entry.id !== next) throw new Error("Pi context parent entry is missing");
    const matches = entry.type === "message" && entry.message.role === "assistant"
      ? entry.message.content.filter(part => part.type === "toolCall" && part.id === toolCallId).length
      : 0;
    if (matches) {
      if (owner || matches !== 1) throw new Error("Spawn tool identity is duplicated in the current Pi branch");
      owner = { id: entry.id, timestamp: entry.timestamp, parentId: entry.parentId };
    } else if (owner) {
      const encoded = JSON.stringify(entry);
      bytes += Buffer.byteLength(encoded) + (entries.length ? 1 : 0);
      if (bytes > maximumBytes) throw new Error("Pi context exceeds its byte bound");
      entries.push(encoded);
    }
    next = entry.parentId;
  }
  if (!owner) throw new Error("Spawn invocation is absent from the current Pi branch");
  entries.reverse();
  const json = `[${entries.join(",")}]`;
  if (bytes > maximumBytes) throw new Error("Pi context exceeds its byte bound");
  return {
    mode: "caller_tool" as const,
    parentSessionId: session.getSessionId(),
    parentSessionFile: session.getSessionFile() ?? null,
    parentCwd: session.getCwd(),
    sourceLeafId,
    ownerEntryId: owner.id,
    parentLeafId: owner.parentId,
    capturedAt: owner.timestamp,
    entriesJson: json,
    sha256: createHash("sha256").update(json).digest("hex"),
  };
}

/** Revalidate the pinned branch immediately before admission. */
export function assertParentCutCurrent(session: ExtensionContext["sessionManager"], toolCallId: string,
  cut: ReturnType<typeof captureParentCut>, maximumEntries: number, maximumBytes: number) {
  const current = captureParentCut(session, toolCallId, maximumEntries, maximumBytes);
  if (Object.entries(current).some(([key, value]) => value !== cut[key as keyof typeof cut])) throw new Error("Pi parent context changed before spawn admission");
}

/** Create a separate native child from fresh or authenticated pinned context. */
export function nativeChildSession(session: ExtensionContext["sessionManager"], childCwd: string,
  callerCut?: ReturnType<typeof captureParentCut>) {
  if (callerCut && (callerCut.mode !== "caller_tool" || callerCut.parentSessionId !== session.getSessionId() ||
      callerCut.parentSessionFile !== (session.getSessionFile() ?? null) || callerCut.parentCwd !== session.getCwd())) throw new Error("Pi inherited context must originate from the actual caller native session");
  if (callerCut && createHash("sha256").update(callerCut.entriesJson).digest("hex") !== callerCut.sha256) throw new Error("Pinned Pi context digest differs");
  const entries: SessionEntry[] = callerCut ? JSON.parse(callerCut.entriesJson) : [];
  if (!Array.isArray(entries)) throw new Error("Pinned Pi context is not an entry list");
  let previous: string | null = null;
  const ids = new Set<string>();
  for (const entry of entries) {
    if (!entry || typeof entry.id !== "string" || ids.has(entry.id) || entry.parentId !== previous ||
        (entry as { type: string }).type === "session") throw new Error("Pinned Pi context is not the selected parent chain");
    ids.add(entry.id); previous = entry.id;
  }
  if (previous !== (callerCut ? callerCut.parentLeafId : null)) throw new Error("Pinned Pi context leaf differs");
  const manager = SessionManager.inMemory(childCwd, undefined, entries), header = manager.getHeader();
  if (!header) throw new Error("Pi SDK omitted the independent child header");
  const cut = callerCut ?? {
    mode: "scratch" as const, parentSessionId: session.getSessionId(), parentSessionFile: session.getSessionFile() ?? null,
    parentCwd: session.getCwd(), sourceLeafId: null, ownerEntryId: null, parentLeafId: null,
    capturedAt: header.timestamp, entriesJson: "[]", sha256: createHash("sha256").update("[]").digest("hex"),
  };
  if (manager.getSessionId() === cut.parentSessionId) throw new Error("Child Pi session must have its own identity");
  const pending = new Set<string>();
  for (const message of manager.buildSessionContext().messages) {
    if (message.role === "assistant") {
      if (pending.size) throw new Error("Pinned Pi context has an incomplete tool exchange");
      for (const part of message.content) if (part.type === "toolCall") {
        if (pending.has(part.id)) throw new Error("Pinned Pi context repeats a tool identity");
        pending.add(part.id);
      }
    } else if (message.role === "toolResult") {
      if (!pending.delete(message.toolCallId)) throw new Error("Pinned Pi context has an orphan tool result");
    } else if (pending.size) throw new Error("Pinned Pi context interrupts a tool exchange");
  }
  if (pending.size) throw new Error("Pinned Pi context has an incomplete tool exchange");
  return { cut, native_session_id: manager.getSessionId(), session_bytes:
    [{ ...header, timestamp: cut.capturedAt }, ...manager.getEntries()].map(entry => JSON.stringify(entry)).join("\n") + "\n" };
}

const privateControlKeys = new Set([
  "caller", "callerproof", "proof", "environment", "tasklease", "signer", "runtime", "operator",
  "operatorscope", "operatorkey", "bearer", "authority",
]);
/** Remove private fields recursively without inspecting text payloads. */
export function visibleControlResult(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(visibleControlResult);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value).filter(([key]) => !privateControlKeys.has(key.replaceAll("_", "").toLowerCase()))
    .map(([key, entry]) => [key, visibleControlResult(entry)]));
}
/** Missing acknowledgement never authorizes a second effect. */
export class PiControlAcknowledgementError extends Error {
  constructor(readonly operation?: string) {
    super(`Pi service acknowledgement unavailable${operation ? ` for original operation ${JSON.stringify(operation)}` : ""}; outcome uncertain, retain this operation and inspect its status`);
  }
}
/** Retain the original invocation and rejection for each SDK operation. */
export function piToolOperations() {
  const invocations = new Map<string, { request: string; result: Promise<Record<string, unknown>> }>();
  return (session: string, operation: string, fields: Readonly<Record<string, unknown>>, invoke: () => Promise<Record<string, unknown>>) => {
    if (!operation) throw new Error("Pi controls require the original SDK tool call identity");
    const key = JSON.stringify([session, operation]), request = JSON.stringify(fields), retained = invocations.get(key);
    if (retained) {
      if (retained.request !== request) throw new Error("Pi operation identity was reused with changed arguments");
      return retained.result;
    }
    if (invocations.size >= 4096) throw new Error("Pi retained control invocation bound exceeded");
    const result = Promise.resolve().then(invoke).catch(error => {
      if (error instanceof PiControlAcknowledgementError) throw new PiControlAcknowledgementError(operation);
      throw error;
    });
    invocations.set(key, { request, result }); return result;
  };
}
/** Poll only observations of the original task or operation. */
export async function observePiOperation(operation: string, observe: (signal: AbortSignal) => Promise<Record<string, unknown>>,
  signal?: AbortSignal, bounds = { maximumObservations: 120, timeoutMs: 30_000, intervalMs: 250 },
  terminal = (result: Record<string, unknown>) => {
    if (result.state === "completed") {
      if (result.outcome === null || result.outcome === undefined) throw new Error("Pi completed task observation omitted its actual terminal outcome");
      return true;
    }
    if (result.state !== "pending") throw new Error("Pi task wait returned an invalid observation state");
    return false;
  }, initial?: Record<string, unknown>) {
  if (!operation || !Number.isSafeInteger(bounds.maximumObservations) || bounds.maximumObservations <= 0 ||
      !Number.isSafeInteger(bounds.timeoutMs) || bounds.timeoutMs <= 0 ||
      !Number.isSafeInteger(bounds.intervalMs) || bounds.intervalMs < 0) throw new Error("Pi native wait requires an original operation and finite observation bounds");
  signal?.throwIfAborted();
  const deadline = new AbortController(), waiting = signal ? AbortSignal.any([signal, deadline.signal]) : deadline.signal;
  const timeout = setTimeout(() => deadline.abort(new DOMException("Pi polling deadline exceeded", "TimeoutError")), bounds.timeoutMs);
  let last = initial, observations = 0;
  try {
    while (observations < bounds.maximumObservations) {
      waiting.throwIfAborted();
      const result = await observe(waiting);
      waiting.throwIfAborted(); observations++;
      if (result.uncertain || terminal(result)) return result;
      last = result;
      if (observations < bounds.maximumObservations) await delay(bounds.intervalMs, undefined, { signal: waiting });
    }
  } catch (error) {
    signal?.throwIfAborted();
    if (!deadline.signal.aborted) throw error;
    if (!last) throw new PiControlAcknowledgementError(operation);
  } finally { clearTimeout(timeout); }
  return { ...last, wait: { bounded: true, observations } };
}

/** Prepare once, then observe human consent and the original publication. */
export async function approvePiWriteback(operation: string,
  fields: { source: string; root?: string; paths: readonly string[]; confirm: boolean },
  ctx: Pick<ExtensionContext, "hasUI" | "sessionManager"> & { ui: Pick<ExtensionContext["ui"], "confirm">; model?: ExtensionContext["model"] },
  control: (fields: Readonly<Record<string, unknown>>) => Promise<Record<string, unknown>>, signal: AbortSignal | undefined,
  observation: {
    prepared: (descriptor: Readonly<Record<string, unknown>>) => void;
    observe: (fields: Readonly<Record<string, unknown>>, signal: AbortSignal) => Promise<Record<string, unknown>>;
    decide?: (operation: string, digest: string, decision: "approve" | "decline") => Promise<void>;
    bounds?: { maximumObservations: number; timeoutMs: number; intervalMs: number };
  }) {
  signal?.throwIfAborted();
  const session = ctx.sessionManager, native = session.getSessionId(), file = session.getSessionFile(), leaf = session.getLeafId();
  const model = ctx.model && { provider: ctx.model.provider, id: ctx.model.id };
  const current = () => {
    signal?.throwIfAborted();
    if (ctx.sessionManager !== session || session.getSessionId() !== native || session.getSessionFile() !== file ||
        session.getLeafId() !== leaf || ctx.model?.provider !== model?.provider || ctx.model?.id !== model?.id) throw new Error("Pi writeback native session, branch or model changed during approval");
  };
  const prepared = await control({ operation, source: fields.source, paths: fields.paths, confirm: fields.confirm,
    ...(fields.root === undefined ? {} : { root: fields.root }) });
  current();
  if (prepared.uncertain) return prepared;
  if (prepared.status !== "prepared" || prepared.operation !== operation ||
      typeof prepared.approval_digest !== "string" || !prepared.approval_digest || prepared.approval_digest.length > 4096 ||
      typeof prepared.source !== "string" || !prepared.source || typeof prepared.root !== "string" || !prepared.root ||
      (fields.root !== undefined && prepared.root !== fields.root) || JSON.stringify(prepared.paths) !== JSON.stringify(fields.paths)) throw new Error("Pi writeback omitted its exact original prepared approval digest or selection");
  const digest = prepared.approval_digest;
  if (ctx.hasUI) {
    const approved = await ctx.ui.confirm("Approve native root writeback",
      `Operation: ${operation}\nSource: ${prepared.source}\nRoot: ${prepared.root}\nSelected paths: ${JSON.stringify(fields.paths)}\nPrepared approval digest: ${digest}`, { signal });
    current();
    if (!observation.decide) throw new Error("Pi native UI approval requires the trusted host operator transport");
    await observation.decide(operation, digest, approved ? "approve" : "decline"); current();
  }
  const descriptor = { operation, source: prepared.source, root: prepared.root, paths: [...fields.paths], approval_digest: digest };
  if (!ctx.hasUI) observation.prepared(descriptor);
  current();
  let last: Record<string, unknown> = { ...descriptor, status: "pending", approval: "required" };
  try {
    return await observePiOperation(operation, async waiting => {
      current();
      const result = await observation.observe({ operation, source: fields.source, root: prepared.root, paths: fields.paths }, waiting);
      current(); waiting.throwIfAborted();
      if (result.uncertain) return { ...last, uncertain: result.uncertain };
      if (result.operation !== operation || result.source !== prepared.source || result.root !== prepared.root ||
          result.approval_digest !== digest || JSON.stringify(result.paths) !== JSON.stringify(fields.paths)) throw new Error("Pi writeback observation changed its original prepared selection");
      if (result.status === "published") {
        if (!result.receipt || typeof result.receipt !== "object" || (result.receipt as Record<string, unknown>).phase !== "Applied") throw new Error("Pi writeback publication omitted its actual Applied receipt");
        return result;
      }
      if (result.status === "declined") return result;
      if (result.status !== "pending" && result.status !== "prepared") throw new Error("Pi writeback returned an invalid passive observation");
      return last = { ...result, status: "pending", approval: "required" };
    }, signal, observation.bounds ?? { maximumObservations: 240, timeoutMs: 60_000, intervalMs: 250 },
    result => result.status === "published" || result.status === "declined", last);
  } finally { current(); }
}
export interface PiInboxMessage { task_id: string; sequence: number; message_id: string; kind: "message" | "steer" }
const inboxMessageType = "acyclic_mail";
/** Advance only from the own-task receipt in the actual persisted native branch. */
export function persistedPiInboxCursor(session: ExtensionContext["sessionManager"], task: string) {
  const candidates: [SessionEntry, number][] = [];
  const seen = new Set<string>();
  let next = session.getLeafId();
  while (next !== null) {
    if (seen.size >= maximumContextEntries || seen.has(next)) throw new Error("Pi inbox branch exceeds its finite entry bound");
    seen.add(next);
    const entry = session.getEntry(next);
    if (!entry) throw new Error("Pi inbox branch has a missing native entry");
    if (entry.type === "custom_message" && entry.customType === inboxMessageType && entry.details && typeof entry.details === "object") {
      const details = entry.details as Record<string, unknown>;
      if (details.task_id === task) {
        if (!Number.isSafeInteger(details.sequence) || (details.sequence as number) <= 0 || typeof details.message_id !== "string" ||
            !details.message_id || (details.kind !== "message" && details.kind !== "steer")) throw new Error("Pi inbox native receipt is malformed");
        candidates.push([entry, details.sequence as number]);
      }
    }
    next = entry.parentId;
  }
  if (!candidates.length) return 0;
  const file = session.getSessionFile();
  if (!file) throw new Error("Pi inbox requires an actual persisted native session");
  let persisted: SessionManager;
  try {
    const native = session.getSessionId(), cwd = session.getCwd(), handle = openSync(file, "r");
    let bytes: Buffer, used = 0;
    try {
      const metadata = fstatSync(handle);
      if (!metadata.isFile() || metadata.size === 0 || metadata.size > maximumContextBytes) throw new Error("Pi native inbox session file is empty, nonregular or exceeds its byte bound");
      bytes = Buffer.alloc(maximumContextBytes + 1);
      while (used < bytes.length) {
        const count = readSync(handle, bytes, used, bytes.length - used, null);
        if (count === 0) break;
        used += count;
      }
      if (used > maximumContextBytes) throw new Error("Pi native inbox session file exceeds its byte bound");
    } finally { closeSync(handle); }
    const entries = parseSessionEntries(bytes.subarray(0, used).toString("utf8")), header = entries[0];
    if (!header || header.type !== "session" || header.version !== 3 || header.id !== native || header.cwd !== cwd) throw new Error("Pi persisted inbox session differs from the actual native session");
    persisted = SessionManager.inMemory(cwd, undefined, entries);
  } catch (error) {
    // Pi buffers initial entries until the first assistant; no bytes means no receipt.
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return 0;
    throw error;
  }
  let cursor = 0;
  for (const [entry, sequence] of candidates) if (JSON.stringify(persisted.getEntry(entry.id)) === JSON.stringify(entry)) cursor = Math.max(cursor, sequence);
  return cursor;
}
/** Queue one original item until its native receipt is persisted. */
export async function consumePiInbox(pi: Pick<ExtensionAPI, "sendMessage">, ctx: Pick<ExtensionContext, "sessionManager">,
  task: string, receive: (after: number, signal: AbortSignal) => Promise<Record<string, unknown>>, signal: AbortSignal,
  pending: { item?: PiInboxMessage }) {
  const native = ctx.sessionManager.getSessionId(), deadline = new AbortController(), polling = AbortSignal.any([signal, deadline.signal]);
  const timeout = setTimeout(() => deadline.abort(new DOMException("Pi inbox deadline exceeded", "TimeoutError")), 30_000);
  try {
    for (let count = 0; count < 120; count++) {
      polling.throwIfAborted();
      if (ctx.sessionManager.getSessionId() !== native) throw new Error("Pi inbox native session changed");
      const after = persistedPiInboxCursor(ctx.sessionManager, task);
      if (pending.item && after >= pending.item.sequence) pending.item = undefined;
      if (!pending.item) {
        const result = await receive(after, polling);
        polling.throwIfAborted();
        if (ctx.sessionManager.getSessionId() !== native) throw new Error("Pi inbox native session changed");
        if (result.uncertain) throw new PiControlAcknowledgementError();
        if (result.item !== undefined && result.item !== null) {
          if (typeof result.item !== "object" || !result.envelope || typeof result.envelope !== "object") throw new Error("Pi inbox omitted its original item or envelope");
          const item = result.item as Record<string, unknown>, envelope = result.envelope as Record<string, unknown>;
          if (item.task_id !== task || !Number.isSafeInteger(item.sequence) || (item.sequence as number) <= after ||
              typeof item.message_id !== "string" || !item.message_id || (envelope.kind !== "message" && envelope.kind !== "steer") ||
              typeof envelope.message !== "string" || Buffer.byteLength(envelope.message) > maximumContextBytes) throw new Error("Pi inbox returned another task or invalid original envelope");
          pending.item = { task_id: task, sequence: item.sequence as number, message_id: item.message_id, kind: envelope.kind };
          pi.sendMessage({ customType: inboxMessageType, content: envelope.message, display: true, details: pending.item },
            { triggerTurn: true, deliverAs: envelope.kind === "steer" ? "steer" : "followUp" });
        }
      }
      if (count < 119) await delay(250, undefined, { signal: polling });
    }
  } catch (error) { if (!signal.aborted && !deadline.signal.aborted) throw error; }
  finally { clearTimeout(timeout); }
}

function builtinPath(input: string, cwd: string) {
  if (typeof input !== "string" || !input || Buffer.byteLength(input) > 4096 || input.includes("\0")) throw new Error("Pi builtin requires a bounded file path");
  let path = input.replace(/[\u00A0\u2000-\u200A\u202F\u205F\u3000]/g, " ");
  if (path.startsWith("@")) path = path.slice(1);
  if (process.platform === "win32" && path.startsWith("/") && !path.startsWith("//") && !path.includes("\\")) {
    const drive = path.match(/^\/(?:mnt\/|cygdrive\/)?([a-z])(?:\/(.*))?$/i);
    if (drive) path = `${drive[1].toUpperCase()}:\\${drive[2]?.replaceAll("/", "\\") ?? ""}`;
  }
  if (path === "~") path = homedir();
  else if (path.startsWith("~/") || (process.platform === "win32" && path.startsWith("~\\"))) path = join(homedir(), path.slice(2));
  if (/^file:\/\//.test(path)) path = fileURLToPath(path);
  if (/^[a-z]:(?![\\/])/i.test(path)) throw new Error("Pi builtin rejects a drive-relative namespace");
  if (process.platform !== "win32" && (/^[a-z]:/i.test(path) || path.includes("\\") || path.startsWith("//"))) throw new Error("Pi builtin rejects a foreign drive or UNC namespace");
  if (path.split(/[\\/]/).includes("..")) throw new Error("Pi builtin rejects parent namespace traversal");
  const absolute = resolve(cwd, path);
  if (Buffer.byteLength(absolute) > 4096) throw new Error("Pi builtin expanded path exceeds its bound");
  return absolute;
}
function builtinBytes(result: Record<string, unknown>, operation: string) {
  if (result.uncertain) throw new PiControlAcknowledgementError(operation);
  const { content_base64: encoded, sha256: digest } = result;
  if (typeof encoded !== "string" || encoded.length > 4 * Math.ceil(maximumBuiltinBytes / 3) ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded) ||
      typeof digest !== "string" || !/^[a-f0-9]{64}$/.test(digest)) throw new Error("Pi builtin read omitted its bounded original byte response");
  const bytes = Buffer.from(encoded, "base64");
  if (bytes.length > maximumBuiltinBytes || createHash("sha256").update(bytes).digest("hex") !== digest) throw new Error("Pi builtin read byte response differs from its original digest");
  return { bytes, digest };
}
function builtinWritten(result: Record<string, unknown>, operation: string) {
  if (result.uncertain) throw new PiControlAcknowledgementError(operation);
  if (result.written !== true) throw new Error("Pi builtin write omitted its original completed write");
}
function builtinImageMime(bytes: Buffer) {
  const buffer = bytes.subarray(0, 4100);
  const ascii = (offset: number, text: string) => buffer.subarray(offset, offset + text.length).toString("latin1") === text;
  if (buffer.length >= 3 && buffer[0] === 255 && buffer[1] === 216 && buffer[2] === 255) return buffer[3] === 247 ? undefined : "image/jpeg";
  if (buffer.length >= 16 && buffer.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10])) && buffer.readUInt32BE(8) === 13 && ascii(12, "IHDR")) {
    for (let offset = 8; offset + 8 <= buffer.length;) {
      if (ascii(offset + 4, "acTL")) return undefined;
      if (ascii(offset + 4, "IDAT")) break;
      const next = offset + 12 + buffer.readUInt32BE(offset);
      if (next <= offset || next > buffer.length) break;
      offset = next;
    }
    return "image/png";
  }
  if (ascii(0, "GIF87a") || ascii(0, "GIF89a")) return "image/gif";
  if (ascii(0, "RIFF") && ascii(8, "WEBP")) return "image/webp";
  if (buffer.length >= 26 && ascii(0, "BM")) {
    const size = buffer.readUInt32LE(2), pixels = buffer.readUInt32LE(10), header = buffer.readUInt32LE(14);
    if ((size !== 0 && size < 26) || pixels < 14 + header || (size !== 0 && pixels >= size)) return undefined;
    const offset = header === 12 ? 22 : header >= 40 && header <= 124 && buffer.length >= 30 ? 26 : undefined;
    if (offset !== undefined && buffer.readUInt16LE(offset) === 1 && [1, 4, 8, 16, 24, 32].includes(buffer.readUInt16LE(offset + 2))) return "image/bmp";
  }
}
function builtinReadText(bytes: Buffer, input: ReadToolInput) {
  const lines = bytes.toString("utf8").split("\n"), start = input.offset ? Math.max(0, input.offset - 1) : 0;
  if (start >= lines.length) throw new Error(`Offset ${input.offset} is beyond end of file (${lines.length} lines total)`);
  const end = input.limit === undefined ? lines.length : Math.min(start + input.limit, lines.length);
  const truncation = truncateHead(lines.slice(start, end).join("\n"));
  let text = truncation.content, details: ReadToolDetails | undefined;
  if (truncation.firstLineExceedsLimit) {
    text = `[Line ${start + 1} is ${formatSize(Buffer.byteLength(lines[start], "utf8"))}, exceeds ${formatSize(DEFAULT_MAX_BYTES)} limit.]`;
    details = { truncation };
  } else if (truncation.truncated) {
    const last = start + truncation.outputLines, bound = truncation.truncatedBy === "bytes" ? ` (${formatSize(DEFAULT_MAX_BYTES)} limit)` : "";
    text += `\n\n[Showing lines ${start + 1}-${last} of ${lines.length}${bound}. Use offset=${last + 1} to continue.]`;
    details = { truncation };
  } else if (input.limit !== undefined && end < lines.length) text += `\n\n[${lines.length - end} more lines in file. Use offset=${end + 1} to continue.]`;
  return { content: [{ type: "text" as const, text }], details, structuredContent: text };
}

export default function acyclic(pi: ExtensionAPI) {
  if (VERSION !== "1.1.0") throw new Error(`Acyclic requires Pi 1.1.0 APIs; found ${VERSION}`);
  let caller: { session: string; proof: unknown } | undefined;
  let owner: { native: string; task: string; root: string } | undefined;
  let inboxController: AbortController | undefined, inboxWork: Promise<void> | undefined, inboxFailed = false;
  const pendingInbox: { item?: PiInboxMessage } = {};
  const control = async (operation: string, ctx: Pick<ExtensionContext, "cwd" | "sessionManager">,
    fields: Readonly<Record<string, unknown>> = {}, signal?: AbortSignal) => {
    signal?.throwIfAborted();
    const manager = ctx.sessionManager, native = manager.getSessionId(), environment = process.env.ACYCLIC_PI_CALLER;
    const original = owner;
    if (operation.startsWith("NativeBuiltin") && (!original || !original.task || !original.root || original.native !== native ||
        typeof fields.operation !== "string" || !fields.operation || Buffer.byteLength(fields.operation) > 4096 ||
        typeof fields.path !== "string")) throw new Error("Pi builtin has no authenticated original task and SDK invocation");
    if (operation !== "SessionStart" && caller?.session !== native && !environment) throw new Error("Pi native session changed before its caller was registered");
    let proof = caller?.session === native ? caller.proof : undefined;
    if (environment) {
      if (Buffer.byteLength(environment) > 1024 * 1024) throw new Error("Pi caller proof exceeds its byte bound");
      try { proof = JSON.parse(environment); } catch { throw new Error("Pi inherited caller proof is invalid"); }
    }
    const input = JSON.stringify({ ...fields, ...(proof === undefined ? {} : { caller: proof }), cwd: ctx.cwd, session_id: native });
    if (Buffer.byteLength(input) > 1024 * 1024) throw new Error("Pi control exceeds its byte bound");
    const invocation = execute(executable, ["__pi", operation], {
      cwd: ctx.cwd, timeout: 60_000, maxBuffer: 4 * 1024 * 1024, windowsHide: true, encoding: "utf8", ...(signal ? { signal } : {}),
    });
    invocation.child.stdin?.end(input);
    let result: Record<string, unknown>;
    try { result = JSON.parse((await invocation).stdout); } catch { throw new PiControlAcknowledgementError(); }
    const scoped = scopedPiControls.has(operation);
    if (!result || typeof result !== "object" || result.native_session !== native || typeof result.session !== "string" ||
        ctx.sessionManager.getSessionId() !== native || (!scoped && !Array.isArray(result.tasks))) throw new Error("Pi control returned another or changed native session");
    if (operation === "SessionStart") {
      if (typeof result.caller_task !== "string" || !result.caller_task) throw new Error("Pi startup omitted its authenticated original caller task");
      if (owner?.native === native && (owner.task !== result.caller_task || owner.root !== result.session)) throw new Error("Pi original caller task or root session changed");
      if (owner?.native !== native) { inboxController?.abort(); pendingInbox.item = undefined; inboxFailed = false; }
      owner = { native, task: result.caller_task, root: result.session };
    }
    if (scoped) {
      signal?.throwIfAborted();
      if (!owner || !owner.task || !owner.root || owner.native !== native || result.session !== owner.root ||
          (operation === "TaskWritebackStatus" ? result.current_task : result.caller_task) !== owner.task) throw new Error("Pi finite control response differs from its authenticated original task and native session");
      if (operation === "TaskWritebackStatus" && (!("operation" in result) || result.operation !== fields.operation ||
          !("source" in result) || result.source !== fields.source || !("root" in result) || result.root !== fields.root ||
          !("paths" in result) || JSON.stringify(result.paths) !== JSON.stringify(fields.paths))) throw new Error("Pi writeback status differs from its original Root task and prepared selection");
    }
    if ("caller" in result) caller = { session: native, proof: result.caller };
    if (operation.startsWith("NativeBuiltin")) {
      signal?.throwIfAborted();
      if (!original || ctx.sessionManager !== manager || manager.getSessionId() !== native || owner?.native !== native ||
          owner.task !== original.task || owner.root !== original.root || result.operation !== fields.operation || result.event !== operation ||
          typeof result.path !== "string" || (result.path !== fields.path && !(operation === "NativeBuiltinRead" && Array.isArray(fields.variants) && fields.variants.includes(result.path))) ||
          !["SOURCE", "PROJECT", "SCRATCH"].includes(result.volume_role as string) || (result.volume_role === "SOURCE" && operation !== "NativeBuiltinRead") ||
          (operation === "NativeBuiltinEdit" && (!("phase" in result) || result.phase !== fields.phase))) throw new Error("Pi builtin response differs from its original task, invocation or proven native namespace");
    }
    return visibleControlResult(result) as Record<string, unknown>;
  };
  const operations = piToolOperations();
  const toolResult = (result: Record<string, unknown>) => ({ content: [{ type: "text" as const, text: JSON.stringify(result) }], details: result });
  const effect = (event: string, id: string, fields: Readonly<Record<string, unknown>>, ctx: ExtensionContext, signal?: AbortSignal,
    invoke = () => control(event, ctx, { operation: id, ...fields }, signal)) => operations(ctx.sessionManager.getSessionId(), id, { control: event, ...fields }, invoke);
  pi.on("session_start", async (_event, ctx) => {
    const entrypoint = process.argv[1];
    if (!entrypoint || !isAbsolute(entrypoint)) throw new Error("Pi CLI entrypoint is not absolute");
    await control("SessionStart", ctx, { runtime: {
      version: VERSION, executable: process.execPath, executable_prefix: [entrypoint], extension: fileURLToPath(import.meta.url),
      provider_extension: process.env.ACYCLIC_PI_PROVIDER_EXTENSION ?? null,
      environment: Object.fromEntries(Object.entries(process.env).filter((entry): entry is [string, string] => typeof entry[1] === "string")),
    } });
  });
  pi.on("agent_start", (_event, ctx) => {
    inboxController?.abort();
    if (inboxFailed) return;
    const task = owner?.task;
    if (!task || owner?.native !== ctx.sessionManager.getSessionId()) throw new Error("Pi inbox has no authenticated original caller task");
    const controller = new AbortController(), previous = inboxWork;
    inboxController = controller;
    inboxWork = (async () => {
      if (previous) await previous;
      if (controller.signal.aborted) return;
      try { await consumePiInbox(pi, ctx, task, (after, signal) => control("TaskReceive", ctx, { after }, signal), controller.signal, pendingInbox); }
      catch {
        inboxFailed = true;
        ctx.ui.notify("Pi native inbox stopped after an unavailable acknowledgement or invalid delivery; original cursor retained, no automatic retry", "warning");
      }
    })();
  });
  pi.on("agent_end", () => { inboxController?.abort(); });
  pi.on("session_shutdown", async () => { inboxController?.abort(); if (inboxWork) await inboxWork; });
  const supportedModelTools = new Set([
    "read", "write", "edit", "acyclic_spawn", "acyclic_send", "acyclic_steer", "acyclic_wait", "acyclic_inspect",
    "acyclic_diff", "acyclic_import", "acyclic_writeback", "acyclic_model", "acyclic_context", "acyclic_agents", "acyclic_cancel",
  ]);
  pi.on("tool_call", (event, ctx) => {
    if (!supportedModelTools.has(event.toolName)) return { block: true, reason: "This Pi model tool has no original SDK-scoped IO adapter" };
    if (!event.toolCallId || Buffer.byteLength(event.toolCallId) > 4096 || owner?.native !== ctx.sessionManager.getSessionId() || !owner.task) {
      return { block: true, reason: "Pi tool has no authenticated current original invocation" };
    }
  });
  pi.registerTool({ ...createReadToolDefinition(process.cwd()), defaultActive: false,
    async execute(operation, input, signal, _update, ctx) {
      signal?.throwIfAborted();
      const path = builtinPath(input.path, ctx.cwd), alternatives = [path.replace(/ (AM|PM)\./gi, "\u202F$1."), path.normalize("NFD"), path.replace(/'/g, "\u2019"), path.normalize("NFD").replace(/'/g, "\u2019")];
      const variants = alternatives.filter((variant, index) => variant !== path && alternatives.indexOf(variant) === index);
      const { bytes } = builtinBytes(await control("NativeBuiltinRead", ctx, { operation, path, variants, maximum_bytes: maximumBuiltinBytes }, signal), operation);
      signal?.throwIfAborted();
      const mime = builtinImageMime(bytes);
      if (!mime) return builtinReadText(bytes, input);
      const nonVision = ctx.model && !ctx.model.input.includes("image") ? "\n[Current model does not support images. The image will be omitted from this request.]" : "";
      let imageBytes: Uint8Array = bytes, imageMime = mime;
      const hints: string[] = [];
      if (mime === "image/bmp") {
        const converted = await convertToPng(bytes.toString("base64"), mime); signal?.throwIfAborted();
        if (!converted) {
          const text = `Read image file [${mime}]\n[Image omitted: could not be converted to a supported inline image format.]${nonVision}`;
          return { content: [{ type: "text" as const, text }], details: undefined, structuredContent: text };
        }
        imageBytes = Buffer.from(converted.data, "base64"); imageMime = converted.mimeType;
        hints.push(`[Image converted from ${mime} to ${imageMime}.]`);
      }
      const resized = await resizeImage(imageBytes, imageMime, ctx.model?.inputLimits?.images?.resize); signal?.throwIfAborted();
      if (!resized) {
        const text = `Read image file [${mime}]\n[Image omitted: could not be resized below the inline image size limit.]${nonVision}`;
        return { content: [{ type: "text" as const, text }], details: undefined, structuredContent: text };
      }
      const dimensions = formatDimensionNote(resized);
      if (dimensions) hints.push(dimensions);
      const text = `Read image file [${resized.mimeType}]${hints.length ? "\n" + hints.join("\n") : ""}${nonVision}`;
      return { content: [{ type: "text" as const, text }, { type: "image" as const, data: resized.data, mimeType: resized.mimeType }],
        details: undefined, structuredContent: { type: "image", data: resized.data, mimeType: resized.mimeType, note: text } };
    },
  });
  pi.registerTool({ ...createWriteToolDefinition(process.cwd()), defaultActive: false,
    async execute(operation, input, signal, _update, ctx) {
      signal?.throwIfAborted();
      const path = builtinPath(input.path, ctx.cwd);
      if (typeof input.content !== "string" || Buffer.byteLength(input.content) > maximumBuiltinBytes) throw new Error("Pi builtin write content exceeds its original bounded file IO");
      builtinWritten(await control("NativeBuiltinWrite", ctx, { operation, path, content: input.content }, signal), operation);
      signal?.throwIfAborted();
      return { content: [{ type: "text" as const, text: `Successfully wrote to ${input.path}` }], details: undefined };
    },
  });
  pi.registerTool({ ...createEditToolDefinition(process.cwd()), defaultActive: false,
    async execute(operation, input, signal, update, ctx) {
      signal?.throwIfAborted();
      const path = builtinPath(input.path, ctx.cwd);
      let original: ReturnType<typeof builtinBytes> | undefined;
      const edit = createEditToolDefinition(ctx.cwd, { operations: {
        access: async () => {
          signal?.throwIfAborted();
          original = builtinBytes(await control("NativeBuiltinEdit", ctx, { operation, path, phase: "read", maximum_bytes: maximumBuiltinBytes }, signal), operation);
          signal?.throwIfAborted();
        },
        readFile: async () => { signal?.throwIfAborted(); if (!original) throw new Error("Pi edit has no original bounded read"); return original.bytes; },
        writeFile: async (_resolved, content) => {
          signal?.throwIfAborted();
          if (!original) throw new Error("Pi edit has no original compare-and-write digest");
          if (Buffer.byteLength(content) > maximumBuiltinBytes) throw new Error("Pi builtin edit exceeds its original bounded file IO");
          builtinWritten(await control("NativeBuiltinEdit", ctx, { operation, path, phase: "write", content, expected_sha256: original.digest }, signal), operation);
          signal?.throwIfAborted();
        },
      } });
      return await edit.execute(operation, input, signal, update, ctx);
    },
  });
  pi.registerTool({ name: "acyclic_spawn", label: "Spawn native Pi task",
    description: "Spawn a fresh child or inherit the caller's pre-tool branch with native_cut. Source selects workspace only.",
    parameters: Type.Object({
      id: Type.String({ minLength: 1, maxLength: 128, pattern: "^[A-Za-z0-9_.-]+$" }), prompt: Type.String({ minLength: 1, maxLength: 262144 }),
      source: Type.Optional(Type.String({ minLength: 1, maxLength: 256 })), root: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
      context: Type.Optional(Type.Union([Type.Literal("scratch"), Type.Literal("native_cut")], { default: "scratch" })),
    }, { additionalProperties: false }),
    async execute(operation, params, signal, _update, ctx) {
      const model = ctx.model;
      if (!model) throw new Error("Pi has no selected native provider/model");
      const fields = { ...params, context: params.context ?? "scratch", provider: model.provider, model: model.id };
      return toolResult(await operations(ctx.sessionManager.getSessionId(), operation, { control: "Spawn", ...fields }, async () => {
        const parentCut = fields.context === "native_cut" ? captureParentCut(ctx.sessionManager, operation, maximumContextEntries, maximumContextBytes) : undefined;
        const prepared = await control("SpawnPrepare", ctx, { operation, ...fields, ...(parentCut ? { cut: parentCut } : {}) }, signal);
        signal?.throwIfAborted();
        if (prepared.uncertain) return { ...prepared, operation };
        if (!prepared.child || typeof prepared.child !== "object") throw new Error("Pi service omitted the prepared child binding");
        const child = prepared.child as Record<string, unknown>;
        if (child.id !== params.id || typeof child.task !== "string" || typeof child.cwd !== "string" || !isAbsolute(child.cwd) ||
            typeof child.session_file !== "string" || !isAbsolute(child.session_file)) throw new Error("Pi service returned an invalid child binding");
        const childSession = nativeChildSession(ctx.sessionManager, child.cwd, parentCut);
        if (parentCut) assertParentCutCurrent(ctx.sessionManager, operation, parentCut, maximumContextEntries, maximumContextBytes);
        if (ctx.model?.provider !== model.provider || ctx.model?.id !== model.id) throw new Error("Pi selected native provider/model changed before spawn admission");
        return control("SpawnCommit", ctx, { operation, ...fields, cut: childSession.cut,
          native_session_id: childSession.native_session_id, session_bytes: childSession.session_bytes }, signal);
      }));
    },
  });
  for (const [name, label, event] of [["acyclic_send", "Send task message", "TaskSend"], ["acyclic_steer", "Steer native task", "TaskSteer"]] as const) {
    pi.registerTool({ name, label, description: "Publish one original message through the retained task mailbox; uncertain acknowledgements are not retried.",
      parameters: Type.Object({ id: Type.String({ minLength: 1, maxLength: 256 }), message: Type.String({ minLength: 1, maxLength: 262144 }) }, { additionalProperties: false }),
      async execute(operation, params, signal, _update, ctx) { return toolResult(await effect(event, operation, params, ctx, signal)); },
    });
  }
  pi.registerTool({ name: "acyclic_wait", label: "Observe native task completion", description: "Observe the original task for up to 120 polls/30 seconds.",
    parameters: Type.Object({ id: Type.String({ minLength: 1, maxLength: 256 }) }, { additionalProperties: false }),
    async execute(operation, params, signal, _update, ctx) {
      const native = ctx.sessionManager.getSessionId();
      return toolResult(await observePiOperation(operation, async waiting => {
        if (ctx.sessionManager.getSessionId() !== native) throw new Error("Pi native session changed during task wait");
        return control("TaskWait", ctx, { operation, ...params }, waiting);
      }, signal));
    },
  });
  for (const [name, label, event] of [["acyclic_inspect", "Inspect selected task files", "TaskInspect"], ["acyclic_diff", "Diff selected task files", "TaskDiff"]] as const) {
    pi.registerTool({ name, label, description: "Page a selected source directory with a pinned generation and directory cursor.",
      parameters: Type.Object({ id: Type.String({ minLength: 1, maxLength: 256 }), directory: Type.Optional(Type.String({ maxLength: 4096 })),
        root: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })), cursor: Type.Optional(Type.String({ minLength: 1, maxLength: 16384 })),
        maximum_entries: Type.Optional(Type.Integer({ minimum: 1, maximum: 256 })) }, { additionalProperties: false }),
      async execute(operation, params, signal, _update, ctx) { return toolResult(await control(event, ctx, { operation, ...params, maximum_entries: params.maximum_entries ?? 64 }, signal)); },
    });
  }
  const paths = Type.Array(Type.String({ minLength: 1, maxLength: 4096 }), { minItems: 1, maxItems: 256, uniqueItems: true });
  pi.registerTool({ name: "acyclic_import", label: "Import approved selected files", description: "Import explicitly selected files into an authorized private destination.",
    parameters: Type.Object({ source: Type.String({ minLength: 1, maxLength: 256 }), root: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
      destination: Type.String({ minLength: 1, maxLength: 256 }), paths, confirm: Type.Boolean() }, { additionalProperties: false }),
    async execute(operation, params, signal, _update, ctx) {
      if (!params.confirm) throw new Error("Pi selected file import requires explicit confirmation");
      return toolResult(await effect("TaskImport", operation, params, ctx, signal));
    },
  });
  pi.registerTool({ name: "acyclic_writeback", label: "Write back approved root files", description: "Prepare exact selected root files; publication requires native user approval of the digest.",
    parameters: Type.Object({ source: Type.String({ minLength: 1, maxLength: 256 }), paths, confirm: Type.Boolean(),
      root: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })) }, { additionalProperties: false }),
    async execute(operation, params, signal, update, ctx) {
      if (!params.confirm) throw new Error("Pi root writeback requires explicit confirmation");
      return toolResult(await effect("TaskWriteback", operation, params, ctx, signal,
        () => approvePiWriteback(operation, params, ctx, fields => control("TaskWriteback", ctx, fields, signal), signal, {
          prepared: descriptor => { update?.(toolResult({ customType: "acyclic.writeback.prepared", ...descriptor })); },
          observe: (fields, waiting) => control("TaskWritebackStatus", ctx, fields, waiting),
          decide: async (id, digest, decision) => {
            try {
              const { stdout } = await execute(executable, ["pi", "approve", "--session", ctx.sessionManager.getSessionId(),
                "--operation", id, "--digest", digest, "--decision", decision], { cwd: ctx.cwd, timeout: 60_000, maxBuffer: 4 * 1024 * 1024,
                windowsHide: true, encoding: "utf8", ...(signal ? { signal } : {}) });
              const acknowledgement: unknown = JSON.parse(stdout);
              if (!acknowledgement || typeof acknowledgement !== "object") throw new Error("invalid operator acknowledgement");
            } catch { throw new PiControlAcknowledgementError(id); }
          },
        })));
    },
  });
  pi.registerTool({ name: "acyclic_model", label: "Select native Pi model", description: "Observe or switch the native model.",
    parameters: Type.Object({ provider: Type.Optional(Type.String({ minLength: 1 })), model: Type.Optional(Type.String({ minLength: 1 })) }, { additionalProperties: false }),
    async execute(_operation, params, signal, _update, ctx) {
      signal?.throwIfAborted();
      if ((params.provider === undefined) !== (params.model === undefined)) throw new Error("Pi model selection requires both provider and model");
      if (params.provider !== undefined && params.model !== undefined) {
        const model = ctx.modelRegistry.find(params.provider, params.model);
        if (!model) throw new Error("Pi native model is not registered");
        if (!await pi.setModel(model)) throw new Error("Pi native model provider has no configured authentication");
      }
      return toolResult({ provider: ctx.model?.provider ?? null, model: ctx.model?.id ?? null });
    },
  });
  pi.registerTool({ name: "acyclic_context", label: "Observe native Pi context", description: "Observe context usage and selected native branch.",
    parameters: Type.Object({}, { additionalProperties: false }),
    async execute(_operation, _params, signal, _update, ctx) {
      signal?.throwIfAborted();
      return toolResult({ native_session_id: ctx.sessionManager.getSessionId(), leaf_id: ctx.sessionManager.getLeafId(),
        usage: ctx.getContextUsage() ?? null, provider: ctx.model?.provider ?? null, model: ctx.model?.id ?? null });
    },
  });
  pi.registerTool({ name: "acyclic_agents", label: "Acyclic tasks", description: "Observe retained task identities and durable outcomes.",
    parameters: Type.Object({}, { additionalProperties: false }),
    async execute(_id, _params, signal, _update, ctx) { return toolResult(await control("TaskStatus", ctx, {}, signal)); },
  });
  pi.registerTool({ name: "acyclic_cancel", label: "Cancel Acyclic task", description: "Cancel an original retained task and its subtree.",
    parameters: Type.Object({ task: Type.String({ format: "uuid" }) }, { additionalProperties: false }),
    async execute(_id, params, signal, _update, ctx) { return toolResult(await control("TaskCancel", ctx, params, signal)); },
  });
  pi.registerCommand("acyclic-doctor", {
    description: "Inspect the installed Acyclic service and platform qualification",
    async handler(_args, ctx) {
      let stdout: string;
      try { ({ stdout } = await execute(executable, ["doctor", "--json"], { timeout: 20_000, maxBuffer: 1024 * 1024, windowsHide: true, cwd: ctx.cwd })); }
      catch (error) {
        const failure = error as { code?: number; stdout?: string; killed?: boolean };
        if (failure.code !== 1 || failure.killed || !failure.stdout) throw error;
        stdout = failure.stdout;
      }
      const report = JSON.parse(stdout);
      if (report.schemaVersion !== 2 || typeof report.ok !== "boolean") throw new Error("Acyclic doctor returned an invalid report");
      ctx.ui.notify(JSON.stringify(report), report.ok === true ? "info" : "warning");
    },
  });
}
