import {
  VERSION,
  SessionManager,
  type ExtensionAPI,
  type ExtensionContext,
  type SessionEntry,
} from "@earendil-works/pi-coding-agent";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";

// Replaced with a JSON string by the installer; never resolved through a shell.
const executable = "__ACYCLIC_EXECUTABLE__";
const execute = promisify(execFile);

/** Select the native history that produced this exact spawn invocation. */
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
      // Serialize each selected entry before retaining it; cap the cumulative
      // bytes before joining, cloning, hashing or sending the captured branch.
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

/** Revalidate before admission after asynchronous preparation; never replace the cut. */
export function assertParentCutCurrent(
  session: ExtensionContext["sessionManager"],
  toolCallId: string,
  cut: ReturnType<typeof captureParentCut>,
  maximumEntries: number,
  maximumBytes: number,
) {
  const current = captureParentCut(session, toolCallId, maximumEntries, maximumBytes);
  if (current.parentSessionId !== cut.parentSessionId ||
      current.parentSessionFile !== cut.parentSessionFile ||
      current.parentCwd !== cut.parentCwd ||
      current.sourceLeafId !== cut.sourceLeafId ||
      current.ownerEntryId !== cut.ownerEntryId ||
      current.parentLeafId !== cut.parentLeafId ||
      current.capturedAt !== cut.capturedAt ||
      current.entriesJson !== cut.entriesJson || current.sha256 !== cut.sha256) {
    throw new Error("Pi parent context changed before spawn admission");
  }
}

/** Construct a separate native manager; the live parent is never passed here. */
export function childSessionBytes(
  cut: ReturnType<typeof captureParentCut>,
  childSessionId: string,
  childCwd: string,
) {
  if (createHash("sha256").update(cut.entriesJson).digest("hex") !== cut.sha256) {
    throw new Error("Pinned Pi context digest differs");
  }
  if (childSessionId === cut.parentSessionId) throw new Error("Child Pi session must have its own identity");
  const entries: SessionEntry[] = JSON.parse(cut.entriesJson);
  if (!Array.isArray(entries)) throw new Error("Pinned Pi context is not an entry list");
  let previous: string | null = null;
  const ids = new Set<string>();
  for (const entry of entries) {
    if (!entry || typeof entry.id !== "string" || ids.has(entry.id) || entry.parentId !== previous ||
        (entry as { type: string }).type === "session") {
      throw new Error("Pinned Pi context is not the selected parent chain");
    }
    ids.add(entry.id);
    previous = entry.id;
  }
  if (previous !== cut.parentLeafId) throw new Error("Pinned Pi context leaf differs");
  const child = SessionManager.inMemory(childCwd, { id: childSessionId }, entries);
  // Validate the native projection, including compaction, without rewriting it.
  const pending = new Set<string>();
  for (const message of child.buildSessionContext().messages) {
    if (message.role === "assistant") {
      if (pending.size) throw new Error("Pinned Pi context has an incomplete tool exchange");
      for (const part of message.content) {
        if (part.type === "toolCall") {
          if (pending.has(part.id)) throw new Error("Pinned Pi context repeats a tool identity");
          pending.add(part.id);
        }
      }
    } else if (message.role === "toolResult") {
      if (!pending.delete(message.toolCallId)) throw new Error("Pinned Pi context has an orphan tool result");
    } else if (pending.size) {
      throw new Error("Pinned Pi context interrupts a tool exchange");
    }
  }
  if (pending.size) throw new Error("Pinned Pi context has an incomplete tool exchange");
  // Native header structure, with the admitted cut time instead of a new clock
  // reading on each retry. Session identity/cwd come from the prepared binding.
  const header = { ...child.getHeader(), timestamp: cut.capturedAt };
  return [header, ...child.getEntries()].map(entry => JSON.stringify(entry)).join("\n") + "\n";
}

export default function acyclic(pi: ExtensionAPI) {
  if (VERSION !== "1.1.0") {
    throw new Error(`Acyclic requires Pi 1.1.0 APIs; found ${VERSION}`);
  }
  pi.registerCommand("acyclic-doctor", {
    description: "Inspect the installed Acyclic service and platform qualification",
    async handler(_args, ctx) {
      let stdout: string;
      try {
        ({ stdout } = await execute(executable, ["doctor", "--json"], {
          timeout: 20_000,
          maxBuffer: 1024 * 1024,
          windowsHide: true,
          cwd: ctx.cwd,
        }));
      } catch (error) {
        // Doctor returns exit 1 with its JSON report when a gate fails.
        const failure = error as { code?: number; stdout?: string; killed?: boolean };
        if (failure.code !== 1 || failure.killed || !failure.stdout) throw error;
        stdout = failure.stdout;
      }
      const report = JSON.parse(stdout);
      if (report.schemaVersion !== 2 || typeof report.ok !== "boolean") {
        throw new Error("Acyclic doctor returned an invalid report");
      }
      ctx.ui.notify(JSON.stringify(report), report.ok === true ? "info" : "warning");
    },
  });
}
