import assert from "node:assert/strict";
import { copyFileSync, mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import { discoverAndLoadExtensions, SessionManager, parseSessionEntries } from "@earendil-works/pi-coding-agent";

// The installed extension is outside the SDK package and Pi resolves its imports.
// For these native-SDK unit tests, copy the exact source beneath the qualification
// package so Node resolves the same pinned SDK without a custom import hook.
const directory = mkdtempSync(fileURLToPath(new URL(".pi-context-", import.meta.url)));
let context: typeof import("../../pi/index.ts");
try {
  const extension = join(directory, "index.ts");
  copyFileSync(new URL("../../pi/index.ts", import.meta.url), extension);
  context = await import(pathToFileURL(extension).href);
} finally {
  rmSync(directory, { recursive: true });
}
const { assertParentCutCurrent, captureParentCut, childSessionBytes } = context;

test("the native Pi loader resolves and registers the extension outside the host package", async () => {
  const isolated = mkdtempSync(fileURLToPath(new URL(".pi-loader-", import.meta.url)));
  try {
    const loaded = await discoverAndLoadExtensions(
      [fileURLToPath(new URL("../../pi/index.ts", import.meta.url))], isolated, isolated);
    assert.deepEqual(loaded.errors, []);
    assert.equal(loaded.extensions.length, 1);
    assert.ok(loaded.extensions[0].commands.has("acyclic-doctor"));
  } finally {
    rmSync(isolated, { recursive: true });
  }
});

function spawnMessage(id: string) {
  return {
    role: "assistant" as const,
    content: [{ type: "toolCall" as const, id, name: "acyclic_spawn", arguments: {} }],
    api: "openai-responses" as const,
    provider: "openai",
    model: "deterministic",
    usage: {
      input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "toolUse" as const,
    timestamp: 1,
  };
}

test("child keeps only the pinned current branch and never changes parent state", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  const root = parent.appendMessage({ role: "user", content: "root instruction", timestamp: 1 });
  parent.appendMessage({ role: "user", content: "excluded alternative", timestamp: 2 });
  parent.branch(root);
  parent.appendMessage(spawnMessage("spawn-1"));
  const before = JSON.stringify([parent.getSessionId(), parent.getSessionFile(), parent.getLeafId(), parent.getEntries()]);
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const child = childSessionBytes(cut, "child", process.cwd());
  assert.equal(cut.parentLeafId, root);
  assert.ok(child.includes("root instruction"));
  assert.ok(!child.includes("excluded alternative"));
  assert.ok(!child.includes("spawn-1"));
  assert.equal(childSessionBytes(cut, "child", process.cwd()), child);
  const restored = SessionManager.inMemory(process.cwd(), undefined, parseSessionEntries(child));
  assert.equal(restored.getSessionId(), "child");
  assert.equal(restored.getLeafId(), cut.parentLeafId);
  assert.equal(JSON.stringify([parent.getSessionId(), parent.getSessionFile(), parent.getLeafId(), parent.getEntries()]), before);
  parent.appendMessage({ role: "user", content: "later parent change", timestamp: 3 });
  assert.ok(!childSessionBytes(cut, "child", process.cwd()).includes("later parent change"));
});

test("bounds, missing invocation and digest corruption reject without changing parent", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage({ role: "user", content: "context", timestamp: 1 });
  parent.appendMessage(spawnMessage("spawn-1"));
  const before = JSON.stringify(parent.getEntries());
  assert.throws(() => captureParentCut(parent, "missing", 100, 100_000));
  assert.throws(() => captureParentCut(parent, "spawn-1", 1, 100_000));
  assert.throws(() => captureParentCut(parent, "spawn-1", 100, 1));
  assert.throws(() => captureParentCut(parent, "spawn-1", Infinity, 100_000));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  assert.throws(() => childSessionBytes({ ...cut, entriesJson: "[]" }, "child", process.cwd()));
  assert.equal(JSON.stringify(parent.getEntries()), before);
});

test("pre-admission revalidation rejects parent changes without replacing the pinned cut", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  const root = parent.appendMessage({ role: "user", content: "pinned instruction", timestamp: 1 });
  const owner = parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const before = JSON.stringify(cut);
  assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000);
  parent.appendMessage({ role: "user", content: "later history", timestamp: 2 });
  assert.throws(() => assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000), /changed before/);
  parent.branch(owner);
  assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000);
  const switched = SessionManager.inMemory(join(process.cwd(), "another-project"), { id: "parent" },
    structuredClone(parent.getEntries()));
  assert.throws(() => assertParentCutCurrent(switched, "spawn-1", cut, 100, 100_000), /changed before/);
  parent.branch(root);
  assert.throws(() => assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000), /absent/);
  assert.equal(JSON.stringify(cut), before);
});

test("an earlier unfinished tool exchange cannot be laundered into a child", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage(spawnMessage("unfinished"));
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  assert.throws(() => childSessionBytes(cut, "child", process.cwd()), /incomplete tool exchange/);
});

test("a complete tool exchange survives; an omitted result rejects the native projection", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage(spawnMessage("completed"));
  const result = parent.appendMessage({ role: "toolResult", toolCallId: "completed", toolName: "acyclic_spawn",
    content: [{ type: "text", text: "completed result" }], isError: false, timestamp: 2 });
  const expected = parent.buildSessionContext();
  const boundary = parent.getLeafId();
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const child = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries(childSessionBytes(cut, "child", process.cwd())));
  assert.deepEqual(child.buildSessionContext(), expected);
  parent.branch(boundary!);
  parent.appendContextEdit(result, null);
  parent.appendMessage(spawnMessage("spawn-2"));
  assert.throws(() => childSessionBytes(captureParentCut(parent, "spawn-2", 100, 100_000),
    "child", process.cwd()), /incomplete tool exchange/);
});

test("a first-turn spawn produces a separate empty native child session", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 1, 2);
  assert.equal(cut.entriesJson, "[]");
  assert.equal(cut.parentLeafId, null);
  const child = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries(childSessionBytes(cut, "child", process.cwd())));
  assert.equal(child.getSessionId(), "child");
  assert.equal(child.getLeafId(), null);
  assert.deepEqual(child.buildSessionContext().messages, []);
  assert.throws(() => childSessionBytes(cut, "parent", process.cwd()), /own identity/);
});

test("compaction and context-edit provenance survive native child serialization", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage({ role: "user", content: "old history", timestamp: 1 });
  const retained = parent.appendMessage({ role: "user", content: "retained history", timestamp: 2 });
  const compact = parent.appendCompaction("native summary", retained, 20_000, { provenance: "original" });
  parent.appendContextEdit(retained, { content: "edited projection" });
  const expected = parent.buildSessionContext();
  const expectedCompaction = JSON.stringify(parent.getEntry(compact));
  parent.appendMessage(spawnMessage("spawn-1"));
  const before = JSON.stringify(parent.getEntries());
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const child = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries(childSessionBytes(cut, "child", process.cwd())));
  assert.deepEqual(child.buildSessionContext(), expected);
  assert.equal(JSON.stringify(child.getEntry(compact)), expectedCompaction);
  assert.equal(JSON.stringify(parent.getEntries()), before);
});

test("parallel sibling tool calls/results are excluded with their owning assistant", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage({ role: "user", content: "instruction", timestamp: 1 });
  const message = spawnMessage("spawn-1");
  message.content.push({ type: "toolCall", id: "sibling", name: "read", arguments: {} });
  parent.appendMessage(message);
  parent.appendMessage({ role: "toolResult", toolCallId: "sibling", toolName: "read",
    content: [{ type: "text", text: "sibling result" }], isError: false, timestamp: 2 });
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  assert.ok(!cut.entriesJson.includes("sibling"));
  assert.ok(!cut.entriesJson.includes("spawn-1"));
});

test("duplicate identities, branch changes and cyclic parent chains reject", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  const root = parent.appendMessage({ role: "user", content: "instruction", timestamp: 1 });
  const duplicate = spawnMessage("spawn-1");
  duplicate.content.push(duplicate.content[0]);
  parent.appendMessage(duplicate);
  assert.throws(() => captureParentCut(parent, "spawn-1", 100, 100_000), /duplicated/);
  parent.branch(root);
  assert.throws(() => captureParentCut(parent, "spawn-1", 100, 100_000), /absent/);
  parent.appendMessage(spawnMessage("spawn-2"));
  const entries = structuredClone(parent.getEntries());
  entries[0].parentId = entries.at(-1)!.id;
  const cyclic = SessionManager.inMemory(process.cwd(), { id: "cyclic" }, entries);
  assert.throws(() => captureParentCut(cyclic, "spawn-2", 100, 100_000), /cyclic/);
});

test("reloaded native parent selects the same cut; deep history observes finite bounds", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  for (let index = 0; index < 50; index++) parent.appendCustomEntry("history", { index });
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const restored = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries([parent.getHeader(), ...parent.getEntries()].map(entry => JSON.stringify(entry)).join("\n")));
  assert.deepEqual(captureParentCut(restored, "spawn-1", 100, 100_000), cut);
  assert.throws(() => captureParentCut(restored, "spawn-1", 50, 100_000), /entry bound/);
});

test("bounded native branch histories preserve the exact pre-spawn projection", () => {
  let cases = 0;
  for (let history = 1; history <= 4; history++) {
    for (let alternatives = 0; alternatives <= 2; alternatives++) {
      for (const exchange of [false, true]) {
        for (const compact of [false, true]) {
          const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
          let retained = "";
          for (let index = 0; index < history; index++) {
            retained = parent.appendMessage({ role: "user", content: `instruction ${index}`, timestamp: index });
          }
          if (exchange) {
            parent.appendMessage(spawnMessage("completed"));
            parent.appendMessage({ role: "toolResult", toolCallId: "completed", toolName: "acyclic_spawn",
              content: [{ type: "text", text: "completed result" }], isError: false, timestamp: 5 });
          }
          if (compact) parent.appendCompaction("native summary", retained, 20_000);
          const selected = parent.getLeafId()!;
          const expected = parent.buildSessionContext();
          for (let index = 0; index < alternatives; index++) {
            parent.appendMessage({ role: "user", content: `excluded branch ${index}`, timestamp: 6 + index });
          }
          parent.branch(selected);
          parent.appendMessage(spawnMessage("spawn"));
          const before = JSON.stringify([parent.getHeader(), parent.getLeafId(), parent.getEntries()]);
          const cut = captureParentCut(parent, "spawn", 100, 100_000);
          const bytes = childSessionBytes(cut, "child", process.cwd());
          const child = SessionManager.inMemory(process.cwd(), undefined, parseSessionEntries(bytes));
          assert.deepEqual(child.buildSessionContext(), expected);
          assert.equal(child.getLeafId(), selected);
          assert.ok(!bytes.includes("excluded branch"));
          assert.equal(JSON.stringify([parent.getHeader(), parent.getLeafId(), parent.getEntries()]), before);
          parent.appendMessage({ role: "user", content: "later mutation", timestamp: 10 });
          assert.equal(childSessionBytes(cut, "child", process.cwd()), bytes);
          cases++;
        }
      }
    }
  }
  assert.equal(cases, 48);
});
