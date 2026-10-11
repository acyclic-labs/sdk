import assert from "node:assert/strict";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { fileURLToPath } from "node:url";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { invokePi, loadPiContextHelpers } from "./pi-fixture.ts";

const { captureParentCut, childSessionBytes } = await loadPiContextHelpers();

// The installed Pi model loop calls its own deterministic provider. This
// fixture supplies model responses; it does not substitute a session runner.
const provider = `
import { fauxProvider, fauxAssistantMessage, getCurrentTools } from "@earendil-works/pi-ai";
export default function (pi) {
  const faux = fauxProvider({ provider: "acyclic-qualification", models: [{ id: "probe" }] });
  faux.setResponses([context => fauxAssistantMessage(JSON.stringify({
    messages: context.messages,
    tools: getCurrentTools(context.messages).map(tool => tool.name).sort(),
  }))]);
  pi.registerProvider(faux.provider);
}
`;

function probeArguments(session: string, extension: string, model = "probe") {
  return [
    "--print", "--mode", "text", "--session", session,
    "--provider", "acyclic-qualification", "--model", model,
    "--no-extensions", "--no-mcp", "--no-builtin-tools", "-e", extension,
  ];
}

function spawnMessage(ids: string[]) {
  return fauxAssistantMessage(ids.map(id => fauxToolCall("acyclic_spawn", {}, { id })), { stopReason: "toolUse", timestamp: 1 });
}

function userMessages(stdout: string): string[] {
  const snapshot: {
    tools: string[];
    messages: Array<{ role: string; content: string | Array<{ type: string; text?: string }> }>;
  } = JSON.parse(stdout.trim());
  assert.deepEqual(snapshot.tools, []);
  return snapshot.messages.filter(message => message.role === "user")
    .map(message => typeof message.content === "string" ? message.content : message.content.map(part => {
      assert.equal(part.type, "text");
      return part.text;
    }).join(""));
}

test("installed Pi consumes only the pinned child history and retains its own session across reopen", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-session-", import.meta.url)));
  try {
    const project = join(directory, "project");
    mkdirSync(project);
    const parent = SessionManager.inMemory(project, { id: randomUUID() });
    const root = parent.appendMessage({ role: "user", content: "pinned parent instruction", timestamp: 1 });
    parent.appendMessage({ role: "user", content: "excluded alternate branch", timestamp: 2 });
    parent.branch(root);
    const callId = "spawn-session-probe";
    parent.appendMessage(spawnMessage([callId]));
    const cut = captureParentCut(parent, callId, 100, 100_000);
    const childId = randomUUID();
    const session = join(directory, "child.jsonl");
    writeFileSync(session, childSessionBytes(cut, childId, project));
    parent.appendMessage({ role: "user", content: "later parent change", timestamp: 4 });
    const originalParent = JSON.stringify(parent.getEntries());
    const extension = join(directory, "provider.ts");
    writeFileSync(extension, provider);
    const args = probeArguments(session, extension);
    for (const [prompt, expected] of [
      ["child invocation", ["pinned parent instruction", "child invocation"]],
      ["explicit child resume", ["pinned parent instruction", "child invocation", "explicit child resume"]],
    ] as const) {
      const result = await invokePi([...args, prompt], project, directory);
      assert.deepEqual(userMessages(result.stdout), expected);
      assert.doesNotMatch(result.stdout, /excluded alternate branch|later parent change|spawn-session-probe/);
      const restored = SessionManager.open(session, directory);
      assert.equal(restored.getSessionId(), childId);
      assert.equal(restored.getCwd(), project);
      assert.equal(restored.buildSessionContext().messages.at(-1)?.role, "assistant");
      assert.equal(JSON.stringify(parent.getEntries()), originalParent);
    }
    assert.match(readFileSync(session, "utf8"), /explicit child resume/);
  } finally {
    rmSync(directory, { recursive: true });
  }
});

test("native Pi recursive context cuts isolate siblings and pin the grandchild's parent history", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-recursive-context-", import.meta.url)));
  try {
    const rootCwd = join(directory, "root");
    mkdirSync(rootCwd);
    const root = SessionManager.inMemory(rootCwd, { id: randomUUID() });
    root.appendMessage({ role: "user", content: "original root instruction", timestamp: 1 });
    root.appendMessage(spawnMessage(["child-a", "child-b"]));
    const cuts = ["child-a", "child-b"].map(id => captureParentCut(root, id, 100, 100_000));
    const extension = join(directory, "provider.ts");
    writeFileSync(extension, provider);
    const sessions: string[] = [];
    const identities: string[] = [];
    for (const [index, cut] of cuts.entries()) {
      const cwd = join(directory, `child-${index}`);
      mkdirSync(cwd);
      const session = join(directory, `child-${index}.jsonl`);
      const id = randomUUID();
      sessions.push(session);
      identities.push(id);
      writeFileSync(session, childSessionBytes(cut, id, cwd));
      const result = await invokePi([...probeArguments(session, extension), `child ${index} instruction`], cwd, directory);
      assert.deepEqual(userMessages(result.stdout), ["original root instruction", `child ${index} instruction`]);
      const restored = SessionManager.open(session, directory);
      assert.equal(restored.getSessionId(), id);
      assert.equal(restored.getCwd(), cwd);
    }
    const first = sessions.at(0)!;
    const second = sessions.at(1)!;
    const child = SessionManager.open(first, directory);
    child.appendMessage(spawnMessage(["grandchild"]));
    const grandchildCut = captureParentCut(child, "grandchild", 100, 100_000);
    const grandchildCwd = join(directory, "grandchild");
    mkdirSync(grandchildCwd);
    const grandchild = join(directory, "grandchild.jsonl");
    const grandchildId = randomUUID();
    writeFileSync(grandchild, childSessionBytes(grandchildCut, grandchildId, grandchildCwd));
    child.appendMessage({ role: "user", content: "later child change", timestamp: 4 });
    root.appendMessage({ role: "user", content: "later root change", timestamp: 5 });
    const retainedRoot = JSON.stringify(root.getEntries());
    const retainedChildren = [first, second].map(session => readFileSync(session, "utf8"));
    for (const [prompt, expected] of [
      ["grandchild instruction", ["original root instruction", "child 0 instruction", "grandchild instruction"]],
      ["grandchild resume", ["original root instruction", "child 0 instruction", "grandchild instruction", "grandchild resume"]],
    ] as const) {
      const result = await invokePi([...probeArguments(grandchild, extension), prompt], grandchildCwd, directory);
      assert.deepEqual(userMessages(result.stdout), expected);
      assert.doesNotMatch(result.stdout, /child 1 instruction|later child change|later root change|acyclic_spawn/);
      const restored = SessionManager.open(grandchild, directory);
      assert.equal(restored.getSessionId(), grandchildId);
      assert.equal(restored.getCwd(), grandchildCwd);
      assert.equal(JSON.stringify(root.getEntries()), retainedRoot);
      assert.deepEqual([first, second].map(session => readFileSync(session, "utf8")), retainedChildren);
    }
    assert.equal(new Set([...identities, root.getSessionId(), grandchildId]).size, 4);
  } finally {
    rmSync(directory, { recursive: true });
  }
});

test("a child consumes the native parent's completed compaction across reopen", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-compaction-context-", import.meta.url)));
  try {
    const project = join(directory, "project");
    mkdirSync(project);
    const agent = join(directory, "agent");
    mkdirSync(agent);
    writeFileSync(join(agent, "settings.json"), JSON.stringify({
      compaction: { enabled: false, keepRecentTokens: 1, reserveTokens: 1024 },
    }));
    const parent = SessionManager.create(project, directory);
    for (const instruction of ["old parent instruction", "recent parent instruction"]) {
      parent.appendMessage({ role: "user", content: instruction, timestamp: 1 });
      parent.appendMessage({ ...spawnMessage([]),
        content: [{ type: "text", text: "completed parent response" }], stopReason: "stop" });
    }
    const parentFile = parent.getSessionFile()!;
    const compactExtension = join(directory, "compact.ts");
    writeFileSync(compactExtension, `
import { fauxProvider, fauxAssistantMessage } from "@earendil-works/pi-ai";
export default function (pi) {
  const faux = fauxProvider({ provider: "acyclic-qualification", models: [{ id: "probe" }] });
  faux.setResponses([
    fauxAssistantMessage("qualified native parent summary"),
    fauxAssistantMessage("qualified native retained turn summary"),
  ]);
  pi.registerProvider(faux.provider);
  pi.registerCommand("qualify-compact", {
    description: "Run native compaction for qualification",
    async handler(_args, ctx) {
      await new Promise((resolve, reject) => ctx.compact({ onComplete: resolve, onError: reject }));
    },
  });
}
`);
    const compactResult = await invokePi(
      [...probeArguments(parentFile, compactExtension), "/qualify-compact"], project, directory,
    );
    const compacted = SessionManager.open(parentFile, directory);
    const compaction = compacted.getEntries().find(entry => entry.type === "compaction");
    assert.ok(compaction, `native CLI did not retain its completed compaction: ${compactResult.stdout} ${compactResult.stderr}`);
    assert.match(compaction.summary, /qualified native parent summary/);
    compacted.appendMessage(spawnMessage(["spawn-after-compaction"]));
    const cut = captureParentCut(compacted, "spawn-after-compaction", 100, 100_000);
    const child = join(directory, "child.jsonl");
    const childId = randomUUID();
    writeFileSync(child, childSessionBytes(cut, childId, project));
    const originalParent = readFileSync(parentFile, "utf8");
    const extension = join(directory, "provider.ts");
    writeFileSync(extension, provider);
    for (const prompt of ["child after compaction", "resume compacted child"]) {
      const result = await invokePi([...probeArguments(child, extension), prompt], project, directory);
      assert.match(result.stdout, /qualified native parent summary/);
      assert.doesNotMatch(result.stdout, /old parent instruction|spawn-after-compaction/);
      assert.equal(SessionManager.open(child, directory).getSessionId(), childId);
      assert.equal(readFileSync(parentFile, "utf8"), originalParent);
    }
  } finally {
    rmSync(directory, { recursive: true });
  }
});

test("native retries and a model switch retain one child history across reopen", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-retry-context-", import.meta.url)));
  try {
    const project = join(directory, "project");
    mkdirSync(project);
    const agent = join(directory, "agent");
    mkdirSync(agent);
    writeFileSync(join(agent, "settings.json"), JSON.stringify({
      compaction: { enabled: false },
      retry: { enabled: true, maxRetries: 1, baseDelayMs: 1, maxAgentDelayMs: 10 },
    }));
    const parent = SessionManager.inMemory(project, { id: randomUUID() });
    parent.appendMessage({ role: "user", content: "original retry parent", timestamp: 1 });
    parent.appendMessage(spawnMessage(["retry-child"]));
    const cut = captureParentCut(parent, "retry-child", 100, 100_000);
    const childId = randomUUID();
    const child = join(directory, "child.jsonl");
    writeFileSync(child, childSessionBytes(cut, childId, project));
    const originalParent = JSON.stringify(parent.getEntries());
    const extension = join(directory, "retry.ts");
    writeFileSync(extension, `
import { fauxProvider, fauxAssistantMessage, getCurrentTools } from "@earendil-works/pi-ai";
export default function (pi) {
  const faux = fauxProvider({
    provider: "acyclic-qualification", models: [{ id: "probe" }, { id: "alternate" }],
  });
  faux.setResponses([
    fauxAssistantMessage("", { stopReason: "error", errorMessage: "503 Service Unavailable" }),
    (context, _options, state, model) => fauxAssistantMessage(JSON.stringify({
      attempts: state.callCount, model: model.id, messages: context.messages,
      tools: getCurrentTools(context.messages).map(tool => tool.name).sort(),
    })),
  ]);
  pi.registerProvider(faux.provider);
}
`);
    for (const [model, prompt, expected] of [
      ["probe", "first retry invocation", ["original retry parent", "first retry invocation"]],
      ["alternate", "resume with another model", ["original retry parent", "first retry invocation", "resume with another model"]],
    ] as const) {
      const result = await invokePi([...probeArguments(child, extension, model), prompt], project, directory);
      const snapshot = JSON.parse(result.stdout.trim());
      assert.equal(snapshot.attempts, 2, "native model loop did not retry its failed response exactly once");
      assert.equal(snapshot.model, model);
      assert.deepEqual(userMessages(result.stdout), expected);
      assert.doesNotMatch(result.stdout, /retry-child/);
      const restored = SessionManager.open(child, directory);
      assert.equal(restored.getSessionId(), childId);
      assert.deepEqual(restored.buildSessionContext().model, {
        provider: "acyclic-qualification", modelId: model,
      });
      const storedMessages = restored.getEntries()
        .filter(entry => entry.type === "message").map(entry => entry.message);
      assert.deepEqual(userMessages(JSON.stringify({ tools: [], messages: storedMessages })), expected);
      assert.equal(JSON.stringify(parent.getEntries()), originalParent);
    }
  } finally {
    rmSync(directory, { recursive: true });
  }
});
