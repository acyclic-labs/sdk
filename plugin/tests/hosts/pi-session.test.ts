import assert from "node:assert/strict";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { fileURLToPath } from "node:url";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { invokePi, loadPiContextHelpers } from "./pi-fixture.ts";

const { captureParentCut, nativeChildSession } = await loadPiContextHelpers();
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
    const native = nativeChildSession(compacted, project, cut);
    writeFileSync(child, native.session_bytes);
    const originalParent = readFileSync(parentFile, "utf8");
    const extension = join(directory, "provider.ts");
    writeFileSync(extension, provider);
    for (const prompt of ["child after compaction", "resume compacted child"]) {
      const result = await invokePi([...probeArguments(child, extension), prompt], project, directory);
      assert.match(result.stdout, /qualified native parent summary/);
      assert.doesNotMatch(result.stdout, /old parent instruction|spawn-after-compaction/);
      assert.equal(SessionManager.open(child, directory).getSessionId(), native.native_session_id);
      assert.equal(readFileSync(parentFile, "utf8"), originalParent);
    }
  } finally {
    rmSync(directory, { recursive: true });
  }
});
