import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "./process.mjs";

test("configured hook records the exact envelope and preserves allow, deny and ask", async () => {
  const root = await mkdtemp(join(tmpdir(), "e2e-hook-")), processes = new Processes();
  try {
    for (const decision of ["allow", "deny", "ask"]) {
      const path = join(root, `${decision}.jsonl`);
      const envelope = { hook_event_name: "PreToolUse", tool_name: "write",
        tool_input: { path: "approved.txt", text: "EXACT_SUBJECT" } };
      const result = await processes.run(process.execPath,
        [fileURLToPath(new URL("./hook.mjs", import.meta.url)), path, decision], {}, JSON.stringify(envelope));
      assert.equal(result.code, 0, result.stderr);
      assert.deepEqual(JSON.parse(await readFile(path, "utf8")), envelope);
      assert.equal(JSON.parse(result.stdout).hookSpecificOutput.permissionDecision, decision);
    }
    const result = await processes.run(process.execPath,
      [fileURLToPath(new URL("./hook.mjs", import.meta.url)), join(root, "stop.jsonl"), "allow"],
      {}, JSON.stringify({ hook_event_name: "Stop" }));
    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.stdout, "", "recording must not alter the model's context");
    const context = await processes.run(process.execPath,
      [fileURLToPath(new URL("./hook.mjs", import.meta.url)), join(root, "context.jsonl"), "context"],
      {}, JSON.stringify({ hook_event_name: "Stop" }));
    assert.equal(context.code, 0, context.stderr);
    assert.equal(JSON.parse(context.stdout).hookSpecificOutput.additionalContext, "HOOK_E2E_CONTEXT");
  } finally {
    await processes.close();
    await rm(root, { recursive: true, force: true });
  }
});
