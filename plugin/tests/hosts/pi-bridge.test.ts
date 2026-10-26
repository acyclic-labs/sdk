import assert from "node:assert/strict";
import * as childProcess from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { mock, test } from "bun:test";

// Run this file in its own Bun process: only the harness request boundary is
// stubbed. The child and its failed-spawn/pipe event ordering are native Node APIs.
const native = { ...childProcess };

test("Pi failed spawn closes custody and shutdown without an exit event", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".pi-bridge-"));
  const missing = join(directory, "absent-native-bridge");
  let closed: Promise<void> | undefined;
  let child: childProcess.ChildProcess | undefined;
  let exits = 0;
  const hooks = new Map<string, (...args: any[]) => Promise<void>>();
  const tools = new Map<string, { execute: (...args: any[]) => Promise<unknown> }>();
  mock.module("node:child_process", () => ({ ...native,
    execFile: (_program: string, _args: string[], _options: unknown, callback: Function) => {
      queueMicrotask(() => callback(null, JSON.stringify({ task: "fixture-task", cwd: directory })));
      return { stdin: { end() {} } };
    },
    spawn: () => {
      child = native.spawn(missing, [], { stdio: "pipe", windowsHide: true });
      child.on("exit", () => exits++);
      closed = new Promise(resolve => child!.once("close", () => resolve()));
      return child;
    },
  }));
  try {
    const path = join(directory, "index.ts");
    writeFileSync(path, readFileSync(new URL("../../pi/index.ts", import.meta.url)));
    const { default: acyclic } = await import(pathToFileURL(path).href);
    acyclic({ on: (name: string, handler: (...args: any[]) => Promise<void>) => hooks.set(name, handler),
      registerTool: (tool: any) => tools.set(tool.name, tool) });
    const ctx = { cwd: directory, sessionManager: { getSessionId: () => "fixture", getSessionFile: () => undefined } };
    await hooks.get("session_start")!({}, ctx);
    await assert.rejects(tools.get("spawn")!.execute("fixture", { reference: "child", prompt: "fixture" }), /ENOENT/);
    await closed;
    assert.equal(exits, 0, "a failed native spawn never emits exit");
    assert(child);
    assert.doesNotThrow(() => child!.stdin!.emit("error", new Error("closed native stdin")),
      "a failed native pipe must not crash Pi");
    let finished = false;
    const shutdown = hooks.get("session_shutdown")!({}, ctx).then(() => { finished = true; });
    // close has already happened; all remaining work is resolved promises and
    // the stubbed harness callback. One event-loop turn is sufficient, no timer.
    await new Promise(resolve => setImmediate(resolve));
    assert(finished, "shutdown must settle after close even when exit never occurred");
    await shutdown;
  } finally {
    mock.restore();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("Pi boundaries leave managed capture to the physical process owner", async () => {
  const directory = mkdtempSync(join(import.meta.dir, ".pi-boundary-"));
  const hooks = new Map<string, (...args: any[]) => Promise<void>>();
  const actions: Record<string, any>[] = [];
  const previous = process.env.ACYCLIC_TASK;
  process.env.ACYCLIC_TASK = "managed-child";
  mock.module("node:child_process", () => ({ ...native,
    execFile: (_program: string, _args: string[], _options: unknown, callback: Function) => ({
      stdin: { end(input: string) {
        const action = JSON.parse(input);
        actions.push(action);
        // This boundary models the shared owner's live custody refusal.
        // The harness contract also exercises the real exclusive lease.
        queueMicrotask(() => callback(action.kind === "capture"
          ? new Error("native process custody is live") : null, "{}"));
      } },
    }),
  }));
  try {
    const path = join(directory, "index.ts");
    writeFileSync(path, readFileSync(new URL("../../pi/index.ts", import.meta.url)));
    const { default: acyclic } = await import(pathToFileURL(path).href);
    acyclic({on: (name: string, handler: (...args: any[]) => Promise<void>) => hooks.set(name, handler),
      registerTool() {}});
    const ctx = {cwd: directory, sessionManager: {getSessionId: () => "fixture", getSessionFile: () => undefined}};
    await hooks.get("session_start")!({}, ctx);
    await hooks.get("agent_end")!({}, ctx);
    await hooks.get("session_shutdown")!({}, ctx);
    assert.deepEqual(actions.map(action => [action.kind, action.host, action.event, action.input?.task]), [
      ["hook", "pi", "NativeSession", "managed-child"],
      ["hook", "pi", "Stop", "managed-child"],
      ["hook", "pi", "SessionEnd", "managed-child"],
    ]);
  } finally {
    if (previous === undefined) delete process.env.ACYCLIC_TASK;
    else process.env.ACYCLIC_TASK = previous;
    mock.restore();
    rmSync(directory, {recursive: true, force: true});
  }
});
