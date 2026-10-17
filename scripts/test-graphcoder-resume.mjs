#!/usr/bin/env node
// Identical argv after SIGKILL must rediscover private-volume identity.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import { fixture } from "./graphcoder-fixture.mjs";
import { Terminal, screenText, hasBoldAnswer } from "./graphcoder-pty/screen.mjs";
const [binary, mode = "headless"] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary));
assert.ok(["headless", "interactive"].includes(mode));
const interactive = mode === "interactive";
const host = await fixture();
host.holdCount("resume after kill");
const args = [...host.args, ...(interactive ? [] : ["-p", "resume after kill"])];
const children = [];
let operation;
const start = first => {
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  const child = interactive
    ? spawn("python3", [fileURLToPath(new URL("graphcoder-pty/transport.py", import.meta.url)), binary, ...args], { env: { ...process.env, TERM: "xterm-256color" }, stdio: ["pipe", "pipe", "pipe"] })
    : spawn(binary, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"] });
  children.push(child);
  let stdout = "", stderr = "", pending = "", submitted = false, complete = false, stopped = false;
  child.stdout.on("data", bytes => {
    stdout += bytes;
    if (!interactive) {
      pending += bytes;
      for (let newline; (newline = pending.indexOf("\n")) !== -1;) {
        const line = pending.slice(0, newline); pending = pending.slice(newline + 1);
        const frame = JSON.parse(line);
        if (Array.isArray(frame)) operation ??= frame[0].TaskSession.session;
      }
    } else screen.write(bytes, () => {
      if (stopped) return;
      const visible = screenText(screen);
      if (first && !submitted && visible.includes("GraphCoder") && stdout.includes("\x1b[?25h")) {
        submitted = true; child.stdin.write("/delegate resume after kill\r");
      }
      if (!first && !complete && hasBoldAnswer(screen) && visible.includes("○")) {
        complete = true; child.stdin.write("\x04");
      }
    });
  });
  child.stderr.on("data", bytes => { stderr += bytes; });
  const exit = new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code, signal) => {
      stopped = true; screen.dispose(); resolve({ code, signal, stdout, stderr, complete });
    });
  });
  return { child, exit };
};
const kill = child => { if (child.exitCode === null) child.kill(interactive ? "SIGUSR1" : "SIGKILL"); };
const timeout = setTimeout(() => { for (const child of children) kill(child); }, 8000);
let failure;
try {
  const first = start(true);
  await Promise.race([host.counting, first.exit.then(result => { throw new Error(`turn exited before preparation: ${JSON.stringify(result)}`); })]);
  assert.equal(host.calls(), 0, "no model dispatch before kill");
  kill(first.child);
  const killed = await first.exit;
  assert.ok(killed.signal || killed.code !== 0, "executable was not killed");
  host.completeCount();
  const resumed = await start(false).exit;
  assert.equal(resumed.code, 0, resumed.stderr + resumed.stdout);
  if (interactive) assert.ok(resumed.complete, "restart without input must complete the retained turn");
  else {
    const records = resumed.stdout.trim().split("\n").map(JSON.parse);
    assert.equal(records[0][0].TaskSession.session, operation, "private launch identity must survive kill");
    assert.ok(records.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean)).some(file => file.key.Session?.file.ExecutionResult === operation && JSON.parse(file.bytes.text).completion === "completed"));
  }
  assert.equal(host.calls(), 1, "resume dispatches the retained turn exactly once");
  if (!interactive) {
    const fresh = await start(false).exit;
    assert.equal(fresh.code, 0, fresh.stderr);
    assert.notEqual(JSON.parse(fresh.stdout.split("\n")[0])[0].TaskSession.session, operation, "completed request starts fresh");
    assert.equal(host.calls(), 2);
  }
} catch (error) { failure = error; }
finally {
  clearTimeout(timeout);
  for (const child of children) kill(child);
  try { await host.close(); }
  catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], "resume qualification and cleanup failed") : cleanup; }
}
if (failure) throw failure;
