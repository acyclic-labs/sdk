#!/usr/bin/env node
// Explicit submission identity survives SIGKILL and resumes the retained turn.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { isAbsolute, join } from "node:path";
import { fixture } from "./graphcoder-fixture.mjs";
import { Processes, OutputWatch } from "./graphcoder-e2e/process.mjs";
import { withOwnedCase } from "./graphcoder-e2e/suite.mjs";
import { Terminal, screenText, hasBoldAnswer } from "./graphcoder-pty/screen.mjs";
const [binary, mode = "headless"] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary));
assert.ok(["headless", "interactive", "interactive-prompt"].includes(mode));
const interactive = mode !== "headless";
const host = await fixture();
host.holdCount("resume after kill");
const args = [...host.args, ...(interactive ? [] : ["-p", "resume after kill"])];
const submission = randomUUID();
const processes = new Processes();
const start = (first, resume = true) => {
  const launchArgs = [...args, ...(resume ? ["--operation", submission] : [])];
  const env = { ...process.env, CODEX_HOME: host.settings, HOME: host.settings,
    USERPROFILE: host.settings, LOCALAPPDATA: join(host.settings, "AppData", "Local"),
    APPDATA: join(host.settings, "AppData", "Roaming"), XDG_DATA_HOME: host.settings,
    OPENAI_API_KEY: "", GRAPHCODER_MODEL: "", TERM: "xterm-256color" };
  const running = interactive ? processes.startTerminal(binary, launchArgs, { env }) : processes.start(binary, launchArgs, { env });
  if (!interactive) return { running, exit: running.closed };
  const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
  let submitted = false, complete = false, stopped = false;
  const raw = new OutputWatch(["\x1b[?25h"]);
  running.child.stdout.on("data", bytes => { raw.write(bytes); screen.write(bytes, () => {
    if (stopped) return;
    const visible = screenText(screen);
    if (first && !submitted && visible.includes("GraphCoder") && raw.has("\x1b[?25h")) {
      submitted = true;
      running.child.stdin.write(`${mode === "interactive" ? "/delegate " : ""}resume after kill\r`);
    }
    if (!first && !complete && hasBoldAnswer(screen) && visible.includes("○")) {
      complete = true; running.child.stdin.write("\x04");
    }
  }); });
  const exit = running.closed.then(result => { stopped = true; screen.dispose(); return { ...result, complete }; });
  return { running, exit };
};
// Complete JSONL frames only: SIGKILL may cut the final line.
const records = stdout => stdout.split("\n").slice(0, -1).filter(Boolean).map(JSON.parse);
let failure;
try {
  const { expired } = await withOwnedCase({ processes, run: async () => {
    const first = start(true);
    await Promise.race([host.counting, first.exit.then(result => { throw new Error(`turn exited before preparation: ${JSON.stringify(result)}`); })]);
    assert.equal(host.calls(), 0, "no model dispatch before kill");
    first.running.killExecutable();
    const killed = await first.exit;
    assert.ok(killed.signal || killed.code !== 0, "executable was not killed");
    const operation = interactive ? undefined : records(killed.stdout).find(Array.isArray)?.[0].TaskSession.session;
    host.completeCount();
    const resumed = await start(false).exit;
    assert.equal(resumed.code, 0, resumed.stderr + resumed.stdout);
    if (interactive) assert.ok(resumed.complete, "restart without input must complete the retained turn");
    else {
      const frames = resumed.stdout.trim().split("\n").map(JSON.parse);
      assert.equal(frames[0][0].TaskSession.session, operation, "explicit submission identity must survive kill");
      assert.ok(frames.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean)).some(file => file.key.Session?.file.ExecutionResult === operation && JSON.parse(file.bytes.text).completion === "completed"));
    }
    assert.equal(host.calls(), 1, "resume dispatches the retained turn exactly once");
    if (!interactive) {
      const fresh = await start(false, false).exit;
      assert.equal(fresh.code, 0, fresh.stderr);
      assert.notEqual(JSON.parse(fresh.stdout.split("\n")[0])[0].TaskSession.session, operation, "completed request starts fresh");
      assert.equal(host.calls(), 2);
    }
  } });
  assert.ok(!expired, "resume qualification exceeded its owned deadline");
} catch (error) { failure = error; }
finally {
  await processes.close();
  try { await host.close(); }
  catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], "resume qualification and cleanup failed") : cleanup; }
}
if (failure) throw failure;
