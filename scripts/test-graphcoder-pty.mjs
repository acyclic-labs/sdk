#!/usr/bin/env node
// Real PTY over the production registry/client/TUI path. Fail if any binding is missing.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { fixture } from "./graphcoder-fixture.mjs";
import { Terminal, screenText, hasBoldAnswer, thinkingPhase } from "./graphcoder-pty/screen.mjs";
const [binary, scenario = "success"] = process.argv.slice(2);
assert.ok(["success", "command-recovery", "uncertain", "typeahead", "cancel", "cancel-preparation", "thinking"].includes(scenario));
assert.ok(binary && isAbsolute(binary), "absolute installed binary required");
const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
const host = await fixture();
if (scenario === "uncertain") host.fail();
else host.hold();
if (scenario === "cancel-preparation") host.holdCount("cancel before headers");
if (scenario === "thinking") host.holdCount("say hello");
const quote = text => `'${text.replaceAll("'", "'\\''")}'`;
let launcher, args;
if (process.platform === "win32") {
  launcher = join(process.env.SystemRoot, "System32/WindowsPowerShell/v1.0/powershell.exe");
  args = ["-NoProfile", "-NonInteractive", "-File", fileURLToPath(new URL("graphcoder-conpty.ps1", import.meta.url)), "-Binary", binary, "-Workspace", host.root, "-SettingsRoot", host.settings, "-Catalog", host.args.at(-1), "-Model", "scripted/test", "-DeadlineMilliseconds", "8000"];
} else {
  launcher = "/usr/bin/script";
  const executable = [binary, ...host.args].map(quote).join(" ");
  // A redirected parent gives script(1) a 0x0 PTY unless sized explicitly.
  const command = `stty rows 24 cols 80\nexec ${executable}`;
  args = process.platform === "linux" ? ["-q", "-e", "-f", "-c", command, "/dev/null"] : ["-q", "/dev/null", "/bin/sh", "-c", command];
}
const child = spawn(launcher, args, { windowsHide: true, env: { ...process.env, TERM: "xterm-256color" }, stdio: ["pipe", "pipe", "pipe"] });
if (scenario === "cancel-preparation") host.counting.then(() => child.stdin.write("\x03"));
const sendPrompt = text => child.stdin.write(`\x1b[200~${text}\x1b[201~\r`);
let failure, stopped = false;
let output = "", submitted = false, retried = false, completed = false, intermediate = false;
const phases = new Set();
let countWaiting = false;
if (scenario === "thinking") host.counting.then(() => { countWaiting = true; });
const timeout = setTimeout(() => child.kill("SIGKILL"), 8000);
function frame() {
  const visible = screenText(screen);
  if (scenario === "thinking" && countWaiting) {
    const phase = thinkingPhase(screen);
    if (phase >= 0) phases.add(phase);
    if (phases.size >= 2) {
      assert.equal(host.calls(), 0, "thinking must animate before model dispatch");
      countWaiting = false;
      host.completeCount();
    }
  }
  if (scenario === "typeahead" && !retried && visible.includes("GraphCoder")) {
    retried = true;
    child.stdin.write("\x1b[?1;2c\x1b[>0;1;0c\x1b[12;40R");
  }
  if (!submitted && visible.includes("GraphCoder") && output.includes("\x1b[?25h")) {
    submitted = true;
    sendPrompt(scenario === "command-recovery" ? "/compact summarize" : scenario === "cancel-preparation" ? "/delegate cancel before headers" : "/delegate say hello");
  }
  const feedback = scenario === "uncertain" ? visible.includes("indeterminate:") : /invalid\s*request:/.test(visible);
  if (["uncertain", "command-recovery"].includes(scenario) && submitted && !retried && feedback) {
    assert.equal(host.calls(), scenario === "uncertain" ? 1 : 0, "failed command must not resend a model call");
    retried = true; sendPrompt("/delegate say hello again");
  }
  if (scenario !== "uncertain" && !intermediate && visible.includes("test answer")) {
    assert.ok(!visible.includes("**test answer**"), "raw Markdown must not count as a rendered answer");
    assert.ok(hasBoldAnswer(screen), "answer must have a bold terminal span");
    intermediate = true;
    if (scenario === "cancel") child.stdin.write("\x03");
    else host.complete();
  }
  const settled = ["cancel", "cancel-preparation"].includes(scenario) ? (intermediate || scenario === "cancel-preparation") && /cancelled/i.test(visible) : scenario === "uncertain" ? retried && visible.includes("conflict:") && visible.includes("admitted") : intermediate && visible.includes("○");
  if (!completed && settled && (scenario !== "thinking" || !visible.includes("Thinking…"))) {
    if (scenario === "thinking") {
      assert.ok(phases.size >= 2, "no independent thinking frames while preparation waited");
      assert.ok(!visible.includes("Thinking…"), "settled work must stop animating");
    }
    completed = true; child.stdin.write("\x04");
  }
}
child.stdout.on("data", bytes => {
  output += bytes.toString("utf8");
  if (scenario === "typeahead" && !submitted && output.includes("\x1b[")) {
    submitted = true;
    child.stdin.write("/delegate say hello\r\n");
  }
  screen.write(bytes, () => {
    if (stopped || failure) return;
    try { frame(); }
    catch (error) { failure = error; child.kill(); }
  });
});
child.stderr.on("data", bytes => { output += bytes.toString("utf8"); });
try {
  const code = await new Promise((resolve, reject) => { child.on("error", reject); child.on("exit", (code, signal) => signal ? reject(new Error(`PTY ${scenario} terminated by ${signal} (submitted=${submitted}, retried=${retried}, calls=${host.calls()}): ${screenText(screen)}`)) : resolve(code)); });
  assert.equal(code, 0, output);
  assert.ok(submitted, "composed prompt never appeared");
  if (["uncertain", "command-recovery", "typeahead"].includes(scenario)) assert.ok(retried, "command outcome never became visible feedback");
  assert.ok(completed, "command recovery or admission rejection was not rendered in the real PTY");
  if (!["uncertain", "cancel-preparation"].includes(scenario)) assert.ok(intermediate, "no styled frame before provider completion");
  assert.equal(host.calls(), scenario === "cancel-preparation" ? 0 : 1, "each scenario must execute exactly one stock model turn");
  assert.ok(output.includes("\x1b["), "no terminal frames emitted");
} catch (error) { failure ??= error; }
finally {
  stopped = true; clearTimeout(timeout); child.kill(); screen.dispose();
  try { await host.close(); }
  catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], "PTY qualification and cleanup failed") : cleanup; }
}
if (failure) throw failure;
