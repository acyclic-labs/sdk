#!/usr/bin/env node
// Real PTY over the production registry/client/TUI path. Fail if any binding is missing.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import { fixture } from "./graphcoder-fixture.mjs";
const [binary, scenario = "success"] = process.argv.slice(2);
assert.ok(["success", "command-recovery", "uncertain", "typeahead", "cancel", "cancel-preparation"].includes(scenario));
assert.ok(binary && isAbsolute(binary), "absolute installed binary required");
// Interpret SGR state at the answer itself; a bold header cannot satisfy this.
function hasBoldAnswer(bytes) {
  const ansi = /\x1b\[[0-?]*[ -/]*[@-~]/g;
  let bold = false, cursor = 0;
  for (const match of bytes.matchAll(ansi)) {
    if (bold && bytes.slice(cursor, match.index).includes("test answer")) return true;
    if (match[0].endsWith("m")) {
      const codes = match[0].slice(2, -1).split(";").map(value => Number(value || 0));
      for (let index = 0; index < codes.length; index++) {
        const code = codes[index];
        if ([38, 48, 58].includes(code)) { index += codes[index + 1] === 2 ? 4 : 2; continue; }
        if (code === 0 || code === 22) bold = false;
        if (code === 1) bold = true;
      }
    }
    cursor = match.index + match[0].length;
  }
  return bold && bytes.slice(cursor).includes("test answer");
}
const host = await fixture();
if (scenario === "uncertain") host.fail();
else host.hold();
if (scenario === "cancel-preparation") host.holdCount("cancel before headers");
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
let output = "", submitted = false, retried = false, completed = false, intermediate = false, intermediateCut = 0;
const timeout = setTimeout(() => child.kill("SIGKILL"), 8000);
child.stdout.on("data", bytes => {
  output += bytes.toString("utf8");
  const visible = output.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "");
  if (scenario === "typeahead" && !submitted && output.includes("\x1b[")) {
    submitted = true;
    child.stdin.write("/delegate say hello\r\n");
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
    assert.ok(hasBoldAnswer(output), "answer must have a bold terminal span");
    intermediate = true; intermediateCut = output.length;
    if (scenario === "cancel") child.stdin.write("\x03");
    else host.complete();
  }
  const settled = ["cancel", "cancel-preparation"].includes(scenario) ? (intermediate || scenario === "cancel-preparation") && /cancelled/i.test(visible) : scenario === "uncertain" ? retried && visible.includes("conflict:") && visible.includes("admitted") : intermediate && output.slice(intermediateCut).includes("○");
  if (!completed && settled) { completed = true; child.stdin.write("\x04"); }
});
child.stderr.on("data", bytes => { output += bytes.toString("utf8"); });
let failure;
try {
  const code = await new Promise((resolve, reject) => { child.on("error", reject); child.on("exit", (code, signal) => signal ? reject(new Error(`PTY ${scenario} terminated by ${signal} (submitted=${submitted}, retried=${retried}, calls=${host.calls()}): ${output}`)) : resolve(code)); });
  assert.equal(code, 0, output);
  assert.ok(submitted, "composed prompt never appeared");
  if (["uncertain", "command-recovery", "typeahead"].includes(scenario)) assert.ok(retried, "command outcome never became visible feedback");
  assert.ok(completed, "command recovery or admission rejection was not rendered in the real PTY");
  if (!["uncertain", "cancel-preparation"].includes(scenario)) assert.ok(intermediate, "no styled frame before provider completion");
  assert.equal(host.calls(), scenario === "cancel-preparation" ? 0 : 1, "each scenario must execute exactly one stock model turn");
  assert.ok(output.includes("\x1b["), "no terminal frames emitted");
} catch (error) { failure = error; }
finally {
  clearTimeout(timeout); child.kill();
  try { await host.close(); }
  catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], "PTY qualification and cleanup failed") : cleanup; }
}
if (failure) throw failure;
