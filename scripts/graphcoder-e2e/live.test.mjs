import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, toNamespacedPath } from "node:path";
import { fileURLToPath } from "node:url";
import { Processes } from "./process.mjs";
import { fileBytes, jsonlReply, messageReply, requests } from "./assertions.mjs";
const captured = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url), "utf8")).captures;
const childResults = JSON.parse(readFileSync(new URL("./fixtures/live-child-results.json", import.meta.url), "utf8")).cases;
const mergeResults = JSON.parse(readFileSync(new URL("./fixtures/live-merge-results.json", import.meta.url), "utf8")).cases;
import { answer, requireTools, runCase } from "./live.mjs";

test("live oracle uses the shipping text and committed JSONL contracts", () => {
  const result = stdout => ({ code: 0, signal: null, stdout, stderr: "" });
  const text = "hé λ 🙂 **streamed answer**\n";
  assert.equal(answer(captured["text-responses"], "text", text), text);
  const message = { id: "e2e", inbox: 1, text: "done" };
  assert.deepEqual(answer(captured["jsonl-reopen"], "jsonl", message.text, message.id), message);
  assert.deepEqual(jsonlReply(captured["jsonl-reopen"].stdout, message.id), message);
  assert.deepEqual(messageReply(message, message.id), message);
  // A successful process and two identical committed replies cannot admit a
  // wrong answer. Require the case's original expected bytes in either format.
  for (const [format, original, expected] of [["text", captured["text-responses"], text],
    ["jsonl", captured["jsonl-reopen"], message.text]]) {
    for (const wrong of ["", expected + " ", "\ufeff" + expected]) {
      assert.throws(() => answer(original, format, wrong, message.id), error => {
        assert.equal(error.actual, expected);
        assert.equal(error.expected, wrong);
        return true;
      });
    }
  }
  for (const output of ["", "answer", "{}", JSON.stringify({ ...message, error: "failed" }),
    JSON.stringify({ ok: { text: "obsolete ExecutionResult" } }),
    JSON.stringify(message) + "\n" + JSON.stringify(message)]) {
    assert.throws(() => answer(result(output), "jsonl", undefined, message.id));
  }
  // Mutations of the captured reply must fail the shared decoder used by the
  // scripted runner too. An id-less error cannot be filtered out of success.
  for (const mutate of [
    value => { delete value.id; },
    value => { value.id = 7; },
    value => { value.id = "another prompt"; },
    value => { delete value.inbox; },
    value => { value.inbox = -1; },
    value => { value.inbox = Number.MAX_SAFE_INTEGER + 1; },
    value => { delete value.text; },
    value => { value.text = false; },
    value => { value.error = false; },
  ]) {
    const value = { ...message }; mutate(value);
    assert.throws(() => messageReply(value, message.id), { code: "ERR_ASSERTION" });
    assert.throws(() => jsonlReply(JSON.stringify(value), message.id), { code: "ERR_ASSERTION" });
    assert.throws(() => answer(result(JSON.stringify(value)), "jsonl", undefined, message.id), { code: "ERR_ASSERTION" });
  }
  const terminal = { error: "execution failed" };
  for (const stdout of [JSON.stringify(terminal) + "\n" + captured["jsonl-reopen"].stdout,
    captured["jsonl-reopen"].stdout + JSON.stringify(terminal) + "\n"]) {
    assert.throws(() => jsonlReply(stdout, message.id), { code: "ERR_ASSERTION" });
    assert.throws(() => answer(result(stdout), "jsonl", undefined, message.id), { code: "ERR_ASSERTION" });
  }
  const failed = { id: message.id, error: "input rejected before commit" };
  assert.deepEqual(jsonlReply(JSON.stringify(failed), message.id), failed);
  assert.throws(() => answer(result(JSON.stringify(failed)), "jsonl", undefined, message.id), /failed turn/);
  assert.throws(() => answer({ ...result("answer"), code: 1, stderr: "startup failed" }, "text"), /startup failed/);
  assert.throws(() => answer({ ...result("answer"), signal: "SIGKILL" }, "text"), /interrupted/);
});

test("model prose and pre-tool events cannot prove a completed native tool", () => {
  assert.throws(() => requireTools([], ["shell"]));
  assert.throws(() => requireTools([{ hook_event_name: "PreToolUse", tool_name: "shell" }], ["shell"]));
  // SDK hook envelopes are internal contract controls, not external captures.
  requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell", tool_response: { text: "done" } }], ["shell"]);
  assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell" }], ["shell"]), /admit completed shell result/);
  assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell", tool_response: null }], ["shell"]), /admit completed shell result/);
  for (const tool_response of [{ error: "denied" }, { error: "" }, { error: false },
    "tool executor rejected the admitted call (conflict): workspace generation changed"]) {
    assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "shell", tool_response }], ["shell"]), /unexpected tool error/);
  }
  // Even a later successful call cannot erase an earlier failure or a failed
  // auxiliary tool that the required-tool list did not mention.
  assert.throws(() => requireTools([
    { hook_event_name: "PostToolUse", tool_name: "fs", tool_response: { error: "merge failed" } },
    { hook_event_name: "PostToolUse", tool_name: "shell", tool_response: { text: "done" } },
  ], ["shell"]), /merge failed/);
  assert.throws(() => requireTools([{ hook_event_name: "PostToolUse", tool_name: "spawn" }], ["spawn", "wait"]));
});

test("collaboration requires the spawned child's exact settled public result", () => {
  // Original live calls used both supported references: canonical path and
  // the exact name returned by spawn. Settlement still binds canonical identity.
  for (const { events: original } of childResults) {
    const [spawn, wait] = original;
    const child = spawn.tool_response.agent, expected = wait.tool_response.settled[0].answer;
    requireTools(original, ["spawn", "wait"], expected);
    for (const mutate of [
      events => events.pop(),
      events => { events[1].hook_event_name = "PreToolUse"; },
      events => { events[1].tool_input.agents = ["other"]; },
      events => { events[1].tool_input.agents = ["agents/" + spawn.tool_response.name]; },
      events => { events[1].tool_input.agents.push(child); },
      events => { events[1].tool_response.settled[0].agent = "agents/project/other"; },
      events => { events[1].tool_response.running = [child]; },
      events => { events[1].tool_response.settled = []; },
      events => { events[1].tool_response.settled[0].phase = "failed"; },
      events => { events[1].tool_response.settled[0].answer = "parent's invented result"; },
      events => { events[1].tool_response.error = "wait failed"; },
      events => { events.push(structuredClone(spawn)); },
      events => { events.push({ ...structuredClone(spawn), tool_response: {
        ...spawn.tool_response, agent: "agents/project/other", name: "other" } }); },
    ]) {
      const events = structuredClone(original);
      mutate(events);
      assert.throws(() => requireTools(events, ["spawn", "wait"], expected));
    }
  }
});

test("a child merge needs its committed branch result after settlement", () => {
  // These are successful native operations from cases that still failed their
  // whole-case time requirement. No historical case is requalified here.
  for (const { events: original } of mergeResults) {
    const expected = original.find(event => event.tool_name === "wait").tool_response.settled[0].answer;
    const workspace = original[0].cwd;
    const check = events => requireTools(events, ["spawn", "wait", "fs"], expected, workspace);
    check(original);
    // Native Windows canonical paths use the namespace form. Both spellings
    // must retain the same exact parent-workspace and settled-branch checks.
    const absolute = resolve(workspace), native = structuredClone(original);
    for (const event of native) event.cwd = toNamespacedPath(absolute);
    requireTools(native, ["spawn", "wait", "fs"], expected, absolute);
    const nativeMerge = native.find(event => event.tool_response?.result?.Committed);
    for (const cwd of [toNamespacedPath(join(absolute, "child")), "workspace", undefined]) {
      nativeMerge.cwd = cwd;
      assert.throws(() => requireTools(native, ["spawn", "wait", "fs"], expected, absolute));
    }
    for (const mutate of [
      (events, merge) => { events.splice(events.indexOf(merge), 1); },
      (_, merge) => { merge.hook_event_name = "PreToolUse"; },
      (_, merge) => { merge.tool_response.result.Committed = null; },
      (events, merge) => { merge.tool_response = structuredClone(events.find(event =>
        event.tool_response?.result?.Filesystem?.Data?.kind === "diff").tool_response); },
      (_, merge) => { merge.tool_response.result.Committed.message = "merge agents/other"; },
      (_, merge) => { merge.tool_response.result.Committed.message = "rebase agents/worker"; },
      (_, merge) => { merge.cwd += "/child"; },
      (events, merge) => {
        events.splice(events.indexOf(merge), 1);
        events.splice(1, 0, merge);
      },
      (events, merge) => {
        // The former unanchored command-text check accepted this inspection.
        merge.tool_input.command = "diff 'merge agents/worker'";
        merge.tool_response = structuredClone(events.find(event =>
          event.tool_response?.result?.Filesystem?.Data?.kind === "diff").tool_response);
      },
    ]) {
      const events = structuredClone(original);
      const merge = events.find(event => event.tool_response?.result?.Committed);
      mutate(events, merge);
      assert.throws(() => check(events));
    }
  }
});

test("a real command must publish exact bytes and a successful shell status", async () => {
  const root = await mkdtemp(join(tmpdir(), "live-shell-result-")), processes = new Processes();
  try {
    const path = join(root, "effect.txt");
    const result = await processes.run(process.execPath,
      ["-e", `const {writeFileSync}=require('node:fs');
        const path=process.argv[1], expected='EXPECTED_BYTES';
        writeFileSync(path, expected);
        for (const [index, value] of [' '+expected, expected+' ', expected+'\\n',
          expected+'\\r\\n', '\\ufeff'+expected, expected+expected].entries())
          writeFileSync(path+index, value);
        process.exitCode=17`, path]);
    assert.equal(result.code, 17, result.stderr);
    await fileBytes(path, "EXPECTED_BYTES");
    for (let index = 0; index < 6; index++)
      await assert.rejects(fileBytes(path + index, "EXPECTED_BYTES"), { code: "ERR_ASSERTION" });
    await assert.rejects(fileBytes(path + "missing", "EXPECTED_BYTES"), { code: "ENOENT" });
    // A real filesystem refusal must remain a preparation failure, with no
    // executable admitted and no invented product-latency sample.
    const refused = await runCase(process.execPath, root, path, "session", new AbortController().signal);
    assert.equal(refused.status, "fail");
    assert.ok(["ENOTDIR", "EEXIST"].includes(refused.code), refused.error);
    assert.match(refused.failure.error, new RegExp(refused.code));
    assert.equal(refused.phase, "preparation");
    assert.equal(refused.seconds, undefined);
    assert.deepEqual(refused.captures, []);
    assert.equal(refused.phases.at(-1).phase, "retired");
    assert.equal(refused.phases.at(-1).outcome, "completed");
    assert.ok(!refused.phases.some(phase => phase.phase === "admitted" || phase.phase === "run"));
    const cancelled = new AbortController();
    cancelled.abort(new Error("cancelled before preparation"));
    const stopped = await runCase(process.execPath, root, root, "session", cancelled.signal);
    assert.match(stopped.error, /cancelled before preparation/);
    assert.deepEqual(stopped.phases.map(phase => [phase.phase, phase.outcome]),
      [["retired", "entered"], ["retired", "completed"]]);
    assert.deepEqual(stopped.captures, []);
    // Invalid campaign admission is a real CLI refusal, before a binary or
    // model can run, and keeps the same shared diagnostic as failed attempts.
    const rejectedCli = await processes.run(process.execPath,
      [fileURLToPath(new URL("./live.mjs", import.meta.url)), process.execPath, join(root, "unadmitted"), "0"]);
    assert.equal(rejectedCli.code, 1, rejectedCli.stderr);
    assert.equal(rejectedCli.stdout, "");
    const diagnostic = JSON.parse(rejectedCli.stderr);
    assert.equal(diagnostic.step, "live admission or publication");
    assert.match(diagnostic.error, /AssertionError.*admit explicit positive sample count/);
    assert.equal(diagnostic.assertion.actual, "false");
    assert.equal(diagnostic.assertion.expected, "true");
    assert.match(diagnostic.recovery, /node scripts\/graphcoder-e2e\/live.mjs/);
    // Original OS completion supplies these SDK ToolResult fields; the two
    // mutations require each status field to reject failure independently.
    const status = { success: result.code === 0, exit_code: result.code, stop: null };
    const success = { success: true, exit_code: 0, stop: null };
    const hook = tool_response => ({ hook_event_name: "PostToolUse", tool_name: "shell", tool_response });
    const fixture = { id: "shell-status", model: { steps: [{}] }, assert: { requests: 1 } };
    const record = value => [{ path: "responses", step: 0, status: 200,
      body: { input: [{ type: "function_call_output", output: JSON.stringify(value) }] } }];
    requireTools([hook(success)], ["shell"]);
    requests(fixture, record(success));
    for (const value of [status, { ...status, success: true }, { ...status, exit_code: 0 }]) {
      assert.throws(() => requireTools([hook(value), hook(success)], ["shell"]), /unexpected tool error/);
      assert.throws(() => requests(fixture, record(value)), /unexpected tool error/);
    }
    assert.throws(() => requireTools([hook(status), { hook_event_name: "PostToolUse",
      tool_name: "spawn", tool_response: { agent: "agents/worker" } }], ["spawn"]), /unexpected tool error/);
    requests({ ...fixture, assert: { requests: 1, tool_errors: ["exit_code=17"] } }, record(status));
  } finally {
    await processes.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("the completed-tool recorder keeps literal arguments and evidence outside guest writes", async () => {
  const root = await mkdtemp(join(tmpdir(), "live hook & $ literal '-")), processes = new Processes();
  try {
    // The real child rejects GraphCoder's arguments, after the original case
    // preparation has written its actual settings. No model is launched.
    const result = await runCase(process.execPath, root, root, "session", new AbortController().signal);
    assert.equal(result.status, "fail");
    assert.deepEqual(result.failure.assertion, {
      operator: "strictEqual", actual: String(result.captures[0].code), expected: "0",
    });
    const settings = JSON.parse(await readFile(join(root, "session/settings/.claude/settings.json"), "utf8"));
    const [{ hooks: [hook] }] = settings.hooks.PostToolUse;
    const original = childResults[0].events[0];
    const executed = await processes.run(hook.command, hook.args,
      { cwd: join(root, "session/workspace") }, JSON.stringify(original));
    assert.equal(executed.code, 0, executed.stderr);
    assert.equal(executed.stdout, "", "the live recorder must not inject model context");
    // A real workspace command can replace a same-named project file. It must
    // not replace the host recorder's original captured envelope.
    const forged = { ...original, tool_response: { error: "workspace forgery" } };
    const overwritten = await processes.run(process.execPath,
      ["-e", "require('node:fs').writeFileSync('hooks.jsonl', process.argv[1])", JSON.stringify(forged)],
      { cwd: join(root, "session/workspace") });
    assert.equal(overwritten.code, 0, overwritten.stderr);
    const recorded = JSON.parse(await readFile(hook.args[1], "utf8"));
    assert.deepEqual(recorded, original);
    requireTools([recorded], ["spawn"]);
    // A native command rejection happens before tool assertions. The actual
    // recorder's output must still reach the failed attempt's raw evidence.
    const failedRoot = join(root, "failed-answer");
    await mkdir(join(failedRoot, "shell"), { recursive: true });
    await writeFile(join(failedRoot, "shell/hooks.jsonl"), await readFile(hook.args[1]));
    const rejected = await runCase(process.execPath, root, failedRoot, "shell", new AbortController().signal);
    assert.equal(rejected.status, "fail");
    assert.deepEqual(rejected.hooks, [original]);
    assert.deepEqual(rejected.failure.assertion, result.failure.assertion);
    // A damaged recording is independent of the same original command exit.
    const damagedRoot = join(root, "damaged-recording");
    await mkdir(join(damagedRoot, "shell"), { recursive: true });
    await writeFile(join(damagedRoot, "shell/hooks.jsonl"), "{");
    const damaged = await runCase(process.execPath, root, damagedRoot, "shell", new AbortController().signal);
    assert.deepEqual(damaged.failure.assertion, result.failure.assertion);
    assert.match(damaged.hooks_failure.error, /SyntaxError/);
    assert.equal(damaged.hooks_failure.step, "case-hooks");
    assert.equal(damaged.phases.at(-1).phase, "retired");
    assert.equal(damaged.phases.at(-1).outcome, "completed");
    // Native launch/capture rejection is independent of existing recorder
    // evidence: a missing result object must not suppress the unread log.
    const missingRoot = join(root, "missing-capture");
    await mkdir(join(missingRoot, "shell"), { recursive: true });
    await writeFile(join(missingRoot, "shell/hooks.jsonl"), await readFile(hook.args[1]));
    const missing = await runCase(join(root, "absent-executable"), root, missingRoot, "shell", new AbortController().signal);
    assert.equal(missing.status, "fail");
    assert.equal(missing.code, "ENOENT");
    assert.deepEqual(missing.captures, []);
    assert.deepEqual(missing.hooks, [original]);
    assert.match(missing.failure.error, /ENOENT/);
  } finally {
    await processes.close();
    await rm(root, { recursive: true, force: true });
  }
});
