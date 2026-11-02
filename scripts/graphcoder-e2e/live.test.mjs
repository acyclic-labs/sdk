import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve, toNamespacedPath } from "node:path";
import { fileURLToPath } from "node:url";
import { Processes } from "./process.mjs";
import { executionSucceeded, fileBytes, fileRead, jsonlReply, messageReply, requests, toolOutcome } from "./assertions.mjs";
const captured = JSON.parse(readFileSync(new URL("./fixtures/shipping-output.json", import.meta.url), "utf8")).captures;
const childResults = JSON.parse(readFileSync(new URL("./fixtures/live-child-results.json", import.meta.url), "utf8")).cases;
const mergeResults = JSON.parse(readFileSync(new URL("./fixtures/live-merge-results.json", import.meta.url), "utf8")).cases;
const shellResults = JSON.parse(readFileSync(new URL("./fixtures/live-shell-results.json", import.meta.url), "utf8")).cases;
const nativeFailure = JSON.parse(readFileSync(new URL("./fixtures/native-execution-failure.json", import.meta.url), "utf8")).capture;
const nativeReady = JSON.parse(readFileSync(new URL("./fixtures/native-report-ready.json", import.meta.url), "utf8")).ready;
const fileReads = JSON.parse(readFileSync(new URL("./fixtures/live-file-read-results.json", import.meta.url), "utf8")).cases;
const codeFailure = JSON.parse(readFileSync(new URL("./fixtures/live-code-failure.json", import.meta.url), "utf8")).event;
const nestedFailures = JSON.parse(readFileSync(new URL("./fixtures/live-nested-settlement-failure.json", import.meta.url), "utf8")).cases;
const childMessage = JSON.parse(readFileSync(new URL("./fixtures/live-child-message.json", import.meta.url), "utf8"));
const committedReply = JSON.parse(captured["jsonl-reopen"].stdout);
import { answer, requireTools, runCase } from "./live.mjs";

test("completed code calls cannot erase the original VM refusal", () => {
  assert.throws(() => requireTools([codeFailure], ["code"]), error => {
    assert.ok(error.message.includes(codeFailure.tool_response.output.error));
    return true;
  });
  const selectedValue = structuredClone(codeFailure);
  selectedValue.tool_response.output = { value: { error: codeFailure.tool_response.output.error } };
  requireTools([selectedValue], ["code"]);
  const opaqueValue = structuredClone(codeFailure);
  delete opaqueValue.tool_response.version;
  delete opaqueValue.tool_response.context;
  requireTools([opaqueValue], ["code"]);
});

test("completed wait retains the original failed descendant settlement", () => {
  for (const { event } of nestedFailures) {
    assert.throws(() => requireTools([event], ["wait"]), /conflict: volume alias names another binding/);
    toolOutcome(event.tool_response, ["volume alias names another binding"], "inspect original child settlement");
    // A selected program value is data, not a native wait-result envelope.
    toolOutcome({ version: 2, context: [], output: { value: event.tool_response } });
  }
});

test("every retained tool failure needs its own selected allowance", () => {
  const allowed = "volume alias names another binding";
  const original = nestedFailures[0].event.tool_response;
  const first = original.settled[0], second = structuredClone(nestedFailures[1].event.tool_response.settled[0]);
  // Two real captured failures supply the envelopes; substitute the other
  // retained VM refusal to make the second child's disposition independent.
  second.phase.done.outcome.failed = { message: codeFailure.tool_response.output.error };
  for (const settled of [[first, second], [second, first]]) {
    const value = { ...original, settled };
    assert.throws(() => toolOutcome(value, [allowed], "inspect original settlement"),
      error => error.message.includes(codeFailure.tool_response.output.error));
    toolOutcome(value, [allowed, codeFailure.tool_response.output.error]);
  }
  for (const value of [
    { ...original, error: allowed, settled: [second] },
    { version: 2, context: [], error: allowed, output: { error: codeFailure.tool_response.output.error } },
    { error: allowed, success: false },
  ]) assert.throws(() => toolOutcome(value, [allowed], "inspect original tool failures"));
});

test("file reads retain the original authored or content source", () => {
  for (const { event } of fileReads) {
    const original = event.tool_response, expected = original.text;
    const kind = Object.keys(original.file)[0], source = original.file[kind];
    assert.equal(fileRead(original, event.tool_input.path, expected), source);
    for (const mutate of [
      value => { delete value.file; },
      value => { value.file.other = value.file[kind]; },
      value => { value.file[kind].path = "different.txt"; },
      value => { delete value.file[kind].volume; },
      value => { delete value.file[kind][kind === "authored" ? "generation" : "version"]; },
      value => { value.text += " "; },
    ]) {
      const changed = structuredClone(original); mutate(changed);
      assert.throws(() => fileRead(changed, event.tool_input.path, expected));
    }
  }
});

test("successful execution cannot erase the original returned native cause", () => {
  let stderrReads = 0;
  for (const original of [captured["text-responses"], captured["jsonl-reopen"]]) executionSucceeded({
    ...original, get stderr() { stderrReads++; return original.stderr; },
  });
  assert.equal(stderrReads, 0, "successful status admission must not reread its transcript for an unused error message");
  // Mutate only the original failure's independent exit/ACK observations.
  // Successful stdout comes from its separately retained shipping contract.
  // Neither can promote a retained native failure to a successful execution.
  for (const [format, successful] of [["text", captured["text-responses"]], ["jsonl", captured["jsonl-reopen"]]]) {
    for (const field of ["execution_failure", "startup_rejection"]) {
      for (const acknowledged of [false, true]) {
        const failure = nativeFailure.termination.execution_failure;
        const termination = { ...nativeFailure.termination, execution_failure: undefined,
          [field]: failure, report_acknowledged: acknowledged,
          ...(acknowledged ? { report_outcome: "empty", missing_wait_report: false } : {}) };
        assert.throws(() => answer({ ...successful, termination }, format, undefined, "e2e"), error => {
          assert.equal(error.actual, failure, "retain the exact native cause as the failed assertion operand");
          assert.match(error.message, /^selected native execution reported failure/);
          return true;
        });
      }
    }
  }
  assert.throws(() => executionSucceeded(nativeFailure), error => {
    assert.equal(error.actual, nativeFailure.termination.execution_failure);
    return true;
  });
});

test("reply-only cases reject recorded tool failure despite successful native completion", {
  skip: process.platform === "win32" && "the protocol child uses an executable POSIX script",
}, async () => {
  const root = await mkdtemp(join(tmpdir(), "live-reply-tools-"));
  try {
    const catalog = join(root, "catalog.json");
    await writeFile(catalog, JSON.stringify({ selected: { endpoint: "http://127.0.0.1" } }));
    for (const [captured, capability, journey, selection] of [[undefined, "ready", "session"],
      ...shellResults.map(captured => [captured, "ready", "session"]),
      [undefined, "absent", "session"], [undefined, "foreign", "session"],
      [undefined, "absent", "child-result"],
      ...["openai/gpt-6-luna", "anthropic/claude-haiku-5-5", "gemini/gemini-3.8-flash"]
        .map(model => [undefined, "ready", "session", { model, catalog }])]) {
      const name = `${captured?.status ?? "no-tools"}-${capability}-${journey}-${selection?.model.split("/")[0] ?? "login"}`, directory = join(root, name);
      await mkdir(directory);
      const program = join(directory, "reply.mjs");
      // This native child speaks only the driver's text protocol. Tool events
      // are original shipping captures, replayed through the real recorder.
      await writeFile(program, `#!${process.execPath}
import { readFileSync, writeSync } from "node:fs";
import { join } from "node:path";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
const capability = ${JSON.stringify(capability)};
const selection = ${JSON.stringify(selection ?? null)};
if (selection) {
  assert.equal(process.argv[process.argv.indexOf("--model") + 1], selection.model);
  assert.equal(process.argv[process.argv.indexOf("--catalog") + 1], selection.catalog);
  assert.equal(JSON.parse(readFileSync(selection.catalog, "utf8")).selected.endpoint, "http://127.0.0.1");
}
if (capability !== "absent") writeSync(Number(process.env.ACYCLIC_REPORT_FD), JSON.stringify({
  ...${JSON.stringify(nativeReady)}, generation: capability === "ready" ? process.env.ACYCLIC_REPORT_GENERATION : ${JSON.stringify(nativeReady.generation)},
}) + "\\n");
const settings = process.argv[process.argv.indexOf("--settings-root") + 1];
const event = ${JSON.stringify(captured?.event ?? null)};
if (event) {
  const { hooks: { PostToolUse: [{ hooks: [hook] }] } } = JSON.parse(readFileSync(join(settings, ".claude/settings.json"), "utf8"));
  const recorded = spawnSync(hook.command, hook.args, { input: JSON.stringify(event), encoding: "utf8" });
  if (recorded.error) throw recorded.error;
  if (recorded.status !== 0) throw new Error(recorded.stderr);
}
if (process.argv.includes("--input-format")) {
  let marker, turn = 0;
  createInterface({input:process.stdin}).on("line", line => {
    const input = JSON.parse(line);
    marker ??= input.prompt.match(/LIVE_[a-f0-9]+/)[0];
    process.stdout.write(JSON.stringify({ ...${JSON.stringify(committedReply)},
      id: input.id, inbox: ++turn, text: turn === 1 ? "SPAWNED" : marker }) + "\\n");
  });
} else {
  const prompt = process.argv[process.argv.indexOf("-p") + 1];
  process.stdout.write(prompt.match(/LIVE_[a-f0-9]+/)[0] + "\\n");
}
`);
      await chmod(program, 0o700);
      const result = await runCase(program, root, directory, journey, new AbortController().signal, selection);
      assert.equal(result.captures.length, 1);
      assert.equal(result.captures[0].code, 0, result.captures[0].stderr);
      assert.equal(result.captures[0].signal, null);
      assert.equal(result.status, captured?.status === "fail" || capability !== "ready" ? "fail" : "pass", result.error);
      if (captured?.status === "fail") {
        assert.deepEqual(result.hooks, [captured.event]);
        assert.match(result.failure.error, /unexpected tool error.*exit_code=1/);
      } else if (capability !== "ready") {
        assert.equal(result.captures[0].report_capability.ready, false);
        if (capability === "foreign") {
          assert.equal(result.captures[0].termination.control_failure, "foreign-or-invalid-acknowledgement");
          assert.match(result.failure.error, /step=admit complete native report stream/);
        } else {
          assert.equal(result.captures[0].termination, undefined);
          assert.match(result.failure.error, /step=admit selected native report capability/);
        }
      }
      assert.deepEqual([result.phases.at(-1).phase, result.phases.at(-1).outcome], ["retired", "completed"]);
    }
  } finally { await rm(root, { recursive: true }); }
});

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
  for (const tool_response of [{ error: "denied" }, { error: "" }, { error: false }]) {
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
    assert.throws(() => requireTools(original, ["spawn", "wait"], expected, undefined, 2),
      /different child set|different child reference/);
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

test("child message settlement follows its original selected send", () => {
  const { events: original, expected } = childMessage;
  const check = events => requireTools(events, ["spawn", "send", "wait"], expected);
  check(original);
  const selectedSend = events => events.find(event => event.tool_name === "send" && event.tool_input.message === expected);
  for (const mutate of [
    events => events.pop(),
    events => { events.splice(events.indexOf(selectedSend(events)), 1); },
    events => { selectedSend(events).hook_event_name = "PreToolUse"; },
    events => { selectedSend(events).tool_input.agent += "/other"; },
    events => { selectedSend(events).tool_response.sent += "/other"; },
    events => { selectedSend(events).tool_response.status = "refused"; },
    events => { selectedSend(events).tool_response.error = "message refused"; },
    events => { events.at(-1).tool_response.settled[0].answer = "READY"; },
    events => { events.at(-1).tool_response.settled[0].agent += "/other"; },
    events => { events.at(-1).tool_input.agents = ["other"]; },
    events => { events.splice(1, 0, events.pop()); },
    events => { events.push(structuredClone(events.find(event => event.tool_input?.message === "READY"))); },
    events => { events[1].tool_response.error = "earlier wait refused"; },
  ]) {
    const events = structuredClone(original);
    mutate(events);
    assert.throws(() => check(events));
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
