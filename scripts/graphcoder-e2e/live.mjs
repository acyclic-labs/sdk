// Real provider acceptance. Catalogs and optional login directories are passed
// by path; this driver never reads or copies login contents.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve, toNamespacedPath } from "node:path";
import { fileURLToPath } from "node:url";
import { Processes } from "./process.mjs";
import { environment, scratchDirectory } from "./scratch.mjs";
import { commandCancellation, withOwnedCase } from "./suite.mjs";
import { failedCase, failureReport, publishReport, summarizeCases } from "./report.mjs";
import { conversation } from "./conversation.mjs";
import { executionSucceeded, requireReportCapability, fileBytes, fileRead, jsonlReply, toolOutcome } from "./assertions.mjs";

const recovery = "LIVE_CODEX_HOME=/absolute/existing/login node scripts/graphcoder-e2e/live.mjs GRAPHCODER FRESH_OUTPUT SAMPLES [CASE ...]";
export const liveCases = ["session", "instructions", "skill-invocation", "shell", "file-tools", "child-result", "nested-children", "resume", "fork-merge"];

export function answer(result, format, expected, id) {
  executionSucceeded(result);
  let value;
  if (format === "text") value = result.stdout.replace(/\r?\n$/, "");
  else {
    assert.equal(format, "jsonl");
    const line = jsonlReply(result.stdout, id);
    assert.equal(line.error, undefined, `failed turn: ${line.error}`);
    value = line;
  }
  if (expected !== undefined) assert.equal(format === "text" ? value : value.text, expected, "unexpected answer text");
  return value;
}

export function requireTools(hooks, names, expectedChildResult, mergeWorkspace, expectedChildren = 1) {
  const remaining = new Set(names);
  const mergeCwd = mergeWorkspace === undefined ? undefined : toNamespacedPath(mergeWorkspace);
  const children = new Map();
  let merged = false;
  for (const hook of hooks) {
    if (hook.hook_event_name !== "PostToolUse") continue;
    if (hook.tool_response === undefined || hook.tool_response === null)
      assert.fail(`step=admit completed ${hook.tool_name} result; recovery=${recovery}`);
    toolOutcome(hook.tool_response, undefined, recovery);
    remaining.delete(hook.tool_name);
    if (expectedChildResult === undefined) continue;
    if (hook.tool_name === "spawn" && typeof hook.tool_response?.agent === "string") {
      const child = hook.tool_response.agent;
      assert.ok(!children.has(child), "journey repeated a spawned child");
      const selected = { agent: child, name: hook.tool_response.name, consumed: false };
      children.set(child, selected);
    } else if (hook.tool_name === "wait" && hook.tool_response?.settled?.length) {
      const agents = hook.tool_input?.agents;
      const reference = Array.isArray(agents) && agents.length === 1 ? agents[0] : undefined;
      const only = expectedChildren === 1 && children.size === 1 ? children.values().next().value : undefined;
      const child = children.get(reference) ?? (only?.name === reference ? only : undefined);
      assert.ok(child,
      "wait used a different child reference");
      assert.deepEqual(hook.tool_response.running, [], "child remains running");
      assert.equal(hook.tool_response.settled.length, 1, "wait settled a different child set");
      const [settled] = hook.tool_response.settled;
      assert.equal(settled.agent, child.agent, "settlement names a different child");
      assert.equal(settled.phase, "idle", "child did not settle successfully");
      assert.equal(settled.answer, expectedChildResult, "parent did not consume the child's exact result");
      child.consumed = true;
    } else if (mergeWorkspace !== undefined && children.size === 1
      && hook.tool_name === "fs" && typeof hook.cwd === "string" && isAbsolute(hook.cwd)
      && toNamespacedPath(hook.cwd) === mergeCwd
      && children.values().next().value.consumed
      && hook.tool_response?.result?.Committed?.message === `merge agents/${children.values().next().value.name}`) {
      // The filesystem generates this message from its completed join operand.
      merged = true;
    }
  }
  for (const name of remaining) assert.fail(`missing completed ${name} tool call`);
  if (expectedChildResult !== undefined) {
    assert.equal(children.size, expectedChildren, "journey spawned a different child set");
    for (const child of children.values()) assert.ok(child.consumed, "missing completed public wait for the spawned child's result");
    if (expectedChildren === 2) {
      const [first, second] = children.keys();
      assert.ok(first.startsWith(second + "/children/") || second.startsWith(first + "/children/"),
        "nested journey requires the original descendant path, not two siblings");
    }
  }
  if (mergeWorkspace !== undefined) assert.ok(merged,
    `step=admit committed merge of the settled child's branch in its parent workspace; recovery=${recovery}`);
}

export async function runCase(binary, login, root, name, signal, { model = "openai/gpt-6-luna", catalog } = {}) {
  const setupBegin = performance.now();
  const directory = join(root, name), workspace = join(directory, "workspace");
  const settings = join(directory, "settings");
  // Keep host observations outside the guest's mutable project tree.
  const hookFile = join(directory, "hooks.jsonl");
  const operation = randomUUID(), captures = [], processes = new Processes();
  const marker = `LIVE_${randomUUID().replaceAll("-", "")}`;
  const messageId = "live";
  let step = "prepare workspace and isolated settings", env, childHooks, begin, receipt, hooksPending = false;
  const providerArgs = ["--model", model, ...(catalog ? ["--catalog", catalog] : [])];
  const invoke = async (prompt, format, expected) => {
    hooksPending = true;
    const args = ["--workspace", workspace, "--settings-root", settings,
      ...providerArgs, "--operation", operation,
      ...(format === "text" ? ["-p", prompt] : ["--input-format", "jsonl"])];
    const result = await processes.run(binary, args, { cwd: workspace, env, scopeReport: true },
      format === "text" ? "" : JSON.stringify({ prompt, id: messageId }) + "\n");
    captures.push({ argv: args, ...result });
    step = "assert GraphCoder answer";
    const value = answer(result, format, expected, messageId);
    requireReportCapability(result, recovery);
    return value;
  };
  const hooks = async () => {
    hooksPending = false;
    try { return (await readFile(hookFile, "utf8")).trim().split(/\r?\n/).filter(Boolean).map(JSON.parse); }
    catch (error) { if (error.code === "ENOENT") return []; throw error; }
  };
  const collaborate = async (task, followup) => {
    hooksPending = true;
    const fixture = { workspace, env, args: ["--workspace", workspace,
      "--settings-root", settings, ...providerArgs] };
    await conversation(processes, binary, fixture, { operation, turns: [
      { id: "spawn", text: task, answer: "SPAWNED" },
      { id: "report", text: followup, answer: marker },
    ] }, capture => captures.push(capture));
    requireReportCapability(captures.at(-1), recovery);
  };
  const phases = [];
  const prepare = async () => {
    await mkdir(workspace, { recursive: true });
    if (name === "instructions") {
      await writeFile(join(workspace, "AGENTS.md"), `agents_token: AGENT_${marker}\n`);
      await writeFile(join(workspace, "CLAUDE.md"), `claude_token: CLAUDE_${marker}\n`);
    } else if (name === "skill-invocation") {
      const skill = join(workspace, ".agents/skills/inventory");
      await mkdir(skill, { recursive: true });
      await writeFile(join(skill, "SKILL.md"), `---\nname: inventory\ndescription: Report the selected inventory token\n---\nReply with exactly SKILL_${marker} and no additional text. Do not use tools.\n`);
    }
    env = environment(settings);
    // Reuse only the admitted login path after stripping provider keys.
    if (login) env.CODEX_HOME = login;
    const recorder = fileURLToPath(new URL("./hook.mjs", import.meta.url));
    await writeFile(join(settings, ".claude/settings.json"), JSON.stringify({ hooks: {
      PostToolUse: [{ hooks: [{ type: "command", command: process.execPath, args: [recorder, hookFile] }] }],
    } }));
  };
  try {
    receipt = await withOwnedCase({ processes, signal, origin: setupBegin, prepare,
      onPhase: phase => phases.push(phase), run: async () => {
      begin = performance.now();
      step = "invoke shipping GraphCoder";
      if (name === "session") {
        await invoke(`Reply with exactly ${marker}. Do not use tools.`, "text", marker);
      } else if (name === "instructions" || name === "skill-invocation") {
        await invoke(name === "instructions"
          ? "Report agents_token then claude_token from the current project instructions, separated by one space. Do not use tools. Reply with exactly those two values and no additional text."
          : "/inventory Follow the selected skill precisely.", "text",
        name === "instructions" ? `AGENT_${marker} CLAUDE_${marker}` : `SKILL_${marker}`);
        step = "assert selected instructions reached the model without a file tool";
        const recorded = childHooks = await hooks();
        requireTools(recorded, []);
        assert.equal(recorded.length, 0, "instruction acceptance used a tool instead of the selected context");
      } else if (name === "shell") {
        await invoke(`Use the shell tool to create shell.txt containing exactly the UTF-8 bytes ${marker}, with no newline or other bytes, then reply with exactly DONE and no additional text.`, "text", "DONE");
        step = "assert shell effect and completed tool event";
        const recorded = childHooks = await hooks();
        await fileBytes(join(workspace, "shell.txt"), marker);
        requireTools(recorded, ["shell"]);
      } else if (name === "file-tools") {
        await invoke(`Use write to create result.txt containing exactly BEFORE_${marker}, with no newline. Use edit to replace BEFORE_ with AFTER_. Use read to verify the edited file, then reply with exactly its contents and no additional text.`, "text", `AFTER_${marker}`);
        step = "assert write edit and read share the published file";
        const recorded = childHooks = await hooks();
        await fileBytes(join(workspace, "result.txt"), `AFTER_${marker}`);
        requireTools(recorded, ["write", "edit", "read"]);
        const read = recorded.findLast(hook => hook.tool_name === "read");
        fileRead(read.tool_response, "result.txt", `AFTER_${marker}`);
      } else if (name === "child-result") {
        await collaborate(`Spawn a child named worker, asking it to reply with exactly ${marker}. End this turn with exactly SPAWNED so the spawn commits. Retain the exact canonical agent path returned by spawn for the next turn.`,
          "Use wait with the exact canonical agent path from the preceding spawn result. Read that child's settled idle answer, then reply with exactly that answer, without additional text.");
        step = "assert child result read by parent";
        const recorded = childHooks = await hooks();
        requireTools(recorded, ["spawn", "wait"], marker);
      } else if (name === "nested-children") {
        const task = `Spawn exactly one child named leaf, asking it to reply with exactly ${marker} and no additional text. Preserve the canonical agent path from spawn. In a later distinct tool call, use wait with that exact path, read its settled idle answer, then reply with exactly that answer. Do not return a console-only code result or use a top-level return statement; a code tool program returns its final expression.`;
        await collaborate(`Spawn exactly one child named worker with this task: ${JSON.stringify(task)}. Preserve the returned canonical agent path. End this turn with exactly SPAWNED so the worker spawn commits.`,
          "Use wait with the exact canonical worker path from the preceding spawn result. Read its settled idle answer, then reply with exactly that answer and no additional text.");
        step = "assert original worker and descendant settlements";
        const recorded = childHooks = await hooks();
        requireTools(recorded, ["spawn", "wait"], marker, undefined, 2);
      } else if (name === "resume") {
        const prompt = `Use the shell tool to append exactly the UTF-8 bytes ${marker} to once.txt, with no newline or other bytes, then reply with exactly ${marker}.`;
        const first = await invoke(prompt, "jsonl", marker);
        const before = childHooks = await hooks();
        requireTools(before, ["shell"]);
        step = "reopen settled operation without repeating work";
        const second = await invoke(prompt, "jsonl", marker);
        assert.deepEqual(second, first, "reopen changed the committed result");
        await fileBytes(join(workspace, "once.txt"), marker);
        assert.equal((childHooks = await hooks()).filter(hook => hook.hook_event_name === "PostToolUse").length,
          before.filter(hook => hook.hook_event_name === "PostToolUse").length, "replay repeated a tool call");
      } else if (name === "fork-merge") {
        await collaborate(`Spawn a child named worker to use shell to write child.txt in its project checkout containing exactly the UTF-8 bytes ${marker}, with no newline or other bytes, then reply with exactly ${marker}. End your spawn turn with exactly SPAWNED so it commits. Retain the exact canonical agent path returned by spawn for the next turn. The spawn result is a canonical agent path, not a branch alias.`,
          "Use wait with the exact canonical agent path from the preceding spawn result and read that child's settled idle answer. Its changes are on agents/<spawn name>. Use fs diff to inspect that branch, then fs merge to bring it into your project, and read child.txt. Reply with exactly those file contents, without additional text.");
        step = "assert child fork and merged parent bytes";
        const recorded = childHooks = await hooks();
        await fileBytes(join(workspace, "child.txt"), marker);
        requireTools(recorded, ["spawn", "wait", "shell", "fs"], marker, workspace);
      } else assert.fail(`unknown case ${name}`);
      if (hooksPending) {
        step = "assert remaining completed tool outcomes";
        requireTools(childHooks = await hooks(), []);
      }
    } });
    step = "assert completed case latency";
    if (!(receipt.seconds < 10)) throw new assert.AssertionError({
      actual: receipt.seconds, expected: 10, operator: "<", message: "case exceeded ten seconds",
    });
    return { case: name, platform: process.platform, status: "pass", seconds: receipt.seconds,
      setup_seconds: receipt.preparation_seconds, total_seconds: receipt.total_seconds, phases, captures, hooks: childHooks };
  } catch (error) {
    // Completed measurements belong to the original owner. For an interrupted
    // case, stop the fallback clock before formatting its diagnostic.
    const ended = receipt === undefined ? performance.now() : undefined;
    let result = { case: name, platform: process.platform, status: "fail", code: error.code,
      failure: failureReport(error),
      phase: receipt === undefined
        ? phases.findLast(phase => phase.outcome === "failed")?.phase ?? (begin === undefined ? "admission" : "run")
        : "latency",
      ...(begin === undefined ? {} : { seconds: receipt?.seconds ?? (ended - begin) / 1000 }),
      setup_seconds: receipt?.preparation_seconds
        ?? phases.find(phase => phase.phase === "preparation" && phase.outcome !== "entered")?.seconds ?? 0,
      total_seconds: receipt?.total_seconds ?? (ended - setupBegin) / 1000, phases,
      error: `step=${step}: ${error.message}; recovery=${recovery}`, captures, hooks: childHooks };
    // The original process and retirement have joined. A rejected answer can
    // precede the tool assertions, but cannot discard their recorded evidence.
    // A failed read is retained separately and is never retried.
    if (hooksPending) {
      try { result.hooks = await hooks(); }
      catch (evidenceError) { result = failedCase(result, failureReport(evidenceError), "hooks", recovery); }
    }
    return result;
  }
}

async function main() {
  const cancellation = commandCancellation();
  try { await campaign(cancellation.signal); }
  finally { cancellation.dispose(); }
}

async function campaign(signal) {
  const [binary, output, count, ...selected] = process.argv.slice(2);
  const samples = Number(count);
  assert.ok(/^[1-9][0-9]*$/.test(count ?? "") && Number.isSafeInteger(samples),
    `step=admit explicit positive sample count; recovery=${recovery}`);
  const login = process.env.LIVE_CODEX_HOME ?? process.env.E2E_LIVE_CODEX_HOME;
  const catalog = process.env.E2E_LIVE_CATALOG, model = process.env.E2E_LIVE_MODEL ?? "openai/gpt-6-luna";
  assert.ok(binary && isAbsolute(binary) && output && (catalog ? isAbsolute(catalog) : login && isAbsolute(login)),
    `step=admit arguments and provider catalog or existing login path; recovery=${recovery}`);
  const names = selected.length ? selected : liveCases;
  assert.equal(new Set(names).size, names.length, `step=reject duplicate cases; recovery=${recovery}`);
  for (const name of names) assert.ok(liveCases.includes(name), `step=admit case ${name}; recovery=${recovery}`);
  await mkdir(output); // An existing evidence identity must never be reused.
  const root = scratchDirectory("graphcoder-live-");
  const results = [];
  sampling: for (let sample = 1; sample <= samples; sample++) {
    for (const name of names) {
      if (signal.aborted) break sampling;
      results.push({ sample, ...await runCase(resolve(binary), login, join(root, String(sample)), name, signal, { model, catalog }) });
      if (signal.aborted) break sampling;
    }
  }
  const report = { tier: catalog ? "real-provider-catalog" : "real-codex-login", model, platform: process.platform,
    measurement: "complete case including original physical retirement; nearest-rank successful-case percentiles, not overhead",
    cells: summarizeCases(names, results), results,
    ...(signal.aborted ? { cancellation: signal.reason.message } : {}),
    pass: !signal.aborted && results.every(result => result.status === "pass") };
  await publishReport(join(output, "matrix.json"), report, undefined, signal);
  process.exitCode = !signal.aborted && report.pass ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(); }
  catch (error) {
    console.error(JSON.stringify({ ...failureReport(error), step: "live admission or publication", recovery }));
    process.exitCode = 1;
  }
}
