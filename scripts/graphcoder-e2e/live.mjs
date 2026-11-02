// Real Codex-login acceptance. Login directories are passed to GraphCoder by
// path; this driver never opens, copies or serializes their contents.
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
import { fileBytes, toolOutcome } from "./assertions.mjs";

const recovery = "LIVE_CODEX_HOME=/absolute/existing/login node scripts/graphcoder-e2e/live.mjs GRAPHCODER FRESH_OUTPUT SAMPLES [CASE ...]";
export const liveCases = ["session", "shell", "child-result", "resume", "fork-merge"];

export function answer(result, format) {
  assert.equal(result.code, 0, `GraphCoder exit ${result.code}: ${result.stderr}`);
  assert.equal(result.signal, null, "GraphCoder was interrupted");
  if (format === "text") return result.stdout.replace(/\r?\n$/, "");
  assert.equal(format, "jsonl");
  const lines = result.stdout.trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
  assert.equal(lines.length, 1, "JSONL needs exactly one authoritative answer");
  const [line] = lines;
  assert.ok(typeof line.id === "string" && Number.isSafeInteger(line.inbox) && line.inbox >= 0,
    "missing committed message identity");
  assert.equal(line.error, undefined, `failed turn: ${line.error}`);
  assert.equal(typeof line.text, "string", "missing answer text");
  return line;
}

export function requireTools(hooks, names, expectedChildResult, mergeWorkspace) {
  const remaining = new Set(names);
  const mergeCwd = mergeWorkspace === undefined ? undefined : toNamespacedPath(mergeWorkspace);
  let child, childName, consumed = false, merged = false;
  for (const hook of hooks) {
    if (hook.hook_event_name !== "PostToolUse") continue;
    if (hook.tool_response === undefined || hook.tool_response === null)
      assert.fail(`step=admit completed ${hook.tool_name} result; recovery=${recovery}`);
    toolOutcome(hook.tool_response, undefined, recovery);
    remaining.delete(hook.tool_name);
    if (expectedChildResult === undefined) continue;
    if (hook.tool_name === "spawn" && typeof hook.tool_response?.agent === "string") {
      assert.equal(child, undefined, "single-child journey spawned another child");
      child = hook.tool_response.agent;
      childName = hook.tool_response.name;
    } else if (hook.tool_name === "wait" && child && hook.tool_response?.settled?.length) {
      const references = hook.tool_input?.agents;
      assert.ok(Array.isArray(references) && references.length === 1
        && (references[0] === child || (typeof childName === "string" && references[0] === childName)),
      "wait used a different child reference");
      assert.deepEqual(hook.tool_response.running, [], "child remains running");
      assert.equal(hook.tool_response.settled.length, 1, "wait settled a different child set");
      const [settled] = hook.tool_response.settled;
      assert.equal(settled.agent, child, "settlement names a different child");
      assert.equal(settled.phase, "idle", "child did not settle successfully");
      assert.equal(settled.answer, expectedChildResult, "parent did not consume the child's exact result");
      consumed = true;
    } else if (mergeWorkspace !== undefined && consumed && typeof childName === "string"
      && hook.tool_name === "fs" && typeof hook.cwd === "string" && isAbsolute(hook.cwd)
      && toNamespacedPath(hook.cwd) === mergeCwd
      && hook.tool_response?.result?.Committed?.message === `merge agents/${childName}`) {
      // The filesystem generates this message from its completed join operand.
      merged = true;
    }
  }
  for (const name of remaining) assert.fail(`missing completed ${name} tool call`);
  if (expectedChildResult !== undefined) assert.ok(consumed, "missing completed public wait for the spawned child's result");
  if (mergeWorkspace !== undefined) assert.ok(merged,
    `step=admit committed merge of the settled child's branch in its parent workspace; recovery=${recovery}`);
}

export async function runCase(binary, login, root, name, signal) {
  const setupBegin = performance.now();
  const directory = join(root, name), workspace = join(directory, "workspace");
  const settings = join(directory, "settings");
  // Keep host observations outside the guest's mutable project tree.
  const hookFile = join(directory, "hooks.jsonl");
  const operation = randomUUID(), captures = [], processes = new Processes();
  const marker = `LIVE_${randomUUID().replaceAll("-", "")}`;
  let step = "prepare workspace and isolated settings", env, childHooks, begin, hooksPending = false;
  const invoke = async (prompt, format = "text") => {
    hooksPending = true;
    const args = ["--workspace", workspace, "--settings-root", settings,
      "--model", "openai/gpt-6-luna", "--operation", operation,
      ...(format === "text" ? ["-p", prompt] : ["--input-format", "jsonl"])];
    const result = await processes.run(binary, args, { cwd: workspace, env, scopeReport: true },
      format === "text" ? "" : JSON.stringify({ prompt, id: "live" }) + "\n");
    captures.push({ argv: args, ...result });
    return answer(result, format);
  };
  const hooks = async () => {
    hooksPending = false;
    try { return (await readFile(hookFile, "utf8")).trim().split(/\r?\n/).filter(Boolean).map(JSON.parse); }
    catch (error) { if (error.code === "ENOENT") return []; throw error; }
  };
  const collaborate = async (task, followup) => {
    hooksPending = true;
    const fixture = { id: name, workspace, env, args: ["--workspace", workspace,
      "--settings-root", settings, "--model", "openai/gpt-6-luna"] };
    await conversation(processes, binary, fixture, { operation, turns: [
      { id: "spawn", text: task, answer: "SPAWNED" },
      { id: "report", text: followup, answer: marker },
    ] }, capture => captures.push(capture));
  };
  const phases = [];
  const prepare = async () => {
    await mkdir(workspace, { recursive: true });
    env = environment(settings);
    // Reuse only the admitted login path after stripping provider keys.
    env.CODEX_HOME = login;
    const recorder = fileURLToPath(new URL("./hook.mjs", import.meta.url));
    await writeFile(join(settings, ".claude/settings.json"), JSON.stringify({ hooks: {
      PostToolUse: [{ hooks: [{ type: "command", command: process.execPath, args: [recorder, hookFile] }] }],
    } }));
  };
  try {
    const receipt = await withOwnedCase({ processes, signal, origin: setupBegin, prepare,
      onPhase: phase => phases.push(phase), run: async () => {
      begin = performance.now();
      step = "invoke shipping GraphCoder";
      if (name === "session") {
        const text = await invoke(`Reply with exactly ${marker}. Do not use tools.`);
        step = "assert session answer";
        assert.equal(text, marker);
      } else if (name === "shell") {
        await invoke(`Use the shell tool to create shell.txt containing exactly the UTF-8 bytes ${marker}, with no newline or other bytes, then reply DONE.`);
        step = "assert shell effect and completed tool event";
        const recorded = childHooks = await hooks();
        await fileBytes(join(workspace, "shell.txt"), marker);
        requireTools(recorded, ["shell"]);
      } else if (name === "child-result") {
        await collaborate(`Spawn a child named worker, asking it to reply with exactly ${marker}. End this turn with exactly SPAWNED so the spawn commits. Retain the exact canonical agent path returned by spawn for the next turn.`,
          "Use wait with the exact canonical agent path from the preceding spawn result. Read that child's settled idle answer, then reply with exactly that answer, without additional text.");
        step = "assert child result read by parent";
        const recorded = childHooks = await hooks();
        requireTools(recorded, ["spawn", "wait"], marker);
      } else if (name === "resume") {
        const prompt = `Use the shell tool to append exactly the UTF-8 bytes ${marker} to once.txt, with no newline or other bytes, then reply with exactly ${marker}.`;
        const first = await invoke(prompt, "jsonl");
        const before = childHooks = await hooks();
        requireTools(before, ["shell"]);
        step = "reopen settled operation without repeating work";
        const second = await invoke(prompt, "jsonl");
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
    } });
    assert.ok(receipt.seconds < 10, "case exceeded ten seconds");
    return { case: name, platform: process.platform, status: "pass", seconds: receipt.seconds,
      setup_seconds: receipt.preparation_seconds, total_seconds: receipt.total_seconds, phases, captures, hooks: childHooks };
  } catch (error) {
    let result = { case: name, platform: process.platform, status: "fail", code: error.code,
      failure: failureReport(error),
      phase: phases.findLast(phase => phase.outcome === "failed")?.phase ?? (begin === undefined ? "admission" : "run"),
      ...(begin === undefined ? {} : { seconds: (performance.now() - begin) / 1000 }),
      setup_seconds: phases.find(phase => phase.phase === "preparation" && phase.outcome !== "entered")?.seconds ?? 0,
      total_seconds: (performance.now() - setupBegin) / 1000, phases,
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
  assert.ok(binary && isAbsolute(binary) && output && login && isAbsolute(login), `step=admit arguments and existing login path; recovery=${recovery}`);
  const names = selected.length ? selected : liveCases;
  assert.equal(new Set(names).size, names.length, `step=reject duplicate cases; recovery=${recovery}`);
  for (const name of names) assert.ok(liveCases.includes(name), `step=admit case ${name}; recovery=${recovery}`);
  await mkdir(output); // An existing evidence identity must never be reused.
  const root = scratchDirectory("graphcoder-live-");
  const results = [];
  sampling: for (let sample = 1; sample <= samples; sample++) {
    for (const name of names) {
      if (signal.aborted) break sampling;
      results.push({ sample, ...await runCase(resolve(binary), login, join(root, String(sample)), name, signal) });
      if (signal.aborted) break sampling;
    }
  }
  const report = { tier: "real-codex-login", model: "openai/gpt-6-luna", platform: process.platform,
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
