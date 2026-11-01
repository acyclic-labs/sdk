// Real Codex-login acceptance. Login directories are passed to GraphCoder by
// path; this driver never opens, copies or serializes their contents.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Processes } from "./process.mjs";
import { environment, scratchDirectory } from "./scratch.mjs";
import { withOwnedCase } from "./suite.mjs";
import { publishReport } from "./report.mjs";
import { conversation } from "./conversation.mjs";

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

export function requireTools(hooks, names, expectedChildResult) {
  const remaining = new Set(names);
  let child, consumed = false;
  for (const hook of hooks) {
    if (hook.hook_event_name !== "PostToolUse") continue;
    remaining.delete(hook.tool_name);
    if (expectedChildResult === undefined) continue;
    if (hook.tool_name === "spawn" && typeof hook.tool_response?.agent === "string") {
      child = hook.tool_response.agent;
    } else if (hook.tool_name === "wait" && child && hook.tool_response?.settled?.length) {
      assert.deepEqual(hook.tool_input?.agents, [child], "wait used a different child reference");
      assert.deepEqual(hook.tool_response.running, [], "child remains running");
      assert.equal(hook.tool_response.settled.length, 1, "wait settled a different child set");
      const [settled] = hook.tool_response.settled;
      assert.equal(settled.agent, child, "settlement names a different child");
      assert.equal(settled.phase, "idle", "child did not settle successfully");
      assert.equal(settled.answer, expectedChildResult, "parent did not consume the child's exact result");
      consumed = true;
    }
  }
  for (const name of remaining) assert.fail(`missing completed ${name} tool call`);
  if (expectedChildResult !== undefined) assert.ok(consumed, "missing completed public wait for the spawned child's result");
}

async function runCase(binary, login, root, name) {
  const setupBegin = performance.now();
  const directory = join(root, name), workspace = join(directory, "workspace");
  await mkdir(workspace, { recursive: true });
  const settings = join(directory, "settings");
  const env = environment(settings);
  // Explicitly authorized existing login, after shared fixture isolation and
  // provider-key stripping. Neither the HOME nor the rest of the profile moves.
  env.CODEX_HOME = login;
  const hookFile = join(workspace, "hooks.jsonl");
  const recorder = fileURLToPath(new URL("./hook.mjs", import.meta.url));
  const command = `"${process.execPath}" "${recorder}" "${hookFile}"`;
  await mkdir(join(settings, ".claude"), { recursive: true });
  await writeFile(join(settings, ".claude/settings.json"), JSON.stringify({ hooks: Object.fromEntries(
    ["SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop"].map(event => [event, [{ hooks: [{ type: "command", command }] }]])) }));
  const operation = randomUUID(), captures = [], processes = new Processes();
  const marker = `LIVE_${randomUUID().replaceAll("-", "")}`;
  let step = "invoke shipping GraphCoder", childHooks;
  const invoke = async (prompt, format = "text") => {
    const args = ["--workspace", workspace, "--settings-root", settings,
      "--model", "openai/gpt-6-luna", "--operation", operation,
      ...(format === "text" ? ["-p", prompt] : ["--input-format", "jsonl"])];
    const result = await processes.run(binary, args, { cwd: workspace, env, scopeReport: true },
      format === "text" ? "" : JSON.stringify({ prompt, id: "live" }) + "\n");
    captures.push({ argv: args, ...result });
    return answer(result, format);
  };
  const hooks = async () => {
    try { return (await readFile(hookFile, "utf8")).trim().split(/\r?\n/).filter(Boolean).map(JSON.parse); }
    catch (error) { if (error.code === "ENOENT") return []; throw error; }
  };
  const collaborate = async (task, followup) => {
    const fixture = { id: name, workspace, env, args: ["--workspace", workspace,
      "--settings-root", settings, "--model", "openai/gpt-6-luna"] };
    await conversation(processes, binary, fixture, { operation, turns: [
      { id: "spawn", text: task, answer: "SPAWNED" },
      { id: "report", text: followup, answer: marker },
    ] }, capture => captures.push(capture));
  };
  const begin = performance.now(), phases = [];
  try {
    const receipt = await withOwnedCase({ processes, onPhase: phase => phases.push(phase), run: async () => {
      if (name === "session") {
        const text = await invoke(`Reply with exactly ${marker}. Do not use tools.`);
        step = "assert session answer";
        assert.equal(text.trim(), marker);
      } else if (name === "shell") {
        await invoke(`Use the shell tool to create shell.txt containing exactly ${marker}, then reply DONE.`);
        step = "assert shell effect and completed tool event";
        assert.equal((await readFile(join(workspace, "shell.txt"), "utf8")).trim(), marker);
        requireTools(await hooks(), ["shell"]);
      } else if (name === "child-result") {
        await collaborate(`Spawn a child named worker, asking it to reply with exactly ${marker}. End this turn with exactly SPAWNED so the spawn commits. Retain the exact canonical agent path returned by spawn for the next turn.`,
          "Use wait with the exact canonical agent path from the preceding spawn result. Read that child's settled idle answer, then reply with exactly that answer, without additional text.");
        step = "assert child result read by parent";
        const recorded = childHooks = await hooks();
        requireTools(recorded, ["spawn", "wait"], marker);
      } else if (name === "resume") {
        const prompt = `Use the shell tool to append exactly one line ${marker} to once.txt, then reply with exactly ${marker}.`;
        const first = await invoke(prompt, "jsonl");
        const before = await hooks();
        requireTools(before, ["shell"]);
        step = "reopen settled operation without repeating work";
        const second = await invoke(prompt, "jsonl");
        assert.deepEqual(second, first, "reopen changed the committed result");
        assert.equal((await readFile(join(workspace, "once.txt"), "utf8")).trim(), marker);
        assert.equal((await hooks()).filter(hook => hook.hook_event_name === "PostToolUse").length,
          before.filter(hook => hook.hook_event_name === "PostToolUse").length, "replay repeated a tool call");
      } else if (name === "fork-merge") {
        await collaborate(`Spawn a child named worker to use shell to write child.txt in its project checkout containing exactly ${marker}, then reply with exactly ${marker}. End your spawn turn with exactly SPAWNED so it commits. Retain the exact canonical agent path returned by spawn for the next turn. The spawn result is a canonical agent path, not a branch alias.`,
          "Use wait with the exact canonical agent path from the preceding spawn result and read that child's settled idle answer. Then use fs branch to find the actual published agents/ branch, merge that exact branch into your project, and read child.txt. Reply with exactly those file contents, without additional text.");
        step = "assert child fork and merged parent bytes";
        assert.equal((await readFile(join(workspace, "child.txt"), "utf8")).trim(), marker);
        const recorded = childHooks = await hooks();
        requireTools(recorded, ["spawn", "wait", "shell", "fs"], marker);
        assert.ok(recorded.some(hook => hook.hook_event_name === "PostToolUse" && hook.tool_name === "fs"
          && /\bmerge\s+['\"]?agents\//.test(hook.tool_input?.command ?? "")), "missing completed child merge");
      } else assert.fail(`unknown case ${name}`);
    } });
    assert.equal(receipt.expired, false, "case expired");
    assert.ok(receipt.seconds < 10, "case exceeded ten seconds");
    return { case: name, platform: process.platform, status: "pass", seconds: receipt.seconds,
      setup_seconds: (begin - setupBegin) / 1000, phases, captures, hooks: childHooks };
  } catch (error) {
    return { case: name, platform: process.platform, status: "fail", seconds: (performance.now() - begin) / 1000,
      setup_seconds: (begin - setupBegin) / 1000, phases,
      error: `step=${step}: ${error.message}; recovery=${recovery}`, captures, hooks: childHooks };
  }
}

async function main() {
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
  for (let sample = 1; sample <= samples; sample++) {
    for (const name of names) results.push({ sample, ...await runCase(resolve(binary), login, join(root, String(sample)), name) });
  }
  const cells = names.map(name => {
    const attempts = results.filter(result => result.case === name);
    const passed = attempts.filter(result => result.status === "pass").map(result => result.seconds).sort((a, b) => a - b);
    const percentile = fraction => passed.length ? passed[Math.ceil(fraction * passed.length) - 1] : null;
    return { case: name, attempts: attempts.length, n: passed.length, failures: attempts.length - passed.length,
      p50_seconds: percentile(0.5), p99_seconds: percentile(0.99) };
  });
  const report = { tier: "real-codex-login", model: "openai/gpt-6-luna", platform: process.platform,
    measurement: "complete case including original physical retirement; nearest-rank successful-case percentiles, not overhead",
    cells, results,
    pass: results.every(result => result.status === "pass") };
  await publishReport(join(output, "matrix.json"), report);
  process.exitCode = report.pass ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(); }
  catch (error) { console.error(`step=live admission or publication: ${error.message}; recovery=${recovery}`); process.exitCode = 1; }
}
