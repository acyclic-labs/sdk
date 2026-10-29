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
import { waitForHook } from "./hook-events.mjs";

const recovery = "LIVE_CODEX_HOME=/absolute/existing/login node scripts/graphcoder-e2e/live.mjs GRAPHCODER FRESH_OUTPUT [CASE ...]";
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

export function requireTools(hooks, names) {
  const used = new Set(hooks.filter(hook => hook.hook_event_name === "PostToolUse").map(hook => hook.tool_name));
  for (const name of names) assert.ok(used.has(name), `missing completed ${name} tool call`);
}

async function runCase(binary, login, root, name) {
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
  const catalog = join(directory, "catalog.json");
  await writeFile(catalog, JSON.stringify({ openai: {
    adapter: "responses", authentication: "api_key_or_codex_login", base_url: "https://api.openai.com/v1",
    models: { "gpt-6.1-sol": { capacity: { context_tokens: 475000, output_tokens: 128000 }, features: { tools: true, images: true } } },
    max_event_bytes: 1048576,
    token_counter: { path: "responses/input_tokens", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
  } }));
  const operation = randomUUID(), captures = [], processes = new Processes();
  const marker = `LIVE_${randomUUID().replaceAll("-", "")}`;
  let step = "invoke shipping GraphCoder";
  const invoke = async (prompt, format = "text") => {
    const args = ["--workspace", workspace, "--settings-root", settings, "--catalog", catalog,
      "--model", "openai/gpt-6.1-sol", "--operation", operation,
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
      "--settings-root", settings, "--catalog", catalog, "--model", "openai/gpt-6.1-sol"] };
    await conversation(processes, binary, fixture, { operation, turns: [
      { id: "spawn", text: task, answer: "SPAWNED" },
      { id: "report", text: followup, answer: marker, child: true },
    ] }, capture => captures.push(capture), async (turn, active) => {
      if (turn.child) await waitForHook(hookFile, hook => hook.hook_event_name === "UserPromptSubmit"
        && typeof hook.prompt === "string" && hook.prompt.includes('"child_done"')
        && hook.prompt.includes(marker), active.closed);
    });
  };
  const begin = performance.now();
  try {
    const receipt = await withOwnedCase({ processes, run: async () => {
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
        await collaborate(`Spawn a child named worker, asking it to reply with exactly ${marker}. End this turn with exactly SPAWNED so the spawn commits. A child_done event will deliver the child's result on a later turn; read it when delivered.`,
          "Reply with exactly the child's result from the preceding child_done event, without tools or additional text.");
        step = "assert child result read by parent";
        const recorded = await hooks();
        requireTools(recorded, ["spawn"]);
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
        await collaborate(`Spawn a child named worker to use shell to write child.txt in its project checkout containing exactly ${marker}, then reply with exactly ${marker}. End your spawn turn with exactly SPAWNED so it commits. On the later child_done event, use fs branch to find the actual published agents/ branch, merge that exact branch into your project, and read child.txt. The spawn result is a canonical agent path, not a branch alias.`,
          "Confirm the child project has been merged and reply with exactly the contents of child.txt, without additional text.");
        step = "assert child fork and merged parent bytes";
        assert.equal((await readFile(join(workspace, "child.txt"), "utf8")).trim(), marker);
        const recorded = await hooks();
        requireTools(recorded, ["spawn", "shell", "fs"]);
        assert.ok(recorded.some(hook => hook.hook_event_name === "PostToolUse" && hook.tool_name === "fs"
          && /\bmerge\s+['\"]?agents\//.test(hook.tool_input?.command ?? "")), "missing completed child merge");
      } else assert.fail(`unknown case ${name}`);
    } });
    assert.equal(receipt.expired, false, "case expired");
    assert.ok(receipt.seconds < 10, "case exceeded ten seconds");
    return { case: name, platform: process.platform, status: "pass", seconds: receipt.seconds, captures };
  } catch (error) {
    return { case: name, platform: process.platform, status: "fail", seconds: (performance.now() - begin) / 1000,
      error: `step=${step}: ${error.message}; recovery=${recovery}`, captures };
  }
}

async function main() {
  const [binary, output, ...selected] = process.argv.slice(2);
  const login = process.env.LIVE_CODEX_HOME ?? process.env.E2E_LIVE_CODEX_HOME;
  assert.ok(binary && isAbsolute(binary) && output && login && isAbsolute(login), `step=admit arguments and existing login path; recovery=${recovery}`);
  const names = selected.length ? selected : liveCases;
  assert.equal(new Set(names).size, names.length, `step=reject duplicate cases; recovery=${recovery}`);
  for (const name of names) assert.ok(liveCases.includes(name), `step=admit case ${name}; recovery=${recovery}`);
  await mkdir(output); // An existing evidence identity must never be reused.
  const root = scratchDirectory("graphcoder-live-");
  const results = [];
  for (const name of names) results.push(await runCase(resolve(binary), login, root, name));
  const report = { tier: "real-codex-login", platform: process.platform, results,
    pass: results.every(result => result.status === "pass") };
  await publishReport(join(output, "matrix.json"), report);
  process.exitCode = report.pass ? 0 : 1;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await main(); }
  catch (error) { console.error(`step=live admission or publication: ${error.message}; recovery=${recovery}`); process.exitCode = 1; }
}
