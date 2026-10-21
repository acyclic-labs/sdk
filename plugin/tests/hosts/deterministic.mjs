#!/usr/bin/env node
// Deterministic host acceptance: the real host binary and the real plugin,
// installed the way a user installs it into isolated fixture homes, driven by
// the shared scripted model server (harness-mock-model). Every case runs in
// parallel against one server, each in its own URL namespace; nothing sleeps or
// polls: cases wait on process exit and the server's request notifications.
//
//   node deterministic.mjs --mock <harness-mock-model> (--binary <acyclic> | --package <dir>)
//                          [--hosts claude-code] [--only <case,...>] [--keep]
//
// Prints one JSON line per case and a summary line with wall times; exits 1 on
// any failure. Evidence for failed cases stays in the printed directory.
import { spawn, spawnSync } from "node:child_process";
import {
  cpSync, existsSync, linkSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

const suiteStarted = Date.now();
const here = dirname(fileURLToPath(import.meta.url));
const pluginRoot = resolve(here, "..", "..");
const windows = process.platform === "win32";
const CASE_BUDGET_MS = 60_000;

function argument(name, fallback) {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : process.argv[index + 1];
}
const mockBinary = resolve(argument("--mock") ?? "");
const prebuilt = argument("--package");
const binary = prebuilt ? null : resolve(argument("--binary") ?? "");
const hosts = (argument("--hosts") ?? "claude-code").split(",");
const only = argument("--only")?.split(",");
const keep = process.argv.includes("--keep");

function target() {
  const arch = { x64: "x64", arm64: "arm64" }[process.arch];
  if (windows) return `win32-${arch}`;
  if (process.platform === "darwin") return `darwin-${arch}`;
  return `linux-${arch}-gnu`;
}

function onPath(name) {
  for (const directory of (process.env.PATH ?? "").split(delimiter)) {
    for (const candidate of windows ? [`${name}.exe`] : [name]) {
      if (existsSync(join(directory, candidate))) return join(directory, candidate);
    }
  }
  throw new Error(`${name} is not installed`);
}

const root = mkdtempSync(join(tmpdir(), "acyclic-hosts-"));

/** A complete per-user profile: Windows resolves known folders from USERPROFILE. */
function profile(home) {
  for (const directory of ["config", "state", "data", "claude", join("AppData", "Local"), join("AppData", "Roaming")]) {
    mkdirSync(join(home, directory), { recursive: true });
  }
  const env = {
    ...process.env,
    HOME: home,
    USERPROFILE: home,
    XDG_CONFIG_HOME: join(home, "config"),
    XDG_STATE_HOME: join(home, "state"),
    XDG_DATA_HOME: join(home, "data"),
    LOCALAPPDATA: join(home, "AppData", "Local"),
    APPDATA: join(home, "AppData", "Roaming"),
    CLAUDE_CONFIG_DIR: join(home, "claude"),
    PATH: [join(root, "command-bin"), process.env.PATH].join(delimiter),
  };
  for (const name of ["ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "CODEX_HOME", "CODEX_PERMISSION_PROFILE"]) {
    delete env[name];
  }
  return env;
}

function run(command, args, options) {
  const result = spawnSync(command, args, { encoding: "utf8", ...options });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed: ${result.status}\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

// Preparation: one package, one command directory, one installed seed home per
// host. Cases copy the seed; nothing is installed or packaged per case.
const prepareStarted = Date.now();
if (prebuilt) cpSync(resolve(prebuilt), join(root, "package", "plugin"), { recursive: true });
else run("node", [join(pluginRoot, "scripts", "package.mjs"), "--binary", `${target()}=${binary}`, "--out", join(root, "package")]);
const packageRoot = join(root, "package", "plugin");
run("node", [join(packageRoot, "bin", "install.js")]);
const native = join(packageRoot, "bin", windows ? "acyclic.exe" : "acyclic");
mkdirSync(join(root, "command-bin"));
linkSync(native, join(root, "command-bin", windows ? "acyclic-plugin.exe" : "acyclic-plugin"));
// Merges or discards every child of the calling agent; scripted turns run it
// because child refs are host-generated identities.
const allChildren = join(root, "all-children.mjs");
writeFileSync(allChildren, `
import { spawnSync } from "node:child_process";
const verb = process.argv[2];
const listed = spawnSync("acyclic-plugin", ["agents", "--json"], { encoding: "utf8" });
if (listed.status !== 0) { console.log("agents failed", listed.stdout, listed.stderr); process.exit(1); }
let status = 0;
for (const agent of JSON.parse(listed.stdout).agents.filter(agent => agent.parent === "root")) {
  const args = verb === "merge" ? ["git", "merge", agent.ref] : ["discard", agent.ref];
  const result = spawnSync("acyclic-plugin", args, { encoding: "utf8" });
  console.log(verb, agent.ref, "status", result.status, result.stdout.trim(), result.stderr.trim());
  status ||= result.status;
}
process.exit(status);
`);
const seeds = {};
for (const host of hosts) {
  const home = join(root, "seed", host);
  run(native, ["install", host], { env: profile(home) });
  seeds[host] = home;
}
const prepareMs = Date.now() - prepareStarted;

const shellPath = path => path.replaceAll("\\", "/");
const ALL = verb => `node "${shellPath(allChildren)}" ${verb}`;
const agent = (id, child) => ({
  id, name: "Agent",
  arguments: { description: `child ${child}`, prompt: `CHILD-${child}`, subagent_type: "general-purpose", run_in_background: false },
});
const write = (id, path, content) => ({ id, name: "Write", arguments: { file_path: path, content } });
const bash = (id, command) => ({ id, name: "Bash", arguments: { command, description: "scripted step" } });
// Scripted commands run in the platform's own shell tool. On Windows, Claude
// Code's Bash tool starts a Git Bash login shell per command, which costs
// seconds before the command runs; the bash case still covers that tool.
const shell = (id, command) => (windows
  ? { id, name: "PowerShell", arguments: { command, description: "scripted step" } }
  : bash(id, command));
const setText = (path, text) => (windows ? `Set-Content -NoNewline -Path ${path} -Value ${text}` : `printf ${text} > ${path}`);
const after = id => `"tool_use_id":"${id}"`;
// Claude Code resolves a child's relative file-tool path against the
// session directory, the parent's checkout: the plugin maps it to the same
// path in the child's fork view. The merge and left children write with the
// file tool, the others with the shell tool.
const childStep = (child, tool) => ({ when: { user: `CHILD-${child}` }, tools: [tool] });
const childDone = (child, id) => ({ when: { user: `CHILD-${child}`, contains: after(id) }, text: [`${child.toUpperCase()}DONE`] });

// Each case: its scripted model turns, its seed files and its assertions.
// Child steps are guarded by the child's own prompt, so parallel children in
// one namespace cannot take each other's turns; parent steps follow the tool
// call they answer.
const claudeCases = {
  edit: {
    steps: [
      { when: { user: "CASE-edit" }, tools: [write("w", "root.txt", "root")] },
      { when: { contains: after("w") }, text: ["done"] },
    ],
    check: ({ read }) => ({ rootEdit: read("root.txt") === "root" }),
  },
  bash: {
    steps: [
      { when: { user: "CASE-bash" }, tools: [bash("b", "echo bash > bash.txt")] },
      { when: { contains: after("b") }, text: ["done"] },
    ],
    check: ({ read }) => ({ bash: read("bash.txt") === "bash" }),
  },
  merge: {
    steps: [
      { when: { user: "CASE-merge" }, tools: [agent("a", "merge")] },
      childDone("merge", "merge-w"),
      childStep("merge", write("merge-w", "child.txt", "child")),
      { when: { user: "CASE-merge", tool_result: "MERGEDONE" }, tools: [write("p", "parent.txt", "parent")] },
      { when: { user: "CASE-merge", contains: after("p") }, tools: [shell("m", ALL("merge"))] },
      { when: { user: "CASE-merge", contains: after("m") }, text: ["done"] },
    ],
    check: ({ read, events }) => ({
      childMerged: read("child.txt") === "child",
      parentKept: read("parent.txt") === "parent",
      hookOrder: hookOrder(events),
    }),
  },
  conflict: {
    seed: { "same.txt": "base\n" },
    steps: [
      { when: { user: "CASE-conflict" }, tools: [agent("a", "conflict")] },
      childDone("conflict", "conflict-w"),
      childStep("conflict", shell("conflict-w", setText("same.txt", "child"))),
      { when: { user: "CASE-conflict", tool_result: "CONFLICTDONE" }, tools: [shell("p", setText("same.txt", "parent"))] },
      { when: { user: "CASE-conflict", contains: after("p") }, tools: [shell("m", ALL("merge"))] },
      { when: { user: "CASE-conflict", contains: after("m") }, text: ["done"] },
    ],
    check: ({ read, requests }) => ({
      parentPreserved: /parent/.test(read("same.txt") ?? ""),
      conflictReported: requests.some(request => request.includes("merge agents/") && /conflictCount[^0-9]+[1-9]/.test(request)),
    }),
  },
  parallel: {
    steps: [
      { when: { user: "CASE-parallel" }, tools: [agent("a1", "left"), agent("a2", "right")] },
      childDone("left", "left-w"),
      childDone("right", "right-w"),
      childStep("left", write("left-w", "left.txt", "left")),
      childStep("right", shell("right-w", setText("right.txt", "right"))),
      { when: { user: "CASE-parallel", contains: "LEFTDONE", tool_result: "RIGHTDONE" }, tools: [shell("m", ALL("merge"))] },
      { when: { user: "CASE-parallel", contains: after("m") }, text: ["done"] },
    ],
    check: ({ read }) => ({ left: read("left.txt") === "left", right: read("right.txt") === "right" }),
  },
  discard: {
    steps: [
      { when: { user: "CASE-discard" }, tools: [agent("a", "discard")] },
      childDone("discard", "discard-w"),
      childStep("discard", shell("discard-w", setText("discard.txt", "discard"))),
      { when: { user: "CASE-discard", tool_result: "DISCARDDONE" }, tools: [shell("d", ALL("discard"))] },
      { when: { user: "CASE-discard", contains: after("d") }, text: ["done"] },
    ],
    check: ({ read, requests }) => ({
      discarded: read("discard.txt") === null,
      discardSucceeded: requests.some(request => /discard agents\/\S+ status 0/.test(request)),
    }),
  },
  resume: {
    turns: 2,
    steps: [
      { when: { user: "CASE-resume-1" }, tools: [write("w1", "first.txt", "first")] },
      { when: { user: "CASE-resume-1", contains: after("w1") }, text: ["done"] },
      { when: { user: "CASE-resume-2", contains: "CASE-resume-1" }, tools: [write("w2", "second.txt", "second")] },
      { when: { user: "CASE-resume-2", contains: after("w2") }, text: ["done"] },
    ],
    check: ({ read }) => ({ first: read("first.txt") === "first", second: read("second.txt") === "second" }),
  },
};

/** Hook events in stream order, from the host's own hook telemetry. */
function hookOrder(events) {
  const order = events.filter(event => event.type === "system" && event.subtype === "hook_response")
    .map(event => `${event.hook_event}:${event.outcome}`);
  const at = name => order.findIndex(entry => entry.startsWith(`${name}:`));
  return order.every(entry => entry.endsWith(":success"))
    && at("SessionStart") === 0
    && at("PreToolUse") > 0
    && at("SubagentStart") > at("PreToolUse")
    && at("SubagentStop") > at("SubagentStart")
    && order.lastIndexOf("PostToolUse:success") > at("SubagentStop");
}

// One mock server for every case, each in its own namespace.
const script = { cases: {} };
const selected = Object.entries(claudeCases).filter(([name]) => !only || only.includes(name));
for (const [name, definition] of selected) script.cases[`claude-code-${name}`] = { steps: definition.steps };
writeFileSync(join(root, "script.json"), JSON.stringify(script));
const mock = spawn(mockBinary, ["--script", join(root, "script.json"), "--record", join(root, "requests.jsonl")], {
  stdio: ["ignore", "pipe", "inherit"],
});
const notifications = createInterface({ input: mock.stdout });
const port = await new Promise((done, failed) => {
  mock.once("exit", code => failed(new Error(`mock model exited before readiness: ${code}`)));
  mock.once("error", failed);
  notifications.once("line", line => done(JSON.parse(line).port));
});
const unmatched = {};
notifications.on("line", line => {
  const notification = JSON.parse(line);
  if (notification.event === "request" && notification.step === null && notification.path === "messages") {
    unmatched[notification.case] = (unmatched[notification.case] ?? 0) + 1;
  }
});

function host(command, args, options) {
  return new Promise(done => {
    const child = spawn(command, args, { ...options, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", chunk => { stdout += chunk; });
    child.stderr.on("data", chunk => { stderr += chunk; });
    const budget = setTimeout(() => child.kill(), CASE_BUDGET_MS);
    // A host that cannot launch is a failed case, not a crashed suite.
    child.once("error", error => { clearTimeout(budget); done({ status: null, stdout, stderr: `${stderr}${error}` }); });
    child.once("close", status => { clearTimeout(budget); done({ status, stdout, stderr }); });
  });
}

const claude = onPath("claude");
async function claudeCase(name, definition) {
  const started = Date.now();
  const caseRoot = join(root, "cases", `claude-code-${name}`);
  const home = join(caseRoot, "home");
  const workspace = join(caseRoot, "workspace");
  cpSync(seeds["claude-code"], home, { recursive: true });
  mkdirSync(workspace, { recursive: true });
  for (const [path, content] of Object.entries({ "README.md": "seed\n", ...definition.seed })) {
    writeFileSync(join(workspace, path), content);
  }
  const env = {
    ...profile(home),
    ANTHROPIC_BASE_URL: `http://127.0.0.1:${port}/claude-code-${name}`,
    ANTHROPIC_API_KEY: "mock-model-key",
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: "1",
    ...(windows ? { CLAUDE_CODE_USE_POWERSHELL_TOOL: "1" } : {}),
    DISABLE_AUTOUPDATER: "1",
  };
  const events = [];
  let sessionId;
  let exited = true;
  for (let turn = 1; turn <= (definition.turns ?? 1); turn += 1) {
    const prompt = definition.turns ? `CASE-${name}-${turn}` : `CASE-${name}`;
    const args = ["-p", prompt, "--output-format", "stream-json", "--verbose", "--include-hook-events",
      "--permission-mode", "bypassPermissions", "--debug", "hooks", "--debug-file", join(caseRoot, `claude-debug-${turn}.log`)];
    if (sessionId) args.push("--resume", sessionId);
    const result = await host(claude, args, { cwd: workspace, env });
    writeFileSync(join(caseRoot, `host-${turn}.stdout`), result.stdout);
    writeFileSync(join(caseRoot, `host-${turn}.stderr`), result.stderr);
    for (const line of result.stdout.split("\n")) {
      try { events.push(JSON.parse(line)); } catch { /* non-JSON diagnostics */ }
    }
    sessionId = events.findLast(event => event.type === "result")?.session_id;
    exited &&= result.status === 0 && events.findLast(event => event.type === "result")?.is_error === false;
  }
  const read = path => (existsSync(join(workspace, path)) ? readFileSync(join(workspace, path), "utf8").trim() : null);
  const requests = readFileSync(join(root, "requests.jsonl"), "utf8").split("\n")
    .filter(line => line.includes(`"case":"claude-code-${name}"`));
  const debug = Array.from({ length: definition.turns ?? 1 }, (_, index) => {
    const path = join(caseRoot, `claude-debug-${index + 1}.log`);
    return existsSync(path) ? readFileSync(path, "utf8") : "";
  }).join("\n");
  const service = spawnSync(native, ["__service-status"], { cwd: workspace, env, encoding: "utf8" });
  const checks = {
    exited,
    scripted: !unmatched[`claude-code-${name}`],
    sessionEnded: (debug.match(/__hook claude-code SessionEnd] completed with status 0/g) ?? []).length
      === (definition.turns ?? 1),
    serviceSurvived: service.status === 0 && JSON.parse(service.stdout || "{}").reachableIdentity != null,
    ...definition.check({ read, requests, events }),
  };
  return { host: "claude-code", case: name, pass: Object.values(checks).every(Boolean), checks, ms: Date.now() - started, evidence: caseRoot };
}

let results;
try {
  results = await Promise.all(selected.map(([name, definition]) => claudeCase(name, definition)));
} finally {
  mock.kill();
}
for (const result of results) console.log(JSON.stringify(result));
const pass = results.every(result => result.pass);
console.log(JSON.stringify({
  platform: `${process.platform}-${process.arch}`, pass, cases: results.length,
  prepareMs, suiteMs: Date.now() - suiteStarted - prepareMs, wallMs: Date.now() - suiteStarted,
  slowestCaseMs: Math.max(...results.map(result => result.ms)), evidence: root,
}));
if (pass && !keep) {
  for (const result of results) spawnSync(native, ["__service-drain"], { env: profile(join(result.evidence, "home")) });
  rmSync(root, { recursive: true, force: true });
}
process.exit(pass ? 0 : 1);
