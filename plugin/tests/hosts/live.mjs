#!/usr/bin/env node
// Live host acceptance: the real host binary, a real model login, and the
// Acyclic plugin packaged and installed the way a user installs it, into an
// isolated fixture home. Credentials are never read: the host's own login
// file is linked into the fixture home and the link is removed again.
//
//   node live.mjs --host codex|claude-code --binary <acyclic executable> [--keep]
//
// Prints one JSON verdict line; evidence stays in the fixture directory.
import { spawn, spawnSync } from "node:child_process";
import {
  cpSync, existsSync, linkSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { hostLogin, linkLogin } from "./login-link.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const pluginRoot = resolve(here, "..", "..");
const windows = process.platform === "win32";
const LIVE_BUDGET_MS = 300_000;

function argument(name, fallback) {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : process.argv[index + 1];
}
const host = argument("--host");
const prebuilt = argument("--package");
const binary = prebuilt ? null : resolve(argument("--binary"));
const keep = process.argv.includes("--keep");
if (!["codex", "claude-code"].includes(host)) throw new Error("--host codex|claude-code");

function target() {
  const arch = { x64: "x64", arm64: "arm64" }[process.arch];
  if (windows) return `win32-${arch}`;
  if (process.platform === "darwin") return `darwin-${arch}`;
  return `linux-${arch}-gnu`;
}

const root = mkdtempSync(join(tmpdir(), `acyclic-live-${host}-`));
const home = join(root, "home");
const workspace = join(root, "workspace");
// A complete profile: Windows resolves known folders (Local AppData) from
// USERPROFILE and refuses ones that do not exist.
for (const directory of ["config", "state", "data", "codex", "claude", join("AppData", "Local"), join("AppData", "Roaming")]) {
  mkdirSync(join(home, directory), { recursive: true });
}
mkdirSync(workspace);
writeFileSync(join(workspace, "README.md"), "seed\n");

const env = {
  ...process.env,
  HOME: home,
  USERPROFILE: home,
  XDG_CONFIG_HOME: join(home, "config"),
  XDG_STATE_HOME: join(home, "state"),
  XDG_DATA_HOME: join(home, "data"),
  LOCALAPPDATA: join(home, "AppData", "Local"),
  APPDATA: join(home, "AppData", "Roaming"),
  CODEX_HOME: join(home, "codex"),
  CLAUDE_CONFIG_DIR: join(home, "claude"),
};
delete env.CODEX_PERMISSION_PROFILE;
delete env.ANTHROPIC_API_KEY;
delete env.ANTHROPIC_BASE_URL;

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { env, encoding: "utf8", ...options });
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed: ${result.status}\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

const timings = {};
let started = Date.now();
if (prebuilt) cpSync(resolve(prebuilt), join(root, "package", "plugin"), { recursive: true });
else run("node", [join(pluginRoot, "scripts", "package.mjs"), "--binary", `${target()}=${binary}`, "--out", join(root, "package")]);
const packageRoot = join(root, "package", "plugin");
run("node", [join(packageRoot, "bin", "install.js")]);
const native = join(packageRoot, "bin", windows ? "acyclic.exe" : "acyclic");
const commands = join(root, "command-bin");
mkdirSync(commands);
linkSync(native, join(commands, windows ? "acyclic-plugin.exe" : "acyclic-plugin"));
env.PATH = [commands, process.env.PATH].join(delimiter);
timings.package = Date.now() - started;

// The host's login file, linked rather than copied, and only for the run.
// The login the caller's host uses, wherever its configuration lives.
const login = hostLogin(host, home);

started = Date.now();
const installOutput = run(native, ["install", host]);
timings.install = Date.now() - started;
writeFileSync(join(root, "install.log"), installOutput);

const PROMPT = [
  "This is an automated acceptance test of workspace isolation. Follow these steps exactly and do nothing else.",
  "1. With your file-editing tool, create the file root.txt containing exactly the word root.",
  "2. Run the shell command: echo bash > bash.txt",
  "3. Start one subagent whose whole task is: create the file child.txt containing exactly the word child, then stop. Wait for it to finish.",
  "4. Run the shell command: acyclic-plugin agents --json . Then merge that finished subagent with: acyclic-plugin git merge agents/<ref> (use the ref listed for it).",
  "5. Start another subagent whose whole task is: create the file discard.txt containing exactly the word discard, then stop. Wait for it to finish.",
  "6. Discard that second subagent with: acyclic-plugin discard agents/<ref> (use its listed ref from acyclic-plugin agents --json).",
  "7. Run: acyclic-plugin git log and finish by replying DONE.",
].join("\n");

// The host's native executable: spawned directly, never through a shell, so
// the prompt reaches it as one argument.
function hostBinary(name) {
  const explicit = argument("--host-binary");
  if (explicit) return explicit;
  for (const directory of (process.env.PATH ?? "").split(delimiter)) {
    if (!windows) {
      if (existsSync(join(directory, name))) return join(directory, name);
      continue;
    }
    if (existsSync(join(directory, `${name}.exe`))) return join(directory, `${name}.exe`);
    const vendor = join(directory, "node_modules", "@openai", "codex", "node_modules", "@openai",
      `codex-win32-${process.arch}`, "vendor", `${process.arch === "arm64" ? "aarch64" : "x86_64"}-pc-windows-msvc`, "bin", "codex.exe");
    if (name === "codex" && existsSync(vendor)) return vendor;
  }
  throw new Error(`${name} is not installed`);
}

function hostCommand() {
  if (host === "codex") {
    return [hostBinary("codex"), [
      "exec", "--json", "--skip-git-repo-check",
      "--dangerously-bypass-approvals-and-sandbox", "--dangerously-bypass-hook-trust",
      "--enable", "hooks", PROMPT,
    ]];
  }
  return [hostBinary("claude"), [
    "-p", PROMPT, "--output-format", "stream-json", "--verbose", "--include-hook-events",
    "--permission-mode", "bypassPermissions", "--debug", "hooks", "--debug-file", join(root, "claude-debug.log"),
  ]];
}

linkLogin(login);
let verdict;
try {
  started = Date.now();
  const [command, args] = hostCommand();
  const child = spawn(command, args, {
    cwd: workspace, env, stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; });
  child.stderr.on("data", chunk => { stderr += chunk; });
  const budget = setTimeout(() => child.kill(), LIVE_BUDGET_MS);
  const status = await new Promise((done, failed) => {
    child.once("error", failed);
    child.once("close", done);
  }).finally(() => clearTimeout(budget));
  timings.host = Date.now() - started;
  writeFileSync(join(root, "host.stdout"), stdout);
  writeFileSync(join(root, "host.stderr"), stderr);
  const read = name => (existsSync(join(workspace, name)) ? readFileSync(join(workspace, name), "utf8").trim() : null);
  const log = spawnSync(native, ["git", "log"], { cwd: workspace, env, encoding: "utf8" });
  const agents = spawnSync(native, ["agents", "--json"], { cwd: workspace, env, encoding: "utf8" });
  const doctor = spawnSync(native, ["doctor"], { cwd: workspace, env, encoding: "utf8" });
  writeFileSync(join(root, "after.txt"), [log.stdout, log.stderr, agents.stdout, agents.stderr, doctor.stdout, doctor.stderr].join("\n---\n"));
  // Outcomes are proved by the plugin's own reports, not by file state alone:
  // a child that never started also leaves no discard.txt behind.
  // The plugin's JSON reply, as the host relays it (possibly JSON-escaped).
  const reported = status => new RegExp(String.raw`status\\?":\s*\\?"` + status).test(stdout);
  const hookResponses = stdout.split("\n").flatMap(line => {
    try {
      const event = JSON.parse(line);
      return event.type === "system" && event.subtype === "hook_response" && event.outcome === "success" ? [event.hook_event] : [];
    } catch { return []; }
  });
  const doctorCheck = name => new RegExp(String.raw`^pass\s+` + name + String.raw`\s`, "m").test(doctor.stdout);
  const checks = {
    exited: status === 0,
    rootEdit: read("root.txt") === "root",
    bash: read("bash.txt") === "bash",
    childMerged: read("child.txt") === "child" && reported("applied"),
    discarded: read("discard.txt") === null && reported("discarded"),
    // Claude Code reports each hook it ran; Codex reports none, and a merged
    // child proves its spawn hooks routed it.
    hooksRan: host === "claude-code"
      ? ["SessionStart", "PreToolUse", "SubagentStart", "SubagentStop"].every(event => hookResponses.includes(event))
      : reported("applied"),
    installed: doctorCheck(host === "codex" ? "codex-install" : "claude-code-install") && doctorCheck("service"),
  };
  verdict = { host, platform: `${process.platform}-${process.arch}`, pass: Object.values(checks).every(Boolean), checks, timings, evidence: root };
} finally {
  rmSync(login.link, { force: true });
  spawnSync(native, ["uninstall", host], { env, encoding: "utf8" });
}
console.log(JSON.stringify(verdict));
if (verdict.pass && !keep) rmSync(root, { recursive: true, force: true });
process.exit(verdict.pass ? 0 : 1);
