#!/usr/bin/env node
// Host sandbox probe: the plugin under Codex's own workspace-write sandbox
// (landlock on Linux, seatbelt on macOS) with no model and no login. The
// plugin is packaged and installed into a fixture home under the user's home
// directory, as a user installs it, and its service is started outside the
// sandbox. Each check then runs one command inside the sandbox:
//
//   workspaceWritable    the workspace is a writable root
//   outsideBlocked       a write beside the workspace is still refused
//   cliInSandbox         the plugin CLI reaches its service (merge, discard,
//                        agents and log all go through this path)
//   subagentRootWritable the root under which the plugin places a subagent's
//                        working directory accepts the subagent's writes
//
// A failing check is a place where the plugin breaks the host sandbox.
//
//   node sandbox-probe.mjs --binary <acyclic executable> [--codex <codex>] [--keep]
//
// Prints one JSON verdict line and exits nonzero unless every check passes.
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const pluginRoot = resolve(here, "..", "..");

function argument(name, fallback) {
  const index = process.argv.indexOf(name);
  return index === -1 ? fallback : process.argv[index + 1];
}
const binary = resolve(argument("--binary"));
const codex = resolve(argument("--codex", join(here, "node_modules", ".bin", "codex")));
const keep = process.argv.includes("--keep");
if (!["linux", "darwin"].includes(process.platform)) {
  throw new Error("the Codex sandbox probe runs on Linux and macOS");
}
const target = `${process.platform}-${process.arch}${process.platform === "linux" ? "-gnu" : ""}`;

const root = mkdtempSync(join(argument("--root-parent", homedir()), ".acyclic-sandbox-probe-"));
const home = join(root, "home");
const workspace = join(root, "workspace");
const outside = join(root, "outside.txt");
for (const directory of ["config", "state", "data", "codex"]) mkdirSync(join(home, directory), { recursive: true });
mkdirSync(workspace);
writeFileSync(join(workspace, "README.md"), "seed\n");
const env = {
  ...process.env,
  HOME: home,
  XDG_CONFIG_HOME: join(home, "config"),
  XDG_STATE_HOME: join(home, "state"),
  XDG_DATA_HOME: join(home, "data"),
  CODEX_HOME: join(home, "codex"),
  // `install codex` finds the host on PATH, as it does for a user.
  PATH: `${dirname(codex)}${delimiter}${process.env.PATH ?? ""}`,
};
delete env.CODEX_PERMISSION_PROFILE;

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { env, cwd: workspace, encoding: "utf8", ...options });
  return { status: result.status, output: `${result.stdout ?? ""}${result.stderr ?? ""}`.trim().slice(0, 400) };
}
function must(command, args) {
  const result = run(command, args);
  if (result.status !== 0) throw new Error(`${command} ${args.join(" ")} failed: ${result.output}`);
}
function sandboxed(command, args) {
  return run(codex, ["sandbox", "-c", 'sandbox_mode="workspace-write"', "--", command, ...args]);
}

let verdict;
try {
  must("node", [join(pluginRoot, "scripts", "package.mjs"), "--binary", `${target}=${binary}`, "--out", join(root, "package")]);
  const packageRoot = join(root, "package", "plugin");
  must("node", [join(packageRoot, "bin", "install.js")]);
  const native = join(packageRoot, "bin", "acyclic");
  must(native, ["install", "codex"]);
  // Outside the sandbox: starts the service the agent's commands reach.
  must(native, ["agents", "--json"]);

  // The directory under which subagent working directories are placed.
  const findMountRoot = directory => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const path = join(directory, entry.name);
      if (/^state-v\d+$/.test(entry.name)) return join(path, "w");
      const found = findMountRoot(path);
      if (found) return found;
    }
    return null;
  };
  const mountRoot = findMountRoot(home);
  if (mountRoot) mkdirSync(mountRoot, { recursive: true });

  const probes = {
    workspaceWritable: sandboxed("sh", ["-c", `echo in > ${join(workspace, "in.txt")}`]),
    outsideBlocked: sandboxed("sh", ["-c", `echo outside > ${outside}`]),
    cliInSandbox: sandboxed(native, ["agents", "--json"]),
    subagentRootWritable: mountRoot
      ? sandboxed("sh", ["-c", `echo child > ${join(mountRoot, "probe.txt")}`])
      : { status: null, output: "no plugin state directory found" },
  };
  const checks = {
    workspaceWritable: probes.workspaceWritable.status === 0 && existsSync(join(workspace, "in.txt")),
    outsideBlocked: probes.outsideBlocked.status !== 0 && !existsSync(outside),
    cliInSandbox: probes.cliInSandbox.status === 0,
    subagentRootWritable: probes.subagentRootWritable.status === 0,
  };
  verdict = {
    platform: `${process.platform}-${process.arch}`,
    codex: run(codex, ["--version"]).output,
    pass: Object.values(checks).every(Boolean),
    checks,
    failures: Object.fromEntries(Object.entries(probes).filter(([name]) => !checks[name])),
    workspace,
    mountRoot,
    evidence: root,
  };
} finally {
  const native = join(root, "package", "plugin", "bin", "acyclic");
  if (existsSync(native)) run(native, ["uninstall", "codex"]);
}
console.log(JSON.stringify(verdict));
if (!keep) rmSync(root, { recursive: true, force: true });
process.exit(verdict.pass ? 0 : 1);
