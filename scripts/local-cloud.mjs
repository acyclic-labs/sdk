#!/usr/bin/env node
// Opt-in local cloud fixture: runs a test command against the real cloud
// service binaries on one Linux host, with no quorum outside localhost.
//
//   node scripts/local-cloud.mjs \
//     --cloud <cloud checkout> --binaries <CLOUD_E2E_BINARIES> --out <new dir> -- <test argv...>
//
// Build the binaries first with `node ops/local/e2e.mjs --build-only` in the
// cloud checkout. The checkout is only read: the gate stages helpers, engines
// and dependencies beside itself, so the fixture runs it from a snapshot of the
// committed tree in <out>/cloud, with the checkout's installed node_modules
// linked in, and gives it fresh operator state in <out>/state and <out>/cache.
// The cloud's own local gate (`ops/local/e2e.mjs
// --identity-only`, as root, in a delegated systemd scope) prepares custody, starts the Objects and Streams
// Hosts and their Edges in a private network namespace, and waits on their
// readiness, the account keyring and identity. Its last step runs
// `cloud-e2e identity-check`; a wrapper in front of that one command runs the
// test command first, inside the fabric, with ACYCLIC_LOCAL_CLOUD naming
// <out>/endpoints.json (Edge endpoints, the fabric CA and the bearers the
// gate minted locally). The gate then finishes its own check and tears the
// fabric down. The run passes only when the gate passes and the test command
// ran and exited 0. A prebuilt harness e2e binary run away from its build tree
// finds its scenario data through CARGO_MANIFEST_DIR, as under cargo.
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const HOOKED = "identity-check";
const PRODUCTS = ["streams", "objects"];

/** The SDK-owned endpoints file projected from the gate's topology. */
export function endpoints(topology) {
  const ca_file = topology.ca_file;
  if (typeof ca_file !== "string" || !path.isAbsolute(ca_file)) throw new Error("topology has no absolute ca_file");
  const out = { version: 1 };
  for (const product of PRODUCTS) {
    const address = topology.edge?.[product], bearer = topology.bearers?.[product];
    if (typeof address !== "string" || !address) throw new Error(`topology has no ${product} Edge`);
    if (typeof bearer !== "string" || !bearer) throw new Error(`topology has no ${product} bearer`);
    out[product] = { endpoint: `https://${address}`, bearer, ca_file };
  }
  return out;
}

export function parseArgs(argv) {
  const at = argv.indexOf("--");
  const options = {}, rest = at < 0 ? argv : argv.slice(0, at);
  options.test = at < 0 ? [] : argv.slice(at + 1);
  for (let i = 0; i < rest.length; i += 2) {
    const [name, value] = [rest[i], rest[i + 1]];
    if (!["--cloud", "--binaries", "--out"].includes(name) || !value) throw new Error(`usage: --cloud DIR --binaries DIR --out DIR -- TEST...`);
    if (!path.isAbsolute(value)) throw new Error(`${name} must be absolute`);
    options[name.slice(2)] = value;
  }
  for (const name of ["cloud", "binaries", "out"]) if (!options[name]) throw new Error(`--${name} is required`);
  if (!options.test.length) throw new Error("a test command follows --");
  return options;
}

/** The real path of `file`, resolving its deepest existing ancestor. */
export function canonical(file) {
  const rest = [];
  for (let current = path.resolve(file); ; current = path.dirname(current)) {
    try { return path.join(fs.realpathSync.native(current), ...rest.reverse()); }
    catch (error) {
      if (error.code !== "ENOENT" || path.dirname(current) === current) throw error;
      rest.push(path.basename(current));
    }
  }
}

/** Whether `file` is `root` or below it, compared by real path components. */
export function within(file, root) {
  const relative = path.relative(canonical(root), canonical(file));
  return relative === "" || (relative.split(path.sep)[0] !== ".." && !path.isAbsolute(relative));
}

/** Operator state must be fixture-owned: never at or below the shared `.cloudctl`. */
export function requireStateDir(state, shared) {
  if (!state || !path.isAbsolute(state)) throw new Error("operator state must be an absolute fixture-owned directory");
  if (within(state, shared)) throw new Error(`operator state ${state} is the shared operator state ${shared}`);
  return state;
}

/** Paths embedded in the C wrapper; anything else would need escaping. */
export function wrapperPath(file) {
  if (!path.isAbsolute(file) || !/^[\w./+-]+$/.test(file)) throw new Error(`unsupported wrapper path ${file}`);
  return file;
}

export function wrapperSource() {
  return `#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
int main(int argc, char **argv) {
  if (argc >= 3 && strcmp(argv[1], "${HOOKED}") == 0) {
    pid_t child = fork();
    if (child < 0) return 70;
    if (child == 0) { execl(NODE, NODE, SCRIPT, "hook", argv[2], (char *)0); _exit(127); }
    int status;
    if (waitpid(child, &status, 0) < 0) return 70;
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) return 71;
  }
  argv[0] = REAL;
  execv(REAL, argv);
  return 127;
}
`;
}

/** Pass requires the gate's success and the test command's own exit 0. */
export function verdict(gate, result) {
  if (!result) return { passed: false, reason: "the test command never ran inside the local cloud" };
  if (result.code !== 0) return { passed: false, reason: `the test command exited ${result.code ?? result.signal}` };
  if (gate !== 0) return { passed: false, reason: `the local cloud gate failed (${gate})` };
  return { passed: true };
}

/**
 * A child's stdio with stdout and stderr on one descriptor of `file`. Two
 * separate opens keep two file offsets, so each stream overwrites the other.
 */
export function logStdio(file) {
  const fd = fs.openSync(file, "w");
  return ["ignore", fd, fd];
}

function readJson(file) {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); }
  catch (error) { if (error.code === "ENOENT") return undefined; throw error; }
}

/** Runs inside the fabric, in place of the gate's last command. */
async function hook(run) {
  const out = process.env.ACYCLIC_LOCAL_CLOUD_OUT, test = JSON.parse(process.env.ACYCLIC_LOCAL_CLOUD_TEST);
  const file = path.join(out, "endpoints.json");
  fs.writeFileSync(file, JSON.stringify(endpoints(readJson(path.join(run, "topology.json"))), null, 2), { mode: 0o600 });
  const started = performance.now();
  const child = spawn(test[0], test.slice(1), { stdio: logStdio(path.join(out, "test.log")), env: { ...process.env, ACYCLIC_LOCAL_CLOUD: file } });
  const [code, signal] = await new Promise((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (status, killed) => resolve([status, killed]));
  });
  fs.writeFileSync(path.join(out, "result.json"), JSON.stringify({ code, signal, wall_ms: Math.round(performance.now() - started) }));
  return code === 0 ? 0 : 1;
}

function git(cloud, args, options = {}) {
  const result = spawnSync("git", ["--no-optional-locks", "-c", `safe.directory=${cloud}`, "-C", cloud, ...args], { encoding: "utf8", ...options });
  if (result.status !== 0) throw new Error(`git ${args.join(" ")}: ${String(result.stderr).trim()}`);
  return typeof result.stdout === "string" ? result.stdout.trim() : result.stdout;
}

/** Untracked installed dependency directories, which a committed snapshot lacks. */
export function dependencyDirectories(root, relative = "") {
  const found = [];
  for (const entry of fs.readdirSync(path.join(root, relative), { withFileTypes: true })) {
    if (!entry.isDirectory() || entry.name === ".git") continue;
    const child = path.join(relative, entry.name);
    if (entry.name === "node_modules") found.push(child);
    else found.push(...dependencyDirectories(root, child));
  }
  return found;
}

/** A private copy of the committed tree the binaries were built from. */
function snapshot(cloud, destination) {
  if (git(cloud, ["status", "--porcelain", "--untracked-files=no"])) throw new Error("the cloud checkout has uncommitted changes, so a snapshot would differ from its build");
  fs.mkdirSync(destination);
  const archive = git(cloud, ["archive", "--format=tar", "HEAD"], { encoding: "buffer", maxBuffer: 1 << 30 });
  const tar = spawnSync("tar", ["-x", "-C", destination], { input: archive });
  if (tar.status !== 0) throw new Error(`snapshot extraction failed: ${tar.stderr}`);
  for (const directory of dependencyDirectories(cloud)) {
    if (fs.existsSync(path.join(destination, path.dirname(directory)))) fs.symlinkSync(path.join(cloud, directory), path.join(destination, directory));
  }
}

async function main(options) {
  if (process.platform !== "linux" || process.getuid() !== 0) throw new Error("the local cloud gate runs as Linux root");
  const shared = path.join(path.dirname(path.resolve(options.cloud, git(options.cloud, ["rev-parse", "--git-common-dir"]))), ".cloudctl");
  const [head, tree] = git(options.cloud, ["rev-parse", "HEAD", "HEAD^{tree}"]).split("\n");
  if (within(options.out, options.cloud)) throw new Error("--out must be outside the cloud checkout");
  fs.mkdirSync(options.out, { recursive: false });
  const state = requireStateDir(path.join(options.out, "state"), shared);
  const cache = requireStateDir(path.join(options.out, "cache"), shared);
  fs.mkdirSync(state, { mode: 0o700 });
  fs.mkdirSync(cache, { mode: 0o700 });
  const cloud = path.join(options.out, "cloud");
  snapshot(options.cloud, cloud);
  const { QUALIFICATION_BINARIES } = await import(pathToFileURL(path.join(cloud, "ops/local/qualification-binaries.mjs")).href);
  const bin = path.join(options.out, "bin");
  fs.mkdirSync(bin);
  for (const name of QUALIFICATION_BINARIES) {
    fs.accessSync(path.join(options.binaries, name), fs.constants.X_OK);
    if (name !== "cloud-e2e") fs.symlinkSync(path.join(options.binaries, name), path.join(bin, name));
  }
  const source = path.join(options.out, "wrapper.c");
  fs.writeFileSync(source, wrapperSource());
  const define = (name, value) => `-D${name}="${wrapperPath(value)}"`;
  const cc = spawnSync("cc", ["-O2", "-o", path.join(bin, "cloud-e2e"), source,
    define("REAL", path.join(options.binaries, "cloud-e2e")), define("NODE", process.execPath),
    define("SCRIPT", fileURLToPath(import.meta.url))], { encoding: "utf8" });
  if (cc.status !== 0) throw new Error(`wrapper compile failed: ${cc.stderr}`);
  const started = performance.now();
  const gateLog = path.join(options.out, "gate.log");
  // The gate builds its heap cgroups inside its own delegated, memory-bounded scope.
  const gate = spawn("systemd-run", ["--scope", "-p", "Delegate=yes", "-p", "MemoryMax=16G", "--",
    process.execPath, path.join(cloud, "ops/local/e2e.mjs"), "--identity-only"], {
    cwd: cloud, stdio: logStdio(gateLog),
    env: { ...process.env, CLOUDCTL_STATE_DIR: state, CLOUD_CACHE_DIR: cache, CLOUD_E2E_BINARIES: bin, ACYCLIC_LOCAL_CLOUD_OUT: options.out, ACYCLIC_LOCAL_CLOUD_TEST: JSON.stringify(options.test) },
  });
  const stop = () => gate.kill("SIGTERM");
  process.on("SIGTERM", stop); process.on("SIGINT", stop);
  const status = await new Promise((resolve, reject) => {
    gate.once("error", reject);
    gate.once("close", (code, signal) => resolve(code ?? signal));
  });
  const phases = {};
  for (const line of fs.readFileSync(gateLog, "utf8").split("\n")) {
    if (!line.startsWith("{")) continue;
    try { const entry = JSON.parse(line); if (entry.operation === "local.e2e.phase") phases[entry.phase] = entry.wall_ms; } catch { /* not a phase line */ }
  }
  const result = readJson(path.join(options.out, "result.json"));
  const outcome = { ...verdict(status, result), cloud: { head, tree }, gate: status, test: result ?? null, phases, total_ms: Math.round(performance.now() - started) };
  console.log(JSON.stringify(outcome));
  return outcome.passed ? 0 : 1;
}

if (process.argv[1] && fs.realpathSync(process.argv[1]) === fs.realpathSync(fileURLToPath(import.meta.url))) {
  const [command, ...rest] = process.argv.slice(2);
  process.exitCode = await (command === "hook" ? hook(rest[0]) : main(parseArgs(process.argv.slice(2))));
}
