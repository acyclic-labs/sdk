#!/usr/bin/env node
// Installed, source-bound native PTY qualification. No fixture/playback or mock host.
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { StringDecoder } from "node:string_decoder";

const suitePath = fileURLToPath(import.meta.url);
const sourceRoot = resolve(dirname(suitePath), "..");
const [binary, ...options] = process.argv.slice(2);
const flags = new Map();
while (options.length) {
  const key = options.shift(), value = options.shift();
  assert.ok(["--fresh", "--manifest", "--evidence"].includes(key) && value && !flags.has(key), "invalid PTY lane arguments");
  flags.set(key, value);
}
assert.ok(binary && isAbsolute(binary) && statSync(binary).isFile(), "absolute installed binary required");
assert.ok(flags.has("--fresh") !== flags.has("--manifest"), "supply either --fresh ABS_FRESH_SCENARIO_DIR or --manifest ABS_GENUINE_MANIFEST");
const evidence = flags.get("--evidence");
assert.ok(evidence && isAbsolute(evidence) && !existsSync(evidence), "--evidence requires an absolute fresh evidence directory");
for (const value of flags.values()) assert.ok(isAbsolute(value), "all paths must be absolute");
assert.ok(["linux", "darwin", "win32"].includes(process.platform), `unsupported real PTY platform: ${process.platform}`);
mkdirSync(evidence, { mode: 0o700 });
const hash = path => createHash("sha256").update(readFileSync(path)).digest("hex");
const json = path => {
  assert.ok(isAbsolute(path) && statSync(path).isFile() && statSync(path).size <= 1_048_576, `bounded actual input required: ${path}`);
  return JSON.parse(readFileSync(path, "utf8"));
};
const report = { format_version: 1, qualified: false, platform: process.platform, binary,
  binary_sha256: hash(binary), suite_sha256: hash(suitePath), evidence: "actual OS slave PTY / actual Win32 ConPTY, installed --session, no --headless", launches: [], files: [], observations: {} };
const env = { PATH: "", TERM: "xterm-256color", LANG: "C.UTF-8" };
for (const key of ["SystemRoot", "WINDIR", "TEMP", "TMP"]) if (process.env[key] !== undefined) env[key] = process.env[key];
function bind(path) {
  const record = { path, sha256: hash(path), bytes: statSync(path).size };
  report.files.push(record); return path;
}
// Retain the exact checkout input inventory, independently of helper manifest claims.
const sources = [];
function inventory(directory) {
  for (const item of readdirSync(directory, { withFileTypes: true })) {
    if (["target", ".git", "node_modules"].includes(item.name)) continue;
    const path = join(directory, item.name);
    if (item.isDirectory()) inventory(path);
    else if (item.isFile() && (/\.(?:rs|toml|proto)$/.test(item.name) || item.name === "Cargo.lock"))
      sources.push({ path: relative(sourceRoot, path).replaceAll("\\", "/"), sha256: hash(path) });
  }
}
inventory(join(sourceRoot, "rust"));
inventory(join(sourceRoot, "proto"));
inventory(join(sourceRoot, ".cargo"));
for (const name of ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]) {
  sources.push({ path: name, sha256: hash(join(sourceRoot, name)) });
}
sources.sort((a, b) => a.path.localeCompare(b.path));
const sourceInventory = join(evidence, "source-hashes.json");
writeFileSync(sourceInventory, JSON.stringify(sources, null, 2) + "\n", { mode: 0o600 });
report.source_inventory = bind(sourceInventory);

let active, configPath, manifestPath;
let suiteFailure;
const overallDeadline = setTimeout(() => {
  suiteFailure = new Error("finite 300-second real PTY lane deadline exceeded");
  if (active) active.abort(suiteFailure);
}, 300_000);
const quote = text => `'${text.replaceAll("'", "'\\''")}'`;
let launcher, launchArgs;
function prepareLauncher() {
if (process.platform === "win32") {
  const systemRoot = process.env.SystemRoot;
  assert.ok(systemRoot && isAbsolute(systemRoot), "Windows SystemRoot prerequisite missing");
  launcher = join(systemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
  const helper = bind(join(dirname(suitePath), "graphcoder-conpty.ps1"));
  launchArgs = config => ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", helper,
    "-Binary", binary, "-Config", config, "-DeadlineMilliseconds", "90000"];
} else {
  launcher = "/usr/bin/script";
  assert.ok(existsSync("/bin/sh"), "absolute /bin/sh prerequisite missing");
  const stty = ["/bin/stty", "/usr/bin/stty"].find(path => existsSync(path));
  assert.ok(stty, "absolute native stty prerequisite missing");
  const command = config => `${quote(stty)} cols 32767 rows 100 || exit 125; exec ${quote(binary)} --session ${quote(config)}`;
  launchArgs = process.platform === "linux"
    ? config => ["-q", "-e", "-f", "-c", command(config), "/dev/null"]
    : config => ["-q", "/dev/null", "/bin/sh", "-c", command(config)];
  bind("/bin/sh"); bind(stty);
}
assert.ok(existsSync(launcher) && statSync(launcher).isFile(), `real OS PTY prerequisite missing: ${launcher}`);
bind(launcher);
}

// Remove host VT decoration only for matching. Exact raw bytes remain in evidence.
const visible = text => text.replace(/\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/g, "")
  .replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "").replace(/\r\n/g, "\n").replaceAll("\r", "");
class PtySession {
  constructor(name, config) {
    this.name = name; this.text = ""; this.hostText = ""; this.bytes = 0; this.waiters = new Set(); this.failure = null; this.finished = null;
    this.pid = null; this.intentionalKill = false;
    this.raw = join(evidence, `${name}.pty.raw`); this.host = join(evidence, `${name}.host.raw`);
    writeFileSync(this.raw, "", { mode: 0o600 }); writeFileSync(this.host, "", { mode: 0o600 });
    this.record = { name, executable: launcher, argv: launchArgs(config), runtime_argv: [binary, "--session", config], stdin: [], raw: this.raw, host: this.host };
    report.launches.push(this.record);
    this.child = spawn(launcher, this.record.argv, { env, cwd: dirname(config), windowsHide: true,
      detached: process.platform !== "win32", stdio: ["pipe", "pipe", "pipe"] });
    const decoder = new StringDecoder("utf8"), hostDecoder = new StringDecoder("utf8");
    this.child.stdout.on("data", bytes => this.capture(bytes, false, decoder));
    this.child.stderr.on("data", bytes => this.capture(bytes, true, hostDecoder));
    this.child.stdin.on("error", error => { if (!this.intentionalKill && !this.finished) this.abort(error); });
    this.child.on("error", error => this.abort(error));
    this.exited = new Promise(resolveExit => this.child.on("close", (code, signal) => {
      clearTimeout(this.deadline); this.text += decoder.end(); this.hostText += hostDecoder.end();
      this.finished = { code, signal }; this.record.exit = this.finished;
      this.record.raw_sha256 = hash(this.raw); this.record.host_sha256 = hash(this.host);
      this.notify(); resolveExit(this.finished);
    }));
    this.deadline = setTimeout(() => this.abort(new Error(`${name}: finite 90-second PTY process deadline exceeded`)), 90_000);
  }
  capture(bytes, host, decoder) {
    this.bytes += bytes.length;
    if (this.bytes > 1_048_576) { this.abort(new Error(`${this.name}: PTY output exceeds 1 MiB`)); return; }
    appendFileSync(host ? this.host : this.raw, bytes);
    const text = decoder.write(bytes); if (host) this.hostText += text; else this.text += text;
    // The PID comes only from the real runtime; echoed input is never a marker.
    const marker = /terminal-mode stdin-is-terminal=true interactive=true headless=false process-id=([0-9]+)/.exec(visible(this.text));
    if (marker) this.pid = Number(marker[1]);
    this.notify();
  }
  notify() { for (const check of [...this.waiters]) check(); }
  terminate() {
    if (this.finished) return;
    if (this.pid) { try { process.kill(this.pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") this.record.kill_error = String(error); } }
    try {
      if (process.platform === "win32") this.child.kill("SIGKILL");
      else if (this.child.pid) process.kill(-this.child.pid, "SIGKILL");
    } catch (error) { if (error.code !== "ESRCH") this.record.launcher_kill_error = String(error); }
  }
  abort(error) { this.failure ??= error; this.terminate(); this.notify(); }
  expect(pattern, from = 0, host = false) {
    return new Promise((resolveMatch, reject) => {
      let timer;
      const finish = (error, match) => { clearTimeout(timer); this.waiters.delete(check); error ? reject(error) : resolveMatch(match); };
      const check = () => {
        if (this.failure || suiteFailure) return finish(this.failure || suiteFailure);
        const match = pattern.exec(visible((host ? this.hostText : this.text).slice(from)));
        if (match) return finish(null, match);
        if (this.finished) finish(new Error(`${this.name}: actual PTY exited before ${pattern}`));
      };
      timer = setTimeout(() => { this.abort(new Error(`${this.name}: finite 15-second input/output deadline before ${pattern}`)); }, 15_000);
      this.waiters.add(check); check();
    });
  }
  async ready() {
    await this.expect(/GraphCoder[^\n]*actual local Harness runtime/);
    assert.doesNotMatch(visible(this.text), /UI playback only/);
    await this.expect(/terminal-mode stdin-is-terminal=true interactive=true headless=false process-id=[0-9]+/);
    await this.expect(/graphcoder> /);
    if (process.platform === "win32") {
      const host = await this.expect(/conpty-host primitive=CreatePseudoConsole process-attribute=PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE child-process-id=([0-9]+)/, 0, true);
      assert.equal(Number(host[1]), this.pid, "actual ConPTY child and runtime process identity agree");
    }
    this.record.runtime_pid = this.pid; this.record.observed_stdin_is_terminal = true;
  }
  async command(line, pattern) {
    assert.ok(!/[\r\n]/.test(line) && Buffer.byteLength(line) <= 4096, "single canonical bounded PTY input line");
    assert.ok(this.record.stdin.length < 128, "finite PTY command count");
    this.record.stdin.push(line);
    const from = this.text.length;
    const response = this.expect(pattern, from);
    const prompt = this.expect(/graphcoder> /, from);
    const write = new Promise((resolveWrite, reject) => this.child.stdin.write(line + (process.platform === "win32" ? "\r" : "\n"), error => error ? reject(error) : resolveWrite()));
    const [, , match] = await Promise.all([write, prompt, response]); return match;
  }
  async commandJson(line, kind) {
    const value = await this.command(line, new RegExp(`(?:^|\\n)${kind}-json=([^\\n]+)\\n`, "m"));
    return JSON.parse(value[1]);
  }
  async quit() {
    this.record.stdin.push("/quit");
    this.child.stdin.write(process.platform === "win32" ? "/quit\r" : "/quit\n");
    const exited = await this.exited;
    if (this.failure || suiteFailure) throw this.failure || suiteFailure;
    assert.deepEqual(exited, { code: 0, signal: null }, "actual PTY process successful exit");
  }
  async killOriginal() {
    assert.ok(this.pid, "actual original runtime PID required for process fault");
    this.intentionalKill = true; this.record.runtime_requested_signal = "SIGKILL"; process.kill(this.pid, "SIGKILL");
    await this.exited;
    if (this.failure) throw this.failure;
    // BSD script can return zero even when its slave process was killed.
    assert.throws(() => process.kill(this.pid, 0), error => error.code === "ESRCH", "actual original runtime PID must be gone");
  }
}
let requestIndex = 0;
const input = (label, value) => {
  const path = join(evidence, `${++requestIndex}-${label}.json`);
  writeFileSync(path, JSON.stringify(value) + "\n", { mode: 0o600 }); return bind(path);
};
const escapeRegex = value => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
// Construct real public Resume frames using the installed helper's exact protocol.
// Only authority/cursor demand is changed; no response is supplied to the runtime.
function varint(value) { const bytes = []; do { bytes.push((value & 127) | (value >= 128 ? 128 : 0)); value = Math.floor(value / 128); } while (value); return Buffer.from(bytes); }
function field(number, value) { const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value); return Buffer.concat([varint(number * 8 + 2), varint(bytes.length), bytes]); }
function fields(bytes) {
  let offset = 0; const found = new Map();
  const read = () => { let value = 0, shift = 0; for (let count = 0; count < 8; count++) { assert.ok(offset < bytes.length); const byte = bytes[offset++]; value += (byte & 127) * 2 ** shift; if (!(byte & 128)) return value; shift += 7; } throw new Error("bounded protobuf varint required"); };
  while (offset < bytes.length) { const tag = read(); assert.equal(tag % 8, 2, "template only length-delimited envelope fields"); const size = read(); assert.ok(offset + size <= bytes.length); found.set(Math.floor(tag / 8), bytes.subarray(offset, offset + size)); offset += size; }
  return found;
}
function cursor(manifest, authority, revision, label) {
  const template = readFileSync(manifest.cursor_protobuf);
  const protocol = fields(fields(template).get(1)).get(1); assert.ok(protocol);
  const kind = { agent: 1, conversation: 2, session: 3, turn: 4, task: 5 }[authority.kind.toLowerCase()]; assert.ok(kind);
  const encodedAuthority = Buffer.concat([varint(8), varint(kind), field(2, authority.id)]);
  const encodedCursor = Buffer.concat([field(1, encodedAuthority), field(2, json(manifest.config).generation), varint(24), varint(revision)]);
  const path = join(evidence, `${++requestIndex}-${label}.pb`);
  writeFileSync(path, field(1, Buffer.concat([field(1, protocol), field(2, encodedCursor)])), { mode: 0o600 }); return bind(path);
}
const operationId = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
let blake3;
function modelExecutionOperation(task, command) {
  assert.match(task, operationId); assert.match(command, operationId);
  const bytes = Buffer.concat([Buffer.from("harness/v2/task-execution\0"),
    Buffer.from(task.replaceAll("-", ""), "hex"), Buffer.from(command.replaceAll("-", ""), "hex")]);
  const hex = Buffer.from(blake3(bytes)).subarray(0, 16).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
function invocationLedger(path) {
  assert.ok(isAbsolute(path) && statSync(path).size <= 1_048_576, "bounded actual model invocation observation");
  const bytes = readFileSync(path), text = bytes.toString("utf8");
  assert.deepEqual(Buffer.from(text), bytes, "actual invocation JSONL must be UTF-8");
  assert.ok(text.length === 0 || text.endsWith("\n"), "no partial durable observer row");
  const lines = text.length ? text.slice(0, -1).split("\n") : []; assert.ok(lines.length <= 128);
  return lines.map(line => {
    const row = JSON.parse(line);
    assert.deepEqual(Object.keys(row).sort(), ["format_version", "operation_id", "phase", "request_digest", "step"]);
    assert.equal(row.format_version, 1); assert.ok(["generate", "reconcile"].includes(row.phase));
    assert.match(row.operation_id, operationId); assert.match(row.request_digest, /^[0-9a-f]{64}$/);
    assert.ok(Number.isInteger(row.step) && row.step >= 0 && row.step <= 0xffff_ffff); return row;
  });
}
try {
  try {
    ({ blake3 } = await import("@noble/hashes/blake3.js"));
    bind(fileURLToPath(import.meta.resolve("@noble/hashes/blake3.js")));
  } catch (cause) {
    throw new Error("actual PTY prerequisite unavailable: repository @noble/hashes BLAKE3 dependency", { cause });
  }
  prepareLauncher();
  if (flags.has("--fresh")) {
    const directory = flags.get("--fresh"); assert.ok(!existsSync(directory), "installed helper scenario directory must be fresh");
    const args = ["--prepare-scenario", directory];
    const result = spawnSync(binary, args, { env, encoding: "utf8", timeout: 120_000, killSignal: "SIGKILL", maxBuffer: 1_048_576 });
    writeFileSync(join(evidence, "prepare.stdout.raw"), result.stdout ?? "", { mode: 0o600 });
    writeFileSync(join(evidence, "prepare.stderr.raw"), result.stderr ?? "", { mode: 0o600 });
    report.preparation = { argv: [binary, ...args], exit: result.status, signal: result.signal,
      stdout_sha256: hash(join(evidence, "prepare.stdout.raw")), stderr_sha256: hash(join(evidence, "prepare.stderr.raw")) };
    if (result.error) throw result.error;
    assert.equal(result.signal, null); assert.equal(result.status, 0, "genuine installed preparation prerequisites required");
    const returned = /^scenario-json=(.+)\r?$/m.exec(result.stdout); assert.ok(returned, "installed helper must return its real manifest");
    manifestPath = JSON.parse(returned[1]).manifest;
  } else manifestPath = flags.get("--manifest");
  const manifest = json(bind(manifestPath));
  assert.equal(manifest.format_version, 1); assert.equal(manifest.qualification_ready, true);
  assert.deepEqual(manifest.missing_prerequisites, []);
  configPath = bind(manifest.config); const originalConfigHash = hash(configPath), config = json(configPath);
  assert.ok(config.deterministic_delay_ms >= 1000 && config.deterministic_delay_ms <= 5000, "real finite dispatch window required");
  assert.equal(config.model_invocation_ledger, manifest.model_invocation_ledger);
  assert.ok(isAbsolute(config.model_invocation_ledger), "actual independent provider invocation observation required");
  const { root, left, right, grandchild } = manifest.tasks;
  assert.equal(new Set([root, left, right, grandchild]).size, 4);
  const leases = {}, identities = {};
  for (const label of ["root", "left", "right", "grandchild"]) {
    leases[label] = json(bind(manifest.leases[label]));
    const prepared = manifest.preparations.find(value => value.label === label); assert.equal(prepared.executed, true);
    const record = json(bind(prepared.report)); bind(prepared.request);
    assert.equal(record.child.operation_id, manifest.tasks[label]);
    assert.equal(record.seed.child_agent, manifest.callers[label].agent);
    assert.notEqual(record.seed.child_agent, manifest.tasks[label]);
    identities[label] = { task: manifest.tasks[label], agent: record.seed.child_agent,
      preparation_task: prepared.task, preparation_admission_operation: prepared.operation,
      child_admission_operation: prepared.child_task_operation, source_parent: prepared.parent };
  }
  report.observations.identities = identities;
  active = new PtySession("interactive-native", configPath); await active.ready();
  await active.command("/help", /\/cancel-task TASK/);
  const rootPage = await active.commandJson(`/children ${input("root-children", { task: root, revision: null, after: null })}`, "children");
  assert.ok(rootPage.entries.some(value => value.task_id === left) && rootPage.entries.some(value => value.task_id === right));
  assert.ok(!rootPage.entries.some(value => value.task_id === grandchild), "direct child page never hydrates grandchildren");
  const leftPage = await active.commandJson(`/children ${input("left-children", { task: left, revision: null, after: null })}`, "children");
  assert.ok(leftPage.entries.some(value => value.task_id === grandchild));
  assert.ok(rootPage.entries.length <= 16 && leftPage.entries.length <= 16);
  report.observations.direct_pages = { root: rootPage, left: leftPage };
  const transcript = await active.commandJson(`/file ${input("real-transcript", { caller: root, lease: leases.root, file: manifest.files.root_transcript })}`, "file");
  assert.ok(transcript.text.length > 0); assert.deepEqual(transcript.file, manifest.files.root_transcript);
  const history = await active.commandJson(`/history ${input("owned-history", { caller: root, lease: leases.root, cursor: null })}`, "history");
  assert.equal(history.events.length, 16); assert.deepEqual(history.cursor.authority, manifest.callers.root.authority);
  const nextHistory = await active.commandJson(`/history ${input("owned-history-next", { caller: root, lease: leases.root, cursor: history.cursor })}`, "history");
  assert.ok(nextHistory.events.length > 0 && nextHistory.events.length <= 16);
  const archival = cursor(manifest, manifest.callers.root.authority, 0, "real-root-cursor");
  const page = await active.command(`/cursor ${archival}`, /activity ([0-9]+)\.\.([0-9]+) generation=[^\n]+\n/);
  assert.ok(Number(page[2]) - Number(page[1]) < 16);
  await active.command("/next", /activity [0-9]+\.\.[0-9]+ generation=/);
  assert.match(visible(active.text), /transcript /, "actual EventPayload transcript layout must render");
  await active.command("/back 0", /activity [0-9]+\.\.[0-9]+ generation=/);
  await active.command("/back 1024", /display page unavailable; request an explicit archival page/);
  const before = await active.commandJson(`/read ${input("diff-before", { caller: root, lease: leases.root, file: manifest.files.root_project_before })}`, "file");
  const after = await active.commandJson(`/read ${input("diff-after", { caller: root, lease: leases.root, file: manifest.files.root_project_after })}`, "file");
  assert.notEqual(before.text, after.text);
  const diff = await active.command(`/diff ${input("real-diff", { caller: root, lease: leases.root, before: before.file, after: after.file })}`, /diff exact before=[^\n]+\n--- before\n[\s\S]*?\n\+\+\+ after\n/);
  assert.ok(diff[0].includes(before.text.trimEnd()));
  await active.command("/back 0", /diff exact before=/);
  const publicationRequest = bind(manifest.native_publication_request);
  const pending = await active.commandJson(`/publication-open ${publicationRequest}`, "publication");
  assert.equal(pending.status, "pending"); assert.equal(pending.caller, root);
  assert.equal(pending.operation_id, manifest.native_publication.operation_id);
  // Demand the original root history after its prior exact revision, including the real approval ticket.
  let last = Number(page[2]); let layoutSeen = false;
  for (let count = 0; count < 16 && !layoutSeen; count++) {
    const from = active.text.length;
    const frame = await active.command(`/cursor ${cursor(manifest, manifest.callers.root.authority, last, "approval-cursor")}`, /activity ([0-9]+)\.\.([0-9]+) generation=/);
    assert.ok(Number(frame[2]) > last && Number(frame[2]) - Number(frame[1]) < 16);
    last = Number(frame[2]); layoutSeen = /Decision requires an exact scoped response; displaying is not approval\./.test(visible(active.text.slice(from)));
  }
  assert.ok(layoutSeen, "actual real pending ticket approval layout required");
  assert.match(pending.approved_frame_hex, /^(?:[0-9a-f]{2})+$/);
  const approval = join(evidence, "exact-owned-approval.pb");
  writeFileSync(approval, Buffer.from(pending.approved_frame_hex, "hex"), { mode: 0o600 }); bind(approval);
  await active.command(`/approve ${approval}`, /Admission\([^\n]*Accepted/);
  const approved = await active.commandJson(`/publication-observe ${publicationRequest}`, "publication");
  assert.equal(approved.status, "approved"); assert.deepEqual(approved.action_digest, pending.action_digest);
  report.observations.approval = { operation_id: pending.operation_id, interaction: pending.interaction.id, action_digest: pending.action_digest };
  // Queue through actual interactive prompt while another original task is in the real provider.
  const modelFrom = active.text.length;
  await active.command(`/resume ${manifest.leases.right}`, /original lease submitted for authoritative reconciliation/);
  await active.expect(/deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]{64})/, modelFrom);
  const queued = await active.command(`actual PTY queued input ${randomUUID()}`, /queued task=([0-9a-f-]+) admission-operation=([0-9a-f-]+)/);
  assert.ok(!visible(active.text.slice(modelFrom)).includes(`worker completed task=${right}`),
    "real provider must still be active when interactive queued input is admitted");
  assert.notEqual(queued[1], queued[2]);
  await active.command(`/cancel-task ${queued[1]}`, new RegExp(`cancellation requested task=${escapeRegex(queued[1])}`));
  const cancelled = await active.commandJson(`/outcome ${queued[1]}`, "task");
  assert.equal(cancelled.outcome, "Cancelled"); assert.equal(cancelled.admission_operation, queued[2]);
  await active.expect(new RegExp(`worker completed task=${escapeRegex(right)}`), modelFrom);
  const rightOutcome = await active.commandJson(`/outcome ${right}`, "task"); assert.ok(rightOutcome.outcome.Succeeded);
  // Separate actual caller/source/child/operation observations, with no task-to-agent cast.
  const cross = manifest.preparations.find(value => value.label === "cross_source"); assert.equal(cross.executed, false);
  const crossRequest = json(bind(cross.request)); assert.equal(crossRequest.parent, left);
  const inspected = await active.commandJson(`/inspect ${input("caller-left-source-root", { caller: left, lease: leases.left, source: root })}`, "inspection");
  assert.equal(inspected.caller, left); assert.equal(inspected.source_task, root); assert.equal(inspected.agent, manifest.callers.root.agent);
  await active.command(`/prepare ${cross.request}`, /queued preparation-task=([0-9a-f-]+) admission-operation=([0-9a-f-]+)/);
  const prepFrom = active.text.length;
  await active.command(`/run ${cross.task}`, /worker started task=/);
  await active.expect(new RegExp(`worker completed task=${escapeRegex(cross.task)}`), prepFrom);
  const published = await active.commandJson(`/publish-fork ${input("real-cross-publish", { caller: left, lease: leases.left, preparation: cross.task })}`, "fork");
  const crossTask = published.admission.Accepted; assert.ok(crossTask && !Object.values(manifest.tasks).includes(crossTask));
  assert.deepEqual(published.seed.parent, manifest.callers.left.authority);
  assert.equal(published.seed.child_agent, crossRequest.request.payload.child_agent);
  assert.notEqual(published.seed.child_agent, crossTask);
  report.observations.cross_source = { caller_task: left, source_task: root, source_agent: inspected.agent,
    new_child_task: crossTask, new_child_agent: published.seed.child_agent, preparation_task: cross.task,
    preparation_admission_operation: cross.operation, child_admission_operation: crossRequest.request.child.operation_id };
  // Native handle reopen preserves original graph; it never executes queued admissions.
  await active.command("/reopen", /native providers reopened; no admission or effect resent/);
  assert.deepEqual(await active.commandJson(`/outcome ${right}`, "task"), rightOutcome);
  const rootLeaseHash = hash(manifest.leases.root);
  const dispatchFrom = active.text.length;
  await active.command(`/resume ${manifest.leases.root}`, /original lease submitted for authoritative reconciliation/);
  const dispatch = await active.expect(/deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]{64})/, dispatchFrom);
  const rootModelOperation = modelExecutionOperation(root, manifest.callers.root.admission.input.operation_id);
  assert.equal(dispatch[1], rootModelOperation, "actual dispatch identity derives from the original admitted WorkflowCommand");
  const originalLedgerBytes = readFileSync(config.model_invocation_ledger);
  const beforeKillLedger = join(evidence, "model-before-original-process-kill.jsonl");
  writeFileSync(beforeKillLedger, originalLedgerBytes, { mode: 0o600 }); bind(beforeKillLedger);
  const originalInvocations = invocationLedger(config.model_invocation_ledger);
  const originalGenerate = originalInvocations.filter(row => row.phase === "generate" && row.operation_id === dispatch[1]);
  assert.equal(originalGenerate.length, 1); assert.equal(originalGenerate[0].request_digest, dispatch[2]);
  assert.ok(!visible(active.text.slice(dispatchFrom)).includes(`worker completed task=${root}`), "kill actual original model before completion");
  await active.killOriginal();
  active = new PtySession("original-lease-recovery", configPath); await active.ready();
  await active.command(`/reconcile ${root}`, /retained=Some/);
  const recoveryFrom = active.text.length;
  await active.command(`/resume ${manifest.leases.root}`, /original lease submitted for authoritative reconciliation/);
  await active.expect(new RegExp(`worker completed task=${escapeRegex(root)}`), recoveryFrom);
  const rootOutcome = await active.commandJson(`/outcome ${root}`, "task"); assert.ok(rootOutcome.outcome.Succeeded);
  assert.ok(rootOutcome.outcome.Succeeded.text.includes(dispatch[2]), "original root output binds its exact request digest");
  assert.equal(hash(manifest.leases.root), rootLeaseHash);
  const recovered = invocationLedger(config.model_invocation_ledger).filter(row => row.operation_id === dispatch[1]);
  assert.equal(recovered.filter(row => row.phase === "generate").length, 1);
  assert.ok(recovered.some(row => row.phase === "reconcile" && row.request_digest === dispatch[2] && row.step === 0));
  assert.doesNotMatch(visible(active.text), /deterministic-dispatch/);
  const retainedApproval = await active.commandJson(`/publication-observe ${publicationRequest}`, "publication");
  assert.deepEqual(retainedApproval.action_digest, approved.action_digest); assert.equal(retainedApproval.status, "approved");
  const originalChildren = await active.commandJson(`/children ${input("reopened-root-children", { task: root, revision: null, after: null })}`, "children");
  assert.deepEqual(originalChildren.entries.map(value => value.task_id).sort(), rootPage.entries.map(value => value.task_id).sort(),
    "original direct-child identities survive reopen; current revisions are observed, never guessed");
  const outcomes = new Map([[root, rootOutcome], [right, rightOutcome], [queued[1], cancelled]]);
  for (const label of ["left", "grandchild", "bootstrap_seed"]) {
    const from = active.text.length;
    await active.command(`/resume ${manifest.leases[label]}`, /original lease submitted for authoritative reconciliation/);
    await active.expect(new RegExp(`worker completed task=${escapeRegex(manifest.tasks[label])}`), from);
    const outcome = await active.commandJson(`/outcome ${manifest.tasks[label]}`, "task");
    assert.ok(outcome.outcome.Succeeded); outcomes.set(manifest.tasks[label], outcome);
  }
  const crossFrom = active.text.length;
  await active.command(`/run ${crossTask}`, /worker started task=/);
  await active.expect(new RegExp(`worker completed task=${escapeRegex(crossTask)}`), crossFrom);
  const crossOutcome = await active.commandJson(`/outcome ${crossTask}`, "task");
  assert.ok(crossOutcome.outcome.Succeeded); outcomes.set(crossTask, crossOutcome);
  for (const preparation of manifest.preparations) {
    const outcome = await active.commandJson(`/outcome ${preparation.task}`, "task");
    assert.ok(outcome.outcome.Succeeded); outcomes.set(preparation.task, outcome);
  }
  assert.equal(outcomes.size, 12, "every actual positive PTY task has an authoritative terminal outcome");
  report.observations.terminal_outcomes = [...outcomes.values()];
  await active.quit();
  active = new PtySession("cold-reopen-identities", configPath); await active.ready();
  for (const [task, outcome] of outcomes)
    assert.deepEqual(await active.commandJson(`/outcome ${task}`, "task"), outcome);
  await active.quit();
  report.observations.original_model = { operation_id: dispatch[1], request_digest: dispatch[2], lease_sha256: rootLeaseHash, invocations: recovered, outcome: rootOutcome };
  const finalLedgerBytes = readFileSync(config.model_invocation_ledger);
  assert.deepEqual(finalLedgerBytes.subarray(0, originalLedgerBytes.length), originalLedgerBytes, "durable observer prefix survives original process death");
  const finalRows = invocationLedger(config.model_invocation_ledger);
  const expectedModelOperations = ["bootstrap_seed", "root", "left", "right", "grandchild"].map(label =>
    modelExecutionOperation(manifest.tasks[label], manifest.callers[label].admission.input.operation_id));
  expectedModelOperations.push(modelExecutionOperation(crossTask, crossRequest.request.child.input.operation_id));
  assert.deepEqual(finalRows.filter(row => row.phase === "generate").map(row => row.operation_id).sort(),
    expectedModelOperations.sort(), "six actual non-cancelled PTY model commands each generate exactly once; no unbound invocations");
  assert.ok(finalRows.every(row => expectedModelOperations.includes(row.operation_id) && row.step === 0));
  const finalLedger = join(evidence, "model-after-cold-reopen.jsonl");
  writeFileSync(finalLedger, finalLedgerBytes, { mode: 0o600 }); bind(finalLedger);
  report.observations.model_generations = 6;
  bind(config.model_invocation_ledger);
  assert.equal(hash(binary), report.binary_sha256, "installed binary unchanged throughout actual PTY lane");
  assert.equal(hash(configPath), originalConfigHash, "actual original config unchanged");
  if (suiteFailure) throw suiteFailure;
  report.qualified = true;
} catch (error) {
  report.error = error.stack ?? String(error); process.exitCode = 1;
} finally {
  clearTimeout(overallDeadline);
  if (active && !active.finished) { active.intentionalKill = true; active.terminate(); await active.exited; }
  const reportPath = join(evidence, "pty-evidence.json");
  writeFileSync(reportPath, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
  console.log(JSON.stringify({ qualified: report.qualified, evidence: reportPath, evidence_sha256: hash(reportPath), error: report.error }));
}
