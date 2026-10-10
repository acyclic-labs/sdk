#!/usr/bin/env node
// Installed process/presentation checks. Pipes are never claimed as PTY evidence.
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { isDeepStrictEqual } from "node:util";

const [binary, ...options] = process.argv.slice(2);
let session;
let scenarioDirectory;
while (options.length) {
  const option = options.shift();
  if (option === "--full" && !scenarioDirectory) {
    scenarioDirectory = options.shift();
    if (!scenarioDirectory) throw new Error("--full requires an absolute scenario directory");
  }
  else if (!session && !option.startsWith("--")) session = option;
  else throw new Error(`unexpected argument: ${option}`);
}
if (!binary || !isAbsolute(binary) || !statSync(binary).isFile()
    || (session && (!isAbsolute(session) || !statSync(session).isFile() || statSync(session).size > 1_048_576))
    || (scenarioDirectory !== undefined && !isAbsolute(scenarioDirectory))) {
  throw new Error("usage: node scripts/test-graphcoder.mjs ABSOLUTE_INSTALLED_BINARY [ABSOLUTE_NATIVE_CONFIG] [--full ABSOLUTE_SCENARIO_DIR]");
}
const digest = path => createHash("sha256").update(readFileSync(path)).digest("hex");
console.log(`binary=${binary}`);
console.log(`binary_sha256=${digest(binary)}`);
console.log(`suite_sha256=${digest(fileURLToPath(import.meta.url))}`);
console.log(JSON.stringify({ evidence: "installed child processes over actual pipes; not PTY",
  stock_regression: Boolean(session), full_scenario: Boolean(scenarioDirectory) }));
// The installed binary receives no tool search path or environment credentials.
const env = { PATH: "" };
for (const key of ["SystemRoot", "WINDIR", "TEMP", "TMP"]) {
  if (process.env[key] !== undefined) env[key] = process.env[key];
}
function run(name, args, input, success, timeout = 5_000) {
  const result = spawnSync(binary, args, {
    input, encoding: "utf8", env, windowsHide: true,
    timeout, killSignal: "SIGKILL", maxBuffer: 1_048_576,
  });
  console.log(JSON.stringify({ case: name, argv: args, stdin: input, exit: result.status,
    signal: result.signal, stdout: result.stdout, stderr: result.stderr }));
  // Timeouts, launch failures and signal deaths never satisfy a negative case.
  if (result.error) throw result.error;
  assert.equal(result.signal, null, `${name}: terminated by signal`);
  assert.notEqual(result.status, null, `${name}: missing exit status`);
  assert.equal(result.status === 0, success, `${name}: unexpected exit`);
  return result;
}
const unavailableHost = run("default-unavailable", [], "", false);
assert.match(unavailableHost.stderr, /durable terminal host is unavailable/);
const args = ["--fixture", "wire", "--headless"];
const playback = run("explicit-playback", args, "/next\n/next\n/reset\n/next\n/quit\n", true);
assert.match(playback.stdout, /UI playback only, no durable effects/);
assert.equal(playback.stdout.split("activity ").length - 1, 2);
assert.equal(playback.stdout.split("end of fixture activity").length - 1, 1);
assert.match(playback.stdout, /fixture display reset; no session restored/);
assert.doesNotMatch(playback.stdout, /fixture> /);
const unavailableInput = run("input-not-queued", args, "hello\n/next\n", false);
assert.match(unavailableInput.stderr, /fixture input is not queued/);
assert.doesNotMatch(unavailableInput.stdout, /activity /);
const oversized = run("input-bound", args, "x".repeat(65_537) + "\n/next\n", false);
assert.match(oversized.stderr, /terminal input exceeds bounds/);
assert.doesNotMatch(oversized.stdout, /activity /);

  // Each finite child is the actual installed binary with real local providers.
  // This harness supplies no reducer, journal, provider outcome or fake terminal.
  class InstalledSession {
    constructor(name, configuration) {
      this.name = name;
      this.stdout = "";
      this.stderr = "";
      this.waiters = new Set();
      this.failure = null;
      this.finished = null;
      this.requestedKill = false;
      this.argv = ["--session", configuration, "--headless"];
      this.capturedBytes = 0;
      this.child = spawn(binary, this.argv, {
        env, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
      });
      this.child.stdout.setEncoding("utf8");
      this.child.stderr.setEncoding("utf8");
      const capture = (stream, chunk) => {
        this.capturedBytes += Buffer.byteLength(chunk);
        if (this.capturedBytes > 1_048_576) {
          this.failure = new Error(`${name}: captured output exceeds bounds`);
          this.child.kill("SIGKILL");
        } else this[stream] += chunk;
        this.notify();
      };
      this.child.stdout.on("data", chunk => capture("stdout", chunk));
      this.child.stderr.on("data", chunk => capture("stderr", chunk));
      this.child.stdin.on("error", error => { this.failure = error; this.notify(); });
      this.exited = new Promise(resolve => {
        this.child.on("error", error => { this.failure = error; this.notify(); });
        this.child.on("close", (code, signal) => {
          clearTimeout(this.deadline);
          this.finished = { code, signal };
          console.log(JSON.stringify({ case: name, argv: this.argv,
            exit: code, signal, stdout: this.stdout, stderr: this.stderr }));
          this.notify();
          resolve(this.finished);
        });
      });
      this.deadline = setTimeout(() => {
        this.failure = new Error(`${name}: finite installed process deadline exceeded`);
        this.child.kill("SIGKILL");
        this.notify();
      }, 60_000);
    }
    notify() { for (const waiter of [...this.waiters]) waiter(); }
    expect(pattern, stream = "stdout", from = 0) {
      return new Promise((resolve, reject) => {
        const check = () => {
          if (this.failure) { this.waiters.delete(check); reject(this.failure); return; }
          const match = pattern.exec(this[stream].slice(from));
          if (match) { this.waiters.delete(check); resolve(match); return; }
          if (this.finished) {
            this.waiters.delete(check);
            reject(new Error(`${this.name}: exited before ${pattern}`));
          }
        };
        this.waiters.add(check);
        check();
      });
    }
    command(line, pattern) {
      assert.ok(Buffer.byteLength(line) <= 65_536 && !/[\r\n]/.test(line), "bounded single input command");
      console.log(JSON.stringify({ case: this.name, stdin: line }));
      const pending = this.expect(pattern, "stdout", this.stdout.length);
      this.child.stdin.write(`${line}\n`);
      return pending;
    }
    async commandJson(line, kind) {
      assert.match(kind, /^[a-z-]+$/);
      const match = await this.command(line, new RegExp(`^${kind}-json=(.+)\\r?\\n`, "m"));
      return JSON.parse(match[1]);
    }
    async quit() {
      this.child.stdin.end("/quit\n");
      const exit = await this.exited;
      if (this.failure) throw this.failure;
      assert.deepEqual(exit, { code: 0, signal: null });
    }
    async stop() {
      if (!this.finished) this.requestedKill = this.child.kill("SIGKILL");
      return await this.exited;
    }
  }
if (session) {
  const config = JSON.parse(readFileSync(session, "utf8"));
  assert.ok(config.deterministic_delay_ms >= 1000 && config.deterministic_delay_ms <= 5000,
    "native fault qualification requires an explicitly bound 1000..5000ms dispatch delay");
  console.log(`native_config_sha256=${digest(session)}`);
  const temporary = mkdtempSync(join(tmpdir(), "graphcoder-installed-"));


  let active;
  try {
    active = new InstalledSession("native-stock-queued-and-model-fault-regression", session);
    await active.expect(/actual local Harness runtime/);
    const queue = text => active.command(text,
      /queued task=([0-9a-f-]+) admission-operation=([0-9a-f-]+)/);
    const root = await queue("root operation from actual installed stdin");
    await active.command(`/parent ${root[1]}`, /selected parent-task=/);
    const left = await queue("left child");
    const right = await queue("right child");
    await active.command(`/parent ${left[1]}`, /selected parent-task=/);
    const grandchild = await queue("grandchild");
    const tasks = [root[1], left[1], right[1], grandchild[1]];
    const operations = [root[2], left[2], right[2], grandchild[2]];
    assert.equal(new Set(tasks).size, 4, "distinct retained task identities");
    assert.equal(new Set(operations).size, 4, "distinct original admission identities");
    await active.command("/reopen", /providers reopened; no admission or effect resent/);
    for (const task of tasks) {
      await active.command(`/outcome ${task}`, /outcome=None/);
    }
    const children = join(temporary, "children.json");
    writeFileSync(children, JSON.stringify({ task: root[1], revision: null, after: null }));
    const childrenPage = await active.command(`/children ${children}`, /children parent-task=[^\n]+\n/);
    assert.ok(childrenPage[0].includes(left[1]) && childrenPage[0].includes(right[1]));
    assert.ok(!childrenPage[0].includes(grandchild[1]), "direct-child demand cannot hydrate grandchildren");

    const claim = await active.command(`/run ${root[1]}`, /worker-claim task=[^\n]+ lease-json-hex=([0-9a-f]+)/);
    // The provider barrier occurs after ordinary model-start journaling, before
    // delayed model content. Only the real installed process emits this barrier.
    await active.expect(/deterministic-dispatch operation=[0-9a-f-]+ step=0 request=[0-9a-f]+/, "stderr");
    // The prompt must remain responsive while an ordinary worker/provider is
    // active. Cancel a queued sibling through the real owner-bound task host.
    await active.command(`/cancel-task ${right[1]}`, /cancellation requested task=/);
    await active.command(`/outcome ${right[1]}`, /outcome=Some\(Cancelled\)/);
    await active.stop();
    if (active.failure) throw active.failure;
    assert.ok(!active.stdout.includes(`worker completed task=${root[1]}`),
      "fault must interrupt actual dispatch, not kill an already completed run");
    const lease = join(temporary, "original-lease.json");
    const originalLease = Buffer.from(claim[1], "hex");
    JSON.parse(originalLease.toString("utf8"));
    writeFileSync(lease, originalLease);

    active = new InstalledSession("native-stock-original-lease-reconcile-regression", session);
    await active.expect(/actual local Harness runtime/);
    await active.command(`/reconcile ${root[2]}`, /retained=Some/);
    await active.command(`/resume ${lease}`, /worker completed task=/);
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/,
      "interrupted original model attempt must reconcile, not generate again");
    for (const task of [left[1], grandchild[1]]) {
      await active.command(`/run ${task}`, /worker completed task=/);
    }
    await active.command(`/run ${right[1]}`, /worker idle task=/);
    const outcomes = [];
    for (const task of tasks) {
      const outcome = await active.command(`/outcome ${task}`, /outcome=Some\([^\n]+\n/);
      if (task === right[1]) assert.match(outcome[0], /Cancelled/);
      else assert.match(outcome[0], /Succeeded.*deterministic request=/);
      outcomes.push(outcome[0]);
    }
    await active.command("/reopen", /providers reopened; no admission or effect resent/);
    for (let index = 0; index < tasks.length; index++) {
      const after = await active.command(`/outcome ${tasks[index]}`, /outcome=Some\([^\n]+\n/);
      assert.equal(after[0], outcomes[index]);
      await active.command(`/run ${tasks[index]}`, /worker idle task=/);
    }
    await active.quit();

    active = new InstalledSession("native-stock-process-reopen-regression", session);
    await active.expect(/actual local Harness runtime/);
    for (let index = 0; index < tasks.length; index++) {
      const after = await active.command(`/outcome ${tasks[index]}`, /outcome=Some\([^\n]+\n/);
      assert.equal(after[0], outcomes[index]);
      await active.command(`/run ${tasks[index]}`, /worker idle task=/);
    }
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/);
    await active.quit();
  } finally {
    if (active) await active.stop();
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (scenarioDirectory) {
  // This is a separate real-provider qualification, never the fixture playback
  // or stock queued-task regression above. The installed helper bootstraps real
  // retained allocations/admissions; it does not provide expected UI outcomes.
  console.log("full scenario bootstrap: actual installed helper precreates the ordinary fork tree");
  try {
    run("native-full-real-provider-bootstrap",
      ["--prepare-scenario", scenarioDirectory], "", true, 120_000);
  } catch (error) {
    throw new Error("full installed scenario prerequisite failed: real native scenario producer must be installed and available",
      { cause: error });
  }
  const manifestPath = join(scenarioDirectory, "native-scenario.json");
  assert.ok(statSync(manifestPath).isFile() && statSync(manifestPath).size <= 1_048_576,
    "full scenario producer must publish a bounded native-scenario.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  console.log(`native_scenario_sha256=${digest(manifestPath)}`);
  assert.equal(manifest.format_version, 1, "native scenario manifest format");
  assert.ok(Array.isArray(manifest.missing_prerequisites), "producer must disclose unavailable domain contracts");
  if (manifest.missing_prerequisites.length) {
    throw new Error(`full installed scenario prerequisites unavailable: ${manifest.missing_prerequisites.join("; ")}`);
  }
  assert.equal(manifest.qualification_ready, true, "producer must have real inputs for every mandatory domain control");
  console.log(JSON.stringify({ bootstrap: manifest.bootstrap, authentication: manifest.authentication }));
  assert.ok(isAbsolute(manifest.config) && statSync(manifest.config).isFile()
    && statSync(manifest.config).size <= 1_048_576,
  "producer must supply the exact finitely bounded native provider configuration");
  console.log(`full_native_config_sha256=${digest(manifest.config)}`);
  const fullConfig = JSON.parse(readFileSync(manifest.config, "utf8"));
  assert.ok(fullConfig.deterministic_delay_ms >= 1000 && fullConfig.deterministic_delay_ms <= 5000,
    "full qualification requires an explicitly bounded real dispatch window for steering and process death");
  assert.deepEqual(fullConfig.session_limits, manifest.session_limits,
    "actual original native configuration, not manifest assertions, fixes the finite session budget");
  const temporary = mkdtempSync(join(tmpdir(), "graphcoder-full-installed-"));
  let active;
  let negativeActive;
  const input = (name, value) => {
    const bytes = JSON.stringify(value);
    assert.ok(Buffer.byteLength(bytes) <= 65_536, `${name}: prepared request exceeds terminal bound`);
    const path = join(temporary, `${name}.json`);
    writeFileSync(path, bytes);
    return path;
  };
  const load = path => {
    assert.ok(isAbsolute(path) && statSync(path).isFile() && statSync(path).size <= 1_048_576,
      "producer must supply a finite retained artifact");
    return JSON.parse(readFileSync(path, "utf8"));
  };
  const queueMailbox = async (client, facts, label) => {
    const rootLease = load(facts.leases.root);
    const receiveOperation = randomUUID();
    const modelOperation = randomUUID();
    const receive = await client.commandJson(`/write ${input(`${label}-receive-command`, {
      caller: facts.tasks.root, lease: rootLease, volume: facts.callers.root.private_volume,
      operation: receiveOperation, path: `installed/${label}-receive.json`, bytes: JSON.stringify({ after: 0 }),
    })}`, "file");
    const model = await client.commandJson(`/write ${input(`${label}-model-command`, {
      caller: facts.tasks.root, lease: rootLease, volume: facts.callers.root.private_volume,
      operation: modelOperation, path: `installed/${label}-model.json`,
      bytes: JSON.stringify({ input: "actual installed mailbox steering",
        selected_context: null, max_steps: 1, max_output_tokens: 256, mailbox_operation: receiveOperation }),
    })}`, "file");
    const operation = randomUUID();
    const admitted = await client.command(`/queue-mailbox ${input(`${label}-mailbox-admission`, {
      operation, parent: facts.tasks.root, lease: rootLease, input: {
        receive: { operation_id: receiveOperation, kind: "acyclic.mail.receive.v1", payload: receive.file },
        model: { operation_id: modelOperation, kind: "acyclic.model.v1", payload: model.file },
      },
    })}`, /queued task=([0-9a-f-]+) admission-operation=([0-9a-f-]+)/);
    assert.equal(admitted[2], operation);
    assert.notEqual(admitted[1], facts.tasks.root);
    return { task: admitted[1], operation, receiveOperation, modelOperation };
  };
  try {
    const { root, left, right, grandchild } = manifest.tasks;
    assert.equal(new Set([root, left, right, grandchild]).size, 4, "actual retained task identities are distinct");
    assert.notEqual(root, manifest.tasks.bootstrap_seed, "logical root is disclosed as a genuinely allocated child");
    const retained = {};
    for (const label of ["root", "left", "right", "grandchild"]) {
      const preparation = manifest.preparations.find(value => value.label === label);
      assert.ok(preparation?.executed, `${label}: real bootstrap preparation required`);
      const request = load(preparation.request);
      const result = load(preparation.report);
      assert.equal(request.operation, preparation.operation);
      assert.equal(request.parent, preparation.parent);
      assert.equal(result.child.operation_id, manifest.tasks[label]);
      assert.equal(result.child.parent, preparation.parent);
      assert.ok(result.child.resource_proofs.length >= 2, `${label}: actual creator proofs, not invented grants`);
      assert.equal(result.report.request.operation_id, result.seed.operation_id);
      assert.equal(result.report.request.child_agent, result.seed.child_agent);
      assert.notEqual(result.seed.child_agent, manifest.tasks[label], "agent identity is not a task UUID cast");
      assert.equal(new Set([preparation.task, request.request.child.operation_id,
        request.request.payload.operation_id]).size, 3,
      `${label}: preparation task, child admission, and fork effect are distinct`);
      retained[label] = result;
    }
    assert.equal(retained.left.child.parent, root);
    assert.equal(retained.right.child.parent, root);
    assert.equal(retained.grandchild.child.parent, left);
    assert.equal(new Set(Object.values(retained).map(value =>
      JSON.stringify(value.report.request.preparation.child_project_volume))).size, 4,
    "real children use separate project allocations");
    assert.equal(new Set(Object.values(retained).map(value =>
      JSON.stringify(value.seed.child_private_volume))).size, 4,
    "real children use separate caller-owned private allocations");
    active = new InstalledSession("native-full-recursive-installed-interactions", manifest.config);
    await active.expect(/actual local Harness runtime/);
    const rootChildren = await active.commandJson(`/children ${input("root-children",
      { task: root, revision: null, after: null })}`, "children");
    assert.ok(rootChildren.entries.length <= 16);
    assert.ok(rootChildren.entries.some(value => value.task_id === left));
    assert.ok(rootChildren.entries.some(value => value.task_id === right));
    assert.ok(!rootChildren.entries.some(value => value.task_id === grandchild),
      "direct child inspection never hydrates grandchildren");
    const leftChildren = await active.commandJson(`/children ${input("left-children",
      { task: left, revision: null, after: null })}`, "children");
    assert.ok(leftChildren.entries.length <= 16);
    assert.ok(leftChildren.entries.some(value => value.task_id === grandchild));
    const leases = Object.fromEntries(["root", "left", "right", "grandchild"].map(label =>
      [label, load(manifest.leases[label])]));
    const before = {};
    for (const label of ["root", "left", "right", "grandchild"]) {
      assert.ok(manifest.files[`${label}_project`], `${label}: actual project sentinel FileRef required`);
      const value = await active.commandJson(`/read ${input(`project-before-${label}`, {
        caller: manifest.tasks[label], lease: leases[label], file: manifest.files[`${label}_project`],
      })}`, "file");
      assert.deepEqual(value.file, manifest.files[`${label}_project`]);
      before[label] = value.text;
    }
    assert.equal(new Set(Object.values(before)).size, 4,
      "genuine bootstrap edits produced independent caller workspace content");
    const mutation = `installed-left-project-${randomUUID()}`;
    const changed = await active.commandJson(`/write ${input("left-project-write", {
      caller: left, lease: leases.left, volume: retained.left.report.request.preparation.child_project_volume,
      operation: randomUUID(), path: manifest.files.left_project.path, bytes: mutation,
    })}`, "file");
    assert.deepEqual(changed.file.volume, manifest.files.left_project.volume);
    assert.notEqual(changed.file.version, manifest.files.left_project.version);
    const changedRead = await active.commandJson(`/read ${input("left-project-written", {
      caller: left, lease: leases.left, file: changed.file,
    })}`, "file");
    assert.equal(changedRead.text, mutation);
    for (const label of ["root", "left", "right", "grandchild"]) {
      const pinned = await active.commandJson(`/read ${input(`project-pinned-${label}`, {
        caller: manifest.tasks[label], lease: leases[label], file: manifest.files[`${label}_project`],
      })}`, "file");
      assert.equal(pinned.text, before[label], "retained before-generation content remains immutable");
    }
    for (const label of ["root", "left", "right", "grandchild"]) {
      const current = await active.commandJson(`/read-path ${input(`project-head-${label}`, {
        caller: manifest.tasks[label], lease: leases[label],
        volume: manifest.files[`${label}_project`].volume, path: manifest.files[`${label}_project`].path,
      })}`, "file");
      assert.equal(current.text, label === "left" ? mutation : before[label],
        "actual mutable workspace heads stay isolated after a real left write");
    }
    const callerContent = {};
    for (const label of ["root", "left"]) {
      for (const kind of ["transcript", "scratch"]) {
        const value = await active.commandJson(`/file ${input(`caller-file-${label}-${kind}`, {
          caller: manifest.tasks[label], lease: leases[label], file: manifest.files[`${label}_${kind}`],
        })}`, "file");
        assert.ok(value.text.length > 0);
        callerContent[`${label}_${kind}`] = value.text;
      }
    }
    assert.notEqual(callerContent.root_transcript, callerContent.left_transcript,
      "cross-source transcript test uses genuinely different caller content");
    assert.notEqual(callerContent.root_scratch, callerContent.left_scratch,
      "cross-source scratch test uses genuinely different private content");
    const beforeRoot = await active.commandJson(`/read ${input("root-before-diff", {
      caller: root, lease: leases.root, file: manifest.files.root_project_before,
    })}`, "file");
    assert.notEqual(beforeRoot.text, before.root, "real diff selects different retained generation bytes");
    const diff = await active.command(`/diff ${input("root-exact-diff", {
      caller: root, lease: leases.root,
      before: manifest.files.root_project_before, after: manifest.files.root_project_after,
    })}`, /^diff exact before=[^\n]+\n--- before\n[\s\S]*?\n\+\+\+ after\n[^\n]*\n/m);
    assert.ok(diff[0].includes(beforeRoot.text));
    assert.ok(diff[0].includes(before.root.trimEnd()));
    let historyCursor = null;
    const rootHistory = [];
    let historyPages = 0;
    do {
      assert.ok(historyPages < 16, "bounded explicit archival demands cannot become an unbounded history scan");
      const page = await active.commandJson(`/history ${input(`root-history-${historyPages}`, {
        caller: root, lease: leases.root, cursor: historyCursor,
      })}`, "history");
      assert.ok(page.events.length > 0 && page.events.length <= 16);
      assert.deepEqual(page.cursor.authority, manifest.callers.root.authority);
      assert.deepEqual(page.events.map(value => value.revision),
        Array.from({ length: page.events.length }, (_, index) => (historyCursor?.after_revision ?? 0) + index + 1));
      assert.equal(page.cursor.after_revision, page.events.at(-1).revision);
      if (historyCursor) assert.equal(page.cursor.through_revision, historyCursor.through_revision,
        "archival continuation stays pinned to its actual original committed boundary");
      else {
        assert.equal(page.events.length, 16);
        assert.ok(page.cursor.after_revision < page.cursor.through_revision,
          "real seeded caller history requires another explicit page");
      }
      rootHistory.push(...page.events);
      historyCursor = page.cursor;
      historyPages++;
    } while (historyCursor.after_revision < historyCursor.through_revision);
    const messages = rootHistory.filter(value => value.payload.kind === "conversation_message_appended");
    assert.ok(messages.length >= 17, "bounded history reads retrieve genuine signed caller messages");
    assert.ok(messages.some(value => isDeepStrictEqual(value.payload.message.content, manifest.files.root_transcript)));
    assert.ok(isAbsolute(manifest.cursor_protobuf) && statSync(manifest.cursor_protobuf).size <= 65_536);
    const wirePage = await active.command(`/cursor ${manifest.cursor_protobuf}`,
      /activity ([0-9]+)\.\.([0-9]+) generation=([^\n]+)\n/);
    assert.ok(Number(wirePage[2]) - Number(wirePage[1]) + 1 <= 16,
      "actual protobuf cursor dispatch exposes one bounded activity page");
    assert.equal(wirePage[3], load(manifest.config).generation);
    await active.command("/back 1024", /display page unavailable; request an explicit archival page/);
    const sends = [];
    for (let index = 0; index < 17; index++) {
      const request = { caller: root, lease: leases.root, recipient: left,
        operation: randomUUID(), payload: manifest.files.root_project };
      const path = input(`bounded-mail-${index}`, request);
      const ack = await active.commandJson(`/mail ${path}`, "mail");
      assert.equal(ack.sender, root);
      assert.equal(ack.recipient, left);
      assert.equal(ack.operation_id, request.operation);
      assert.deepEqual(ack.payload, request.payload);
      sends.push({ request, path, ack });
    }
    const exactReplay = await active.commandJson(`/mail ${sends[0].path}`, "mail");
    assert.deepEqual(exactReplay, sends[0].ack, "exact real send retry observes its original receipt");
    const inboxPath = input("bounded-inbox-first", { caller: left, lease: leases.left, after: 0 });
    const inboxFirst = await active.commandJson(`/inbox ${inboxPath}`, "inbox");
    assert.equal(inboxFirst.task, left);
    assert.equal(inboxFirst.consumption, "observation_only");
    assert.equal(inboxFirst.items.length, 16, "actual inbox demand is bounded to sixteen");
    assert.equal(inboxFirst.next_after, inboxFirst.items.at(-1).sequence);
    assert.deepEqual(inboxFirst.items.map(value => value.sequence),
      Array.from({ length: 16 }, (_, index) => index + 1));
    assert.ok(inboxFirst.items.every(value => value.task_id === left
      && isDeepStrictEqual(value.payload, manifest.files.root_project)));
    const inboxAgain = await active.commandJson(`/inbox ${inboxPath}`, "inbox");
    assert.deepEqual(inboxAgain, inboxFirst, "passive inbox inspection never consumes model steering");
    const inboxLast = await active.commandJson(`/inbox ${input("bounded-inbox-next", {
      caller: left, lease: leases.left, after: inboxFirst.next_after,
    })}`, "inbox");
    assert.equal(inboxLast.items.length, 1, "seventeenth mail requires explicit continuation");
    assert.equal(inboxLast.items[0].sequence, 17, "exact send retry never appends another inbox item");
    assert.equal(manifest.session_limits.total_tasks, 12, "actual positive named graph plus one ordinary mailbox task");
    assert.equal(manifest.session_limits.model_steps, 7, "actual positive model-task allowance");
    const mailbox = await queueMailbox(active, manifest, "positive");
    const mailboxClaim = await active.command(`/claim ${mailbox.task}`,
      /worker-claim task=([0-9a-f-]+) lease-json-hex=([0-9a-f]+)/);
    assert.equal(mailboxClaim[1], mailbox.task);
    const mailboxLeaseBytes = Buffer.from(mailboxClaim[2], "hex");
    const mailboxLease = JSON.parse(mailboxLeaseBytes.toString("utf8"));
    const mailboxLeasePath = join(temporary, "mailbox-original-lease.json");
    writeFileSync(mailboxLeasePath, mailboxLeaseBytes);
    console.log(`mailbox_original_lease_sha256=${digest(mailboxLeasePath)}`);
    const mailboxStart = active.stdout.length;
    await active.command(`/resume ${mailboxLeasePath}`, /original lease submitted for authoritative reconciliation/);
    await active.expect(new RegExp(`worker suspended task=${mailbox.task} checkpoint=`), "stdout", mailboxStart);
    const waiting = await active.commandJson(`/checkpoint ${input("mailbox-wait-checkpoint", {
      caller: mailbox.task, lease: mailboxLease,
    })}`, "checkpoint");
    assert.equal(waiting.task, mailbox.task);
    assert.ok(waiting.checkpoint.revision > 0 && waiting.transition,
      "waiting is a real committed workflow checkpoint and retained outbox");
    assert.equal(waiting.checkpoint.machine.name, "acyclic.mailbox_model");
    assert.equal(waiting.checkpoint.state.mail, null);
    assert.equal(waiting.checkpoint.state.stage, "receive");
    assert.equal(waiting.transition.status.kind, "suspended");
    assert.equal(waiting.transition.commands[0].operation_id, mailbox.receiveOperation);
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/,
      "empty inbox suspension cannot admit model work");
    const firstSteer = await active.commandJson(`/mail ${input("mailbox-first-steer", {
      caller: root, lease: leases.root, recipient: mailbox.task,
      operation: randomUUID(), payload: manifest.files.root_project_before,
    })}`, "mail");
    assert.equal(firstSteer.recipient, mailbox.task);
    const dispatchStart = active.stderr.length;
    const modelStart = active.stdout.length;
    await active.command(`/resume ${mailboxLeasePath}`, /original lease submitted for authoritative reconciliation/);
    const mailboxDispatch = await active.expect(
      /deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]+)/,
      "stderr", dispatchStart);
    const consumed = await active.commandJson(`/checkpoint ${input("mailbox-consumed-checkpoint", {
      caller: mailbox.task, lease: mailboxLease,
    })}`, "checkpoint");
    assert.ok(consumed.checkpoint.revision > waiting.checkpoint.revision);
    assert.equal(consumed.checkpoint.state.mail.sequence, 1);
    assert.deepEqual(consumed.checkpoint.state.mail.payload, manifest.files.root_project_before);
    assert.equal(consumed.transition.commands[0].operation_id, mailbox.modelOperation);
    await active.commandJson(`/mail ${input("mailbox-later-steer", {
      caller: root, lease: leases.root, recipient: mailbox.task,
      operation: randomUUID(), payload: manifest.files.root_project_after,
    })}`, "mail");
    const queuedSteering = await active.commandJson(`/inbox ${input("mailbox-live-inbox", {
      caller: mailbox.task, lease: mailboxLease, after: 0,
    })}`, "inbox");
    assert.equal(queuedSteering.consumption, "observation_only");
    assert.equal(queuedSteering.items.length, 2);
    assert.deepEqual(queuedSteering.items[1].payload, manifest.files.root_project_after);
    const notReinterpreted = await active.commandJson(`/checkpoint ${input("mailbox-live-checkpoint", {
      caller: mailbox.task, lease: mailboxLease,
    })}`, "checkpoint");
    assert.deepEqual(notReinterpreted.checkpoint.state.mail, consumed.checkpoint.state.mail,
      "later steering cannot replace the first checkpointed model input");
    assert.ok(!active.stdout.slice(modelStart).includes(`worker completed task=${mailbox.task}`),
      "later steering and inspection must happen while the real model attempt remains active");
    await active.expect(new RegExp(`worker completed task=${mailbox.task}`), "stdout", modelStart);
    const mailboxOutcome = await active.commandJson(`/outcome ${mailbox.task}`, "task");
    assert.ok(mailboxOutcome.outcome.Succeeded);
    assert.deepEqual(mailboxOutcome.outcome.Succeeded.mail, consumed.checkpoint.state.mail);
    assert.ok(mailboxOutcome.outcome.Succeeded.model.Succeeded.text.includes(mailboxDispatch[2]),
      "completed model outcome binds the actual original dispatch digest");
    const pendingCross = manifest.preparations.find(value => value.label === "cross_source");
    assert.equal(pendingCross.executed, false, "cross-source preparation must be fresh installed interaction");
    const crossInput = load(pendingCross.request);
    assert.equal(crossInput.parent, left);
    assert.deepEqual(crossInput.caller_lease, leases.left);
    assert.deepEqual(crossInput.request.payload.parent, manifest.callers.left.authority);
    const selectedProject = crossInput.request.payload.selections.find(value => value.revision.kind === "project");
    const selectedPrivate = crossInput.request.payload.selections.find(value => value.revision.kind === "private_volume");
    assert.deepEqual(selectedProject.revision.reference.volume, manifest.callers.root.project);
    assert.deepEqual(selectedPrivate.revision.reference.volume, manifest.callers.left.private_volume);
    assert.deepEqual(selectedPrivate.revision.reference.paths, [], "scratch fork starts empty, retaining only actual caller lineage");
    const selectedInspection = await active.commandJson(`/inspect ${input("left-selected-root-inspection", {
      caller: left, lease: leases.left, source: root,
    })}`, "inspection");
    assert.equal(selectedInspection.caller, left);
    assert.equal(selectedInspection.source_task, root);
    assert.equal(selectedInspection.source.operation_id, retained.root.child.operation_id);
    assert.equal(selectedInspection.agent, manifest.callers.root.agent);
    assert.equal(selectedInspection.generation_semantics, "original_allocation");
    assert.deepEqual(selectedInspection.project.volume, selectedProject.revision.reference.volume);
    assert.deepEqual(selectedInspection.project.generation, selectedProject.revision.reference.generation);
    assert.deepEqual(selectedInspection.private_volume.volume, manifest.callers.root.private_volume);
    assert.equal(selectedInspection.volumes.length, 2, "selected inspection returns only actual requested source allocation metadata");
    const selectedForeign = await active.commandJson(`/read-path ${input("root-exact-selected-foreign-generation", {
      caller: root, lease: leases.root, volume: selectedProject.revision.reference.volume,
      path: manifest.files.root_project.path, generation: selectedProject.revision.reference.generation,
    })}`, "file");
    assert.notEqual(selectedForeign.file.version, manifest.files.root_project.version,
      "foreign selection explicitly pins original allocation, never silently substitutes latest mutable root head");
    const preparedCross = await active.command(`/prepare ${pendingCross.request}`,
      /queued preparation-task=([0-9a-f-]+) admission-operation=([0-9a-f-]+) caller-task=([0-9a-f-]+)/);
    assert.equal(preparedCross[1], pendingCross.task);
    assert.equal(preparedCross[2], pendingCross.operation);
    assert.equal(preparedCross[3], left);
    const crossStart = active.stdout.length;
    await active.command(`/run ${pendingCross.task}`, /worker-claim task=[^\n]+ lease-json-hex=[0-9a-f]+/);
    await active.expect(new RegExp(`worker completed task=${pendingCross.task}`), "stdout", crossStart);
    const preparedReport = await active.commandJson(`/outcome ${pendingCross.task}`, "task");
    assert.equal(preparedReport.task, pendingCross.task);
    assert.ok(preparedReport.outcome.Succeeded, "real creator preparation must authoritatively succeed");
    const crossReport = preparedReport.outcome.Succeeded;
    assert.deepEqual(crossReport.request, crossInput.request.payload);
    assert.notEqual(crossReport.request.child_agent, left);
    assert.equal(new Set([pendingCross.task, crossInput.request.child.operation_id,
      crossReport.request.child_agent, crossReport.request.operation_id]).size, 4,
    "caller selection, new child agent, preparation task and admissions never collapse");
    const publishedCross = await active.commandJson(`/publish-fork ${input("cross-source-publish", {
      caller: left, lease: leases.left, preparation: pendingCross.task,
    })}`, "fork");
    assert.equal(publishedCross.preparation, pendingCross.task);
    assert.equal(publishedCross.new_child_agent, crossReport.request.child_agent);
    assert.equal(publishedCross.child_admission_operation, crossInput.request.child.operation_id);
    const crossTask = publishedCross.admission.Accepted;
    assert.ok(crossTask && !Object.values(manifest.tasks).includes(crossTask));
    assert.deepEqual(publishedCross.seed.parent, manifest.callers.left.authority);
    const crossProject = publishedCross.seed.resources.find(value => value.source.kind === "project");
    const crossPrivate = publishedCross.seed.resources.find(value => value.source.kind === "private_volume");
    assert.deepEqual(crossProject.source.reference.volume, manifest.callers.root.project);
    assert.deepEqual(crossPrivate.source.reference.volume, manifest.callers.left.private_volume);
    assert.deepEqual(crossPrivate.source.reference.paths, []);
    assert.deepEqual(crossProject.revision.reference.volume, crossReport.request.preparation.child_project_volume);
    assert.deepEqual(publishedCross.seed.child_private_volume, crossReport.request.preparation.child_private_volume);
    const crossClaim = await active.command(`/claim ${crossTask}`,
      /worker-claim task=([0-9a-f-]+) lease-json-hex=([0-9a-f]+)/);
    assert.equal(crossClaim[1], crossTask);
    const crossLeaseBytes = Buffer.from(crossClaim[2], "hex");
    const crossLease = JSON.parse(crossLeaseBytes.toString("utf8"));
    const crossLeasePath = join(temporary, "cross-source-original-lease.json");
    writeFileSync(crossLeasePath, crossLeaseBytes);
    const crossWorkspace = await active.commandJson(`/read-path ${input("cross-source-project-head", {
      caller: crossTask, lease: crossLease,
      volume: crossProject.revision.reference.volume, path: manifest.files.root_project.path,
    })}`, "file");
    assert.equal(crossWorkspace.text, selectedForeign.text,
      "actual independent child imports the exact selected foreign ROOT generation");
    assert.notEqual(crossWorkspace.text, changedRead.text, "foreign project selection never substitutes caller project");
    const prefixFile = publishedCross.seed.inherited_context[0];
    const inheritedPrefix = await active.commandJson(`/file ${input("cross-source-caller-prefix", {
      caller: crossTask, lease: crossLease, file: prefixFile,
    })}`, "file");
    const prefix = JSON.parse(inheritedPrefix.text);
    assert.deepEqual(prefix.parent, manifest.callers.left.authority);
    assert.equal(prefix.parent_agent, manifest.callers.left.agent);
    assert.equal(prefix.through_sequence, publishedCross.seed.inherited_through_sequence);
    const inherited = await active.commandJson(`/inherited-history ${input("cross-source-caller-messages", {
      caller: crossTask, lease: crossLease, seed: publishedCross.seed,
      after_sequence: retained.left.seed.inherited_through_sequence,
    })}`, "inherited-history");
    assert.ok(inherited.messages.length > 0 && inherited.messages.length <= 16);
    assert.ok(inherited.messages.some(value => isDeepStrictEqual(value.content, manifest.files.left_transcript)),
    "actual inherited message references retain the caller LEFT transcript, not the selected ROOT transcript alone");
    for (const label of ["root", "left"]) {
      const retainedScratch = await active.commandJson(`/file ${input(`scratch-retained-${label}`, {
        caller: manifest.tasks[label], lease: leases[label], file: manifest.files[`${label}_scratch`],
      })}`, "file");
      assert.equal(retainedScratch.text, callerContent[`${label}_scratch`],
        "cross-source project import preserves original callers' private scratch");
    }
    const nativeFacts = manifest.native_publication;
    const nativeRequest = load(manifest.native_publication_request);
    assert.equal(nativeRequest.caller, root);
    assert.deepEqual(nativeRequest.caller_lease, leases.root);
    const nativeBefore = digest(nativeFacts.conflicting_edit_path);
    const nativeConcurrentBefore = digest(nativeFacts.concurrent_edit_path);
    const imported = await active.commandJson(`/file ${input("native-imported-baseline", {
      caller: manifest.tasks.bootstrap_seed, lease: load(manifest.leases.bootstrap_seed),
      file: manifest.files.native_import_baseline,
    })}`, "file");
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), imported.text,
      "genuine Fs import pins the actual original native root bytes");
    const pendingPublication = await active.commandJson(`/publication-open ${manifest.native_publication_request}`, "publication");
    assert.equal(pendingPublication.status, "pending");
    assert.equal(pendingPublication.publication.journal.phase, "Prepared");
    assert.equal(pendingPublication.operation_id, nativeRequest.operation_id);
    assert.equal(pendingPublication.caller, root);
    assert.deepEqual(pendingPublication.before.project, manifest.files.native_import_baseline.volume);
    assert.deepEqual(pendingPublication.proposed.project, manifest.files.root_project.volume);
    assert.deepEqual(pendingPublication.source_root_identity, nativeFacts.source_root_identity);
    assert.equal(pendingPublication.interaction.approval.operation_id, nativeRequest.operation_id);
    assert.deepEqual(pendingPublication.interaction.approval.action_digest, pendingPublication.action_digest);
    assert.equal(pendingPublication.action_digest.length, 32);
    assert.equal(digest(nativeFacts.conflicting_edit_path), nativeBefore, "preparing/displaying an exact approval cannot write native root");
    assert.equal(digest(nativeFacts.concurrent_edit_path), nativeConcurrentBefore);

    const negative = load(manifest.negative_control);
    assert.equal(negative.session_limits.total_tasks, 11, "negative store has exactly one below the full actual admission count");
    console.log(`negative_native_scenario_sha256=${digest(manifest.negative_control)}`);
    console.log(`negative_native_config_sha256=${digest(negative.config)}`);
    negativeActive = new InstalledSession("native-full-real-one-below-budget", negative.config);
    await negativeActive.expect(/actual local Harness runtime/);
    await queueMailbox(negativeActive, negative, "negative");
    const negativeCross = negative.preparations.find(value => value.label === "cross_source");
    const negativeCrossRequest = load(negativeCross.request);
    await negativeActive.command(`/prepare ${negativeCross.request}`, /queued preparation-task=/);
    const negativeCrossStart = negativeActive.stdout.length;
    await negativeActive.command(`/run ${negativeCross.task}`, /worker-claim task=[^\n]+ lease-json-hex=[0-9a-f]+/);
    await negativeActive.expect(new RegExp(`worker completed task=${negativeCross.task}`), "stdout", negativeCrossStart);
    const negativePrepared = await negativeActive.commandJson(`/outcome ${negativeCross.task}`, "task");
    assert.ok(negativePrepared.outcome.Succeeded, "task eleven genuinely prepares before task twelve is refused");
    const negativePublishPath = input("negative-cross-source-publish", {
      caller: negative.tasks.left, lease: load(negative.leases.left), preparation: negativeCross.task,
    });
    await negativeActive.quit();
    const budgetRejection = run("native-full-real-twelfth-admission-negative",
      ["--session", negative.config, "--headless"], `/publish-fork ${negativePublishPath}\n`, true, 15_000);
    const rejectedLine = /^fork-json=(.+)\r?\n/m.exec(budgetRejection.stdout);
    assert.ok(rejectedLine, "real owner must return its canonical prepared child admission");
    const rejectedAdmission = JSON.parse(rejectedLine[1]);
    assert.equal(rejectedAdmission.child_admission_operation, negativeCrossRequest.request.child.operation_id);
    assert.match(rejectedAdmission.admission.Rejected.reason, /budget|limit|allowance|total/i);
    negativeActive = new InstalledSession("native-full-retained-negative-controls", negative.config);
    await negativeActive.expect(/actual local Harness runtime/);
    await negativeActive.command(`/reconcile ${negativeCrossRequest.request.child.operation_id}`, /retained=None/);
    const negativePending = await negativeActive.commandJson(`/publication-open ${negative.native_publication_request}`, "publication");
    assert.equal(negativePending.status, "pending");
    assert.notEqual(negativePending.interaction.id, pendingPublication.interaction.id);
    assert.match(negativePending.approved_frame_hex, /^(?:[0-9a-f]{2})+$/);
    const foreignApproval = join(temporary, "genuine-other-ticket-approval.pb");
    writeFileSync(foreignApproval, Buffer.from(negativePending.approved_frame_hex, "hex"));
    assert.ok(statSync(foreignApproval).size <= 65_536);
    console.log(`foreign_approval_sha256=${digest(foreignApproval)}`);
    await active.quit();
    const unapprovedWrite = run("native-full-writeback-without-approval-negative",
      ["--session", manifest.config, "--headless"], `/publication-apply ${manifest.native_publication_request}\n`,
      false, 15_000);
    assert.match(unapprovedWrite.stderr, /approv|interaction|unauthorized|resolution/i);
    const mismatchedApproval = run("native-full-exact-other-ticket-approval-negative",
      ["--session", manifest.config, "--headless"], `/approve ${foreignApproval}\n`, false, 15_000);
    assert.match(mismatchedApproval.stderr, /generation|ticket|interaction|scope|grant|not found|unauthorized|conflict/i);
    assert.equal(digest(nativeFacts.conflicting_edit_path), nativeBefore);
    active = new InstalledSession("native-full-exact-approval-root-writeback", manifest.config);
    await active.expect(/actual local Harness runtime/);
    const stillPending = await active.commandJson(`/publication-observe ${manifest.native_publication_request}`, "publication");
    assert.equal(stillPending.status, "pending", "foreign valid responder cannot resolve this original ticket");
    assert.deepEqual(stillPending.action_digest, pendingPublication.action_digest);
    assert.match(pendingPublication.approved_frame_hex, /^(?:[0-9a-f]{2})+$/);
    const approval = join(temporary, "exact-original-root-approval.pb");
    writeFileSync(approval, Buffer.from(pendingPublication.approved_frame_hex, "hex"));
    assert.ok(statSync(approval).size <= 65_536);
    console.log(`root_approval_sha256=${digest(approval)}`);
    const decision = await active.command(`/approve ${approval}`, /Admission\([^\n]+\n/);
    assert.match(decision[0], /Accepted/);
    const approved = await active.commandJson(`/publication-observe ${manifest.native_publication_request}`, "publication");
    assert.equal(approved.status, "approved");
    assert.equal(approved.resolution.outcome.kind, "approved");
    assert.equal(approved.resolution.id, pendingPublication.interaction.id);
    assert.deepEqual(approved.action_digest, pendingPublication.action_digest);
    assert.equal(digest(nativeFacts.conflicting_edit_path), nativeBefore,
      "exact accepted approval still does not automatically dispatch root writeback");
    const concurrentEdit = `actual concurrent native edit ${randomUUID()}\n`;
    writeFileSync(nativeFacts.concurrent_edit_path, concurrentEdit);
    const concurrentEditHash = digest(nativeFacts.concurrent_edit_path);
    const applied = await active.commandJson(`/publication-apply ${manifest.native_publication_request}`, "publication");
    assert.equal(applied.status, "applied");
    assert.equal(applied.publication.journal.phase, "Applied");
    assert.ok(applied.publication.work && !applied.failure, "real measured SDK publication receipt");
    assert.deepEqual(applied.action_digest, pendingPublication.action_digest);
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), before.root,
      "approved logical ROOT generation, not child or selected foreign source, writes back to actual native root");
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash,
      "an unrelated real native edit made after captured preparation is preserved");
    console.log(JSON.stringify({ evidence: "actual native publication", operation: applied.operation_id,
      original_root_sha256: nativeBefore, applied_root_sha256: digest(nativeFacts.conflicting_edit_path),
      concurrent_native_sha256: concurrentEditHash }));
    const originalPublication = JSON.stringify(applied.publication.journal);
    const observedPublication = await active.commandJson(`/publication-reconcile ${manifest.native_publication_request}`, "publication");
    assert.equal(observedPublication.status, "applied");
    assert.equal(JSON.stringify(observedPublication.publication.journal), originalPublication,
      "same original publication reconciliation cannot create another plan or resend an applied write");

    const negativeApproval = await negativeActive.command(`/approve ${foreignApproval}`, /Admission\([^\n]+\n/);
    assert.match(negativeApproval[0], /Accepted/);
    const overlappingEdit = `actual overlapping native edit ${randomUUID()}\n`;
    writeFileSync(negative.native_publication.conflicting_edit_path, overlappingEdit);
    const overlappingHash = digest(negative.native_publication.conflicting_edit_path);
    const refusedOverwrite = await negativeActive.commandJson(`/publication-apply ${negative.native_publication_request}`, "publication");
    assert.equal(refusedOverwrite.status, "failed", "captured original preimage must refuse a later overlapping native edit");
    assert.ok(refusedOverwrite.failure?.work, "conflict carries the SDK's actual measured failure receipt");
    assert.match(refusedOverwrite.failure.message, /conflict|diverg|preimage|changed|concurrent/i);
    assert.equal(digest(negative.native_publication.conflicting_edit_path), overlappingHash,
      "approval cannot overwrite an overlapping real native edit after preparation");
    await negativeActive.quit();
    // The same reference read under another task's retained lease must fail,
    // not borrow the broader native host configuration's owner authority.
    await active.quit();
    const mismatchedOwner = run("native-full-original-lease-owner-negative",
      ["--session", manifest.config, "--headless"],
      `/read ${input("mismatched-original-owner", {
        caller: left, lease: leases.root, file: manifest.files.left_project,
      })}\n`, false, 15_000);
    assert.match(mismatchedOwner.stderr, /owner|lease|fence|task|unauthorized/i);
    const invalidChildCursor = run("native-full-child-cursor-negative",
      ["--session", manifest.config, "--headless"],
      `/children ${input("invalid-child-cursor", { task: root, revision: null, after: "unobserved-slot" })}\n`,
      false, 15_000);
    assert.match(invalidChildCursor.stderr, /cursor|continuation|revision/i);
    const changedMail = run("native-full-exact-mail-identity-negative",
      ["--session", manifest.config, "--headless"],
      `/mail ${input("changed-mail-same-operation", {
        ...sends[0].request, payload: manifest.files.root_project_before,
      })}\n`, false, 15_000);
    assert.match(changedMail.stderr, /conflict|differ|changed|identity/i);
    const foreignScratch = run("native-full-inspection-never-adopts-source-authority",
      ["--session", manifest.config, "--headless"],
      `/read ${input("foreign-private-with-caller-lease", {
        caller: left, lease: leases.left, file: manifest.files.root_scratch,
      })}\n`, false, 15_000);
    assert.match(foreignScratch.stderr, /unauthorized|grant|scope|access|reader|binding/i);
    const emptyScratch = run("native-full-new-child-scratch-remains-empty",
      ["--session", manifest.config, "--headless"],
      `/read-path ${input("cross-source-empty-own-scratch", {
        caller: crossTask, lease: crossLease, volume: publishedCross.seed.child_private_volume,
        path: manifest.files.left_scratch.path,
      })}\n`, false, 15_000);
    assert.match(emptyScratch.stderr, /not found|missing|does not exist/i);
    const fileBound = run("native-full-file-staging-bound-negative",
      ["--session", manifest.config, "--headless"],
      `/write ${input("oversized-owned-file", {
        caller: root, lease: leases.root, volume: manifest.callers.root.project,
        operation: randomUUID(), path: "installed/oversized.txt", bytes: "x".repeat(32_769),
      })}\n`, false, 15_000);
    assert.match(fileBound.stderr, /file staging exceeds terminal bounds/);

    active = new InstalledSession("native-full-child-completion-and-original-root-service-fault", manifest.config);
    await active.expect(/actual local Harness runtime/);
    const outcomes = new Map([[mailbox.task, mailboxOutcome]]);
    for (const [task, leasePath] of [
      [left, manifest.leases.left], [right, manifest.leases.right],
      [grandchild, manifest.leases.grandchild], [crossTask, crossLeasePath],
    ]) {
      const started = active.stdout.length;
      await active.command(`/resume ${leasePath}`, /original lease submitted for authoritative reconciliation/);
      await active.expect(new RegExp(`worker completed task=${task}`), "stdout", started);
      const outcome = await active.commandJson(`/outcome ${task}`, "task");
      assert.ok(outcome.outcome.Succeeded, "actual admitted descendant model task must complete");
      assert.match(outcome.outcome.Succeeded.text, /^deterministic request=[0-9a-f]+ step=0$/);
      outcomes.set(task, outcome);
    }
    const rootBeforeFault = await active.commandJson(`/outcome ${root}`, "task");
    assert.equal(rootBeforeFault.outcome, null, "fault uses the original still-live root admission, never an extra task");
    const rootLeaseBytes = readFileSync(manifest.leases.root);
    const rootLeaseHash = digest(manifest.leases.root);
    console.log(`root_original_lease_sha256=${rootLeaseHash}`);
    const rootDispatchStart = active.stderr.length;
    const rootStart = active.stdout.length;
    await active.command(`/resume ${manifest.leases.root}`, /original lease submitted for authoritative reconciliation/);
    const rootDispatch = await active.expect(
      /deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]+)/,
      "stderr", rootDispatchStart);
    assert.ok(!active.stdout.slice(rootStart).includes(`worker completed task=${root}`),
      "service fault must interrupt a real root dispatch, not kill an already completed worker");
    assert.equal(active.finished, null);
    const killed = await active.stop();
    if (active.failure) throw active.failure;
    assert.equal(active.requestedKill, true, "OS must accept the explicit installed service kill, not an unrelated launch or natural-exit failure");
    assert.ok(killed.signal !== null || killed.code !== 0, "actual installed service process must die");
    assert.ok(!active.stdout.slice(rootStart).includes(`worker completed task=${root}`));
    assert.equal(digest(manifest.leases.root), rootLeaseHash);
    assert.deepEqual(readFileSync(manifest.leases.root), rootLeaseBytes,
      "resume retains exact original lease bytes, not a new reservation or UI recreation");

    active = new InstalledSession("native-full-original-root-lease-and-publication-recovery", manifest.config);
    await active.expect(/actual local Harness runtime/);
    await active.command(`/reconcile ${retained.root.child.operation_id}`, /retained=Some/);
    const recoveredPublication = await active.commandJson(`/publication-reconcile ${manifest.native_publication_request}`, "publication");
    assert.equal(recoveredPublication.status, "applied");
    assert.equal(JSON.stringify(recoveredPublication.publication.journal), originalPublication,
      "process death cannot re-plan or repeat original approved native publication");
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), before.root);
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash);
    const rootResumeStart = active.stdout.length;
    await active.command(`/resume ${manifest.leases.root}`, /original lease submitted for authoritative reconciliation/);
    await active.expect(new RegExp(`worker completed task=${root}`), "stdout", rootResumeStart);
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/,
      "original retained model attempt must reconcile without generating another request");
    const rootOutcome = await active.commandJson(`/outcome ${root}`, "task");
    assert.ok(rootOutcome.outcome.Succeeded.text.includes(rootDispatch[2]),
      "resumed root completion binds the exact request observed before actual service death");
    outcomes.set(root, rootOutcome);
    const bootstrapStart = active.stdout.length;
    await active.command(`/resume ${manifest.leases.bootstrap_seed}`, /original lease submitted for authoritative reconciliation/);
    await active.expect(new RegExp(`worker completed task=${manifest.tasks.bootstrap_seed}`), "stdout", bootstrapStart);
    const bootstrapOutcome = await active.commandJson(`/outcome ${manifest.tasks.bootstrap_seed}`, "task");
    assert.ok(bootstrapOutcome.outcome.Succeeded,
      "the disclosed genuine bootstrap task also completes; no retained helper worker is left running");
    outcomes.set(manifest.tasks.bootstrap_seed, bootstrapOutcome);
    for (const preparation of manifest.preparations) {
      const prepared = await active.commandJson(`/outcome ${preparation.task}`, "task");
      assert.ok(prepared.outcome.Succeeded, "all five actual ordinary preparation tasks remain authoritatively completed");
      outcomes.set(preparation.task, prepared);
    }
    assert.equal(outcomes.size, 12, "all twelve actual admissions have authoritative terminal outcomes");
    await active.command("/reopen", /providers reopened; no admission or effect resent/);
    for (const [task, original] of outcomes) {
      const reopened = await active.commandJson(`/outcome ${task}`, "task");
      assert.deepEqual(reopened, original);
      await active.command(`/run ${task}`, /worker idle task=/);
    }
    await active.quit();
    active = new InstalledSession("native-full-cold-reopen-retains-exact-completed-graph", manifest.config);
    await active.expect(/actual local Harness runtime/);
    for (const [task, original] of outcomes) {
      assert.deepEqual(await active.commandJson(`/outcome ${task}`, "task"), original);
      await active.command(`/run ${task}`, /worker idle task=/);
    }
    assert.doesNotMatch(active.stderr, /deterministic-dispatch/);
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), before.root);
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash);
    assert.equal(digest(negative.native_publication.conflicting_edit_path), overlappingHash);
    await active.quit();
    console.log(JSON.stringify({ case: "native-full-installed-qualified", evidence: "actual pipes, not PTY",
      logical_agent_tasks: [root, left, right, grandchild, crossTask],
      ordinary_mailbox_task: mailbox.task, retained_root_lease_sha256: rootLeaseHash }));
  } finally {
    if (active) await active.stop();
    if (negativeActive) await negativeActive.stop();
    rmSync(temporary, { recursive: true, force: true });
  }
}
