#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { tmpdir } from "node:os";
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
  else if (!session && option && !option.startsWith("--")) session = option;
  else throw new Error(`unexpected argument: ${option}`);
}
if (!binary || !isAbsolute(binary) || !statSync(binary).isFile()
    || (session && (!isAbsolute(session) || !statSync(session).isFile() || statSync(session).size > 1_048_576))
    || (scenarioDirectory !== undefined && !isAbsolute(scenarioDirectory))) {
  throw new Error("usage: node scripts/test-graphcoder.mjs ABSOLUTE_INSTALLED_BINARY [ABSOLUTE_NATIVE_CONFIG] [--full ABSOLUTE_SCENARIO_DIR]");
}
const digest = path => createHash("sha256").update(readFileSync(path)).digest("hex");
function invocationLedgerPath(facts, configuration) {
  if (typeof facts.model_invocation_ledger !== "string" || !isAbsolute(facts.model_invocation_ledger)
      || configuration.model_invocation_ledger !== facts.model_invocation_ledger) {
    throw new Error("qualification requires matching absolute provider invocation ledger paths");
  }
  try {
    const info = statSync(facts.model_invocation_ledger);
    assert.ok(info.isFile() && info.size <= 1_048_576);
  } catch (cause) {
    throw new Error("provider invocation ledger unavailable",
      { cause });
  }
  return facts.model_invocation_ledger;
}
function verifyInvocations(bytes, expected, requiredReconciles = []) {
  assert.ok(Buffer.isBuffer(bytes) && bytes.length <= 1_048_576);
  const text = bytes.toString("utf8");
  assert.ok(!text || text.endsWith("\n"));
  const lines = text ? text.slice(0, -1).split("\n") : [];
  assert.ok(lines.length <= 128);
  const generates = new Set();
  const reconciles = new Set();
  for (const line of lines) {
    const row = JSON.parse(line);
    const original = expected.get(row.operation_id);
    assert.ok(original);
    assert.equal(row.step, original.step, "observer step differs from original dispatch");
    assert.equal(row.request_digest, original.request_digest, "observer request differs from original dispatch");
    if (row.phase === "generate") {
      assert.ok(!generates.has(row.operation_id),
        "duplicate generate: identical request and identical output still represent another actual invocation");
      generates.add(row.operation_id);
    } else {
      assert.ok(generates.has(row.operation_id));
      reconciles.add(row.operation_id);
    }
  }
  assert.equal(generates.size, expected.size, "exactly one genuine generate per dispatched model task");
  for (const operation of requiredReconciles) {
    assert.ok(reconciles.has(operation), "original interrupted attempt must have an actual provider reconcile invocation");
  }
}
const env = { PATH: "" };
for (const key of ["SystemRoot", "WINDIR", "TEMP", "TMP"]) {
  if (process.env[key] !== undefined) env[key] = process.env[key];
}
function run(args, input, success, timeout = 5_000) {
  const result = spawnSync(binary, args, {
    input, encoding: "utf8", env, windowsHide: true,
    timeout, killSignal: "SIGKILL", maxBuffer: 1_048_576,
  });
  if (result.error) throw result.error;
  assert.equal(result.signal, null);
  assert.notEqual(result.status, null);
  assert.equal(result.status === 0, success);
  return result;
}
run([], "", false);
const args = ["--fixture", "wire", "--headless"];
run(args, "hello\n/next\n", false);
run(args, "/next\n/next\n/reset\n/next\n/quit\n", true);
run(args, "x".repeat(65_537) + "\n/next\n", false);
  class InstalledSession {
    constructor(configuration) {
      this.stdout = "";
      this.stderr = "";
      this.waiters = new Set();
      this.failure = null;
      this.finished = null;
      this.requestedKill = false;
      this.capturedBytes = 0;
      this.child = spawn(binary, ["--session", configuration, "--headless"], {
        env, windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
      });
      this.child.stdout.setEncoding("utf8");
      this.child.stderr.setEncoding("utf8");
      const capture = (stream, chunk) => {
        this.capturedBytes += Buffer.byteLength(chunk);
        if (this.capturedBytes > 1_048_576) {
          this.failure = new Error("captured output exceeds bounds");
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
          this.notify();
          resolve(this.finished);
        });
      });
      this.deadline = setTimeout(() => {
        this.failure = new Error("installed process deadline exceeded");
        this.child.kill("SIGKILL");
        this.notify();
      }, 60_000);
    }
    notify() { for (const waiter of this.waiters) waiter(); }
    expect(pattern, stream = "stdout", from = 0) {
      return new Promise((resolve, reject) => {
        const check = () => {
          if (this.failure) { this.waiters.delete(check); reject(this.failure); return; }
          const match = pattern.exec(this[stream].slice(from));
          if (match) { this.waiters.delete(check); resolve(match); return; }
          if (this.finished) {
            this.waiters.delete(check);
            reject(new Error(`exited before ${pattern}`));
          }
        };
        this.waiters.add(check);
        check();
      });
    }
    command(line, pattern) {
      assert.ok(Buffer.byteLength(line) <= 65_536 && !/[\r\n]/.test(line));
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
  assert.ok(config.deterministic_delay_ms >= 1000 && config.deterministic_delay_ms <= 5000);
  const temporary = mkdtempSync(join(tmpdir(), "graphcoder-installed-"));

  let active;
  try {
    active = new InstalledSession(session);
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
    assert.equal(new Set(tasks).size, 4);
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
    await active.expect(/deterministic-dispatch operation=[0-9a-f-]+ step=0 request=[0-9a-f]+/, "stderr");
    await active.command(`/cancel-task ${right[1]}`, /cancellation requested task=/);
    await active.command(`/outcome ${right[1]}`, /outcome=Some\(Cancelled\)/);
    await active.stop();
    if (active.failure) throw active.failure;
    assert.ok(!active.stdout.includes(`worker completed task=${root[1]}`));
    const lease = join(temporary, "original-lease.json");
    const originalLease = Buffer.from(claim[1], "hex");
    JSON.parse(originalLease.toString("utf8"));
    writeFileSync(lease, originalLease);

    active = new InstalledSession(session);
    await active.expect(/actual local Harness runtime/);
    await active.command(`/reconcile ${root[2]}`, /retained=Some/);
    await active.command(`/resume ${lease}`, /worker completed task=/);
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

    active = new InstalledSession(session);
    await active.expect(/actual local Harness runtime/);
    for (let index = 0; index < tasks.length; index++) {
      const after = await active.command(`/outcome ${tasks[index]}`, /outcome=Some\([^\n]+\n/);
      assert.equal(after[0], outcomes[index]);
      await active.command(`/run ${tasks[index]}`, /worker idle task=/);
    }
  } finally {
    if (active) await active.stop();
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (scenarioDirectory) {
  try {
    run(["--prepare-scenario", scenarioDirectory], "", true, 120_000);
  } catch (error) {
    throw new Error("full installed scenario prerequisite failed: real native scenario producer must be installed and available",
      { cause: error });
  }
  const manifestPath = join(scenarioDirectory, "native-scenario.json");
  assert.ok(statSync(manifestPath).isFile() && statSync(manifestPath).size <= 1_048_576);
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  if (manifest.missing_prerequisites.length) {
    throw new Error(`full installed scenario prerequisites unavailable: ${manifest.missing_prerequisites.join("; ")}`);
  }
  const fullConfig = JSON.parse(readFileSync(manifest.config, "utf8"));
  assert.ok(fullConfig.deterministic_delay_ms >= 1000 && fullConfig.deterministic_delay_ms <= 5000);
  const ledgerPath = invocationLedgerPath(manifest, fullConfig);
  const observedModels = new Map();
  let retainedLedgerBytes = Buffer.alloc(0);
  const snapshotInvocations = (requiredReconciles = []) => {
    const info = statSync(ledgerPath);
    assert.ok(info.isFile() && info.size <= 1_048_576);
    const bytes = readFileSync(ledgerPath);
    assert.deepEqual(bytes.subarray(0, retainedLedgerBytes.length), retainedLedgerBytes);
    verifyInvocations(bytes, observedModels, requiredReconciles);
    retainedLedgerBytes = bytes;
    return bytes;
  };
  snapshotInvocations();
  const temporary = mkdtempSync(join(tmpdir(), "graphcoder-full-installed-"));
  let active;
  let negativeActive;
  let inputSequence = 0;
  const input = value => {
    const bytes = JSON.stringify(value);
    assert.ok(Buffer.byteLength(bytes) <= 65_536);
    const path = join(temporary, `${++inputSequence}.json`);
    writeFileSync(path, bytes);
    return path;
  };
  const load = path => {
    assert.ok(isAbsolute(path) && statSync(path).isFile() && statSync(path).size <= 1_048_576);
    return JSON.parse(readFileSync(path, "utf8"));
  };
  const bindDispatch = (command, barrier, transition) => {
    assert.ok(transition?.commands.some(value => isDeepStrictEqual(value, command)));
    const execution = barrier[1];
    assert.ok(!observedModels.has(execution));
    observedModels.set(execution, { step: 0, request_digest: barrier[2] });
  };
  const modelCheckpoint = async (client, task, lease) => {
    const checkpoint = await client.commandJson(`/checkpoint ${input({ caller: task, lease })}`, "checkpoint");
    assert.equal(checkpoint.task, task);
    return checkpoint;
  };
  const restart = async () => {
    const ledger = snapshotInvocations();
    const stopped = await active.stop();
    if (active.failure) throw active.failure;
    assert.ok(active.requestedKill && (stopped.signal !== null || stopped.code !== 0));
    active = new InstalledSession(manifest.config);
    await active.expect(/actual local Harness runtime/);
    assert.deepEqual(snapshotInvocations(), ledger);
  };
  const queueMailbox = async (client, facts) => {
    const rootLease = load(facts.leases.root);
    const receiveOperation = randomUUID();
    const modelOperation = randomUUID();
    const receive = await client.commandJson(`/write ${input({
      caller: facts.tasks.root, lease: rootLease, volume: facts.callers.root.private_volume,
      operation: receiveOperation, path: "installed/receive.json", bytes: JSON.stringify({ after: 0 }),
    })}`, "file");
    const model = await client.commandJson(`/write ${input({
      caller: facts.tasks.root, lease: rootLease, volume: facts.callers.root.private_volume,
      operation: modelOperation, path: "installed/model.json",
      bytes: JSON.stringify({ input: "actual installed mailbox steering",
        selected_context: null, max_steps: 1, max_output_tokens: 256, mailbox_operation: receiveOperation }),
    })}`, "file");
    const operation = randomUUID();
    const admitted = await client.command(`/queue-mailbox ${input({
      operation, parent: facts.tasks.root, lease: rootLease, input: {
        receive: { operation_id: receiveOperation, kind: "acyclic.mail.receive.v1", payload: receive.file },
        model: { operation_id: modelOperation, kind: "acyclic.model.v1", payload: model.file },
      },
    })}`, /queued task=([0-9a-f-]+) admission-operation=([0-9a-f-]+)/);
    assert.equal(admitted[2], operation);
    assert.notEqual(admitted[1], facts.tasks.root);
    return { task: admitted[1], operation, receiveOperation, modelOperation,
      modelCommand: { operation_id: modelOperation, kind: "acyclic.model.v1", payload: model.file } };
  };
  try {
    const { root, left, right, grandchild } = manifest.tasks;
    const retained = Object.fromEntries(manifest.preparations.filter(value => value.executed)
      .map(value => [value.label, load(value.report)]));
    active = new InstalledSession(manifest.config);
    await active.expect(/actual local Harness runtime/);
    const leases = Object.fromEntries(["root", "left", "right", "grandchild"].map(label =>
      [label, load(manifest.leases[label])]));
    const before = {};
    for (const label of ["root", "left", "right", "grandchild"]) {
      before[label] = (await active.commandJson(`/read ${input({
        caller: manifest.tasks[label], lease: leases[label], file: manifest.files[`${label}_project`],
      })}`, "file")).text;
    }
    const command = { caller: manifest.native_command.caller, lease: manifest.native_command.lease,
      operation: manifest.native_command.operation };
    const commandRequest = input(command);
    run(["--session", manifest.config, "--headless"], `/command ${commandRequest}\n`, false, 15_000);
    run(["--session", manifest.config, "--headless"], `/command ${input({
      ...command, caller: left, lease: leases.left,
    })}\n`, false, 15_000);
    await active.commandJson(`/command ${input({
      ...command, decision: true,
    })}`, "command-approval");
    const commandResult = await active.commandJson(`/command ${commandRequest}`, "command");
    assert.equal(commandResult.state, "succeeded");
    const capturedCommand = JSON.parse((await active.commandJson(`/read ${input({
      caller: right, lease: leases.right, file: commandResult.result,
    })}`, "file")).text);
    assert.equal(capturedCommand.exit_code, 0);
    assert.ok(Buffer.from(capturedCommand.stdout).toString("utf8").includes("graphcoder-approved-command"));
    await active.command("/reopen", /providers reopened/);
    assert.deepEqual(await active.commandJson(`/command ${commandRequest}`, "command"), commandResult);
    assert.equal((await active.commandJson(`/read ${input({
      caller: right, lease: leases.right, file: manifest.files.right_project,
    })}`, "file")).text, before.right);

    const discoveryVolume = manifest.callers.bootstrap_seed.project;
    const discoveryWrite = async (path, bytes) => active.commandJson(`/write ${input({
      caller: manifest.tasks.bootstrap_seed, lease: load(manifest.leases.bootstrap_seed),
      volume: discoveryVolume, operation: randomUUID(), path, bytes,
    })}`, "file");
    await discoveryWrite("AGENTS.md", "workflow repository instructions");
    await discoveryWrite("skills/review/SKILL.md",
      "---\nname: review\ndescription: Review the selected change\n---\nLAZY SKILL BODY");
    const workflow = {
      discovery: { instructions: [{ root: { volume: discoveryVolume, directory: "" }, active_directory: "" }],
        skills: [{ volume: discoveryVolume, directory: "skills" }],
        policy: { instruction_names: ["AGENTS.md"], skill_name: "SKILL.md" },
        limits: { entries: 64, directories: 8, header_bytes: 256, instruction_bytes: 512 } },
      attributes: [{ type_name: "graphcoder.role", type_revision: "1", schema: { type: "string" },
        state_revision: "reviewer-1", value: "reviewer" }], attribute_mode: "prompt",
    };
    const initialContext = await active.commandJson(`/context ${input(workflow)}`, "context");
    assert.equal(initialContext.discovered.skills[0].description, "Review the selected change");
    assert.equal(initialContext.discovered.instructions.length, 1);
    assert.ok(!JSON.stringify(initialContext).includes("LAZY SKILL BODY"));
    const oldInstructions = initialContext.discovered.instructions;
    await discoveryWrite("AGENTS.md", "reloaded workflow instructions");
    const updatedWorkflow = { ...workflow, attribute_mode: "update", attributes: [
      { ...workflow.attributes[0], state_revision: "implementor-2", value: "implementor" },
    ] };
    const updatedContext = await active.commandJson(`/context ${input(updatedWorkflow)}`, "context");
    assert.notDeepEqual(updatedContext.discovered.instructions, oldInstructions);
    assert.deepEqual(initialContext.discovered.instructions, oldInstructions);
    assert.equal(updatedContext.attributes[0].value, "implementor");
    const originalFork = fullConfig.fork_sources.find(value => value.caller === root);
    for (const history of ["fork", "fresh"]) {
      const selection = await active.commandJson(`/fork-selection ${input({
        caller: root, lease: leases.root, operation: originalFork.request.operation_id, history,
      })}`, "fork-selection");
      assert.equal(selection.parent_revision, originalFork.snapshot.revision);
      if (history === "fresh") assert.equal(selection.preparation.inherited_through_sequence, 0);
      else assert.ok(selection.preparation.inherited_through_sequence > 0);
      assert.equal(selection.child_agent, originalFork.request.child_agent);
    }
    run(["--session", manifest.config, "--headless"], `/fork-selection ${input({
      caller: left, lease: leases.left, operation: originalFork.request.operation_id, history: "fresh",
    })}\n`, false, 15_000);
    const mutation = `installed-left-project-${randomUUID()}`;
    const changed = await active.commandJson(`/write ${input({
      caller: left, lease: leases.left, volume: retained.left.report.request.preparation.child_project_volume,
      operation: randomUUID(), path: manifest.files.left_project.path, bytes: mutation,
    })}`, "file");
    assert.deepEqual(changed.file.volume, manifest.files.left_project.volume);
    assert.notEqual(changed.file.version, manifest.files.left_project.version);
    const changedRead = await active.commandJson(`/read ${input({
      caller: left, lease: leases.left, file: changed.file,
    })}`, "file");
    assert.equal(changedRead.text, mutation);
    const writeRequest = input({
      caller: left, lease: leases.left, volume: changed.file.volume, operation: randomUUID(),
      path: "ack-loss.txt", bytes: "original tool write",
    });
    await active.commandJson(`/write ${writeRequest}`, "file");
    await restart();
    const concurrentWrite = await active.commandJson(`/write ${input({
      ...load(writeRequest), operation: randomUUID(), bytes: "concurrent destination edit",
    })}`, "file");
    await active.commandJson(`/write ${writeRequest}`, "file");
    assert.equal((await active.commandJson(`/read-path ${input({
      caller: left, lease: leases.left, volume: concurrentWrite.file.volume, path: concurrentWrite.file.path,
    })}`, "file")).text, "concurrent destination edit", "lost write acknowledgement must not mutate the destination twice");
    const sends = [];
    for (let index = 0; index < 17; index++) {
      const request = { caller: root, lease: leases.root, recipient: left, operation: randomUUID(), payload: manifest.files.root_project };
      const path = input(request);
      sends.push({ request, path, ack: await active.commandJson(`/mail ${path}`, "mail") });
    }
    assert.deepEqual(await active.commandJson(`/mail ${sends[0].path}`, "mail"), sends[0].ack);
    const inboxPath = input({ caller: left, lease: leases.left, after: 0 });
    const inboxFirst = await active.commandJson(`/inbox ${inboxPath}`, "inbox");
    assert.ok(inboxFirst.items.length <= 16);
    assert.deepEqual(await active.commandJson(`/inbox ${inboxPath}`, "inbox"), inboxFirst);
    const inboxLast = await active.commandJson(`/inbox ${input({
      caller: left, lease: leases.left, after: inboxFirst.next_after,
    })}`, "inbox");
    assert.deepEqual([...inboxFirst.items, ...inboxLast.items].map(value => value.message_id), sends.map(value => value.request.operation));
    const mailbox = await queueMailbox(active, manifest);
    const mailboxClaim = await active.command(`/claim ${mailbox.task}`, /worker-claim task=([0-9a-f-]+) lease-json-hex=([0-9a-f]+)/);
    const mailboxLeaseBytes = Buffer.from(mailboxClaim[2], "hex");
    const mailboxLease = JSON.parse(mailboxLeaseBytes.toString("utf8"));
    const mailboxLeasePath = join(temporary, "mailbox-original-lease.json");
    writeFileSync(mailboxLeasePath, mailboxLeaseBytes);
    const mailboxStart = active.stdout.length;
    await active.command(`/resume ${mailboxLeasePath}`, /original lease submitted for authoritative reconciliation/);
    await active.expect(new RegExp(`worker suspended task=${mailbox.task} checkpoint=`), "stdout", mailboxStart);
    await active.commandJson(`/mail ${input({
      caller: root, lease: leases.root, recipient: mailbox.task,
      operation: randomUUID(), payload: manifest.files.root_project_before,
    })}`, "mail");
    const dispatchStart = active.stderr.length, modelStart = active.stdout.length;
    await active.command(`/resume ${mailboxLeasePath}`, /original lease submitted for authoritative reconciliation/);
    const mailboxDispatch = await active.expect(/deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]+)/, "stderr", dispatchStart);
    const consumed = await modelCheckpoint(active, mailbox.task, mailboxLease);
    await active.commandJson(`/context ${input({
      ...updatedWorkflow, attributes: [{ ...updatedWorkflow.attributes[0],
        state_revision: "researcher-3", value: "researcher" }],
    })}`, "context");
    bindDispatch(mailbox.modelCommand, mailboxDispatch, consumed.transition);
    await active.commandJson(`/mail ${input({
      caller: root, lease: leases.root, recipient: mailbox.task, operation: randomUUID(), payload: manifest.files.root_project_after,
    })}`, "mail");
    const notReinterpreted = await modelCheckpoint(active, mailbox.task, mailboxLease);
    assert.deepEqual(notReinterpreted.checkpoint.state.mail, consumed.checkpoint.state.mail);
    await active.expect(new RegExp(`worker completed task=${mailbox.task}`), "stdout", modelStart);
    const mailboxOutcome = await active.commandJson(`/outcome ${mailbox.task}`, "task");
    assert.deepEqual(mailboxOutcome.outcome.Succeeded.mail, consumed.checkpoint.state.mail);
    assert.ok(mailboxOutcome.outcome.Succeeded.model.Succeeded.text.includes(mailboxDispatch[2]));
    await active.commandJson(`/context ${input({ attributes: [] })}`, "context");
    const pendingCross = manifest.preparations.find(value => value.label === "cross_source");
    assert.equal(pendingCross.executed, false);
    const crossInput = load(pendingCross.request);
    assert.equal(crossInput.parent, left);
    assert.deepEqual(crossInput.caller_lease, leases.left);
    assert.deepEqual(crossInput.request.payload.parent, manifest.callers.left.authority);
    const selectedProject = crossInput.request.payload.selections.find(value => value.revision.kind === "project");
    const selectedPrivate = crossInput.request.payload.selections.find(value => value.revision.kind === "private_volume");
    assert.deepEqual(selectedProject.revision.reference.volume, manifest.callers.root.project);
    assert.deepEqual(selectedPrivate.revision.reference.volume, manifest.callers.left.private_volume);
    const selectedForeign = await active.commandJson(`/read-path ${input({
      caller: root, lease: leases.root, volume: selectedProject.revision.reference.volume,
      path: manifest.files.root_project.path, generation: selectedProject.revision.reference.generation,
    })}`, "file");
    assert.notEqual(selectedForeign.file.version, manifest.files.root_project.version);
    const preparePattern = /queued preparation-task=([0-9a-f-]+) admission-operation=([0-9a-f-]+) caller-task=([0-9a-f-]+)/;
    const admittedPreparation = await active.command(`/prepare ${pendingCross.request}`, preparePattern);
    await restart();
    assert.equal((await active.command(`/prepare ${pendingCross.request}`, preparePattern))[0], admittedPreparation[0]);
    const crossStart = active.stdout.length;
    await active.command(`/run ${pendingCross.task}`, /worker-claim task=[^\n]+ lease-json-hex=[0-9a-f]+/);
    await active.expect(new RegExp(`worker completed task=${pendingCross.task}`), "stdout", crossStart);
    const preparedReport = await active.commandJson(`/outcome ${pendingCross.task}`, "task");
    assert.equal(preparedReport.task, pendingCross.task);
    assert.ok(preparedReport.outcome.Succeeded, "real creator preparation must authoritatively succeed");
    await restart();
    assert.deepEqual(await active.commandJson(`/outcome ${pendingCross.task}`, "task"), preparedReport);
    await active.command(`/run ${pendingCross.task}`, /worker idle task=/);
    const publishPath = input({
      caller: left, lease: leases.left, preparation: pendingCross.task,
    });
    const publishedCross = await active.commandJson(`/publish-fork ${publishPath}`, "fork");
    await restart();
    assert.deepEqual(await active.commandJson(`/publish-fork ${publishPath}`, "fork"), publishedCross);
    const crossTask = publishedCross.admission.Accepted;
    assert.ok(crossTask && !Object.values(manifest.tasks).includes(crossTask));
    const crossProject = publishedCross.seed.resources.find(value => value.source.kind === "project");
    const crossClaim = await active.command(`/claim ${crossTask}`,
      /worker-claim task=([0-9a-f-]+) lease-json-hex=([0-9a-f]+)/);
    const crossLeaseBytes = Buffer.from(crossClaim[2], "hex");
    const crossLease = JSON.parse(crossLeaseBytes.toString("utf8"));
    const crossLeasePath = join(temporary, "cross-source-original-lease.json");
    writeFileSync(crossLeasePath, crossLeaseBytes);
    await restart();
    await active.command("/reopen", /providers reopened; no admission or effect resent/);
    await active.command(`/reconcile ${publishedCross.child_admission_operation}`, /retained=Some/);
    assert.deepEqual(await active.commandJson(`/outcome ${pendingCross.task}`, "task"), preparedReport);
    const crossWorkspace = await active.commandJson(`/read-path ${input({
      caller: crossTask, lease: crossLease,
      volume: crossProject.revision.reference.volume, path: manifest.files.root_project.path,
    })}`, "file");
    assert.equal(crossWorkspace.text, selectedForeign.text);
    assert.notEqual(crossWorkspace.text, changedRead.text);
    const inherited = await active.commandJson(`/inherited-history ${input({
      caller: crossTask, lease: crossLease, seed: publishedCross.seed, after_sequence: retained.left.seed.inherited_through_sequence,
    })}`, "inherited-history");
    assert.ok(inherited.messages.some(value => isDeepStrictEqual(value.content, manifest.files.left_transcript)));
    const nativeFacts = manifest.native_publication;
    const nativeRequest = load(manifest.native_publication_request);
    const nativeBefore = digest(nativeFacts.conflicting_edit_path);
    const nativeConcurrentBefore = digest(nativeFacts.concurrent_edit_path);
    const imported = await active.commandJson(`/file ${input({
      caller: manifest.tasks.bootstrap_seed, lease: load(manifest.leases.bootstrap_seed),
      file: manifest.files.native_import_baseline,
    })}`, "file");
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), imported.text);
    const pendingPublication = await active.commandJson(`/publication-open ${manifest.native_publication_request}`, "publication");
    assert.equal(pendingPublication.operation_id, nativeRequest.operation_id);
    assert.equal(pendingPublication.caller, root);
    assert.equal(digest(nativeFacts.conflicting_edit_path), nativeBefore);
    assert.equal(digest(nativeFacts.concurrent_edit_path), nativeConcurrentBefore);

    const negative = load(manifest.negative_control);
    assert.equal(negative.session_limits.total_tasks, 11);
    const negativeLedgerPath = invocationLedgerPath(negative, load(negative.config));
    assert.notEqual(negativeLedgerPath, ledgerPath);
    const negativeLedgerBytes = readFileSync(negativeLedgerPath);
    verifyInvocations(negativeLedgerBytes, new Map());
    negativeActive = new InstalledSession(negative.config);
    await negativeActive.expect(/actual local Harness runtime/);
    await queueMailbox(negativeActive, negative);
    const negativeCross = negative.preparations.find(value => value.label === "cross_source");
    const negativeCrossRequest = load(negativeCross.request);
    await negativeActive.command(`/prepare ${negativeCross.request}`, /queued preparation-task=/);
    const negativeCrossStart = negativeActive.stdout.length;
    await negativeActive.command(`/run ${negativeCross.task}`, /worker-claim task=[^\n]+ lease-json-hex=[0-9a-f]+/);
    await negativeActive.expect(new RegExp(`worker completed task=${negativeCross.task}`), "stdout", negativeCrossStart);
    const negativePrepared = await negativeActive.commandJson(`/outcome ${negativeCross.task}`, "task");
    assert.ok(negativePrepared.outcome.Succeeded);
    const negativePublishPath = input({
      caller: negative.tasks.left, lease: load(negative.leases.left), preparation: negativeCross.task,
    });
    await negativeActive.quit();
    const budgetRejection = run(["--session", negative.config, "--headless"], `/publish-fork ${negativePublishPath}\n`, true, 15_000);
    const rejectedLine = /^fork-json=(.+)\r?\n/m.exec(budgetRejection.stdout);
    assert.ok(rejectedLine);
    const rejectedAdmission = JSON.parse(rejectedLine[1]);
    assert.equal(rejectedAdmission.child_admission_operation, negativeCrossRequest.request.child.operation_id);
    assert.match(rejectedAdmission.admission.Rejected.reason, /budget|limit|allowance|total/i);
    negativeActive = new InstalledSession(negative.config);
    await negativeActive.expect(/actual local Harness runtime/);
    await negativeActive.command(`/reconcile ${negativeCrossRequest.request.child.operation_id}`, /retained=None/);
    const negativePending = await negativeActive.commandJson(`/publication-open ${negative.native_publication_request}`, "publication");
    assert.equal(negativePending.status, "pending");
    assert.notEqual(negativePending.interaction.id, pendingPublication.interaction.id);
    const foreignApproval = join(temporary, "genuine-other-ticket-approval.pb");
    writeFileSync(foreignApproval, Buffer.from(negativePending.approved_frame_hex, "hex"));
    await active.quit();
    run(["--session", manifest.config, "--headless"], `/publication-apply ${manifest.native_publication_request}\n`,
      false, 15_000);
    run(["--session", manifest.config, "--headless"], `/approve ${foreignApproval}\n`, false, 15_000);
    assert.equal(digest(nativeFacts.conflicting_edit_path), nativeBefore);
    active = new InstalledSession(manifest.config);
    await active.expect(/actual local Harness runtime/);
    const stillPending = await active.commandJson(`/publication-observe ${manifest.native_publication_request}`, "publication");
    assert.equal(stillPending.status, "pending");
    const approval = join(temporary, "exact-original-root-approval.pb");
    writeFileSync(approval, Buffer.from(pendingPublication.approved_frame_hex, "hex"));
    const decision = await active.command(`/approve ${approval}`, /Admission\([^\n]+\n/);
    assert.match(decision[0], /Accepted/);
    assert.equal((await active.commandJson(`/publication-observe ${manifest.native_publication_request}`, "publication")).status, "approved");
    assert.equal(digest(nativeFacts.conflicting_edit_path), nativeBefore);
    const concurrentEdit = `actual concurrent native edit ${randomUUID()}\n`;
    writeFileSync(nativeFacts.concurrent_edit_path, concurrentEdit);
    const concurrentEditHash = digest(nativeFacts.concurrent_edit_path);
    assert.equal((await active.commandJson(`/publication-apply ${manifest.native_publication_request}`, "publication")).status, "applied");
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), before.root);
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash);
    await restart();
    await active.commandJson(`/publication-reconcile ${manifest.native_publication_request}`, "publication");
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), before.root);
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash);

    const negativeApproval = await negativeActive.command(`/approve ${foreignApproval}`, /Admission\([^\n]+\n/);
    assert.match(negativeApproval[0], /Accepted/);
    const overlappingEdit = `actual overlapping native edit ${randomUUID()}\n`;
    writeFileSync(negative.native_publication.conflicting_edit_path, overlappingEdit);
    const overlappingHash = digest(negative.native_publication.conflicting_edit_path);
    const refusedOverwrite = await negativeActive.commandJson(`/publication-apply ${negative.native_publication_request}`, "publication");
    assert.equal(refusedOverwrite.status, "failed");
    assert.ok(refusedOverwrite.failure?.work, "conflict carries the SDK's actual measured failure receipt");
    assert.match(refusedOverwrite.failure.message, /conflict|diverg|preimage|changed|concurrent/i);
    assert.equal(digest(negative.native_publication.conflicting_edit_path), overlappingHash,
      "approval cannot overwrite an overlapping real native edit after preparation");
    await negativeActive.quit();
    await active.quit();
    run(["--session", manifest.config, "--headless"],
      `/read ${input({
        caller: left, lease: leases.root, file: manifest.files.left_project,
      })}\n`, false, 15_000);
    active = new InstalledSession(manifest.config);
    await active.expect(/actual local Harness runtime/);
    const outcomes = new Map([[mailbox.task, mailboxOutcome]]);
    for (const [task, leasePath, command] of [
      [left, manifest.leases.left, manifest.callers.left.admission.input],
      [right, manifest.leases.right, manifest.callers.right.admission.input],
      [grandchild, manifest.leases.grandchild, manifest.callers.grandchild.admission.input],
      [crossTask, crossLeasePath, crossInput.request.child.input],
    ]) {
      const started = active.stdout.length;
      const dispatchedFrom = active.stderr.length;
      await active.command(`/resume ${leasePath}`, /original lease submitted for authoritative reconciliation/);
      const dispatch = await active.expect(
        /deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]+)/,
        "stderr", dispatchedFrom);
      const checkpoint = await modelCheckpoint(active, task, load(leasePath));
      bindDispatch(command, dispatch, checkpoint.transition);
      snapshotInvocations();
      await active.expect(new RegExp(`worker completed task=${task}`), "stdout", started);
      const outcome = await active.commandJson(`/outcome ${task}`, "task");
      assert.ok(outcome.outcome.Succeeded);
      assert.match(outcome.outcome.Succeeded.text, /^deterministic request=[0-9a-f]+ step=0$/);
      outcomes.set(task, outcome);
      snapshotInvocations();
    }
    const rootBeforeFault = await active.commandJson(`/outcome ${root}`, "task");
    assert.equal(rootBeforeFault.outcome, null);
    const rootLeaseBytes = readFileSync(manifest.leases.root);
    const rootDispatchStart = active.stderr.length;
    const rootStart = active.stdout.length;
    await active.command(`/resume ${manifest.leases.root}`, /original lease submitted for authoritative reconciliation/);
    const rootDispatch = await active.expect(
      /deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]+)/,
      "stderr", rootDispatchStart);
    const rootCheckpoint = await modelCheckpoint(active, root, leases.root);
    bindDispatch(manifest.callers.root.admission.input, rootDispatch, rootCheckpoint.transition);
    const beforeKillLedger = snapshotInvocations();
    assert.ok(!active.stdout.slice(rootStart).includes(`worker completed task=${root}`));
    assert.equal(active.finished, null);
    const killed = await active.stop();
    if (active.failure) throw active.failure;
    assert.equal(active.requestedKill, true, "OS must accept the explicit installed service kill, not an unrelated launch or natural-exit failure");
    assert.ok(killed.signal !== null || killed.code !== 0, "actual installed service process must die");
    assert.ok(!active.stdout.slice(rootStart).includes(`worker completed task=${root}`));
    assert.deepEqual(snapshotInvocations(), beforeKillLedger,
      "synced original invocation bytes survive actual OS process death exactly");

    active = new InstalledSession(manifest.config);
    await active.expect(/actual local Harness runtime/);
    assert.deepEqual(snapshotInvocations(), beforeKillLedger);
    await active.command(`/reconcile ${retained.root.child.operation_id}`, /retained=Some/);
    await active.commandJson(`/publication-reconcile ${manifest.native_publication_request}`, "publication");
    assert.equal(readFileSync(nativeFacts.conflicting_edit_path, "utf8"), before.root);
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash);
    const rootResumeStart = active.stdout.length;
    await active.command(`/resume ${manifest.leases.root}`, /original lease submitted for authoritative reconciliation/);
    await active.expect(new RegExp(`worker completed task=${root}`), "stdout", rootResumeStart);
    const rootOutcome = await active.commandJson(`/outcome ${root}`, "task");
    assert.ok(rootOutcome.outcome.Succeeded.text.includes(rootDispatch[2]));
    outcomes.set(root, rootOutcome);
    const rootModelOperation = rootDispatch[1];
    snapshotInvocations([rootModelOperation]);
    const bootstrapStart = active.stdout.length;
    const bootstrapDispatchStart = active.stderr.length;
    await active.command(`/resume ${manifest.leases.bootstrap_seed}`, /original lease submitted for authoritative reconciliation/);
    const bootstrapDispatch = await active.expect(
      /deterministic-dispatch operation=([0-9a-f-]+) step=0 request=([0-9a-f]+)/,
      "stderr", bootstrapDispatchStart);
    const bootstrapCheckpoint = await modelCheckpoint(active, manifest.tasks.bootstrap_seed, load(manifest.leases.bootstrap_seed));
    bindDispatch(manifest.callers.bootstrap_seed.admission.input,
      bootstrapDispatch, bootstrapCheckpoint.transition);
    snapshotInvocations([rootModelOperation]);
    await active.expect(new RegExp(`worker completed task=${manifest.tasks.bootstrap_seed}`), "stdout", bootstrapStart);
    const bootstrapOutcome = await active.commandJson(`/outcome ${manifest.tasks.bootstrap_seed}`, "task");
    assert.ok(bootstrapOutcome.outcome.Succeeded);
    outcomes.set(manifest.tasks.bootstrap_seed, bootstrapOutcome);
    for (const preparation of manifest.preparations) {
      const prepared = await active.commandJson(`/outcome ${preparation.task}`, "task");
      assert.ok(prepared.outcome.Succeeded);
      outcomes.set(preparation.task, prepared);
    }
    assert.equal(outcomes.size, 12);
    assert.equal(observedModels.size, 7);
    const completedLedger = snapshotInvocations([rootModelOperation]);
    await active.quit();
    active = new InstalledSession(manifest.config);
    await active.expect(/actual local Harness runtime/);
    for (const [task, original] of outcomes) {
      assert.deepEqual(await active.commandJson(`/outcome ${task}`, "task"), original);
      await active.command(`/run ${task}`, /worker idle task=/);
    }
    assert.equal(digest(nativeFacts.concurrent_edit_path), concurrentEditHash);
    assert.equal(digest(negative.native_publication.conflicting_edit_path), overlappingHash);
    await active.quit();
    assert.deepEqual(snapshotInvocations([rootModelOperation]), completedLedger);
    assert.ok(statSync(negativeLedgerPath).size <= 1_048_576);
    assert.deepEqual(readFileSync(negativeLedgerPath), negativeLedgerBytes,
      "existing negative controls admit no extra model/fault task and cannot contaminate positive invocation proof");
    const cancellationDirectory = join(scenarioDirectory, "cancel-preparation");
    run(["--prepare-scenario", cancellationDirectory], "", true, 120_000);
    const cancellation = load(join(cancellationDirectory, "native-scenario.json"));
    assert.deepEqual(cancellation.missing_prerequisites, []);
    const cancellationLedger = invocationLedgerPath(cancellation, load(cancellation.config), "cancel-preparation");
    const originalCalls = readFileSync(cancellationLedger);
    const cancelledPreparation = cancellation.preparations.find(value => value.label === "cross_source");
    const cancelledRequest = load(cancelledPreparation.request);
    const caller = cancelledRequest.parent, lease = cancelledRequest.caller_lease;
    const sourcePath = input("cancel-preparation-source", {
      caller, lease, volume: cancellation.files.left_scratch.volume, path: cancellation.files.left_scratch.path,
    });
    active = new InstalledSession(cancellation.config);
    await active.expect(/actual local Harness runtime/);
    const originalSource = await active.commandJson(`/read-path ${sourcePath}`, "file");
    await active.command(`/prepare ${cancelledPreparation.request}`, /queued preparation-task=/);
    await active.command(`/cancel-task ${cancelledPreparation.task}`, /cancellation requested task=/);
    await active.commandJson(`/outcome ${cancelledPreparation.task}`, "task");
    await active.stop();
    if (active.failure) throw active.failure;
    assert.ok(active.requestedKill);
    active = new InstalledSession(cancellation.config);
    await active.expect(/actual local Harness runtime/);
    assert.equal((await active.commandJson(`/outcome ${cancelledPreparation.task}`, "task")).outcome, "Cancelled");
    await active.command(`/run ${cancelledPreparation.task}`, /worker idle task=/);
    await active.command(`/reconcile ${cancelledRequest.request.child.operation_id}`, /retained=None/);
    assert.deepEqual(await active.commandJson(`/read-path ${sourcePath}`, "file"), originalSource);
    await active.quit();
    const refusedPublication = input("cancelled-preparation-publish", {
      caller, lease, preparation: cancelledPreparation.task,
    });
    run(["--session", cancellation.config, "--headless"], `/publish-fork ${refusedPublication}\n`, false, 15_000);
    assert.deepEqual(readFileSync(cancellationLedger), originalCalls,
      "cancelled preparation must not dispatch a model or restart its original child");
  } finally {
    if (active) await active.stop();
    if (negativeActive) await negativeActive.stop();
    rmSync(temporary, { recursive: true, force: true });
  }
}
