import { taskFixture, modelCapacity, modelTokenCount } from "./browser-task-fixture.mjs";
import { jsonToolProjectionSchema } from "../generated/wasm/acyclic_harness_wasm.js";

const assert = (condition, message) => { if (!condition) throw new Error(message); };
const describe = value => JSON.stringify(value, (_, item) => typeof item === "bigint" ? `${item}n` : item);
const workerIdentity = { id: "browser-deadline-worker", available: {}, labels: {} };
const toolDefinition = () => ({
  name: "test.effect", revision: "1", description: "Deadline-bound external receipt",
  input_schema: { type: "object", properties: { value: { type: "string" } }, required: ["value"], additionalProperties: false },
  output_schema: { type: "string" },
  projection_schema: jsonToolProjectionSchema({ type: "object", properties: { observed: { type: "string" } },
    required: ["observed"], additionalProperties: false }),
});
const remotePath = id => `/__harness-fixture/tool/${id}`;
async function remoteCall(id, method, invocation) {
  const response = await fetch(`${remotePath(id)}/${method}`, { method: "POST", body: JSON.stringify(invocation) });
  if (!response.ok) throw new Error(`external receipt ${method}: ${response.status}`);
  return response.json();
}
async function inspectReceipt(id) {
  const response = await fetch(`${remotePath(id)}/inspect`);
  assert(response.ok, `receipt observation failed: ${response.status}`);
  return response.json();
}
function assertUncertain(outcome, label) {
  assert(outcome === null || (typeof outcome === "object" && outcome.Indeterminate != null),
    `${label} invented a terminal outcome: ${describe(outcome)}`);
}
function assertRetained(observation, task, deadline, lease) {
  assert(observation.task_id === task && observation.operation_id === task, `changed original task: ${describe(observation)}`);
  assert(observation.run_limits.deadline_epoch_ms === BigInt(deadline), `original admitted deadline changed: ${describe(observation)}`);
  assert(observation.cancellation_requested, `deadline did not request durable cancellation: ${describe(observation)}`);
  assert(observation.reservation?.reservation_id === lease.reservation.id
    && observation.reservation?.placement === lease.reservation.placement, `original fence was released or replaced: ${describe(observation)}`);
}

async function expiredDispatch(model) {
  const fixture = await taskFixture(crypto.randomUUID(), model ? "acyclic.stock_turn" : "test.browser");
  let runtime;
  let executions = 0;
  let reconciliations = 0;
  const deadline = Date.now() - 1;
  fixture.options.run_limits.deadline_epoch_ms = BigInt(deadline);
  try {
    if (model) fixture.registry.registerStockTurn();
    fixture.registry.registerTool(toolDefinition(),
      () => { executions++; throw new Error("already-expired task dispatched a tool"); },
      () => { reconciliations++; throw new Error("unstarted task reconciled a tool"); },
      () => { throw new Error("already-expired task projected a result"); });
    const open = async () => {
      const value = await fixture.open();
      value.configureModel({ provider: "browser-deadline", name: "forbidden", revision: "1", options: {} },
        () => { executions++; throw new Error("already-expired task dispatched a model"); },
        () => { reconciliations++; throw new Error("unstarted task reconciled a model"); },
        modelCapacity, modelTokenCount, { kind: "disabled" });
      return value;
    };
    runtime = await open();
    await runtime.initializeVolume();
    const operation = crypto.randomUUID();
    const file = await runtime.stage(operation, model ? "model" : "tool", new TextEncoder().encode(JSON.stringify(
      model ? { input: "do not generate", max_steps: 1 } : { name: "test.effect", revision: "1", arguments: { value: "do not execute" } })));
    const admission = await runtime.admit(crypto.randomUUID(), model ? "acyclic.stock_turn" : "test.browser", "1",
      model ? { operation_id: operation, kind: "acyclic.model.v1", payload: file } : { operation, kind: "acyclic.tool.v1", file });
    assert(admission.kind === "accepted", describe(admission));
    const work = await runtime.runOperation(workerIdentity, admission.task_id, 4);
    assert(work?.kind === "unresolved" && !work.error.message.includes("unsupported"), `expired deadline is unsupported or ran: ${describe(work)}`);
    assert(executions === 0 && reconciliations === 0 && fixture.transitions() === 0, "expired deadline crossed dispatch or machine boundary");
    assertRetained(await runtime.observeTask(admission.task_id), admission.task_id, deadline, work.lease);
    assertUncertain(await runtime.outcome(admission.task_id), "expired admission");
    runtime.free();
    runtime = await open();
    const retained = await runtime.recoverWork(admission.task_id);
    assert(retained.kind === "unresolved" && describe(retained.lease.reservation) === describe(work.lease.reservation),
      `reopen replaced the expired reservation: ${describe(retained)}`);
    assertRetained(await runtime.observeTask(admission.task_id), admission.task_id, deadline, retained.lease);
    assert((await runtime.resume(work.lease, 4)).kind === "unresolved", "expired lease gained execution authority on reopen");
    assert(executions === 0 && reconciliations === 0 && fixture.transitions() === 0, "reopen redispatched an expired task");
  } finally {
    runtime?.free();
    fixture.free();
  }
}
if (typeof document === "undefined") {
  let fixture;
  let runtime;
  let session;
  let originalInvocation;
  let release;
  let executions = 0;
  let reconciliations = 0;
  let projections = 0;
  const counts = () => ({ executions, reconciliations, projections, transitions: fixture.transitions() });
  self.onmessage = async ({ data }) => {
    try {
      let value;
      if (data.kind === "open") {
        session = data.session;
        fixture = await taskFixture(session);
        fixture.options.run_limits.deadline_epoch_ms = BigInt(data.deadline);
        fixture.registry.registerTool(toolDefinition(), async invocation => {
          executions++;
          assert(executions === 1, "original uncertain tool was redispatched");
          originalInvocation = invocation;
          let acknowledgmentLost = false;
          try { await remoteCall(session, "execute", invocation); }
          catch { acknowledgmentLost = true; }
          assert(acknowledgmentLost, "fixture did not lose the original acknowledgment");
          const receipt = await inspectReceipt(session);
          assert(receipt.executions === 1 && receipt.intent && receipt.result, "hung callback lacks a durable external effect");
          const pending = new Promise(resolve => { release = resolve; });
          self.postMessage({ kind: "effect-started", invocation });
          return pending;
        }, async invocation => {
          reconciliations++;
          if (originalInvocation) assert(describe(invocation) === describe(originalInvocation), "reconciliation changed original intent");
          const result = await remoteCall(session, "reconcile", invocation);
          assert(result?.value === "committed", "reconciliation did not observe the real receiver result");
          return result;
        }, (_, canonical) => {
          projections++;
          return { kind: "json", value: { observed: canonical.value } };
        });
        runtime = await fixture.open();
        value = { kind: "ready" };
      } else if (data.kind === "run") {
        value = await runtime.runOperation(workerIdentity, data.task, 4);
      } else if (data.kind === "resume") {
        value = await runtime.resume(data.lease, 4);
      } else if (data.kind === "recover") {
        value = await runtime.recoverWork(data.task);
      } else if (data.kind === "observe") {
        value = await runtime.observeTask(data.task);
      } else if (data.kind === "outcome") {
        value = await runtime.outcome(data.task);
      } else if (data.kind === "release") {
        assert(release && originalInvocation, "no original callback to release");
        release(await remoteCall(session, "reconcile", originalInvocation));
        await new Promise(resolve => setTimeout(resolve, 0));
        value = { kind: "released" };
      } else if (data.kind === "reconcile") {
        value = await runtime.reconcile(data.lease, 4, 5000);
      } else {
        throw new Error(`unknown deadline actor command ${data.kind}`);
      }
      self.postMessage({ id: data.id, value, counts: counts() });
    } catch (error) {
      self.postMessage({ id: data.id, error: String(error.stack ?? error) });
    }
  };
}

function deadlineActor() {
  const worker = new Worker(new URL("./browser-task-deadline.mjs", import.meta.url), { type: "module" });
  const pending = new Map();
  let started;
  const effectStarted = new Promise(resolve => { started = resolve; });
  const rejectAll = error => { for (const item of pending.values()) item.reject(error); pending.clear(); };
  worker.onmessage = ({ data }) => {
    if (data.kind === "effect-started") { started(data.invocation); return; }
    const item = pending.get(data.id);
    if (!item) return;
    pending.delete(data.id);
    data.error ? item.reject(new Error(data.error)) : item.resolve(data);
  };
  worker.onerror = event => rejectAll(new Error(event.message));
  return {
    effectStarted,
    call: (kind, data = {}) => new Promise((resolve, reject) => {
      const id = crypto.randomUUID();
      pending.set(id, { resolve, reject });
      worker.postMessage({ ...data, id, kind });
    }),
    terminate: () => { worker.terminate(); rejectAll(new Error("deadline actor terminated")); },
  };
}
async function observeBy(deadline, description, promise) {
  let timer;
  try {
    return await Promise.race([promise, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out observing ${description}`)), Math.max(0, deadline - Date.now()));
    })]);
  } finally { clearTimeout(timer); }
}

async function activeDeadline(phase) {
  const session = crypto.randomUUID();
  const fixture = await taskFixture(session);
  const deadline = Date.now() + 15000;
  fixture.options.run_limits.deadline_epoch_ms = BigInt(deadline);
  let runtime;
  let actor;
  try {
    runtime = await fixture.open();
    await runtime.initializeVolume();
    const operation = crypto.randomUUID();
    const file = await runtime.stage(operation, "deadline-tool", new TextEncoder().encode(JSON.stringify(
      { name: "test.effect", revision: "1", arguments: { value: "committed" } })));
    const admission = await runtime.admit(crypto.randomUUID(), "test.browser", "1", { operation, kind: "acyclic.tool.v1", file });
    assert(admission.kind === "accepted", describe(admission));
    actor = deadlineActor();
    await actor.call("open", { session, deadline });
    phase("real Web Worker dispatches one external effect and hangs after its lost acknowledgment");
    const run = actor.call("run", { task: admission.task_id });
    const invocation = await observeBy(deadline, "original effect dispatch before deadline", Promise.race([
      actor.effectStarted,
      run.then(value => { throw new Error(`runner returned before original effect dispatch: ${describe(value)}`); }),
    ]));
    assert(invocation.arguments.value === "committed", "dispatch substituted original effect arguments");
    phase("ordinary runtime interrupts the hung callback at the original admitted wall-clock deadline");
    const interrupted = await observeBy(deadline + 30000, "production deadline interruption", run);
    assert(Date.now() >= deadline, "task interrupted before its admitted deadline");
    assert(interrupted.value?.kind === "unresolved", `deadline fabricated completed work: ${describe(interrupted)}`);
    assert(!interrupted.value.error.message.includes("unsupported"), describe(interrupted));
    assert(interrupted.counts.executions === 1 && interrupted.counts.reconciliations === 0
      && interrupted.counts.projections === 0 && interrupted.counts.transitions === 1, `deadline lost or settled uncertain intent: ${describe(interrupted)}`);
    const lease = interrupted.value.lease;
    assertRetained(await runtime.observeTask(admission.task_id), admission.task_id, deadline, lease);
    assertUncertain(await runtime.outcome(admission.task_id), "hung original effect");
    phase("late original callback and stale lease cannot publish after expiry");
    await actor.call("release");
    const stale = await actor.call("resume", { lease });
    assert(stale.value.kind === "unresolved" && stale.counts.executions === 1 && stale.counts.reconciliations === 0
      && stale.counts.projections === 0 && stale.counts.transitions === 1, `stale actor published or redispatched: ${describe(stale)}`);
    assertRetained(await runtime.observeTask(admission.task_id), admission.task_id, deadline, lease);
    assertUncertain(await runtime.outcome(admission.task_id), "late callback");
    actor.terminate();
    actor = deadlineActor();
    await actor.call("open", { session, deadline });
    phase("replacement Web Worker recovers the exact original deadline and reservation");
    const retained = await actor.call("recover", { task: admission.task_id });
    assert(retained.value.kind === "unresolved" && describe(retained.value.lease.reservation) === describe(lease.reservation),
      `replacement changed the uncertain fence: ${describe(retained)}`);
    assertRetained((await actor.call("observe", { task: admission.task_id })).value, admission.task_id, deadline, retained.value.lease);
    const denied = await actor.call("resume", { lease });
    assert(denied.value.kind === "unresolved" && denied.counts.executions === 0 && denied.counts.reconciliations === 0
      && denied.counts.projections === 0 && denied.counts.transitions === 0, `replacement renewed deadline or granted stale publication: ${describe(denied)}`);
    assertUncertain((await actor.call("outcome", { task: admission.task_id })).value, "replacement observation");
    phase("explicit ordinary reconciliation observes the original lost-ack receipt without new dispatch");
    const reconciled = await actor.call("reconcile", { lease });
    assert(reconciled.value.kind === "completed" && reconciled.counts.executions === 0 && reconciled.counts.reconciliations === 1
      && reconciled.counts.projections === 1 && reconciled.counts.transitions === 0, `truthful receipt recovery differed: ${describe(reconciled)}`);
    assert((await runtime.outcome(admission.task_id)) === "Cancelled", "known receipt fabricated success instead of observing cancellation");
    const settled = await runtime.observeTask(admission.task_id);
    assert(settled.run_limits.deadline_epoch_ms === BigInt(deadline) && settled.cancellation_requested && settled.reservation === null,
      `settled task changed deadline or retained reservation: ${describe(settled)}`);
    const duplicate = await actor.call("run", { task: admission.task_id });
    assert(duplicate.value === null && duplicate.counts.reconciliations === 1 && duplicate.counts.projections === 1,
      `completed cancellation reexecuted its effect: ${describe(duplicate)}`);
    const receipt = await inspectReceipt(session);
    assert(receipt.executions === 1 && receipt.intent && receipt.result, "lost-ack recovery changed external execution count");
    const changed = await fetch(`${remotePath(session)}/reconcile`, { method: "POST",
      body: JSON.stringify({ ...invocation, arguments: { value: "changed" } }) });
    assert(changed.status === 409, "receiver accepted changed original intent after deadline");
    runtime.free();
    runtime = await fixture.open();
    assert((await runtime.outcome(admission.task_id)) === "Cancelled", "truthful cancellation was lost on provider reopen");
  } finally {
    actor?.terminate();
    runtime?.free();
    fixture.free();
  }
}

export async function qualifyTaskDeadlines(phase) {
  phase("already-expired admitted deadline denies tool dispatch before the first machine transition");
  await expiredDispatch(false);
  phase("already-expired admitted deadline denies stock model dispatch before the first machine transition");
  await expiredDispatch(true);
  await activeDeadline(phase);
}
