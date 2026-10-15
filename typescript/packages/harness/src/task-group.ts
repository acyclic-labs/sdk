import { type GroupId, type BatchId, type InputKey, type Admission, type Outcome, type TaskDefinition, type TaskRef, registeredTaskDefinition, type HostBatchReplay, AdmissionUncertainError, pinnedOutputSchema, validateTaskOutcome, observeAdmittedTask, validatedHostTask, durableMachineDigest, nativeLimits, nativeRunLimits, type CancelReceipt } from "./task.js";
import { type TaskId } from "./index.js";
import { type GroupPolicy } from "./enums.js";
import { type WasmTaskAdmissionWire, type WasmDurableBatchWire, type WasmExecutionPlacementWire, type WasmBatchAdmissionInput, type WasmToolJsonValue } from "./native-contracts.js";
import { type AgentHarness } from "./agent-harness.js";

class BatchInputError extends Error {}

class BatchProviderError extends Error {}
export interface BatchAdmissionRequest {
  readonly contract: "harness.batch.v2";
  readonly groupId: GroupId;
  readonly batchId: BatchId;
  readonly taskName: string;
  readonly revision: string;
  readonly implementationDigest: string;
  readonly parentTaskId: TaskId | null;
  readonly policy: GroupPolicy;
  readonly members: readonly WasmTaskAdmissionWire[];
  /** Exact Rust-admitted manifest retained by the owner before member admission. */
  readonly canonical: WasmDurableBatchWire;
  readonly inputDigest: readonly number[];
}
export interface Batch<Input> { readonly id: BatchId; readonly inputs: readonly Input[] }
export interface GroupEntry<Output> { readonly key: InputKey; readonly admission: Admission<Output>; readonly outcome?: Outcome<Output> }
export type BatchCancellationStatus =
  | Readonly<{ kind: "requested" }>
  | Readonly<{ kind: "not-admitted"; reason: Extract<Admission<unknown>, { kind: "rejected" }>["reason"] }>
  | Readonly<{ kind: "indeterminate"; operationId: string }>
  | Readonly<{ kind: "unresolved"; message: string }>;
export interface BatchCancellationReport {
  readonly groupId: GroupId;
  readonly batchId: BatchId;
  /** The declaration is retained first; member requests are reconciled separately. */
  readonly entries: readonly Readonly<{ key: InputKey; status: BatchCancellationStatus }>[];
}

function assertBatchCancellationReport(report: BatchCancellationReport | null, groupId: GroupId,
  batchId: BatchId, count: number): asserts report is BatchCancellationReport {
  if (report === null || report.groupId !== groupId || report.batchId !== batchId
    || report.entries.length !== count || report.entries.some((entry, index) =>
      entry.key.batchId !== batchId || entry.key.index !== index)) {
    throw new Error("batch owner returned another cancellation report");
  }
}
export interface GroupOutcome<Output> { readonly groupId: GroupId; readonly complete: boolean; readonly entries: readonly GroupEntry<Output>[]; readonly cancellation: "not-needed" | "requested" | "indeterminate" }
type TaskAuthorizer = <Input, Output>(definition: TaskDefinition<Input, Output>) => void;
type GroupTask<Authority extends "owner" | "scoped", Input, Output> = Authority extends "scoped"
  ? TaskRef<Input, Output> : TaskDefinition<Input, Output>;

export class TaskGroup<Output, Authority extends "owner" | "scoped" = "owner"> {
  readonly #harness: AgentHarness;
  readonly #authorizeTask: TaskAuthorizer | undefined;
  readonly #entries = new Map<string, GroupEntry<Output>>();
  readonly #batchDigests = new Map<BatchId, Uint8Array>();
  readonly #durableBatchIds = new Set<BatchId>();
  #closed = false;
  constructor(harness: AgentHarness, readonly policy: GroupPolicy, readonly id: GroupId = (crypto.randomUUID() as GroupId), readonly parentTaskId?: TaskId, authorizeTask?: TaskAuthorizer) {
    this.#harness = harness;
    this.#authorizeTask = authorizeTask;
  }
  async spawnMany<Input>(selected: GroupTask<Authority, Input, Output>, batch: Batch<Input>): Promise<readonly GroupEntry<Output>[]> {
    if (!batch.id) throw new TypeError("batch id is required");
    if (this.#closed) return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index }, admission: { kind: "rejected", reason: { code: "group_closed", message: "group is closed" } } }));
    let definition: TaskDefinition<Input, Output>;
    try {
      definition = registeredTaskDefinition<Input, Output>(selected);
      this.#authorizeTask?.(definition);
    }
    catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index }, admission: { kind: "rejected", reason: { code: "unsupported", message } } }));
    }
    if (definition.implementation.kind === "resumable" && (!this.#harness.components.spawner?.admitBatch
      || !this.#harness.components.state)) {
      return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index },
        admission: { kind: "rejected", reason: { code: "unsupported", message: "durable batch requires spawner and state bindings" } } }));
    }
    let request: BatchAdmissionRequest | undefined;
    try {
      this.#harness.task(definition);
      if (definition.implementation.kind === "resumable") {
        await this.#harness.assertPolicyIdentity();
        const admittedInputs = this.#validateBatchInputs(definition, batch);
        if (!this.#harness.components.spawner?.loadBatch) {
          throw new Error("durable batch requires an owner-retained manifest provider");
        }
        const retainedValue = await this.#harness.components.spawner.loadBatch(batch.id, this.#harness);
        await this.#harness.assertPolicyIdentity();
        const retained = retainedValue === null ? null : this.#harness.contracts.validate("durable_batch_request", retainedValue);
        // Load retained placement before qualification so replay survives provider unavailability.
        request = await this.#batchRequest(definition, batch, retained?.execution, admittedInputs);
        if (retained && !this.#harness.contracts.canonicalEqual(retained, request.canonical)) {
          throw new Error("batch identity belongs to another admission request");
        }
      }
      else this.#bindBatchInputs(definition, batch);
    }
    catch (error) {
      if (!(error instanceof BatchInputError)) throw error;
      const message = error instanceof Error ? error.message : String(error);
      return batch.inputs.map((_input, index) => ({ key: { batchId: batch.id, index },
        admission: { kind: "rejected", reason: { code: "invalid_input", message } } }));
    }
    if (request) {
      const local = [...this.#entries.values()].filter(entry => entry.key.batchId === batch.id);
      if (local.length) {
        if (local.length !== batch.inputs.length) {
          throw new Error("batch identity belongs to another admission request");
        }
        return local;
      }
      const spawner = this.#harness.components.spawner;
      if (!spawner?.admitBatch) throw new Error("durable batch spawner disappeared after validation");
      await this.#harness.assertPolicyIdentity();
      let replay: HostBatchReplay;
      try { replay = await spawner.admitBatch(request, this.#harness); }
      catch (error) {
        if (!(error instanceof AdmissionUncertainError) || error.operationId !== batch.id) throw error;
        replay = { taskName: definition.name, revision: definition.revision,
          implementationDigest: definition.options.implementationDigest!, inputDigest: request.inputDigest,
          entries: request.members.map((member, index) => ({ key: { batchId: batch.id, index },
            admission: { kind: "indeterminate", operationId: member.operation_id } })) };
      }
      await this.#harness.assertPolicyIdentity();
      return this.#acceptBatchReplay(definition, batch, request, replay);
    }
    const entries = await Promise.all(batch.inputs.map(async (input, index): Promise<GroupEntry<Output>> => {
      const key = { batchId: batch.id, index };
      const existing = this.#entries.get(`${batch.id}:${index}`);
      let admission: Admission<Output>;
      if (existing) admission = existing.admission;
      else if (definition.implementation.kind === "live") {
        if (this.parentTaskId) admission = { kind: "rejected", reason: { code: "unsupported", message: "durable groups cannot spawn local-only tasks" } };
        else if (this.#harness.components.execution) admission = { kind: "rejected", reason: { code: "unsupported", message: "live task closures cannot cross an execution provider" } };
        else {
          try { admission = { kind: "accepted", task: this.#harness.spawn(definition, input) }; }
          catch (error) { admission = { kind: "rejected", reason: { code: "invalid_input", message: error instanceof Error ? error.message : String(error) } }; }
        }
      } else throw new Error("durable batch bypassed immutable admission");
      return { key, admission };
    }));
    for (const entry of entries) this.#entries.set(`${batch.id}:${entry.key.index}`, entry);
    return entries;
  }
  async reconcileBatch<Input>(selected: GroupTask<Authority, Input, Output>, batch: Batch<Input>): Promise<readonly GroupEntry<Output>[]> {
    if (!batch.id) throw new TypeError("batch id is required");
    const definition = registeredTaskDefinition<Input, Output>(selected);
    this.#authorizeTask?.(definition);
    this.#harness.task(definition);
    await this.#harness.assertPolicyIdentity();
    const spawner = this.#harness.components.spawner;
    const host = this.#harness.components.state;
    if (!spawner?.loadBatch || !spawner.reconcileBatch || !host) throw new Error("durable batch reconciliation requires retained owner manifest and state bindings");
    const loadedValue = await spawner.loadBatch(batch.id, this.#harness);
    if (loadedValue === null) throw new Error("retained batch is missing");
    const loaded = this.#harness.contracts.validate("durable_batch_request", loadedValue);
    const request = await this.#batchRequest(definition, batch, loaded.execution);
    if (!this.#harness.contracts.canonicalEqual(loaded, request.canonical)) {
      throw new Error("retained batch differs from its pinned task, inputs, or scope");
    }
    const local = [...this.#entries.values()].filter(entry => entry.key.batchId === batch.id);
    if (local.length) {
      if (local.length === batch.inputs.length && local.every(entry => entry.admission.kind !== "indeterminate"
        && entry.outcome?.kind !== "indeterminate")) return local;
    }
    await this.#harness.assertPolicyIdentity();
    const recovered = await spawner.reconcileBatch(request, this.#harness);
    await this.#harness.assertPolicyIdentity();
    return this.#acceptBatchReplay(definition, batch, request, recovered);
  }
  /** Reattach by the caller-retained ID after losing the original input vector. */
  async reconcileBatchId<Input>(selected: GroupTask<Authority, Input, Output>, batchId: BatchId): Promise<readonly GroupEntry<Output>[] | null> {
    const definition = registeredTaskDefinition<Input, Output>(selected);
    this.#authorizeTask?.(definition);
    this.#harness.task(definition);
    if (definition.implementation.kind !== "resumable") throw new Error("batch task is not resumable");
    const spawner = this.#harness.components.spawner;
    if (!spawner?.loadBatch || !spawner.reconcileBatch || !this.#harness.components.state) {
      throw new Error("batch ID reconciliation requires spawner and state bindings");
    }
    await this.#harness.assertPolicyIdentity();
    const loaded = await spawner.loadBatch(batchId, this.#harness);
    if (loaded === null) return null;
    const retained = this.#harness.contracts.validate("durable_batch_request", loaded);
    if (retained.batch_id !== batchId || retained.group_id !== this.id) {
      throw new Error("retained batch differs from its pinned group bindings");
    }
    const batch = { id: batchId, inputs: retained.inputs as readonly Input[] };
    const expected = await this.#batchRequest(definition, batch, retained.execution);
    if (!this.#harness.contracts.canonicalEqual(retained, expected.canonical)) {
      throw new Error("retained batch differs from its pinned task, inputs, or scope");
    }
    const recovered = await spawner.reconcileBatch(expected, this.#harness);
    await this.#harness.assertPolicyIdentity();
    return this.#acceptBatchReplay(definition, batch, expected, recovered);
  }
  /** Retain batch cancellation before requesting member cancellation. */
  async cancelBatchId<Input>(selected: GroupTask<Authority, Input, Output>, batchId: BatchId): Promise<BatchCancellationReport | null> {
    const admitted = await this.reconcileBatchId(selected, batchId);
    if (admitted === null) return null;
    const spawner = this.#harness.components.spawner;
    if (!spawner?.cancelBatch) throw new Error("durable batch cancellation is not bound");
    const report = await spawner.cancelBatch(batchId, this.#harness);
    assertBatchCancellationReport(report, this.id, batchId, admitted.length);
    return report;
  }
  async #acceptBatchReplay<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>,
    request: BatchAdmissionRequest, recovered: HostBatchReplay): Promise<readonly GroupEntry<Output>[]> {
    const { inputDigest } = request;
    const canonical = this.#harness.contracts.validate("durable_batch_request", request.canonical);
    const recomputedDigest = this.#harness.contracts.digestCanonicalJson(canonical);
    const pinned = this.#batchDigests.get(batch.id);
    if (!pinned || pinned.length !== recomputedDigest.length
      || pinned.some((byte, index) => byte !== recomputedDigest[index])
      || inputDigest.length !== recomputedDigest.length
      || inputDigest.some((byte, index) => byte !== recomputedDigest[index])) {
      throw new Error("batch request changed after admission binding");
    }
    const host = this.#harness.components.state;
    if (!host) throw new Error("durable batch state is not bound");
    if (inputDigest.length !== recovered.inputDigest.length
      || inputDigest.some((byte, index) => byte !== recovered.inputDigest[index])) {
      throw new Error("host batch input digest differs from the requested batch");
    }
    if (definition.name !== recovered.taskName || definition.revision !== recovered.revision
      || definition.options.implementationDigest !== recovered.implementationDigest
      || definition.implementation.kind !== "resumable") throw new Error("host returned an unregistered batch task implementation");
    const outputSchema = pinnedOutputSchema(definition);
    if (recovered.entries.length !== batch.inputs.length) throw new Error("host returned an incomplete batch");
    const entries: GroupEntry<Output>[] = [];
    const seen = new Set<number>();
    for (const entry of recovered.entries) {
      if (entry.key.batchId !== batch.id || !Number.isSafeInteger(entry.key.index)
        || entry.key.index < 0 || entry.key.index >= batch.inputs.length || seen.has(entry.key.index)) {
        throw new Error("host returned an invalid batch entry identity");
      }
      seen.add(entry.key.index);
      const operationId = this.#harness.contracts.batchMemberOperationId(this.id, batch.id, entry.key.index);
      if (entry.admission.kind === "indeterminate" && entry.admission.operationId !== operationId) {
        throw new Error("host returned an unrelated batch admission");
      }
      const outcome = entry.outcome === undefined ? undefined : validateTaskOutcome(entry.outcome, outputSchema, this.#harness.contracts);
      let admission: Admission<Output>;
      if (entry.admission.kind === "accepted") {
        const observed = await observeAdmittedTask(host, entry.admission.task.id(), this.#harness, operationId);
        if (observed === null) admission = { kind: "indeterminate", operationId };
        else {
          if (observed.operationId !== operationId || observed.task.id() !== entry.admission.task.id()
            || observed.taskName !== definition.name || observed.revision !== definition.revision
            || observed.implementationDigest !== definition.options.implementationDigest) {
            throw new Error("spawner and state host batch bindings differ");
          }
          await this.#harness.attestAdmission(observed, request.members[entry.key.index]!);
          admission = { kind: "accepted", task: validatedHostTask(observed.task, operationId, outputSchema, this.#harness.contracts) };
        }
      } else admission = entry.admission;
      if (outcome !== undefined && admission.kind !== "accepted") {
        throw new Error("batch outcome cannot precede an owner-observed accepted admission");
      }
      const validated: GroupEntry<Output> = { key: entry.key, admission, ...(outcome === undefined ? {} : { outcome }) };
      entries.push(validated);
    }
    entries.sort((a, b) => a.key.index - b.key.index);
    for (const entry of entries) this.#entries.set(`${batch.id}:${entry.key.index}`, entry);
    this.#durableBatchIds.add(batch.id);
    return entries;
  }
  async #batchRequest<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>,
    retainedExecution?: WasmExecutionPlacementWire | null,
    admittedInputs?: readonly Input[]): Promise<BatchAdmissionRequest> {
    if (definition.implementation.kind !== "resumable" || !definition.options.input
      || !definition.options.implementationDigest) throw new BatchInputError("durable batch needs a pinned resumable task");
    let request: BatchAdmissionRequest;
    try {
      const contracts = this.#harness.contracts;
      const inputs = admittedInputs ?? this.#validateBatchInputs(definition, batch);
      const machineDigest = durableMachineDigest(definition);
      const requirements = [...definition.options.requirements ?? []];
      const policy = this.#harness.durablePolicyIdentity();
      if (this.policy.kind === "cancel-on-failure" && !this.#harness.components.spawner?.cancelBatch) {
        throw new BatchProviderError("durable cancel-on-failure requires owner-retained batch cancellation");
      }
      const baseInput: WasmBatchAdmissionInput = {
        group_id: this.id,
        batch_id: batch.id,
        group_policy: this.policy.kind,
        name: definition.name,
        version: definition.revision,
        inputs: inputs as readonly WasmToolJsonValue[],
        input_schema: definition.options.input!.document,
        output_schema: definition.options.output!.document,
        requirements,
        machine_digest: machineDigest,
        parent: this.parentTaskId ?? null,
        resource_proofs: [],
        grants: [...this.#harness.scope.grants].sort(),
        limits: nativeLimits(this.#harness.components.limits),
        run_limits: nativeRunLimits(this.#harness.scope.limits),
        extensions: null,
        policy,
        execution: null,
      };
      const base = contracts.admitBatchRequest(baseInput);
      let execution = retainedExecution ?? null;
      if (retainedExecution === undefined && this.#harness.components.execution) {
        try {
          execution = contracts.validate("execution_placement", await this.#harness.components.execution.qualifyBatch(base.canonical));
        } catch (error) {
          throw new BatchProviderError(error instanceof Error ? error.message : String(error));
        }
      }
      await this.#harness.validateExecutionPlacement(execution);
      request = execution === null
        ? base
        : contracts.admitBatchRequest({ ...baseInput, execution });
    }
    catch (error) {
      if (error instanceof BatchInputError || error instanceof BatchProviderError) throw error;
      throw new BatchInputError(error instanceof Error ? error.message : String(error));
    }
    this.#pinBatchDigest(batch.id, request.inputDigest);
    return request;
  }
  #validateBatchInputs<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>): readonly Input[] {
    if (definition.implementation.kind !== "resumable" || !definition.options.input
      || !definition.options.implementationDigest) throw new BatchInputError("durable batch needs a pinned resumable task");
    try {
      const contracts = this.#harness.contracts;
      const admittedInputs: Input[] = [];
      for (const input of batch.inputs) {
        const admitted = definition.options.input.parse(contracts.validateToolValue(definition.options.input.document, input));
        contracts.validateToolValue(definition.options.input.document, admitted);
        admittedInputs.push(admitted);
      }
      return admittedInputs;
    } catch (error) {
      throw new BatchInputError(error instanceof Error ? error.message : String(error));
    }
  }
  #pinBatchDigest(batchId: BatchId, digest: readonly number[] | Uint8Array): void {
    const pinned = this.#batchDigests.get(batchId);
    if (pinned !== undefined && (pinned.length !== digest.length || pinned.some((byte, index) => byte !== digest[index]))) throw new Error("batch identity belongs to another admission request");
    this.#batchDigests.set(batchId, Uint8Array.from(digest));
  }
  #bindBatchInputs<Input>(definition: TaskDefinition<Input, Output>, batch: Batch<Input>): void {
    let digest: Uint8Array;
    try {
      digest = this.#harness.contracts.digestCanonicalJson({
        contract: "harness.batch.v2",
        group_id: this.id,
        batch_id: batch.id,
        task: registrationKey(definition),
        parent_task_id: this.parentTaskId ?? null,
        policy: this.policy.kind,
        grants: [...this.#harness.scope.grants].sort(),
        limits: this.#harness.scope.limits,
        inputs: batch.inputs,
      });
    }
    catch (error) { throw new BatchInputError(error instanceof Error ? error.message : String(error)); }
    this.#pinBatchDigest(batch.id, digest);
  }
  async *asCompleted(): AsyncIterable<GroupEntry<Output>> {
    for (const entry of this.#entries.values()) if (entry.admission.kind !== "accepted") yield entry;
    const completed: GroupEntry<Output>[] = [];
    let next = 0;
    let total = 0;
    let wake: (() => void) | undefined;
    for (const entry of this.#entries.values()) {
      if (entry.admission.kind !== "accepted") continue;
      total++;
      void entry.admission.task.result().then(outcome => {
        completed.push({ ...entry, outcome });
        wake?.();
        wake = undefined;
      });
    }
    while (next < total) {
      if (next === completed.length) await new Promise<void>(resolve => { wake = resolve; });
      while (next < completed.length) yield completed[next++]!;
    }
  }
  async join(): Promise<GroupOutcome<Output>> {
    this.#closed = true;
    const observed = new Map<string, GroupEntry<Output>>();
    let cancellation: GroupOutcome<Output>["cancellation"] = "not-needed";
    for await (const entry of this.asCompleted()) {
      observed.set(`${entry.key.batchId}:${entry.key.index}`, entry);
      const failed = entry.admission.kind === "rejected"
        || entry.outcome?.kind === "failed" || entry.outcome?.kind === "cancelled";
      if (failed && this.policy.kind === "cancel-on-failure" && cancellation === "not-needed") {
        try { cancellation = (await this.cancel()).requested ? "requested" : "indeterminate"; }
        catch { cancellation = "indeterminate"; break; }
        if (cancellation === "indeterminate") break;
      }
    }
    const entries = [...this.#entries.values()].map(entry => observed.get(`${entry.key.batchId}:${entry.key.index}`) ?? entry);
    const complete = cancellation !== "indeterminate" && entries.every(entry =>
      entry.admission.kind !== "indeterminate" && entry.outcome?.kind !== "indeterminate"
      && (entry.admission.kind !== "accepted" || entry.outcome !== undefined));
    return { groupId: this.id, complete, entries, cancellation };
  }
  async race(): Promise<GroupEntry<Output>> {
    for await (const entry of this.asCompleted()) if (entry.admission.kind === "accepted") return entry;
    throw new GroupError(this.id, [...this.#entries.values()]);
  }
  async firstSuccess(): Promise<{ readonly key: InputKey; readonly value: Output }> {
    const completed = new Map<string, GroupEntry<Output>>();
    for await (const entry of this.asCompleted()) {
      if (entry.admission.kind !== "accepted") continue;
      if (entry.outcome?.kind === "succeeded") return { key: entry.key, value: entry.outcome.value };
      completed.set(entry.key.batchId + ":" + entry.key.index, entry);
    }
    const entries = [...this.#entries.values()];
    throw new GroupError(this.id, completed.size
      ? entries.filter(entry => entry.admission.kind === "accepted").map(entry => completed.get(entry.key.batchId + ":" + entry.key.index)!)
      : entries);
  }
  close(): void { this.#closed = true; }
  async cancel(): Promise<CancelReceipt> {
    this.#closed = true;
    const spawner = this.#harness.components.spawner;
    if (this.#durableBatchIds.size && !spawner?.cancelBatch) {
      throw new Error("durable batch cancellation is not bound");
    }
    const reports = await Promise.all([...this.#durableBatchIds].map(async batchId => {
      const report = await spawner!.cancelBatch!(batchId, this.#harness);
      assertBatchCancellationReport(report, this.id, batchId,
        [...this.#entries.values()].filter(entry => entry.key.batchId === batchId).length);
      return report;
    }));
    const liveReceipts = await Promise.all([...this.#entries.values()].map(entry =>
      !this.#durableBatchIds.has(entry.key.batchId) && entry.admission.kind === "accepted"
        ? entry.admission.task.cancel() : undefined));
    const requested = reports.every(report => report.entries.every(entry =>
      entry.status.kind === "requested" || entry.status.kind === "not-admitted"))
      && liveReceipts.every(receipt => receipt?.requested !== false);
    return { requested, groupId: this.id };
  }
}

export class GroupError<Output> extends Error { constructor(readonly groupId: GroupId, readonly entries: readonly GroupEntry<Output>[]) { super("task group did not satisfy its completion strategy"); } }

function registrationKey<Input, Output>(definition: TaskDefinition<Input, Output>): string {
  return `${definition.name}@${definition.revision}#${definition.options.implementationDigest ?? "live"}`;
}
