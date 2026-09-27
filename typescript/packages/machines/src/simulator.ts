/** Browser and Node simulator backed by the canonical Rust Machines provider. */
import { WasmSimulatedMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  CheckpointOut, ContractOut, EventOut, EventsOut, FactOut, ImageOut, MutationOut,
  ObservationOut, OperationOut, PageOut, QualificationOut, TimedOut, UsageOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";
import type {
  CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image,
  ImageQualification, MachineEvent, MachineId, MachineObservation, MachinesProvider,
  MachineEventPage, MachinePage, MutationOutcome, OperationId, OperationObservation, Performance, SuspensionPolicy,
  UsageReceipt, CompatibilityPolicy, ExpirationPolicy, MachineContract,
} from "./index.js";
import { checkpointId, machineId, operationId } from "./index.js";

/**
 * Rust owns validation and serializes every output through the generated WASM
 * DTOs.  Keep a runtime guard for a future incompatible generated variant,
 * while making the current projection exhaustive at compile time.
 */
function unreachable(value: never): never {
  throw new TypeError(`unexpected generated Machines value: ${JSON.stringify(value)}`);
}

function unexpected(value: unknown): never {
  throw new TypeError(`unexpected generated Machines value: ${JSON.stringify(value)}`);
}

function copyBytes(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value);
}

export function imageOut(value: ImageOut): Image {
  switch (value.kind) {
    case "checkpoint": return { kind: value.kind, checkpointId: checkpointId(value.checkpointId) };
    case "managed-oci": return { kind: value.kind, digestHex: value.digestHex };
    case "custom": return { kind: value.kind, digestHex: value.digestHex };
    default: return unexpected(value);
  }
}

export function compatibilityOut(value: ContractOut["compatibility"]): CompatibilityPolicy {
  switch (value.kind) {
    case "best-effort": return { kind: value.kind };
    case "require": return { kind: value.kind, capabilities: [...value.capabilities] };
    default: return unexpected(value);
  }
}

export function suspensionOut(value: TimedOut): SuspensionPolicy {
  switch (value.kind) {
    case "manual": return { kind: value.kind };
    case "after-idle": return { kind: value.kind, milliseconds: value.milliseconds };
    default: return unexpected(value);
  }
}

export function expirationOut(value: TimedOut): ExpirationPolicy {
  switch (value.kind) {
    case "never": return { kind: value.kind };
    case "max-age":
    case "at":
    case "idle": return { kind: value.kind, milliseconds: value.milliseconds };
    default: return unexpected(value);
  }
}

export function contractOut(value: ContractOut): MachineContract {
  return {
    image: imageOut(value.image),
    capabilities: [...value.capabilities],
    compatibility: compatibilityOut(value.compatibility),
    compatibilityRevisionHex: value.compatibilityRevisionHex,
    performance: value.performance,
    suspension: suspensionOut(value.suspension),
    expiration: expirationOut(value.expiration),
    networkPolicyDigestHex: value.networkPolicyDigestHex,
    budgets: { spendMicros: value.budgets.spendMicros, concurrency: value.budgets.concurrency },
  };
}

export function observationOut(value: ObservationOut): MachineObservation {
  return {
    id: machineId(value.id),
    state: value.state,
    contract: contractOut(value.contract),
    endpoints: value.endpoints.map(({ name, uri }) => ({ name, uri })),
    lastCheckpoint: value.lastCheckpoint === null ? null : checkpointId(value.lastCheckpoint),
    createdAtUnixMs: value.createdAtUnixMs,
    changedAtUnixMs: value.changedAtUnixMs,
  };
}

export function checkpointOut(value: CheckpointOut): CheckpointObservation {
  return {
    id: checkpointId(value.id),
    source: machineId(value.source),
    contract: contractOut(value.contract),
    forkable: value.forkable,
    createdAtUnixMs: value.createdAtUnixMs,
  };
}

export function factOut(value: FactOut): MachineEvent["fact"] {
  switch (value.kind) {
    case "state": return { kind: value.kind, state: value.state };
    case "pressure": return { kind: value.kind, pressure: value.pressure };
    case "capacity-changed": return { kind: value.kind };
    default: return unreachable(value);
  }
}

export function eventOut(value: EventOut): MachineEvent {
  return {
    machine: machineId(value.machine),
    sequence: value.sequence,
    observedAtUnixMs: value.observedAtUnixMs,
    fact: factOut(value.fact),
  };
}

export function mutationOut(value: MutationOut): MutationOutcome {
  switch (value.kind) {
    case "created": return { kind: value.kind, machine: observationOut(value.machine) };
    case "checkpointed": return { kind: value.kind, checkpoint: checkpointOut(value.checkpoint) };
    case "forked": return { kind: value.kind, machines: value.machines.map(observationOut) };
    case "machine-forked": return { kind: value.kind, source: machineId(value.source), fidelity: value.fidelity, children: value.children.map(observationOut) };
    case "suspended": return { kind: value.kind, machineId: machineId(value.machineId) };
    case "woken": return { kind: value.kind, machineId: machineId(value.machineId) };
    case "suspension-policy-set": return { kind: value.kind, machineId: machineId(value.machineId), policy: suspensionOut(value.policy) };
    case "machine-destroyed": return { kind: value.kind, machineId: machineId(value.machineId) };
    case "checkpoint-destroyed": return { kind: value.kind, checkpointId: checkpointId(value.checkpointId) };
    default: return unreachable(value);
  }
}

export function qualificationOut(value: QualificationOut): ImageQualification {
  return {
    image: imageOut(value.image),
    capabilities: [...value.capabilities],
    compatibilityRevisionHex: value.compatibilityRevisionHex,
  };
}

export function operationOut(value: OperationOut): OperationObservation {
  return { id: operationId(value.id), phase: value.phase };
}

export function pageOut(value: PageOut): MachinePage {
  return { machines: value.machines.map(observationOut), next: value.next === null ? null : machineId(value.next) };
}

export function eventsOut(value: EventsOut): MachineEventPage {
  return { events: value.events.map(eventOut), nextSequence: value.nextSequence };
}

export function usageOut(value: UsageOut): UsageReceipt {
  return {
    machine: machineId(value.machine),
    startUnixMs: value.startUnixMs,
    endUnixMs: value.endUnixMs,
    elasticCpuNs: value.elasticCpuNs,
    dedicatedCpuNs: value.dedicatedCpuNs,
    privateResidentByteSeconds: value.privateResidentByteSeconds,
    durablePrivateBytes: value.durablePrivateBytes,
    lineageReceiptSha256: copyBytes(value.lineageReceiptSha256),
    egressBytes: value.egressBytes,
    receipt: copyBytes(value.receipt),
  };
}

export class SimulatedMachines implements MachinesProvider {
  readonly assurance = "process-local-simulation" as const;
  readonly #inner: Promise<WasmSimulatedMachines>;

  constructor() {
    this.#inner = ensureMachinesWasm().then(() => new WasmSimulatedMachines());
  }

  async #run<Input, Output>(payload: Input, invoke: (inner: WasmSimulatedMachines, payload: Input) => Promise<Output>): Promise<Output> {
    // Take the caller's snapshot before asynchronous WASM initialization.
    const authored = structuredClone(payload);
    return invoke(await this.#inner, authored);
  }

  qualifyImage(image: Image): Promise<ImageQualification> { return this.#run(image, (inner, authored) => inner.qualifyImage(authored)).then(qualificationOut); }
  create(request: CreateMachine): Promise<MutationOutcome> { return this.#run(request, (inner, authored) => inner.create(authored)).then(mutationOut); }
  inspectMachine(machineId: MachineId): Promise<MachineObservation> { return this.#run(machineId, (inner, authored) => inner.inspectMachine(authored)).then(observationOut); }
  listMachines(after: MachineId | null, limit: number): Promise<MachinePage> { return this.#run({ after, limit }, (inner, authored) => inner.listMachines(authored)).then(pageOut); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.checkpoint(authored)).then(mutationOut); }
  inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation> { return this.#run(checkpointId, (inner, authored) => inner.inspectCheckpoint(authored)).then(checkpointOut); }
  fork(checkpointId: CheckpointId, count: number, performance: Performance, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ checkpointId, count, performance, idempotencyKey }, (inner, authored) => inner.fork(authored)).then(mutationOut); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, count, idempotencyKey }, (inner, authored) => inner.forkMachine(authored)).then(mutationOut); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.suspend(authored)).then(mutationOut); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.wake(authored)).then(mutationOut); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, policy, idempotencyKey }, (inner, authored) => inner.setSuspensionPolicy(authored)).then(mutationOut); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.destroyMachine(authored)).then(mutationOut); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ checkpointId, idempotencyKey }, (inner, authored) => inner.destroyCheckpoint(authored)).then(mutationOut); }
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<MachineEventPage> { return this.#run({ machineId, afterSequence, limit }, (inner, authored) => inner.events(authored)).then(eventsOut); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { return this.#run({ machineId, startUnixMs, endUnixMs }, (inner, authored) => inner.usage(authored)).then(usageOut); }
  recover(key: IdempotencyKey): Promise<MutationOutcome> { return this.#run(key, (inner, authored) => inner.recover(authored)).then(mutationOut); }
  recoverOperation(key: IdempotencyKey): Promise<OperationId> { return this.#run(key, (inner, authored) => inner.recoverOperation(authored)).then(operationId); }
  inspectOperation(operationId: OperationId): Promise<OperationObservation> { return this.#run(operationId, (inner, authored) => inner.inspectOperation(authored)).then(operationOut); }
  cancel(operationId: OperationId): Promise<OperationObservation> { return this.#run(operationId, (inner, authored) => inner.cancel(authored)).then(operationOut); }
  async *watchOperation(operationId: OperationId): AsyncIterable<OperationObservation> { for (const value of await this.#run(operationId, (inner, authored) => inner.watchOperation(authored))) yield operationOut(value); }
}
