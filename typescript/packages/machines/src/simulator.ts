/** Browser and Node simulator backed by the canonical Rust Machines provider. */
import { WasmSimulatedMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  CheckpointOut, EventsOut, MutationOut, ObservationOut, OperationOut, PageOut,
  QualificationOut, UsageOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";
import type {
  CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image,
  ImageQualification, MachineId, MachineObservation, MachinesProvider,
  MachineEventPage, MachinePage, MutationOutcome, OperationId, OperationObservation, SuspensionPolicy,
  UsageReceipt,
} from "./index.js";
import { machineId, operationId } from "./index.js";

/**
 * The generated Rust DTO is already the public object graph. Brands are
 * compile-time only, so this constrained cast keeps the ergonomic API without
 * rebuilding every nested union and array in TypeScript.  `Public` must still
 * be assignable to the generated DTO; if Rust adds or changes a required field
 * the public type aliases fail here instead of hiding the drift behind
 * `unknown`.
 */
export function asPublic<Generated, Public extends Generated>(value: Generated): Public {
  return value as unknown as Public;
}

/** Keep byte fields detached from the generated result object. */
export function usageOut(value: UsageOut): UsageReceipt {
  return {
    ...value,
    machine: machineId(value.machine),
    lineageReceiptSha256: Uint8Array.from(value.lineageReceiptSha256),
    receipt: Uint8Array.from(value.receipt),
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

  qualifyImage(image: Image): Promise<ImageQualification> { return this.#run(image, (inner, authored) => inner.qualifyImage(authored)).then(asPublic<QualificationOut, ImageQualification>); }
  create(request: CreateMachine): Promise<MutationOutcome> { return this.#run(request, (inner, authored) => inner.create(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  inspectMachine(machineId: MachineId): Promise<MachineObservation> { return this.#run(machineId, (inner, authored) => inner.inspectMachine(authored)).then(asPublic<ObservationOut, MachineObservation>); }
  listMachines(after: MachineId | null, limit: number): Promise<MachinePage> { return this.#run({ after, limit }, (inner, authored) => inner.listMachines(authored)).then(asPublic<PageOut, MachinePage>); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.checkpoint(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation> { return this.#run(checkpointId, (inner, authored) => inner.inspectCheckpoint(authored)).then(asPublic<CheckpointOut, CheckpointObservation>); }
  fork(checkpointId: CheckpointId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ checkpointId, count, idempotencyKey }, (inner, authored) => inner.fork(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, count, idempotencyKey }, (inner, authored) => inner.forkMachine(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.suspend(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.wake(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, policy, idempotencyKey }, (inner, authored) => inner.setSuspensionPolicy(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.destroyMachine(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#run({ checkpointId, idempotencyKey }, (inner, authored) => inner.destroyCheckpoint(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<MachineEventPage> { return this.#run({ machineId, afterSequence, limit }, (inner, authored) => inner.events(authored)).then(asPublic<EventsOut, MachineEventPage>); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { return this.#run({ machineId, startUnixMs, endUnixMs }, (inner, authored) => inner.usage(authored)).then(usageOut); }
  recover(key: IdempotencyKey): Promise<MutationOutcome> { return this.#run(key, (inner, authored) => inner.recover(authored)).then(asPublic<MutationOut, MutationOutcome>); }
  recoverOperation(key: IdempotencyKey): Promise<OperationId> { return this.#run(key, (inner, authored) => inner.recoverOperation(authored)).then(operationId); }
  inspectOperation(operationId: OperationId): Promise<OperationObservation> { return this.#run(operationId, (inner, authored) => inner.inspectOperation(authored)).then(asPublic<OperationOut, OperationObservation>); }
  cancel(operationId: OperationId): Promise<OperationObservation> { return this.#run(operationId, (inner, authored) => inner.cancel(authored)).then(asPublic<OperationOut, OperationObservation>); }
  async *watchOperation(operationId: OperationId): AsyncIterable<OperationObservation> { for (const value of await this.#run(operationId, (inner, authored) => inner.watchOperation(authored))) yield asPublic<OperationOut, OperationObservation>(value); }
}
