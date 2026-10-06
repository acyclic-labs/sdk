/** Browser and Node simulator backed by the canonical Rust Machines provider. */
import { WasmSimulatedMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  CheckpointOut, EventsOut, MutationOut, ObservationOut, OperationOut, PageOut,
  QualificationOut, UsageOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";
import { invokeWithAbort } from "./generated-client.js";
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

  async #run<Input, Output>(payload: Input, invoke: (inner: WasmSimulatedMachines, payload: Input) => Promise<Output>, signal?: AbortSignal): Promise<Output> {
    // Take the caller's snapshot before asynchronous WASM initialization.
    const authored = structuredClone(payload);
    return invokeWithAbort(async () => invoke(await this.#inner, authored), signal);
  }

  qualifyImage(image: Image, signal?: AbortSignal): Promise<ImageQualification> { return this.#run(image, (inner, authored) => inner.qualifyImage(authored), signal).then(asPublic<QualificationOut, ImageQualification>); }
  create(request: CreateMachine, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run(request, (inner, authored) => inner.create(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  inspectMachine(machineId: MachineId, signal?: AbortSignal): Promise<MachineObservation> { return this.#run(machineId, (inner, authored) => inner.inspectMachine(authored), signal).then(asPublic<ObservationOut, MachineObservation>); }
  listMachines(after: MachineId | null, limit: number, signal?: AbortSignal): Promise<MachinePage> { return this.#run({ after, limit }, (inner, authored) => inner.listMachines(authored), signal).then(asPublic<PageOut, MachinePage>); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.checkpoint(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  inspectCheckpoint(checkpointId: CheckpointId, signal?: AbortSignal): Promise<CheckpointObservation> { return this.#run(checkpointId, (inner, authored) => inner.inspectCheckpoint(authored), signal).then(asPublic<CheckpointOut, CheckpointObservation>); }
  fork(checkpointId: CheckpointId, count: number, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ checkpointId, count, idempotencyKey }, (inner, authored) => inner.fork(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ machineId, count, idempotencyKey }, (inner, authored) => inner.forkMachine(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.suspend(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.wake(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ machineId, policy, idempotencyKey }, (inner, authored) => inner.setSuspensionPolicy(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ machineId, idempotencyKey }, (inner, authored) => inner.destroyMachine(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run({ checkpointId, idempotencyKey }, (inner, authored) => inner.destroyCheckpoint(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  events(machineId: MachineId, afterSequence: number | null, limit: number, signal?: AbortSignal): Promise<MachineEventPage> { return this.#run({ machineId, afterSequence, limit }, (inner, authored) => inner.events(authored), signal).then(asPublic<EventsOut, MachineEventPage>); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number, signal?: AbortSignal): Promise<UsageReceipt> { return this.#run({ machineId, startUnixMs, endUnixMs }, (inner, authored) => inner.usage(authored), signal).then(usageOut); }
  recover(key: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#run(key, (inner, authored) => inner.recover(authored), signal).then(asPublic<MutationOut, MutationOutcome>); }
  recoverOperation(key: IdempotencyKey, signal?: AbortSignal): Promise<OperationId> { return this.#run(key, (inner, authored) => inner.recoverOperation(authored), signal).then(operationId); }
  inspectOperation(operationId: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return this.#run(operationId, (inner, authored) => inner.inspectOperation(authored), signal).then(asPublic<OperationOut, OperationObservation>); }
  cancel(operationId: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return this.#run(operationId, (inner, authored) => inner.cancel(authored), signal).then(asPublic<OperationOut, OperationObservation>); }
  async *watchOperation(operationId: OperationId, signal?: AbortSignal): AsyncIterable<OperationObservation> { for (const value of await this.#run(operationId, (inner, authored) => inner.watchOperation(authored), signal)) yield asPublic<OperationOut, OperationObservation>(value); }
}
