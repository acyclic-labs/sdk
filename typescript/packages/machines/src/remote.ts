/** Browser remote Machines provider backed by the Rust WASM transport. */
import { WasmRemoteMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  CheckpointOut, EventsOut, MutationOut, ObservationOut, OperationOut, PageOut,
  QualificationOut, UsageOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";
import type {
  CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image,
  ImageQualification, MachineId, MachineObservation, MachinesProvider,
  MachineEventPage, MachinePage, MutationOutcome, OperationId, OperationObservation,
  SuspensionPolicy, UsageReceipt,
} from "./index.js";
import { machineId, operationId } from "./index.js";
import { asPublic, usageOut } from "./simulator.js";

/**
 * Thin TypeScript facade over `WasmRemoteMachines`. Request decoding,
 * validation, authentication, transport selection, and response projection
 * remain in the Rust provider and its generated WASM DTO boundary.
 */
export class RemoteMachines implements MachinesProvider {
  readonly assurance = "customer-hosted" as const;
  readonly #inner: Promise<WasmRemoteMachines>;

  constructor(endpoint: string, token: string) {
    this.#inner = ensureMachinesWasm().then(() => WasmRemoteMachines.connect(endpoint, token));
  }

  async #call<Output>(operation: string, payload: unknown): Promise<Output> {
    const authored = structuredClone(payload);
    return (await this.#inner).call(operation, authored) as Output;
  }

  qualifyImage(image: Image): Promise<ImageQualification> { return this.#call<QualificationOut>("qualifyImage", image).then(asPublic<QualificationOut, ImageQualification>); }
  create(request: CreateMachine): Promise<MutationOutcome> { return this.#call<MutationOut>("create", request).then(asPublic<MutationOut, MutationOutcome>); }
  inspectMachine(machineIdValue: MachineId): Promise<MachineObservation> { return this.#call<ObservationOut>("inspectMachine", machineIdValue).then(asPublic<ObservationOut, MachineObservation>); }
  listMachines(after: MachineId | null, limit: number): Promise<MachinePage> { return this.#call<PageOut>("listMachines", { after, limit }).then(asPublic<PageOut, MachinePage>); }
  checkpoint(machineIdValue: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("checkpoint", { machineId: machineIdValue, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  inspectCheckpoint(checkpointIdValue: CheckpointId): Promise<CheckpointObservation> { return this.#call<CheckpointOut>("inspectCheckpoint", checkpointIdValue).then(asPublic<CheckpointOut, CheckpointObservation>); }
  fork(checkpointIdValue: CheckpointId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("fork", { checkpointId: checkpointIdValue, count, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  forkMachine(machineIdValue: MachineId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("forkMachine", { machineId: machineIdValue, count, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  suspend(machineIdValue: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("suspend", { machineId: machineIdValue, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  wake(machineIdValue: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("wake", { machineId: machineIdValue, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  setSuspensionPolicy(machineIdValue: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("setSuspensionPolicy", { machineId: machineIdValue, policy, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  destroyMachine(machineIdValue: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("destroyMachine", { machineId: machineIdValue, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  destroyCheckpoint(checkpointIdValue: CheckpointId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("destroyCheckpoint", { checkpointId: checkpointIdValue, idempotencyKey }).then(asPublic<MutationOut, MutationOutcome>); }
  events(machineIdValue: MachineId, afterSequence: number | null, limit: number): Promise<MachineEventPage> { return this.#call<EventsOut>("events", { machineId: machineIdValue, afterSequence, limit }).then(asPublic<EventsOut, MachineEventPage>); }
  usage(machineIdValue: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { return this.#call<UsageOut>("usage", { machineId: machineIdValue, startUnixMs, endUnixMs }).then(usageOut); }
  recover(key: IdempotencyKey): Promise<MutationOutcome> { return this.#call<MutationOut>("recover", key).then(asPublic<MutationOut, MutationOutcome>); }
  recoverOperation(key: IdempotencyKey): Promise<OperationId> { return this.#call<string>("recoverOperation", key).then(operationId); }
  inspectOperation(operationIdValue: OperationId): Promise<OperationObservation> { return this.#call<OperationOut>("inspectOperation", operationIdValue).then(asPublic<OperationOut, OperationObservation>); }
  cancel(operationIdValue: OperationId): Promise<OperationObservation> { return this.#call<OperationOut>("cancel", operationIdValue).then(asPublic<OperationOut, OperationObservation>); }
  async *watchOperation(operationIdValue: OperationId): AsyncIterable<OperationObservation> {
    const values = await this.#call<readonly OperationOut[]>("watchOperation", operationIdValue);
    for (const value of values) yield asPublic<OperationOut, OperationObservation>(value);
  }
}
