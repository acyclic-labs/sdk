/** Browser remote Machines provider backed by the Rust WASM transport. */
import { WasmRemoteMachines } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  CheckpointOut, EventsOut, MutationOut, ObservationOut, OperationOut, PageOut,
  QualificationOut, UsageOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";
import { invokeWithAbort } from "./generated-client.js";
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

  /** Connects only after Rust/WASM has admitted the endpoint and credential. */
  static async connect(endpoint: string, token: string): Promise<RemoteMachines> {
    const provider = new RemoteMachines(endpoint, token);
    await provider.#inner;
    return provider;
  }

  async #call<Output>(operation: string, payload: unknown, signal?: AbortSignal): Promise<Output> {
    const authored = structuredClone(payload);
    // WasmRemoteMachines does not expose a cancellation hook; abort stops
    // awaiting the accepted Rust operation while its underlying lifetime continues.
    return invokeWithAbort(() => (this.#inner.then(inner => inner.call(operation, authored)) as Promise<Output>), signal);
  }

  qualifyImage(image: Image, signal?: AbortSignal): Promise<ImageQualification> { return this.#call<QualificationOut>("qualifyImage", image, signal).then(asPublic<QualificationOut, ImageQualification>); }
  create(request: CreateMachine, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("create", request, signal).then(asPublic<MutationOut, MutationOutcome>); }
  inspectMachine(machineIdValue: MachineId, signal?: AbortSignal): Promise<MachineObservation> { return this.#call<ObservationOut>("inspectMachine", machineIdValue, signal).then(asPublic<ObservationOut, MachineObservation>); }
  listMachines(after: MachineId | null, limit: number, signal?: AbortSignal): Promise<MachinePage> { return this.#call<PageOut>("listMachines", { after, limit }, signal).then(asPublic<PageOut, MachinePage>); }
  checkpoint(machineIdValue: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("checkpoint", { machineId: machineIdValue, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  inspectCheckpoint(checkpointIdValue: CheckpointId, signal?: AbortSignal): Promise<CheckpointObservation> { return this.#call<CheckpointOut>("inspectCheckpoint", checkpointIdValue, signal).then(asPublic<CheckpointOut, CheckpointObservation>); }
  fork(checkpointIdValue: CheckpointId, count: number, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("fork", { checkpointId: checkpointIdValue, count, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  forkMachine(machineIdValue: MachineId, count: number, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("forkMachine", { machineId: machineIdValue, count, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  suspend(machineIdValue: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("suspend", { machineId: machineIdValue, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  wake(machineIdValue: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("wake", { machineId: machineIdValue, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  setSuspensionPolicy(machineIdValue: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("setSuspensionPolicy", { machineId: machineIdValue, policy, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  destroyMachine(machineIdValue: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("destroyMachine", { machineId: machineIdValue, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  destroyCheckpoint(checkpointIdValue: CheckpointId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("destroyCheckpoint", { checkpointId: checkpointIdValue, idempotencyKey }, signal).then(asPublic<MutationOut, MutationOutcome>); }
  events(machineIdValue: MachineId, afterSequence: number | null, limit: number, signal?: AbortSignal): Promise<MachineEventPage> { return this.#call<EventsOut>("events", { machineId: machineIdValue, afterSequence, limit }, signal).then(asPublic<EventsOut, MachineEventPage>); }
  usage(machineIdValue: MachineId, startUnixMs: number, endUnixMs: number, signal?: AbortSignal): Promise<UsageReceipt> { return this.#call<UsageOut>("usage", { machineId: machineIdValue, startUnixMs, endUnixMs }, signal).then(usageOut); }
  recover(key: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.#call<MutationOut>("recover", key, signal).then(asPublic<MutationOut, MutationOutcome>); }
  recoverOperation(key: IdempotencyKey, signal?: AbortSignal): Promise<OperationId> { return this.#call<string>("recoverOperation", key, signal).then(operationId); }
  inspectOperation(operationIdValue: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return this.#call<OperationOut>("inspectOperation", operationIdValue, signal).then(asPublic<OperationOut, OperationObservation>); }
  cancel(operationIdValue: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return this.#call<OperationOut>("cancel", operationIdValue, signal).then(asPublic<OperationOut, OperationObservation>); }
  async *watchOperation(operationIdValue: OperationId, signal?: AbortSignal): AsyncIterable<OperationObservation> {
    const values = await this.#call<readonly OperationOut[]>("watchOperation", operationIdValue, signal);
    for (const value of values) yield asPublic<OperationOut, OperationObservation>(value);
  }
}
