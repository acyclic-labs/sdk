/** Public shape-free Machines contracts. The deterministic provider is process-local only. */

import type { PublicEnum } from "./enums.js";
import { managedOci as managedOciRust } from "../generated/wasm/acyclic_machines_wasm.js";
import type {
  Capability as WireCapability,
  ExpirationKind as WireExpirationKind,
  MachineStatus as WireMachineStatus,
  OperationStatus as WireOperationStatus, Performance as WirePerformance,
  PressureKind as WirePressureKind,
} from "../generated/proto/machines/v1/machines_pb.js";
import type {
  BudgetsIn as WireBudgetsIn,
  CheckpointOut as WireCheckpointOut,
  CompatibilityOut as WireCompatibilityOut,
  ContractOut as WireContractOut,
  CreateIn as WireCreateIn,
  EndpointOut as WireEndpointOut,
  EventOut as WireEventOut,
  EventsOut as WireEventsOut,
  FactOut as WireFactOut,
  ImageOut as WireImageOut,
  MutationOut as WireMutationOut,
  ObservationOut as WireObservationOut,
  OperationOut as WireOperationOut,
  PageOut as WirePageOut,
  QualificationOut as WireQualificationOut,
  TimedOut as WireTimedOut,
  UsageOut as WireUsageOut,
} from "../generated/wasm/acyclic_machines_wasm.js";
import { ensureMachinesWasm, normalizeIdentity } from "./wasm-runtime.js";

// The root package is self-initializing so synchronous identity constructors
// are ready after an ordinary package import in browsers as well as Node/Bun.
await ensureMachinesWasm();

/** Preserve the Rust generated field set while making public DTOs immutable. */
type ReadonlyGenerated<Value> = Value extends unknown
  ? { readonly [Key in keyof Value]: Value[Key] }
  : never;

declare const machineBrand: unique symbol;
export type IdempotencyKey = string & { readonly [machineBrand]: "IdempotencyKey" };
export type MachineId = string & { readonly [machineBrand]: "MachineId" };
export type CheckpointId = string & { readonly [machineBrand]: "CheckpointId" };
export type OperationId = string & { readonly [machineBrand]: "OperationId" };
export function idempotencyKey(value: string): IdempotencyKey { return normalizeIdentity("idempotency", value) as IdempotencyKey; }
export function machineId(value: string): MachineId { return normalizeIdentity("machine", value) as MachineId; }
export function checkpointId(value: string): CheckpointId { return normalizeIdentity("checkpoint", value) as CheckpointId; }
export function operationId(value: string): OperationId { return normalizeIdentity("operation", value) as OperationId; }
export type OperationPhase = PublicEnum<typeof WireOperationStatus>;
export type OperationObservation = Omit<ReadonlyGenerated<WireOperationOut>, "id" | "phase"> & {
  readonly id: OperationId;
  readonly phase: OperationPhase;
};
type PublicImage<Value> = Value extends unknown
  ? { readonly [Key in keyof Value]: Key extends "checkpointId" ? CheckpointId : Value[Key] }
  : never;
export type Image = PublicImage<ReadonlyGenerated<WireImageOut>>;

export type Capability = PublicEnum<typeof WireCapability>;
type PublicCompatibility<Value> = Value extends unknown
  ? { readonly [Key in keyof Value]: Key extends "capabilities" ? readonly Capability[] : Value[Key] }
  : never;
export type CompatibilityPolicy = PublicCompatibility<ReadonlyGenerated<WireCompatibilityOut>>;
export type Performance = PublicEnum<typeof WirePerformance>;
type PublicTimed = ReadonlyGenerated<WireTimedOut>;
export type SuspensionPolicy = Extract<PublicTimed, { readonly kind: "manual" | "after-idle" }>;
type ExpirationKind = PublicEnum<typeof WireExpirationKind>;
export type ExpirationPolicy = Extract<PublicTimed, { readonly kind: Exclude<ExpirationKind, "never"> }>
  | Extract<PublicTimed, { readonly kind: "never" }>;
export type Budgets = ReadonlyGenerated<WireBudgetsIn>;

type PublicCreate = ReadonlyGenerated<WireCreateIn>;
export type CreateMachine = Omit<PublicCreate, "idempotencyKey" | "image" | "compatibility" | "performance" | "suspension" | "expiration" | "budgets"> & {
  readonly idempotencyKey: IdempotencyKey;
  readonly image: Image;
  readonly compatibility: CompatibilityPolicy;
  readonly performance: Performance;
  readonly suspension: SuspensionPolicy;
  readonly expiration: ExpirationPolicy;
  readonly budgets: Budgets;
};

type PublicContract = ReadonlyGenerated<WireContractOut>;
export type MachineContract = Omit<PublicContract, "image" | "capabilities" | "compatibility" | "performance" | "suspension" | "expiration" | "budgets"> & {
  readonly image: Image;
  readonly capabilities: readonly Capability[];
  readonly compatibility: CompatibilityPolicy;
  readonly performance: Performance;
  readonly suspension: SuspensionPolicy;
  readonly expiration: ExpirationPolicy;
  readonly budgets: Budgets;
};
/** Fork fidelity is emitted by the Rust WASM DTO and follows its generated union. */
export type ForkFidelity = Extract<MutationOutcome, { readonly kind: "machine-forked" }> extends infer Fork
  ? Fork extends { readonly fidelity: infer Fidelity } ? Fidelity : never
  : never;
export type ImageQualification = Omit<ReadonlyGenerated<WireQualificationOut>, "image" | "capabilities"> & {
  readonly image: Image;
  readonly capabilities: readonly Capability[];
};
export type MachineState = PublicEnum<typeof WireMachineStatus>;
export type Endpoint = ReadonlyGenerated<WireEndpointOut>;
/** Timestamps and event cursors are deliberately limited to exact JavaScript safe integers. */
export type MachineObservation = Omit<ReadonlyGenerated<WireObservationOut>, "id" | "state" | "contract" | "endpoints" | "lastCheckpoint"> & {
  readonly id: MachineId;
  readonly state: MachineState;
  readonly contract: MachineContract;
  readonly endpoints: readonly Endpoint[];
  readonly lastCheckpoint: CheckpointId | null;
};
export type CheckpointObservation = Omit<ReadonlyGenerated<WireCheckpointOut>, "id" | "source" | "contract"> & {
  readonly id: CheckpointId;
  readonly source: MachineId;
  readonly contract: MachineContract;
};
export type Pressure = PublicEnum<typeof WirePressureKind>;
type PublicFact<Value> = Value extends unknown
  ? { readonly [Key in keyof Value]: Key extends "state" ? MachineState : Key extends "pressure" ? Pressure : Value[Key] }
  : never;
export type EventFact = PublicFact<ReadonlyGenerated<WireFactOut>>;
/** Sequence and timestamp remain exact and are rejected above Number.MAX_SAFE_INTEGER. */
export type MachineEvent = Omit<ReadonlyGenerated<WireEventOut>, "machine" | "fact"> & {
  readonly machine: MachineId;
  readonly fact: EventFact;
};
export type UsageReceipt = Omit<ReadonlyGenerated<WireUsageOut>, "machine"> & { readonly machine: MachineId };
type PublicMutation<Value> = Value extends unknown
  ? { readonly [Key in keyof Value]: Key extends "machine" ? MachineObservation
      : Key extends "checkpoint" ? CheckpointObservation
      : Key extends "machines" ? readonly MachineObservation[]
      : Key extends "children" ? readonly MachineObservation[]
      : Key extends "machineId" ? MachineId
      : Key extends "checkpointId" ? CheckpointId
      : Key extends "source" ? MachineId
      : Key extends "policy" ? SuspensionPolicy
      : Value[Key] }
  : never;
export type MutationOutcome = PublicMutation<ReadonlyGenerated<WireMutationOut>>;

export type MachinePage = Omit<ReadonlyGenerated<WirePageOut>, "machines" | "next"> & {
  readonly machines: readonly MachineObservation[];
  readonly next: MachineId | null;
};
export type MachineEventPage = Omit<ReadonlyGenerated<WireEventsOut>, "events"> & {
  readonly events: readonly MachineEvent[];
};

/** Provider contract. Implementations must document their actual isolation and durability. */
export interface MachinesProvider {
  readonly assurance: "process-local-simulation" | "customer-hosted" | "managed-service";
  qualifyImage(image: Image): Promise<ImageQualification>;
  create(request: CreateMachine): Promise<MutationOutcome>;
  inspectMachine(machineId: MachineId): Promise<MachineObservation>;
  listMachines(after: MachineId | null, limit: number): Promise<MachinePage>;
  checkpoint(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation>;
  fork(checkpointId: CheckpointId, count: number, performance: Performance, key: IdempotencyKey): Promise<MutationOutcome>;
  forkMachine(machineId: MachineId, count: number, key: IdempotencyKey): Promise<MutationOutcome>;
  suspend(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  wake(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<MutationOutcome>;
  destroyMachine(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  destroyCheckpoint(checkpointId: CheckpointId, key: IdempotencyKey): Promise<MutationOutcome>;
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<MachineEventPage>;
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt>;
  recover(key: IdempotencyKey): Promise<MutationOutcome>;
  recoverOperation(key: IdempotencyKey): Promise<OperationId>;
  inspectOperation(operationId: OperationId): Promise<OperationObservation>;
  cancel(operationId: OperationId): Promise<OperationObservation>;
  watchOperation(operationId: OperationId): AsyncIterable<OperationObservation>;
}

/** Constructs a managed image using the Rust-generated OCI reference contract. */
export function managedOci(reference: string): Image {
  return managedOciRust(reference) as Image;
}

export { SimulatedMachines } from "./simulator.js";

export * from "./client.js";
export * from "./http.js";
