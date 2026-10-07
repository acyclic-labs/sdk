/** Public shape-free Machines contracts. The deterministic provider is process-local only. */
import type { BudgetsIn as RustBudgetsIn, CapabilityIn as RustCapability, CheckpointOut as RustCheckpointOut, CompatibilityOut as RustCompatibilityOut, ContractOut as RustContractOut, CreateIn as RustCreateIn, EndpointOut as RustEndpointOut, EventOut as RustEventOut, EventsOut as RustEventsOut, FactOut as RustFactOut, ImageOut as RustImageOut, MutationOut as RustMutationOut, ObservationOut as RustObservationOut, OperationOut as RustOperationOut, PageOut as RustPageOut, QualificationOut as RustQualificationOut, UsageOut as RustUsageOut } from "../generated/wasm/acyclic_machines_wasm.js";
/** Preserve the Rust generated field set while making public DTOs immutable. */
type ReadonlyGenerated<Value> = Value extends unknown ? {
    readonly [Key in keyof Value]: Value[Key];
} : never;
declare const machineBrand: unique symbol;
export type IdempotencyKey = string & {
    readonly [machineBrand]: "IdempotencyKey";
};
export type MachineId = string & {
    readonly [machineBrand]: "MachineId";
};
export type CheckpointId = string & {
    readonly [machineBrand]: "CheckpointId";
};
export type OperationId = string & {
    readonly [machineBrand]: "OperationId";
};
export declare function idempotencyKey(value: string): IdempotencyKey;
export declare function machineId(value: string): MachineId;
export declare function checkpointId(value: string): CheckpointId;
export declare function operationId(value: string): OperationId;
export type OperationPhase = RustOperationOut["phase"];
export type OperationObservation = Omit<ReadonlyGenerated<RustOperationOut>, "id" | "phase"> & {
    readonly id: OperationId;
    readonly phase: OperationPhase;
};
type PublicImage<Value> = Value extends unknown ? {
    readonly [Key in keyof Value]: Key extends "checkpointId" ? CheckpointId : Value[Key];
} : never;
export type Image = PublicImage<ReadonlyGenerated<RustImageOut>>;
export type Capability = RustCapability;
type PublicCompatibility<Value> = Value extends unknown ? {
    readonly [Key in keyof Value]: Key extends "capabilities" ? readonly Capability[] : Value[Key];
} : never;
export type CompatibilityPolicy = PublicCompatibility<ReadonlyGenerated<RustCompatibilityOut>>;
export type SuspensionPolicy = ReadonlyGenerated<RustCreateIn["suspension"]>;
export type ExpirationPolicy = ReadonlyGenerated<RustCreateIn["expiration"]>;
export type Budgets = ReadonlyGenerated<RustBudgetsIn>;
type PublicCreate = ReadonlyGenerated<RustCreateIn>;
export type CreateMachine = Omit<PublicCreate, "idempotencyKey" | "image" | "compatibility" | "suspension" | "expiration" | "budgets"> & {
    readonly idempotencyKey: IdempotencyKey;
    readonly image: Image;
    readonly compatibility: CompatibilityPolicy;
    readonly suspension: SuspensionPolicy;
    readonly expiration: ExpirationPolicy;
    readonly budgets: Budgets;
};
type PublicContract = ReadonlyGenerated<RustContractOut>;
export type MachineContract = Omit<PublicContract, "image" | "capabilities" | "compatibility" | "suspension" | "expiration" | "budgets"> & {
    readonly image: Image;
    readonly capabilities: readonly Capability[];
    readonly compatibility: CompatibilityPolicy;
    readonly suspension: SuspensionPolicy;
    readonly expiration: ExpirationPolicy;
    readonly budgets: Budgets;
};
/** Fork fidelity is emitted by the Rust WASM DTO and follows its generated union. */
export type ForkFidelity = Extract<MutationOutcome, {
    readonly kind: "machine-forked";
}> extends infer Fork ? Fork extends {
    readonly fidelity: infer Fidelity;
} ? Fidelity : never : never;
export type ImageQualification = Omit<ReadonlyGenerated<RustQualificationOut>, "image" | "capabilities"> & {
    readonly image: Image;
    readonly capabilities: readonly Capability[];
};
export type MachineState = RustObservationOut["state"];
export type Endpoint = ReadonlyGenerated<RustEndpointOut>;
/** Timestamps and event cursors are deliberately limited to exact JavaScript safe integers. */
export type MachineObservation = Omit<ReadonlyGenerated<RustObservationOut>, "id" | "state" | "contract" | "endpoints" | "lastCheckpoint"> & {
    readonly id: MachineId;
    readonly state: MachineState;
    readonly contract: MachineContract;
    readonly endpoints: readonly Endpoint[];
    readonly lastCheckpoint: CheckpointId | null;
};
export type CheckpointObservation = Omit<ReadonlyGenerated<RustCheckpointOut>, "id" | "source" | "contract"> & {
    readonly id: CheckpointId;
    readonly source: MachineId;
    readonly contract: MachineContract;
};
export type Pressure = Extract<RustFactOut, {
    readonly kind: "pressure";
}>["pressure"];
type PublicFact<Value> = Value extends unknown ? {
    readonly [Key in keyof Value]: Key extends "state" ? MachineState : Key extends "pressure" ? Pressure : Value[Key];
} : never;
export type EventFact = PublicFact<ReadonlyGenerated<RustFactOut>>;
/** Sequence and timestamp remain exact and are rejected above Number.MAX_SAFE_INTEGER. */
export type MachineEvent = Omit<ReadonlyGenerated<RustEventOut>, "machine" | "fact"> & {
    readonly machine: MachineId;
    readonly fact: EventFact;
};
export type UsageReceipt = Omit<ReadonlyGenerated<RustUsageOut>, "machine"> & {
    readonly machine: MachineId;
};
type PublicMutation<Value> = Value extends unknown ? {
    readonly [Key in keyof Value]: Key extends "machine" ? MachineObservation : Key extends "checkpoint" ? CheckpointObservation : Key extends "machines" ? readonly MachineObservation[] : Key extends "children" ? readonly MachineObservation[] : Key extends "machineId" ? MachineId : Key extends "checkpointId" ? CheckpointId : Key extends "source" ? MachineId : Key extends "policy" ? SuspensionPolicy : Value[Key];
} : never;
export type MutationOutcome = PublicMutation<ReadonlyGenerated<RustMutationOut>>;
export type MachinePage = Omit<ReadonlyGenerated<RustPageOut>, "machines" | "next"> & {
    readonly machines: readonly MachineObservation[];
    readonly next: MachineId | null;
};
export type MachineEventPage = Omit<ReadonlyGenerated<RustEventsOut>, "events"> & {
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
    fork(checkpointId: CheckpointId, count: number, key: IdempotencyKey): Promise<MutationOutcome>;
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
export declare function managedOci(reference: string): Image;
export { SimulatedMachines } from "./simulator.js";
export * from "./client.js";
export * from "./http.js";
export { performanceObserver, type AcyclicObserver, type OperationEvent } from "./observe.js";
