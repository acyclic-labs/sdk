/** Public shape-free Machines contracts. The deterministic provider is process-local only. */
import { digestHex } from "./digest.js";

declare const machineBrand: unique symbol;
export type IdempotencyKey = string & { readonly [machineBrand]: "IdempotencyKey" };
export type MachineId = string & { readonly [machineBrand]: "MachineId" };
export type CheckpointId = string & { readonly [machineBrand]: "CheckpointId" };
export type OperationId = string & { readonly [machineBrand]: "OperationId" };
export function idempotencyKey(value: string): IdempotencyKey { if (!value.trim()) throw new TypeError("idempotency key is required"); return value as IdempotencyKey; }
export function machineId(value: string): MachineId { if (!value.trim()) throw new TypeError("machine ID is required"); return value as MachineId; }
export function checkpointId(value: string): CheckpointId { if (!value.trim()) throw new TypeError("checkpoint ID is required"); return value as CheckpointId; }
export function operationId(value: string): OperationId { if (!value.trim()) throw new TypeError("operation ID is required"); return value as OperationId; }
export type OperationPhase = "pending" | "succeeded" | "cancelled" | "indeterminate" | "failed";
export interface OperationObservation { readonly id: OperationId; readonly phase: OperationPhase }
export type Image =
  | { readonly kind: "managed-oci"; readonly digestHex: string }
  | { readonly kind: "custom"; readonly digestHex: string }
  | { readonly kind: "checkpoint"; readonly checkpointId: CheckpointId };

export type Capability = "elastic-cpu" | "elastic-memory" | "live-checkpoint" | "live-fork" | "suspend-resume" | "live-movement" | "disk-fork";
/** Fidelity of children produced by a live machine fork. */
export type ForkFidelity = "memory-and-disk" | "disk-only";
export function forkFidelity(capabilities: readonly Capability[]): ForkFidelity | null {
  return capabilities.includes("live-fork") ? "memory-and-disk" : capabilities.includes("disk-fork") ? "disk-only" : null;
}
export type CompatibilityPolicy = { readonly kind: "best-effort" } | { readonly kind: "require"; readonly capabilities: readonly Capability[] };
export type Performance = "elastic" | "dedicated";
export type SuspensionPolicy = { readonly kind: "manual" } | { readonly kind: "after-idle"; readonly milliseconds: number };
export type ExpirationPolicy = { readonly kind: "never" } | { readonly kind: "max-age" | "at" | "idle"; readonly milliseconds: number };
export interface Budgets { readonly spendMicros: bigint; readonly concurrency: number }

export interface CreateMachine {
  readonly idempotencyKey: IdempotencyKey;
  readonly image: Image;
  readonly compatibility: CompatibilityPolicy;
  readonly performance: Performance;
  readonly suspension: SuspensionPolicy;
  readonly expiration: ExpirationPolicy;
  readonly networkPolicyDigestHex: string;
  readonly budgets: Budgets;
}

export interface MachineContract extends Omit<CreateMachine, "idempotencyKey"> {
  readonly capabilities: readonly Capability[];
  readonly compatibilityRevisionHex: string;
}
export interface ImageQualification { readonly image: Image; readonly capabilities: readonly Capability[]; readonly compatibilityRevisionHex: string }
export type MachineState = "starting" | "running" | "suspending" | "suspended" | "waking" | "destroying" | "destroyed" | "failed" | "indeterminate";
export interface Endpoint { readonly name: string; readonly uri: string }
/** Timestamps and event cursors are deliberately limited to exact JavaScript safe integers. */
export interface MachineObservation { readonly id: MachineId; readonly state: MachineState; readonly contract: MachineContract; readonly endpoints: readonly Endpoint[]; readonly lastCheckpoint: CheckpointId | null; readonly createdAtUnixMs: number; readonly changedAtUnixMs: number }
export interface CheckpointObservation { readonly id: CheckpointId; readonly source: MachineId; readonly contract: MachineContract; readonly forkable: boolean; readonly createdAtUnixMs: number }
export type Pressure = "customer-budget" | "machine-limit" | "service-saturation";
export type EventFact = { readonly kind: "state"; readonly state: MachineState } | { readonly kind: "pressure"; readonly pressure: Pressure } | { readonly kind: "capacity-changed" };
/** Sequence and timestamp remain exact and are rejected above Number.MAX_SAFE_INTEGER. */
export interface MachineEvent { readonly machine: MachineId; readonly sequence: number; readonly observedAtUnixMs: number; readonly fact: EventFact }
export interface UsageReceipt { readonly machine: MachineId; readonly startUnixMs: number; readonly endUnixMs: number; readonly elasticCpuNs: bigint; readonly dedicatedCpuNs: bigint; readonly privateResidentByteSeconds: bigint; readonly durablePrivateBytes: bigint; readonly lineageReceiptSha256: Uint8Array; readonly egressBytes: bigint; readonly receipt: Uint8Array }
export type MutationOutcome = { readonly kind: "created"; readonly machine: MachineObservation } | { readonly kind: "checkpointed"; readonly checkpoint: CheckpointObservation } | { readonly kind: "forked"; readonly machines: readonly MachineObservation[] } | { readonly kind: "machine-forked"; readonly source: MachineId; readonly fidelity: ForkFidelity; readonly children: readonly MachineObservation[] } | { readonly kind: "suspended" | "woken" | "machine-destroyed"; readonly machineId: MachineId } | { readonly kind: "suspension-policy-set"; readonly machineId: MachineId; readonly policy: SuspensionPolicy } | { readonly kind: "checkpoint-destroyed"; readonly checkpointId: CheckpointId };

/** Provider contract. Implementations must document their actual isolation and durability. */
export interface MachinesProvider {
  readonly assurance: "process-local-simulation" | "customer-hosted" | "managed-service";
  qualifyImage(image: Image): Promise<ImageQualification>;
  create(request: CreateMachine): Promise<MutationOutcome>;
  inspectMachine(machineId: MachineId): Promise<MachineObservation>;
  listMachines(after: MachineId | null, limit: number): Promise<{ readonly machines: readonly MachineObservation[]; readonly next: MachineId | null }>;
  checkpoint(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation>;
  fork(checkpointId: CheckpointId, count: number, performance: Performance, key: IdempotencyKey): Promise<MutationOutcome>;
  forkMachine(machineId: MachineId, count: number, key: IdempotencyKey): Promise<MutationOutcome>;
  suspend(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  wake(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<MutationOutcome>;
  destroyMachine(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  destroyCheckpoint(checkpointId: CheckpointId, key: IdempotencyKey): Promise<MutationOutcome>;
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }>;
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt>;
  recover(key: IdempotencyKey): Promise<MutationOutcome>;
  recoverOperation(key: IdempotencyKey): Promise<OperationId>;
  inspectOperation(operationId: OperationId): Promise<OperationObservation>;
  cancel(operationId: OperationId): Promise<OperationObservation>;
  watchOperation(operationId: OperationId): AsyncIterable<OperationObservation>;
}

/** Constructs a managed image only from an immutable OCI digest reference. */
export function managedOci(reference: string): Extract<Image, { kind: "managed-oci" }> {
  if (!/^.+@sha256:[0-9a-fA-F]{64}$/.test(reference)) throw new Error("OCI image must contain an immutable SHA-256 digest");
  return { kind: "managed-oci", digestHex: digestHex(reference.slice(-64)) };
}
/** Constructs a custom image from a non-zero immutable SHA-256 digest. */
export function customImage(value: string): Extract<Image, { kind: "custom" }> { return { kind: "custom", digestHex: digestHex(value) }; }

export { SimulatedMachines } from "./memory.js";
export * from "./client.js";
export { HttpMachinesProvider, MachinesTransportError } from "./http.js";
export type { HttpMachinesOptions } from "./http.js";
