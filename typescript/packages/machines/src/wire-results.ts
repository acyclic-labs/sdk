import { fromBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";
import type {
  Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy, EventFact,
  ExpirationPolicy, Image, ImageQualification, MachineContract, MachineEvent, MachineId,
  MachineObservation, MachineState, MutationOutcome, OperationId, OperationObservation,
  OperationPhase, Performance, Pressure, SuspensionPolicy, UsageReceipt, ForkFidelity,
} from "./index.js";
import { machinesResponseValidation as validate } from "./http.js";
import { digestBytes } from "./digest.js";

function required<T>(value: T | undefined, name: string): T {
  if (value === undefined) throw new TypeError(`missing machine ${name}`);
  return value;
}
function exact(value: bigint): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) throw new RangeError("machine integer is outside the exact JavaScript range");
  return Number(value);
}
function hex(value: Uint8Array): string { return Array.from(value, byte => byte.toString(16).padStart(2, "0")).join(""); }
function uuid(value: Uint8Array): string {
  if (value.byteLength !== 16 || value.every(byte => byte === 0)) throw new TypeError("machine identity is invalid");
  const digits = hex(value);
  return `${digits.slice(0, 8)}-${digits.slice(8, 12)}-${digits.slice(12, 16)}-${digits.slice(16, 20)}-${digits.slice(20)}`;
}
function machineId(value: wire.MachineId | undefined): MachineId { return uuid(required(value, "machine ID").value) as MachineId; }
function checkpointId(value: wire.CheckpointId | undefined): CheckpointId { return uuid(required(value, "checkpoint ID").value) as CheckpointId; }
function operationId(value: wire.OperationId | undefined): OperationId { return uuid(required(value, "operation ID").value) as OperationId; }

function image(value: wire.Image | undefined): Image {
  const source = required(value, "image");
  const reference = source.immutableReference;
  switch (reference.case) {
    case "managedDigest":
      if (source.kind !== wire.ImageKind.MANAGED_OCI) break;
      return { kind: "managed-oci", digestHex: digestBytes(reference.value) };
    case "customDigest":
      if (source.kind !== wire.ImageKind.CUSTOM) break;
      return { kind: "custom", digestHex: digestBytes(reference.value) };
    case "checkpoint":
      if (source.kind !== wire.ImageKind.CHECKPOINT) break;
      return { kind: "checkpoint", checkpointId: checkpointId(reference.value) };
  }
  throw new TypeError("machine image kind and reference disagree");
}
const capabilities: Partial<Record<wire.Capability, Capability>> = {
  [wire.Capability.ELASTIC_CPU]: "elastic-cpu",
  [wire.Capability.ELASTIC_MEMORY]: "elastic-memory",
  [wire.Capability.LIVE_CHECKPOINT]: "live-checkpoint",
  [wire.Capability.LIVE_FORK]: "live-fork",
  [wire.Capability.SUSPEND_RESUME]: "suspend-resume",
  [wire.Capability.LIVE_MOVEMENT]: "live-movement",
  [wire.Capability.DISK_FORK]: "disk-fork",
};
function capability(value: wire.Capability): Capability { return required(capabilities[value], "capability"); }
function compatibility(value: wire.CompatibilityPolicy | undefined): CompatibilityPolicy {
  const policy = required(value, "compatibility");
  switch (policy.mode) {
    case wire.CompatibilityMode.BEST_EFFORT:
      if (policy.required.length !== 0) throw new TypeError("best-effort compatibility has required capabilities");
      return { kind: "best-effort" };
    case wire.CompatibilityMode.REQUIRE: return { kind: "require", capabilities: policy.required.map(capability) };
    default: throw new TypeError("machine compatibility mode is invalid");
  }
}
function performance(value: wire.Performance): Performance {
  switch (value) {
    case wire.Performance.ELASTIC: return "elastic";
    case wire.Performance.DEDICATED: return "dedicated";
    default: throw new TypeError("machine performance is invalid");
  }
}
function suspension(value: wire.SuspensionPolicy | undefined): SuspensionPolicy {
  const policy = required(value, "suspension").policy;
  switch (policy.case) {
    case "manual":
      if (!policy.value) throw new TypeError("manual suspension is invalid");
      return { kind: "manual" };
    case "afterIdleMs": return { kind: "after-idle", milliseconds: exact(policy.value) };
    default: throw new TypeError("machine suspension policy is invalid");
  }
}
function expiration(value: wire.ExpirationPolicy | undefined): ExpirationPolicy {
  const policy = required(value, "expiration");
  switch (policy.kind) {
    case wire.ExpirationKind.NEVER:
      if (policy.valueMs !== 0n) throw new TypeError("never expiration has a deadline");
      return { kind: "never" };
    case wire.ExpirationKind.MAX_AGE: return { kind: "max-age", milliseconds: exact(policy.valueMs) };
    case wire.ExpirationKind.AT: return { kind: "at", milliseconds: exact(policy.valueMs) };
    case wire.ExpirationKind.IDLE: return { kind: "idle", milliseconds: exact(policy.valueMs) };
    default: throw new TypeError("machine expiration policy is invalid");
  }
}
function contract(value: wire.MachineContract | undefined): MachineContract {
  const item = required(value, "contract");
  const budgets = required(item.budgets, "budgets");
  return {
    image: image(item.image), capabilities: item.capabilities.map(capability),
    compatibility: compatibility(item.compatibility), compatibilityRevisionHex: digestBytes(item.compatibilityRevision),
    performance: performance(item.performance), suspension: suspension(item.suspension),
    expiration: expiration(item.expiration), networkPolicyDigestHex: digestBytes(item.networkPolicyDigest),
    budgets: { spendMicros: budgets.spendMicros, concurrency: budgets.concurrency },
  };
}
const states: Partial<Record<wire.MachineStatus, MachineState>> = {
  [wire.MachineStatus.STARTING]: "starting",
  [wire.MachineStatus.RUNNING]: "running",
  [wire.MachineStatus.SUSPENDING]: "suspending",
  [wire.MachineStatus.SUSPENDED]: "suspended",
  [wire.MachineStatus.WAKING]: "waking",
  [wire.MachineStatus.DESTROYING]: "destroying",
  [wire.MachineStatus.DESTROYED]: "destroyed",
  [wire.MachineStatus.FAILED]: "failed",
  [wire.MachineStatus.INDETERMINATE]: "indeterminate",
};
function state(value: wire.MachineStatus): MachineState { return required(states[value], "state"); }
function machine(value: wire.MachineState): MachineObservation {
  return {
    id: machineId(value.machine), state: state(value.status), contract: contract(value.contract),
    endpoints: value.endpoints.map(({ name, uri }) => ({ name, uri })),
    lastCheckpoint: value.lastCheckpoint === undefined ? null : checkpointId(value.lastCheckpoint),
    createdAtUnixMs: exact(value.createdAtUnixMs), changedAtUnixMs: exact(value.changedAtUnixMs),
  };
}
function checkpoint(value: wire.CheckpointState): CheckpointObservation {
  return {
    id: checkpointId(value.checkpoint), source: machineId(value.source), contract: contract(value.contract),
    forkable: value.forkable, createdAtUnixMs: exact(value.createdAtUnixMs),
  };
}
const phases: Partial<Record<wire.OperationStatus, OperationPhase>> = {
  [wire.OperationStatus.PENDING]: "pending",
  [wire.OperationStatus.SUCCEEDED]: "succeeded",
  [wire.OperationStatus.CANCELLED]: "cancelled",
  [wire.OperationStatus.INDETERMINATE]: "indeterminate",
  [wire.OperationStatus.FAILED]: "failed",
};
function operation(value: wire.OperationState): OperationObservation {
  return { id: operationId(value.operation), phase: required(phases[value.status], "operation phase") };
}
const pressures: Partial<Record<wire.PressureKind, Pressure>> = {
  [wire.PressureKind.CUSTOMER_BUDGET]: "customer-budget",
  [wire.PressureKind.MACHINE_LIMIT]: "machine-limit",
  [wire.PressureKind.SERVICE_SATURATION]: "service-saturation",
};
function fact(value: wire.MachineEvent): EventFact {
  switch (value.kind) {
    case wire.EventKind.STATE: return { kind: "state", state: state(value.state) };
    case wire.EventKind.PRESSURE: return { kind: "pressure", pressure: required(pressures[value.pressure], "pressure") };
    case wire.EventKind.CAPACITY: return { kind: "capacity-changed" };
    default: throw new TypeError("machine event kind is invalid");
  }
}
function event(value: wire.MachineEvent): MachineEvent {
  return {
    machine: machineId(value.machine), sequence: exact(value.sequence),
    observedAtUnixMs: exact(value.observedAtUnixMs), fact: fact(value),
  };
}
function usage(value: wire.UsageReceipt): UsageReceipt {
  return {
    machine: machineId(value.machine), startUnixMs: exact(value.startUnixMs), endUnixMs: exact(value.endUnixMs),
    elasticCpuNs: value.elasticCpuNs, dedicatedCpuNs: value.dedicatedCpuNs,
    privateResidentByteSeconds: value.privateResidentByteSeconds, durablePrivateBytes: value.durablePrivateBytes,
    lineageReceiptSha256: value.lineageReceiptSha256, egressBytes: value.egressBytes, receipt: value.receipt,
  };
}
function mutation(value: wire.MutationOutcome): MutationOutcome {
  switch (value.result.case) {
    case "created": return { kind: "created", machine: machine(value.result.value) };
    case "checkpointed": return { kind: "checkpointed", checkpoint: checkpoint(value.result.value) };
    case "forked": return { kind: "forked", machines: value.result.value.machines.map(machine) };
    case "machineForked": {
      const result = value.result.value;
      const fidelity: ForkFidelity = result.fidelity === wire.ForkFidelity.MEMORY_AND_DISK
        ? "memory-and-disk"
        : result.fidelity === wire.ForkFidelity.DISK_ONLY
          ? "disk-only"
          : (() => { throw new TypeError("machine fork fidelity is invalid"); })();
      return { kind: "machine-forked", source: machineId(result.source), fidelity, children: result.children.map(machine) };
    }
    case "suspended": return { kind: "suspended", machineId: machineId(value.result.value) };
    case "woken": return { kind: "woken", machineId: machineId(value.result.value) };
    case "suspensionPolicySet": return { kind: "suspension-policy-set", machineId: machineId(value.result.value.machine), policy: suspension(value.result.value.policy) };
    case "machineDestroyed": return { kind: "machine-destroyed", machineId: machineId(value.result.value) };
    case "checkpointDestroyed": return { kind: "checkpoint-destroyed", checkpointId: checkpointId(value.result.value) };
    default: throw new TypeError("machine mutation result is missing");
  }
}

export const wireResults = {
  image,
  identity: uuid,
  qualification(bytes: Uint8Array, expected: Image): ImageQualification {
    const value = fromBinary(wire.ImageQualificationSchema, bytes);
    const result = { image: image(value.image), capabilities: value.capabilities.map(capability), compatibilityRevisionHex: digestBytes(value.compatibilityRevision) };
    return validate.qualification(result, expected);
  },
  mutation(bytes: Uint8Array, expected?: Parameters<typeof validate.checkedMutation>[1]): MutationOutcome {
    const outcome = validate.mutation(mutation(fromBinary(wire.MutationOutcomeSchema, bytes)));
    return expected === undefined ? outcome : validate.checkedMutation(outcome, expected);
  },
  machine(bytes: Uint8Array, expected: MachineId): MachineObservation { return validate.machineFor(machine(fromBinary(wire.MachineStateSchema, bytes)), expected); },
  machines(bytes: Uint8Array) {
    const value = fromBinary(wire.MachinePageSchema, bytes);
    return validate.machinePage({ machines: value.machines.map(machine), next: value.next === undefined ? null : machineId(value.next) });
  },
  checkpoint(bytes: Uint8Array, expected: CheckpointId): CheckpointObservation { return validate.checkpointFor(checkpoint(fromBinary(wire.CheckpointStateSchema, bytes)), expected); },
  events(bytes: Uint8Array, expected: MachineId, after: number | null) {
    const value = fromBinary(wire.EventPageSchema, bytes);
    return validate.eventPageFor({ events: value.events.map(event), nextSequence: value.nextSequence === 0n ? null : exact(value.nextSequence) }, expected, after);
  },
  usage(bytes: Uint8Array, expected: MachineId, start: number, end: number): UsageReceipt {
    return validate.usageFor(usage(fromBinary(wire.UsageReceiptSchema, bytes)), expected, start, end);
  },
  operation(bytes: Uint8Array, expected: OperationId): OperationObservation {
    return validate.operationFor(operation(fromBinary(wire.OperationStateSchema, bytes)), expected);
  },
  operations(bytes: Uint8Array, expected: OperationId): readonly OperationObservation[] {
    return fromBinary(wire.OperationPageSchema, bytes).operations.map(item => validate.operationFor(operation(item), expected));
  },
  operationId(bytes: Uint8Array): OperationId { return operationId(fromBinary(wire.OperationIdSchema, bytes)); },
} as const;

/** Exact u64 decoder shared with the boundary fixture. */
export function decodeUsageReceipt(bytes: Uint8Array): UsageReceipt {
  return usage(fromBinary(wire.UsageReceiptSchema, bytes));
}
