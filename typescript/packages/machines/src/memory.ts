import { create, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";
import type {
  Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy, CreateMachine,
  ExpirationPolicy, IdempotencyKey, Image, ImageQualification, MachineEvent, MachineId,
  MachineObservation, MachinesProvider, MutationOutcome, OperationId, OperationObservation,
  Performance, SuspensionPolicy, UsageReceipt,
} from "./index.js";
import { wireResults } from "./wire-results.js";
import { digestHex } from "./digest.js";

type Binding = import("../generated/wasm/acyclic_machines_wasm.js").SimulatedMachinesBinding;
let wasmModule: Promise<typeof import("../generated/wasm/acyclic_machines_wasm.js")> | undefined;
async function loadWasm(): Promise<typeof import("../generated/wasm/acyclic_machines_wasm.js")> {
  wasmModule ??= import("../generated/wasm/acyclic_machines_wasm.js").then(async module => {
    const node = (globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node;
    if (node === undefined) await module.default();
    else {
      const fsPath: string = "node:fs/promises";
      const { readFile } = await import(fsPath) as { readFile(url: URL): Promise<Uint8Array> };
      await module.default({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_machines_wasm_bg.wasm", import.meta.url)) });
    }
    return module;
  }).catch((error: unknown) => { wasmModule = undefined; throw error; });
  return wasmModule;
}

type Normalize = typeof import("../generated/wasm/acyclic_machines_wasm.js").normalizeIdentityBytes;
function exact(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError("machine integer is outside the exact JavaScript range");
  return value;
}
function uint32(value: number): number {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) throw new RangeError("machine integer is outside the uint32 wire range");
  return value;
}
function bytes(hex: string): Uint8Array {
  const canonical = digestHex(hex);
  return Uint8Array.from({ length: 32 }, (_unused, index) => Number.parseInt(canonical.slice(index * 2, index * 2 + 2), 16));
}
function image(value: Image, normalize: Normalize): wire.Image {
  switch (value.kind) {
    case "managed-oci": return create(wire.ImageSchema, { kind: wire.ImageKind.MANAGED_OCI, immutableReference: { case: "managedDigest", value: bytes(value.digestHex) } });
    case "custom": return create(wire.ImageSchema, { kind: wire.ImageKind.CUSTOM, immutableReference: { case: "customDigest", value: bytes(value.digestHex) } });
    case "checkpoint": return create(wire.ImageSchema, { kind: wire.ImageKind.CHECKPOINT, immutableReference: { case: "checkpoint", value: { value: normalize(value.checkpointId) } } });
  }
}
const capabilities: Record<Capability, wire.Capability> = {
  "elastic-cpu": wire.Capability.ELASTIC_CPU, "elastic-memory": wire.Capability.ELASTIC_MEMORY,
  "live-checkpoint": wire.Capability.LIVE_CHECKPOINT, "live-fork": wire.Capability.LIVE_FORK,
  "suspend-resume": wire.Capability.SUSPEND_RESUME, "live-movement": wire.Capability.LIVE_MOVEMENT,
  "disk-fork": wire.Capability.DISK_FORK,
};
function compatibility(value: CompatibilityPolicy): wire.CompatibilityPolicy {
  return create(wire.CompatibilityPolicySchema, value.kind === "best-effort"
    ? { mode: wire.CompatibilityMode.BEST_EFFORT }
    : { mode: wire.CompatibilityMode.REQUIRE, required: value.capabilities.map(item => capabilities[item]) });
}
function suspension(value: SuspensionPolicy): wire.SuspensionPolicy {
  return create(wire.SuspensionPolicySchema, { policy: value.kind === "manual"
    ? { case: "manual", value: true } : { case: "afterIdleMs", value: BigInt(exact(value.milliseconds)) } });
}
function expiration(value: ExpirationPolicy): wire.ExpirationPolicy {
  switch (value.kind) {
    case "never": return create(wire.ExpirationPolicySchema, { kind: wire.ExpirationKind.NEVER });
    case "max-age": return create(wire.ExpirationPolicySchema, { kind: wire.ExpirationKind.MAX_AGE, valueMs: BigInt(exact(value.milliseconds)) });
    case "idle": return create(wire.ExpirationPolicySchema, { kind: wire.ExpirationKind.IDLE, valueMs: BigInt(exact(value.milliseconds)) });
    case "at": return create(wire.ExpirationPolicySchema, { kind: wire.ExpirationKind.AT, valueMs: BigInt(exact(value.milliseconds)) });
  }
}
function performance(value: Performance): wire.Performance {
  if (value === "elastic") return wire.Performance.ELASTIC;
  if (value === "dedicated") return wire.Performance.DEDICATED;
  throw new TypeError("unknown machine performance");
}
async function canonical<Id extends MachineId | CheckpointId | OperationId>(value: Id): Promise<Id> {
  return wireResults.identity((await loadWasm()).normalizeIdentityBytes(value)) as Id;
}

/** Deterministic bounded simulator backed by the canonical Rust provider. */
export interface SimulatedMachinesOptions {
  readonly capabilities?: readonly Capability[];
}
export class SimulatedMachines implements MachinesProvider {
  readonly assurance = "process-local-simulation" as const;
  #binding: Promise<Binding> | undefined;
  readonly #capabilities: readonly Capability[] | undefined;
  constructor(options: SimulatedMachinesOptions = {}) {
    this.#capabilities = options.capabilities === undefined ? undefined : [...options.capabilities];
  }
  #ready(): Promise<Binding> {
    this.#binding ??= loadWasm().then(module => this.#capabilities === undefined
      ? new module.SimulatedMachinesBinding()
      : module.SimulatedMachinesBinding.with_capabilities(Int32Array.from(this.#capabilities.map(value => capabilities[value]))))
      .catch((error: unknown) => { this.#binding = undefined; throw error; });
    return this.#binding;
  }
  async qualifyImage(value: Image): Promise<ImageQualification> {
    const request = image(value, (await loadWasm()).normalizeIdentityBytes);
    return wireResults.qualification(await (await this.#ready()).qualify_image(toBinary(wire.ImageSchema, request)), wireResults.image(request));
  }
  async create(request: CreateMachine): Promise<MutationOutcome> {
    const { normalizeIdentityBytes } = await loadWasm();
    const encoded = toBinary(wire.CreateMachineRequestSchema, create(wire.CreateMachineRequestSchema, {
      protocol: { major: 1, minor: 0 },
      idempotencyKey: { value: normalizeIdentityBytes(request.idempotencyKey) },
      image: image(request.image, normalizeIdentityBytes),
      compatibility: compatibility(request.compatibility), performance: performance(request.performance),
      suspension: suspension(request.suspension), expiration: expiration(request.expiration),
      networkPolicyDigest: bytes(request.networkPolicyDigestHex),
      budgets: { spendMicros: request.budgets.spendMicros, concurrency: uint32(request.budgets.concurrency) },
    }));
    return wireResults.mutation(await (await this.#ready()).create(encoded), { kind: "created" });
  }
  async inspectMachine(id: MachineId): Promise<MachineObservation> {
    return wireResults.machine(await (await this.#ready()).inspect_machine(id), await canonical(id));
  }
  async listMachines(after: MachineId | null, limit: number): Promise<{ readonly machines: readonly MachineObservation[]; readonly next: MachineId | null }> {
    return wireResults.machines(await (await this.#ready()).list_machines(after, uint32(limit)));
  }
  async checkpoint(id: MachineId, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).checkpoint(id, key), { kind: "checkpointed", source: await canonical(id) });
  }
  async inspectCheckpoint(id: CheckpointId): Promise<CheckpointObservation> {
    return wireResults.checkpoint(await (await this.#ready()).inspect_checkpoint(id), await canonical(id));
  }
  async fork(id: CheckpointId, count: number, grant: Performance, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).fork(id, uint32(count), performance(grant), key),
      { kind: "forked", checkpointId: await canonical(id), count, performance: grant });
  }
  async forkMachine(id: MachineId, count: number, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).fork_machine(id, uint32(count), key),
      { kind: "machine-forked", machineId: await canonical(id), count });
  }
  async suspend(id: MachineId, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).suspend(id, key), { kind: "suspended", machineId: await canonical(id) });
  }
  async wake(id: MachineId, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).wake(id, key), { kind: "woken", machineId: await canonical(id) });
  }
  async setSuspensionPolicy(id: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).set_suspension_policy(id, toBinary(wire.SuspensionPolicySchema, suspension(policy)), key),
      { kind: "suspension-policy-set", machineId: await canonical(id), policy });
  }
  async destroyMachine(id: MachineId, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).destroy_machine(id, key), { kind: "machine-destroyed", machineId: await canonical(id) });
  }
  async destroyCheckpoint(id: CheckpointId, key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).destroy_checkpoint(id, key), { kind: "checkpoint-destroyed", checkpointId: await canonical(id) });
  }
  async events(id: MachineId, afterSequence: number | null, limit: number): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }> {
    return wireResults.events(await (await this.#ready()).events(id, afterSequence === null ? undefined : BigInt(exact(afterSequence)), uint32(limit)), await canonical(id), afterSequence);
  }
  async usage(id: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> {
    return wireResults.usage(await (await this.#ready()).usage(id, BigInt(exact(startUnixMs)), BigInt(exact(endUnixMs))), await canonical(id), startUnixMs, endUnixMs);
  }
  async recover(key: IdempotencyKey): Promise<MutationOutcome> {
    return wireResults.mutation(await (await this.#ready()).recover(key));
  }
  async recoverOperation(key: IdempotencyKey): Promise<OperationId> {
    return wireResults.operationId(await (await this.#ready()).recover_operation(key));
  }
  async inspectOperation(id: OperationId): Promise<OperationObservation> {
    return wireResults.operation(await (await this.#ready()).inspect_operation(id), await canonical(id));
  }
  async cancel(id: OperationId): Promise<OperationObservation> {
    return wireResults.operation(await (await this.#ready()).cancel(id), await canonical(id));
  }
  async *watchOperation(id: OperationId): AsyncIterable<OperationObservation> {
    for (const value of wireResults.operations(await (await this.#ready()).watch_operation(id), await canonical(id))) yield value;
  }
}
