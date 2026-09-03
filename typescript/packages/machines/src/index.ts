/** Public shape-free Machines contracts. The deterministic provider is process-local only. */

export type IdempotencyKey = string;
export type MachineId = string;
export type CheckpointId = string;
export type OperationId = string;
declare const immutableOciReference: unique symbol;
export type ImmutableOciReference = string & { readonly [immutableOciReference]: true };

export type Image =
  | { readonly kind: "managed-oci"; readonly digestReference: ImmutableOciReference }
  | { readonly kind: "custom"; readonly digestHex: string }
  | { readonly kind: "checkpoint"; readonly checkpointId: CheckpointId };

export type Capability = "elastic-cpu" | "elastic-memory" | "live-checkpoint" | "live-fork" | "suspend-resume" | "live-movement";
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
export interface MachineObservation { readonly id: MachineId; readonly state: MachineState; readonly contract: MachineContract; readonly endpoints: readonly Endpoint[]; readonly lastCheckpoint: CheckpointId | null; readonly createdAtUnixMs: number; readonly changedAtUnixMs: number }
export interface CheckpointObservation { readonly id: CheckpointId; readonly source: MachineId; readonly contract: MachineContract; readonly forkable: boolean; readonly createdAtUnixMs: number }
export type Pressure = "customer-budget" | "machine-limit" | "service-saturation";
export type EventFact = { readonly kind: "state"; readonly state: MachineState } | { readonly kind: "pressure"; readonly pressure: Pressure } | { readonly kind: "capacity-changed" };
export interface MachineEvent { readonly machine: MachineId; readonly sequence: number; readonly observedAtUnixMs: number; readonly fact: EventFact }
export interface UsageReceipt { readonly machine: MachineId; readonly startUnixMs: number; readonly endUnixMs: number; readonly elasticCpuNs: bigint; readonly dedicatedCpuNs: bigint; readonly privateResidentByteSeconds: bigint; readonly durablePrivateBytes: bigint; readonly lineageSharedBytes: bigint; readonly egressBytes: bigint; readonly receipt: Uint8Array }
export type MutationOutcome = { readonly kind: "created"; readonly machine: MachineObservation } | { readonly kind: "checkpointed"; readonly checkpoint: CheckpointObservation } | { readonly kind: "forked"; readonly machines: readonly MachineObservation[] } | { readonly kind: "suspended" | "woken" | "machine-destroyed"; readonly machineId: MachineId } | { readonly kind: "suspension-policy-set"; readonly machineId: MachineId; readonly policy: SuspensionPolicy } | { readonly kind: "checkpoint-destroyed"; readonly checkpointId: CheckpointId };

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
  suspend(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  wake(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<MutationOutcome>;
  destroyMachine(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome>;
  destroyCheckpoint(checkpointId: CheckpointId, key: IdempotencyKey): Promise<MutationOutcome>;
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }>;
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt>;
  recover(key: IdempotencyKey): Promise<MutationOutcome>;
}

const capabilities: readonly Capability[] = ["elastic-cpu", "elastic-memory", "live-checkpoint", "live-fork", "suspend-resume", "live-movement"];
const revision = "2b58d52764ff9f662ec12b1aa029526543852dd34a13db4c878cfe5e0f13fc6a";
const clone = <T>(value: T): T => structuredClone(value);
const derivedId = (domain: string, key: string, index = 0): string => `${domain}:${key}:${index}`;
const immutableOciPattern = /^.+@sha256:[0-9a-fA-F]{64}$/;

/** Constructs a managed image only from an immutable OCI digest reference. */
export function managedOci(reference: string): Image {
  if (!immutableOciPattern.test(reference)) throw new Error("OCI image must contain an immutable SHA-256 digest");
  return { kind: "managed-oci", digestReference: reference as ImmutableOciReference };
}

function validateImage(image: Image): void {
  if (image.kind === "managed-oci" && !immutableOciPattern.test(image.digestReference)) throw new Error("managed OCI image is not immutable");
  if (image.kind === "custom" && (!/^[0-9a-fA-F]{64}$/.test(image.digestHex) || /^0+$/.test(image.digestHex))) throw new Error("custom image digest is invalid");
  if (image.kind === "checkpoint" && image.checkpointId.length === 0) throw new Error("checkpoint identity is empty");
}

/** Deterministic bounded simulator with no OS execution, isolation, durability, or availability. */
export class SimulatedMachines implements MachinesProvider {
  readonly assurance = "process-local-simulation" as const;
  readonly #machines = new Map<MachineId, MachineObservation>();
  readonly #checkpoints = new Map<CheckpointId, CheckpointObservation>();
  readonly #events = new Map<MachineId, MachineEvent[]>();
  readonly #replays = new Map<IdempotencyKey, { readonly intent: string; readonly outcome: MutationOutcome }>();
  #now = 1;

  async qualifyImage(image: Image): Promise<ImageQualification> { validateImage(image); return clone({ image, capabilities, compatibilityRevisionHex: revision }); }
  async create(request: CreateMachine): Promise<MutationOutcome> {
    validateImage(request.image);
    return this.#mutate(request.idempotencyKey, JSON.stringify(request, (_name, value: unknown) => typeof value === "bigint" ? value.toString() : value), () => {
      if (this.#machines.size >= 1024) throw new Error("simulation machine limit reached");
      if (request.compatibility.kind === "require" && request.compatibility.capabilities.some((value) => !capabilities.includes(value))) throw new Error("required capability is unavailable");
      const id = derivedId("machine", request.idempotencyKey); const now = this.#tick();
      const contract: MachineContract = { image: clone(request.image), compatibility: clone(request.compatibility), performance: request.performance, suspension: clone(request.suspension), expiration: clone(request.expiration), networkPolicyDigestHex: request.networkPolicyDigestHex, budgets: request.budgets, capabilities, compatibilityRevisionHex: revision };
      const machine: MachineObservation = { id, state: "running", contract, endpoints: [{ name: "default", uri: `memory://${id}` }], lastCheckpoint: null, createdAtUnixMs: now, changedAtUnixMs: now };
      this.#machines.set(id, machine); this.#event(id, { kind: "state", state: "running" }, now); return { kind: "created", machine };
    });
  }
  async inspectMachine(machineId: MachineId): Promise<MachineObservation> { return clone(this.#required(this.#machines, machineId)); }
  async listMachines(after: MachineId | null, limit: number): Promise<{ readonly machines: readonly MachineObservation[]; readonly next: MachineId | null }> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 256) throw new Error("machine page limit must be 1..=256");
    const values = [...this.#machines.values()].filter((value) => after === null || value.id > after).sort((left, right) => left.id.localeCompare(right.id));
    const machines = values.slice(0, limit); return clone({ machines, next: values.length > limit ? machines.at(-1)?.id ?? null : null });
  }
  async checkpoint(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `checkpoint:${machineId}`, () => { const source = this.#required(this.#machines, machineId); if (source.state !== "running" && source.state !== "suspended") throw new Error("machine is not checkpointable"); const id = derivedId("checkpoint", key); const now = this.#tick(); const checkpoint = { id, source: machineId, contract: clone(source.contract), forkable: true, createdAtUnixMs: now }; this.#checkpoints.set(id, checkpoint); this.#machines.set(machineId, { ...source, lastCheckpoint: id, changedAtUnixMs: now }); return { kind: "checkpointed", checkpoint }; }); }
  async inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation> { return clone(this.#required(this.#checkpoints, checkpointId)); }
  async fork(checkpointId: CheckpointId, count: number, performance: Performance, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `fork:${checkpointId}:${count}:${performance}`, () => { if (!Number.isInteger(count) || count < 1 || count > 1024) throw new Error("fork count must be 1..=1024"); const checkpoint = this.#required(this.#checkpoints, checkpointId); if (!checkpoint.forkable) throw new Error("checkpoint no longer accepts forks"); if (this.#machines.size + count > 1024) throw new Error("simulation machine limit reached"); const machines = Array.from({ length: count }, (_unused, index) => { const id = derivedId("machine", key, index); const now = this.#tick(); const value = { id, state: "running" as const, contract: { ...clone(checkpoint.contract), image: { kind: "checkpoint" as const, checkpointId }, performance }, endpoints: [{ name: "default", uri: `memory://${id}` }], lastCheckpoint: checkpointId, createdAtUnixMs: now, changedAtUnixMs: now }; this.#machines.set(id, value); this.#event(id, { kind: "state", state: "running" }, now); return value; }); return { kind: "forked", machines }; }); }
  async suspend(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#transition(machineId, key, "running", "suspended", "suspended"); }
  async wake(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#transition(machineId, key, "suspended", "running", "woken"); }
  async setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `policy:${machineId}:${JSON.stringify(policy)}`, () => { const value = this.#required(this.#machines, machineId); if (value.state === "destroyed") throw new Error("destroyed machine cannot change policy"); this.#machines.set(machineId, { ...value, contract: { ...value.contract, suspension: clone(policy) }, changedAtUnixMs: this.#tick() }); return { kind: "suspension-policy-set", machineId, policy }; }); }
  async destroyMachine(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `destroy-machine:${machineId}`, () => { const value = this.#required(this.#machines, machineId); const now = this.#tick(); this.#machines.set(machineId, { ...value, state: "destroyed", changedAtUnixMs: now }); this.#event(machineId, { kind: "state", state: "destroyed" }, now); return { kind: "machine-destroyed", machineId }; }); }
  async destroyCheckpoint(checkpointId: CheckpointId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `destroy-checkpoint:${checkpointId}`, () => { const value = this.#required(this.#checkpoints, checkpointId); this.#checkpoints.set(checkpointId, { ...value, forkable: false }); return { kind: "checkpoint-destroyed", checkpointId }; }); }
  async events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }> { this.#required(this.#machines, machineId); if (!Number.isInteger(limit) || limit < 1 || limit > 1024) throw new Error("event page limit must be 1..=1024"); const values = (this.#events.get(machineId) ?? []).filter((value) => afterSequence === null || value.sequence > afterSequence); const events = values.slice(0, limit); return clone({ events, nextSequence: values.length > limit ? events.at(-1)?.sequence ?? null : null }); }
  async usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { this.#required(this.#machines, machineId); if (!Number.isSafeInteger(startUnixMs) || !Number.isSafeInteger(endUnixMs) || startUnixMs >= endUnixMs) throw new Error("usage interval must be non-empty safe integers"); return { machine: machineId, startUnixMs, endUnixMs, elasticCpuNs: 0n, dedicatedCpuNs: 0n, privateResidentByteSeconds: 0n, durablePrivateBytes: 0n, lineageSharedBytes: 0n, egressBytes: 0n, receipt: new Uint8Array() }; }
  async recover(key: IdempotencyKey): Promise<MutationOutcome> { const value = this.#replays.get(key); if (value === undefined) throw new Error("operation not found"); return clone(value.outcome); }
  async #transition(machineId: MachineId, key: IdempotencyKey, required: MachineState, target: MachineState, kind: "suspended" | "woken"): Promise<MutationOutcome> { return this.#mutate(key, `${kind}:${machineId}`, () => { const value = this.#required(this.#machines, machineId); if (value.state !== required && value.state !== target) throw new Error("machine cannot perform transition"); const now = this.#tick(); this.#machines.set(machineId, { ...value, state: target, changedAtUnixMs: now }); this.#event(machineId, { kind: "state", state: target }, now); return { kind, machineId }; }); }
  #mutate(key: IdempotencyKey, intent: string, action: () => MutationOutcome): MutationOutcome { const replay = this.#replays.get(key); if (replay !== undefined) { if (replay.intent !== intent) throw new Error("idempotency key is bound to another intent"); return clone(replay.outcome); } if (this.#replays.size >= 4096) throw new Error("simulation operation limit reached"); const outcome = action(); this.#replays.set(key, { intent, outcome: clone(outcome) }); return clone(outcome); }
  #required<K, V>(values: Map<K, V>, id: K): V { const value = values.get(id); if (value === undefined) throw new Error("resource not found"); return value; }
  #event(machine: MachineId, fact: EventFact, observedAtUnixMs: number): void { const values = this.#events.get(machine) ?? []; if (values.length >= 4096) throw new Error("simulation event limit reached"); values.push({ machine, sequence: values.length + 1, observedAtUnixMs, fact }); this.#events.set(machine, values); }
  #tick(): number { if (this.#now >= Number.MAX_SAFE_INTEGER) throw new Error("simulation clock exhausted"); this.#now += 1; return this.#now; }
}
