/** Public shape-free Machines contracts. The deterministic provider is process-local only. */

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
/** What the children of one `forkMachine` inherited from their source: memory, processes, and disk, or a copy of the disk only (children boot fresh). */
export type ForkFidelity = "memory-and-disk" | "disk-only";
/** The fidelity a machine with `capabilities` forks at, or `null` when it cannot be forked live and the caller must fall back to checkpoint fork or a restart. `live-fork` takes precedence over `disk-fork`. */
export function forkFidelity(capabilities: readonly Capability[]): ForkFidelity | null { return capabilities.includes("live-fork") ? "memory-and-disk" : capabilities.includes("disk-fork") ? "disk-only" : null; }
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
  /**
   * Forks a running machine into `count` (1..=1024) fresh children without an intermediate checkpoint. The source must be running (else a conflict) and its contract must declare
   * `live-fork` or `disk-fork` (else unsupported; fall back to `checkpoint` plus `fork`, or a restart). Children inherit the source's exact contract with fresh identities and endpoints;
   * open network connections are never carried over. Destroy children before their source: a provider may refuse to destroy a source with live children. Not atomic for `count > 1`:
   * a provider may undo a failed attempt; replay `key` to finish, or cancel to undo, an indeterminate one. A provider may fan out by forking earlier children, which then inherit
   * whatever those executed after their own fork. Resolves to a `machine-forked` outcome.
   */
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

const allCapabilities: readonly Capability[] = ["elastic-cpu", "elastic-memory", "live-checkpoint", "live-fork", "suspend-resume", "live-movement", "disk-fork"];
const revision = "2b58d52764ff9f662ec12b1aa029526543852dd34a13db4c878cfe5e0f13fc6a";
const clone = <T>(value: T): T => structuredClone(value);
const derivedId = <Value extends string>(domain: string, key: string, index = 0): Value => `${domain}:${key}:${index}` as Value;
const immutableOciPattern = /^.+@sha256:[0-9a-fA-F]{64}$/;
const canonicalIntent = (value: unknown): string => {
  if (typeof value === "bigint") return `{"$bigint":${JSON.stringify(value.toString())}}`;
  if (Array.isArray(value)) return `[${value.map(canonicalIntent).join(",")}]`;
  if (value !== null && typeof value === "object") return `{${Object.entries(value).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0).map(([name, item]) => `${JSON.stringify(name)}:${canonicalIntent(item)}`).join(",")}}`;
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error("intent contains an unsupported value");
  return encoded;
};

/** Constructs a managed image only from an immutable OCI digest reference. */
export function managedOci(reference: string): Image {
  if (!immutableOciPattern.test(reference)) throw new Error("OCI image must contain an immutable SHA-256 digest");
  return { kind: "managed-oci", digestHex: reference.slice(-64).toLowerCase() };
}

function validateImage(image: Image): void {
  if ((image.kind === "managed-oci" || image.kind === "custom") && (!/^[0-9a-f]{64}$/.test(image.digestHex) || /^0+$/.test(image.digestHex))) throw new Error("image digest is invalid");
  if (image.kind === "checkpoint" && image.checkpointId.length === 0) throw new Error("checkpoint identity is empty");
}

export interface SimulatedMachinesOptions {
  /** Capabilities the simulator qualifies images with (default: every capability), for example without `live-fork` to exercise a caller's fallback path. Only admission and fork fidelity follow the set. */
  readonly capabilities?: readonly Capability[];
}

/** Deterministic bounded simulator with no OS execution, isolation, durability, or availability. It refuses to destroy a live-fork source while any of its children is not destroyed. */
export class SimulatedMachines implements MachinesProvider {
  readonly assurance = "process-local-simulation" as const;
  readonly #machines = new Map<MachineId, MachineObservation>();
  readonly #checkpoints = new Map<CheckpointId, CheckpointObservation>();
  readonly #events = new Map<MachineId, MachineEvent[]>();
  readonly #replays = new Map<IdempotencyKey, { readonly intent: string; readonly outcome: MutationOutcome }>();
  readonly #operations = new Map<OperationId, OperationObservation>();
  /** Live-fork child to its source. */
  readonly #forkSources = new Map<MachineId, MachineId>();
  readonly #capabilities: readonly Capability[];
  #now = 1;

  constructor(options: SimulatedMachinesOptions = {}) { this.#capabilities = Object.freeze([...new Set(options.capabilities ?? allCapabilities)]); }

  async qualifyImage(image: Image): Promise<ImageQualification> { validateImage(image); return clone({ image, capabilities: this.#capabilities, compatibilityRevisionHex: revision }); }
  async create(request: CreateMachine): Promise<MutationOutcome> {
    validateImage(request.image);
    return this.#mutate(request.idempotencyKey, canonicalIntent(request), () => {
      if (this.#machines.size >= 1024) throw new Error("simulation machine limit reached");
      if (request.compatibility.kind === "require" && request.compatibility.capabilities.some((value) => !this.#capabilities.includes(value))) throw new Error("required capability is unavailable");
      const id = derivedId<MachineId>("machine", request.idempotencyKey); const now = this.#tick();
      const contract: MachineContract = { image: clone(request.image), compatibility: clone(request.compatibility), performance: request.performance, suspension: clone(request.suspension), expiration: clone(request.expiration), networkPolicyDigestHex: request.networkPolicyDigestHex, budgets: clone(request.budgets), capabilities: clone(this.#capabilities), compatibilityRevisionHex: revision };
      const machine: MachineObservation = { id, state: "running", contract, endpoints: [{ name: "default", uri: `memory://${id}` }], lastCheckpoint: null, createdAtUnixMs: now, changedAtUnixMs: now };
      this.#machines.set(id, machine); this.#event(id, { kind: "state", state: "running" }, now); return { kind: "created", machine };
    });
  }
  async inspectMachine(machineId: MachineId): Promise<MachineObservation> { return clone(this.#required(this.#machines, machineId)); }
  async listMachines(after: MachineId | null, limit: number): Promise<{ readonly machines: readonly MachineObservation[]; readonly next: MachineId | null }> {
    if (!Number.isInteger(limit) || limit < 1 || limit > 256) throw new Error("machine page limit must be 1..=256");
    const values = [...this.#machines.values()].filter((value) => after === null || value.id > after).sort((left, right) => left.id < right.id ? -1 : left.id > right.id ? 1 : 0);
    const machines = values.slice(0, limit); return clone({ machines, next: values.length > limit ? machines.at(-1)?.id ?? null : null });
  }
  async checkpoint(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `checkpoint:${machineId}`, () => { const source = this.#required(this.#machines, machineId); if (source.state !== "running" && source.state !== "suspended") throw new Error("machine is not checkpointable"); const id = derivedId<CheckpointId>("checkpoint", key); const now = this.#tick(); const checkpoint = { id, source: machineId, contract: clone(source.contract), forkable: true, createdAtUnixMs: now }; this.#checkpoints.set(id, checkpoint); this.#machines.set(machineId, { ...source, lastCheckpoint: id, changedAtUnixMs: now }); return { kind: "checkpointed", checkpoint }; }); }
  async inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation> { return clone(this.#required(this.#checkpoints, checkpointId)); }
  async fork(checkpointId: CheckpointId, count: number, performance: Performance, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `fork:${checkpointId}:${count}:${performance}`, () => { if (!Number.isInteger(count) || count < 1 || count > 1024) throw new Error("fork count must be 1..=1024"); const checkpoint = this.#required(this.#checkpoints, checkpointId); if (!checkpoint.forkable) throw new Error("checkpoint no longer accepts forks"); if (this.#machines.size + count > 1024) throw new Error("simulation machine limit reached"); const machines = Array.from({ length: count }, (_unused, index) => { const id = derivedId<MachineId>("machine", key, index); const now = this.#tick(); const value = { id, state: "running" as const, contract: { ...clone(checkpoint.contract), image: { kind: "checkpoint" as const, checkpointId }, performance }, endpoints: [{ name: "default", uri: `memory://${id}` }], lastCheckpoint: checkpointId, createdAtUnixMs: now, changedAtUnixMs: now }; this.#machines.set(id, value); this.#event(id, { kind: "state", state: "running" }, now); return value; }); return { kind: "forked", machines }; }); }
  async forkMachine(machineId: MachineId, count: number, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `fork-machine:${machineId}:${count}`, () => { if (!Number.isInteger(count) || count < 1 || count > 1024) throw new Error("fork count must be 1..=1024"); const source = this.#required(this.#machines, machineId); const fidelity = forkFidelity(source.contract.capabilities); if (fidelity === null) throw new Error("machine contract does not declare live fork"); if (source.state !== "running") throw new Error("only a running machine can be forked"); if (this.#machines.size + count > 1024) throw new Error("simulation machine limit reached"); const children = Array.from({ length: count }, (_unused, index) => { const id = derivedId<MachineId>("machine", key, index); const now = this.#tick(); const value: MachineObservation = { id, state: "running", contract: clone(source.contract), endpoints: [{ name: "default", uri: `memory://${id}` }], lastCheckpoint: null, createdAtUnixMs: now, changedAtUnixMs: now }; this.#machines.set(id, value); this.#forkSources.set(id, machineId); this.#event(id, { kind: "state", state: "running" }, now); return value; }); return { kind: "machine-forked", source: machineId, fidelity, children }; }); }
  async suspend(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#transition(machineId, key, "running", "suspended", "suspended"); }
  async wake(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#transition(machineId, key, "suspended", "running", "woken"); }
  async setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `policy:${machineId}:${JSON.stringify(policy)}`, () => { const value = this.#required(this.#machines, machineId); if (value.state === "destroyed") throw new Error("destroyed machine cannot change policy"); this.#machines.set(machineId, { ...value, contract: { ...value.contract, suspension: clone(policy) }, changedAtUnixMs: this.#tick() }); return { kind: "suspension-policy-set", machineId, policy }; }); }
  async destroyMachine(machineId: MachineId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `destroy-machine:${machineId}`, () => { const value = this.#required(this.#machines, machineId); if (value.state === "destroyed") return { kind: "machine-destroyed", machineId }; if ([...this.#forkSources].some(([child, source]) => source === machineId && this.#machines.get(child)?.state !== "destroyed")) throw new Error("machine has live-fork children; destroy them first"); const now = this.#tick(); this.#machines.set(machineId, { ...value, state: "destroyed", changedAtUnixMs: now }); this.#event(machineId, { kind: "state", state: "destroyed" }, now); return { kind: "machine-destroyed", machineId }; }); }
  async destroyCheckpoint(checkpointId: CheckpointId, key: IdempotencyKey): Promise<MutationOutcome> { return this.#mutate(key, `destroy-checkpoint:${checkpointId}`, () => { const value = this.#required(this.#checkpoints, checkpointId); this.#checkpoints.set(checkpointId, { ...value, forkable: false }); return { kind: "checkpoint-destroyed", checkpointId }; }); }
  async events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }> { this.#required(this.#machines, machineId); if (!Number.isInteger(limit) || limit < 1 || limit > 1024) throw new Error("event page limit must be 1..=1024"); const values = (this.#events.get(machineId) ?? []).filter((value) => afterSequence === null || value.sequence > afterSequence); const events = values.slice(0, limit); return clone({ events, nextSequence: values.length > limit ? events.at(-1)?.sequence ?? null : null }); }
  async usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { this.#required(this.#machines, machineId); if (!Number.isSafeInteger(startUnixMs) || !Number.isSafeInteger(endUnixMs) || startUnixMs >= endUnixMs) throw new Error("usage interval must be non-empty safe integers"); return { machine: machineId, startUnixMs, endUnixMs, elasticCpuNs: 0n, dedicatedCpuNs: 0n, privateResidentByteSeconds: 0n, durablePrivateBytes: 0n, lineageReceiptSha256: new Uint8Array(32), egressBytes: 0n, receipt: new Uint8Array() }; }
  async recover(key: IdempotencyKey): Promise<MutationOutcome> { const value = this.#replays.get(key); if (value === undefined) throw new Error("operation not found"); return clone(value.outcome); }
  async recoverOperation(key: IdempotencyKey): Promise<OperationId> { const operation = derivedId<OperationId>("operation", key); this.#required(this.#operations, operation); return operation; }
  async inspectOperation(operationId: OperationId): Promise<OperationObservation> { return clone(this.#required(this.#operations, operationId)); }
  async cancel(operationId: OperationId): Promise<OperationObservation> { const operation = this.#required(this.#operations, operationId); if (operation.phase !== "pending") return clone(operation); const cancelled = { ...operation, phase: "cancelled" as const }; this.#operations.set(operationId, cancelled); return clone(cancelled); }
  async *watchOperation(operationId: OperationId): AsyncIterable<OperationObservation> { yield await this.inspectOperation(operationId); }
  async #transition(machineId: MachineId, key: IdempotencyKey, required: MachineState, target: MachineState, kind: "suspended" | "woken"): Promise<MutationOutcome> { return this.#mutate(key, `${kind}:${machineId}`, () => { const value = this.#required(this.#machines, machineId); if (value.state === target) return { kind, machineId }; if (value.state !== required) throw new Error("machine cannot perform transition"); const now = this.#tick(); this.#machines.set(machineId, { ...value, state: target, changedAtUnixMs: now }); this.#event(machineId, { kind: "state", state: target }, now); return { kind, machineId }; }); }
  #mutate(key: IdempotencyKey, intent: string, action: () => MutationOutcome): MutationOutcome { const replay = this.#replays.get(key); if (replay !== undefined) { if (replay.intent !== intent) throw new Error("idempotency key is bound to another intent"); return clone(replay.outcome); } if (this.#replays.size >= 4096) throw new Error("simulation operation limit reached"); const outcome = action(); this.#replays.set(key, { intent, outcome: clone(outcome) }); const operation = derivedId<OperationId>("operation", key); this.#operations.set(operation, { id: operation, phase: "succeeded" }); return clone(outcome); }
  #required<K, V>(values: Map<K, V>, id: K): V { const value = values.get(id); if (value === undefined) throw new Error("resource not found"); return value; }
  #event(machine: MachineId, fact: EventFact, observedAtUnixMs: number): void { const values = this.#events.get(machine) ?? []; if (values.length >= 4096) throw new Error("simulation event limit reached"); values.push({ machine, sequence: values.length + 1, observedAtUnixMs, fact }); this.#events.set(machine, values); }
  #tick(): number { if (this.#now >= Number.MAX_SAFE_INTEGER) throw new Error("simulation clock exhausted"); this.#now += 1; return this.#now; }
}

export * from "./client.js";
export * from "./http.js";
