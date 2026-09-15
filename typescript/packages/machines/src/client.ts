import type {
  CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, MachineEvent, MachineId,
  MachineObservation, MachinesProvider, MutationOutcome, OperationId, OperationObservation,
  Performance, SuspensionPolicy, UsageReceipt,
} from "./index.js";
import { HttpMachinesProvider } from "./http.js";

export interface MachinesEnvironment {
  readonly endpoint: string;
  readonly token: string;
}

export interface MachineListOptions {
  /** Exclusive cursor. */
  readonly after?: MachineId;
  /** Per-request page size. */
  readonly pageSize?: number;
  /** Hard bound on the number of yielded Machines. */
  readonly maximum?: number;
}

export class Machines {
  constructor(readonly provider: MachinesProvider) {}
  static fromEnv(environment?: Partial<MachinesEnvironment>): Machines { return new Machines(new HttpMachinesProvider({ endpoint: environment?.endpoint ?? environmentValue("ACYCLIC_MACHINES_ENDPOINT"), token: environment?.token ?? environmentValue("ACYCLIC_MACHINES_TOKEN") })); }
  qualifyImage(image: import("./index.js").Image): Promise<import("./index.js").ImageQualification> { return this.provider.qualifyImage(image); }
  async create(request: CreateMachine): Promise<Machine> { const outcome = await this.provider.create(request); return new Machine(this.provider, expectOutcome(outcome, "created").machine.id); }
  async attach(id: MachineId): Promise<Machine> { await this.provider.inspectMachine(id); return new Machine(this.provider, id); }
  machine(id: MachineId): Machine { return new Machine(this.provider, id); }
  checkpoint(id: CheckpointId): Checkpoint { return new Checkpoint(this.provider, id); }
  operation(id: OperationId): Operation { return new Operation(this.provider, id); }
  /** Observes a durable operation by its exact retained operation identity. */
  recover(id: OperationId): Promise<OperationObservation> { return this.provider.inspectOperation(id); }
  /** Recovers the mutation outcome associated with an idempotency key. */
  recoverMutation(key: IdempotencyKey): Promise<MutationOutcome> { return this.provider.recover(key); }
  async recoverOperation(key: IdempotencyKey): Promise<Operation> { return this.operation(await this.provider.recoverOperation(key)); }
  async *list(options: MachineListOptions = {}): AsyncIterable<Machine> {
    const pageSize = options.pageSize ?? 256;
    const maximum = options.maximum ?? 1024;
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 256) throw new RangeError("pageSize must be 1..=256");
    if (!Number.isSafeInteger(maximum) || maximum < 0 || maximum > 65_536) throw new RangeError("maximum must be 0..=65536");
    let cursor = options.after ?? null;
    let yielded = 0;
    while (yielded < maximum) {
      const page = await this.provider.listMachines(cursor, Math.min(pageSize, maximum - yielded));
      for (const observation of page.machines) {
        yield new Machine(this.provider, observation.id);
        yielded += 1;
        if (yielded === maximum) return;
      }
      if (page.next === null) return;
      if (page.next === cursor || page.machines.length === 0) throw new Error("machine listing did not advance its cursor");
      cursor = page.next;
    }
  }
}

export class Machine {
  constructor(readonly provider: MachinesProvider, readonly id: MachineId) {}
  inspect(): Promise<MachineObservation> { return this.provider.inspectMachine(this.id); }
  async checkpoint(key: IdempotencyKey): Promise<Checkpoint> { return new Checkpoint(this.provider, expectOutcome(await this.provider.checkpoint(this.id, key), "checkpointed").checkpoint.id); }
  suspend(key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "suspended" }>> { return this.#outcome("suspended", this.provider.suspend(this.id, key)); }
  wake(key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "woken" }>> { return this.#outcome("woken", this.provider.wake(this.id, key)); }
  setSuspensionPolicy(policy: SuspensionPolicy, key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "suspension-policy-set" }>> { return this.#outcome("suspension-policy-set", this.provider.setSuspensionPolicy(this.id, policy, key)); }
  destroy(key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "machine-destroyed" }>> { return this.#outcome("machine-destroyed", this.provider.destroyMachine(this.id, key)); }
  events(afterSequence: number | null = null, limit = 256): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }> { return this.provider.events(this.id, afterSequence, limit); }
  usage(startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { return this.provider.usage(this.id, startUnixMs, endUnixMs); }
  async #outcome<Kind extends MutationOutcome["kind"]>(kind: Kind, value: Promise<MutationOutcome>): Promise<Extract<MutationOutcome, { kind: Kind }>> { return expectOutcome(await value, kind); }
}

export class Checkpoint {
  constructor(readonly provider: MachinesProvider, readonly id: CheckpointId) {}
  inspect(): Promise<CheckpointObservation> { return this.provider.inspectCheckpoint(this.id); }
  async fork(count: number, performance: Performance, key: IdempotencyKey): Promise<readonly Machine[]> { return expectOutcome(await this.provider.fork(this.id, count, performance, key), "forked").machines.map(value => new Machine(this.provider, value.id)); }
  destroy(key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "checkpoint-destroyed" }>> { return this.provider.destroyCheckpoint(this.id, key).then(value => expectOutcome(value, "checkpoint-destroyed")); }
}

export class Operation {
  constructor(readonly provider: MachinesProvider, readonly id: OperationId) {}
  inspect(): Promise<OperationObservation> { return this.provider.inspectOperation(this.id); }
  cancel(): Promise<OperationObservation> { return this.provider.cancel(this.id); }
  watch(): AsyncIterable<OperationObservation> { return this.provider.watchOperation(this.id); }
}

function expectOutcome<Kind extends MutationOutcome["kind"]>(outcome: MutationOutcome, kind: Kind): Extract<MutationOutcome, { kind: Kind }> { if (outcome.kind !== kind) throw new MachineOutcomeError(kind, outcome); return outcome as Extract<MutationOutcome, { kind: Kind }>; }
export class MachineOutcomeError extends Error { constructor(readonly expected: MutationOutcome["kind"], readonly outcome: MutationOutcome) { super(`expected ${expected} outcome, received ${outcome.kind}`); } }
function environmentValue(name: string): string { const runtime = globalThis as typeof globalThis & { process?: { env?: Readonly<Record<string, string | undefined>> } }; const value = runtime.process?.env?.[name]; if (!value?.trim()) throw new TypeError(`${name} is required`); return value; }
