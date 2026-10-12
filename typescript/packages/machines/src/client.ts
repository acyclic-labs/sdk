import type { CheckpointId, CreateMachine, MachineEventPage, ForkFidelity, IdempotencyKey, MachineId, MachinesProvider, MutationOutcome, SuspensionPolicy } from "./index.js";
import { MANAGED_OCI_CONTRACT } from "./managed-oci-contract.js";
import { validatePageSize } from "../generated/wasm/acyclic_machines_wasm.js";

export interface MachineListOptions {
  /** Exclusive cursor. */
  readonly after?: MachineId;
  /** Per-request page size. */
  readonly pageSize?: number;
  /** Hard bound on the number of yielded Machines. */
  readonly maximum?: number;
}

/** Creates a machine and checks the outcome kind. */
export async function createMachine(provider: MachinesProvider, request: CreateMachine): Promise<MachineId> {
  const outcome = await provider.create(request);
  if (outcome.kind !== "created") throw new MachineOutcomeError("created", outcome);
  return outcome.machine.id;
}
/** Checkpoints a machine and checks the outcome kind. */
export async function checkpointMachine(provider: MachinesProvider, id: MachineId, key: IdempotencyKey): Promise<CheckpointId> {
  const outcome = await provider.checkpoint(id, key);
  if (outcome.kind !== "checkpointed") throw new MachineOutcomeError("checkpointed", outcome);
  return outcome.checkpoint.id;
}
/** Children of one live fork and its declared fidelity. */
export interface MachineFork { readonly fidelity: ForkFidelity; readonly children: readonly MachineId[] }
export async function forkMachine(provider: MachinesProvider, id: MachineId, count: number, key: IdempotencyKey): Promise<MachineFork> {
  const outcome = await provider.forkMachine(id, count, key);
  if (outcome.kind !== "machine-forked" || outcome.source !== id) throw new MachineOutcomeError("machine-forked", outcome);
  return { fidelity: outcome.fidelity, children: outcome.children.map(value => value.id) };
}
export async function suspendMachine(provider: MachinesProvider, id: MachineId, key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "suspended" }>> {
  const outcome = await provider.suspend(id, key);
  if (outcome.kind !== "suspended") throw new MachineOutcomeError("suspended", outcome);
  return outcome;
}
export async function wakeMachine(provider: MachinesProvider, id: MachineId, key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "woken" }>> {
  const outcome = await provider.wake(id, key);
  if (outcome.kind !== "woken") throw new MachineOutcomeError("woken", outcome);
  return outcome;
}
export async function setSuspensionPolicy(provider: MachinesProvider, id: MachineId, policy: SuspensionPolicy, key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "suspension-policy-set" }>> {
  const outcome = await provider.setSuspensionPolicy(id, policy, key);
  if (outcome.kind !== "suspension-policy-set") throw new MachineOutcomeError("suspension-policy-set", outcome);
  return outcome;
}
export async function destroyMachine(provider: MachinesProvider, id: MachineId, key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "machine-destroyed" }>> {
  const outcome = await provider.destroyMachine(id, key);
  if (outcome.kind !== "machine-destroyed") throw new MachineOutcomeError("machine-destroyed", outcome);
  return outcome;
}
export async function forkCheckpoint(provider: MachinesProvider, id: CheckpointId, count: number, key: IdempotencyKey): Promise<readonly MachineId[]> {
  const outcome = await provider.fork(id, count, key);
  if (outcome.kind !== "forked") throw new MachineOutcomeError("forked", outcome);
  return outcome.machines.map(value => value.id);
}
export async function destroyCheckpoint(provider: MachinesProvider, id: CheckpointId, key: IdempotencyKey): Promise<Extract<MutationOutcome, { kind: "checkpoint-destroyed" }>> {
  const outcome = await provider.destroyCheckpoint(id, key);
  if (outcome.kind !== "checkpoint-destroyed") throw new MachineOutcomeError("checkpoint-destroyed", outcome);
  return outcome;
}

export function machineEvents(provider: MachinesProvider, id: MachineId, afterSequence: number | null = null, limit = MANAGED_OCI_CONTRACT.maxEventPageSize): Promise<MachineEventPage> {
  return provider.events(id, afterSequence, limit);
}

export async function* listMachines(provider: MachinesProvider, options: MachineListOptions = {}): AsyncIterable<MachineId> {
  const pageSize = options.pageSize ?? MANAGED_OCI_CONTRACT.maxPageSize;
  const maximum = options.maximum ?? 1024;
  try { validatePageSize(pageSize); }
  catch (error) { throw new RangeError(error instanceof Error ? error.message : String(error)); }
  if (!Number.isSafeInteger(maximum) || maximum < 0) throw new RangeError("maximum must be a nonnegative safe integer");
  let cursor = options.after ?? null;
  let yielded = 0;
  while (yielded < maximum) {
    const page = await provider.listMachines(cursor, Math.min(pageSize, maximum - yielded));
    for (const observation of page.machines) {
      yield observation.id;
      yielded += 1;
      if (yielded === maximum) return;
    }
    if (page.next === null) return;
    if (page.next === cursor || page.machines.length === 0) throw new Error("machine listing did not advance its cursor");
    cursor = page.next;
  }
}

export class MachineOutcomeError extends Error { constructor(readonly expected: MutationOutcome["kind"], readonly outcome: MutationOutcome) { super(`expected ${expected} outcome, received ${outcome.kind}`); } }
