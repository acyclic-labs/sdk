import type { CreateMachine, MachineEventPage, MachineId, MachinesProvider } from "./index.js";
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
  if (outcome.kind !== "created") throw new TypeError(`expected created outcome, received ${outcome.kind}`);
  return outcome.machine.id;
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
