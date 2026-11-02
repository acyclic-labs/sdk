import type { CreateMachine, MachineEventPage, MachineId, MachinesProvider } from "./index.js";
import { MANAGED_OCI_CONTRACT } from "./managed-oci-contract.js";
import { pageSize as normalizePageSize } from "./wasm-runtime.js";

export interface MachineListOptions {
  /** Exclusive cursor. */
  readonly after?: MachineId;
  /** Per-request page size. */
  readonly pageSize?: number;
  /** Optional caller-selected bound on yielded Machines; absent lists all pages. */
  readonly maximum?: number;
}

/** Creates a machine and checks the outcome kind. */
export async function createMachine(provider: MachinesProvider, request: CreateMachine): Promise<MachineId> {
  const outcome = await provider.create(request);
  if (outcome.kind !== "created") throw new TypeError(`expected created outcome, received ${outcome.kind}`);
  const [machine] = outcome.machines;
  if (!machine || outcome.machines.length !== 1 || outcome.provenance !== null) throw new TypeError("create admitted other than one machine");
  return machine.id;
}
export function machineEvents(provider: MachinesProvider, id: MachineId, afterSequence: number | null = null, limit = MANAGED_OCI_CONTRACT.maxEventPageSize): Promise<MachineEventPage> {
  return provider.events(id, afterSequence, normalizePageSize(limit, MANAGED_OCI_CONTRACT.maxEventPageSize));
}

export async function* listMachines(provider: MachinesProvider, options: MachineListOptions = {}): AsyncIterable<MachineId> {
  const pageSize = normalizePageSize(options.pageSize ?? MANAGED_OCI_CONTRACT.maxPageSize, MANAGED_OCI_CONTRACT.maxPageSize);
  const maximum = options.maximum;
  if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 0)) throw new RangeError("maximum must be a nonnegative safe integer");
  let cursor = options.after ?? null;
  let yielded = 0;
  while (maximum === undefined || yielded < maximum) {
    const page = await provider.listMachines(cursor, maximum === undefined ? pageSize : Math.min(pageSize, maximum - yielded));
    for (const observation of page.machines) {
      yield observation.id;
      if (maximum !== undefined && ++yielded === maximum) return;
    }
    if (page.next === null) return;
    if (page.next === cursor || page.machines.length === 0) throw new Error("machine listing did not advance its cursor");
    cursor = page.next;
  }
}
