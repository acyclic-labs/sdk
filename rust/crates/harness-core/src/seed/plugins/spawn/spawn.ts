import { atomically } from "acyclic";
import type { Agent, Change, Child, Copy } from "acyclic";

// Creation, the brief and home files share the caller's one commit. The
// ordinary change lowering owns grants, escrow, hooks and name validation.
async function create(name: string, change: Change): Promise<Agent> {
  await atomically([change]);
  return { name, live: `live/agents/${name}/` };
}

export function spawn(name: string, child: Child): Promise<Agent> {
  return create(name, { ...child, spawn: name });
}

export function fork(name: string, copy: Copy): Promise<Agent> {
  return create(name, { ...copy, fork: name });
}
