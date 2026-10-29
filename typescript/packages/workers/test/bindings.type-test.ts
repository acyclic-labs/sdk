import type { ActorNamespaceBinding, ActorStateBinding } from "@acyclic-labs/actors";
import type { ImmutableObjects } from "@acyclic-labs/objects";
import { materializeBindings, type BindingAuthority, type WorkerServiceBinding } from "@acyclic-labs/workers";

type Platform = { readonly platform: unique symbol };
type Access = { readonly access: unique symbol };
type Workspace = { readonly workspace: unique symbol };
type Streams = { readonly streams: unique symbol };

export async function materialize(authority: BindingAuthority<Platform, Access, Workspace, Streams>) {
  const env = await materializeBindings([], authority);
  const binding: ActorNamespaceBinding | WorkerServiceBinding | ActorStateBinding | ImmutableObjects | Platform | Access | Workspace | Streams | undefined = env.selected;
  return binding;
}

export async function requiresResourceOwners(authority: BindingAuthority<Platform, Access, Workspace, Streams>) {
  // @ts-expect-error the owner must return its workspace, not another resource
  authority.machinesWorkspace = () => ({} as Platform);
  // @ts-expect-error the owner must return its Streams provider
  authority.streams = () => ({} as Workspace);
}
