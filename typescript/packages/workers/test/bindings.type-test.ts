import type { ActorNamespaceBinding, ActorStateBinding } from "@acyclic-labs/actors";
import type { ObjectsV1BinaryPort } from "@acyclic-labs/objects";
import type { SecretBinding } from "@acyclic-labs/auth";
import { materializeBindings, type BindingAuthority, type WorkerServiceBinding } from "@acyclic-labs/workers";

type Platform = { readonly platform: unique symbol };
type Access = { readonly access: unique symbol };
type Workspace = { readonly workspace: unique symbol };
type Streams = { readonly streams: unique symbol };
type Filesystem = { readonly filesystem: unique symbol };
type Inference = { readonly inference: unique symbol };

export async function materialize(authority: BindingAuthority<Platform, Access, Workspace, Streams>) {
  const env = await materializeBindings([], authority);
  const binding: ActorNamespaceBinding | WorkerServiceBinding | ActorStateBinding | ObjectsV1BinaryPort | SecretBinding | Platform | Access | Workspace | Streams | undefined = env.selected;
  return binding;
}

export async function requiresResourceOwners(authority: BindingAuthority<Platform, Access, Workspace, Streams>) {
  // @ts-expect-error the owner must return its workspace, not another resource
  authority.machinesWorkspace = () => ({} as Platform);
  // @ts-expect-error the owner must return its Streams provider
  authority.streams = () => ({} as Workspace);
}

export async function materializeResourceOwners(authority: BindingAuthority<Platform, Access, Workspace, Streams, Filesystem, Inference>) {
  const env = await materializeBindings([], authority);
  const binding: ActorNamespaceBinding | WorkerServiceBinding | ActorStateBinding | ObjectsV1BinaryPort | SecretBinding | Platform | Access | Workspace | Streams | Filesystem | Inference | undefined = env.selected;
  // @ts-expect-error fs must preserve its owner's type
  authority.fs = () => ({} as Inference);
  // @ts-expect-error infer must preserve its owner's type
  authority.infer = () => ({} as Filesystem);
  return binding;
}
