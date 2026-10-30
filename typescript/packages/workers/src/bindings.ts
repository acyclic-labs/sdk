import { ActorNamespaceBinding, materializeActorState, validateDeploymentBinding, type ActorsClient, type ActorStateBinding, type ActorStateHost, type semantic } from "@acyclic-labs/actors";
import { materializeGuestAdmission, type GuestAdmissionHost } from "@acyclic-labs/auth";
import { materializeImmutableObjects, type ImmutableObjects } from "@acyclic-labs/objects";
import { WorkerServiceBinding } from "./service.js";

export type DeploymentBinding<A = never, H = never, M = never, S = never> = ActorNamespaceBinding | WorkerServiceBinding | ActorStateBinding | ImmutableObjects | A | H | M | S;
export interface BindingAuthority<A = never, H = never, M = never, S = never> {
  /** The host admits the declaration and returns the holder's scoped transport. */
  namespace(binding: semantic.Binding): Pick<ActorsClient, "invokeActor">;
  service(binding: semantic.Binding): Pick<ActorsClient, "invokeWorker">;
  /** All aliases share the original executing actor and admitted invocation. */
  stateTransaction?: ActorStateHost;
  guestAdmission?: GuestAdmissionHost<A, H>;
  immutableObjects?(binding: semantic.Binding): ImmutableObjects;
  /** The Machines owner supplies its concrete workspace type and producer. */
  machinesWorkspace?(binding: semantic.Binding): M;
  /** The Streams owner supplies the original hosted provider for the shared subtree. */
  streams?(binding: semantic.Binding): S;
}

function kind(binding: semantic.Binding) {
  if (binding.stateTransaction) return "stateTransaction";
  if (binding.guestAdmission === "platformAuthority" || binding.guestAdmission === "hostAccess") return binding.guestAdmission;
  if (binding.actorNamespace !== undefined) return "namespace";
  if (binding.workerService !== undefined) return "service";
  if (binding.resource.startsWith("objects:/")) return "immutableObjects";
  if (binding.resource.startsWith("machines:/")) return "machinesWorkspace";
  if (binding.resource.startsWith("streams:/")) return "streams";
  throw new TypeError("binding materialization: unsupported kind; recovery: select a supported deployment binding kind");
}

/** Materialize selected, host-admitted deployment declarations as typed env objects.
 * Complete canonical admission and producer availability checks before any host
 * factory runs. Resource producers keep the original grant and invocation. */
export async function materializeBindings<A = never, H = never, M = never, S = never>(declarations: readonly semantic.Binding[], authority: BindingAuthority<A, H, M, S>): Promise<Readonly<Record<string, DeploymentBinding<A, H, M, S>>>> {
  const selected = declarations.map(binding => {
    if (!(binding.targetAccount instanceof Uint8Array)) throw new TypeError("binding account must be Uint8Array bytes");
    return Object.freeze({ ...binding, targetAccount: new Uint8Array(binding.targetAccount) });
  });
  const names = new Set<string>();
  for (const binding of selected) {
    if (names.has(binding.name)) throw new TypeError("duplicate binding name");
    names.add(binding.name);
    await validateDeploymentBinding(binding);
    const selection = kind(binding);
    const producer = selection === "platformAuthority" || selection === "hostAccess" ? authority.guestAdmission : authority[selection];
    if (producer === undefined) throw new TypeError(`binding materialization: missing ${selection} producer; recovery: supply the original admitted host producer before guest entry`);
  }
  const env: Record<string, DeploymentBinding<A, H, M, S>> = Object.create(null) as Record<string, DeploymentBinding<A, H, M, S>>;
  let state: ActorStateBinding | undefined;
  for (const binding of selected) {
    switch (kind(binding)) {
      case "namespace": env[binding.name] = new ActorNamespaceBinding(binding.targetAccount, binding.actorNamespace!, authority.namespace(binding), binding.name); break;
      case "service": env[binding.name] = new WorkerServiceBinding(binding.targetAccount, binding.workerService!, authority.service(binding), binding.name); break;
      case "stateTransaction": env[binding.name] = state ??= materializeActorState(authority.stateTransaction!); break;
      case "platformAuthority": env[binding.name] = materializeGuestAdmission("platformAuthority", authority.guestAdmission); break;
      case "hostAccess": env[binding.name] = materializeGuestAdmission("hostAccess", authority.guestAdmission); break;
      case "immutableObjects": env[binding.name] = materializeImmutableObjects(authority.immutableObjects!(binding)); break;
      case "machinesWorkspace": env[binding.name] = authority.machinesWorkspace!(binding); break;
      case "streams": env[binding.name] = authority.streams!(binding); break;
    }
  }
  return Object.freeze(env);
}
