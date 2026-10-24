import { ActorNamespaceBinding, validateDeploymentBinding, type ActorsClient, type semantic } from "@acyclic-labs/actors";
import { WorkerServiceBinding } from "./service.js";

export type DeploymentBinding = ActorNamespaceBinding | WorkerServiceBinding;
export interface BindingAuthority {
  /** The host admits the declaration and returns the holder's scoped transport. */
  namespace(binding: semantic.Binding): Pick<ActorsClient, "invokeActor">;
  service(binding: semantic.Binding): Pick<ActorsClient, "invokeWorker">;
}

/** Materialize selected, host-admitted deployment declarations as typed env objects. */
export async function materializeBindings(declarations: readonly semantic.Binding[], authority: BindingAuthority): Promise<Readonly<Record<string, DeploymentBinding>>> {
  declarations = declarations.map(binding => {
    if (!(binding.targetAccount instanceof Uint8Array) && !Array.isArray(binding.targetAccount)) throw new TypeError("binding account must contain bytes");
    return Object.freeze({ ...binding, targetAccount: Uint8Array.from(binding.targetAccount) });
  });
  const names = new Set<string>();
  for (const binding of declarations) {
    if ((binding.actorNamespace === undefined) === (binding.workerService === undefined)) throw new TypeError("binding must select exactly one namespace or Worker service");
    if (names.has(binding.name)) throw new TypeError("duplicate binding name");
    names.add(binding.name);
    await validateDeploymentBinding(binding);
  }
  const env: Record<string, DeploymentBinding> = Object.create(null) as Record<string, DeploymentBinding>;
  for (const binding of declarations) {
    if (!binding.name || Object.hasOwn(env, binding.name)) throw new TypeError("duplicate or empty binding name");
    if ((binding.actorNamespace === undefined) === (binding.workerService === undefined)) {
      throw new TypeError("binding must select exactly one namespace or Worker service");
    }
    const account = Uint8Array.from(binding.targetAccount);
    env[binding.name] = binding.actorNamespace !== undefined
      ? new ActorNamespaceBinding(account, binding.actorNamespace, authority.namespace(binding), binding.name)
      : new WorkerServiceBinding(account, binding.workerService!, authority.service(binding), binding.name);
  }
  return Object.freeze(env);
}
