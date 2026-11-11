import { ActorNamespaceBinding, admitDeploymentBindings, materializeActorState, type ActorsClient, type ActorStateBinding, type ActorStateHost, type semantic } from "@acyclic-labs/actors";
import { materializeGuestAdmission, materializeSecretBinding, type GuestAdmissionHost, type SecretBinding, type SecretBindingHost } from "@acyclic-labs/auth";
import type { ObjectsV1BinaryPort } from "@acyclic-labs/objects";
import { WorkerServiceBinding } from "./service.js";

export type DeploymentBinding<A = never, H = never, M = never, S = never, F = never, N = never> = ActorNamespaceBinding | WorkerServiceBinding | ActorStateBinding | ObjectsV1BinaryPort | SecretBinding | A | H | M | S | F | N;
export interface BindingAuthority<A = never, H = never, M = never, S = never, F = never, N = never> {
  /** The host admits the declaration and returns the holder's scoped transport. */
  namespace(binding: semantic.Binding): Pick<ActorsClient, "invokeActor">;
  service(binding: semantic.Binding): Pick<ActorsClient, "invokeWorker">;
  /** All aliases share the original executing actor and admitted invocation. */
  stateTransaction?: ActorStateHost;
  guestAdmission?: GuestAdmissionHost<A, H>;
  /** Original host owns selected references and secret resolution, outside guest env. */
  secretBindings?: SecretBindingHost;
  /** The admitted account's original Objects v1 transport. */
  objects?(binding: semantic.Binding): ObjectsV1BinaryPort;
  /** The Machines owner supplies its concrete workspace type and producer. */
  machinesWorkspace?(binding: semantic.Binding): M;
  /** The Streams owner supplies the original hosted provider for the shared subtree. */
  streams?(binding: semantic.Binding): S;
  /** Original host-admitted resource owners; each retains the selected grant and invocation. */
  fs?(binding: semantic.Binding): F;
  infer?(binding: semantic.Binding): N;
}

/** Resource producers by the product of the canonical `product:/path` name. */
const RESOURCES = new Map<string, "objects" | "machinesWorkspace" | "streams" | "fs" | "infer">([
  ["objects", "objects"], ["machines", "machinesWorkspace"], ["streams", "streams"],
  ["fs", "fs"], ["infer", "infer"],
]);

function kind(binding: semantic.Binding) {
  if (binding.stateTransaction) return "stateTransaction";
  if (binding.guestAdmission === "platformAuthority" || binding.guestAdmission === "hostAccess") return binding.guestAdmission;
  if (binding.actorNamespace !== undefined) return "namespace";
  if (binding.workerService !== undefined) return "service";
  // The Rust validator already admitted the name; its product ends at the first ":/".
  const separator = binding.resource.indexOf(":/");
  const resource = separator > 0 ? RESOURCES.get(binding.resource.slice(0, separator)) : undefined;
  if (resource !== undefined) return resource;
  throw new TypeError("binding materialization: unsupported kind; recovery: select a supported deployment binding kind");
}

/** Materialize selected, host-admitted deployment declarations as typed env objects.
 * Complete canonical admission and require selected factory functions before any
 * host factory runs. Retain those exact producers and their original receiver for the
 * batch. Resource producers keep the original grant and invocation. */
export async function materializeBindings<A = never, H = never, M = never, S = never, F = never, N = never>(declarations: readonly semantic.Binding[], authority: BindingAuthority<A, H, M, S, F, N>, secrets: readonly semantic.SecretSelection[] = []): Promise<Readonly<Record<string, DeploymentBinding<A, H, M, S, F, N>>>> {
  const selected = await admitDeploymentBindings(declarations);
  const names = new Set<string>();
  const kinds: ReturnType<typeof kind>[] = [];
  const producers: Partial<BindingAuthority<A, H, M, S, F, N>> = Object.create(null);
  function retain<K extends keyof BindingAuthority<A, H, M, S, F, N>>(key: K): void {
    if (Object.hasOwn(producers, key)) return;
    const producer = authority[key];
    const absent = producer === undefined || producer === null;
    if (absent || (key !== "stateTransaction" && key !== "guestAdmission" && typeof producer !== "function"))
      throw new TypeError(`binding materialization: ${absent ? "missing" : "non-callable"} ${key} producer; recovery: supply the original admitted host producer before guest entry`);
    producers[key] = producer;
  }
  for (const binding of selected) {
    if (names.has(binding.name)) throw new TypeError("duplicate binding name");
    names.add(binding.name);
    const selection = kind(binding);
    retain(selection === "platformAuthority" || selection === "hostAccess" ? "guestAdmission" : selection);
    kinds.push(selection);
  }
  // These are the original host-admitted deployment selections. References
  // stay on the host: only names are needed to construct guest handles.
  for (const secret of secrets) {
    if (names.has(secret.name)) throw new TypeError("duplicate binding name");
    names.add(secret.name);
  }
  const secretHost = secrets.length ? authority.secretBindings : undefined;
  if (secrets.length && (!secretHost || typeof secretHost.verifyHmac !== "function"))
    throw new TypeError("binding materialization: missing protected secret host");
  const env: Record<string, DeploymentBinding<A, H, M, S, F, N>> = Object.create(null) as Record<string, DeploymentBinding<A, H, M, S, F, N>>;
  for (const secret of secrets) env[secret.name] = materializeSecretBinding(secret.name, secretHost!);
  let state: ActorStateBinding | undefined;
  for (let index = 0; index < selected.length; index++) {
    const binding = selected[index]!;
    const selection = kinds[index]!;
    switch (selection) {
      case "namespace": env[binding.name] = new ActorNamespaceBinding(binding.targetAccount, binding.actorNamespace!, producers.namespace!.call(authority, binding), binding.name); break;
      case "service": env[binding.name] = new WorkerServiceBinding(binding.targetAccount, binding.workerService!, producers.service!.call(authority, binding), binding.name); break;
      case "stateTransaction": env[binding.name] = state ??= materializeActorState(producers.stateTransaction!); break;
      case "platformAuthority": case "hostAccess": env[binding.name] = materializeGuestAdmission(selection, producers.guestAdmission); break;
      case "objects": {
        // Only the port crosses: its receiver, promises and errors stay the producer's.
        const port = producers.objects!.call(authority, binding);
        env[binding.name] = Object.freeze({ open: port.open.bind(port) });
        break;
      }
      case "machinesWorkspace": env[binding.name] = producers.machinesWorkspace!.call(authority, binding); break;
      case "streams": env[binding.name] = producers.streams!.call(authority, binding); break;
      case "fs": env[binding.name] = producers.fs!.call(authority, binding); break;
      case "infer": env[binding.name] = producers.infer!.call(authority, binding); break;
    }
  }
  return Object.freeze(env);
}
