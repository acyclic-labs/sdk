/** One namespace guest over the shared Actors Handler ABI. */
import type { GuestContext } from "@acyclic-labs/actors";
import { dependencyBinding, requireBinding } from "@acyclic-labs/actors/guest";
import type { StreamLedger, WasmObjectsPort } from "@acyclic-labs/harness/hosting";
import type { AgentGuest, BrowserAgentOptions, BrowserMachinesWorkspaceAdmission,
  WasmAuthorityCapability } from "@acyclic-labs/harness/browser";
import type { CompactionPolicy, ModelCredentialResolver, ModelRegistry, ProviderConfig } from "@acyclic-labs/harness";
import { guestManifest, openGuest } from "./open.js";
import { HARNESS_BINDINGS, MODEL_FETCH, HarnessActorError } from "./bindings.js";

/** Owner-selected configuration, sealed into the artifact. */
export interface HarnessActorConfig {
  readonly options: Omit<BrowserAgentOptions, "children"> & { readonly children?: boolean };
  readonly model: {
    readonly provider: string;
    readonly spec: ProviderConfig;
    readonly selection: string;
    readonly limits: ConstructorParameters<typeof ModelRegistry>[1];
    readonly compaction: CompactionPolicy;
  };
}
interface Values {
  objects: WasmObjectsPort;
  /** Namespace storage; the host core owns placement and wake publication. */
  streams: Pick<StreamLedger, "dispatch" | "open">;
  authority: WasmAuthorityCapability;
  /** Admitted by request id inside onRecord, never while the heap opens. */
  workspace: BrowserMachinesWorkspaceAdmission;
}
export type Bound = Values & { model?: ModelCredentialResolver };
export type HarnessBindings = { readonly [K in keyof Values as (typeof HARNESS_BINDINGS)[K]]: Values[K] }
  & { readonly [MODEL_FETCH]?: ModelCredentialResolver };
const required = Object.entries(HARNESS_BINDINGS) as [keyof Values, string][];
/** Every selected binding is required. The Machines workspace is only ever
 * called at delivery admission, so any failure of it is that dependency's. */
function bindings(env: Partial<HarnessBindings>): Bound {
  const values: Partial<Record<keyof Bound, unknown>> = {};
  for (const [key, name] of required) values[key] = requireBinding<object>(env, name);
  const bound = values as Bound;
  return { ...bound, workspace: dependencyBinding(HARNESS_BINDINGS.workspace, bound.workspace) };
}
declare const HARNESS_ACTOR: { readonly config: HarnessActorConfig };
let heap: Promise<AgentGuest> | undefined;

/** Bind the admitted environment once at readiness; all actors share this
 * guest, while each onRecord admits its own Machine by delivery request id. */
export async function manifest(context?: Pick<GuestContext<HarnessBindings>, "env">): Promise<Uint8Array> {
  if (heap === undefined) {
    if (context === undefined) throw new HarnessActorError("failed_precondition", "environment");
    // A refused binding is retained as the heap's outcome, so every delivery
    // reports the same typed dependency instead of an unopened environment.
    const env = context.env;
    heap = Promise.resolve().then(() => {
      const bound = bindings(env);
      const { config } = HARNESS_ACTOR;
      if (config.model.spec.adapter !== "scripted") {
        bound.model = requireBinding<ModelCredentialResolver>(env, MODEL_FETCH);
      }
      return openGuest(bound, config);
    });
  }
  return guestManifest(await heap);
}

/** Canonical delivery in, canonical transition out; publication belongs to the host. */
export async function onRecord(bytes: Uint8Array, signal?: AbortSignal, interrupt?: AbortSignal): Promise<Uint8Array> {
  signal?.throwIfAborted();
  if (heap === undefined) throw new HarnessActorError("failed_precondition", "environment");
  const guest = await heap;
  signal?.throwIfAborted();
  return guest.onRecord(bytes, signal, interrupt);
}

/** Whether a control that arrived during a delivery interrupts it, by the
 * harness's own predicate; a host that cancels without asking cancels work
 * this control was never for. */
export async function acceptsInterrupt(query: Uint8Array, signal: AbortSignal): Promise<boolean> {
  signal.throwIfAborted();
  if (heap === undefined) throw new HarnessActorError("failed_precondition", "environment");
  const guest = await heap;
  signal.throwIfAborted();
  return guest.acceptsInterrupt(query, signal);
}
