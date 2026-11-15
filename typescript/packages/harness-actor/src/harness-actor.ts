/** One namespace guest over the shared Actors Handler ABI. */
import type { GuestContext } from "@acyclic-labs/actors";
import { dependencyBinding, requireBinding } from "@acyclic-labs/actors/guest";
import { initializeHosting, WasmObjects, WasmStream, streamLedgerPort,
  type StreamLedger, type WasmObjectsPort } from "@acyclic-labs/harness/hosting";
import { AgentGuest, BrowserAgentAuthority, BrowserAgentRegistry, BrowserAgentProviders, type BrowserAgentOptions,
  type BrowserMachinesWorkspaceAdmission, type WasmAuthorityCapability } from "@acyclic-labs/harness/browser";
import { ModelRegistry, type CompactionPolicy, type ModelCredentialResolver, type ProviderSpec } from "@acyclic-labs/harness";
import { HARNESS_BINDINGS, MODEL_FETCH, HarnessActorError } from "./bindings.js";

/** Owner-selected configuration, sealed into the artifact. */
export interface HarnessActorConfig {
  readonly options: Omit<BrowserAgentOptions, "children">;
  readonly model: {
    readonly provider: string;
    readonly spec: ProviderSpec;
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
type Bound = Values & { model: ModelCredentialResolver };
export type HarnessBindings = { readonly [K in keyof Values as (typeof HARNESS_BINDINGS)[K]]: Values[K] }
  & { readonly [MODEL_FETCH]: ModelCredentialResolver };
const required: readonly (readonly [keyof Bound, string])[] = [
  ...(Object.entries(HARNESS_BINDINGS) as [keyof Values, string][]), ["model", MODEL_FETCH]];
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

async function open(env: Bound): Promise<AgentGuest> {
  await initializeHosting();
  const { options, model } = HARNESS_ACTOR.config;
  const selected = { ...options, children: false };
  const owner = new BrowserAgentAuthority(env.authority);
  const registry = new BrowserAgentRegistry();
  try {
    const models = new ModelRegistry({}, model.limits);
    try {
      models.bindCredentials(env.model);
      models.register(model.provider, model.spec);
      const provider = models.resolve(model.selection);
      try { provider.install(registry, model.compaction); } finally { provider.free(); }
    } finally { models.free(); }
    registry.bindMachinesWorkspaceSource(env.workspace);
    // An empty origin conveys no actor authority. The host's original request
    // binding selects each actor; this guest never runs a placement scheduler.
    const stream = WasmStream.connectProvider(streamLedgerPort(env.streams, ""));
    try {
      const objects = new WasmObjects({ port: env.objects });
      try {
        const providers = BrowserAgentProviders.injected(stream, objects, true, selected);
        try { return AgentGuest.open(providers, selected, owner, registry); }
        finally { providers.free(); }
      } finally { objects.free(); }
    } finally { stream.free(); }
  } finally { registry.free(); owner.free(); }
}

/** Bind the admitted environment once at readiness; all actors share this
 * guest, while each onRecord admits its own Machine by delivery request id. */
export async function manifest(context?: Pick<GuestContext<HarnessBindings>, "env">): Promise<Uint8Array> {
  if (heap === undefined) {
    if (context === undefined) throw new HarnessActorError("failed_precondition", "environment");
    // A refused binding is retained as the heap's outcome, so every delivery
    // reports the same typed dependency instead of an unopened environment.
    const env = context.env;
    heap = Promise.resolve().then(() => open(bindings(env)));
  }
  return (await heap).manifest();
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
