/** The shared harness as one Actor application: the same event host and
 * session engine as every other deployment, behind the public guest ABI. */
import { defineActor, GUEST_ABI_VERSION, type CheckpointCodec, type GuestState } from "@acyclic-labs/actors";
import { ActorStateStorage, EventAgentHost, EventStreamLedger, initializeHosting, serveActorFetch,
  type ActorState, type EventHostAccess, type ImmutableObjects } from "@acyclic-labs/harness/hosting";
import { BrowserAgentAuthority, BrowserAgentRegistry, type BrowserAgentOptions,
  type BrowserMachinesWorkspace, type WasmAuthorityCapability } from "@acyclic-labs/harness/browser";
import { ModelRegistry, type CompactionPolicy, type ModelCredentialResolver, type ProviderSpec } from "@acyclic-labs/harness";
import { HARNESS_BINDINGS, MODEL_FETCH } from "./bindings.js";

/** The artifact owner's configuration. Credentials appear only as protected
 * `secrets:/` references inside `model.spec.authentication`. */
export interface HarnessActorConfig {
  readonly options: BrowserAgentOptions;
  /** Capacities of the one journal domain shared by every agent of this Actor. */
  readonly ledger_limits: ConstructorParameters<typeof EventStreamLedger>[4];
  readonly model: {
    readonly provider: string;
    readonly spec: ProviderSpec;
    readonly selection: string;
    readonly limits: ConstructorParameters<typeof ModelRegistry>[1];
    readonly compaction: CompactionPolicy;
  };
}

interface Values {
  /** Both names select the executing actor's one Binding.stateTransaction. */
  storage: ActorState;
  objects: ImmutableObjects;
  journal: ActorState;
  access: EventHostAccess;
  authority: WasmAuthorityCapability;
  workspace: BrowserMachinesWorkspace;
}
type Bound = Values & { model: ModelCredentialResolver };

/** Environment selected by the manifest entry, keyed by the descriptor names. */
export type HarnessBindings = { readonly [K in keyof Values as (typeof HARNESS_BINDINGS)[K]]: Values[K] }
  & { readonly [MODEL_FETCH]: ModelCredentialResolver };

/** Typed refusal: a required platform binding or primitive is not available. */
export class HarnessActorError extends Error {
  constructor(readonly code: "failed_precondition", readonly subject: string) {
    super(`STEP harness-actor failed: ${subject} is not available; recovery: deploy the generated manifest, which selects every harness binding`);
  }
}

const required: readonly (readonly [keyof Bound, string])[] = [
  ...(Object.entries(HARNESS_BINDINGS) as [keyof Values, string][]), ["model", MODEL_FETCH]];

function bindings(env: Partial<HarnessBindings>): Bound {
  const values: Partial<Record<keyof Bound, unknown>> = {};
  for (const [key, name] of required) {
    const value: unknown = (env as Record<string, unknown>)[name];
    if (value === undefined || value === null) throw new HarnessActorError("failed_precondition", name);
    values[key] = value;
  }
  // One state owner: metadata, wake rows and journal frames commit together.
  if (values.journal !== values.storage) throw new HarnessActorError("failed_precondition", HARNESS_BINDINGS.journal);
  return values as Bound;
}

/** The owner's configuration, written by the build ahead of the bundle. Every
 * module the bundle loads is a declared manifest asset beside this entry. */
declare const HARNESS_ACTOR: { readonly config: HarnessActorConfig };

interface Opened { readonly host: EventAgentHost; readonly ledger: EventStreamLedger }
function opened(state: GuestState<Opened>): Opened {
  let value: Opened | undefined;
  state.with(current => { value = current; return undefined; });
  return value!;
}

const identity = { name: "acyclic.harness.actor", version: "1" };
const empty = new Uint8Array();
/** Durable host state lives in the actor state binding, flushed before effects. The Actor
 * checkpoint carries none of it, so restore always reopens from that storage. */
const codec: CheckpointCodec<Opened> = {
  identity, readable: [identity],
  encode: () => empty,
  decode: () => {
    throw new Error("STEP harness-actor-checkpoint failed: state is retained in the actor state binding; recovery: restore the actor");
  },
};

export const actor = defineActor<Opened, HarnessBindings>({
  abiVersion: GUEST_ABI_VERSION,
  codec,
  async restore(_checkpoint, context) {
    await initializeHosting();
    const env = bindings(context.env);
    const name = context.actor.id;
    if (name === null) {
      throw new Error("STEP harness-actor-restore failed: no actor identity; recovery: deploy as an Actor namespace entry");
    }
    const { options, ledger_limits, model } = HARNESS_ACTOR.config;
    const owner = new BrowserAgentAuthority(env.authority);
    const registry = new BrowserAgentRegistry();
    const models = new ModelRegistry({}, model.limits);
    try {
      models.bindCredentials(env.model);
      models.register(model.provider, model.spec);
      const provider = models.resolve(model.selection);
      try { provider.install(registry, model.compaction); } finally { provider.free(); }
    } finally { models.free(); }
    registry.bindMachinesWorkspace(env.workspace);
    const state = new ActorStateStorage(env.storage);
    let host: EventAgentHost | undefined;
    const ledger = new EventStreamLedger(state.domain("wakes"), env.objects, appended => state.journal(appended),
      async () => { await host?.wake(); }, ledger_limits);
    host = new EventAgentHost(state.domain("actor"), ledger, name, options, owner, registry, env.access);
    await host.create(name);
    return { host, ledger };
  },
  async event(state, event) {
    const { host, ledger } = opened(state);
    if (event.type === "stream" || event.type === "timer" || event.type === "alarm") {
      await ledger.alarm();
      await host.wake();
    }
    return null;
  },
  checkpoint: () => ({ codec: identity, bytes: empty }),
  // The invocation's own admission carries the caller's credential.
  fetch: (state, request, context) =>
    serveActorFetch(opened(state).host, request, bindings(context.env).access.credential),
});
