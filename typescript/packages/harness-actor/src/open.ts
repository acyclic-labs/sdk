/** The harness guest's readiness: each step that consumes a binding fails as
 * that binding's typed dependency, every other step as its declared guest
 * step, so the host names the exact step without reading guest text. */
import { DependencyUnavailableError, GuestStepError, readinessStep, type GuestStep } from "@acyclic-labs/actors/guest";
import { initializeHosting, WasmObjects, WasmStream, streamLedgerPort } from "@acyclic-labs/harness/hosting";
import { AgentGuest, BrowserAgentAuthority, BrowserAgentRegistry, BrowserAgentProviders } from "@acyclic-labs/harness/browser";
import { ModelRegistry } from "@acyclic-labs/harness";
import { HARNESS_BINDINGS, MODEL_FETCH } from "./bindings.js";
import type { Bound, HarnessActorConfig } from "./harness-actor.js";

const step = <T>(name: GuestStep, run: () => T): T => readinessStep(cause => new GuestStepError(name, { cause }), run);
const binding = <T>(name: string, run: () => T): T => readinessStep(cause => new DependencyUnavailableError(name, { cause }), run);

export async function openGuest(env: Bound, config: HarnessActorConfig): Promise<AgentGuest> {
  await step("harness.hosting", () => initializeHosting());
  const { options, model } = config;
  const selected = { ...options, children: options.children ?? false };
  const owner = binding(HARNESS_BINDINGS.authority, () => new BrowserAgentAuthority(env.authority));
  const registry = new BrowserAgentRegistry();
  try {
    const models = step("harness.model", () => new ModelRegistry({}, model.limits));
    try {
      const credentials = env.model;
      if (credentials !== undefined) binding(MODEL_FETCH, () => models.bindCredentials(credentials));
      step("harness.model", () => {
        models.register(model.provider, model.spec);
        const provider = models.resolve(model.selection);
        try { provider.install(registry, model.compaction); } finally { provider.free(); }
      });
    } finally { models.free(); }
    binding(HARNESS_BINDINGS.workspace, () => registry.bindMachinesWorkspaceSource(env.workspace));
    // An empty origin conveys no actor authority. The host's original request
    // binding selects each actor; this guest never runs a placement scheduler.
    const stream = binding(HARNESS_BINDINGS.streams, () => WasmStream.connectProvider(streamLedgerPort(env.streams, "")));
    try {
      const objects = binding(HARNESS_BINDINGS.objects, () => new WasmObjects({ port: env.objects }));
      try {
        const providers = step("harness.providers", () => BrowserAgentProviders.injected(stream, objects, true, selected));
        try { return step("harness.open", () => AgentGuest.open(providers, selected, owner, registry)); }
        finally { providers.free(); }
      } finally { objects.free(); }
    } finally { stream.free(); }
  } finally { registry.free(); owner.free(); }
}

/** The opened guest's strict-wire manifest, or its declared step's failure. */
export const guestManifest = (guest: AgentGuest): Promise<Uint8Array> | Uint8Array =>
  step("harness.manifest", () => guest.manifest());
