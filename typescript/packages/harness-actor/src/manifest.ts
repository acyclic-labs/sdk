/** Lower the owner's deployment choices to the public deployment manifest.
 * The Rust validator remains the sole admission rule set; this adds only the
 * harness entry's own selection of its descriptor bindings. */
import type { ActorLimits, Binding, BindingGrant, DeploymentManifest, EntryAsset, HostCredential,
  SubscriptionSpec, VolumeProvider } from "@acyclic-labs/workers/manifest";
import type { ProviderSpec } from "@acyclic-labs/harness";
import { ACCOUNT_LEAF, HARNESS_BINDINGS, HARNESS_MODULE, MACHINES_ENDPOINT, MODEL_FETCH, OBJECTS_ENDPOINT } from "./bindings.js";

/** Declared name of the model credential reference; never a credential value. */
export const MODEL_SECRET = "modelCredential";

/** The owner's Actor namespace and the resource grant behind each harness binding. */
export interface HarnessDeployment {
  readonly environment: string;
  readonly entry: string;
  readonly jurisdiction: string;
  readonly limits: ActorLimits;
  readonly subscriptions: SubscriptionSpec[];
  readonly bindings: Binding[];
  readonly grants: BindingGrant[];
  /** The platform's cell Edge origin. Its Machines endpoint creates each
   * actor's Machine, with a credential that is by default the platform's
   * account leaf; the `objects` binding's Objects v1 endpoint is the same
   * origin, always with the account leaf. Both are host-only: neither origin
   * nor credential enters the guest environment. */
  readonly machines: { readonly origin: string; readonly credential?: HostCredential };
}

/** Typed refusal of a deployment that omits a harness binding. */
export class HarnessManifestError extends Error {
  constructor(readonly path: string, readonly rule: string) {
    super(`manifest ${path}: ${rule}`);
  }
}

/** The credential-store reference named by the provider authentication. The Rust
 * manifest validator is the sole judge of whether it is a protected reference. */
function reference(authentication: ProviderSpec["authentication"]): string {
  const credential = typeof authentication === "object" && authentication !== null
    ? Object.values(authentication)[0] as { secret_reference?: string } | undefined : undefined;
  return credential?.secret_reference ?? "";
}

/** `volumeProvider` is the owner's `options.filesystem_provider`: the platform
 * selects each actor agent's private volume of that provider for its Machine. */
export function harnessManifest(deployment: HarnessDeployment, spec: ProviderSpec, volumeProvider: VolumeProvider,
  artifactSha256: string, assets: EntryAsset[]): DeploymentManifest {
  const declared = new Set(deployment.bindings.map(binding => binding.name));
  for (const name of Object.values(HARNESS_BINDINGS)) {
    if (!declared.has(name)) throw new HarnessManifestError(`$.environments[0].bindings.${name}`, "harness-binding");
  }
  const origin = new URL(spec.base_url);
  const machines = new URL(deployment.machines.origin);
  if (machines.protocol !== "https:" || machines.origin !== deployment.machines.origin) {
    throw new HarnessManifestError("$.deployment.machines.origin", "https-origin");
  }
  const cell = { host: machines.hostname, port: Number(machines.port || 443) };
  // An anonymous endpoint has no credential to protect; every other mode names one.
  const secrets = spec.authentication === "optional" ? [] : [MODEL_SECRET];
  return {
    schemaVersion: 1,
    environments: [{
      name: deployment.environment,
      entries: [{
        name: deployment.entry,
        artifactSha256,
        assets,
        module: HARNESS_MODULE.id,
        bindings: Object.values(HARNESS_BINDINGS),
        secrets,
        runtime: {
          kind: "actorNamespace",
          jurisdiction: deployment.jurisdiction,
          limits: deployment.limits,
          subscriptions: deployment.subscriptions,
          timerInbox: null,
          actorMachine: { binding: HARNESS_BINDINGS.workspace, endpoint: MACHINES_ENDPOINT, volumeProvider },
        },
        fetch: { name: MODEL_FETCH, egress: [{ host: origin.hostname, port: Number(origin.port || 443) }] },
        hostFetch: [
          { name: MACHINES_ENDPOINT, origin: cell, credential: deployment.machines.credential ?? ACCOUNT_LEAF },
          { name: OBJECTS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF },
        ],
      }],
      bindings: deployment.bindings,
      grants: deployment.grants,
      secrets: secrets.map(name => ({ name, reference: reference(spec.authentication) })),
      crons: [],
      assets: [],
      routes: [],
      workerRoutes: [],
    }],
  };
}
