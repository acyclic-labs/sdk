// Generated from acyclic_manifest. Do not edit.
export type Binding = {
  name?: string;
  capability?: string;
  resource?: string;
};
export type ActorLimits = {
  handlerTimeoutMillis?: string | number;
  memoryBytes?: string | number;
  checkpointBytes?: string | number;
};
export type SubscriptionStart = {
  cursor?: string | number;
  currentHead?: boolean;
};
export type SubscriptionSpec = {
  subscriptionId?: string;
  streamPath?: string;
  start?: SubscriptionStart;
  placementAnchor?: boolean;
};
export type ResourcePattern = {
  name?: string;
  prefix?: boolean;
};
export type ResourceGrant = {
  resource?: ResourcePattern;
  actions?: (string)[];
};
export type Limits = {
  rps?: string | number;
  burst?: string | number;
  bytesPerS?: string | number;
  channels?: string | number;
  concurrency?: string | number;
  bytes?: string | number;
  spendMicros?: string | number;
  tokens?: string | number;
};
export type DeploymentManifest = {
/**
 * Schema discriminator, exactly one.
 */
schemaVersion: number,
/**
 * Independent DNS-label-named deployment environments.
 */
environments: Array<Environment>, };
export type Environment = {
/**
 * Declaration name; environment names are DNS labels, env properties are identifiers.
 */
name: string,
/**
 * Artifact entries available for deployment.
 */
entries: Array<Entry>,
/**
 * The Actors contract binding, with capability naming a typed grant below.
 */
bindings: Binding[],
/**
 * Named typed workload grants; account spend entitlements remain host-owned.
 */
grants: Array<BindingGrant>,
/**
 * Credential-store references available only through explicit selection.
 */
secrets: Array<SecretRef>,
/**
 * UTC schedules delivering idempotent records to subscribed inboxes.
 */
crons: Array<CronSpec>,
/**
 * Content-addressed asset bundle declarations.
 */
assets: Array<AssetSpec>,
/**
 * Customer routes; host/prefix pairs are unique across all environments.
 */
routes: Array<RouteSpec>, };
export type Entry = {
/**
 * Declaration name; environment names are DNS labels, env properties are identifiers.
 */
name: string,
/**
 * Nonzero lowercase SHA-256 of the immutable code artifact.
 */
artifactSha256: string,
/**
 * Logical artifact module identifier, never a filesystem path.
 */
module: string,
/**
 * Explicitly selected binding names.
 */
bindings: Array<string>,
/**
 * Credential-store references available only through explicit selection.
 */
secrets: Array<string>,
/**
 * Actor instance or stateless Worker configuration.
 */
runtime: EntryRuntime,
/**
 * Optional scoped HTTPS handle; absence denies outbound HTTPS.
 */
fetch: FetchBinding | null, };
export type EntryRuntime = { "kind": "actor",
/**
 * Explicit initial placement region.
 */
homeRegion: string,
/**
 * Product-specific execution or capability ceilings.
 */
limits: ActorLimits,
/**
 * Canonical Actors subscription specs, including starts and placement anchors.
 */
subscriptions: SubscriptionSpec[],
/**
 * Optional durable alarm destination; enabling timers requires its wake subscription.
 */
timerInbox: TimerInbox | null, } | { "kind": "worker",
/**
 * Product-specific execution or capability ceilings.
 */
limits: WorkerLimits, };
export type TimerInbox = {
/**
 * Selected environment binding name.
 */
binding: string,
/**
 * Exact canonical Streams resource for durable fires.
 */
stream: ResourcePattern, };
export type WorkerLimits = {
/**
 * Positive CPU milliseconds per request.
 */
cpuMillis: number,
/**
 * Positive memory ceiling in bytes.
 */
memoryBytes: number,
/**
 * Positive wall-clock milliseconds per request.
 */
timeoutMillis: number, };
export type FetchBinding = {
/**
 * Declaration name; environment names are DNS labels, env properties are identifiers.
 */
name: string,
/**
 * Complete allow-list; an empty list is rejected.
 */
egress: Array<EgressRule>, };
export type BindingGrant = {
/**
 * Declaration name; environment names are DNS labels, env properties are identifiers.
 */
name: string,
/**
 * Authority using the shared auth.v1 resource and action contract.
 */
grant: ResourceGrant,
/**
 * Product-specific execution or capability ceilings.
 */
limits: Limits | null, };
export type SecretRef = {
/**
 * Declaration name; environment names are DNS labels, env properties are identifiers.
 */
name: string,
/**
 * Only secrets:/ followed by nonempty ASCII identifier path segments.
 */
reference: SecretReference, };
export type SecretReference = string;
export type EgressRule = {
/**
 * Lowercase canonical ASCII DNS name, excluding numeric IP forms.
 */
host: string,
/**
 * The host always uses TLS with certificate validation, on this port.
 */
port: number, };
export type CronSpec = {
/**
 * Declaration name; environment names are DNS labels, env properties are identifiers.
 */
name: string,
/**
 * Five numeric UTC cron fields, with lists, ranges and positive steps.
 */
schedule: string,
/**
 * Target entry name in this environment.
 */
entry: string,
/**
 * Selected Streams binding authorizing append to this inbox.
 */
inboxBinding: string,
/**
 * Exact streams:/ resource; the target must subscribe to this stream.
 */
inbox: ResourcePattern, };
export type AssetSpec = {
/**
 * Target entry name in this environment.
 */
entry: string,
/**
 * Selected environment binding name.
 */
binding: string,
/**
 * Immutable Objects keys are assets/<bundleSha256>/<path>.
 */
bundleSha256: string,
/**
 * Serve the bundle index for missing asset paths when enabled.
 */
spaFallback: boolean,
/**
 * Explicit cache policy for bundle responses.
 */
cache: AssetCache, };
export type AssetCache = "immutable" | "revalidate";
export type RouteSpec = {
/**
 * Lowercase canonical ASCII DNS name, excluding numeric IP forms.
 */
host: string,
/**
 * Canonical absolute prefix; segment-boundary matching, no trailing slash except /.
 */
pathPrefix: string,
/**
 * Target entry name in this environment.
 */
entry: string, };
