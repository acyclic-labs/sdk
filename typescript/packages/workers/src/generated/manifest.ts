// Generated from acyclic_workers::manifest. Do not edit.
export type DeploymentManifest = {
/**
 * Exactly 1 for this schema.
 */
schemaVersion: number,
/**
 * Independent named deployment configurations.
 */
environments: Array<Environment>, };
export type Environment = {
/**
 * Stable environment name.
 */
name: string,
/**
 * Artifact entry points.
 */
entries: Array<Entry>,
/**
 * Declared capabilities available for explicit entry selection.
 */
bindings: Array<BindingSpec>,
/**
 * Credential-store references available for explicit entry selection.
 */
secrets: Array<SecretRef>,
/**
 * Exact outbound HTTPS destinations; DNS/private-address enforcement is host-owned.
 */
egress: Array<EgressRule>,
/**
 * Schedules delivering to actor inbox streams.
 */
crons: Array<CronSpec>,
/**
 * Objects-backed static asset declarations.
 */
assets: Array<AssetSpec>,
/**
 * HTTP routing declarations.
 */
routes: Array<RouteSpec>, };
export type Entry = {
/**
 * Unique entry name within the environment.
 */
name: string,
/**
 * Actor or stateless worker.
 */
kind: EntryKind,
/**
 * Lowercase hexadecimal SHA-256 of the artifact.
 */
artifactSha256: string,
/**
 * Logical module name within the artifact; not a filesystem path.
 */
module: string,
/**
 * Names of explicitly granted bindings.
 */
bindings: Array<string>,
/**
 * Names of explicitly granted credential references.
 */
secrets: Array<string>, };
export type EntryKind = "actor" | "worker";
export type BindingSpec = {
/**
 * Guest env property name.
 */
name: string,
/**
 * Runtime handle family.
 */
kind: BindingKind,
/**
 * Opaque platform resource reference.
 */
resource: string,
/**
 * Opaque Trust capability/policy reference, not a bearer token.
 */
capability: string, };
export type BindingKind = "streams" | "volume" | "objects" | "compute" | "actors" | "fetch";
export type SecretRef = {
/**
 * Guest env property name.
 */
name: string,
/**
 * Opaque credential-store reference; never the secret value.
 */
reference: string, };
export type EgressRule = {
/**
 * Canonical lowercase ASCII DNS hostname.
 */
host: string,
/**
 * Explicit TCP port.
 */
port: number, };
export type CronSpec = {
/**
 * Unique schedule name.
 */
name: string,
/**
 * Minute/hour/day-of-month/month/day-of-week numeric cron expression.
 */
schedule: string,
/**
 * Target actor entry name.
 */
entry: string,
/**
 * Target inbox stream binding selected by that entry.
 */
inboxBinding: string,
/**
 * Canonical account-relative stream path resolved through that binding.
 */
inboxStream: string, };
export type AssetSpec = {
/**
 * Entry serving the assets.
 */
entry: string,
/**
 * Objects binding selected by the entry.
 */
binding: string,
/**
 * Opaque object-key prefix.
 */
prefix: string, };
export type RouteSpec = {
/**
 * Canonical DNS hostname.
 */
host: string,
/**
 * Absolute URL path prefix, not a filesystem path.
 */
pathPrefix: string,
/**
 * Target entry name.
 */
entry: string, };
