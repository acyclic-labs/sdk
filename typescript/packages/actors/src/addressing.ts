/** Global views of existing account-scoped IDs, backed by the one Rust codec. */
export { ActorReference, ResourceIdentity, canonicalResourceHost } from "@acyclic-labs/auth/addressing";
export type { ActorReferenceFields, ResourceIdentityFields, IdentityError, DnsError } from "@acyclic-labs/auth/addressing";

export { WorkerRoutePattern, selectWorkerRoute, type WorkerRoute } from "@acyclic-labs/auth/addressing";
