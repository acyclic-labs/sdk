/** Parent-controlled, provider-neutral fork values. Rust owns durable admission. */
import type { FileRef, ProviderRef, VolumeRef } from "./conversation.js";
import { NativeContracts } from "./native-contracts.js";
import type { AgentId, Authority, OperationId } from "./index.js";

/** Protocol ceiling for a child-owned inherited conversation prefix. */
export const MAX_FORK_INHERITED_MESSAGES = 16_384n;

export type ResourceKind = "workspace" | "generation" | "artifact" | "sandbox" | "checkpoint" | "stream" | "context" | "run";
type ResourceProvider<Kind extends ResourceKind> =
  Kind extends "stream" ? ProviderRef<"stream">
  : Kind extends "checkpoint" ? ProviderRef<"machines">
  : Kind extends "workspace" | "generation" ? ProviderRef<"filesystem">
  : ProviderRef;
export interface ResourceRef<Kind extends ResourceKind = ResourceKind> {
  readonly kind: Kind;
  readonly provider: ResourceProvider<Kind>;
  readonly key: readonly number[];
  readonly version: string | null;
}

/** Validates and detaches a provider-owned immutable resource address in Rust. */
export async function resourceRef<Kind extends ResourceKind>(value: ResourceRef<Kind>): Promise<ResourceRef<Kind>> {
  return (await NativeContracts.create()).validate("resource_ref", value);
}

export type ResourceRevision =
  | Readonly<{ kind: "history"; reference: ResourceRef<"stream"> }>
  | Readonly<{ kind: "project"; reference: Readonly<{ volume: VolumeRef<"project", "filesystem">; generation: ResourceRef<"generation"> }> }>
  | Readonly<{ kind: "context"; reference: ResourceRef<"context"> }>
  | Readonly<{ kind: "process"; reference: ResourceRef<"checkpoint"> }>
  | Readonly<{ kind: "artifact"; reference: ResourceRef<"artifact"> }>
  | Readonly<{ kind: "shared_volume"; reference: VolumeRef<"session_shared"> }>
  | Readonly<{ kind: "extension"; reference: Readonly<{ name: string; version: number; implementation_digest: readonly number[]; reference: ResourceRef }> }>;

export interface ForkSelection {
  readonly required: boolean;
  readonly revision: ResourceRevision;
}

export interface AttestedBoundary {
  readonly provider: ProviderRef;
  readonly evidence: readonly number[];
}

export interface SharedGrant {
  readonly volume: VolumeRef<"session_shared">;
  readonly child_agent: AgentId;
  readonly operations: readonly ["read" | "write", ...("read" | "write")[]];
}

export interface ReferenceGrant {
  readonly file: FileRef;
  readonly reader: AgentId;
  /** Published list proving a private attachment not directly carried by an event. */
  readonly attachment_manifest?: FileRef;
}

export interface ForkRequest {
  readonly operation_id: OperationId;
  readonly parent: Authority<"conversation">;
  readonly parent_revision: bigint;
  readonly child: Authority<"conversation">;
  readonly child_agent: AgentId;
  readonly attached_agents: readonly AgentId[];
  readonly selections: readonly ForkSelection[];
  readonly boundary: AttestedBoundary | null;
}

export interface CapturedResource {
  readonly source: ResourceRevision;
  readonly revision: ResourceRevision;
}

export type Capture =
  | Readonly<{ kind: "captured"; value: CapturedResource }>
  | Readonly<{ kind: "unsupported"; value: string }>
  | Readonly<{ kind: "in_flight" | "indeterminate"; value: OperationId }>;

export interface ForkOmission {
  readonly selection: ForkSelection;
  readonly outcome: Exclude<Capture, { kind: "captured" }>;
}

export interface ForkReport {
  readonly request: ForkRequest;
  readonly captures: readonly Capture[];
  readonly child_private_volume: VolumeRef<"agent_private", "filesystem">;
  readonly child_private_generation: ResourceRef<"generation">;
  readonly inherited_context: readonly FileRef<"agent_private", "filesystem">[];
  readonly inherited_through_sequence: bigint;
  readonly shared_grants: readonly SharedGrant[];
  readonly reference_grants: readonly ReferenceGrant[];
  readonly attachment_manifests: readonly FileRef[];
}

export interface ForkSeed extends Omit<ForkRequest, "selections"> {
  readonly resources: readonly CapturedResource[];
  readonly omissions: readonly ForkOmission[];
  readonly child_private_volume: VolumeRef<"agent_private", "filesystem">;
  readonly child_private_generation: ResourceRef<"generation">;
  readonly inherited_context: readonly FileRef<"agent_private", "filesystem">[];
  readonly inherited_through_sequence: bigint;
  readonly shared_grants: readonly SharedGrant[];
  readonly reference_grants: readonly ReferenceGrant[];
  readonly attachment_manifests: readonly FileRef[];
}

/** Rust alone converts the complete capture report to its child-visible seed. */
export async function forkSeed(report: ForkReport): Promise<ForkSeed> {
  return (await NativeContracts.create()).forkSeed(report);
}

export async function validateForkRequest(request: ForkRequest): Promise<void> {
  (await NativeContracts.create()).validate("fork_request", request);
}

export async function validateForkSeed(seed: ForkSeed): Promise<void> {
  (await NativeContracts.create()).validate("fork_seed", seed);
}
export async function validateForkReport(report: ForkReport): Promise<void> {
  (await NativeContracts.create()).validate("fork_report", report);
}
/** Pinned refs that the owning provider may authorize for an attached reader.
 * This is an access plan, not a bearer token or a byte-resolution grant. */
export async function forkReadableReferences(seed: ForkSeed, reader: AgentId): Promise<readonly FileRef[]> {
  const native = await NativeContracts.create();
  const admitted = native.validate("fork_seed", seed);
  const canonicalReader = native.validateIdentity("agent", reader);
  if (canonicalReader !== admitted.child_agent && !admitted.attached_agents.includes(canonicalReader)) {
    throw new TypeError("agent is not attached to fork");
  }
  const decoder = new TextDecoder();
  const identity = (file: FileRef): string => decoder.decode(native.encodeCanonicalJson(file));
  const refs = new Map<string, FileRef>();
  for (const file of admitted.inherited_context) refs.set(identity(file), file);
  for (const grant of admitted.reference_grants) {
    if (grant.reader === canonicalReader) refs.set(identity(grant.file), grant.file);
  }
  return Object.freeze([...refs.values()]);
}
