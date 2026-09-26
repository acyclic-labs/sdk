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
  : Kind extends "workspace" ? ProviderRef<"filesystem">
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
  readonly preparation: ForkPreparation;
  readonly selections: readonly ForkSelection[];
  readonly boundary: AttestedBoundary | null;
}

export interface ForkPreparation {
  readonly child_project_volume: VolumeRef<"project", "filesystem">;
  readonly child_private_volume: VolumeRef<"agent_private", "filesystem">;
  readonly inherited_through_sequence: bigint;
  readonly maximum_inherited_messages: bigint;
  readonly maximum_inherited_bytes: bigint;
  readonly maximum_inherited_references: number;
}

export interface CapturedResource {
  readonly source: ResourceRevision;
  readonly revision: ResourceRevision;
}

export type Capture =
  | Readonly<{ kind: "captured"; value: CapturedResource }>
  | Readonly<{ kind: "unsupported"; value: string }>
  | Readonly<{ kind: "in_flight" | "indeterminate"; value: OperationId }>;

/** Replaceable owner of one exact provider's selected fork revision. An
 * uncertain effect rejects so preparation can reconcile the same operation. */
export interface ForkCaptureProvider {
  readonly provider: ProviderRef;
  capture(request: ForkRequest, selection: ForkSelection): Promise<Capture>;
  /** A missing observation is unresolved, never permission to repeat a side effect. */
  reconcile(request: ForkRequest, selection: ForkSelection): Promise<Capture | null>;
}

/** Parent-bound owner of an idempotent multi-resource preparation. */
export interface ForkPreparer {
  /** Exact parent projection captured at binding; later revisions need a new binding. */
  parentSnapshot(): Readonly<{ parent: Authority<"conversation">; revision: bigint }>;
  prepare(request: ForkRequest): Promise<ForkReport>;
  reconcile(request: ForkRequest): Promise<ForkReport | null>;
}

/** Trusted parent-owned publication boundary. The provider must authenticate
 * both signed scopes, append the exact seed at the parent revision, and durably
 * bind or replay the fresh child with a causal link to that append before it
 * returns success. An uncertain reply is observed by operation ID, never
 * recaptured. The facade validates identity, not the provider's internal state. */
export interface ForkPublisher {
  parent(): Authority<"conversation">;
  spawnFromReport(report: ForkReport): Promise<ForkSeed>;
  reconcileSpawn(operationId: OperationId): Promise<ForkSeed | null>;
}

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
  /** Manifest reads need owner, selected-volume, or direct exact-ref authority. */
  readonly attachment_manifests: readonly FileRef[];
}

export interface ForkSeed extends Omit<ForkRequest, "selections" | "preparation"> {
  readonly resources: readonly CapturedResource[];
  readonly omissions: readonly ForkOmission[];
  readonly child_private_volume: VolumeRef<"agent_private", "filesystem">;
  readonly child_private_generation: ResourceRef<"generation">;
  readonly inherited_context: readonly FileRef<"agent_private", "filesystem">[];
  readonly inherited_through_sequence: bigint;
  readonly shared_grants: readonly SharedGrant[];
  readonly reference_grants: readonly ReferenceGrant[];
  /** Manifest bytes and listed members are independently read-authorized. */
  readonly attachment_manifests: readonly FileRef[];
}

/** Rust alone converts the complete capture report to its child-visible seed. */
export async function forkSeed(report: ForkReport): Promise<ForkSeed> {
  checkInheritedMessageLimit(report.inherited_through_sequence);
  return (await NativeContracts.create()).forkSeed(report);
}

export async function validateForkRequest(request: ForkRequest): Promise<void> {
  (await NativeContracts.create()).validate("fork_request", request);
}

export async function validateForkSeed(seed: ForkSeed): Promise<void> {
  (await NativeContracts.create()).validate("fork_seed", seed);
}
export async function validateForkReport(report: ForkReport): Promise<void> {
  checkInheritedMessageLimit(report.inherited_through_sequence);
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
  for (const manifest of admitted.attachment_manifests) {
    const volume = manifest.volume;
    const ownerRead = volume.class === "agent_private" && volume.owner.kind === "agent"
      && volume.owner.id === canonicalReader;
    const sharedRead = volume.class === "session_shared" && admitted.shared_grants.some(grant =>
      grant.child_agent === canonicalReader && grant.operations.includes("read")
        && native.canonicalEqual(grant.volume, volume));
    const projectRead = volume.class === "project" && admitted.resources.some(resource =>
      resource.revision.kind === "project"
        && native.canonicalEqual(resource.revision.reference.volume, volume));
    if (ownerRead || sharedRead || projectRead) refs.set(identity(manifest), manifest);
  }
  return Object.freeze([...refs.values()]);
}

function checkInheritedMessageLimit(sequence: bigint): void {
  if (sequence > MAX_FORK_INHERITED_MESSAGES) {
    throw new TypeError("inherited message limit exceeds protocol cap");
  }
}
