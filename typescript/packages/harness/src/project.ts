/** Parent-bound project fork and merge facade over a provider-owned binding. */
import type { ConversationMessage, ProviderRef, VolumeRef } from "./conversation.js";
import { resourceRef, type ResourceRef } from "./fork.js";
import { NativeContracts } from "./native-contracts.js";
import type { Authority, OperationId } from "./index.js";
import type { ToolJsonValue } from "./model.js";

export type ProjectMergeNotice = ConversationMessage<"merge">;
const filesystemOperationBrand: unique symbol = Symbol("harness.filesystem-operation-id");
export type FilesystemOperationId = readonly number[] & Readonly<{ readonly [filesystemOperationBrand]: true }>;

export function filesystemOperationId(value: Uint8Array | readonly number[]): FilesystemOperationId {
  if (value.length !== 16 || value.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
    throw new TypeError("Filesystem operation identity must contain exactly 16 bytes");
  }
  return Object.freeze([...value]) as FilesystemOperationId;
}

/** Provider-owned proof of one exact immutable Filesystem publication. */
export interface ProviderJoinProof {
  readonly provider: ProviderRef<"filesystem">;
  readonly format: string;
  readonly statement: Exclude<ToolJsonValue, null>;
}

/** Parent-owned result of one verified Filesystem join and its atomic conversation notice. */
export interface ProjectMergeReceipt {
  readonly operation_id: OperationId;
  readonly child: Authority<"conversation">;
  readonly source_project: VolumeRef<"project", "filesystem">;
  readonly source_generation: ResourceRef<"generation">;
  readonly target_project: VolumeRef<"project", "filesystem">;
  readonly expected_target_generation: ResourceRef<"generation">;
  readonly result_generation: ResourceRef<"generation">;
  readonly filesystem_operation_id: FilesystemOperationId;
  readonly provider_proof: ProviderJoinProof;
  readonly notice: ProjectMergeNotice;
}

export type PublishProjectMergeAction = Readonly<{ kind: "publish_project_merge"; receipt: ProjectMergeReceipt }>;

export interface ProjectJoinPreparation<RawPlan> {
  readonly plan: RawPlan;
  readonly source: ResourceRef<"generation">;
  readonly target: ResourceRef<"generation">;
  readonly commonAncestor: ResourceRef<"generation">;
}

/** Providers authenticate the bound parent and verify the grant on every operation. */
export interface ParentProjectBinding<RawPlan, Conflict, Resolution, Outcome, Description = unknown> {
  readonly parent: Authority<"conversation">;
  readonly project: VolumeRef<"project", "filesystem">;
  authorize(capability: "fork:publish" | "project:merge", operation: "read" | "write"): Promise<void>;
  forkProject(sourceGeneration: ResourceRef<"generation">, child: VolumeRef<"project", "filesystem">, idempotencyKey: string): Promise<ResourceRef<"generation">>;
  prepareProjectMerge(child: VolumeRef<"project", "filesystem">): Promise<ProjectJoinPreparation<RawPlan>>;
  describeProjectMergeConflicts(plan: RawPlan, conflicts: readonly Conflict[], truncated: boolean): Promise<Description>;
  applyProjectMerge(plan: RawPlan, expectedTarget: ResourceRef<"generation">, idempotencyKey: string, resolution: Resolution | null): Promise<Outcome>;
  /** Verifies the Filesystem result, then appends receipt and notice as one parent Stream event. */
  publishProjectMergeReceipt(receipt: ProjectMergeReceipt): Promise<void>;
}

/** Opaque controller-owned handle; its raw provider plan is never exposed. */
export interface ParentMergePlan {
  readonly child: VolumeRef<"project", "filesystem">;
  readonly source: ResourceRef<"generation">;
  readonly target: ResourceRef<"generation">;
  readonly commonAncestor: ResourceRef<"generation">;
}

export class ParentProjectController<RawPlan, Conflict, Resolution, Outcome, Description = unknown> {
  readonly #binding: ParentProjectBinding<RawPlan, Conflict, Resolution, Outcome, Description>;
  readonly #contracts: NativeContracts;
  readonly #project: VolumeRef<"project", "filesystem">;
  readonly #plans = new WeakMap<ParentMergePlan, Readonly<{ raw: RawPlan }>>();

  constructor(contracts: NativeContracts, binding: ParentProjectBinding<RawPlan, Conflict, Resolution, Outcome, Description>) {
    if (binding.parent.kind !== "conversation" || !binding.parent.id) {
      throw new TypeError("project controller requires a parent conversation");
    }
    const project = contracts.validate("volume_ref", binding.project);
    this.#contracts = contracts;
    if (project.class !== "project") throw new TypeError("parent project must be a project volume");
    this.#binding = binding;
    this.#project = project;
  }

  async forkProject(sourceGeneration: ResourceRef<"generation">, child: VolumeRef<"project", "filesystem">,
    idempotencyKey: string): Promise<ResourceRef<"generation">> {
    const selected = this.#child(child);
    const generation = await this.#generation(sourceGeneration);
    this.#key(idempotencyKey);
    await this.#binding.authorize("fork:publish", "read");
    return this.#generation(await this.#binding.forkProject(generation, selected, idempotencyKey));
  }

  async prepareProjectMerge(child: VolumeRef<"project", "filesystem">): Promise<ParentMergePlan> {
    const selected = this.#child(child);
    await this.#binding.authorize("project:merge", "write");
    const prepared = await this.#binding.prepareProjectMerge(selected);
    const handle: ParentMergePlan = Object.freeze({
      child: selected,
      source: await this.#generation(prepared.source),
      target: await this.#generation(prepared.target),
      commonAncestor: await this.#generation(prepared.commonAncestor),
    });
    this.#plans.set(handle, { raw: prepared.plan });
    return handle;
  }

  async describeProjectMergeConflicts(plan: ParentMergePlan, conflicts: readonly Conflict[], truncated: boolean): Promise<Description> {
    const raw = this.#ownedPlan(plan);
    await this.#binding.authorize("project:merge", "write");
    return this.#binding.describeProjectMergeConflicts(raw, conflicts, truncated);
  }

  async applyProjectMerge(plan: ParentMergePlan, idempotencyKey: string, resolution: Resolution | null = null): Promise<Outcome> {
    const raw = this.#ownedPlan(plan);
    this.#key(idempotencyKey);
    await this.#binding.authorize("project:merge", "write");
    return this.#binding.applyProjectMerge(raw, plan.target, idempotencyKey, resolution);
  }

  async publishProjectMergeReceipt(plan: ParentMergePlan, receipt: ProjectMergeReceipt): Promise<void> {
    this.#ownedPlan(plan);
    const admitted = this.#contracts.validate("project_merge_receipt", receipt);
    if (admitted.child.kind !== "conversation" || !admitted.child.id
      || !this.#contracts.canonicalEqual(this.#child(admitted.source_project), plan.child)
      || !this.#contracts.canonicalEqual(admitted.target_project, this.#project)
      || !this.#contracts.canonicalEqual(admitted.source_generation, plan.source)
      || !this.#contracts.canonicalEqual(admitted.expected_target_generation, plan.target)) {
      throw new TypeError("project merge receipt is not bound to the inspected parent plan");
    }
    await this.#binding.authorize("project:merge", "write");
    await this.#binding.publishProjectMergeReceipt(admitted);
  }

  #ownedPlan(plan: ParentMergePlan): RawPlan {
    const entry = this.#plans.get(plan);
    if (entry === undefined) throw new TypeError("merge plan belongs to another parent controller");
    return entry.raw;
  }

  #child(value: VolumeRef<"project", "filesystem">): VolumeRef<"project", "filesystem"> {
    const child = this.#contracts.validate("volume_ref", value);
    if (child.class !== "project" || child.id === this.#project.id
      || !this.#contracts.canonicalEqual(child.provider, this.#project.provider)
      || !this.#contracts.canonicalEqual(child.owner, this.#project.owner)) {
      throw new TypeError("child project is outside the parent project lineage");
    }
    return child;
  }

  async #generation(value: ResourceRef<"generation">): Promise<ResourceRef<"generation">> {
    const generation = await resourceRef(value);
    if (generation.kind !== "generation"
      || !this.#contracts.canonicalEqual(generation.provider, this.#project.provider)) {
      throw new TypeError("project generation belongs to another provider");
    }
    return generation;
  }

  #key(value: string): void {
    if (typeof value !== "string" || value.length === 0 || value.length > 255 || /\p{Cc}/u.test(value)) {
      throw new TypeError("project operation identity is invalid");
    }
  }
}
