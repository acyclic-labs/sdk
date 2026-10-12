import { type Model, type ModelProvider, type ToolJsonSchema, type ContextBuilder } from "./model.js";
import { freezeSchema, type HarnessExecutionProvider } from "./task.js";
import { type WasmMachineIdentityWire, NativeContracts } from "./native-contracts.js";
import { type OperationId } from "./index.js";
export interface BoundModel { readonly identity: Model; readonly provider: ModelProvider }

export function bindModel(identity: Model, provider: ModelProvider): BoundModel {
  for (const label of [identity.provider, identity.name, identity.revision]) {
    if (typeof label !== "string" || !label.trim()) throw new TypeError("model identity requires a provider, name, and revision");
  }
  const { contextCapacity, countTokens, generate, reconcile } = provider;
  if (typeof contextCapacity !== "function" || typeof countTokens !== "function"
    || typeof generate !== "function" || typeof reconcile !== "function") {
    throw new TypeError("model provider requires contextCapacity, countTokens, generate, and reconcile");
  }
  const boundProvider = Object.freeze({
    contextCapacity: contextCapacity.bind(provider), countTokens: countTokens.bind(provider),
    generate: generate.bind(provider), reconcile: reconcile.bind(provider),
  });
  return Object.freeze({ identity: Object.freeze({ ...identity, options: freezeSchema(structuredClone(identity.options)) }), provider: boundProvider });
}
export interface InteractionHandler { route(interaction: Interaction): Promise<Answer> }
declare const policyDigestBrand: unique symbol;
export type PolicyDigest = readonly number[] & Readonly<{ readonly [policyDigestBrand]: true }>;
export interface ComponentIdentity {
  readonly name: string;
  readonly version: string;
  readonly digest: PolicyDigest;
}
export type PolicyIdentity = ComponentIdentity | Readonly<{ composition: readonly [PolicyIdentity, PolicyIdentity] }>;

export async function policyIdentity(name: string, version: string, digest: Uint8Array): Promise<ComponentIdentity> {
  return admitPolicyIdentity({ name, version, digest: Object.freeze([...digest]) });
}

async function admitPolicyIdentity(snapshot: WasmMachineIdentityWire): Promise<ComponentIdentity> {
  const admitted = (await NativeContracts.create()).validate("machine_identity", snapshot);
  return Object.freeze({
    name: admitted.name,
    version: admitted.version,
    digest: Object.freeze([...admitted.digest]) as PolicyDigest,
  });
}
export interface Policy {
  /** Immutable implementation revision, checked at admission and again at host dispatch. */
  identity(): ComponentIdentity;
  evaluate(invocation: Invocation, scope: EffectiveScope): Promise<PolicyDecision>;
}
interface EffectivePolicy {
  identity(): PolicyIdentity;
  evaluate(invocation: Invocation, scope: EffectiveScope): Promise<EffectivePolicyDecision>;
}
export type Interaction =
  | { readonly id: string; readonly kind: "question"; readonly prompt: string; readonly responseSchema?: ToolJsonSchema; readonly deadline?: Date }
  | { readonly id: string; readonly kind: "choice"; readonly prompt: string; readonly options: readonly { readonly id: string; readonly label: string; readonly description?: string }[]; readonly minSelections: number; readonly maxSelections: number; readonly deadline?: Date }
  | { readonly id: string; readonly kind: "form"; readonly title: string; readonly schema: ToolJsonSchema; readonly deadline?: Date }
  | { readonly id: string; readonly kind: "approval"; readonly prompt: string; readonly operationId: OperationId; readonly actionDigest: readonly number[]; readonly deadline?: Date };
export type TypedInteractionInput =
  | Omit<Extract<Interaction, { kind: "question" }>, "responseSchema">
  | Omit<Extract<Interaction, { kind: "form" }>, "schema">;
export type Answer<Value = unknown> =
  | { readonly kind: "accepted"; readonly text: string }
  | { readonly kind: "answered"; readonly value: Value }
  | { readonly kind: "approved" }
  | { readonly kind: "declined" | "cancelled" | "expired" | "denied" }
  | { readonly kind: "indeterminate"; readonly operationId: string };

export class ToolApprovalError extends Error {
  constructor(readonly kind: "declined" | "cancelled" | "expired" | "denied" | "indeterminate" | "invalid_response", readonly operationId: OperationId) {
    super(`tool approval was not granted: ${kind}`);
  }
}
export interface Invocation { readonly kind: "tool"; readonly tool: string; readonly arguments: unknown }
export interface EffectiveScope { readonly grants: readonly string[]; readonly limits: Readonly<{ readonly concurrency?: number; readonly deadline?: Date; readonly maxSteps?: number }> }
export type PolicyDecision = { readonly kind: "allow" } | { readonly kind: "deny"; readonly reason: string } | { readonly kind: "require-approval"; readonly prompt: string };
interface PolicyApproval { readonly prompt: string; readonly policy: PolicyIdentity }
type EffectivePolicyDecision = PolicyDecision | Readonly<{ kind: "require-approvals"; approvals: readonly PolicyApproval[] }>;

export class ExecutionScope {
  readonly grants: readonly string[];
  readonly #concurrency: number | undefined;
  readonly #maxSteps: number | undefined;
  readonly #deadlineEpochMs: number | undefined;
  constructor(
    readonly modelBinding?: BoundModel,
    readonly contextBuilder?: ContextBuilder,
    readonly interactionHandler?: InteractionHandler,
    readonly policyProvider?: EffectivePolicy,
    grants: readonly string[] = [],
    limits: EffectiveScope["limits"] = {},
    readonly grantsExplicit = false,
    readonly executionProvider?: HarnessExecutionProvider,
  ) {
    if (grants.some(value => !value.trim())) throw new TypeError("capability grant is empty");
    if (limits.concurrency !== undefined && (!Number.isSafeInteger(limits.concurrency) || limits.concurrency <= 0)) throw new RangeError("scope concurrency must be a positive safe integer");
    if (limits.maxSteps !== undefined && (!Number.isSafeInteger(limits.maxSteps) || limits.maxSteps <= 0)) throw new RangeError("scope maxSteps must be a positive safe integer");
    if (limits.deadline !== undefined && !Number.isFinite(limits.deadline.getTime())) throw new RangeError("scope deadline must be valid");
    this.grants = Object.freeze([...new Set(grants)]);
    this.#concurrency = limits.concurrency;
    this.#maxSteps = limits.maxSteps;
    this.#deadlineEpochMs = limits.deadline?.getTime();
    Object.freeze(this);
  }
  get limits(): EffectiveScope["limits"] {
    return Object.freeze({
      ...(this.#concurrency === undefined ? {} : { concurrency: this.#concurrency }),
      ...(this.#maxSteps === undefined ? {} : { maxSteps: this.#maxSteps }),
      ...(this.#deadlineEpochMs === undefined ? {} : { deadline: new Date(this.#deadlineEpochMs) }),
    });
  }
  model(identity: Model, provider: ModelProvider): ExecutionScope { return new ExecutionScope(bindModel(identity, provider), this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  context(value: ContextBuilder): ExecutionScope { return new ExecutionScope(this.modelBinding, value, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  interactions(value: InteractionHandler): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, value, this.policyProvider, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  policy(value: Policy): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, value, this.grants, this.limits, this.grantsExplicit, this.executionProvider); }
  execution(value: HarnessExecutionProvider): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, this.limits, this.grantsExplicit, value); }
  /** Select a subset of the parent's grants; an empty selection removes all. */
  onlyGrants(...values: readonly string[]): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, this.policyProvider, values, this.limits, true, this.executionProvider); }
  grant(...values: readonly string[]): ExecutionScope { return this.onlyGrants(...this.grants, ...values); }
  withLimits(value: EffectiveScope["limits"]): ExecutionScope { return new ExecutionScope(this.modelBinding, this.contextBuilder, this.interactionHandler, this.policyProvider, this.grants, value, this.grantsExplicit, this.executionProvider); }
}

export function narrowLimits(parent: EffectiveScope["limits"], child: EffectiveScope["limits"]): EffectiveScope["limits"] {
  const concurrency = child.concurrency ?? parent.concurrency;
  if (concurrency !== undefined && (!Number.isSafeInteger(concurrency) || concurrency <= 0)) throw new RangeError("scope concurrency must be a positive safe integer");
  if (parent.concurrency !== undefined && concurrency !== undefined && concurrency > parent.concurrency) throw new Error("child scope cannot widen concurrency");
  const maxSteps = child.maxSteps ?? parent.maxSteps;
  if (maxSteps !== undefined && (!Number.isSafeInteger(maxSteps) || maxSteps <= 0)) throw new RangeError("scope maxSteps must be a positive safe integer");
  if (parent.maxSteps !== undefined && maxSteps !== undefined && maxSteps > parent.maxSteps) throw new Error("child scope cannot widen maxSteps");
  const deadline = child.deadline ?? parent.deadline;
  if (deadline !== undefined && Number.isNaN(deadline.getTime())) throw new RangeError("scope deadline must be valid");
  if (parent.deadline !== undefined && deadline !== undefined && deadline > parent.deadline) throw new Error("child scope cannot extend deadline");
  return { ...(concurrency === undefined ? {} : { concurrency }), ...(maxSteps === undefined ? {} : { maxSteps }), ...(deadline === undefined ? {} : { deadline: new Date(deadline) }) };
}

export function composePolicies(parent: EffectivePolicy | undefined, child: EffectivePolicy | undefined): EffectivePolicy | undefined {
  if (!parent || parent === child) return child ?? parent;
  if (!child) return parent;
  return {
    identity: () => Object.freeze({ composition: Object.freeze([
      parent.identity(), child.identity(),
    ] as const) }),
    async evaluate(invocation, scope) {
      const inherited = await parent.evaluate(invocation, scope);
      if (inherited.kind === "deny") return inherited;
      const narrowed = await child.evaluate(invocation, scope);
      if (narrowed.kind === "deny") return narrowed;
      const approvals = [
        ...policyApprovals(inherited, parent.identity()),
        ...policyApprovals(narrowed, child.identity()),
      ];
      return approvals.length === 0 ? { kind: "allow" } : { kind: "require-approvals", approvals };
    },
  };
}

export function policyApprovals(decision: EffectivePolicyDecision, identity: PolicyIdentity | null): readonly PolicyApproval[] {
  if (decision.kind === "require-approvals") return decision.approvals;
  if (decision.kind !== "require-approval") return [];
  if (identity === null) throw new TypeError("approval policy has no pinned identity");
  return [{ prompt: decision.prompt, policy: identity }];
}

export async function validatePolicyIdentity(identity: PolicyIdentity | WasmMachineIdentityWire | null): Promise<PolicyIdentity | null> {
  if (identity === null) return null;
  // Snapshot all composed identities before recursive asynchronous admission.
  return validatePolicyIdentitySnapshot(structuredClone(identity), new Set<object>());
}

async function validatePolicyIdentitySnapshot(identity: PolicyIdentity | WasmMachineIdentityWire,
  ancestors: Set<object>): Promise<PolicyIdentity> {
  if (ancestors.has(identity)) throw new TypeError("policy identity contains a cycle");
  ancestors.add(identity);
  try {
    if ("composition" in identity) {
      if (Object.keys(identity).length !== 1 || !Array.isArray(identity.composition)
        || identity.composition.length !== 2) throw new TypeError("policy composition is invalid");
      const [parent, child] = identity.composition;
      if (!parent || !child) throw new TypeError("policy composition is incomplete");
      const checkedParent = await validatePolicyIdentitySnapshot(parent, ancestors);
      const checkedChild = await validatePolicyIdentitySnapshot(child, ancestors);
      return Object.freeze({ composition: Object.freeze([checkedParent, checkedChild] as const) });
    }
    if (Object.keys(identity).sort().join("\0") !== "digest\0name\0version") {
      throw new TypeError("policy identity has unexpected fields");
    }
    if (!Array.isArray(identity.digest)) throw new TypeError("policy implementation digest must be an array");
    return await admitPolicyIdentity(identity);
  } finally {
    ancestors.delete(identity);
  }
}

export function samePolicyIdentity(left: PolicyIdentity | null, right: PolicyIdentity | null): boolean {
  if (left === null || right === null) return left === right;
  if ("composition" in left || "composition" in right) {
    return "composition" in left && "composition" in right
      && samePolicyIdentity(left.composition[0], right.composition[0])
      && samePolicyIdentity(left.composition[1], right.composition[1]);
  }
  return left.name === right.name && left.version === right.version
    && left.digest.every((byte, index) => byte === right.digest[index]);
}
