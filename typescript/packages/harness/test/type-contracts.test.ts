import { expect, test } from "bun:test";
import type {
  AgentHarness, AgentId, ApprovalBinding, Authority, ClientCommand, Command, ContentBindings, ConversationMessage, ConversationMessageId, ConversationPage, ConversationState, Event, FileRef, ProviderOperationId, HarnessRuntimeHost, Interaction, InteractionId, InteractionResolution, InteractionTicket, Policy,
  ForkRequest, ForkSeed, ModelContextSelection, ModelToolDefinition, NativeContracts, OperationId, PolicyDigest, ProjectMergeNotice, ProjectableConversation, ProviderJoinProof, ResourceRef, ResourceRevision, ResumableTask, RuntimeSchema, RuntimeTaskId, TaskContext,
  SharedGrant, TaskGroup, ToolRef, VolumeOwner, VolumeRef,
} from "../src/index.js";
import { Harness, TaskDefinition, composeContentBindings, decodeEventPayload, defineRuntimeSchema, defineTool, parseIdentity, resourceRef } from "../src/index.js";
import type { ClientFrame, HandshakeRequest } from "../generated/proto/harness/v2/harness_pb.js";

const provider = { namespace: "type-test", family: "filesystem", version: "2" } as const;
const project: VolumeRef<"project"> = {
  provider, id: "workspace", class: "project", owner: { kind: "project", id: "example" },
};
const privateVolume: VolumeRef<"agent_private"> = {
  provider, id: "private", class: "agent_private", owner: {
    kind: "agent", id: "01010101-0101-0101-0101-010101010101" as AgentId,
  },
};
const shared: VolumeRef<"session_shared"> = {
  provider, id: "shared", class: "session_shared", owner: { kind: "session", id: "example" },
};
const objectsPrivate: VolumeRef<"agent_private", "objects"> = {
  provider: { ...provider, family: "objects" }, id: "private-object", class: "agent_private",
  owner: { kind: "agent", id: "01010101-0101-0101-0101-010101010101" as AgentId },
};
const memoryGeneration: ResourceRef<"generation"> = {
  kind: "generation", provider: { namespace: "local", family: "memory", version: "2" },
  key: [1], version: "1",
};
// @ts-expect-error a directory page is pinned to a generation, not a mutable workspace
const wrongDirectoryGeneration: ResourceRef<"generation"> = { ...memoryGeneration, kind: "workspace" };

// @ts-expect-error project volume cannot be owned by an agent
const wrongOwner: VolumeRef<"project"> = privateVolume;
// @ts-expect-error private volume ownership requires an admitted agent identity
const untypedPrivateOwner: Extract<VolumeOwner, { kind: "agent" }>["id"] = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error conversation binding cannot be represented by an unvalidated string
const untypedBoundAgent: ConversationState["agent"] = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error provider families remain pinned through volume and file references
const wrongFamily: VolumeRef<"agent_private", "filesystem"> = objectsPrivate;
// @ts-expect-error a private file cannot masquerade as a project file
const wrongFile: FileRef<"project"> = { volume: privateVolume, path: "a", version: "1",
  descriptor: { sha256: Array(32).fill(0), byte_length: 0, media_type: "text/plain" }, display_name: "a" };
// @ts-expect-error only a session-shared volume can be delegated as a shared grant
const wrongGrant: SharedGrant = { volume: project, child_agent: "01010101-0101-0101-0101-010101010101" as AgentId, operations: ["read"] };
// @ts-expect-error a shared grant must authorize at least one operation
const emptySharedGrant: SharedGrant = { volume: shared, child_agent: "01010101-0101-0101-0101-010101010101" as AgentId, operations: [] };
// @ts-expect-error a fork cannot admit an unvalidated plain string as an agent identity
const untypedForkAgent: ForkRequest["child_agent"] = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error attached agents retain their identity type at the API boundary
const untypedAttachedAgent: ForkRequest["attached_agents"][number] = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error a fork operation is a stable typed reconciliation identity
const untypedForkOperation: ForkRequest["operation_id"] = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error the child-private capture is an immutable Filesystem generation
const wrongPrivateGeneration: ForkSeed["child_private_generation"] = { kind: "artifact", provider, key: [1], version: null };
// @ts-expect-error tool results require the exact parent-call link
const unlinkedToolResult: ConversationMessage<"tool_result">["reply_to"] = null;
// @ts-expect-error ordinary user records cannot carry a tool-call identity
const toolIdOnUser: ConversationMessage<"user">["tool_call_id"] = "call";
// @ts-expect-error a project merge receipt can only append a merge notice
const wrongMergeNoticeKind: ProjectMergeNotice["kind"] = "assistant";
// @ts-expect-error a Filesystem merge proof cannot claim an Objects provider
const wrongMergeProofProvider: ProviderJoinProof["provider"] = { ...provider, family: "objects" };
// @ts-expect-error a merge proof cannot encode the absence of a provider statement
const missingMergeProofStatement: ProviderJoinProof["statement"] = null;
// @ts-expect-error full Rust u64 conversation positions cannot be rounded JavaScript numbers
const roundedConversationSequence: ConversationMessage["sequence"] = 1;
// @ts-expect-error canonical UUID message identities require native admission
const unvalidatedMessageId: ConversationMessageId = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error model context selections cannot contain arbitrary unvalidated strings
const unvalidatedSelectionId: ModelContextSelection["messageIds"][number] = "01010101-0101-0101-0101-010101010101";
// @ts-expect-error provider-authoritative conversation revisions are full-width u64 values
const roundedContextRevision: ModelContextSelection["conversationRevision"] = 1;
// @ts-expect-error paged history cursors retain full Rust u64 precision
const roundedPageCursor: ConversationPage["next_sequence"] = 1;
// @ts-expect-error an unversioned loaded subset cannot prove its durable conversation revision
const unversionedProjection: ProjectableConversation = { agent: null, messages: [] };
// @ts-expect-error interaction IDs are validated addresses rather than free-form strings
const untypedInteractionId: InteractionId = "question";
// @ts-expect-error approved operations retain their stable reconciliation identity
const untypedApprovalOperation: ApprovalBinding["operation_id"] = "operation";
// @ts-expect-error approval tickets must pin one exact action binding
const unboundApproval: (InteractionTicket & { kind: "approval" })["approval"] = null;
// @ts-expect-error a question cannot carry an approval's action binding
const approvalOnQuestion: (InteractionTicket & { kind: "question" })["approval"] = {} as ApprovalBinding;
// @ts-expect-error a project capture requires a generation revision, not an artifact
const wrongRevision: ResourceRevision = { kind: "project", reference: { volume: project, generation: { kind: "artifact", provider, key: [1], version: null } } };
// @ts-expect-error stream history cannot be captured through a filesystem provider
const wrongHistoryProvider: ResourceRevision = { kind: "history", reference: { kind: "stream", provider, key: [1], version: "1" } };
// @ts-expect-error parent-controlled forks use conversation authorities
const wrongAuthority: Authority<"conversation"> = { kind: "agent", id: "agent" };
// @ts-expect-error execution handlers are not part of model-visible tool schemas
const executableModelSchema: ModelToolDefinition = { name: "tool", revision: "1", description: "tool", inputSchema: {}, outputSchema: {}, handler: () => 1 };
// @ts-expect-error an offline retry command cannot contain inline message text
const inlineOutboxBody: ClientCommand = { operationId: "op" as OperationId, authority: { kind: "conversation", id: "conversation" }, kind: "message.append", payload: { text: "secret" }, offlineSafe: true };
// @ts-expect-error offline metadata cannot persist an arbitrary credential-bearing string
const stringOutboxMetadata: ClientCommand = { operationId: "op" as OperationId, authority: { kind: "conversation", id: "conversation" }, kind: "message.append", payload: { metadata: { note: "secret" } }, offlineSafe: true };
// @ts-expect-error interaction versions use exact unsigned 64-bit positions
const roundedInteractionVersion: InteractionResolution = { id: "01010101-0101-0101-0101-010101010101" as InteractionId, expected_version: 1, outcome: { kind: "approved" } };
// @ts-expect-error project merge publication identities are exact validated bytes, not strings
const stringMergeOperation: ProviderOperationId = "merge-1";
// @ts-expect-error a frame tag cannot carry a different protobuf payload
const mismatchedClientFrame: ClientFrame["frame"] = { case: "resume", value: {} as HandshakeRequest };
// @ts-expect-error replaceable policies must expose a pinned implementation revision
const unpinnedPolicy: Policy = { evaluate: async () => ({ kind: "allow" }) };
// @ts-expect-error durable hosts must declare which policy implementation they enforce
const unpinnedHostPolicy: Pick<HarnessRuntimeHost, "policyIdentity"> = {};
// @ts-expect-error unvalidated byte arrays cannot claim a nonzero 32-byte policy digest
const unvalidatedPolicyDigest: PolicyDigest = [1];
// @ts-expect-error local approvals must bind a stable operation and exact action digest
const unboundLocalApproval: Interaction = { id: "approval", kind: "approval", prompt: "approve" };

function nativeValidationKeepsOwnerClass(native: NativeContracts): VolumeRef<"project"> {
  return native.validate("volume_ref", project);
}

function canonicalComparisonKeepsItsRecordType(native: NativeContracts, generation: ResourceRef<"generation">): void {
  native.canonicalEqual(project, project);
  // @ts-expect-error comparing two volume classes cannot erase their ownership distinction
  native.canonicalEqual(project, privateVolume);
  // @ts-expect-error a resource revision cannot be compared as a project volume
  native.canonicalEqual(project, generation);
}

function nativeAdmissionDoesNotPromiseOriginalIdentitySpelling(native: NativeContracts): void {
  const compact = { provider, id: "original-spelling" as const, class: "project" as const,
    owner: { kind: "project" as const, id: "original-owner" as const } };
  const admitted = native.validate("volume_ref", compact);
  const ownerClass: "project" = admitted.class;
  const family: "filesystem" = admitted.provider.family;
  // @ts-expect-error admission preserves the semantic class, not arbitrary input string literals
  const originalSpelling: "original-spelling" = admitted.id;
  void [ownerClass, family, originalSpelling];
}

function nativeAdmissionIsRequiredAtComposition(native: NativeContracts): void {
  const builder = Harness.builder(native);
  const content = composeContentBindings(native, [{ volume: project, content: {} as ContentBindings }]);
  // @ts-expect-error a runtime cannot be built without initialized Rust admission
  Harness.builder();
  // @ts-expect-error content routing cannot silently skip Rust volume admission
  composeContentBindings([{ volume: project, content }]);
  void [builder, content];
}

function cannotConstructRuntimeDirectly(): void {
  // @ts-expect-error runtime construction must pass through Builder or a validated scope
  new AgentHarness(new Map(), new Map(), {}, undefined, new Map(), new Map(), new Map());
}

function recordedEventCannotExposeBearerProof(event: Event): Uint8Array {
  // @ts-expect-error durable event scope is non-bearer and has no reusable proof
  void event.scope.proof;
  return event.attestation;
}

function durableTasksNeedPinnedTypes(harness: AgentHarness, id: RuntimeTaskId): void {
  // @ts-expect-error a typed checkpoint transition needs its own pinned state schema
  const missingState: ResumableTask<number, number, number> = { initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; } };
  void missingState;
  // @ts-expect-error a resumable task cannot omit its input/output schemas
  TaskDefinition.resumable("durable", "1", { initial: (input: number) => input,
    async transition() { return { kind: "finish" as const, output: 1 }; } }, { implementationDigest: "ab".repeat(32) });
  // @ts-expect-error reattachment may not invent a caller-selected result type
  void harness.attach<number>(id);
  // @ts-expect-error arbitrary parser objects cannot impersonate pinned runtime schemas
  const unpinned: RuntimeSchema<number> = { id: "number", document: { type: "number" }, parse: (value: unknown) => Number(value) };
  // @ts-expect-error a bare boolean is not a durable schema document
  void defineRuntimeSchema("vacuous", true, (value: unknown) => value);
  void unpinned;
}

function toolsNeedTypedAdmission(): void {
  // @ts-expect-error a tool handler cannot claim typed inputs without a parser
  void defineTool<number, number>({ name: "unsafe", revision: "1", description: "unsafe",
    inputSchema: { type: "number" }, outputSchema: { type: "number" } }, (_context, value) => value);
}

function hostRecoveryCannotInventTypes(host: HarnessRuntimeHost, id: RuntimeTaskId): void {
  // @ts-expect-error recovered task output is unknown until its pinned definition validates it
  void host.attach<number>(id, {} as AgentHarness);
  if (host.reconcileBatch) {
    // @ts-expect-error recovered batch output is likewise unknown
    void host.reconcileBatch<number>("group" as never, "batch" as never, {} as AgentHarness);
  }
}

function typedAnswerRequiresSchema(context: TaskContext, schema: RuntimeSchema<number>): void {
  void context.interactTyped({ id: "question", kind: "question", prompt: "Number?" }, schema)
    .then(answer => { if (answer.kind === "answered") { const number: number = answer.value; void number; } });
  // @ts-expect-error approvals cannot masquerade as schema-validated question answers
  void context.interactTyped({ id: "approval", kind: "approval", prompt: "Approve?" }, schema);
}

function taskExecutionCannotReachHostBindings(context: TaskContext, group: TaskGroup<unknown>): void {
  // @ts-expect-error task code has no raw Harness/content binding escape hatch
  void context.harness.content;
  // @ts-expect-error groups cannot expose the Harness through a second path
  void group.harness.content;
}

function childTaskHandlesDoNotExposeExecutors(context: TaskContext, definition: TaskDefinition<number, number>): void {
  const child = context.task(definition);
  // @ts-expect-error task code receives an opaque child handle, not its implementation
  void child.implementation;
  // @ts-expect-error task code cannot bypass the admitted child handle
  void context.spawn(definition, 1);
  void context.spawn(child, 1);
  const group = context.group<number>({ kind: "collect-all" });
  // @ts-expect-error a scoped group cannot accept a raw task implementation
  void group.map(definition, [1]);
  void group.map(child, [1]);
}

function durableToolOutcomeCannotBeErased(context: TaskContext, tool: ToolRef<number, number>, operation: OperationId): void {
  // @ts-expect-error task code cannot forge an admitted tool handle from its declarative fields
  const forged: ToolRef<number, number> = { definition: tool.definition };
  void forged;
  // @ts-expect-error a registered tool handle never exposes its executor
  void tool.executor;
  // @ts-expect-error a registered tool handle never exposes its inline handler
  void tool.definition.handler;
  // @ts-expect-error parsing code stays inside the registered execution boundary
  void tool.definition.parseInput;
  const outcome: Promise<import("../src/index.js").Outcome<number, OperationId>> = context.callDurable(operation, tool, 1);
  // @ts-expect-error a durable call may remain indeterminate and cannot be a bare result
  const bare: Promise<number> = context.callDurable(operation, tool, 1);
  void outcome.then(result => {
    if (result.kind === "indeterminate") {
      const exactIdentity: OperationId = result.operationId;
      void exactIdentity;
    }
  });
  void bare;
}

function eventPayloadNeedsDecoder(harness: Harness, command: Command<{ readonly kind: "bind_conversation"; readonly agent: AgentId }>): void {
  const raw = harness.apply(command);
  // @ts-expect-error undecoded Rust event payload is unknown
  void raw.event.payload.kind;
  const typed = decodeEventPayload(raw.event, (value: unknown): { readonly kind: "conversation_bound"; readonly agent: AgentId } => {
    if (value === null || typeof value !== "object" || !("kind" in value)
      || value.kind !== "conversation_bound" || !("agent" in value) || typeof value.agent !== "string") {
      throw new TypeError("unexpected Rust event payload");
    }
    return { kind: "conversation_bound", agent: value.agent as AgentId };
  });
  const kind: "conversation_bound" = typed.payload.kind;
  void kind;
}

function admittedContentCannotBeRebound(content: ContentBindings): void {
  // @ts-expect-error admitted owner routing is read-only to task consumers
  content.read = async () => new Uint8Array();
  // @ts-expect-error an admitted writer cannot be redirected to another owner
  content.writer!.stage = async () => { throw new Error("not the owner"); };
}

function identityConstructorsKeepDistinctBrands(): void {
  void parseIdentity("agent", "01010101-0101-0101-0101-010101010101").then(agent => {
    const admitted: AgentId = agent;
    // @ts-expect-error an agent identity cannot masquerade as an operation identity
    const operation: OperationId = agent;
    void [admitted, operation];
  });
  // @ts-expect-error only Rust-supported identity kinds are constructible
  void parseIdentity("arbitrary", "01010101-0101-0101-0101-010101010101");
  void parseIdentity("interaction", "01010101-0101-0101-0101-010101010101").then(id => {
    const admitted: InteractionId = id;
    // @ts-expect-error interaction identity is not an operation identity
    const operation: OperationId = id;
    void [admitted, operation];
  });
}

function nativeResourceValidationKeepsProviderKind(reference: ResourceRef<"generation">): void {
  void resourceRef(reference).then(generation => {
    const exact: ResourceRef<"generation"> = generation;
    // @ts-expect-error a generation cannot become an artifact after validation
    const artifact: ResourceRef<"artifact"> = generation;
    void [exact, artifact];
  });
}

test("v2 ownership, revision, and authority contracts remain discriminated", () => {
  expect([project.class, privateVolume.class, shared.class]).toEqual(["project", "agent_private", "session_shared"]);
  void [wrongOwner, untypedPrivateOwner, untypedBoundAgent, wrongFamily, wrongFile, wrongGrant, emptySharedGrant, untypedForkAgent, untypedAttachedAgent, untypedForkOperation, wrongPrivateGeneration, unlinkedToolResult, toolIdOnUser, wrongMergeNoticeKind, wrongMergeProofProvider, missingMergeProofStatement, roundedConversationSequence, unvalidatedMessageId, unvalidatedSelectionId, roundedContextRevision, roundedPageCursor, unversionedProjection, untypedInteractionId, untypedApprovalOperation, unboundApproval, approvalOnQuestion, wrongRevision, wrongHistoryProvider, wrongAuthority, executableModelSchema, inlineOutboxBody, stringOutboxMetadata, roundedInteractionVersion, stringMergeOperation, mismatchedClientFrame, unpinnedPolicy, unpinnedHostPolicy, unvalidatedPolicyDigest, unboundLocalApproval];
  void cannotConstructRuntimeDirectly;
  void nativeValidationKeepsOwnerClass;
  void canonicalComparisonKeepsItsRecordType;
  void nativeAdmissionDoesNotPromiseOriginalIdentitySpelling;
  void nativeAdmissionIsRequiredAtComposition;
  void recordedEventCannotExposeBearerProof;
  void admittedContentCannotBeRebound;
  void identityConstructorsKeepDistinctBrands;
  void nativeResourceValidationKeepsProviderKind;
  void durableTasksNeedPinnedTypes;
  void toolsNeedTypedAdmission;
  void hostRecoveryCannotInventTypes;
  void typedAnswerRequiresSchema;
  void durableToolOutcomeCannotBeErased;
  void eventPayloadNeedsDecoder;
});
