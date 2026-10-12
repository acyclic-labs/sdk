//! Shared portable defaults.

use super::{
    FilesystemHost, FilesystemTaskRuntime, ForkPreparationLimits, ForkToolArguments,
    ParentProjectController, RetainedSourceBinding,
    TaskCommandHost, TaskCommandProgress, FORK_PREPARE_TASK_COMMAND_KIND,
    MODEL_TASK_COMMAND_KIND, TASK_ADMIT_PREPARED_COMMAND_KIND, fork_preparation_request,
};
use crate::runtime::TaskStateProvider;
use crate::{
    Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{ContentGrant, ConversationMessage, FileRef, VolumeOperation, VolumeRef},
    core::{Action, Authority, AuthorityVerifier, Command, Scope},
    durable_tool::{ResumableTool, ResumableToolRegistry},
    fork::{ForkReport, ForkRequest, ForkSeed},
    merge::ProjectMergeReceipt,
    resources::GenerationRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskContext, ToolContext, task_id_for_admission},
    store::StreamAggregate,
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
    workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand, WorkflowJournal},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ConflictSide, JoinOutcome};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use crate::store::history::HistoryIo as _;

const FORK_SOURCE_STAGE: &str = "acyclic.recursive.fork.source_stage.v1";
const FORK_OBSERVE: &str = "acyclic.recursive.fork.observe.v1";
const FORK_PUBLISH: &str = "acyclic.recursive.fork.publish.v1";
const MERGE_APPLY: &str = "acyclic.recursive.merge.apply.v1";
const MERGE_PUBLISH: &str = "acyclic.recursive.merge.publish.v1";
const REVISION: &str = "portable-recursive-1";

/// Portable and resumable recursive tool registries.
pub struct RecursiveDefaultToolset {
    /// StockExecutor and DurableToolRunner registrations.
    pub tools: ToolRegistry,
    /// The same fork/merge implementations for typed resumable callers.
    pub resumable_tools: ResumableToolRegistry,
}

/// Authenticated current caller, not the root session or a selected source task.
pub struct RecursiveToolOwner<P, A, O> {
    /// Actual runtime containing the ordinary preparation/child definitions.
    pub runtime: Arc<FilesystemTaskRuntime<P, A, O>>,
    /// The same actual original caller owner used by the workflow journal.
    pub journal_owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    /// Original tool-effect journal.
    pub workflow: Arc<dyn WorkflowJournal>,
}

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_SOURCE_REQUIRED: Error = Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported, reason: "recursive source has no original native producer" };

/// Real provider configuration for one exact caller and retained source cut.
pub trait RecursiveDefaultToolSource<P, A, O>: acyclic_stream::ProviderPlatform {
    /// Binds the actual caller/effect to its owner-controlled workflow journal.
    fn owner<'a>(&'a self, context: &'a ToolContext, tool: &'a ResumableTool,
        invocation: &'a ToolInvocation, cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveToolOwner<P, A, O>>>;
    /// Authenticates the immutable source and allocation before staging.
    fn select<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments,
        cancellation: &'a CancellationToken, io: Option<&'a super::PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveForkSourceIntent>>;
    /// Creates or restores the authenticated fork source.
    fn fork<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments,
        intent: &'a RecursiveForkSourceIntent, original: Option<&'a RecursiveForkToolPlan>,
        cancellation: &'a CancellationToken, io: Option<&'a super::PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveForkToolBinding<P, A, O>>>;
    /// Binds the actual destination and explicitly readable selected source.
    fn merge<'a>(&'a self, context: &'a ToolContext, arguments: &'a MergeToolArguments,
        original: Option<&'a RecursiveMergeToolPlan>, cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveMergeToolBinding<P, A, O>>>;
    /// Restores the exact retained source for an ordinary preparation worker.
    fn preparation<'a>(&'a self, context: &'a TaskContext,
        preparation: &'a crate::resource_preparation::TaskResourcePreparation,
        cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RetainedSourceBinding<A, O>>>;

    /// True only when every recursive producer preserves the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_source(&self) -> bool { false }

    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_with_native_work<'a>(&'a self, _context: &'a Arc<crate::runtime::NativeTaskContext>,
        _preparation: &'a crate::resource_preparation::TaskResourcePreparation,
        _cancellation: &'a CancellationToken, _work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>> {
        Err(NATIVE_SOURCE_REQUIRED)
    }

    /// Restores only the retained source for passive reconciliation.
    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_for_reconciliation_with_native_work<'a>(
        &'a self, _context: &'a Arc<crate::runtime::NativeTaskContext>,
        _preparation: &'a crate::resource_preparation::TaskResourcePreparation,
        _cancellation: &'a CancellationToken, _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>> {
        Err(NATIVE_SOURCE_REQUIRED)
    }
}

/// The immutable source/allocation choice committed before any source staging.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveForkSourceIntent {
    /// Exact original model arguments, not rewritten defaults.
    pub arguments: ForkToolArguments,
    /// Actual calling durable task.
    pub caller: TaskId,
    /// Explicit selected Project source task; history remains caller-owned.
    pub source_task: TaskId,
    /// Independent original intended child admission identity.
    pub child_operation_id: OperationId,
    /// Canonical configured producer/allocation claims, excluding random MACs.
    pub allocation_digest: [u8; 32],
    /// Exact source revisions, logical history cut, readers and child resources.
    pub request: ForkRequest,
}

/// Fork configuration carries actual aggregates and scopes, not an admission ACK.
pub struct RecursiveForkToolBinding<P, A, O> {
    /// Exact source request and canonical Filesystem preparation provider.
    pub source: RetainedSourceBinding<A, O>,
    /// Requested ordinary child admission. Its parent is the current caller.
    pub child: TaskAdmissionRecord,
    /// Immutable reference to the authenticated caller checkpoint.
    pub source_checkpoint: FileRef,
    /// JSON pointer into child.input containing the exact msg, or its model WorkflowCommand payload FileRef.
    pub child_message_pointer: String,
    /// Explicit pointer inside immutable producer input bytes.
    pub child_message_file_pointer: Option<String>,
    /// Explicit actual typed producer/launch inputs, never a raw reference scan.
    pub input_files: Vec<FileRef>,
    /// Actual publishing parent, opened with canonical fork/content verifiers.
    pub parent: StreamAggregate<P>,
    /// Actual new child aggregate, opened with canonical fork/content verifiers.
    pub child_conversation: StreamAggregate<P>,
    /// Authenticated parent publication scope.
    pub parent_scope: Scope,
    /// Authenticated exact child conversation binding scope.
    pub child_scope: Scope,
    /// Finite actual preparation allowances.
    pub limits: ForkPreparationLimits,
}

/// An explicit import selects project data only, never another task's transcript.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeToolArguments {
    /// Authenticated readable source task, independent of the caller/destination.
    pub source: TaskId,
    /// Explicit choices for exact conflicts returned by the owning Filesystem.
    #[serde(default)]
    pub selections: Vec<MergeToolSelection>,
}

/// Provider-owned exact conflict and immutable side selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeToolSelection {
    /// Original opaque Filesystem conflict key, not a guessed path match.
    pub conflict: Vec<u8>,
    /// Exact immutable side.
    pub side: MergeToolSide,
}

/// Portable names for Filesystem's three-way immutable choices.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeToolSide { Base, Target, Source }

/// Actual current destination configuration; foreign source authority is read-only.
pub struct RecursiveMergeToolBinding<P, A, O> {
    /// Canonical Filesystem provider, shared with the durable runtime.
    pub filesystem: Arc<FilesystemHost<A, O>>,
    /// Actual destination history, opened with its canonical merge verifier.
    pub destination: StreamAggregate<P>,
    /// Authenticates the destination scope.
    pub verifier: AuthorityVerifier,
    /// Exact signed destination scope, not an unverified ref-derived grant.
    pub scope: Scope,
    /// Current caller's writable destination project.
    pub project: VolumeRef,
    /// Selected source conversation identity, not a transcript read grant.
    pub source: Authority,
    /// Selected readable source project.
    pub source_project: VolumeRef,
    /// Exact source generation selected before any effect.
    pub source_generation: GenerationRef,
    /// Authority seal for an approved project import.
    pub source_read: Option<crate::source_authorization::ProjectSourceReadHandle>,
    /// Ref-only canonical merge notice.
    pub notice: ConversationMessage,
}

/// Full original fork provenance is retained before ordinary task admission.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveForkToolPlan {
    /// Exact model-supplied id/msg/source, including default-source provenance.
    pub arguments: ForkToolArguments,
    /// Actual calling durable task.
    pub caller: TaskId,
    /// Exact provider request with selected revisions and child resource proofs.
    pub request: crate::resource_preparation::ResourcePreparationRequest,
    /// Ordinary preparation task; sibling of request.child under caller.
    pub preparation: TaskAdmissionRecord,
    /// Producer input pointer checked against the exact original msg.
    pub child_message_pointer: String,
    /// Original configured immutable input-file message projection.
    pub child_message_file_pointer: Option<String>,
    /// Original selected producer/allocation configuration claims.
    pub allocation_digest: [u8; 32],
    /// Exact original authenticated caller checkpoint used during restoration.
    pub source_checkpoint: FileRef,
}

/// Original inspected import, including the CAS target. Retries never rebase it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveMergeToolPlan {
    /// Exact selected source and conflict choices.
    pub arguments: MergeToolArguments,
    /// Actual calling durable task.
    pub caller: TaskId,
    /// Original admitted tool effect, separate from either task identity.
    pub operation_id: OperationId,
    /// Destination conversation identity.
    pub destination: Authority,
    /// Original selected source conversation identity.
    pub source: Authority,
    /// Original readable source workspace.
    pub source_project: VolumeRef,
    /// Original immutable source generation.
    pub source_generation: GenerationRef,
    /// Immutable RootService Project evidence, never a caller-scope grant.
    pub authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
    /// Original writable target workspace.
    pub target_project: VolumeRef,
    /// Exact compare-and-swap precondition from inspection.
    pub expected_target_generation: GenerationRef,
    /// Exact admitted ref-only notice.
    pub notice: ConversationMessage,
}

/// Installs the same real portable tools for native and browser consumers.
pub fn recursive_default_tools<P, A, O>(
    source: Option<Arc<dyn RecursiveDefaultToolSource<P, A, O>>>, maximum_request_bytes: u64,
) -> Result<RecursiveDefaultToolset>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    if maximum_request_bytes == 0 || maximum_request_bytes == u64::MAX {
        return Err(Error::Invalid("recursive request bound must be finite and positive".into()));
    }
    let mut tools = ToolRegistry::default();
    tools.register(crate::tool::files::read_file()?)?;
    tools.register(crate::tool::files::write_file()?)?;
    tools.register(crate::tool::files::edit_file()?)?;
    let mut resumable_tools = ResumableToolRegistry::default();
    for kind in [Kind::Fork, Kind::Merge] {
        let machine = Arc::new(RecursiveToolMachine::new(kind, maximum_request_bytes));
        let definition = definition(kind, maximum_request_bytes);
        let durable = ResumableTool { definition: definition.clone(), machine };
        resumable_tools.register(durable.clone())?;
        tools.register(Tool { definition, executor: Arc::new(RecursiveExecutor {
            source: source.clone(), durable, maximum_request_bytes,
        }), projection: Arc::new(crate::tool::JsonToolProjection) })?;
    }
    Ok(RecursiveDefaultToolset { tools, resumable_tools })
}

#[derive(Clone, Copy)]
enum Kind { Fork, Merge }
impl Kind {
    fn name(self) -> &'static str { match self { Self::Fork => "acyclic.fork", Self::Merge => "acyclic.merge" } }
}

fn definition(kind: Kind, maximum: u64) -> ToolDefinition {
    let input = match kind {
        Kind::Fork => json!({"type":"object","required":["id","msg"],"properties":{
            "id":{"type":"string"},"msg":{"type":"string"},"source":{"type":"string"}},"additionalProperties":false}),
        Kind::Merge => json!({"type":"object","required":["source"],"properties":{
            "source":{"type":"string"},"selections":{"type":"array","items":{"type":"object","required":["conflict","side"],"properties":{
                "conflict":{"type":"array","minItems":1,"items":{"type":"integer","minimum":0,"maximum":255}},"side":{"enum":["base","target","source"]}},"additionalProperties":false}}},"additionalProperties":false}),
    };
    let mut input = input;
    input["x-harness-recursive-request-bytes"] = json!(maximum);
    let output = json!({"type":"object"});
    ToolDefinition { name: kind.name().into(), revision: REVISION.into(),
        description: match kind { Kind::Fork => "Prepare, publish and durably admit a new child from the actual calling task; source selects only an explicitly authorized project",
            Kind::Merge => "Import an explicitly readable pinned source project into the actual caller's project through approval and the original inspected CAS target" }.into(),
        input_schema: input, output_schema: output.clone(),
        projection_schema: crate::tool::json_projection_schema(output) }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "plan", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Plan { Fork(Box<RecursiveForkToolPlan>), Merge(Box<RecursiveMergeToolPlan>) }
fn command_stage(command: &WorkflowCommand, report: Option<&ForkReport>, io: Option<&super::PreparationIo>) -> Result<&'static str> {
    match command.kind.as_str() {
        TASK_ADMIT_PREPARED_COMMAND_KIND => Ok(if report.is_some() { "child-admit" } else { "prepare" }),
        FORK_OBSERVE => Ok("observe"), FORK_PUBLISH => Ok("fork-publish"),
        MERGE_APPLY => Ok("merge-apply"), MERGE_PUBLISH => Ok("merge-publish"),
        _ => Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "recursive command identity or kind changed")),
    }
}
fn command_kind(stage: &str) -> &'static str { match stage {
    "prepare" | "child-admit" => TASK_ADMIT_PREPARED_COMMAND_KIND,
    "observe" => FORK_OBSERVE, "fork-publish" => FORK_PUBLISH,
    "merge-apply" => MERGE_APPLY, "merge-publish" => MERGE_PUBLISH,
    _ => unreachable!("recursive command stage is derived before dispatch"),
}}
fn next_stage(stage: &str, value: &Value) -> Option<&'static str> {
    match stage {
        "prepare" => Some("observe"), "observe" => Some("fork-publish"), "fork-publish" => Some("child-admit"),
        "merge-apply" if value.get("status").and_then(Value::as_str) == Some("applied") => Some("merge-publish"),
        _ => None,
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready { arguments: Value },
    SourcePinned { arguments: Value, intent: RecursiveForkSourceIntent, command: WorkflowCommand },
    Waiting { arguments: Value, plan: Plan, command: WorkflowCommand,
        report: Option<Box<ForkReport>>, receipt: Option<Box<ProjectMergeReceipt>> },
    Complete { value: Value },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitialPlan { plan: Plan, command: WorkflowCommand }
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePin { intent: RecursiveForkSourceIntent, command: WorkflowCommand }
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt { operation_id: OperationId, value: Outcome<Value>, next: Option<WorkflowCommand> }

struct RecursiveToolMachine { identity: MachineIdentity, schema: Value, maximum: u64 }
impl RecursiveToolMachine {
    fn new(kind: Kind, maximum: u64) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness/v2/recursive-tool-machine\0");
        for line in include_str!("default_tools.rs").split_inclusive('\n') {
            hash.update(line.trim_end_matches(['\r', '\n']).as_bytes());
            hash.update(b"\n");
        }
        hash.update(kind.name().as_bytes());
        hash.update(&maximum.to_le_bytes());
        let digest = *hash.finalize().as_bytes();
        Self { identity: MachineIdentity { name: kind.name().into(), version: REVISION.into(), digest }, maximum,
            schema: json!({"oneOf":[
                {"type":"object","required":["phase","arguments"],"properties":{"phase":{"const":"ready"},"arguments":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","arguments","intent","command"],"properties":{
                    "phase":{"const":"source_pinned"},"arguments":{"type":"object"},"intent":{"type":"object"},
                    "command":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","arguments","plan","command","report","receipt"],"properties":{
                    "phase":{"const":"waiting"},"arguments":{"type":"object"},"plan":{"type":"object"},
                    "command":{"type":"object"},"report":{"type":["object","null"]},"receipt":{"type":["object","null"]}},"additionalProperties":false},
                {"type":"object","required":["phase","value"],"properties":{"phase":{"const":"complete"},"value":{"type":"object"}},"additionalProperties":false}
            ]}) }
    }
}
impl ResumableMachine for RecursiveToolMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> {
        self.initialize_engine(input, None)
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        self.transition_engine(state, input, None)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool { true }
    #[cfg(not(target_arch = "wasm32"))]
    fn identity_with_native_work<'a>(&'a self, work: &acyclic_fs::NativeMaterializationWork) -> Result<&'a MachineIdentity> {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1, ..Default::default()
        }).map_err(Error::from)?;
        Ok(&self.identity)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(&self, input: &Value, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(Value, acyclic_fs::NativeAllocation)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let state = self.initialize_engine(input, Some(&io))?;
        let custody = io.take_native_custody(recursive_value_owned_bytes(&state)?)?;
        Ok((state, custody))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(&self, state: &Value, input: Value, work: &acyclic_fs::NativeMaterializationWork)
        -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = super::PreparationIo::from_native_work(work)?;
        let transition = self.transition_engine(state, &input, Some(&io))?;
        drop(input);
        let custody = io.take_native_custody(recursive_transition_owned_bytes(&transition, &io)?)?;
        Ok((transition, custody))
    }
}

impl RecursiveToolMachine {
    fn initialize_engine(&self, input: &Value, io: Option<&super::PreparationIo>) -> Result<Value> {
        recursive_byte_bound(input, self.maximum, io)?;
        if self.identity.name == "acyclic.fork" {
            recursive_decoded::<ForkToolArguments, _>(input, io, |_| Ok(()))?;
        } else { recursive_decoded::<MergeToolArguments, _>(input, io, |_| Ok(()))?; }
        #[derive(Serialize)]
        struct Ready<'a> { phase: &'static str, arguments: &'a Value }
        encode(&Ready { phase: "ready", arguments: input }, io)
    }

    fn transition_engine(&self, original_state: &Value, input: &Value, io: Option<&super::PreparationIo>) -> Result<MachineTransition> {
        recursive_byte_bound(input, self.maximum, io)?;
        let state: State = decode(original_state, io)?;
        let next = match state {
            State::Ready { arguments } => {
                if self.identity.name == "acyclic.fork" {
                    let selected: SourcePin = decode(input, io)?;
                    validate_source_intent(&selected.intent, &arguments, io)?;
                    check_source_command(&selected.command, &selected.intent, io)?;
                    State::SourcePinned { arguments, intent: selected.intent, command: selected.command }
                } else {
                    let initial: InitialPlan = decode(input, io)?;
                    validate_plan(&initial.plan, &arguments, io)?;
                    if !matches!(initial.plan, Plan::Merge(_)) {
                        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "merge tool plan names another tool"));
                    }
                    check_command(&initial.command, "merge-apply", &initial.plan, io)?;
                    State::Waiting { arguments, plan: initial.plan,
                        command: initial.command, report: None, receipt: None }
                }
            }
            State::SourcePinned { arguments, intent, command } => {
                validate_source_intent(&intent, &arguments, io)?;
                check_source_command(&command, &intent, io)?;
                let initial: InitialPlan = decode(input, io)?;
                validate_plan(&initial.plan, &arguments, io)?;
                let Plan::Fork(plan) = &initial.plan else {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "pinned fork source produced another tool plan"));
                };
                if plan.caller != intent.caller || plan.request.child.operation_id != intent.child_operation_id
                    || plan.allocation_digest != intent.allocation_digest
                    || !recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |request| Ok(*request == intent.request))?
                {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork plan changed its durably selected original source/allocation"));
                }
                check_command(&initial.command, "prepare", &initial.plan, io)?;
                State::Waiting { arguments, plan: initial.plan,
                    command: initial.command, report: None, receipt: None }
            }
            State::Waiting { arguments, plan, command, mut report, mut receipt } => {
                let stage = command_stage(&command, report.as_deref(), io)?;
                validate_plan(&plan, &arguments, io)?;
                check_command(&command, stage, &plan, io)?;
                let result: Receipt = decode(input, io)?;
                if result.operation_id != command.operation_id {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "recursive receipt belongs to another command"));
                }
                let value = match result.value {
                    Outcome::Succeeded(value) => value,
                    Outcome::Indeterminate { operation_id } => return Err(Error::Indeterminate(operation_id)),
                    Outcome::Failed { message } => {
                        #[derive(Serialize)]
                        struct Failed<'a> { status: &'static str, message: &'a str }
                        let value = encode(&Failed { status: "failed", message: &message }, io)?;
                        return Ok(MachineTransition { state: recursive_complete_state(&value, io)?,
                            commands: Vec::new(), status: MachineStatus::Failed { message } });
                    }
                    Outcome::Cancelled => return Err(recursive_reject(io, crate::wire::ErrorCode::Conflict, "recursive command cancelled without a successful receipt")),
                };
                match (&plan, stage) {
                    (Plan::Fork(fork), "prepare") => {
                        if !recursive_decoded::<TaskId, _>(&value, io, |ack| Ok(*ack == fork.preparation.task_id()))? {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "preparation admission acknowledged another child"));
                        }
                    }
                    (Plan::Fork(fork), "observe") => {
                        let observed: ForkReport = decode(&value, io)?;
                        crate::executor::with_history_io(io, |work| observed.validate(work))?;
                        if !recursive_encoded_equal(&observed.request, &fork.request.payload, io)? {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork report changed the original source request"));
                        }
                        report = Some(recursive_box(observed, io)?);
                    }
                    (Plan::Fork(_), "fork-publish") => {
                        let before = io.map(|io| io.owned_bytes()).transpose()?;
                        let seed: ForkSeed = decode(&value, io)?;
                        let expected = match report.as_ref() {
                            Some(report) => {
                                let copy = crate::executor::with_history_io(io, |work| report.copy(work).map(|(report, _)| report))?;
                                Some(crate::executor::with_history_io(io, |work| copy.into_seed(work))?)
                            }
                            None => None,
                        };
                        let changed = Some(seed) != expected;
                        drop(expected);
                        recursive_release_since(io, before)?;
                        if changed {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork publication receipt changed the prepared child"));
                        }
                    }
                    (Plan::Fork(fork), "child-admit") => {
                        if !recursive_decoded::<TaskId, _>(&value, io, |ack| Ok(*ack == fork.request.child.task_id()))? {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "child admission receipt changed its task identity"));
                        }
                        if result.next.is_some() {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "completed fork has an unexpected outbox"));
                        }
                        let prepared = report.take().ok_or_else(|| recursive_reject(
                            io, crate::wire::ErrorCode::Invalid, "fork has no prepared report"))?;
                        let seed = crate::executor::with_history_io(io, |work| (*prepared).into_seed(work))?;
                        #[derive(Serialize)]
                        struct Admitted<'a> {
                            status: &'static str, id: crate::AgentId, task: TaskId,
                            source: TaskId, operation_id: OperationId, seed: &'a ForkSeed,
                        }
                        let value = encode(&Admitted { status: "admitted", id: fork.arguments.id,
                            task: fork.request.child.task_id(), source: fork.arguments.source.unwrap_or(fork.caller),
                            operation_id: recursive_decoded::<ForkRequest, _>(&fork.request.payload, io, |request| Ok(request.operation_id))?,
                            seed: &seed }, io)?;
                        return completed(value, io);
                    }
                    (Plan::Merge(merge), "merge-apply") => {
                        if value.get("status").and_then(Value::as_str) == Some("applied") {
                            let observed: ProjectMergeReceipt = decode(value.get("receipt").ok_or_else(|| recursive_reject(
                                io, crate::wire::ErrorCode::Invalid, "applied merge has no receipt"))?, io)?;
                            validate_merge_receipt(merge, &observed, io)?;
                            receipt = Some(recursive_box(observed, io)?);
                        } else {
                            if !matches!(value.get("status").and_then(Value::as_str),
                                Some("no_changes" | "stale_target" | "conflicted" | "fenced" | "idempotency_conflict")) {
                                return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "merge receipt is not a canonical provider outcome"));
                            }
                            if result.next.is_some() {
                                return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "unapplied merge has a publication command"));
                            }
                            return completed(value, io);
                        }
                    }
                    (Plan::Merge(merge), "merge-publish") => {
                        let observed: ProjectMergeReceipt = decode(&value, io)?;
                        validate_merge_receipt(merge, &observed, io)?;
                        if receipt.as_deref() != Some(&observed) || result.next.is_some() {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "merge publication changed its original receipt"));
                        }
                        #[derive(Serialize)]
                        struct Published<'a> { status: &'static str, receipt: &'a ProjectMergeReceipt }
                        return completed(encode(&Published { status: "published", receipt: &observed }, io)?, io);
                    }
                    _ => return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "recursive stage does not match its original plan")),
                };
                let stage = next_stage(stage, &value).ok_or_else(|| recursive_reject(io, crate::wire::ErrorCode::Invalid, "recursive next stage missing"))?;
                let command = result.next.ok_or_else(|| recursive_reject(io, crate::wire::ErrorCode::Invalid, "recursive next outbox missing"))?;
                check_command(&command, stage, &plan, io)?;
                State::Waiting { arguments, plan, command, report, receipt }
            }
            State::Complete { .. } => return Err(recursive_reject(io, crate::wire::ErrorCode::Conflict, "recursive tool is already complete")),
        };
        let state = encode(&next, io)?;
        let command = match next {
            State::Waiting { command, .. } | State::SourcePinned { command, .. } => command,
            _ => return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "recursive waiting state missing")),
        };
        let commands = recursive_command_outbox(command, io)?;
        Ok(MachineTransition { commands, state, status: MachineStatus::Suspended })
    }
}

fn completed(value: Value, io: Option<&super::PreparationIo>) -> Result<MachineTransition> {
    Ok(MachineTransition { state: recursive_complete_state(&value, io)?,
        commands: Vec::new(), status: MachineStatus::Completed { value } })
}
fn recursive_complete_state(value: &Value, io: Option<&super::PreparationIo>) -> Result<Value> {
    #[derive(Serialize)]
    struct Complete<'a> { phase: &'static str, value: &'a Value }
    encode(&Complete { phase: "complete", value }, io)
}
fn check_command(command: &WorkflowCommand, stage: &str, plan: &Plan, io: Option<&super::PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work| command.validate(work))?;
    let effect = match plan {
        Plan::Fork(plan) => recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |request| Ok(request.operation_id))?,
        Plan::Merge(plan) => plan.operation_id,
    };
    if command.kind != command_kind(stage) || command.operation_id != derived(effect, stage, io)? {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "recursive command identity or kind changed"));
    }
    Ok(())
}
fn validate_source_intent(intent: &RecursiveForkSourceIntent, arguments: &Value, io: Option<&super::PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work| intent.request.validate(work))?;
    for selection in &intent.request.selections {
        recursive_inspect(io, 1)?;
        if matches!(&selection.revision, crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. }
            if !paths.is_empty() || volume.owner() == &crate::conversation::VolumeOwner::Agent(intent.arguments.id)) {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "default fork must create a new agent with empty inherited scratch"));
        }
    }
    if !recursive_encoded_equal(&intent.arguments, arguments, io)? || intent.request.child_agent != intent.arguments.id
        || intent.source_task != intent.arguments.source.unwrap_or(intent.caller)
        || intent.child_operation_id == intent.request.operation_id
        || task_id_for_admission(intent.child_operation_id) == intent.caller
    {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "source intent changed original arguments, caller or independent allocation"));
    }
    Ok(())
}
fn check_source_command(command: &WorkflowCommand, intent: &RecursiveForkSourceIntent, io: Option<&super::PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work| command.validate(work))?;
    if command.kind != FORK_SOURCE_STAGE
        || command.operation_id != derived(intent.request.operation_id, "source-stage", io)?
    {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "source staging command changed its original effect identity"));
    }
    Ok(())
}
fn validate_plan(plan: &Plan, arguments: &Value, io: Option<&super::PreparationIo>) -> Result<()> {
    match plan {
        Plan::Fork(plan) => {
            crate::executor::with_history_io(io, |work| {
                plan.request.validate(work)?;
                plan.preparation.validate(work)?;
                plan.source_checkpoint.validate(work)
            })?;
            if plan.request.source_checkpoint.as_ref() != Some(&plan.source_checkpoint) {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork plan changed its original source checkpoint"));
            }
            recursive_decoded::<ForkRequest, _>(&plan.request.payload, io, |fork| {
                crate::executor::with_history_io(io, |work| fork.validate(work))?;
                if !recursive_encoded_equal(&plan.arguments, arguments, io)? || fork.child_agent != plan.arguments.id
                    || plan.preparation.parent != Some(plan.caller) || plan.request.child.parent != Some(plan.caller)
                    || plan.preparation.operation_id == plan.request.child.operation_id
                    || plan.preparation.operation_id == fork.operation_id || plan.request.child.operation_id == fork.operation_id
                    || plan.preparation.task_id() == plan.caller || plan.request.child.task_id() == plan.caller
                    || !recursive_encoded_equal(&plan.request, &plan.preparation.input, io)?
                    || plan.preparation.machine.name != "acyclic.fork_preparation" || plan.preparation.machine.version != "1"
                    || plan.request.command.as_ref().is_none_or(|command| command.kind != FORK_PREPARE_TASK_COMMAND_KIND || command.operation_id != fork.operation_id)
                {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "fork plan changed original arguments, parent or independent identities"));
                }
                Ok(())
            })?;
            if plan.child_message_pointer.is_empty() || !plan.child_message_pointer.starts_with('/') {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "fork producer message pointer is invalid"));
            }
            if plan.child_message_file_pointer.as_ref().is_some_and(|pointer| pointer.is_empty() || !pointer.starts_with('/')) {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "fork immutable input message pointer is invalid"));
            }
        }
        Plan::Merge(plan) => {
            if !recursive_encoded_equal(&plan.arguments, arguments, io)? || plan.source_project == plan.target_project {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "merge plan changed original arguments or source/destination"));
            }
            crate::executor::with_history_io(io, |work| {
                plan.source_project.validate(work)?;
                plan.target_project.validate(work)?;
                plan.source_generation.validate(work)?;
                plan.expected_target_generation.validate(work)?;
                plan.notice.validate(work)
            })?;
            if let Some(source) = &plan.authorized_source {
                crate::executor::with_history_io(io, |work| source.validate(work))?;
                if source.operation_id != plan.operation_id || source.caller_task != plan.caller
                    || source.source_task != plan.arguments.source
                    || source.project != plan.source_project || source.generation != plan.source_generation
                {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "merge plan source policy changed the original caller, operation or immutable selection"));
                }
            }
        }
    }
    Ok(())
}
fn validate_merge_receipt(plan: &RecursiveMergeToolPlan, receipt: &ProjectMergeReceipt, io: Option<&super::PreparationIo>) -> Result<()> {
    crate::executor::with_history_io(io, |work| receipt.validate_shape(work))?;
    if receipt.operation_id != plan.operation_id || receipt.child != plan.source || receipt.source_project != plan.source_project
        || receipt.source_generation != plan.source_generation || receipt.target_project != plan.target_project
        || receipt.expected_target_generation != plan.expected_target_generation || receipt.notice != plan.notice {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized, "merge receipt changed the inspected source or CAS target"));
    }
    Ok(())
}

pub(super) fn recursive_decode_slot_bytes<T>() -> u64 {
    let typed = [
        std::mem::size_of::<State>(), std::mem::size_of::<Plan>(),
        std::mem::size_of::<RecursiveForkToolPlan>(), std::mem::size_of::<RecursiveMergeToolPlan>(),
        std::mem::size_of::<crate::resource_preparation::ResourcePreparationRequest>(),
        std::mem::size_of::<TaskAdmissionRecord>(), std::mem::size_of::<ForkRequest>(),
        std::mem::size_of::<ForkReport>(), std::mem::size_of::<ForkSeed>(),
        std::mem::size_of::<crate::fork::ForkSelection>(), std::mem::size_of::<crate::fork::CapturedResource>(),
        std::mem::size_of::<crate::fork::Capture>(), std::mem::size_of::<crate::fork::ResourceRevision>(),
        std::mem::size_of::<crate::fork::SummaryForkCapture>(), std::mem::size_of::<ProjectMergeReceipt>(),
        std::mem::size_of::<crate::merge::ProviderJoinProof>(), std::mem::size_of::<WorkflowCommand>(),
        std::mem::size_of::<crate::source_authorization::ProjectSourceAuthorization>(),
    ].into_iter().max().unwrap_or(0) as u64;
    let content = std::mem::size_of::<serde::__private229::de::Content<'static>>().max(
        std::mem::size_of::<(serde::__private229::de::Content<'static>, serde::__private229::de::Content<'static>)>(),
    ) as u64;
    super::preparation_slot_bytes::<T>() + typed + 2 * 2 * content
}
macro_rules! recursive_preparation_records {
    ($($record:ty => $schema:ident),+ $(,)?) => {$(
        impl super::PreparationRecord for $record {
            fn maximum_slot_bytes() -> u64 { recursive_decode_slot_bytes::<Self>() }
            fn json_value_schema() -> crate::contract::JsonValueSchema {
                crate::contract::JsonValueSchema::$schema
            }
        }
    )+};
}
recursive_preparation_records!(
    ForkToolArguments => None, MergeToolArguments => None,
    RecursiveForkSourceIntent => RecursiveForkSourceIntent, RecursiveForkToolPlan => RecursiveForkToolPlan,
    RecursiveMergeToolPlan => RecursiveMergeToolPlan,
    Plan => RecursivePlan, State => RecursiveState, InitialPlan => RecursiveInitialPlan,
    SourcePin => RecursiveSourcePin, Receipt => RecursiveReceipt,
    ForkSeed => ForkSeed,
    TaskId => None, ProjectMergeReceipt => ProjectMergeReceipt,
);

pub(super) use super::interaction_host::interaction_boundary as recursive_reject;
fn recursive_inspect(io: Option<&super::PreparationIo>, items: u64) -> Result<()> {
    if let Some(io) = io { io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items }, false)?; }
    Ok(())
}
fn recursive_byte_bound(value: &Value, maximum: u64, io: Option<&super::PreparationIo>) -> Result<()> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io { return io.check_json_byte_bound(value, maximum); }
    let _ = io;
    crate::contract::validate_json_byte_bound(value, maximum)
}
pub(super) fn decode<T: super::PreparationRecord>(value: &Value, io: Option<&super::PreparationIo>) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return Ok(crate::filesystem::retain_decoded(io.value_decode_preparation::<T>(value, crate::wire::ErrorCode::Invalid)?));
    }
    let _ = io;
    serde_json::from_value(value.clone()).map_err(|error| Error::Invalid(error.to_string()))
}
fn recursive_decoded<T: super::PreparationRecord, R>(
    value: &Value, io: Option<&super::PreparationIo>, operation: impl FnOnce(&T) -> Result<R>,
) -> Result<R> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        let decoded = io.value_decode_preparation::<T>(value, crate::wire::ErrorCode::Invalid)?;
        return operation(&decoded.0);
    }
    let _ = io;
    operation(&decode::<T>(value, None)?)
}
fn recursive_temporary_io(io: Option<&super::PreparationIo>) -> Result<Option<super::PreparationIo>> {
    #[cfg(not(target_arch = "wasm32"))]
    return io.and_then(super::PreparationIo::original_native_work)
        .map(super::PreparationIo::from_native_work).transpose();
    #[cfg(target_arch = "wasm32")]
    { let _ = io; Ok(None) }
}
fn recursive_release_since(io: Option<&super::PreparationIo>, before: Option<u64>) -> Result<()> {
    if let (Some(io), Some(before)) = (io, before) {
        io.release(io.owned_bytes()?.checked_sub(before).ok_or_else(|| recursive_reject(
            Some(io), crate::wire::ErrorCode::Invalid, "recursive temporary custody underflow"))?)?;
    }
    Ok(())
}
pub(super) fn encode<T: Serialize>(value: &T, io: Option<&super::PreparationIo>) -> Result<Value> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io {
        return io.value_json(value, io.remaining()?.bytes_encoded);
    }
    let _ = io;
    serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))
}
fn recursive_encoded_equal<T: Serialize>(expected: &T, actual: &Value, io: Option<&super::PreparationIo>) -> Result<bool> {
    let before = io.map(|io| io.owned_bytes()).transpose()?;
    let encoded = encode(expected, io)?;
    let equal = encoded == *actual;
    drop(encoded);
    recursive_release_since(io, before)?;
    Ok(equal)
}
pub(super) fn recursive_box<T>(value: T, io: Option<&super::PreparationIo>) -> Result<Box<T>> {
    if let Some(io) = io {
        let bytes = std::mem::size_of::<T>() as u64;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    Ok(Box::new(value))
}
pub(super) fn recursive_command_outbox(command: WorkflowCommand, io: Option<&super::PreparationIo>) -> Result<Vec<WorkflowCommand>> {
    let mut commands = Vec::new();
    crate::executor::stock_vec_push(&mut commands, command, io)?;
    Ok(commands)
}
fn derived(effect: OperationId, label: &str, io: Option<&super::PreparationIo>) -> Result<OperationId> {
    if let Some(io) = io {
        let work = acyclic_fs::WorkCounters {
            bytes_hashed: (b"harness/v2/recursive-tool\0".len() + 16 + label.len()) as u64,
            ..Default::default()
        };
        io.preflight(work)?;
        io.charge(work)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/recursive-tool\0"); hash.update(&effect.into_bytes()); hash.update(label.as_bytes());
    let mut bytes = [0; 16]; bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}

fn transition_identity(
    effect: OperationId, revision: u64, io: Option<&super::PreparationIo>,
) -> Result<(OperationId, IdempotencyKey)> {
    let operation = {
        let _label;
        let label = crate::contract::preparation_format(format_args!("transition-{revision}"), io)?;
        _label = io.map(|io| io.guard_owned(label.capacity() as u64)).transpose()?;
        derived(effect, &label, io)?
    };
    let key = crate::contract::preparation_format(format_args!("recursive-tool:{effect}:{revision}"), io)?;
    if let Some(io) = io { io.retain_owned(std::mem::size_of::<IdempotencyKey>() as u64)?; }
    Ok((operation, IdempotencyKey(key)))
}
fn recursive_conflict_keys<S: serde::Serializer>(conflicts: &[acyclic_fs::MergeConflict], serializer: S)
    -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq as _;
    struct Key<'a>(&'a acyclic_fs::MergeConflict);
    impl Serialize for Key<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
            use acyclic_fs::kernel::NameEncoding;
            use serde::ser::SerializeSeq as _;
            match self.0 {
                acyclic_fs::MergeConflict::File(id) => {
                    let mut sequence = serializer.serialize_seq(Some(17))?;
                    sequence.serialize_element(&1_u8)?;
                    for byte in id.into_bytes() { sequence.serialize_element(&byte)?; }
                    sequence.end()
                }
                acyclic_fs::MergeConflict::Binding { directory_id, name } => {
                    let mut sequence = serializer.serialize_seq(Some(18 + name.as_bytes().len()))?;
                    sequence.serialize_element(&2_u8)?;
                    for byte in directory_id.into_bytes() { sequence.serialize_element(&byte)?; }
                    sequence.serialize_element(&match name.encoding() {
                        NameEncoding::Utf8 => 1_u8, NameEncoding::PosixBytes => 2,
                        NameEncoding::WindowsUtf16Le => 3,
                    })?;
                    for byte in name.as_bytes() { sequence.serialize_element(byte)?; }
                    sequence.end()
                }
            }
        }
    }
    let mut sequence = serializer.serialize_seq(Some(conflicts.len()))?;
    for conflict in conflicts { sequence.serialize_element(&Key(conflict))?; }
    sequence.end()
}

#[cfg(not(target_arch = "wasm32"))]
fn recursive_owned_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right).ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason: "recursive payload ownership overflow" })
}
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn recursive_value_owned_bytes(value: &Value) -> Result<u64> {
    recursive_owned_add(std::mem::size_of::<Value>() as u64,
        crate::contract::json_value_logical_owned_bytes(value)?)
}
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn recursive_transition_owned_bytes(transition: &MachineTransition, io: &super::PreparationIo) -> Result<u64> {
    let mut bytes = recursive_owned_add(std::mem::size_of::<MachineTransition>() as u64,
        crate::contract::json_value_logical_owned_bytes(&transition.state)?)?;
    let slots = (transition.commands.capacity() as u64)
        .checked_mul(std::mem::size_of::<WorkflowCommand>() as u64)
        .ok_or_else(|| recursive_reject(Some(io), crate::wire::ErrorCode::Invalid, "recursive outbox ownership overflow"))?;
    bytes = recursive_owned_add(bytes, slots)?;
    for command in &transition.commands {
        recursive_inspect(Some(io), 1)?;
        bytes = recursive_owned_add(bytes, command.kind.capacity() as u64)?;
        bytes = recursive_owned_add(bytes, command.payload.logical_owned_bytes()?)?;
    }
    let status = match &transition.status {
        MachineStatus::Suspended => 0,
        MachineStatus::Completed { value } => crate::contract::json_value_logical_owned_bytes(value)?,
        MachineStatus::Failed { message } => message.capacity() as u64,
    };
    recursive_owned_add(bytes, status)
}

struct RecursiveExecutor<P, A, O> {
    source: Option<Arc<dyn RecursiveDefaultToolSource<P, A, O>>>, durable: ResumableTool,
    maximum_request_bytes: u64,
}

pub(crate) struct InvocationCancellation(pub(crate) CancellationToken);
impl Drop for InvocationCancellation {
    fn drop(&mut self) { self.0.cancel(); }
}
impl<P, A, O> RecursiveExecutor<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn check_authority(&self, context: &ToolContext, invocation: &ToolInvocation,
        io: Option<&super::PreparationIo>) -> Result<()> {
        self.source.as_ref().ok_or_else(|| Error::Unsupported("recursive tool provider is not bound".into()))?;
        crate::executor::with_history_io(io, |work|
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            work.remaining()?;
            if !context.native_metadata_matches(work) {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                    "recursive tool metadata differs from its original native context"));
            }
        }
        let scope = context.effective_scope();
        let (grant, call_grant) = if self.durable.definition.name == "acyclic.fork" {
            ("fork:publish", "tool:call:acyclic.fork")
        } else { ("project:merge", "tool:call:acyclic.merge") };
        if context.operation_id() != invocation.operation_id || context.call_id() != invocation.call_id
            || context.durable_task_id().is_none() || invocation.name != self.durable.definition.name
            || !scope.grants().contains(grant) || !scope.grants().contains(call_grant) {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "recursive tool lacks its exact admitted caller/effect authority"));
        }
        recursive_byte_bound(&invocation.arguments, self.maximum_request_bytes.min(scope.limits().file_bytes), io)
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn run_with_native_work(
        &self, context: &ToolContext, invocation: &ToolInvocation,
        original_work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Option<ToolResult>, acyclic_fs::NativeAllocation)> {
        let mut result_custody = original_work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        let (work, cancellation) = original_work.invocation_view()?;
        let _stop_on_drop = InvocationCancellation(cancellation.clone());
        let mut result = async {
            let work = &work;
            let cancellation = &cancellation;
            let owner_io = super::PreparationIo::from_native_work(work)?;
            let owner = self.source.as_deref().expect("recursive source was authorized").owner(
                context, &self.durable, invocation, cancellation, Some(&owner_io))?.await?;
            let caller = context.durable_task_id().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "recursive native tool has no durable caller",
            })?;
            let admitted = owner.runtime.task_host().observe_admission_with_native_work(caller, work).await?;
            if admitted.0.grants != *context.scope().grants()
                || admitted.0.limits != context.scope().limits()
                || admitted.0.run_limits != context.scope().run_limits()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "recursive native owner differs from original admitted caller" });
            }
            drop(admitted);
            owner.runtime.task_host().verify_dispatch_owner_with_native_work(
                caller, owner.journal_owner.task_binding().1, work)?.await?;
            let mut session = self.durable.open_with_native_context(
                context, invocation, Arc::clone(&owner.workflow), work)?.await?;
            for _ in 0..8 {
                owner.runtime.task_host().verify_dispatch_owner_with_native_work(
                    caller, owner.journal_owner.task_binding().1, work)?.await?;
                let io = super::PreparationIo::from_native_work(work)?;
                let state = crate::filesystem::native_decoded(
                    io.value_decode_preparation::<State>(&session.checkpoint().state, crate::wire::ErrorCode::Invalid)?)?;
                if let State::Complete { value } = state.0 {
                    return Ok((Some(ToolResult { value }), state.1));
                }
                let Some(input) = self.input(context, invocation, &owner, state.0, cancellation, Some(&io)).await?
                    else { return Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?)); };
                let identity = transition_identity(invocation.operation_id, session.checkpoint().revision, Some(&io))?;
                let custody = io.take_native_custody(io.owned_bytes()?)?;
                let transition = session.step_with_native_work(
                    identity.0, identity.1, input, custody, work)?.await?;
                owner.runtime.task_host().verify_dispatch_owner_with_native_work(
                    caller, owner.journal_owner.task_binding().1, work)?.await?;
                match transition.0.status {
                    MachineStatus::Completed { value } => return Ok((Some(ToolResult { value }), transition.1)),
                    MachineStatus::Failed { message } => return Err(Error::NativeDiagnostic {
                        code: crate::wire::ErrorCode::Conflict, message, custody: transition.1,
                    }),
                    MachineStatus::Suspended => {}
                }
            }
            Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?))
        }.await?;
        result_custody.absorb(&mut result.1)?;
        Ok((result.0, result_custody))
    }

    async fn run(&self, context: ToolContext, invocation: ToolInvocation, cancellation: &CancellationToken) -> Result<Option<ToolResult>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        self.authorize_with_context(&context, &invocation)?;
        let owner = self.source.as_deref().expect("recursive source was authorized").owner(&context, &self.durable, &invocation, cancellation, None)?.await?;
        let caller = context.durable_task_id().ok_or_else(|| Error::Unauthorized("recursive tool has no durable caller".into()))?;
        let admitted = owner.runtime.task_host().observe_admission(caller).await?;
        if admitted.grants != *context.scope().grants() || admitted.limits != context.scope().limits()
            || admitted.run_limits != context.scope().run_limits() {
            return Err(Error::Unauthorized("recursive owner differs from the original admitted caller".into()));
        }
        owner.runtime.task_host().verify_dispatch_owner(caller, owner.journal_owner.task_binding().1.clone()).await?;
        let mut session = self.durable.open(context.clone(), invocation.clone(), owner.workflow.clone()).await?;
        for _ in 0..8 {
            cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
            owner.runtime.task_host().verify_dispatch_owner(caller, owner.journal_owner.task_binding().1.clone()).await?;
            let state: State = decode(&session.checkpoint().state, None)?;
            if let State::Complete { value } = state { return Ok(Some(ToolResult { value })); }
            let Some(input) = self.input(&context, &invocation, &owner, state, cancellation, None).await?
                else { return Ok(None); };
            let (operation, key) = transition_identity(invocation.operation_id, session.checkpoint().revision, None)?;
            let transition = session.step(operation, key, input).await?;
            owner.runtime.task_host().verify_dispatch_owner(caller, owner.journal_owner.task_binding().1.clone()).await?;
            match transition.status {
                MachineStatus::Completed { value } => return Ok(Some(ToolResult { value })),
                MachineStatus::Failed { message } => return Err(Error::Conflict(message)),
                MachineStatus::Suspended => {}
            }
        }
        Err(Error::Invalid("recursive tool exceeded its finite command sequence".into()))
    }

    async fn input(
        &self, context: &ToolContext, invocation: &ToolInvocation, owner: &RecursiveToolOwner<P, A, O>,
        state: State, cancellation: &CancellationToken, output_io: Option<&super::PreparationIo>,
    ) -> Result<Option<Value>> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        let caller = context.durable_task_id().expect("input builder has an admitted caller");
        let (arguments, intent) = match state {
            State::Ready { arguments } if self.durable.definition.name == "acyclic.fork" => {
                let original: ForkToolArguments = decode(&arguments, io)?;
                crate::executor::with_history_io(io, |work| original.source_task(caller, context.effective_scope().limits(), work))?;
                let payload = stage(context, derived(invocation.operation_id, "source-arguments", io)?,
                    "source-arguments", &arguments, self.maximum_request_bytes, cancellation, io).await?;
                let intent = self.source.as_deref().expect("recursive source was authorized").select(context, &original, cancellation, io)?.await?;
                validate_source_intent(&intent, &arguments, io)?;
                if intent.caller != caller || intent.request.operation_id != invocation.operation_id {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "source selector is not the actual original caller/effect"));
                }
                return encode(&SourcePin { intent, command: WorkflowCommand {
                    operation_id: derived(invocation.operation_id, "source-stage", io)?,
                    kind: crate::filesystem::fork_preparer_io::clone_text(FORK_SOURCE_STAGE, io)?, payload,
                } }, output_io).map(Some);
            }
            State::Ready { arguments } => (arguments, None),
            State::SourcePinned { arguments, intent, command } => {
                validate_source_intent(&intent, &arguments, io)?;
                check_source_command(&command, &intent, io)?;
                let before_read = io.map(super::PreparationIo::owned_bytes).transpose()?;
                let retained = retained_payload(context, &command.payload, self.maximum_request_bytes, cancellation, io).await?;
                if retained != arguments {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "source-stage outbox changed original tool arguments"));
                }
                drop(retained);
                recursive_release_since(io, before_read)?;
                (arguments, Some(intent))
            }
            State::Waiting { plan, command, report, receipt, .. } => {
                let report = report.as_deref();
                let receipt = receipt.as_deref();
                let stage = command_stage(&command, report, io)?;
                validate_plan(&plan, &invocation.arguments, io)?;
                if io.is_some_and(|io| io.original_native_work().is_some()) { check_command(&command, stage, &plan, io)?; }
                if command.operation_id != derived(invocation.operation_id, stage, io)? {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "recursive outbox operation differs from original effect"));
                }
                let expected = command_payload(stage, &plan, report, receipt, io)?;
                let before_read = io.map(super::PreparationIo::owned_bytes).transpose()?;
                let retained = retained_payload(context, &command.payload, self.maximum_request_bytes, cancellation, io).await?;
                if retained != expected {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "recursive outbox payload changed original retained plan"));
                }
                drop(retained);
                recursive_release_since(io, before_read)?;
                let Some(value) = self.dispatch(context, owner, &plan, stage, &command, expected, report, receipt,
                    cancellation, io).await? else { return Ok(None); };
                let next_stage = next_stage(stage, &value);
                let observed_report = if stage == "observe" { Some(decode::<ForkReport>(&value, io)?) } else { None };
                let observed_receipt = if stage == "merge-apply" && next_stage.is_some() {
                    Some(decode::<ProjectMergeReceipt>(value.get("receipt").ok_or_else(|| recursive_reject(io,
                        crate::wire::ErrorCode::Invalid, "merge receipt missing"))?, io)?)
                } else { None };
                let next_report = observed_report.as_ref().or(report);
                let next_receipt = observed_receipt.as_ref().or(receipt);
                let next = match next_stage { Some(stage) => Some(self.command(context, stage, &plan,
                    next_report, next_receipt, cancellation, io).await?), None => None };
                return encode(&Receipt { operation_id: command.operation_id, value: Outcome::Succeeded(value), next }, output_io).map(Some);
            }
            State::Complete { .. } => unreachable!("completed states return before building input"),
        };
        let stage = if intent.is_some() { "prepare" } else { "merge-apply" };
        let plan = self.plan(context, owner, caller, &arguments, intent.as_ref(), cancellation, io).await?;
        let command = self.command(context, stage, &plan, None, None, cancellation, io).await?;
        encode(&InitialPlan { plan, command }, output_io).map(Some)
    }

    async fn plan(&self, context: &ToolContext, owner: &RecursiveToolOwner<P, A, O>, caller: TaskId,
        arguments: &Value, intent: Option<&RecursiveForkSourceIntent>, cancellation: &CancellationToken,
        io: Option<&super::PreparationIo>) -> Result<Plan> {
        let runtime = owner.runtime.as_ref();
        #[cfg(not(target_arch = "wasm32"))]
        let native_work = io.and_then(super::PreparationIo::original_native_work);
        let plan = if self.durable.definition.name == "acyclic.fork" {
            let original: ForkToolArguments = decode(arguments, io)?;
            crate::executor::with_history_io(io, |work| original.source_task(
                caller, context.effective_scope().limits(), work))?;
            let intent = intent.ok_or_else(|| recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "fork source choice is not durably pinned"))?;
            let binding = self.source.as_deref().expect("recursive source was authorized").fork(context, &original, intent, None, cancellation, io)?.await?;
            if binding.source.request != intent.request || binding.child.operation_id != intent.child_operation_id {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                    "source staging changed the selected immutable source/allocation"));
            }
            validate_fork_binding(context, &original, &binding, cancellation, io).await?;
            let operation = derived(context.operation_id(), "preparation-task", io)?;
            let payload = {
                let hold;
                let before = io.map(super::PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let encoded = encode(&binding.source.request, io)?;
                hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| recursive_reject(Some(io), crate::wire::ErrorCode::Invalid,
                        "recursive payload custody underflow"))?)).transpose()?;
                let file = stage(context, derived(context.operation_id(), "preparation-payload", io)?,
                    "preparation-payload", &encoded, self.maximum_request_bytes, cancellation, io).await?;
                drop(encoded);
                drop(hold);
                file
            };
            let checkpoint = crate::executor::with_history_io(io, |work| binding.source_checkpoint.copy(work))?;
            let request = fork_preparation_request(binding.source.preparer.as_ref(), &binding.source.request,
                task_id_for_admission(operation), binding.child, WorkflowCommand {
                    operation_id: context.operation_id(),
                    kind: crate::filesystem::fork_preparer_io::clone_text(FORK_PREPARE_TASK_COMMAND_KIND, io)?, payload,
                }, binding.limits, Some(checkpoint), binding.input_files, io)?;
            let preparation = 'preparation: {
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(io), Some(work)) = (io, native_work) {
                    let input_io = super::PreparationIo::from_native_work(work)?;
                    let input = encode(&request, Some(&input_io))?;
                    let input = (input, input_io.take_native_custody(input_io.owned_bytes()?)?);
                    let (preparation, custody) = runtime.harness().registered_child_admission_with_native_work(
                        operation, "acyclic.fork_preparation", "1", input, caller, context.scope(), work)?;
                    io.absorb_native_custody(custody)?;
                    break 'preparation preparation;
                }
                break 'preparation runtime.harness().registered_child_admission(
                    operation, "acyclic.fork_preparation", "1", encode(&request, io)?, caller, context.scope())?;
            };
            Plan::Fork(recursive_box(RecursiveForkToolPlan { arguments: original, caller, request, preparation,
                child_message_pointer: binding.child_message_pointer,
                child_message_file_pointer: binding.child_message_file_pointer,
                allocation_digest: intent.allocation_digest, source_checkpoint: binding.source_checkpoint }, io)?)
        } else {
            let original: MergeToolArguments = decode(arguments, io)?;
            let binding = self.source.as_deref().expect("recursive source was authorized").merge(context, &original, None, cancellation, io)?.await?;
            validate_merge_binding(context, original.source, &binding, io)?;
            let controller = ParentProjectController::new(binding.filesystem.as_ref(),
                binding.destination.reducer(), &binding.verifier, &binding.scope, &binding.project, io)?;
            let inspected = match &binding.source_read {
                Some(handle) => controller.prepare_project_import_from_selected_source(
                    &binding.source_project, &binding.source_generation, None, handle,
                    &owner.journal_owner, context.task(), context.operation_id()).await?,
                None => controller.prepare_project_import(&binding.source_project, &binding.source_generation, None).await?,
            };
            let expected_target_generation = 'generation: {
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(io), Some(work)) = (io, native_work) {
                    let (generation, custody) = GenerationRef::new_with_native_work(
                        binding.project.provider(), &inspected.target_head().digest().into_bytes(), None, work)?;
                    io.absorb_native_custody(custody)?;
                    break 'generation generation;
                }
                break 'generation binding.filesystem.generation_ref_id(inspected.target_head())?;
            };
            let destination = match io {
                Some(io) => io.history_io().clone_authority(binding.destination.reducer().authority())?,
                None => binding.destination.reducer().authority().clone(),
            };
            let authorized_source = binding.source_read.as_ref().map(|source|
                crate::executor::with_history_io(io, |work| source.authorization().clone_with_work_and_custody(work))
                    .map(|copied| copied.0)).transpose()?;
            drop(controller);
            Plan::Merge(recursive_box(RecursiveMergeToolPlan { arguments: original, caller,
                operation_id: context.operation_id(), destination, source: binding.source,
                source_project: binding.source_project, source_generation: binding.source_generation,
                target_project: binding.project, expected_target_generation,
                authorized_source, notice: binding.notice }, io)?)
        };
        validate_plan(&plan, arguments, io)?;
        if !io.is_some_and(|io| io.original_native_work().is_some()) {
            crate::contract::validate_json_byte_bound(&plan, self.maximum_request_bytes.min(context.scope().limits().file_bytes))?;
        }
        Ok(plan)
    }

    async fn command(&self, context: &ToolContext, stage_kind: &str, plan: &Plan, report: Option<&ForkReport>,
        receipt: Option<&ProjectMergeReceipt>, cancellation: &CancellationToken,
        io: Option<&super::PreparationIo>) -> Result<WorkflowCommand> {
        let operation_id = derived(context.operation_id(), stage_kind, io)?;
        let payload_hold;
        let before = io.map(super::PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let payload = command_payload(stage_kind, plan, report, receipt, io)?;
        payload_hold = io.map(|io| io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| recursive_reject(Some(io), crate::wire::ErrorCode::Invalid, "recursive payload custody underflow"))?)).transpose()?;
        let file = stage(context, derived(operation_id, "payload", io)?, stage_kind, &payload,
            self.maximum_request_bytes, cancellation, io).await?;
        drop(payload);
        drop(payload_hold);
        Ok(WorkflowCommand { operation_id,
            kind: crate::filesystem::fork_preparer_io::clone_text(command_kind(stage_kind), io)?, payload: file })
    }
    async fn apply_merge(
        &self, context: &ToolContext, owner: &RecursiveToolOwner<P, A, O>,
        merge: &RecursiveMergeToolPlan, cancellation: &CancellationToken,
        output_io: Option<&super::PreparationIo>,
    ) -> Result<Value> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        #[cfg(not(target_arch = "wasm32"))]
        let native_work = io.and_then(super::PreparationIo::original_native_work);
        let binding = self.source.as_deref().expect("recursive source was authorized").merge(context, &merge.arguments, Some(merge), cancellation, io)?.await?;
        validate_merge_binding(context, merge.arguments.source, &binding, io)?;
        check_merge_binding(merge, &binding, io)?;
        let controller = ParentProjectController::new(binding.filesystem.as_ref(), binding.destination.reducer(),
            &binding.verifier, &binding.scope, &binding.project, io)?;
        let inspected = match &binding.source_read {
            Some(handle) => controller.prepare_project_import_from_selected_source(
                &merge.source_project, &merge.source_generation, Some(&merge.expected_target_generation),
                handle, &owner.journal_owner, context.task(), context.operation_id()).await?,
            None => controller.prepare_project_import(
                &merge.source_project, &merge.source_generation, Some(&merge.expected_target_generation)).await?,
        };
        'owner: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = native_work {
                owner.runtime.task_host().verify_dispatch_owner_with_native_work(merge.caller, owner.journal_owner.task_binding().1, work)?.await?;
                break 'owner;
            }
            cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
            owner.runtime.task_host().verify_dispatch_owner(merge.caller, owner.journal_owner.task_binding().1.clone()).await?;
        }
        let outcome = {
            let mut sides = BTreeMap::new();
            for selection in &merge.arguments.selections {
                let conflict = super::project_workspaces::decode_conflict(&selection.conflict, io)?;
                let side = match selection.side {
                    MergeToolSide::Base => ConflictSide::Base, MergeToolSide::Target => ConflictSide::Ours,
                    MergeToolSide::Source => ConflictSide::Theirs,
                };
                if sides.contains_key(&conflict) {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "merge conflict selected twice"));
                }
                if let Some(io) = io {
                    let bytes = std::mem::size_of::<(acyclic_fs::MergeConflict, ConflictSide)>() as u64;
                    io.retain_owned(bytes)?;
                    io.allocation_attempt(bytes)?;
                }
                sides.insert(conflict, side);
            }
            let (outcome, custody) = controller.apply_project_merge(&inspected, context.operation_id(), sides).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(custody) = custody {
                io.expect("native controller IO").absorb_native_custody(custody)?;
            }
            #[cfg(target_arch = "wasm32")]
            let _ = custody;
            outcome
        };
        'owner: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = native_work {
                owner.runtime.task_host().verify_dispatch_owner_with_native_work(merge.caller, owner.journal_owner.task_binding().1, work)?.await?;
                break 'owner;
            }
            owner.runtime.task_host().verify_dispatch_owner(merge.caller, owner.journal_owner.task_binding().1.clone()).await?;
            cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        }
        #[derive(Serialize)]
        struct Applied<'a> { status: &'static str, receipt: &'a ProjectMergeReceipt }
        #[derive(Serialize)]
        struct Generation<'a> { status: &'static str, generation: &'a GenerationRef }
        #[derive(Serialize)]
        struct Conflicted<'a> { status: &'static str,
            #[serde(serialize_with = "recursive_conflict_keys")]
            conflicts: &'a [acyclic_fs::MergeConflict], truncated: bool }
        #[derive(Serialize)]
        struct Status { status: &'static str }
        match &outcome {
            JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => {
                let (receipt, custody) = controller.merge_receipt(&inspected, &outcome,
                    &merge.source, context.operation_id(), &merge.notice)?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(custody) = custody {
                    io.expect("native controller IO").absorb_native_custody(custody)?;
                }
                #[cfg(target_arch = "wasm32")]
                let _ = custody;
                encode(&Applied { status: "applied", receipt: &receipt }, output_io)
            }
            JoinOutcome::NoChanges(generation) | JoinOutcome::StaleTarget(generation) => {
                let reference = 'generation: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let (Some(io), Some(work)) = (io, native_work) {
                        let (reference, custody) = GenerationRef::new_with_native_work(
                            binding.project.provider(), &generation.id().digest().into_bytes(), None, work)?;
                        io.absorb_native_custody(custody)?;
                        break 'generation reference;
                    }
                    break 'generation binding.filesystem.generation_ref_id(generation.id())?;
                };
                let status = if matches!(&outcome, JoinOutcome::NoChanges(_)) { "no_changes" } else { "stale_target" };
                encode(&Generation { status, generation: &reference }, output_io)
            }
            JoinOutcome::Conflicted { conflicts, truncated } => encode(
                &Conflicted { status: "conflicted", conflicts, truncated: *truncated }, output_io),
            JoinOutcome::Fenced => encode(&Status { status: "fenced" }, output_io),
            JoinOutcome::IdempotencyConflict => encode(&Status { status: "idempotency_conflict" }, output_io),
        }
    }

    async fn publish_fork(
        &self, context: &ToolContext, owner: &RecursiveToolOwner<P, A, O>,
        fork: &RecursiveForkToolPlan, report: Option<&ForkReport>,
        cancellation: &CancellationToken, output_io: Option<&super::PreparationIo>,
    ) -> Result<Value> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        let intent = RecursiveForkSourceIntent {
            arguments: ForkToolArguments { id: fork.arguments.id,
                msg: crate::filesystem::fork_preparer_io::clone_text(&fork.arguments.msg, io)?, source: fork.arguments.source },
            caller: fork.caller, source_task: fork.arguments.source.unwrap_or(fork.caller),
            child_operation_id: fork.request.child.operation_id,
            allocation_digest: fork.allocation_digest, request: decode(&fork.request.payload, io)?,
        };
        let mut binding = self.source.as_deref().expect("recursive source was authorized").fork(context, &fork.arguments, &intent, Some(fork), cancellation, io)?.await?;
        validate_fork_binding(context, &fork.arguments, &binding, cancellation, io).await?;
        if !recursive_encoded_equal(&binding.source.request, &fork.request.payload, io)?
            || binding.child_message_pointer != fork.child_message_pointer
            || binding.child_message_file_pointer != fork.child_message_file_pointer
            || binding.source_checkpoint != fork.source_checkpoint
        {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "restored fork source differs from original authenticated cut"));
        }
        let command = fork.request.command.as_ref().ok_or_else(|| recursive_reject(io,
            crate::wire::ErrorCode::Invalid, "original preparation command missing"))?;
        let command = WorkflowCommand { operation_id: command.operation_id,
            kind: crate::filesystem::fork_preparer_io::clone_text(&command.kind, io)?,
            payload: crate::executor::with_history_io(io, |work| command.payload.copy(work))? };
        let restored = fork_preparation_request(
            binding.source.preparer.as_ref(), &binding.source.request,
            fork.preparation.task_id(), binding.child, command, binding.limits,
            Some(binding.source_checkpoint), binding.input_files, io,
        )?;
        if restored != fork.request {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "restored child, source or allowances changed"));
        }
        let report = report.ok_or_else(|| recursive_reject(io, crate::wire::ErrorCode::Invalid,
            "fork publication has no canonical report"))?;
        let expected_seed = crate::executor::with_history_io(io, |work| report.copy(work)?.0.into_seed(work))?;
        let report_copy = crate::executor::with_history_io(io, |work| report.copy(work).map(|(report, _)| report))?;
        let remaining = 'publication: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                binding.parent.publish_fork_report(report_copy, binding.parent_scope,
                    Some(owner.journal_owner.as_ref()), &mut Some(&mut io.history_io())).await?;
                break 'publication None;
            }
            let budget = acyclic_fs::performance::work_budget(fork.request.maximum_work, fork.request.maximum_bytes);
            let mut publication_work = crate::store::HistoryWork::new(budget, cancellation);
            let published = async {
                publication_work.preflight()?;
                binding.parent.publish_fork_report(report_copy, binding.parent_scope,
                    Some(owner.journal_owner.as_ref()), &mut Some(&mut publication_work)).await
            }.await;
            let published = publication_work.finish(published).map_err(|failure| failure.error)?;
            break 'publication Some(published.work.remaining(budget).map_err(|error| Error::Invalid(error.to_string()))?);
        };
        let seed = binding.parent.reducer().fork(&report.request.child).ok_or_else(|| recursive_reject(io,
            crate::wire::ErrorCode::Storage, "acknowledged fork publication has no canonical seed"))?;
        if seed != &expected_seed {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "acknowledged fork seed changed original prepared report"));
        }
        'child: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                binding.child_conversation.bind_published_child(&binding.parent, seed, binding.child_scope,
                    Some(owner.journal_owner.as_ref()), &mut Some(&mut io.history_io())).await?;
                break 'child;
            }
            let mut child_work = crate::store::HistoryWork::new(
                remaining.expect("measured publication preserves its remaining allowance"), cancellation);
            let bound = async {
                child_work.preflight()?;
                binding.child_conversation.bind_published_child(&binding.parent, seed, binding.child_scope,
                    Some(owner.journal_owner.as_ref()), &mut Some(&mut child_work)).await
            }.await;
            child_work.finish(bound).map_err(|failure| failure.error)?;
        }
        encode(seed, output_io)
    }

    async fn publish_merge(
        &self, context: &ToolContext, owner: &RecursiveToolOwner<P, A, O>,
        merge: &RecursiveMergeToolPlan, receipt: Option<&ProjectMergeReceipt>,
        cancellation: &CancellationToken, output_io: Option<&super::PreparationIo>,
    ) -> Result<Value> {
        let temporary_io = recursive_temporary_io(output_io)?;
        let io = temporary_io.as_ref();
        let mut binding = self.source.as_deref().expect("recursive source was authorized").merge(context, &merge.arguments, Some(merge), cancellation, io)?.await?;
        validate_merge_binding(context, merge.arguments.source, &binding, io)?;
        check_merge_binding(merge, &binding, io)?;
        let receipt = receipt.ok_or_else(|| recursive_reject(io, crate::wire::ErrorCode::Invalid,
            "merge publication has no provider receipt"))?;
        validate_merge_receipt(merge, receipt, io)?;
        if receipt.operation_id != context.operation_id() {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "merge receipt belongs to another original effect"));
        }
        let receipt_copy = crate::executor::with_history_io(io, |work| receipt.clone_with_work_and_custody(work))?.0;
        let receipt_copy = recursive_box(receipt_copy, io)?;
        let key = crate::contract::preparation_format(
            format_args!("recursive-merge-publication:{}", context.operation_id()), io)?;
        let command = Command {
            operation_id: context.operation_id(), idempotency_key: IdempotencyKey(key),
            expected_revision: binding.destination.reducer().revision(),
            scope: binding.scope, causal_parent: None,
            action: Action::PublishProjectMerge { receipt: receipt_copy },
        };
        let mut history = io.map(super::PreparationIo::history_io);
        let mut observer = history.as_mut().map(|history| history as &mut dyn crate::store::history::HistoryIo);
        binding.destination.publish(command, Some(owner.journal_owner.as_ref()),
            crate::distributed::JournalWrite::Fresh, false, &mut observer).await?;
        encode(receipt, output_io)
    }

    async fn dispatch(&self, context: &ToolContext, owner: &RecursiveToolOwner<P, A, O>, plan: &Plan,
        stage: &str, command: &WorkflowCommand, payload: Value,
        report: Option<&ForkReport>, receipt: Option<&ProjectMergeReceipt>, cancellation: &CancellationToken,
        io: Option<&super::PreparationIo>) -> Result<Option<Value>> {
        #[cfg(not(target_arch = "wasm32"))]
        let native_work = io.and_then(super::PreparationIo::original_native_work);
        let outcome = match (plan, stage) {
            (Plan::Fork(_), "prepare" | "child-admit") => {
                let progress = 'progress: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = native_work {
                        let context = context.native_task().ok_or_else(|| recursive_reject(io,
                            crate::wire::ErrorCode::Unauthorized, "recursive command has no original native task context"))?;
                        let temporary_io = super::PreparationIo::from_native_work(work)?;
                        let history = &mut temporary_io.history_io();
                        let original_fence = owner.journal_owner.task_binding().1;
                        let fence = crate::scheduler::LeaseFence {
                            reservation_id: history.clone_text(&original_fence.reservation_id)?,
                            placement: history.clone_text(&original_fence.placement)?,
                        };
                        let command = WorkflowCommand { operation_id: command.operation_id,
                            kind: history.clone_text(&command.kind)?,
                            payload: command.payload.copy(&mut Some(&mut *history))? };
                        let payload = crate::core::native_value_clone_with_custody(&payload, &mut Some(&mut *history))?.0;
                        break 'progress owner.runtime.commands().execute_with_native_work(
                            Arc::clone(context), fence, command, payload, work)?.await?;
                    }
                    break 'progress owner.runtime.commands().execute(
                        context.task().clone(), owner.journal_owner.task_binding().1.clone(), command.clone(), payload).await?;
                };
                match progress {
                    TaskCommandProgress::Ready(value) => {
                        if io.is_some_and(|io| io.original_native_work().is_some()) {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                                "native recursive command reply lacks its original payload custody"));
                        }
                        Some(decode::<Outcome<Value>>(&value, io)?)
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    TaskCommandProgress::ReadyWithNativeCustody { value, custody } => {
                        let retained = (value, custody);
                        Some(decode::<Outcome<Value>>(&retained.0, io)?)
                    }
                    TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => None,
                }
            }
            (Plan::Fork(fork), "observe") => {
                let same_admission = 'admission: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(work) = native_work {
                        let retained = owner.runtime.task_host().observe_admission_with_native_work(
                            fork.preparation.task_id(), work).await?;
                        break 'admission retained.0 == fork.preparation;
                    }
                    break 'admission owner.runtime.task_host().observe_admission(fork.preparation.task_id()).await? == fork.preparation;
                };
                if !same_admission {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "observed preparation admission changed"));
                }
                let outcome = 'outcome: {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let (Some(io), Some(work)) = (io, native_work) {
                        let (outcome, custody) = owner.runtime.task_host().outcome_with_native_work(
                            fork.preparation.task_id(), work).await?;
                        io.absorb_native_custody(custody)?;
                        break 'outcome outcome;
                    }
                    break 'outcome owner.runtime.task_host().outcome(fork.preparation.task_id()).await?;
                };
                if let Some(Outcome::Succeeded(value)) = &outcome {
                    recursive_decoded::<ForkReport, _>(value, io, |report| {
                        crate::executor::with_history_io(io, |work| report.validate(work))?;
                        if !recursive_encoded_equal(&report.request, &fork.request.payload, io)? {
                            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                                "preparation output changed original retained request"));
                        }
                        Ok(())
                    })?;
                }
                outcome
            }
            (Plan::Fork(fork), "fork-publish") => return self.publish_fork(
                context, owner, fork, report, cancellation, io).await.map(Some),
            (Plan::Merge(merge), "merge-apply") => return self.apply_merge(
                context, owner, merge, cancellation, io).await.map(Some),
            (Plan::Merge(merge), "merge-publish") => return self.publish_merge(
                context, owner, merge, receipt, cancellation, io).await.map(Some),
            _ => return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid,
                "recursive command does not match its retained plan")),
        };
        match outcome {
            Some(Outcome::Succeeded(value)) => Ok(Some(value)),
            Some(Outcome::Failed { message }) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
                    let custody = io.take_native_custody(io.owned_bytes()?)?;
                    return Err(Error::NativeDiagnostic { code: crate::wire::ErrorCode::Conflict, message, custody });
                }
                Err(Error::Conflict(message))
            }
            Some(Outcome::Cancelled) => Err(recursive_reject(io, crate::wire::ErrorCode::Conflict,
                if stage == "observe" { "preparation cancelled; child not admitted and allocation may remain uncertain" }
                else { "recursive admission cancelled" })),
            None | Some(Outcome::Indeterminate { .. }) => Ok(None),
        }
    }
}

impl<P, A, O> ToolExecutor for RecursiveExecutor<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn resumable_machine_identity(&self) -> Option<MachineIdentity> {
        Some(self.durable.machine.identity().clone())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn resumable_machine_identity_with_native_work<'a>(
        &'a self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&'a MachineIdentity>> {
        self.durable.machine.identity_with_native_work(work).map(Some)
    }
    fn authorize(&self, _: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        Err(Error::Unauthorized("recursive tools require the original durable ToolContext".into()))
    }
    fn authorize_with_context(&self, context: &ToolContext, invocation: &ToolInvocation) -> Result<()> {
        self.check_authority(context, invocation, None)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        work.remaining()?;
        if !self.source.as_ref().is_some_and(|source| source.supports_native_source())
            || !self.durable.machine.supports_native_work() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "recursive producer has no original native execution callbacks" });
        }
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn authorize_with_native_context(
        &self, context: &ToolContext, invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.check_authority(context, invocation, Some(&super::PreparationIo::from_native_work(work)?))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn execute_with_native_context<'a>(
        &'a self, context: &'a ToolContext, invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let result = self.run_with_native_work(context, invocation, work).await?;
            match result.0 {
                Some(value) => Ok((value, result.1)),
                None => Err(Error::Indeterminate(invocation.operation_id)),
            }
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn reconcile_with_native_context<'a>(
        &'a self, context: &'a ToolContext, invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>> {
        self.supports_native_execution(work)?;
        self.authorize_with_native_context(context, invocation, work)?;
        crate::NativeFutureBundle::new(work, || self.run_with_native_work(context, invocation, work))
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Unauthorized("recursive tools require original durable task context".into())) })
    }
    fn execute_with_context<'a>(&'a self, context: ToolContext, invocation: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let operation = invocation.operation_id;
        let cancellation = CancellationToken::new();
        let guard = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _guard = guard;
            self.run(context, invocation, &cancellation).await?.ok_or(Error::Indeterminate(operation))
        })
    }
    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Err(Error::Unauthorized("recursive reconciliation requires original durable task context".into())) })
    }
    fn reconcile_with_context<'a>(&'a self, context: ToolContext, invocation: ToolInvocation) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        let cancellation = CancellationToken::new();
        let guard = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _guard = guard;
            self.run(context, invocation, &cancellation).await
        })
    }
}
fn command_payload(stage: &str, plan: &Plan, report: Option<&ForkReport>,
    receipt: Option<&ProjectMergeReceipt>, io: Option<&super::PreparationIo>) -> Result<Value> {
    #[derive(Serialize)]
    struct Admission<'a> { admission: &'a TaskAdmissionRecord }
    #[derive(Serialize)]
    struct Observation { preparation: TaskId }
    match (plan, stage) {
        (Plan::Fork(plan), "prepare") => encode(&Admission { admission: &plan.preparation }, io),
        (Plan::Fork(plan), "observe") => encode(&Observation { preparation: plan.preparation.task_id() }, io),
        (Plan::Fork(_), "fork-publish") => encode(report.ok_or_else(|| recursive_reject(
            io, crate::wire::ErrorCode::Invalid, "publication report missing"))?, io),
        (Plan::Fork(plan), "child-admit") => encode(&Admission { admission: &plan.request.child }, io),
        (Plan::Merge(plan), "merge-apply") => encode(plan, io),
        (Plan::Merge(_), "merge-publish") => encode(receipt.ok_or_else(|| recursive_reject(
            io, crate::wire::ErrorCode::Invalid, "publication receipt missing"))?, io),
        _ => Err(recursive_reject(io, crate::wire::ErrorCode::Invalid, "command payload does not match retained plan")),
    }
}

async fn retained_payload(
    context: &ToolContext, file: &FileRef, maximum: u64,
    cancellation: &CancellationToken, io: Option<&super::PreparationIo>,
) -> Result<Value> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let work = io.original_native_work().expect("native payload reads preserve their original ledger");
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
        if file.descriptor().byte_length() > maximum.min(context.effective_scope().limits().file_bytes) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "retained recursive payload exceeds its original bound" });
        }
        let bytes = context.read_file_with_native_work(file, work).await?;
        return Ok(crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<Value>, &bytes, crate::wire::ErrorCode::Storage)?));
    }
    let _ = io;
    let bytes = context.task().read_file_measured(file, acyclic_fs::performance::work_budget(maximum, maximum), cancellation).await
        .map(|receipt| receipt.value).map_err(|failure| failure.error)?;
    crate::contract::json_from_slice(&bytes).map_err(|error| Error::Storage(error.to_string()))
}
async fn stage(context: &ToolContext, operation: OperationId, label: &str, value: &Value, maximum: u64,
    cancellation: &CancellationToken, io: Option<&super::PreparationIo>) -> Result<FileRef> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        let work = io.original_native_work().expect("native staging preserves its original ledger");
        let bytes = io.encode(value, maximum.min(context.effective_scope().limits().file_bytes), true)?;
        let _path;
        let path = crate::contract::preparation_format(
            format_args!("recursive-tools/{}/{label}.json", context.operation_id()), Some(io))?;
        _path = io.guard_owned(path.capacity() as u64)?;
        let _display;
        let display = crate::contract::preparation_format(format_args!("{label}.json"), Some(io))?;
        _display = io.guard_owned(display.capacity() as u64)?;
        let (file, custody) = context.stage_file_once_with_native_work(
            operation, &path, &bytes, "application/json", &display, work).await?;
        io.absorb_native_custody(custody)?;
        return Ok(file);
    }
    let _ = io;
    let io = super::fork_preparer::PreparationIo::from_budget(acyclic_fs::performance::work_budget(maximum, maximum), cancellation)?;
    let bytes = io.encode(value, maximum.min(context.scope().limits().file_bytes), true)?;
    io.receive_content(context.stage_file_once_measured(operation,
        &format!("recursive-tools/{}/{label}.json", context.operation_id()), &bytes,
        "application/json", &format!("{label}.json"), io.remaining()?, cancellation).await)
}
async fn validate_fork_binding<P, A, O>(context: &ToolContext, arguments: &ForkToolArguments,
    binding: &RecursiveForkToolBinding<P, A, O>, cancellation: &CancellationToken,
    io: Option<&super::PreparationIo>) -> Result<()>
where P: StreamProvider, A: AsyncAuthorityStore, O: AsyncObjectStore {
    let effective = context.effective_scope();
    crate::executor::with_history_io(io, |work| {
        binding.source.request.validate(work)?;
        binding.child.validate(work)?;
        effective.limits().validate_file(&binding.source_checkpoint, work)
    })?;
    if !crate::executor::with_history_io(io, |work| crate::runtime::read_granted_with_work(effective.grants(), &binding.source_checkpoint, work))? {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "caller cannot read the original fork checkpoint"));
    }
    if binding.source.request.operation_id != context.operation_id() || binding.source.request.child_agent != arguments.id
        || binding.child.parent != context.durable_task_id()
        || binding.parent.reducer().authority() != &binding.source.request.parent
        || binding.child_conversation.reducer().authority() != &binding.source.request.child
        || binding.child_scope.agent() != Some(arguments.id)
        || binding.parent_scope.agent() != binding.parent.reducer().conversation().and_then(|conversation| conversation.agent) {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "fork binding is not the actual original caller and new child"));
    }
    for selection in &binding.source.request.selections {
        recursive_inspect(io, 1)?;
        if let crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. } = &selection.revision {
            let caller_agent = binding.parent_scope.agent().ok_or_else(|| recursive_reject(
                io, crate::wire::ErrorCode::Unauthorized, "caller has no bound agent"))?;
            if !paths.is_empty() || volume.owner() != &crate::conversation::VolumeOwner::Agent(caller_agent)
                || binding.parent_scope.agent() == Some(arguments.id)
            {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                    "fork cannot substitute caller history/private identity"));
            }
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                if recursive_volume_granted(&binding.child.grants, volume, operation, io)?
                    || recursive_volume_granted(binding.child_scope.capabilities(), volume, operation, io)?
                {
                    return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                        "default child cannot inherit broad parent-private authority"));
                }
            }
        }
    }
    for file in &binding.input_files {
        crate::executor::with_history_io(io, |work| effective.limits().validate_file(file, work))?;
        if !crate::executor::with_history_io(io, |work| crate::runtime::read_granted_with_work(effective.grants(), file, work))?
            || !recursive_exact_file_granted(&binding.child.grants, file, io)?
            || !recursive_exact_file_granted(binding.child_scope.capabilities(), file, io)?
        {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "child input is not an actual donated exact-file read"));
        }
    }
    let selected = binding.child.input.pointer(&binding.child_message_pointer)
        .ok_or_else(|| recursive_reject(io, crate::wire::ErrorCode::Invalid,
            "child producer input does not retain fork msg"))?;
    if selected.as_str() == Some(arguments.msg.as_str()) {
        if binding.child_message_file_pointer.is_some() {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid,
                "inline child message has an immutable-file projection"));
        }
        return Ok(());
    }
    let file: FileRef = decode(selected, io)?;
    let command: WorkflowCommand = decode(&binding.child.input, io)?;
    crate::executor::with_history_io(io, |work| command.validate(work))?;
    if command.payload != file || !binding.input_files.contains(&file) {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "child msg reference is not its exact admitted immutable command input"));
    }
    if !crate::executor::with_history_io(io, |work| crate::runtime::read_granted_with_work(&binding.child.grants, &file, work))? {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "child cannot read its exact initial message"));
    }
    let bytes = 'read: {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(super::PreparationIo::original_native_work) {
            break 'read context.read_file_with_native_work(&file, work).await?;
        }
        break 'read context.read_file_measured(&file,
            acyclic_fs::performance::work_budget(binding.limits.work, binding.limits.bytes), cancellation)
            .await.map_err(|failure| failure.error)?.value;
    };
    if let Some(pointer) = &binding.child_message_file_pointer {
        if pointer.is_empty() || !pointer.starts_with('/') {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid,
                "child immutable message pointer is invalid"));
        }
        let input: Value = recursive_decode_bytes(&bytes, io)?;
        if input.pointer(pointer).and_then(Value::as_str) != Some(arguments.msg.as_str()) {
            return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                "child producer immutable message differs from original fork msg"));
        }
        return Ok(());
    }
    if command.kind != MODEL_TASK_COMMAND_KIND {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Invalid,
            "non-model child input requires an explicit immutable message pointer"));
    }
    let model: super::ModelTaskCommand = recursive_decode_bytes(&bytes, io)?;
    if !matches!(&model.input, crate::model::ModelContent::Text(message) if message == &arguments.msg) {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "child initial model message differs from original fork msg"));
    }
    Ok(())
}

fn recursive_exact_file_granted(grants: &Capabilities, file: &FileRef, io: Option<&super::PreparationIo>) -> Result<bool> {
    match io {
        Some(io) => io.contains_capability(grants, file, "file:read:"),
        None => Ok(grants.contains(&file.read_capability()?)),
    }
}

fn recursive_decode_bytes<T: super::PreparationRecord>(bytes: &[u8], io: Option<&super::PreparationIo>) -> Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(io) = io { return Ok(crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<T>, bytes, crate::wire::ErrorCode::Storage)?)); }
    let _ = io;
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

fn validate_merge_binding<P, A, O>(context: &ToolContext, source_task: TaskId,
    binding: &RecursiveMergeToolBinding<P, A, O>, io: Option<&super::PreparationIo>) -> Result<()>
where P: StreamProvider {
    let effective = context.effective_scope();
    crate::executor::with_history_io(io, |work| binding.verifier.verify_scope_for_with_work(
        binding.destination.reducer().authority(), &binding.scope, work))?;
    if !recursive_volume_granted(effective.grants(), &binding.project, VolumeOperation::Write, io)? {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "caller lacks its exact merge destination write grant"));
    }
    recursive_verify_volume(&binding.verifier, &binding.scope, &binding.project, VolumeOperation::Write, io)?;
    match &binding.source_read {
        Some(source) => {
            let authorization = source.authorization();
            if Some(authorization.caller_task) != context.durable_task_id()
                || authorization.operation_id != context.operation_id()
                || authorization.source_task != source_task
            {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                    "merge source handle belongs to another caller, operation or selected task"));
            }
            #[cfg(not(target_arch = "wasm32"))]
            let work = io.and_then(super::PreparationIo::original_native_work);
            #[cfg(target_arch = "wasm32")]
            let work = None;
            source.require_selection(context.operation_id(), source_task,
                &binding.source_project, &binding.source_generation, authorization.native_snapshot.as_ref(), work)?;
            source.grant().require(&binding.source_project, VolumeOperation::Read, io)?;
        }
        None => {
            if !recursive_volume_granted(effective.grants(), &binding.source_project, VolumeOperation::Read, io)? {
                return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
                    "merge source has neither an original caller read grant nor a sealed selected-source handle"));
            }
            recursive_verify_volume(&binding.verifier, &binding.scope, &binding.source_project, VolumeOperation::Read, io)?;
        }
    }
    if binding.scope.agent() != binding.destination.reducer().conversation().and_then(|conversation| conversation.agent) {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "merge destination is not the actual caller conversation"));
    }
    Ok(())
}

fn recursive_volume_granted(grants: &Capabilities, volume: &VolumeRef, operation: VolumeOperation,
    io: Option<&super::PreparationIo>) -> Result<bool> {
    match io {
        Some(io) => io.contains_capability(grants, volume, match operation {
            VolumeOperation::Read => "volume:read:", VolumeOperation::Write => "volume:write:",
        }),
        None => Ok(grants.contains(&volume.capability(operation, &mut None)?)),
    }
}

fn recursive_verify_volume(verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
    operation: VolumeOperation, io: Option<&super::PreparationIo>) -> Result<()> {
    let maximum = io.map(|io| io.remaining().map(|remaining| remaining.bytes_encoded)).transpose()?.unwrap_or(u64::MAX);
    crate::executor::with_history_io(io, |work| ContentGrant::authorize_volume(verifier, scope, volume, operation, maximum, work))
}
fn check_merge_binding<P: StreamProvider, A, O>(plan: &RecursiveMergeToolPlan,
    binding: &RecursiveMergeToolBinding<P, A, O>, io: Option<&super::PreparationIo>) -> Result<()> {
    if plan.destination != *binding.destination.reducer().authority() || plan.source != binding.source
        || plan.source_project != binding.source_project || plan.source_generation != binding.source_generation
        || plan.target_project != binding.project || plan.notice != binding.notice
        || plan.authorized_source.as_ref() != binding.source_read.as_ref().map(|source| source.authorization()) {
        return Err(recursive_reject(io, crate::wire::ErrorCode::Unauthorized,
            "restored merge changed its original inspected binding"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "default_tools_tests.rs"]
mod tests;
