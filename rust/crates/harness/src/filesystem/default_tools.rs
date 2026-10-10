//! Shared portable defaults. File tools use the original TaskContext providers;
//! recursive tools retain their plan and every command in a ResumableTool journal.

use super::{
    FilesystemHost, FilesystemTaskRuntime, ForkPreparationLimits, ForkToolArguments,
    ParentMergePlan, ParentProjectController, PreparedTaskAdmitCommand, RetainedSourceBinding,
    TaskCommandHost, TaskCommandProgress, FORK_PREPARE_TASK_COMMAND_KIND,
    MODEL_TASK_COMMAND_KIND, TASK_ADMIT_PREPARED_COMMAND_KIND, fork_preparation_request,
};
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{ContentGrant, ConversationMessage, FileRef, VolumeOperation, VolumeRef},
    core::{Action, Authority, AuthorityVerifier, Command, Scope},
    durable_tool::{ResumableTool, ResumableToolRegistry},
    fork::{ForkReport, ForkRequest, ForkSeed},
    merge::ProjectMergeReceipt,
    resources::GenerationRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskContext, ToolContext, task_id_for_admission},
    scheduler::LeaseFence,
    store::StreamAggregate,
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry, ToolResult},
    workflow::{MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand, WorkflowJournal},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ConflictSide, JoinOutcome, WorkBudget};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

const FORK_SOURCE_STAGE: &str = "acyclic.recursive.fork.source_stage.v1";
const FORK_OBSERVE: &str = "acyclic.recursive.fork.observe.v1";
const FORK_PUBLISH: &str = "acyclic.recursive.fork.publish.v1";
const MERGE_APPLY: &str = "acyclic.recursive.merge.apply.v1";
const MERGE_PUBLISH: &str = "acyclic.recursive.merge.publish.v1";
const REVISION: &str = "portable-recursive-1";

/// Both registries are installed together; neither substitutes a live fallback
/// for a missing pinned durable implementation.
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
    /// Current caller execution reservation.
    pub fence: LeaseFence,
    /// The same actual original caller owner used by the workflow journal.
    pub journal_owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    /// Actual invoking operation token, cancelled when its live future is dropped.
    /// This does not claim durable task cancellation or completed backend stop.
    pub cancellation: CancellationToken,
    /// Original tool-effect journal. Its provider must retain admissions and
    /// atomically fence publication against this caller's execution reservation.
    pub workflow: Arc<dyn WorkflowJournal>,
}

/// Real provider configuration for one exact caller and retained source cut.
/// Implementations authenticate snapshots and restore the original plan on
/// retry; supplying references never confers source, history or write authority.
/// There are deliberately no default/no-op provider methods.
pub trait RecursiveDefaultToolSource<P, A, O>: acyclic_stream::ProviderPlatform {
    /// Binds the actual caller/effect to its owner-controlled workflow journal.
    /// Forwards the actual borrowed invocation token to recursive_tool_owner;
    /// providers must not substitute a fresh independent cancellation token.
    fn owner<'a>(&'a self, context: &'a ToolContext, tool: &'a ResumableTool,
        invocation: &'a ToolInvocation, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveToolOwner<P, A, O>>>;
    /// Selects and authenticates bounded immutable source/allocation identities
    /// without staging a checkpoint, launching a child or allocating resources.
    fn select<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveForkSourceIntent>>;
    /// Uses recursive_fork_source for a new plan, or
    /// restore_recursive_fork_source for the original authenticated parent cut.
    /// The preparation worker must install this exact preparer through
    /// FilesystemTaskCommands::with_fork_preparer.
    fn fork<'a>(&'a self, context: &'a ToolContext, arguments: &'a ForkToolArguments,
        intent: &'a RecursiveForkSourceIntent, original: Option<&'a RecursiveForkToolPlan>, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveForkToolBinding<P, A, O>>>;
    /// Binds the actual destination and explicitly readable selected source.
    fn merge<'a>(&'a self, context: &'a ToolContext, arguments: &'a MergeToolArguments,
        original: Option<&'a RecursiveMergeToolPlan>, cancellation: &'a CancellationToken)
        -> BoxProviderFuture<'a, Result<RecursiveMergeToolBinding<P, A, O>>>;
    /// Restores the exact retained source for an ordinary preparation worker.
    /// The actual caller is request.child.parent, not this helper task's identity.
    fn preparation<'a>(&'a self, context: &'a TaskContext,
        request: &'a crate::resource_preparation::ResourcePreparationRequest)
        -> BoxProviderFuture<'a, Result<RetainedSourceBinding<A, O>>>;
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
    /// Actual authenticated original caller checkpoint; only a bounded immutable
    /// reference is retained, never a whole reducer clone in a preparation token.
    pub source_checkpoint: FileRef,
    /// JSON pointer into child.input containing the exact msg, or its model
    /// WorkflowCommand payload FileRef. Referenced payloads are checked below.
    pub child_message_pointer: String,
    /// Explicit pointer inside immutable producer input bytes. None uses the
    /// actual ModelTaskCommand Text contract for referenced model input.
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
    /// Ref-only canonical merge notice.
    pub notice: ConversationMessage,
}

/// Full original fork provenance is retained before ordinary task admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    /// Original writable target workspace.
    pub target_project: VolumeRef,
    /// Exact compare-and-swap precondition from inspection.
    pub expected_target_generation: GenerationRef,
    /// Exact admitted ref-only notice.
    pub notice: ConversationMessage,
}

/// Installs the same real portable tools for native and browser consumers.
/// No Bash, native mount, process-global registry or success-shaped fallback.
pub fn recursive_default_tools<P, A, O>(
    source: Arc<dyn RecursiveDefaultToolSource<P, A, O>>, maximum_request_bytes: u64,
) -> Result<RecursiveDefaultToolset>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    if maximum_request_bytes == 0 || maximum_request_bytes == u64::MAX {
        return Err(Error::Invalid("recursive request bound must be finite and positive".into()));
    }
    let mut tools = ToolRegistry::new();
    tools.register(crate::tool::files::read_file()?)?;
    tools.register(crate::tool::files::write_file()?)?;
    tools.register(crate::tool::files::edit_file()?)?;
    let mut resumable_tools = ResumableToolRegistry::default();
    for kind in [Kind::Fork, Kind::Merge] {
        let machine = Arc::new(RecursiveToolMachine::new(kind, maximum_request_bytes));
        let definition = definition(kind, maximum_request_bytes)?;
        let durable = ResumableTool { definition: definition.clone(), machine };
        resumable_tools.register(durable.clone())?;
        tools.register(Tool { definition, executor: Arc::new(RecursiveExecutor {
            source: source.clone(), durable, kind, maximum_request_bytes,
        }), projection: Arc::new(RecursiveProjection) })?;
    }
    Ok(RecursiveDefaultToolset { tools, resumable_tools })
}

#[derive(Clone, Copy)]
enum Kind { Fork, Merge }
impl Kind {
    fn name(self) -> &'static str { match self { Self::Fork => "acyclic.fork", Self::Merge => "acyclic.merge" } }
    fn grant(self) -> &'static str { match self { Self::Fork => "fork:publish", Self::Merge => "project:merge" } }
}

fn definition(kind: Kind, maximum: u64) -> Result<ToolDefinition> {
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
    let value = ToolDefinition { name: kind.name().into(), revision: REVISION.into(),
        description: match kind { Kind::Fork => "Prepare, publish and durably admit a new child from the actual calling task; source selects only an explicitly authorized project",
            Kind::Merge => "Import an explicitly readable pinned source project into the actual caller's project through approval and the original inspected CAS target" }.into(),
        input_schema: input, output_schema: output.clone(),
        projection_schema: crate::tool::json_projection_schema(output) };
    value.validate()?;
    Ok(value)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "plan", rename_all = "snake_case", deny_unknown_fields)]
enum Plan { Fork(Box<RecursiveForkToolPlan>), Merge(Box<RecursiveMergeToolPlan>) }
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Stage { Prepare, Observe, ForkPublish, ChildAdmit, MergeApply, MergePublish }
impl Stage {
    fn kind(self) -> &'static str { match self {
        Self::Prepare | Self::ChildAdmit => TASK_ADMIT_PREPARED_COMMAND_KIND,
        Self::Observe => FORK_OBSERVE, Self::ForkPublish => FORK_PUBLISH,
        Self::MergeApply => MERGE_APPLY, Self::MergePublish => MERGE_PUBLISH,
    }}
    fn label(self) -> &'static str { match self {
        Self::Prepare => "prepare", Self::Observe => "observe", Self::ForkPublish => "fork-publish",
        Self::ChildAdmit => "child-admit", Self::MergeApply => "merge-apply", Self::MergePublish => "merge-publish",
    }}
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready { arguments: Value },
    SourcePinned { arguments: Value, intent: RecursiveForkSourceIntent, command: WorkflowCommand },
    Waiting { arguments: Value, plan: Plan, stage: Stage, command: WorkflowCommand,
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

struct RecursiveToolMachine { identity: MachineIdentity, schema: Value, kind: Kind, maximum: u64 }
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
        Self { identity: MachineIdentity { name: kind.name().into(), version: REVISION.into(), digest }, kind, maximum,
            schema: json!({"oneOf":[
                {"type":"object","required":["phase","arguments"],"properties":{"phase":{"const":"ready"},"arguments":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","arguments","intent","command"],"properties":{
                    "phase":{"const":"source_pinned"},"arguments":{"type":"object"},"intent":{"type":"object"},
                    "command":{"type":"object"}},"additionalProperties":false},
                {"type":"object","required":["phase","arguments","plan","stage","command","report","receipt"],"properties":{
                    "phase":{"const":"waiting"},"arguments":{"type":"object"},"plan":{"type":"object"},"stage":{"type":"string"},
                    "command":{"type":"object"},"report":{"type":["object","null"]},"receipt":{"type":["object","null"]}},"additionalProperties":false},
                {"type":"object","required":["phase","value"],"properties":{"phase":{"const":"complete"},"value":{"type":"object"}},"additionalProperties":false}
            ]}) }
    }
}
impl ResumableMachine for RecursiveToolMachine {
    fn identity(&self) -> &MachineIdentity { &self.identity }
    fn state_schema(&self) -> &Value { &self.schema }
    fn initialize(&self, input: &Value) -> Result<Value> {
        crate::contract::validate_json_byte_bound(input, self.maximum)?;
        match self.kind { Kind::Fork => { let _: ForkToolArguments = decode(input)?; }, Kind::Merge => { let _: MergeToolArguments = decode(input)?; } }
        encode(State::Ready { arguments: input.clone() })
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        crate::contract::validate_json_byte_bound(input, self.maximum)?;
        let state: State = decode(state)?;
        let next = match state {
            State::Ready { arguments } => {
                if matches!(self.kind, Kind::Fork) {
                    let selected: SourcePin = decode(input)?;
                    validate_source_intent(&selected.intent, &arguments)?;
                    check_source_command(&selected.command, &selected.intent)?;
                    State::SourcePinned { arguments, intent: selected.intent, command: selected.command }
                } else {
                    let initial: InitialPlan = decode(input)?;
                    validate_plan(&initial.plan, &arguments)?;
                    if !matches!(initial.plan, Plan::Merge(_)) {
                        return Err(Error::Unauthorized("merge tool plan names another tool".into()));
                    }
                    check_command(&initial.command, Stage::MergeApply, &initial.plan)?;
                    State::Waiting { arguments, plan: initial.plan, stage: Stage::MergeApply,
                        command: initial.command, report: None, receipt: None }
                }
            }
            State::SourcePinned { arguments, intent, command } => {
                validate_source_intent(&intent, &arguments)?;
                check_source_command(&command, &intent)?;
                let initial: InitialPlan = decode(input)?;
                validate_plan(&initial.plan, &arguments)?;
                let Plan::Fork(plan) = &initial.plan else {
                    return Err(Error::Unauthorized("pinned fork source produced another tool plan".into()));
                };
                if plan.caller != intent.caller || plan.request.child.operation_id != intent.child_operation_id
                    || plan.allocation_digest != intent.allocation_digest
                    || decode::<ForkRequest>(&plan.request.payload)? != intent.request
                {
                    return Err(Error::Unauthorized("fork plan changed its durably selected original source/allocation".into()));
                }
                check_command(&initial.command, Stage::Prepare, &initial.plan)?;
                State::Waiting { arguments, plan: initial.plan, stage: Stage::Prepare,
                    command: initial.command, report: None, receipt: None }
            }
            State::Waiting { arguments, plan, stage, command, mut report, mut receipt } => {
                validate_plan(&plan, &arguments)?;
                check_command(&command, stage, &plan)?;
                let result: Receipt = decode(input)?;
                if result.operation_id != command.operation_id {
                    return Err(Error::Unauthorized("recursive receipt belongs to another command".into()));
                }
                let value = match result.value {
                    Outcome::Succeeded(value) => value,
                    Outcome::Indeterminate { operation_id } => return Err(Error::Indeterminate(operation_id)),
                    Outcome::Failed { message } => return Ok(MachineTransition { state: encode(State::Complete { value: json!({"status":"failed","message":message}) })?, commands: Vec::new(), status: MachineStatus::Failed { message } }),
                    Outcome::Cancelled => return Err(Error::Conflict("recursive command cancelled without a successful receipt".into())),
                };
                let following = match (&plan, stage) {
                    (Plan::Fork(fork), Stage::Prepare) => {
                        if decode::<TaskId>(&value)? != fork.preparation.task_id() { return Err(Error::Unauthorized("preparation admission acknowledged another child".into())); }
                        Some(Stage::Observe)
                    }
                    (Plan::Fork(fork), Stage::Observe) => {
                        let observed: ForkReport = decode(&value)?;
                        observed.validate()?;
                        if encode(&observed.request)? != fork.request.payload { return Err(Error::Unauthorized("fork report changed the original source request".into())); }
                        report = Some(Box::new(observed));
                        Some(Stage::ForkPublish)
                    }
                    (Plan::Fork(_), Stage::ForkPublish) => {
                        let seed: ForkSeed = decode(&value)?;
                        if Some(seed) != report.as_ref().map(|report| report.as_ref().clone().into_seed()).transpose()? {
                            return Err(Error::Unauthorized("fork publication receipt changed the prepared child".into()));
                        }
                        Some(Stage::ChildAdmit)
                    }
                    (Plan::Fork(fork), Stage::ChildAdmit) => {
                        if decode::<TaskId>(&value)? != fork.request.child.task_id() { return Err(Error::Unauthorized("child admission receipt changed its task identity".into())); }
                        if result.next.is_some() { return Err(Error::Invalid("completed fork has an unexpected outbox".into())); }
                        return completed(json!({"status":"admitted","id":fork.arguments.id,"task":fork.request.child.task_id(),
                            "source":fork.arguments.source.unwrap_or(fork.caller),"operation_id":decode::<ForkRequest>(&fork.request.payload)?.operation_id,
                            "seed":report.as_ref().ok_or_else(|| Error::Invalid("fork has no prepared report".into()))?.as_ref().clone().into_seed()?}));
                    }
                    (Plan::Merge(merge), Stage::MergeApply) => {
                        if value.get("status").and_then(Value::as_str) == Some("applied") {
                            let observed: ProjectMergeReceipt = decode(value.get("receipt").ok_or_else(|| Error::Invalid("applied merge has no receipt".into()))?)?;
                            validate_merge_receipt(merge, &observed)?;
                            receipt = Some(Box::new(observed));
                            Some(Stage::MergePublish)
                        } else {
                            if !matches!(value.get("status").and_then(Value::as_str),
                                Some("no_changes" | "stale_target" | "conflicted" | "fenced" | "idempotency_conflict")) {
                                return Err(Error::Invalid("merge receipt is not a canonical provider outcome".into()));
                            }
                            if result.next.is_some() { return Err(Error::Invalid("unapplied merge has a publication command".into())); }
                            return completed(value);
                        }
                    }
                    (Plan::Merge(merge), Stage::MergePublish) => {
                        let observed: ProjectMergeReceipt = decode(&value)?;
                        validate_merge_receipt(merge, &observed)?;
                        if receipt.as_deref() != Some(&observed) || result.next.is_some() { return Err(Error::Unauthorized("merge publication changed its original receipt".into())); }
                        return completed(json!({"status":"published","receipt":observed}));
                    }
                    _ => return Err(Error::Invalid("recursive stage does not match its original plan".into())),
                };
                let stage = following.ok_or_else(|| Error::Invalid("recursive next stage missing".into()))?;
                let command = result.next.ok_or_else(|| Error::Invalid("recursive next outbox missing".into()))?;
                check_command(&command, stage, &plan)?;
                State::Waiting { arguments, plan, stage, command, report, receipt }
            }
            State::Complete { .. } => return Err(Error::Conflict("recursive tool is already complete".into())),
        };
        let command = match &next {
            State::Waiting { command, .. } | State::SourcePinned { command, .. } => command.clone(),
            _ => return Err(Error::Invalid("recursive waiting state missing".into())),
        };
        Ok(MachineTransition { commands: vec![command], state: encode(next)?, status: MachineStatus::Suspended })
    }
}

fn completed(value: Value) -> Result<MachineTransition> {
    Ok(MachineTransition { state: encode(State::Complete { value: value.clone() })?, commands: Vec::new(), status: MachineStatus::Completed { value } })
}
fn check_command(command: &WorkflowCommand, stage: Stage, plan: &Plan) -> Result<()> {
    command.validate()?;
    let effect = match plan {
        Plan::Fork(plan) => decode::<ForkRequest>(&plan.request.payload)?.operation_id,
        Plan::Merge(plan) => plan.operation_id,
    };
    if command.kind != stage.kind() || command.operation_id != derived(effect, stage.label()) {
        return Err(Error::Unauthorized("recursive command identity or kind changed".into()));
    }
    Ok(())
}
fn validate_source_intent(intent: &RecursiveForkSourceIntent, arguments: &Value) -> Result<()> {
    intent.request.validate()?;
    if intent.request.selections.iter().any(|selection|
        matches!(&selection.revision, crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. }
            if !paths.is_empty() || volume.owner() == &crate::conversation::VolumeOwner::Agent(intent.arguments.id)))
    {
        return Err(Error::Unauthorized("default fork must create a new agent with empty inherited scratch".into()));
    }
    if encode(&intent.arguments)? != *arguments || intent.request.child_agent != intent.arguments.id
        || intent.source_task != intent.arguments.source.unwrap_or(intent.caller)
        || intent.child_operation_id == intent.request.operation_id
        || task_id_for_admission(intent.child_operation_id) == intent.caller
    {
        return Err(Error::Unauthorized("source intent changed original arguments, caller or independent allocation".into()));
    }
    Ok(())
}
fn check_source_command(command: &WorkflowCommand, intent: &RecursiveForkSourceIntent) -> Result<()> {
    command.validate()?;
    if command.kind != FORK_SOURCE_STAGE
        || command.operation_id != derived(intent.request.operation_id, "source-stage")
    {
        return Err(Error::Unauthorized("source staging command changed its original effect identity".into()));
    }
    Ok(())
}
fn validate_plan(plan: &Plan, arguments: &Value) -> Result<()> {
    match plan {
        Plan::Fork(plan) => {
            plan.request.validate()?;
            plan.preparation.validate()?;
            plan.source_checkpoint.validate()?;
            if plan.request.source_checkpoint.as_ref() != Some(&plan.source_checkpoint) {
                return Err(Error::Unauthorized("fork plan changed its original source checkpoint".into()));
            }
            let fork: ForkRequest = decode(&plan.request.payload)?;
            fork.validate()?;
            if encode(&plan.arguments)? != *arguments || fork.child_agent != plan.arguments.id
                || plan.preparation.parent != Some(plan.caller) || plan.request.child.parent != Some(plan.caller)
                || plan.preparation.operation_id == plan.request.child.operation_id
                || plan.preparation.operation_id == fork.operation_id || plan.request.child.operation_id == fork.operation_id
                || plan.preparation.task_id() == plan.caller || plan.request.child.task_id() == plan.caller
                || plan.preparation.input != encode(&plan.request)?
                || plan.preparation.machine.name != "acyclic.fork_preparation" || plan.preparation.machine.version != "1"
                || plan.request.command.as_ref().is_none_or(|command| command.kind != FORK_PREPARE_TASK_COMMAND_KIND || command.operation_id != fork.operation_id)
            { return Err(Error::Unauthorized("fork plan changed original arguments, parent or independent identities".into())); }
            if plan.child_message_pointer.is_empty() || !plan.child_message_pointer.starts_with('/') {
                return Err(Error::Invalid("fork producer message pointer is invalid".into()));
            }
            if plan.child_message_file_pointer.as_ref().is_some_and(|pointer|
                pointer.is_empty() || !pointer.starts_with('/'))
            {
                return Err(Error::Invalid("fork immutable input message pointer is invalid".into()));
            }
        }
        Plan::Merge(plan) => {
            if encode(&plan.arguments)? != *arguments || plan.source_project == plan.target_project {
                return Err(Error::Unauthorized("merge plan changed original arguments or source/destination".into()));
            }
            plan.source_project.validate()?; plan.target_project.validate()?;
            plan.source_generation.validate()?; plan.expected_target_generation.validate()?; plan.notice.validate()?;
        }
    }
    Ok(())
}
fn validate_merge_receipt(plan: &RecursiveMergeToolPlan, receipt: &ProjectMergeReceipt) -> Result<()> {
    receipt.validate_shape()?;
    if receipt.operation_id != plan.operation_id || receipt.child != plan.source || receipt.source_project != plan.source_project
        || receipt.source_generation != plan.source_generation || receipt.target_project != plan.target_project
        || receipt.expected_target_generation != plan.expected_target_generation || receipt.notice != plan.notice {
        return Err(Error::Unauthorized("merge receipt changed the inspected source or CAS target".into()));
    }
    Ok(())
}

struct RecursiveExecutor<P, A, O> {
    source: Arc<dyn RecursiveDefaultToolSource<P, A, O>>, durable: ResumableTool,
    kind: Kind, maximum_request_bytes: u64,
}

struct InvocationCancellation(CancellationToken);
impl Drop for InvocationCancellation {
    fn drop(&mut self) { self.0.cancel(); }
}
impl<P, A, O> RecursiveExecutor<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn invoke<'a>(&'a self, context: ToolContext, invocation: ToolInvocation)
        -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        let cancellation = CancellationToken::new();
        let guard = InvocationCancellation(cancellation.clone());
        Box::pin(async move {
            let _guard = guard;
            self.run(context, invocation, &cancellation).await
        })
    }
    async fn run(&self, context: ToolContext, invocation: ToolInvocation, cancellation: &CancellationToken) -> Result<Option<ToolResult>> {
        cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
        self.authorize_with_context(&context, &invocation)?;
        let owner = self.source.owner(&context, &self.durable, &invocation, cancellation).await?;
        let caller = context.durable_task_id().ok_or_else(|| Error::Unauthorized("recursive tool has no durable caller".into()))?;
        let admitted = owner.runtime.task_host().observe_admission(caller).await?;
        if admitted.grants != *context.scope().grants() || admitted.limits != context.scope().limits()
            || admitted.run_limits != context.scope().run_limits() {
            return Err(Error::Unauthorized("recursive owner differs from the original admitted caller".into()));
        }
        owner.runtime.task_host().verify_dispatch_owner(caller, owner.fence.clone()).await?;
        let mut session = self.durable.open(context.clone(), invocation.clone(), owner.workflow.clone()).await?;
        // At most seven committed boundaries, including the immutable source
        // selection before any source staging. Pending workers never get polled.
        for _ in 0..8 {
            cancellation.check().map_err(|error| Error::Storage(error.to_string()))?;
            owner.runtime.task_host().verify_dispatch_owner(caller, owner.fence.clone()).await?;
            let state: State = decode(&session.checkpoint().state)?;
            let input = match state {
                State::Complete { value } => return Ok(Some(ToolResult { value })),
                State::Ready { arguments } if matches!(self.kind, Kind::Fork) => {
                    let original_arguments: ForkToolArguments = decode(&arguments)?;
                    original_arguments.source_task(caller, context.scope().limits())?;
                    // This payload is independent of the selected source cut.
                    let payload = stage(&context, derived(invocation.operation_id, "source-arguments"),
                        "source-arguments", &arguments, self.maximum_request_bytes, cancellation).await?;
                    let intent = self.source.select(&context, &original_arguments, cancellation).await?;
                    validate_source_intent(&intent, &arguments)?;
                    if intent.caller != caller || intent.request.operation_id != invocation.operation_id {
                        return Err(Error::Unauthorized("source selector is not the actual original caller/effect".into()));
                    }
                    encode(SourcePin { intent, command: WorkflowCommand {
                        operation_id: derived(invocation.operation_id, "source-stage"),
                        kind: FORK_SOURCE_STAGE.into(), payload,
                    } })?
                }
                State::Ready { arguments } => {
                    let plan = self.plan(&context, &owner.runtime, caller, &arguments, None, cancellation).await?;
                    let command = self.command(&context, Stage::MergeApply, &plan, None, None, cancellation).await?;
                    encode(InitialPlan { plan, command })?
                }
                State::SourcePinned { arguments, intent, command } => {
                    validate_source_intent(&intent, &arguments)?;
                    check_source_command(&command, &intent)?;
                    let bytes = read(context.task(), &command.payload, self.maximum_request_bytes, cancellation).await?;
                    if crate::contract::json_from_slice::<Value>(&bytes)
                        .map_err(|error| Error::Storage(error.to_string()))? != arguments {
                        return Err(Error::Unauthorized("source-stage outbox changed original tool arguments".into()));
                    }
                    let plan = self.plan(&context, &owner.runtime, caller, &arguments, Some(&intent), cancellation).await?;
                    let command = self.command(&context, Stage::Prepare, &plan, None, None, cancellation).await?;
                    encode(InitialPlan { plan, command })?
                }
                State::Waiting { plan, stage, command, report, receipt, .. } => {
                    validate_plan(&plan, &invocation.arguments)?;
                    if command.operation_id != derived(invocation.operation_id, stage.label()) {
                        return Err(Error::Unauthorized("recursive outbox operation differs from original effect".into()));
                    }
                    let expected_payload = command_payload(stage, &plan, report.as_deref(), receipt.as_deref())?;
                    let bytes = read(context.task(), &command.payload, self.maximum_request_bytes, cancellation).await?;
                    if crate::contract::json_from_slice::<Value>(&bytes).map_err(|error| Error::Storage(error.to_string()))? != expected_payload {
                        return Err(Error::Unauthorized("recursive outbox payload changed its original plan".into()));
                    }
                    let Some(value) = self.dispatch(&context, &owner, &plan, stage, command.clone(), expected_payload,
                        report.as_deref(), receipt.as_deref(), cancellation).await? else { return Ok(None); };
                    let next_stage = match stage {
                        Stage::Prepare => Some(Stage::Observe), Stage::Observe => Some(Stage::ForkPublish),
                        Stage::ForkPublish => Some(Stage::ChildAdmit), Stage::ChildAdmit | Stage::MergePublish => None,
                        Stage::MergeApply if value.get("status").and_then(Value::as_str) == Some("applied") => Some(Stage::MergePublish),
                        Stage::MergeApply => None,
                    };
                    let next_report = if stage == Stage::Observe { Some(decode::<ForkReport>(&value)?) } else { report.map(|report| *report) };
                    let next_receipt = if stage == Stage::MergeApply && next_stage.is_some() {
                        Some(decode::<ProjectMergeReceipt>(value.get("receipt").ok_or_else(|| Error::Invalid("merge receipt missing".into()))?)?)
                    } else { receipt.map(|receipt| *receipt) };
                    let next = match next_stage { Some(stage) => Some(self.command(&context, stage, &plan,
                        next_report.as_ref(), next_receipt.as_ref(), cancellation).await?), None => None };
                    encode(Receipt { operation_id: command.operation_id, value: Outcome::Succeeded(value), next })?
                }
            };
            let revision = session.checkpoint().revision;
            let operation = derived(invocation.operation_id, &format!("transition-{revision}"));
            let transition = session.step(operation, IdempotencyKey::new(format!("recursive-tool:{}:{revision}", invocation.operation_id))?, input).await?;
            owner.runtime.task_host().verify_dispatch_owner(caller, owner.fence.clone()).await?;
            match transition.status {
                MachineStatus::Completed { value } => return Ok(Some(ToolResult { value })),
                MachineStatus::Failed { message } => return Err(Error::Conflict(message)),
                MachineStatus::Suspended => {}
            }
        }
        Err(Error::Invalid("recursive tool exceeded its finite command sequence".into()))
    }

    async fn plan(&self, context: &ToolContext, runtime: &FilesystemTaskRuntime<P, A, O>, caller: TaskId,
        arguments: &Value, intent: Option<&RecursiveForkSourceIntent>, cancellation: &CancellationToken) -> Result<Plan> {
        let plan = match self.kind {
            Kind::Fork => {
                let arguments: ForkToolArguments = decode(arguments)?;
                arguments.source_task(caller, context.scope().limits())?;
                let intent = intent.ok_or_else(|| Error::Unauthorized("fork source choice is not durably pinned".into()))?;
                let binding = self.source.fork(context, &arguments, intent, None, cancellation).await?;
                if binding.source.request != intent.request || binding.child.operation_id != intent.child_operation_id {
                    return Err(Error::Unauthorized("source staging changed the selected immutable source/allocation".into()));
                }
                validate_fork_binding(context, &arguments, &binding, cancellation).await?;
                let operation = derived(context.operation_id(), "preparation-task");
                let payload = stage(context, derived(context.operation_id(), "preparation-payload"),
                    "preparation-payload", &encode(&binding.source.request)?, self.maximum_request_bytes, cancellation).await?;
                let request = fork_preparation_request(binding.source.preparer.as_ref(), binding.source.request,
                    task_id_for_admission(operation), binding.child, WorkflowCommand {
                        operation_id: context.operation_id(), kind: FORK_PREPARE_TASK_COMMAND_KIND.into(), payload,
                    }, binding.limits, Some(binding.source_checkpoint.clone()), binding.input_files)?;
                let preparation = runtime.harness().registered_child_admission(operation, "acyclic.fork_preparation", "1", encode(&request)?, caller, context.scope())?;
                Plan::Fork(Box::new(RecursiveForkToolPlan { arguments, caller, request, preparation,
                    child_message_pointer: binding.child_message_pointer,
                    child_message_file_pointer: binding.child_message_file_pointer,
                    allocation_digest: intent.allocation_digest, source_checkpoint: binding.source_checkpoint }))
            }
            Kind::Merge => {
                let arguments: MergeToolArguments = decode(arguments)?;
                let binding = self.source.merge(context, &arguments, None, cancellation).await?;
                validate_merge_binding(context, &binding)?;
                let controller = ParentProjectController::new(binding.filesystem.as_ref(), binding.destination.reducer(), &binding.verifier, &binding.scope, binding.project.clone())?;
                let inspected = controller.prepare_project_import(&binding.source_project, &binding.source_generation).await?;
                Plan::Merge(Box::new(RecursiveMergeToolPlan { arguments, caller, operation_id: context.operation_id(),
                    destination: binding.destination.reducer().authority().clone(), source: binding.source,
                    source_project: binding.source_project, source_generation: binding.source_generation,
                    target_project: binding.project, expected_target_generation: binding.filesystem.generation_ref_id(inspected.target_head())?, notice: binding.notice }))
            }
        };
        validate_plan(&plan, arguments)?;
        crate::contract::validate_json_byte_bound(&plan, self.maximum_request_bytes.min(context.scope().limits().file_bytes))?;
        Ok(plan)
    }

    async fn command(&self, context: &ToolContext, stage_kind: Stage, plan: &Plan, report: Option<&ForkReport>,
        receipt: Option<&ProjectMergeReceipt>, cancellation: &CancellationToken) -> Result<WorkflowCommand> {
        let operation_id = derived(context.operation_id(), stage_kind.label());
        let payload = command_payload(stage_kind, plan, report, receipt)?;
        Ok(WorkflowCommand { operation_id, kind: stage_kind.kind().into(),
            payload: stage(context, derived(operation_id, "payload"), stage_kind.label(), &payload,
                self.maximum_request_bytes, cancellation).await? })
    }

    async fn dispatch(&self, context: &ToolContext, owner: &RecursiveToolOwner<P, A, O>, plan: &Plan,
        stage: Stage, command: WorkflowCommand, payload: Value,
        report: Option<&ForkReport>, receipt: Option<&ProjectMergeReceipt>, cancellation: &CancellationToken) -> Result<Option<Value>> {
        match (plan, stage) {
            (Plan::Fork(_), Stage::Prepare | Stage::ChildAdmit) => {
                match owner.runtime.commands().execute(context.task().clone(), owner.fence.clone(), command, payload).await? {
                    TaskCommandProgress::Ready(value) => match decode::<Outcome<Value>>(&value)? {
                        Outcome::Succeeded(value) => Ok(Some(value)), Outcome::Failed { message } => Err(Error::Conflict(message)),
                        Outcome::Cancelled => Err(Error::Conflict("recursive admission cancelled".into())), Outcome::Indeterminate { .. } => Ok(None),
                    },
                    TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => Ok(None),
                }
            }
            (Plan::Fork(fork), Stage::Observe) => {
                let retained = owner.runtime.task_host().observe_admission(fork.preparation.task_id()).await?;
                if retained != fork.preparation { return Err(Error::Unauthorized("observed preparation admission changed".into())); }
                match owner.runtime.task_host().outcome(fork.preparation.task_id()).await? {
                    Some(Outcome::Succeeded(value)) => { let report: ForkReport = decode(&value)?; report.validate()?;
                        if encode(&report.request)? != fork.request.payload { return Err(Error::Unauthorized("preparation output changed its original request".into())); }
                        Ok(Some(value)) },
                    Some(Outcome::Failed { message }) => Err(Error::Conflict(message)),
                    Some(Outcome::Cancelled) => Err(Error::Conflict("preparation cancelled; child was not admitted".into())),
                    None | Some(Outcome::Indeterminate { .. }) => Ok(None),
                }
            }
            (Plan::Fork(fork), Stage::ForkPublish) => {
                let intent = RecursiveForkSourceIntent {
                    arguments: fork.arguments.clone(), caller: fork.caller,
                    source_task: fork.arguments.source.unwrap_or(fork.caller),
                    child_operation_id: fork.request.child.operation_id,
                    allocation_digest: fork.allocation_digest, request: decode(&fork.request.payload)?,
                };
                let mut binding = self.source.fork(context, &fork.arguments, &intent, Some(fork), cancellation).await?;
                validate_fork_binding(context, &fork.arguments, &binding, cancellation).await?;
                if encode(&binding.source.request)? != fork.request.payload || binding.child_message_pointer != fork.child_message_pointer
                    || binding.child_message_file_pointer != fork.child_message_file_pointer
                    || binding.source_checkpoint != fork.source_checkpoint {
                    return Err(Error::Unauthorized("restored fork source differs from the original cut".into()));
                }
                let restored = fork_preparation_request(
                    binding.source.preparer.as_ref(), binding.source.request,
                    fork.preparation.task_id(), binding.child,
                    fork.request.command.clone().ok_or_else(|| Error::Invalid("fork preparation command missing".into()))?,
                    binding.limits, Some(binding.source_checkpoint), binding.input_files,
                )?;
                if restored != fork.request {
                    return Err(Error::Unauthorized("restored fork child, source or allowances changed".into()));
                }
                let report = report.ok_or_else(|| Error::Invalid("fork publication has no canonical report".into()))?;
                let expected_seed = report.clone().into_seed()?;
                let budget = finite_budget(fork.request.maximum_work, fork.request.maximum_bytes);
                let published = binding.parent.publish_fork_report_measured(report.clone(), binding.parent_scope,
                    owner.journal_owner.as_ref(), budget, &owner.cancellation).await.map_err(|failure| failure.error)?;
                let remaining = published.work.remaining(budget).map_err(|error| Error::Invalid(error.to_string()))?;
                let seed = binding.parent.reducer().fork(&report.request.child)
                    .ok_or_else(|| Error::Storage("acknowledged fork publication has no canonical seed".into()))?;
                if seed != &expected_seed {
                    return Err(Error::Unauthorized("acknowledged fork seed changed the original prepared report".into()));
                }
                binding.child_conversation.bind_published_child_measured(&binding.parent, seed, binding.child_scope,
                    owner.journal_owner.as_ref(), remaining, &owner.cancellation).await.map_err(|failure| failure.error)?;
                Ok(Some(encode(seed)?))
            }
            (Plan::Merge(merge), Stage::MergeApply) => {
                let binding = self.source.merge(context, &merge.arguments, Some(merge), cancellation).await?;
                validate_merge_binding(context, &binding)?;
                check_merge_binding(merge, &binding)?;
                let controller = ParentProjectController::new(binding.filesystem.as_ref(), binding.destination.reducer(), &binding.verifier, &binding.scope, binding.project.clone())?;
                let inspected = controller.prepare_project_import_at(&merge.source_project, &merge.source_generation, &merge.expected_target_generation).await?;
                let outcome = if merge.arguments.selections.is_empty() { controller.apply_project_merge(&inspected, context.operation_id()).await? } else {
                    let mut sides = BTreeMap::new();
                    for selection in &merge.arguments.selections {
                        let side = match selection.side { MergeToolSide::Base => ConflictSide::Base, MergeToolSide::Target => ConflictSide::Ours, MergeToolSide::Source => ConflictSide::Theirs };
                        let conflict = super::project_workspaces::decode_conflict(&selection.conflict)?;
                        if sides.insert(conflict, side).is_some() { return Err(Error::Invalid("merge conflict selected twice".into())); }
                    }
                    controller.apply_project_merge_sides(&inspected, context.operation_id(), sides).await?
                };
                let value = match &outcome {
                    JoinOutcome::Applied(_) | JoinOutcome::AlreadyApplied(_) => json!({"status":"applied","receipt":controller.merge_receipt(&inspected, &outcome, merge.source.clone(), context.operation_id(), merge.notice.clone())?}),
                    JoinOutcome::NoChanges(generation) => json!({"status":"no_changes","generation":binding.filesystem.generation_ref(generation)?}),
                    JoinOutcome::StaleTarget(generation) => json!({"status":"stale_target","generation":binding.filesystem.generation_ref(generation)?}),
                    JoinOutcome::Conflicted { conflicts, truncated } => json!({"status":"conflicted","conflicts":conflicts.iter().map(super::project_workspaces::encode_conflict).collect::<Vec<_>>(),"truncated":truncated}),
                    JoinOutcome::Fenced => json!({"status":"fenced"}), JoinOutcome::IdempotencyConflict => json!({"status":"idempotency_conflict"}),
                };
                Ok(Some(value))
            }
            (Plan::Merge(merge), Stage::MergePublish) => {
                let mut binding = self.source.merge(context, &merge.arguments, Some(merge), cancellation).await?;
                validate_merge_binding(context, &binding)?; check_merge_binding(merge, &binding)?;
                let receipt = receipt.ok_or_else(|| Error::Invalid("merge publication has no provider receipt".into()))?;
                validate_merge_receipt(merge, receipt)?;
                if receipt.operation_id != context.operation_id() { return Err(Error::Unauthorized("merge receipt belongs to another effect".into())); }
                binding.destination.execute(Command { operation_id: context.operation_id(),
                    idempotency_key: IdempotencyKey::new(format!("recursive-merge-publication:{}", context.operation_id()))?,
                    expected_revision: binding.destination.reducer().revision(), scope: binding.scope, causal_parent: None,
                    action: Action::PublishProjectMerge { receipt: Box::new(receipt.clone()) }, }).await?;
                Ok(Some(encode(receipt)?))
            }
            _ => Err(Error::Invalid("recursive command does not match its retained plan".into())),
        }
    }
}

impl<P, A, O> ToolExecutor for RecursiveExecutor<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    fn resumable_machine_identity(&self) -> Option<MachineIdentity> {
        Some(self.durable.machine.identity().clone())
    }
    fn authorize(&self, _: Option<&RuntimeScope>, _: &ToolInvocation) -> Result<()> {
        Err(Error::Unauthorized("recursive tools require the original durable ToolContext".into()))
    }
    fn authorize_with_context(&self, context: &ToolContext, invocation: &ToolInvocation) -> Result<()> {
        invocation.validate()?;
        if context.operation_id() != invocation.operation_id || context.call_id() != invocation.call_id
            || context.durable_task_id().is_none() || invocation.name != self.kind.name()
            || !context.scope().grants().contains(self.kind.grant())
            || !context.scope().grants().contains(&crate::contract::capability::tool_call(self.kind.name())) {
            return Err(Error::Unauthorized("recursive tool lacks its exact admitted caller/effect authority".into()));
        }
        crate::contract::validate_json_byte_bound(&invocation.arguments, self.maximum_request_bytes.min(context.scope().limits().file_bytes))
    }
    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Unauthorized("recursive tools require original durable task context".into())) })
    }
    fn execute_with_context<'a>(&'a self, context: ToolContext, invocation: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        let operation = invocation.operation_id;
        let future = self.invoke(context, invocation);
        Box::pin(async move { future.await?.ok_or(Error::Indeterminate(operation)) })
    }
    fn reconcile<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Err(Error::Unauthorized("recursive reconciliation requires original durable task context".into())) })
    }
    fn reconcile_with_context<'a>(&'a self, context: ToolContext, invocation: ToolInvocation) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        self.invoke(context, invocation)
    }
}
struct RecursiveProjection;
impl ToolProjection for RecursiveProjection {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> { crate::tool::schema::project_json(&result.value) }
}

fn command_payload(stage: Stage, plan: &Plan, report: Option<&ForkReport>, receipt: Option<&ProjectMergeReceipt>) -> Result<Value> {
    match (plan, stage) {
        (Plan::Fork(plan), Stage::Prepare) => encode(PreparedTaskAdmitCommand { admission: plan.preparation.clone() }),
        (Plan::Fork(plan), Stage::Observe) => Ok(json!({"preparation":plan.preparation.task_id()})),
        (Plan::Fork(_), Stage::ForkPublish) => encode(report.ok_or_else(|| Error::Invalid("publication report missing".into()))?),
        (Plan::Fork(plan), Stage::ChildAdmit) => encode(PreparedTaskAdmitCommand { admission: plan.request.child.clone() }),
        (Plan::Merge(plan), Stage::MergeApply) => encode(plan),
        (Plan::Merge(_), Stage::MergePublish) => encode(receipt.ok_or_else(|| Error::Invalid("publication receipt missing".into()))?),
        _ => Err(Error::Invalid("command payload does not match retained plan".into())),
    }
}
async fn stage(context: &ToolContext, operation: OperationId, label: &str, value: &Value, maximum: u64,
    cancellation: &CancellationToken) -> Result<FileRef> {
    let io = super::fork_preparer::PreparationIo::from_budget(finite_budget(maximum, maximum), cancellation)?;
    let bytes = io.encode_json(value, maximum.min(context.scope().limits().file_bytes), true)?;
    io.receive_content(context.stage_file_once_measured(operation,
        &format!("recursive-tools/{}/{label}.json", context.operation_id()), &bytes,
        "application/json", &format!("{label}.json"), io.remaining()?, cancellation).await)
}
async fn read(context: &TaskContext, file: &FileRef, maximum: u64, cancellation: &CancellationToken) -> Result<Vec<u8>> {
    context.read_file_measured(file, finite_budget(maximum, maximum), cancellation).await
        .map(|receipt| receipt.value).map_err(|failure| failure.error)
}
fn finite_budget(work: u64, bytes: u64) -> WorkBudget {
    WorkBudget {
        authority_records_read: work, authority_records_appended: work,
        authority_bytes_read: bytes, authority_bytes_written: bytes,
        object_probes: work, backend_read_operations: work, backend_write_operations: work,
        durability_operations: work, page_reads: work, page_writes: work,
        object_bytes_read: bytes, object_bytes_written: bytes, bytes_hashed: bytes,
        bytes_copied: bytes, bytes_encoded: bytes, source_bytes_read: bytes,
        source_path_components: work, source_entries_visited: work, output_bytes: bytes,
        items_examined: work, items_returned: work, allocation_operations: work,
        peak_allocation_bytes: bytes, materializations: work,
    }
}
fn derived(effect: OperationId, label: &str) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/recursive-tool\0"); hash.update(&effect.into_bytes()); hash.update(label.as_bytes());
    let mut bytes = [0; 16]; bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]); OperationId::from_bytes(bytes)
}
fn decode<T: serde::de::DeserializeOwned>(value: &Value) -> Result<T> {
    serde_json::from_value(value.clone()).map_err(|error| Error::Invalid(error.to_string()))
}
fn encode(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))
}
async fn validate_fork_binding<P, A, O>(context: &ToolContext, arguments: &ForkToolArguments,
    binding: &RecursiveForkToolBinding<P, A, O>, cancellation: &CancellationToken) -> Result<()>
where P: StreamProvider, A: AsyncAuthorityStore, O: AsyncObjectStore {
    binding.source.request.validate()?; binding.child.validate()?; binding.source_checkpoint.validate()?;
    context.scope().limits().validate_file(&binding.source_checkpoint)?;
    if !crate::runtime::read_granted(context.scope().grants(), &binding.source_checkpoint)? {
        return Err(Error::Unauthorized("caller cannot read the original fork checkpoint".into()));
    }
    if binding.source.request.operation_id != context.operation_id() || binding.source.request.child_agent != arguments.id
        || binding.child.parent != context.durable_task_id()
        || binding.parent.reducer().authority() != &binding.source.request.parent
        || binding.child_conversation.reducer().authority() != &binding.source.request.child
        || binding.child_scope.agent() != Some(arguments.id)
        || binding.parent_scope.agent() != binding.parent.reducer().conversation().and_then(|conversation| conversation.agent) {
        return Err(Error::Unauthorized("fork binding is not the actual original caller and new child".into()));
    }
    for selection in &binding.source.request.selections {
        if let crate::fork::ResourceRevision::PrivateVolume { volume, paths, .. } = &selection.revision {
            if !paths.is_empty() || volume.owner() != &crate::conversation::VolumeOwner::Agent(
                binding.parent_scope.agent().ok_or_else(|| Error::Unauthorized("caller has no bound agent".into()))?)
                || binding.parent_scope.agent() == Some(arguments.id)
            {
                return Err(Error::Unauthorized("fork cannot substitute caller history/private identity".into()));
            }
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                let capability = volume.capability(operation)?;
                if binding.child.grants.contains(&capability) || binding.child_scope.capabilities().contains(&capability) {
                    return Err(Error::Unauthorized("default child cannot inherit broad parent-private authority".into()));
                }
            }
        }
    }
    for file in &binding.input_files {
        context.scope().limits().validate_file(file)?;
        let capability = file.read_capability()?;
        if !crate::runtime::read_granted(context.scope().grants(), file)?
            || !binding.child.grants.contains(&capability)
            || !binding.child_scope.capabilities().contains(&capability)
        {
            return Err(Error::Unauthorized("child input is not an actual donated exact-file read".into()));
        }
    }
    let selected = binding.child.input.pointer(&binding.child_message_pointer)
        .ok_or_else(|| Error::Invalid("child producer input does not retain fork msg".into()))?;
    if selected.as_str() == Some(arguments.msg.as_str()) {
        if binding.child_message_file_pointer.is_some() {
            return Err(Error::Invalid("inline child message has an immutable-file projection".into()));
        }
        return Ok(());
    }
    let file: FileRef = decode(selected)?;
    let command: WorkflowCommand = decode(&binding.child.input)?;
    command.validate()?;
    if command.payload != file || !binding.input_files.contains(&file) {
        return Err(Error::Unauthorized("child msg reference is not its exact admitted immutable command input".into()));
    }
    if !crate::runtime::read_granted(&binding.child.grants, &file)? {
        return Err(Error::Unauthorized("child cannot read its exact initial message".into()));
    }
    let bytes = context.read_file_measured(&file, finite_budget(binding.limits.work, binding.limits.bytes), cancellation)
        .await.map_err(|failure| failure.error)?.value;
    if let Some(pointer) = &binding.child_message_file_pointer {
        if pointer.is_empty() || !pointer.starts_with('/') {
            return Err(Error::Invalid("child immutable message pointer is invalid".into()));
        }
        let input: Value = crate::contract::json_from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        if input.pointer(pointer).and_then(Value::as_str) != Some(arguments.msg.as_str()) {
            return Err(Error::Unauthorized("child producer immutable message differs from original fork msg".into()));
        }
        return Ok(());
    }
    if command.kind != MODEL_TASK_COMMAND_KIND {
        return Err(Error::Invalid("non-model child input requires an explicit immutable message pointer".into()));
    }
    let model: super::ModelTaskCommand = crate::contract::json_from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    if !matches!(&model.input, crate::model::ModelContent::Text(message) if message == &arguments.msg) {
        return Err(Error::Unauthorized("child initial model message differs from original fork msg".into()));
    }
    Ok(())
}
fn validate_merge_binding<P, A, O>(context: &ToolContext, binding: &RecursiveMergeToolBinding<P, A, O>) -> Result<()>
where P: StreamProvider {
    binding.verifier.verify(&binding.scope)?;
    for (volume, operation) in [(&binding.project, VolumeOperation::Write), (&binding.source_project, VolumeOperation::Read)] {
        if !context.scope().grants().contains(&volume.capability(operation)?) {
            return Err(Error::Unauthorized("caller lacks exact selected merge source/destination grant".into()));
        }
        ContentGrant::verify(&binding.verifier, &binding.scope, volume, operation)?;
    }
    if binding.scope.agent() != binding.destination.reducer().conversation().and_then(|conversation| conversation.agent) {
        return Err(Error::Unauthorized("merge destination is not the actual caller conversation".into()));
    }
    Ok(())
}
fn check_merge_binding<P: StreamProvider, A, O>(plan: &RecursiveMergeToolPlan, binding: &RecursiveMergeToolBinding<P, A, O>) -> Result<()> {
    if plan.destination != *binding.destination.reducer().authority() || plan.source != binding.source
        || plan.source_project != binding.source_project || plan.source_generation != binding.source_generation
        || plan.target_project != binding.project || plan.notice != binding.notice {
        return Err(Error::Unauthorized("restored merge changed its original inspected binding".into()));
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ParentProjectController<'_, A, O> {
    /// Restores the exact originally inspected plan after a lost acknowledgement.
    /// This does not select a newer target. Filesystem's CAS and idempotency
    /// receipt remain authoritative even when the workspace head has advanced.
    pub async fn prepare_project_import_at(&self, source: &VolumeRef, source_generation: &GenerationRef,
        target_generation: &GenerationRef) -> Result<ParentMergePlan<A, O>> {
        self.require(crate::contract::capability::PROJECT_MERGE, VolumeOperation::Write)?;
        ContentGrant::verify(&self.verifier, &self.scope, source, VolumeOperation::Read)?;
        source.validate()?; self.host.validate_provider(source.provider())?;
        let source_workspace = self.host.open(&super::workspace_ref(self.host.provider.clone(), &source.storage_name()?)).await?;
        let target_workspace = self.host.open(&super::workspace_ref(self.host.provider.clone(), &self.project.storage_name()?)).await?;
        let source_generation = self.host.generation(&source_workspace, source_generation).await?;
        let target_generation = self.host.generation(&target_workspace, target_generation).await?;
        let plan = source_workspace.join_into(&target_workspace).plan_at(&source_generation, &target_generation).await.map_err(super::map_error)?;
        Ok(ParentMergePlan { plan, child_project: source.clone(), parent_project: self.project.clone(), parent_scope: self.scope.clone() })
    }
}

#[cfg(test)]
#[path = "default_tools_tests.rs"]
mod tests;
