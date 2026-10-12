//! Shared portable defaults.

use super::{
    FORK_PREPARE_TASK_COMMAND_KIND, FilesystemHost, FilesystemTaskRuntime, ForkPreparationLimits,
    ForkToolArguments, MODEL_TASK_COMMAND_KIND, ParentProjectController, RetainedSourceBinding,
    TASK_ADMIT_PREPARED_COMMAND_KIND, TaskCommandHost, TaskCommandProgress,
    fork_preparation_request,
};
use crate::runtime::TaskStateProvider;
use crate::store::history::HistoryIo as _;
use crate::{
    Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{ContentGrant, ConversationMessage, FileRef, VolumeOperation, VolumeRef},
    core::{Action, Authority, AuthorityVerifier, Command, Scope},
    durable_tool::{ResumableTool, ResumableToolRegistry},
    fork::{ForkReport, ForkRequest, ForkSeed},
    merge::ProjectMergeReceipt,
    resources::GenerationRef,
    runtime::{
        DurableTaskHost, RuntimeScope, TaskAdmissionRecord, TaskContext, ToolContext,
        task_id_for_admission,
    },
    store::StreamAggregate,
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
        WorkflowJournal,
    },
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ConflictSide, JoinOutcome,
};
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
const NATIVE_SOURCE_REQUIRED: Error = Error::NativeBoundary {
    code: crate::wire::ErrorCode::Unsupported,
    reason: "recursive source has no original native producer",
};

/// Real provider configuration for one exact caller and retained source cut.
pub trait RecursiveDefaultToolSource<P, A, O>: acyclic_stream::ProviderPlatform {
    /// Binds the actual caller/effect to its owner-controlled workflow journal.
    fn owner<'a>(
        &'a self,
        context: &'a ToolContext,
        tool: &'a ResumableTool,
        invocation: &'a ToolInvocation,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveToolOwner<P, A, O>>>;
    /// Authenticates the immutable source and allocation before staging.
    fn select<'a>(
        &'a self,
        context: &'a ToolContext,
        arguments: &'a ForkToolArguments,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveForkSourceIntent>>;
    /// Creates or restores the authenticated fork source.
    fn fork<'a>(
        &'a self,
        context: &'a ToolContext,
        arguments: &'a ForkToolArguments,
        intent: &'a RecursiveForkSourceIntent,
        original: Option<&'a RecursiveForkToolPlan>,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveForkToolBinding<P, A, O>>>;
    /// Binds the actual destination and explicitly readable selected source.
    fn merge<'a>(
        &'a self,
        context: &'a ToolContext,
        arguments: &'a MergeToolArguments,
        original: Option<&'a RecursiveMergeToolPlan>,
        cancellation: &'a CancellationToken,
        io: Option<&'a super::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, RecursiveMergeToolBinding<P, A, O>>>;
    /// Restores the exact retained source for an ordinary preparation worker.
    fn preparation<'a>(
        &'a self,
        context: &'a TaskContext,
        preparation: &'a crate::resource_preparation::TaskResourcePreparation,
        cancellation: &'a CancellationToken,
    ) -> BoxProviderFuture<'a, Result<RetainedSourceBinding<A, O>>>;

    /// True only when every recursive producer preserves the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_source(&self) -> bool {
        false
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_with_native_work<'a>(
        &'a self,
        _context: &'a Arc<crate::runtime::NativeTaskContext>,
        _preparation: &'a crate::resource_preparation::TaskResourcePreparation,
        _cancellation: &'a CancellationToken,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>,
    > {
        Err(NATIVE_SOURCE_REQUIRED)
    }

    /// Restores only the retained source for passive reconciliation.
    #[cfg(not(target_arch = "wasm32"))]
    fn preparation_for_reconciliation_with_native_work<'a>(
        &'a self,
        _context: &'a Arc<crate::runtime::NativeTaskContext>,
        _preparation: &'a crate::resource_preparation::TaskResourcePreparation,
        _cancellation: &'a CancellationToken,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)>,
    > {
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
pub enum MergeToolSide {
    Base,
    Target,
    Source,
}

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
    source: Option<Arc<dyn RecursiveDefaultToolSource<P, A, O>>>,
    maximum_request_bytes: u64,
) -> Result<RecursiveDefaultToolset>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    if maximum_request_bytes == 0 || maximum_request_bytes == u64::MAX {
        return Err(Error::Invalid(
            "recursive request bound must be finite and positive".into(),
        ));
    }
    let mut tools = ToolRegistry::default();
    tools.register((crate::tool::files::read_file()?, None), &mut None, None)?;
    tools.register((crate::tool::files::write_file()?, None), &mut None, None)?;
    tools.register((crate::tool::files::edit_file()?, None), &mut None, None)?;
    let mut resumable_tools = ResumableToolRegistry::default();
    for name in ["acyclic.fork", "acyclic.merge"] {
        let machine = Arc::new(RecursiveToolMachine::new(name, maximum_request_bytes));
        let definition = definition(name, maximum_request_bytes);
        let durable = ResumableTool {
            definition: definition.clone(),
            machine,
        };
        resumable_tools.register(durable.clone())?;
        tools.register((Tool {
            definition,
            executor: Arc::new(RecursiveExecutor {
                source: source.clone(),
                durable,
                maximum_request_bytes,
            }),
            projection: Arc::new(crate::tool::JsonToolProjection),
        }, None), &mut None, None)?;
    }
    Ok(RecursiveDefaultToolset {
        tools,
        resumable_tools,
    })
}

fn definition(name: &str, maximum: u64) -> ToolDefinition {
    let input = match name {
        "acyclic.fork" => json!({"type":"object","required":["id","msg"],"properties":{
            "id":{"type":"string"},"msg":{"type":"string"},"source":{"type":"string"}},"additionalProperties":false}),
        "acyclic.merge" => json!({"type":"object","required":["source"],"properties":{
            "source":{"type":"string"},"selections":{"type":"array","items":{"type":"object","required":["conflict","side"],"properties":{
                "conflict":{"type":"array","minItems":1,"items":{"type":"integer","minimum":0,"maximum":255}},"side":{"enum":["base","target","source"]}},"additionalProperties":false}}},"additionalProperties":false}),
        _ => unreachable!("registered recursive tool name"),
    };
    let mut input = input;
    input["x-harness-recursive-request-bytes"] = json!(maximum);
    let output = json!({"type":"object"});
    ToolDefinition { name: name.into(), revision: REVISION.into(),
        description: match name { "acyclic.fork" => "Prepare, publish and durably admit a new child from the actual calling task; source selects only an explicitly authorized project",
            _ => "Import an explicitly readable pinned source project into the actual caller's project through approval and the original inspected CAS target" }.into(),
        input_schema: input, output_schema: output.clone(),
        projection_schema: crate::tool::json_projection_schema(output) }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "plan",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(super) enum Plan {
    Fork(Box<RecursiveForkToolPlan>),
    Merge(Box<RecursiveMergeToolPlan>),
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
enum State {
    Ready {
        arguments: Value,
    },
    SourcePinned {
        arguments: Value,
        intent: RecursiveForkSourceIntent,
        command: WorkflowCommand,
    },
    Waiting {
        arguments: Value,
        plan: Plan,
        command: WorkflowCommand,
        report: Option<Box<ForkReport>>,
        receipt: Option<Box<ProjectMergeReceipt>>,
    },
    Complete {
        value: Value,
    },
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitialPlan {
    plan: Plan,
    command: WorkflowCommand,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePin {
    intent: RecursiveForkSourceIntent,
    command: WorkflowCommand,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    operation_id: OperationId,
    value: Outcome<Value>,
    next: Option<WorkflowCommand>,
}

struct RecursiveExecutor<P, A, O> {
    source: Option<Arc<dyn RecursiveDefaultToolSource<P, A, O>>>,
    durable: ResumableTool,
    maximum_request_bytes: u64,
}

pub(crate) struct InvocationCancellation(pub(crate) CancellationToken);
impl Drop for InvocationCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

mod codec;
mod dispatch;
mod executor;
mod fork;
mod input;
mod machine;
mod merge;
mod validation;

use codec::*;
use dispatch::*;
use machine::RecursiveToolMachine;
use validation::*;

#[cfg(test)]
#[path = "../default_tools_tests.rs"]
mod tests;
pub(super) use codec::decode;
pub(super) use codec::encode;
pub(super) use codec::recursive_box;
pub(super) use codec::recursive_command_outbox;
pub(super) use codec::recursive_decode_slot_bytes;
pub(super) use codec::recursive_reject;
pub(super) use codec::recursive_transition_owned_bytes;
pub(super) use codec::recursive_value_owned_bytes;
