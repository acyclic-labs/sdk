//! Portable recursive tools over the configured source and existing workflow host.
use super::{
    ConfiguredRecursiveToolSource, FORK_PREPARE_TASK_COMMAND_KIND, FilesystemHost,
    FilesystemTaskRuntime, FilesystemWorkflowJournal, ForkPreparationLimits, ForkToolArguments,
    MODEL_TASK_COMMAND_KIND, ParentProjectController, RetainedSourceBinding,
    TASK_ADMIT_PREPARED_COMMAND_KIND, TaskCommandHost, TaskCommandProgress,
    fork_preparation_request,
};
use crate::{
    Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{ContentGrant, ConversationMessage, FileRef, VolumeOperation, VolumeRef},
    core::{Action, Authority, AuthorityVerifier, Command, Scope},
    durable_tool::{ResumableTool, ResumableToolRegistry},
    fork::{ForkReport, ForkRequest, ForkSeed},
    merge::ProjectMergeReceipt,
    resources::GenerationRef,
    runtime::{
        RuntimeScope, TaskAdmissionRecord, ToolContext,
        task_id_for_admission,
    },
    store::{StreamAggregate, history::HistoryIo as _},
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
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
const REVISION: &str = "portable-recursive-3";

/// The same tools registered for ordinary and resumable callers.
pub struct RecursiveDefaultToolset {
    /// Ordinary tool registrations.
    pub tools: ToolRegistry,
    /// Resumable tool registrations.
    pub resumable_tools: ResumableToolRegistry,
}

/// Selects an explicitly readable project and exact conflict sides.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeToolArguments {
    /// Selected source task.
    pub source: TaskId,
    /// Exact provider conflict selections.
    #[serde(default)]
    pub selections: Vec<MergeToolSelection>,
}

/// Chooses one immutable side of a provider conflict.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeToolSelection {
    /// Opaque provider conflict key.
    pub conflict: Vec<u8>,
    /// Selected immutable side.
    pub side: MergeToolSide,
}

/// Immutable sides of the inspected join.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeToolSide {
    /// Common ancestor.
    Base,
    /// Destination.
    Target,
    /// Source.
    Source,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::filesystem) struct RecursiveForkSourceIntent {
    pub arguments: ForkToolArguments,
    pub caller: TaskId,
    pub source_task: TaskId,
    pub child_operation_id: OperationId,
    pub allocation_digest: [u8; 32],
    pub request: ForkRequest,
}

pub(in crate::filesystem) struct RecursiveForkToolBinding<P, A, O> {
    pub source: RetainedSourceBinding<A, O>,
    pub child: TaskAdmissionRecord,
    pub source_checkpoint: FileRef,
    pub child_message_pointer: String,
    pub child_message_file_pointer: Option<String>,
    pub input_files: Vec<FileRef>,
    pub parent: StreamAggregate<P>,
    pub child_conversation: StreamAggregate<P>,
    pub parent_scope: Scope,
    pub child_scope: Scope,
    pub limits: ForkPreparationLimits,
}

pub(in crate::filesystem) struct RecursiveMergeToolBinding<P, A, O> {
    pub filesystem: Arc<FilesystemHost<A, O>>,
    pub destination: StreamAggregate<P>,
    pub verifier: AuthorityVerifier,
    pub scope: Scope,
    pub project: VolumeRef,
    pub source: Authority,
    pub source_project: VolumeRef,
    pub source_generation: GenerationRef,
    pub source_read: Option<crate::source_authorization::ProjectSourceReadHandle>,
    pub notice: ConversationMessage,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::filesystem) struct RecursiveForkToolPlan {
    pub arguments: ForkToolArguments,
    pub caller: TaskId,
    pub request: crate::resource_preparation::ResourcePreparationRequest,
    pub preparation: TaskAdmissionRecord,
    pub child_message_pointer: String,
    pub child_message_file_pointer: Option<String>,
    pub allocation_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::filesystem) struct RecursiveMergeToolPlan {
    pub arguments: MergeToolArguments,
    pub caller: TaskId,
    pub operation_id: OperationId,
    pub destination: Authority,
    pub source: Authority,
    pub source_project: VolumeRef,
    pub source_generation: GenerationRef,
    pub authorized_source: Option<crate::source_authorization::ProjectSourceAuthorization>,
    pub target_project: VolumeRef,
    pub expected_target_generation: GenerationRef,
    pub notice: ConversationMessage,
}

/// Registers shared file tools and the configured durable fork/merge operations.
pub fn recursive_default_tools<
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    source: Option<Arc<ConfiguredRecursiveToolSource<P, A, O>>>,
    maximum_request_bytes: u64,
) -> Result<RecursiveDefaultToolset> {
    if maximum_request_bytes == 0 || maximum_request_bytes == u64::MAX {
        return Err(Error::Invalid(
            "recursive request bound must be finite and positive".into(),
        ));
    }
    let mut tools = ToolRegistry::default();
    for tool in [
        crate::tool::files::read_file()?,
        crate::tool::files::write_file()?,
        crate::tool::files::edit_file()?,
    ] {
        tools.register((tool, None), &mut None, None)?;
    }
    let mut resumable_tools = ResumableToolRegistry::default();
    for name in ["acyclic.fork", "acyclic.merge"] {
        let durable = ResumableTool {
            definition: definition(name, maximum_request_bytes),
            machine: Arc::new(RecursiveToolMachine::new(name, maximum_request_bytes)),
        };
        resumable_tools.register(durable.clone())?;
        tools.register(
            (
                Tool {
                    definition: durable.definition.clone(),
                    executor: Arc::new(RecursiveExecutor {
                        source: source.clone(),
                        durable,
                        maximum_request_bytes,
                    }),
                    projection: Arc::new(crate::tool::JsonToolProjection),
                },
                None,
            ),
            &mut None,
            None,
        )?;
    }
    Ok(RecursiveDefaultToolset {
        tools,
        resumable_tools,
    })
}

fn definition(name: &str, maximum: u64) -> ToolDefinition {
    let mut input = match name {
        "acyclic.fork" => json!({"type":"object","required":["id","msg"],"properties":{
            "id":{"type":"string"},"msg":{"type":"string"},"source":{"type":"string"}},"additionalProperties":false}),
        "acyclic.merge" => json!({"type":"object","required":["source"],"properties":{
            "source":{"type":"string"},"selections":{"type":"array","items":{"type":"object","required":["conflict","side"],"properties":{
                "conflict":{"type":"array","minItems":1,"items":{"type":"integer","minimum":0,"maximum":255}},"side":{"enum":["base","target","source"]}},"additionalProperties":false}}},"additionalProperties":false}),
        _ => unreachable!("registered recursive tool"),
    };
    input["x-harness-recursive-request-bytes"] = json!(maximum);
    let output = json!({"type":"object"});
    ToolDefinition { name: name.into(), revision: REVISION.into(),
        description: match name { "acyclic.fork" => "Prepare, publish and durably admit a new child from the actual calling task; source selects only an explicitly authorized project",
            _ => "Import an explicitly readable pinned source project into the actual caller's project through approval and the original inspected CAS target" }.into(),
        input_schema: input, output_schema: output.clone(),
        projection_schema: crate::tool::json_projection_schema(output) }
}

#[derive(Serialize, Deserialize)]
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
        intent: RecursiveForkSourceIntent,
        command: WorkflowCommand,
    },
    Waiting {
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
struct Receipt {
    operation_id: OperationId,
    value: Outcome<Value>,
    next: Option<WorkflowCommand>,
}
struct RecursiveExecutor<P, A, O> {
    source: Option<Arc<ConfiguredRecursiveToolSource<P, A, O>>>,
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
pub(super) use codec::{
    decode, encode, recursive_box, recursive_decode_slot_bytes, recursive_reject,
    recursive_transition_owned_bytes, recursive_value_owned_bytes,
};
