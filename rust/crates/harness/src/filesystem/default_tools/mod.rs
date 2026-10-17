//! Portable recursive tools over the configured source and existing workflow host.
use super::{
    ConfiguredRecursiveToolSource, FORK_PREPARE_TASK_COMMAND_KIND, FilesystemHost,
    FilesystemTaskRuntime, ForkPreparationLimits, ForkToolArguments, MODEL_TASK_COMMAND_KIND,
    ParentProjectController, RetainedSourceBinding, fork_preparation_request,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::workflow::{MachineStatus, MachineTransition};
use crate::{
    Capabilities, Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::{ConversationMessage, FileRef, VolumeOperation, VolumeRef},
    core::{Action, Authority, AuthorityVerifier, Command, Scope},
    durable_host::TaskJournalOwner,
    fork::{ForkReport, ForkRequest, ForkSeed},
    merge::ProjectMergeReceipt,
    resources::GenerationRef,
    runtime::{RuntimeScope, TaskAdmissionRecord, ToolContext, task_id_for_admission},
    store::{StreamAggregate, history::HistoryIo as _},
    tool::{Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolRegistry, ToolResult},
    workflow::WorkflowCommand,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, ConflictSide, JoinOutcome,
};
use acyclic_stream::{BoxProviderFuture, StreamProvider};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

type RecursiveOwner<P, A, O> = (
    Arc<FilesystemTaskRuntime<P, A, O>>,
    Arc<TaskJournalOwner<P>>,
);

const REVISION: &str = "portable-recursive-5";

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

/// Registers the configured durable fork and merge operations.
pub fn recursive_tools<
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    source: Option<Arc<ConfiguredRecursiveToolSource<P, A, O>>>,
    maximum_request_bytes: u64,
) -> Result<ToolRegistry> {
    if maximum_request_bytes == 0 || maximum_request_bytes == u64::MAX {
        return Err(Error::Invalid(
            "recursive request bound must be finite and positive".into(),
        ));
    }
    let mut tools = ToolRegistry::default();
    for (name, source) in [("acyclic.fork", source.clone()), ("acyclic.merge", source)] {
        let definition = definition(name, maximum_request_bytes);
        tools.register(
            (
                Tool {
                    hook_names: &[],
                    definition: definition.clone(),
                    executor: Arc::new(RecursiveExecutor {
                        source,
                        definition,
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
    Ok(tools)
}

fn definition(name: &str, maximum: u64) -> ToolDefinition {
    let input = match name {
        "acyclic.fork" => json!({"type":"object","required":["id","msg"],"properties":{
            "id":{"type":"string"},"msg":{"type":"string"},"source":{"type":"string"}},"additionalProperties":false,"x-harness-recursive-request-bytes":maximum}),
        "acyclic.merge" => json!({"type":"object","required":["source"],"properties":{
            "source":{"type":"string"},"selections":{"type":"array","items":{"type":"object","required":["conflict","side"],"properties":{
                "conflict":{"type":"array","minItems":1,"items":{"type":"integer","minimum":0,"maximum":255}},"side":{"enum":["base","target","source"]}},"additionalProperties":false}}},"additionalProperties":false,"x-harness-recursive-request-bytes":maximum}),
        _ => unreachable!("registered recursive tool"),
    };
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
struct RecursiveExecutor<P, A, O> {
    source: Option<Arc<ConfiguredRecursiveToolSource<P, A, O>>>,
    definition: ToolDefinition,
    maximum_request_bytes: u64,
}

pub(crate) struct InvocationCancellation(pub(crate) CancellationToken);
impl Drop for InvocationCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
mod codec;
mod executor;
mod fork;
mod input;
mod merge;
mod validation;
use codec::*;
use validation::*;
#[cfg(test)]
#[path = "../default_tools_tests.rs"]
mod tests;
pub(super) use codec::{
    decode, encode, recursive_box, recursive_decode_slot_bytes, recursive_reject,
};
#[cfg(not(target_arch = "wasm32"))]
pub(super) use codec::{recursive_transition_owned_bytes, recursive_value_owned_bytes};
