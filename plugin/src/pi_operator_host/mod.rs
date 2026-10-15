//! Ordinary interaction resolution and original selected writeback dispatch.
use crate::{
    PiTaskBinding,
    pi_fork::{derived, original_operation},
    pi_host::{denied, ready},
    pi_native_host::PiNativeFilesystem,
    pi_operator::{
        self, PiOperatorBody, PiOperatorDecision, PiOperatorRequest, PiPreparedApproval,
    },
    pi_owner::PiOwnerConfig,
    pi_runtime::PiTaskRuntime,
};
use acyclic_fs::{MaterializationPhase, NativeMaterializationWork};
use acyclic_harness::{
    Capabilities, Error, OperationId, Result,
    core::SchemaRegistry,
    durable_host::TaskJournalOwner,
    executor::ExecutionJournal,
    filesystem::{FilesystemExecutionJournal, PreparationIo, TaskCommandProgress},
    interaction::{Interaction, InteractionOutcome, InteractionResponse},
    runtime::TaskContext,
    scheduler::LeaseFence,
    tool::ToolApprovalVerifier,
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;

type Journal = FilesystemExecutionJournal<
    LocalStream,
    crate::LocalAuthorityBackend,
    crate::LocalObjectBackend,
>;
pub(crate) const PI_OPERATOR_COMMAND_KIND: &str = "acyclic.pi.operator-approval";

impl<'a> From<&'a crate::pi_selected_host::PiSelectedCommand>
    for crate::pi_selected_host::PiSelectedCommand<&'a str, &'a [String]>
{
    fn from(command: &'a crate::pi_selected_host::PiSelectedCommand) -> Self {
        Self {
            caller: command.caller,
            sdk_operation: &command.sdk_operation,
            source: command.source,
            root: command.root.as_deref(),
            directory: &command.directory,
            cursor: command.cursor.as_deref(),
            maximum_entries: command.maximum_entries,
            destination: command.destination,
            paths: &command.paths,
            confirm: command.confirm,
            approval_digest: command.approval_digest.as_deref(),
        }
    }
}
impl<'a> crate::pi_selected_host::PiSelectedCommand<&'a str, &'a [String]> {
    pub(crate) fn normalized(
        command: &'a crate::pi_selected_host::PiSelectedCommand,
        root: &'a str,
    ) -> Self {
        Self {
            root: Some(root),
            approval_digest: None,
            ..Self::from(command)
        }
    }
}
impl<'a> From<&'a PiPreparedApproval>
    for PiPreparedApproval<
        &'a str,
        &'a acyclic_harness::conversation::VolumeRef,
        &'a [String],
        &'a acyclic_fs::MaterializationJournal,
    >
{
    fn from(descriptor: &'a PiPreparedApproval) -> Self {
        Self {
            version: descriptor.version,
            session: &descriptor.session,
            command: (&descriptor.command).into(),
            operation: descriptor.operation,
            approval_digest: &descriptor.approval_digest,
            private_volume: &descriptor.private_volume,
            source_snapshot: descriptor.source_snapshot,
            selected_snapshot: descriptor.selected_snapshot,
            imported: descriptor.imported,
            journal: &descriptor.journal,
            canonical_digest_input: &descriptor.canonical_digest_input,
            root_path: &descriptor.root_path,
            source_path: &descriptor.source_path,
        }
    }
}
type ApprovalRef<'a> = PiPreparedApproval<
    &'a str,
    &'a acyclic_harness::conversation::VolumeRef,
    &'a [String],
    &'a acyclic_fs::MaterializationJournal,
>;
mod approval;
mod dispatch;
mod inspection;
mod status;
use approval::conversation_journal;
pub(crate) use approval::{
    open_prepared_approval, read_prepared_approval, require_resolved_approval,
};
pub(crate) use inspection::{inspect_operator, stage_operator_command, validate_approval_request};
pub(crate) use status::passive_status;
use status::{PiInspectionResponse, PiPendingResponse, observe_writeback, status_response};
