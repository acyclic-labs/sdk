#![cfg_attr(
    test,
    allow(
        clippy::too_many_lines,
        clippy::indexing_slicing,
        reason = "adapter tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../../docs/filesystem.md")]

use crate::contract::capability;
use crate::{
    AgentId, Error, IdempotencyKey, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef,
        MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryEntry, PrivateDirectoryEntryKind,
        PrivateDirectoryPage, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        ForkPublicationGuard, ForkSeed, ForkSeedVerifier, InheritedConversationPrefix,
        ReferenceGrant, ResourceRevision,
    },
    merge::{ProjectMergeReceipt, ProjectMergeVerifier, ProviderJoinProof},
    resources::{GenerationRef, ProviderRef, WorkspaceRef},
};
use acyclic_fs::WorkCounters;
use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding};
use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ConflictSide, Digest, DirectoryPage, Fs,
    Generation, GenerationId, IdempotencyKey as FilesystemKey, JoinCommitWitness, JoinHistory,
    JoinOutcome, JoinPlan, MergeConflict, MergePlan, PublicationReservation, TransactionCommit,
    Workspace, WorkspaceError, WorkspaceStat,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};

const FILESYSTEM_JOIN_PROOF_FORMAT: &str = "acyclic.filesystem.join-commit.v2";

mod content_publisher;
pub use content_publisher::FilesystemContentPublisher;

mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod fork_preparer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::AllocationClaim;
use fork_preparer::bounded_io as fork_preparer_io;
pub use fork_preparer::bounded_io::PreparationIo;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::bounded_io::native_decoded;
pub(crate) use fork_preparer::bounded_io::{
    DecodeReservation, PreparationRecord, UploadRecord, preparation_slot_bytes, retain_decoded,
};
use fork_preparer_io::decode_record;
mod summary_fork;
pub use fork_preparer::{FilesystemForkPreparer, FilesystemResourceCreationProvider};
mod interaction_host;
pub use interaction_host::FilesystemInteractionHost;
mod project_workspaces;
pub use project_workspaces::FilesystemProjectWorkspaces;
mod workflow_journal;
pub use workflow_journal::FilesystemWorkflowJournal;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use workflow_journal::WorkflowReleaseWitness;
mod memory;
pub use memory::MemoryHarnessStorage;
mod text_tools;
#[cfg(not(target_arch = "wasm32"))]
pub use host::{
    bind_original_root_conversation_with_native_work, native_root_output_binding_operation,
};
pub use text_tools::ExactTextReplacement;
mod root_source;
pub use root_source::FsRootProjectSourceProvider;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_execution;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_execution::NativePreparationCompletionTemplate;
#[cfg(all(fuzzing, feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_execution::{fuzz_native_preparation_intent, fuzz_native_preparation_journal};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) mod native_process;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_process::{NativeProcessAuthority, NativeProcessObserver, NativeProcessProvider};
mod native_capture_bounds;
mod native_contract;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod native_directory;
pub use native_contract::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeViewManifest, NativeViewOptions, NativeVolumeMapping,
    NativeVolumeNamespace,
};
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use native_directory::NativeDirectory;
mod configured_recursive_tools;
mod default_tools;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) use default_tools::InvocationCancellation;
mod mailbox_machine;
mod recursive;
mod task_commands;
mod task_runtime;
pub use configured_recursive_tools::{
    ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller, ConfiguredRecursiveFork,
    ConfiguredRecursiveMerge, ConfiguredRecursiveToolSource, ConfiguredRecursiveToolsConfig,
};
pub use default_tools::{MergeToolArguments, MergeToolSelection, MergeToolSide, recursive_tools};
pub(in crate::filesystem) use default_tools::{
    RecursiveForkSourceIntent, RecursiveForkToolBinding, RecursiveForkToolPlan,
    RecursiveMergeToolBinding, RecursiveMergeToolPlan,
};
pub use mailbox_machine::MailboxModelMachine;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_execution::journal::preparation_owner;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_execution::{
    BorrowedNativeVolumeBinding, NativeMountedVolume, NativePreparationRoot,
    NativeViewPublicationError, NativeVolumeView,
};
pub use recursive::{
    ForkPreparationLimits, ForkPreparationMachine, ForkToolArguments, RecursiveForkConfig,
    RecursiveSourceSnapshot, RetainedSourceBinding, fork_preparation_request,
    recursive_fork_source, restore_recursive_fork_source,
};
pub use task_commands::{
    EFFECT_TASK_COMMAND_KIND, FORK_PREPARE_TASK_COMMAND_KIND, FilesystemTaskCommands,
    MAIL_RECEIVE_TASK_COMMAND_KIND, MAIL_SEND_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND,
    MailReceiveTaskCommand, MailSendTaskCommand, ModelTaskCommand, PreparedTaskAdmitCommand,
    TASK_ADMIT_COMMAND_KIND, TASK_ADMIT_PREPARED_COMMAND_KIND, TASK_CANCEL_COMMAND_KIND,
    TASK_INSPECT_COMMAND_KIND, TASK_JOIN_COMMAND_KIND, TASK_OBSERVE_COMMAND_KIND,
    TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskAdmitCommand, TaskCancelCommand,
    TaskInspectCommand, TaskJoinCommand, TaskObserveCommand, TimerTaskCommand, ToolTaskCommand,
};
pub use task_runtime::{
    FilesystemTaskExecution, FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress,
    TaskSourceInspection, TaskWakeCursor, TaskWakePage, TaskWorkerAttempt, TaskWorkerOutcome,
    TaskWorkerTick,
};

mod config;
use config::*;
pub use config::{FilesystemJournalBinding, WorkspaceMutation, WorkspaceObservation};
mod host;
pub(crate) use host::file_publication;
pub(crate) use host::map_error;
use host::*;
pub use host::{FilesystemHost, filesystem_key, workspace_ref};
mod content_verifier;
pub use content_verifier::FilesystemContentVerifier;
mod file_query;
#[cfg(not(target_arch = "wasm32"))]
use content_verifier::require_file_read;
pub use file_query::FilesystemFileQuery;
mod fork_verifier;
pub use fork_verifier::FilesystemForkVerifier;
mod project_verifier;
pub use project_verifier::FilesystemProjectMergeVerifier;
mod parent;
pub use parent::{InheritedContextCapture, ParentMergePlan, ParentProjectController};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    async fn content_for_operation_impl(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        key: &IdempotencyKey,
        expected_path: Option<&str>,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        grant.require(volume, VolumeOperation::Write, io)?;
        let observed = self
            .content_for_operation_record_io(volume, key, expected_path, maximum_bytes, None, io)
            .await?;
        let Some((file, bytes)) = observed else {
            return Ok(None);
        };
        let hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        drop(bytes);
        drop(hold);
        Ok(Some(file))
    }

    async fn read_pinned_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        // Keep the Result temporary across await for the original admitted caller frame.
        (if io.is_none() {
            crate::conversation::BoundedContentFuture::new(None, || async move {
                if reference.volume().provider() != &self.provider
                    || reference.descriptor().byte_length() > maximum_bytes
                {
                    return Err(Error::Invalid(
                        "content provider or size limit mismatch".into(),
                    ));
                }
                let generation = self.file_generation(reference, None)?;
                let workspace =
                    fork_preparer_io::content_workspace(&self.provider, reference.volume(), None)?;
                let path = format!("/{}", reference.path());
                let bytes = self
                    .read(&workspace, Some(&generation), &path, maximum_bytes, None)
                    .await?;
                reference.descriptor().verify(&bytes, &mut None)?;
                Ok(bytes)
            })
        } else {
            crate::conversation::BoundedContentFuture::new(io, || async move {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = io.and_then(PreparationIo::original_native_work) {
                    let scratch = PreparationIo::from_native_work(original)?;
                    let io = &scratch;
                    reference.validate(&mut Some(&mut io.history_io()))?;
                    if reference.volume().provider() != &self.provider
                        || reference.descriptor().byte_length() > maximum_bytes
                    {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "content provider or size limit mismatch",
                        });
                    }
                    let generation = self.file_generation(reference, Some(io))?;
                    let workspace = fork_preparer_io::content_workspace(
                        &self.provider,
                        reference.volume(),
                        Some(io),
                    )?;
                    let rendered = original
                        .display(&format_args!("/{}", reference.path()))
                        .map_err(|cause| Error::NativeWork {
                            cause,
                            custody: None,
                        })?;
                    let path_custody = rendered.1;
                    let path = rendered.0;
                    let opened = (self).open(&workspace, Some(io)).await?;
                    let pinned = (self).generation(&opened, &generation, Some(io)).await?;
                    #[cfg(all(test, not(target_arch = "wasm32")))]
                    acyclic_test_support::differential::record(("read", maximum_bytes));
                    let bytes = pinned
                        .read(&path, maximum_bytes, original, io.cancellation())
                        .await
                        .map_err(|source| Error::NativeWorkspace {
                            source: Box::new(source),
                            custody: None,
                        })?;
                    fork_preparer_io::retain(&pinned, Some(io)).await?;
                    crate::executor::with_history_io(Some(io), |work| {
                        reference.descriptor().verify(&bytes, work)
                    })?;
                    drop(path);
                    drop(path_custody);
                    return Ok(bytes);
                }
                if reference.volume().provider() != &self.provider
                    || reference.descriptor().byte_length() > maximum_bytes
                {
                    return Err(Error::Invalid(
                        "content provider or size limit mismatch".into(),
                    ));
                }
                if let Some(io) = io {
                    io.retain_owned((reference.path().len() + 1) as u64)?;
                }
                let _path_hold = io
                    .map(|io| io.guard_owned((reference.path().len() + 1) as u64))
                    .transpose()?;
                let generation = self.file_generation(reference, io)?;
                let generation_hold = io
                    .map(|io| io.guard_owned(generation.logical_owned_bytes()?))
                    .transpose()?;
                let workspace =
                    fork_preparer_io::content_workspace(&self.provider, reference.volume(), io)?;
                let _workspace_hold = io
                    .map(|io| io.guard_owned(workspace.logical_owned_bytes()?))
                    .transpose()?;
                let path = format!("/{}", reference.path());
                let bytes = match io {
                    Some(io) => {
                        let opened = (self).open(&workspace, Some(io)).await?;
                        let pinned = (self).generation(&opened, &generation, Some(io)).await?;
                        #[cfg(all(test, not(target_arch = "wasm32")))]
                        acyclic_test_support::differential::record(("read", maximum_bytes));
                        let bytes = io.receive(
                            pinned
                                .read(&path, maximum_bytes, io.remaining()?, io.cancellation())
                                .await,
                        )?;
                        let capacity = bytes.retained_capacity().ok_or_else(|| {
                            stock_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Unsupported,
                                "content producer does not expose its owned capacity",
                            )
                        })?;
                        io.retain_owned(capacity as u64)?;
                        fork_preparer_io::retain(&pinned, Some(io)).await?;
                        bytes
                    }
                    None => {
                        self.read(&workspace, Some(&generation), &path, maximum_bytes, None)
                            .await?
                    }
                };
                crate::executor::with_history_io(io, |work| {
                    reference.descriptor().verify(&bytes, work)
                })?;
                drop(generation);
                drop(generation_hold);
                Ok(bytes)
            })
        })?
        .await
    }

    async fn retain_file_generation(&self, file: &FileRef) -> Result<()> {
        file.validate(&mut None)?;
        if file.volume().provider() != &self.provider {
            return Err(Error::Unauthorized(
                "file belongs to another Filesystem provider".into(),
            ));
        }
        let workspace = workspace_ref(
            self.provider.clone(),
            &file.volume().storage_name(&mut None)?,
        )?;
        self.retain_generation(&workspace, &self.file_generation(file, None)?)
            .await
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod pinned_read_tests;

mod stock_turn;
pub use stock_turn::StockTurnMachine;
pub(crate) mod child_agent;
pub use child_agent::ChildAgentMachine;

use crate::executor::{stock_boundary, stock_string_copy};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) mod workspace_policy;

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub(crate) mod workspace_publication;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use workspace_publication::{
    CheckoutRoot, WorkspacePublicationCoordinator, WorkspacePublicationPublisher,
    WorkspaceRootMaterializer, workspace_publication_coordinator,
};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod agent_publication;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use agent_publication::{plan_agent_merge, record_agent_publication};

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod checkout_publication;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
pub use checkout_publication::CheckoutPublication;
