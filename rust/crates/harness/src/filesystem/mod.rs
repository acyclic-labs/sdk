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
    distributed::SchedulerPayloadStore,
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
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ConflictSide, Digest, DirectoryPage,
    ForkOptions, Fs, Generation, GenerationId, IdempotencyKey as FilesystemKey, JoinCommitWitness,
    JoinHistory, JoinOutcome, JoinPlan, MergeConflict, MergePlan, PublicationReservation,
    TransactionCommit, Workspace, WorkspaceError, WorkspaceStat,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
};

const FILESYSTEM_JOIN_PROOF_FORMAT: &str = "acyclic.filesystem.join-commit.v2";

mod content_publisher;
pub use content_publisher::FilesystemContentPublisher;

mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod fork_preparer;
pub(crate) use fork_preparer::AllocationClaim;
use fork_preparer::bounded_io as fork_preparer_io;
pub use fork_preparer::bounded_io::PreparationIo;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::bounded_io::native_decoded;
pub(crate) use fork_preparer::bounded_io::{
    DecodeReservation, PreparationRecord, UploadRecord, preparation_slot_bytes, retain_decoded,
};
mod summary_fork;
pub use fork_preparer::{
    FilesystemForkPreparer, FilesystemResourceCreationProvider, FilesystemRootBootstrapAllocation,
};
mod interaction_host;
pub use interaction_host::FilesystemInteractionHost;
mod project_workspaces;
pub use project_workspaces::FilesystemProjectWorkspaces;
mod workflow_journal;
pub use workflow_journal::FilesystemWorkflowJournal;
pub(crate) use workflow_journal::WorkflowReleaseWitness;
mod memory;
pub use memory::MemoryHarnessStorage;
mod text_tools;
pub use text_tools::ExactTextReplacement;
#[cfg(not(target_arch = "wasm32"))]
mod root_binding;
#[cfg(not(target_arch = "wasm32"))]
pub use root_binding::{
    bind_original_root_conversation_with_native_work, native_root_output_binding_operation,
};
mod root_source;
pub use root_source::{FsRootNativeSource, FsRootProjectSourceProvider};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_execution;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_execution::NativePreparationCompletionTemplate;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_process;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_process::{
    NativeProcessAuthority, NativeProcessDrainProof, NativeProcessProvider,
    NativeProcessPublishedVolume, NativeProcessReceipt,
};
mod native_capture_bounds;
mod native_contract;
pub use native_contract::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeViewManifest, NativeViewOptions, NativeVolumeMapping,
    NativeVolumeNamespace,
};
mod configured_recursive_tools;
mod default_tools;
mod mailbox_machine;
mod recursive;
mod task_commands;
mod task_runtime;
pub use configured_recursive_tools::{
    ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller, ConfiguredRecursiveFork,
    ConfiguredRecursiveMerge, ConfiguredRecursiveSourceCheckpoint, ConfiguredRecursiveToolSource,
    ConfiguredRecursiveToolsConfig,
};
pub(crate) use default_tools::InvocationCancellation;
pub use default_tools::{
    MergeToolArguments, MergeToolSelection, MergeToolSide, RecursiveDefaultToolSource,
    RecursiveDefaultToolset, RecursiveForkSourceIntent, RecursiveForkToolBinding,
    RecursiveForkToolPlan, RecursiveMergeToolBinding, RecursiveMergeToolPlan, RecursiveToolOwner,
    recursive_default_tools,
};
pub use mailbox_machine::MailboxModelMachine;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_execution::{
    BorrowedNativeVolumeBinding, NativeExistingLazyVolume, NativePreparationReceipt,
    NativePreparationRoot, NativeProcessView, NativePublicationCleanup, NativePublicationFailure,
    NativePublicationReservationFailure, NativeRecordedPreparation, NativeRootOutputSource,
    NativeTaskViewDrainReceipt, NativeTaskWorkingView, NativeViewPublicationError,
    NativeViewPublicationReceipt, NativeVolumeView, copy_native_mapping_with_work,
    observe_preparation,
};
#[cfg(not(target_arch = "wasm32"))]
pub use recursive::restore_recursive_fork_source_with_native_work;
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
    TASK_OBSERVE_COMMAND_KIND, TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskAdmitCommand,
    TaskCancelCommand, TaskObserveCommand, TimerTaskCommand, ToolTaskCommand,
};
pub use task_runtime::{
    FilesystemTaskExecution, FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress,
    TaskSourceInspection, TaskWakeCursor, TaskWakePage, TaskWorkerAttempt, TaskWorkerOutcome,
    TaskWorkerTick,
};

mod config;
use config::*;
pub use config::{WorkspaceMutation, WorkspaceObservation};
mod host;
pub(crate) use host::map_error;
use host::*;
pub use host::{FilesystemHost, workspace_ref};
mod scheduler_payload;
pub use scheduler_payload::FilesystemSchedulerPayloadStore;
mod content_verifier;
pub use content_verifier::FilesystemContentVerifier;
use content_verifier::*;
mod fork_verifier;
pub use fork_verifier::FilesystemForkVerifier;
use fork_verifier::*;
mod project_verifier;
pub use project_verifier::FilesystemProjectMergeVerifier;
mod parent;
pub use parent::{
    InheritedContextCapture, ParentMergePlan, ParentProjectController, VolumeImportPlan,
};

#[cfg(not(target_arch = "wasm32"))]
mod source_publication;
#[cfg(not(target_arch = "wasm32"))]
use source_publication::SourcePublicationFailureSlot;

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
        self.content_for_operation_record_io(volume, key, expected_path, maximum_bytes, None, io)
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one implementation pins the complete staged file contract"
    )]
    async fn put_content_impl(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        internal_class: Option<InternalContentClass>,
        expected_generation: Option<&GenerationRef>,
        io: Option<&fork_preparer_io::PreparationIo>,
        publication_lease: Option<&Arc<dyn crate::resource_preparation::CreatedTaskLease>>,
        root_allocation: Option<&fork_preparer::FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        if root_allocation.is_some_and(|allocation| &allocation.request.volume != volume) {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "Root publication allocation names another original volume",
            ));
        }
        #[cfg(target_arch = "wasm32")]
        if publication_lease.is_some() {
            return Err(Error::Unsupported(
                "Root publication lease is native-platform only".into(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(lease) = publication_lease {
            match io {
                Some(io) => io.verify_publication_lease(lease.as_ref()).await?,
                None => lease.verify().await?,
            }
        }
        grant.require(volume, VolumeOperation::Write, io)?;
        let publication_permit = acyclic_fs::PublicationPermit::Unrestricted;
        if volume.provider() != &self.provider || bytes.len() as u64 > maximum_bytes {
            return Err(fork_preparer_io::failure(
                io,
                crate::wire::ErrorCode::Invalid,
                "content provider or size limit mismatch",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            crate::conversation::validate_content_path(path, work)
        })?;
        let descriptor = crate::executor::with_history_io(io, |work| {
            FileDescriptor::from_bytes(bytes, media_type, work)
        })?;
        let provisional = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                volume.copy(work)?,
                fork_preparer_io::clone_text(path, io)?,
                fork_preparer_io::clone_text("pending", io)?,
                FileDescriptor::new(
                    *descriptor.sha256(),
                    descriptor.byte_length(),
                    fork_preparer_io::clone_text(descriptor.media_type(), io)?,
                )?,
                fork_preparer_io::clone_text(display_name, io)?,
                work,
            )
        })?;
        match internal_class {
            Some(class)
                if volume.class() == VolumeClass::AgentPrivate
                    && provisional.path().starts_with(class.prefix()) => {}
            Some(_) => {
                #[cfg(not(target_arch = "wasm32"))]
                if io.and_then(PreparationIo::original_native_work).is_some() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "journal path or volume is not assigned to this host",
                    });
                }
                return Err(Error::Unauthorized(
                    "journal path or volume is not assigned to this host".into(),
                ));
            }
            None if provisional.path() == ".system"
                || provisional.path().starts_with(".system/")
                || provisional.path() == "turns/.canonical"
                || provisional.path().starts_with("turns/.canonical/") =>
            {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "internal storage paths are reserved",
                ));
            }
            None => {}
        }
        let receipt_identity = if internal_class.is_some() {
            provisional.path()
        } else {
            idempotency_key.as_str()
        };
        let receipt_path =
            content_identity_path_io(receipt_identity, false, ".system/harness-uploads/", io)?;
        let metadata_path = content_metadata_path_io(provisional.path(), false, io)?;
        let metadata_bytes = match io {
            Some(io) => io.encode(&provisional, io.remaining()?.bytes_encoded, false)?,
            None => acyclic_stream::OwnedBytes::from(
                serde_json::to_vec(&provisional)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            ),
        };
        let receipt_bytes = match io {
            Some(io) => io.encode(
                &(&provisional, expected_generation),
                io.remaining()?.bytes_encoded,
                true,
            )?,
            None => acyclic_stream::OwnedBytes::from(crate::contract::canonical_json_bytes(&(
                &provisional,
                expected_generation,
            ))?),
        };
        let reconcile_source = root_allocation.is_some();
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let prior = if reconcile_source {
            None
        } else {
            let opened = match io {
                Some(io) => fork_preparer_io::open(self, &workspace, io).await?,
                None => self.open(&workspace).await?,
            };
            match io {
                Some(io) => {
                    let hash = acyclic_fs::WorkCounters {
                        bytes_hashed: idempotency_key.as_str().len() as u64,
                        ..Default::default()
                    };
                    io.preflight(hash)?;
                    let key = filesystem_key(idempotency_key);
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(original) = io.original_native_work() {
                        original.spent(hash).map_err(|cause| Error::NativeWork {
                            cause,
                            custody: None,
                        })?;
                        opened
                            .operation_generation(key, original, io.cancellation())
                            .await
                            .map_err(|source| Error::NativeWorkspace {
                                source,
                                custody: None,
                            })?
                    } else {
                        io.charge(hash)?;
                        io.receive(
                            opened
                                .operation_generation(key, io.remaining()?, io.cancellation())
                                .await,
                        )?
                    }
                    #[cfg(target_arch = "wasm32")]
                    {
                        io.charge(hash)?;
                        io.receive(
                            opened
                                .operation_generation(key, io.remaining()?, io.cancellation())
                                .await,
                        )?
                    }
                }
                None => opened
                    .operation_generation(
                        filesystem_key(idempotency_key),
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| map_error(failure.error))?,
            }
        };
        if let Some(prior) = prior {
            let generation = fork_preparer_io::generation_ref(&self.provider, prior.id(), io)?;
            match io {
                Some(io) => fork_preparer_io::retain(&prior, Some(io)).await?,
                None => self.retain_generation(&workspace, &generation).await?,
            }
            let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
            let receipt_descriptor = crate::executor::with_history_io(io, |work| {
                FileDescriptor::from_bytes(&receipt_bytes, "application/json", work)
            })?;
            let receipt = crate::executor::with_history_io(io, |work| {
                FileRef::new(
                    volume.copy(work)?,
                    fork_preparer_io::clone_text(&receipt_path, io)?,
                    fork_preparer_io::clone_text(&version, io)?,
                    receipt_descriptor,
                    fork_preparer_io::clone_text("receipt.json", io)?,
                    work,
                )
            })?;
            self.read_pinned_io(&receipt, receipt_bytes.len() as u64, io)
                .await?;
            let indexed = match io {
                Some(io) => {
                    let indexed_path = crate::executor::with_history_io(Some(io), |work| crate::contract::preparation_format(format_args!("/{metadata_path}"), work))?;
                    read_generation(&prior, &indexed_path, metadata_bytes.len() as u64, Some(io))
                        .await?
                }
                None => {
                    self.read(
                        &workspace,
                        Some(&generation),
                        &format!("/{metadata_path}"),
                        metadata_bytes.len() as u64,
                    )
                    .await?
                }
            };
            if indexed.as_ref() != metadata_bytes.as_slice() {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "upload metadata index does not match",
                ));
            }
            let reference = {
                let io = io;
                let descriptor = &descriptor;
                crate::executor::with_history_io(io, |work| {
                    crate::conversation::FileRef::new(
                        (volume).copy(work)?,
                        crate::filesystem::fork_preparer_io::clone_text(path, io)?,
                        crate::filesystem::fork_preparer_io::clone_text(&version, io)?,
                        crate::conversation::FileDescriptor::new(
                            *descriptor.sha256(),
                            descriptor.byte_length(),
                            crate::filesystem::fork_preparer_io::clone_text(
                                descriptor.media_type(),
                                io,
                            )?,
                        )?,
                        crate::filesystem::fork_preparer_io::clone_text(display_name, io)?,
                        work,
                    )
                })
            }?;
            let committed = self.read_pinned_io(&reference, maximum_bytes, io).await?;
            if committed.as_ref() != bytes {
                return Err(fork_preparer_io::failure(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "upload identity belongs to different content",
                ));
            }
            return Ok(reference);
        }
        let absolute =
            crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("/{}", provisional.path()), work))?;
        let mut mutations = fork_preparer_io::vector(6, io)?;
        if let Some((parent, _)) = absolute.rsplit_once('/')
            && !parent.is_empty()
        {
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: fork_preparer_io::clone_text(parent, io)?,
            });
        }
        let owned_bytes = fork_preparer_io::clone_bytes(bytes, io)?;
        mutations.push(WorkspaceMutation::PutFile {
            path: absolute,
            bytes: owned_bytes,
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: fork_preparer_io::clone_text("/.system/harness-uploads", io)?,
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: fork_preparer_io::clone_text("/.system/harness-file-metadata", io)?,
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("/{metadata_path}"), work))?,
            bytes: metadata_bytes,
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: crate::executor::with_history_io(io, |work| crate::contract::preparation_format(format_args!("/{receipt_path}"), work))?,
            bytes: receipt_bytes,
        });
        let publication = async {
            let generation = match io {
                Some(io) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(lease) = publication_lease {
                        if io.original_native_work().is_some() {
                            let lease = Arc::clone(lease);
                            let mut before_publication =
                                move |original: &acyclic_fs::NativeMaterializationWork| {
                                    let lease = Arc::clone(&lease);
                                    let original = original.clone();
                                    async move { lease.verify_with_native_work(&original)?.await }
                                };
                            fork_preparer_io::publish_native(
                                self,
                                &workspace,
                                expected_generation,
                                &mutations,
                                idempotency_key,
                                io,
                                &mut before_publication,
                            )
                            .await?
                        } else {
                            let mut before_publication = |budget| async move {
                                let callback =
                                    PreparationIo::from_budget(budget, io.cancellation())
                                        .map_err(acyclic_fs::OperationFailure::before_work)?;
                                let result =
                                    callback.verify_publication_lease(lease.as_ref()).await;
                                callback.finish(result)
                            };
                            fork_preparer_io::publish_generation(
                                self,
                                &workspace,
                                expected_generation,
                                &mutations,
                                idempotency_key,
                                Some(io),
                                &mut before_publication,
                            )
                            .await?
                        }
                    } else {
                        fork_preparer_io::apply_generation(
                            self,
                            &workspace,
                            expected_generation,
                            &mutations,
                            idempotency_key,
                            io,
                        )
                        .await?
                    }
                    #[cfg(target_arch = "wasm32")]
                    fork_preparer_io::apply_generation(
                        self,
                        &workspace,
                        expected_generation,
                        &mutations,
                        idempotency_key,
                        io,
                    )
                    .await?
                }
                None => {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(lease) = publication_lease {
                        let mut before_publication = || lease.verify();
                        self.apply_generation_guarded(
                            &workspace,
                            expected_generation,
                            &mutations,
                            idempotency_key,
                            &mut before_publication,
                        )
                        .await?
                    } else {
                        self.apply_generation(
                            &workspace,
                            expected_generation,
                            &mutations,
                            idempotency_key,
                        )
                        .await?
                    }
                    #[cfg(target_arch = "wasm32")]
                    self.apply_generation(
                        &workspace,
                        expected_generation,
                        &mutations,
                        idempotency_key,
                    )
                    .await?
                }
            };
            Ok::<_, Error>(generation)
        };
        #[cfg(not(target_arch = "wasm32"))]
        let (generation, source_snapshot) = if let Some(allocation) = root_allocation {
            let failure_slot = SourcePublicationFailureSlot::prepare(io)?;
            let (generation, snapshot) = allocation
                .publish_lazy_candidate_with_io(&mutations, idempotency_key, publication_permit, io)
                .await?;
            (generation, Some((snapshot, failure_slot)))
        } else {
            (publication.await?, None)
        };
        #[cfg(target_arch = "wasm32")]
        let generation = {
            let _ = publication_permit;
            publication.await?
        };
        #[cfg(not(target_arch = "wasm32"))]
        let root_retained = if let Some((snapshot, failure_slot)) = source_snapshot {
            let retention = fork_preparer_io::retain(&generation, io).await;
            match (snapshot, retention) {
                (Ok(_), Ok(())) => failure_slot.discard(io)?,
                (Err(source), retention) => {
                    return Err(failure_slot.finish(generation.id(), source, retention.err()));
                }
                (Ok(_), Err(source)) => {
                    return Err(failure_slot.finish(generation.id(), Error::from(source), None));
                }
            }
            true
        } else {
            false
        };
        #[cfg(target_arch = "wasm32")]
        let root_retained = false;
        let generation_reference =
            fork_preparer_io::generation_ref(&self.provider, generation.id(), io)?;
        let version = fork_preparer_io::hex_key(generation_reference.as_resource().key(), io)?;
        if !root_retained {
            match io {
                Some(io) => fork_preparer_io::retain(&generation, Some(io)).await?,
                None => {
                    generation
                        .pin(
                            format!("harness-generation-{version}"),
                            acyclic_fs::WorkBudget::UNBOUNDED,
                            &acyclic_fs::CancellationToken::new(),
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| map_error(failure.error))?;
                }
            }
        }
        let reference = {
            let io = io;
            let descriptor = &descriptor;
            crate::executor::with_history_io(io, |work| {
                crate::conversation::FileRef::new(
                    (volume).copy(work)?,
                    crate::filesystem::fork_preparer_io::clone_text(path, io)?,
                    crate::filesystem::fork_preparer_io::clone_text(&version, io)?,
                    crate::conversation::FileDescriptor::new(
                        *descriptor.sha256(),
                        descriptor.byte_length(),
                        crate::filesystem::fork_preparer_io::clone_text(
                            descriptor.media_type(),
                            io,
                        )?,
                    )?,
                    crate::filesystem::fork_preparer_io::clone_text(display_name, io)?,
                    work,
                )
            })
        }?;
        self.read_pinned_io(&reference, maximum_bytes, io).await?;
        Ok(reference)
    }

    async fn read_pinned_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((io, original)) =
            io.and_then(|io| io.original_native_work().map(|original| (io, original)))
        {
            use sha2::Digest as _;
            reference.validate(&mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(original)?)))?;
            if reference.volume().provider() != &self.provider
                || reference.descriptor().byte_length() > maximum_bytes
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "content provider or size limit mismatch",
                });
            }
            let generation = self.file_generation(reference, Some(io))?;
            let workspace =
                fork_preparer_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
            let rendered = original
                .display(&format_args!("/{}", reference.path()))
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            let path_custody = rendered.1;
            let path = rendered.0;
            let opened = fork_preparer_io::open(self, &workspace, io).await?;
            let pinned = fork_preparer_io::generation(self, &opened, &generation, io).await?;
            let bytes = pinned
                .read(&path, maximum_bytes, original, io.cancellation())
                .await
                .map_err(|source| Error::NativeWorkspace {
                    source,
                    custody: None,
                })?;
            fork_preparer_io::retain(&pinned, Some(io)).await?;
            let hashed = acyclic_fs::WorkCounters {
                bytes_hashed: bytes.len() as u64,
                ..acyclic_fs::WorkCounters::default()
            };
            original.admit(hashed).map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
            let digest = sha2::Sha256::digest(&bytes);
            original.spent(hashed).map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
            if bytes.len() as u64 != reference.descriptor().byte_length()
                || digest.as_slice() != reference.descriptor().sha256()
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "file content does not match its descriptor",
                });
            }
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
        let generation = self.file_generation(reference, io)?;
        let generation_hold = io
            .map(|io| io.guard_owned(generation.logical_owned_bytes()?))
            .transpose()?;
        let workspace =
            fork_preparer_io::content_workspace(&self.provider, reference.volume(), io)?;
        let path = format!("/{}", reference.path());
        let bytes = match io {
            Some(io) => {
                let opened = fork_preparer_io::open(self, &workspace, io).await?;
                let pinned = fork_preparer_io::generation(self, &opened, &generation, io).await?;
                let bytes = io.receive(
                    pinned
                        .read(&path, maximum_bytes, io.remaining()?, io.cancellation())
                        .await,
                )?;
                let capacity = bytes.retained_capacity().ok_or_else(|| {
                    fork_preparer_io::failure(
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
                self.read(&workspace, Some(&generation), &path, maximum_bytes)
                    .await?
            }
        };
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
        drop(generation);
        drop(generation_hold);
        Ok(bytes)
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

mod stock_turn;
pub use stock_turn::StockTurnMachine;
