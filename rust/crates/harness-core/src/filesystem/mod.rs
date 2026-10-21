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
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};
const FILESYSTEM_JOIN_PROOF_FORMAT: &str = "acyclic.filesystem.join-commit.v2";
mod content_input;
use content_input::ContentInput;
mod content_publisher;
pub use content_publisher::FilesystemContentPublisher;
mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod fork_preparer;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::AllocationClaim;
use fork_preparer::bounded_io as fork_preparer_io;
pub use fork_preparer::bounded_io::PreparationIo;
#[doc(hidden)]
pub use fork_preparer::bounded_io::PreparationRecord;
#[doc(hidden)]
pub use fork_preparer::bounded_io::UploadRecord;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use fork_preparer::bounded_io::native_decoded;
pub(crate) use fork_preparer::bounded_io::{
    DecodeReservation, preparation_slot_bytes, retain_decoded,
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
#[doc(hidden)]
pub mod native_process;
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
#[doc(hidden)]
pub use default_tools::InvocationCancellation;
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
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one implementation pins the complete staged file contract"
    )]
    async fn put_content_impl<B: ContentInput + ?Sized>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        input: &B,
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        internal_class: Option<InternalContentClass>,
        expected_generation: Option<&GenerationRef>,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<FileRef>
    where
        A: 'static,
        O: 'static,
    {
        let bytes = input.as_ref();
        grant.require(volume, VolumeOperation::Write, io)?;
        if volume.provider() != &self.provider || bytes.len() as u64 > maximum_bytes {
            return Err(stock_boundary(
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
        let operation = fork_preparer_io::operation_key(idempotency_key, io)?;
        let provisional = crate::executor::with_history_io(io, |work| {
            FileRef::new(
                volume.copy(work)?,
                stock_string_copy(path, io)?,
                fork_preparer_io::hex_key(&operation.into_bytes(), io)?,
                FileDescriptor::new(
                    *descriptor.sha256(),
                    descriptor.byte_length(),
                    stock_string_copy(descriptor.media_type(), io)?,
                )?,
                stock_string_copy(display_name, io)?,
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
                return Err(stock_boundary(
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
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let prior = crate::conversation::BoundedContentFuture::new(io, || async {
            let owned;
            let io = if let Some(io) = io {
                io
            } else {
                owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &owned
            };
            let opened = self.open(&workspace, Some(io)).await?;
            io.sdk(
                opened
                    .operation_generation(operation, io, io.cancellation())
                    .await,
            )
        })?
        .await?;
        if let Some(prior) = prior {
            let generation = (self).generation_ref_id(prior.id(), io)?;
            fork_preparer_io::retain(&prior, io).await?;
            let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
            let receipt_path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
            })?;
            const RECORD_BYTES: u64 = 6
                * (crate::conversation::MAX_PATH_BYTES as u64
                    + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                    + 1024);
            let original_receipt = read_generation(&prior, &receipt_path, RECORD_BYTES, io).await?;
            if original_receipt.as_ref() != receipt_bytes.as_ref() {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "upload identity belongs to a different request",
                ));
            }
            drop(original_receipt);
            let indexed_path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
            })?;
            let indexed =
                read_generation(&prior, &indexed_path, metadata_bytes.len() as u64, io).await?;
            if indexed.as_ref() != metadata_bytes.as_slice() {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "upload metadata index does not match",
                ));
            }
            let reference = {
                let descriptor = &descriptor;
                crate::executor::with_history_io(io, |work| {
                    crate::conversation::FileRef::new(
                        (volume).copy(work)?,
                        stock_string_copy(path, io)?,
                        stock_string_copy(&version, io)?,
                        crate::conversation::FileDescriptor::new(
                            *descriptor.sha256(),
                            descriptor.byte_length(),
                            stock_string_copy(descriptor.media_type(), io)?,
                        )?,
                        stock_string_copy(display_name, io)?,
                        work,
                    )
                })
            }?;
            let committed = self
                .read_retained_file(&prior, &reference, maximum_bytes, io)
                .await?;
            if committed.as_ref() != bytes {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "upload identity belongs to different content",
                ));
            }
            return Ok(reference);
        }
        let absolute = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("/{}", provisional.path()), work)
        })?;
        let mut mutations = crate::model::vec_with_capacity(6, io)?;
        if let Some((parent, _)) = absolute.rsplit_once('/')
            && !parent.is_empty()
        {
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: stock_string_copy(parent, io)?,
            });
        }
        let owned_bytes = input.mutation_bytes(io)?;
        mutations.push(WorkspaceMutation::PutFile {
            path: absolute,
            bytes: owned_bytes,
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: stock_string_copy("/.system/harness-uploads", io)?,
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: stock_string_copy("/.system/harness-file-metadata", io)?,
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
            })?,
            bytes: metadata_bytes,
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
            })?,
            bytes: receipt_bytes,
        });
        let generation = self
            .apply_generation(
                &workspace,
                expected_generation,
                &mutations,
                IdempotencyKey(stock_string_copy(idempotency_key.as_str(), io)?),
                io,
                None,
            )?
            .await?;
        let generation_reference = (self).generation_ref_id(generation.id(), io)?;
        let version = fork_preparer_io::hex_key(generation_reference.as_resource().key(), io)?;
        {
            match io {
                Some(io) => fork_preparer_io::retain(&generation, Some(io)).await?,
                None => {
                    generation
                        .retain_if_unpublished(
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
            let descriptor = &descriptor;
            crate::executor::with_history_io(io, |work| {
                crate::conversation::FileRef::new(
                    (volume).copy(work)?,
                    stock_string_copy(path, io)?,
                    stock_string_copy(&version, io)?,
                    crate::conversation::FileDescriptor::new(
                        *descriptor.sha256(),
                        descriptor.byte_length(),
                        stock_string_copy(descriptor.media_type(), io)?,
                    )?,
                    stock_string_copy(display_name, io)?,
                    work,
                )
            })
        }?;
        self.read_retained_file(&generation, &reference, maximum_bytes, io)
            .await?;
        Ok(reference)
    }
    async fn read_retained_file(
        &self,
        generation: &Generation<A, O>,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        pinned_read_tests::retained::sample_items(false, io);
        #[cfg(all(test, not(target_arch = "wasm32")))]
        if pinned_read_tests::retained::reopen() {
            let result = self.read_pinned_io(reference, maximum_bytes, io).await;
            pinned_read_tests::retained::sample_items(true, io);
            return result;
        }
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            reference.validate(&mut Some(&mut io.history_io()))?;
        }
        if reference.volume().provider() != &self.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "content provider or size limit mismatch",
                });
            }
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        let (_path, path) = {
            let path = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
            })?;
            let hold = io
                .map(|io| io.guard_owned(path.capacity() as u64))
                .transpose()?;
            (hold, path)
        };
        #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
        acyclic_native_runtime::differential::record(("read", maximum_bytes));
        let bytes = read_generation(generation, &path, maximum_bytes, io).await?;
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        pinned_read_tests::retained::sample_items(true, io);
        Ok(bytes)
    }
    async fn read_pinned_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        // Keep the Result temporary across await for the original admitted caller frame.
        (if io.is_none() {
            crate::conversation::BoundedContentFuture::new(None, || {
                self.read_pinned_unmetered(reference, maximum_bytes, pinned_read::bytes_only)
            })
        } else {
            crate::conversation::BoundedContentFuture::new(io, || {
                self.read_pinned_metered(reference, maximum_bytes, io)
            })
        })?
        .await
    }

    async fn read_pinned_receipt_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<PinnedRead<A, O>> {
        if io.is_none() {
            crate::conversation::BoundedContentFuture::new(None, || {
                self.read_pinned_unmetered(reference, maximum_bytes, PinnedRead::from_read)
            })?
            .await
        } else {
            // Metered reads already pin before descriptor verification. Project
            // their bytes after awaiting the unchanged admitted factory.
            Ok(PinnedRead::retained(
                self.read_pinned_io(reference, maximum_bytes, io).await?,
            ))
        }
    }

    fn read_pinned_unmetered<'a, T, F>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        project: F,
    ) -> impl std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a
    where
        F: FnOnce(acyclic_stream::OwnedBytes, Option<Generation<A, O>>) -> T
            + acyclic_stream::ProviderTask
            + 'a,
    {
        async move {
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
            // Keep the authenticated immutable handle from the read. A
            // retaining caller can pin it without opening the workspace
            // and authenticating this same generation a second time.
            #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
            acyclic_native_runtime::differential::record(("read", maximum_bytes));
            let owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            let opened = self.open(&workspace, Some(&owned)).await?;
            let pinned = self.generation(&opened, &generation, Some(&owned)).await?;
            let bytes = owned.sdk(
                pinned
                    .read(&path, maximum_bytes, &owned, owned.cancellation())
                    .await,
            )?;
            owned.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
            reference.descriptor().verify(&bytes, &mut None)?;
            Ok(project(bytes, Some(pinned)))
        }
    }

    fn read_pinned_metered<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a fork_preparer_io::PreparationIo>,
    ) -> impl std::future::Future<Output = Result<acyclic_stream::OwnedBytes>>
    + acyclic_stream::ProviderTask
    + 'a {
        async move {
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
                let pinned = self.generation(&opened, &generation, Some(io)).await?;
                #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
                acyclic_native_runtime::differential::record(("read", maximum_bytes));
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
                    let pinned = self.generation(&opened, &generation, Some(io)).await?;
                    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
                    acyclic_native_runtime::differential::record(("read", maximum_bytes));
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
        }
    }
}

mod pinned_read;
use pinned_read::PinnedRead;

#[cfg(test)]
mod tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod pinned_read_tests;

mod stock_turn;
pub use stock_turn::StockTurnMachine;
#[doc(hidden)]
pub mod child_agent;
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
