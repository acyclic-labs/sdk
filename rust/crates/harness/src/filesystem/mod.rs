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
use acyclic_fs::WorkCounters;
use crate::{
    AgentId, Error, IdempotencyKey, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef, Limits,
        MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryEntry, PrivateDirectoryEntryKind,
        PrivateDirectoryPage, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef, decode_attachment_manifest,
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
use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding};
use acyclic_fs::{
    ApplyOptions, AsyncAuthorityStore, AsyncObjectStore, ConflictSide, Digest, ForkOptions, Fs,
    Generation, GenerationId, IdempotencyKey as FilesystemKey, JoinCommitWitness, JoinHistory,
    JoinOutcome, JoinPlan, MergeConflict, MergePlan,
    PublicationReservation, TransactionCommit, Workspace, WorkspaceDirectoryPage, WorkspaceError,
    WorkspaceStat,
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
use fork_preparer::bounded_io as fork_preparer_io;
pub(crate) use fork_preparer::AllocationClaim;
pub(crate) use fork_preparer::bounded_io::{preparation_slot_bytes, PreparationIo, UploadRecord, PreparationRecord, ReservedDecoded, DecodeReservation};
mod summary_fork;
pub use fork_preparer::{FilesystemForkPreparer, FilesystemResourceCreationProvider, FilesystemRootBootstrapAllocation};
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
mod root_source;
pub use root_source::{FsRootNativeSource, FsRootProjectSourceProvider};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_execution;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub(crate) use native_execution::NativePreparationCompletionTemplate;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_process;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_process::{NativeProcessAuthority, NativeProcessProvider, NativeProcessReceipt, NativeProcessDrainProof, NativeProcessPublishedVolume};
mod native_capture_bounds;
mod native_contract;
pub use native_contract::{
    NATIVE_PROCESS_EFFECT_KIND, NativeVolumeNamespace, NativeProcessRequest, NativeProcessResult,
    NativeProcessStop, NativeProcessStopKind, NativeViewManifest, NativeViewOptions,
    NativeVolumeMapping,
};
mod task_commands;
mod task_runtime;
mod recursive;
mod mailbox_machine;
mod default_tools;
mod configured_recursive_tools;
pub use configured_recursive_tools::{
    ConfiguredRecursiveAuthorityBinding, ConfiguredRecursiveCaller,
    ConfiguredRecursiveFork, ConfiguredRecursiveMerge, ConfiguredRecursiveToolsConfig,
    ConfiguredRecursiveToolSource, ConfiguredRecursiveSourceCheckpoint,
};
pub use default_tools::{
    recursive_default_tools, RecursiveDefaultToolset, RecursiveDefaultToolSource,
    RecursiveToolOwner, RecursiveForkToolBinding, RecursiveForkToolPlan,
    RecursiveForkSourceIntent,
    RecursiveMergeToolBinding, RecursiveMergeToolPlan,
    MergeToolArguments, MergeToolSelection, MergeToolSide,
};
pub(crate) use default_tools::InvocationCancellation;
pub use mailbox_machine::MailboxModelMachine;
pub use recursive::{
    ForkToolArguments, ForkPreparationLimits, ForkPreparationMachine, fork_preparation_request,
    RecursiveSourceSnapshot, RecursiveForkConfig, RetainedSourceBinding,
    recursive_fork_source, restore_recursive_fork_source,
};
#[cfg(not(target_arch = "wasm32"))]
pub use recursive::{
    restore_recursive_fork_source_with_native_work,
};
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_execution::{
    NativeExistingLazyVolume, NativeRootOutputSource, NativeTaskWorkingView,
    BorrowedNativeVolumeBinding, NativePreparationReceipt, NativePreparationRoot,
    NativeProcessView, NativeProcessNamespaceScopes, NativePublicationFailure, NativeVolumeView,
    NativePublicationCleanup, NativePublicationReservation, NativePublicationReservationFailure,
    NativePreparationJournalObservation, NativeRecordedPreparation,
    NativeViewPublicationError, NativeViewPublicationReceipt,
    NativeTaskViewDrainReceipt,
    copy_native_mapping_with_work,
    bind_original_root_conversation_with_native_work, native_root_output_binding_operation,
};
pub use task_commands::{
    EFFECT_TASK_COMMAND_KIND, FilesystemTaskCommands, MAIL_RECEIVE_TASK_COMMAND_KIND,
    MAIL_SEND_TASK_COMMAND_KIND, MODEL_TASK_COMMAND_KIND, MailReceiveTaskCommand,
    MailSendTaskCommand, ModelTaskCommand, TASK_ADMIT_COMMAND_KIND, TASK_OBSERVE_COMMAND_KIND,
    TIMER_TASK_COMMAND_KIND, TOOL_TASK_COMMAND_KIND, TaskAdmitCommand, TaskObserveCommand,
    TimerTaskCommand, ToolTaskCommand,
    FORK_PREPARE_TASK_COMMAND_KIND, TASK_ADMIT_PREPARED_COMMAND_KIND,
    PreparedTaskAdmitCommand, TASK_CANCEL_COMMAND_KIND, TaskCancelCommand,
};
pub use task_runtime::{
    FilesystemTaskExecution, FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress,
    TaskWakeCursor, TaskWakePage, TaskWorkerAttempt, TaskWorkerOutcome, TaskWorkerTick,
    TaskSourceInspection,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_volume_storage_name(
    volume: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let io = PreparationIo::from_native_work(work)?;
    let name = fork_preparer_io::volume_storage_name(volume, Some(&io))?;
    let retained = name.capacity() as u64;
    Ok((name, io.into_native_custody(retained)?))
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct SourcePublicationCause {
    source: Option<Error>,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for SourcePublicationCause {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            Some(source) => std::fmt::Display::fmt(source, formatter),
            None => formatter.write_str("reserved source publication failure"),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::error::Error for SourcePublicationCause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| source as &dyn std::error::Error)
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct SourcePublicationFailureSlot {
    source: Box<SourcePublicationCause>,
    custody: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SourcePublicationFailureSlot {
    fn prepare(io: Option<&PreparationIo>) -> Result<Self> {
        let bytes = std::mem::size_of::<SourcePublicationCause>() as u64;
        let custody = if let Some(io) = io {
            io.claim(bytes)?;
            io.allocation_attempt(bytes)?;
            if io.original_native_work().is_some() {
                Some(io.take_native_custody(bytes)?)
            } else {
                None
            }
        } else {
            None
        };
        Ok(Self { source: Box::new(SourcePublicationCause { source: None }), custody })
    }

    fn finish(
        self, generation: acyclic_fs::GenerationId, source: Error,
        retention_failure: Option<acyclic_fs::LazyWorkspaceError>,
    ) -> Error {
        let Self { source: mut slot, custody } = self;
        slot.source = Some(source);
        Error::NativeSourcePublicationUncertain {
            generation, source: slot, retention_failure, custody,
        }
    }

    fn discard(self, io: Option<&PreparationIo>) -> Result<()> {
        drop(self);
        if let Some(io) = io
            && io.original_native_work().is_none() {
            io.release(std::mem::size_of::<SourcePublicationCause>() as u64)?;
        }
        Ok(())
    }
}

/// Owner-scoped scheduler result staging into one agent-private Filesystem volume.
pub struct FilesystemSchedulerPayloadStore<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
    maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemSchedulerPayloadStore<A, O> {
    /// Binds private-volume write authority with optional metadata accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>, volume: &VolumeRef,
        verifier: &AuthorityVerifier, scope: &Scope, maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider || maximum_bytes == 0
        {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                "scheduler payload store requires a private volume and positive limit"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let grant = ContentGrant::verify(verifier, scope, volume, VolumeOperation::Write,
            u64::MAX, &mut work)?;
        let volume = volume.copy(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                Some(io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "scheduler payload custody underflows"))?)?)
            }
            None => None,
        };
        Ok(Self { host, volume, grant, maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))] _native_metadata })
    }

    /// The intrinsic guard covers Self and deep metadata, not opaque Arc nodes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self._native_metadata.as_ref().is_some_and(|custody| work.shares_ledger(custody.work())) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native payload sharing requires original metadata custody" });
        }
        let io = PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }
}

impl<A, O> SchedulerPayloadStore for FilesystemSchedulerPayloadStore<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn stage<'a>(
        &'a self,
        operation_id: crate::OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(self.stage_with_io(operation_id, idempotency_key, bytes, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn stage_with_native_work<'a>(
        &'a self, operation_id: crate::OperationId, idempotency_key: &'a str,
        bytes: &'a [u8], work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = self.stage_with_io(
                operation_id, idempotency_key, bytes, Some(&io)).await?;
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FilesystemSchedulerPayloadStore<A, O>
{
    async fn stage_with_io(
        &self, operation_id: crate::OperationId, idempotency_key: &str,
        bytes: &[u8], io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        use std::fmt::Write as _;
        let hashed = acyclic_fs::WorkCounters {
            bytes_hashed: idempotency_key.len() as u64, items_examined: 1,
            ..Default::default()
        };
        if let Some(io) = io { io.preflight(hashed)?; }
        let key = blake3::hash(idempotency_key.as_bytes()).to_hex();
        if let Some(io) = io { io.charge(hashed)?; }
        let path_bytes = "scheduler/results/".len() + 36 + 1 + key.len() + ".json".len();
        let retry_bytes = "scheduler-result:".len() + 36 + 1 + key.len();
        let scratch_hold;
        if let Some(io) = io {
            io.preflight(acyclic_fs::WorkCounters {
                bytes_copied: (path_bytes + retry_bytes) as u64, ..Default::default()
            })?;
            io.claim((path_bytes + retry_bytes) as u64)?;
            scratch_hold = Some(io.guard_owned((path_bytes + retry_bytes) as u64)?);
            io.allocation_attempt(path_bytes as u64)?;
        } else { scratch_hold = None; }
        let mut path = String::with_capacity(path_bytes);
        write!(&mut path, "scheduler/results/{operation_id}/{key}.json")
            .map_err(|_| Error::Invalid("scheduler result path formatting failed".into()))?;
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters { bytes_copied: path.len() as u64, ..Default::default() })?;
        }
        if let Some(io) = io { io.allocation_attempt(retry_bytes as u64)?; }
        let mut retry = String::with_capacity(retry_bytes);
        write!(&mut retry, "scheduler-result:{operation_id}:{key}")
            .map_err(|_| Error::Invalid("scheduler retry formatting failed".into()))?;
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                bytes_copied: retry.len() as u64, ..Default::default()
            })?;
        }
        let retry = IdempotencyKey::new(retry)?;
        let file = self.host.put_content_impl(&self.volume, &self.grant, &path, bytes,
            "application/json", "result.json", self.maximum_bytes, &retry,
            None, None, io, None, None).await?;
        drop(retry);
        drop(path);
        drop(scratch_hold);
        Ok(file)
    }
}

/// Filesystem-backed admission barrier for version-pinned conversation content.
pub struct FilesystemContentVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemContentVerifier<A, O> {
    /// Authenticates and copies the caller configuration with optional accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>, verifier: &AuthorityVerifier,
        scope: &Scope, maximum_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "content admission limit must be positive"));
        }
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        verifier.verify_with_work(scope, &mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let verifier = verifier.clone_with_work(&mut work)?;
        let scope = scope.clone_with_work(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_metadata = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                Some(io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                    .ok_or_else(|| fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Invalid, "content verifier custody underflows"))?)?)
            }
            None => None,
        };
        Ok(Self { host, verifier, scope, maximum_bytes,
            #[cfg(not(target_arch = "wasm32"))] _native_metadata })
    }

    /// Owned configuration bytes.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.scope.logical_owned_bytes()?.checked_add(self.verifier.logical_owned_bytes()?)
            .ok_or_else(|| Error::Invalid("content verifier ownership overflow".into()))
    }

    /// Shares the already-funded verifier without copying configuration payloads.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self._native_metadata.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native content sharing requires original resource metadata custody" });
        }
        let io = PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }
}

impl<A, O> ContentResidencyVerifier for FilesystemContentVerifier<A, O>
where A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
{
    fn supports_read(&self, volume: &VolumeRef, io: Option<&PreparationIo>) -> Result<bool> {
        fork_preparer_io::item(io, 0)?;
        let mut history = io.map(PreparationIo::history_io);
        volume.validate(&mut history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
        Ok(volume.provider() == &self.host.provider)
    }

    fn read<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || self.read_file(reference, maximum_bytes, io))
    }

    fn verify<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            drop(self.read_file(reference, maximum_bytes, io).await?);
            if let Some(io) = io { io.remaining()?; }
            Ok(())
        })
    }

    fn read_private_prefix<'a>(&'a self, volume: &'a VolumeRef, granted_prefix: &'a str,
        path: &'a str, generation: &'a GenerationRef, maximum_bytes: u64, io: Option<&'a PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if maximum_bytes == 0 || maximum_bytes > self.maximum_bytes {
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "prefix read exceeds provider limit"));
            }
            let mut history = io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(&self.verifier, &self.scope, volume,
                granted_prefix, &mut history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
            let Some(io) = io else {
                return self.host.read_private_prefix(volume, &grant, path, generation, maximum_bytes).await;
            };
            grant.require_directory_path(volume, path, Some(io))?;
            let reference = fork_preparer_io::content_workspace(&self.host.provider, volume, Some(io))?;
            let reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
            let workspace = fork_preparer_io::open(&self.host, &reference, io).await?;
            let pinned = fork_preparer_io::generation(&self.host, &workspace, generation, io).await?;
            fork_preparer_io::retain(&pinned, Some(io)).await?;
            let path = crate::contract::preparation_format(format_args!("/{path}"), Some(io))?;
            let path_hold = io.guard_owned(path.capacity() as u64)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                let stat = pinned.stat_with_native_work(&path, work, io.cancellation()).await
                    .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                if stat.kind != FileKind::Regular { return Err(io.invalid("prefix requires a regular file")); }
                let length = stat.logical_bytes.ok_or_else(|| io.invalid("file length is missing"))?;
                return pinned.read_range_with_native_work(&path, 0, maximum_bytes.min(length), work, io.cancellation()).await
                    .map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
            }
            let stat = io.receive(pinned.stat(&path, io.remaining()?, io.cancellation()).await)?;
            if stat.kind != FileKind::Regular { return Err(io.invalid("prefix requires a regular file")); }
            let length = stat.logical_bytes.ok_or_else(|| io.invalid("file length is missing"))?;
            let read = io.receive(pinned.read_range(&path, 0, maximum_bytes.min(length), io.remaining()?, io.cancellation()).await)?;
            let bytes = read.bytes;
            drop(path); drop(path_hold); drop(reference); drop(reference_hold);
            Ok(bytes)
        })
    }

    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32, io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, PrivateDirectoryPage>> {
        if io.is_some() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Unsupported, "finite private directory discovery is unavailable"));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let after = after
                .map(|name| {
                    LogicalName::new(NameEncoding::Utf8, name.as_bytes().to_vec(), u32::MAX)
                        .map_err(|error| Error::Invalid(error.to_string()))
                })
                .transpose()?;
            let (generation, page) = FilesystemContentVerifier::list_private_directory(
                self,
                volume,
                granted_prefix,
                path,
                expected_generation,
                after.as_ref(),
                maximum_entries,
            )
            .await?;
            let entries = page
                .entries
                .into_iter()
                .map(|entry| {
                    let name = entry
                        .name
                        .unicode_text()
                        .ok_or_else(|| {
                            Error::Unsupported("private directory name is not Unicode".into())
                        })?
                        .into_owned();
                    let kind = match entry.kind {
                        FileKind::Regular => PrivateDirectoryEntryKind::File,
                        FileKind::Directory => PrivateDirectoryEntryKind::Directory,
                        _ => {
                            return Err(Error::Unsupported(
                                "private directory contains an unsupported entry kind".into(),
                            ));
                        }
                    };
                    Ok(PrivateDirectoryEntry { name, kind })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(PrivateDirectoryPage {
                generation,
                entries,
                has_more: page.has_more,
            })
        })
    }

    fn read_private_path<'a, 'io>(&'a self, volume: &'a VolumeRef, granted_prefix: &'a str,
        path: &'a str, expected_generation: Option<&'a GenerationRef>, maximum_bytes: u64,
        io: Option<&'io PreparationIo>)
        -> Result<crate::conversation::BoundedContentFuture<'a, (FileRef, acyclic_stream::OwnedBytes, Option<crate::conversation::ContentHold<'io>>)>> where 'io: 'a {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let mut history = io.map(PreparationIo::history_io);
            let grant = ContentGrant::verify_directory_read(&self.verifier, &self.scope, volume,
                granted_prefix, &mut history.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo))?;
            let grant_hold = io.map(|io| io.guard_owned(grant.logical_owned_bytes()?)).transpose()?;
            let (file, bytes) = self.host.read_private_path_with_io(volume, &grant, path, expected_generation,
                maximum_bytes.min(self.maximum_bytes), io).await?;
            drop(grant); drop(grant_hold);
            let hold = io.map(|io| io.guard_owned(file.logical_owned_bytes()?)).transpose()?;
            Ok((file, bytes, hold))
        })
    }
}

impl<A, O> FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn read_file(&self, reference: &FileRef, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
        let maximum_bytes = maximum_bytes.min(self.maximum_bytes);
        if let Some(io) = io {
            self.require_read(reference, io)?;
            self.host.read_pinned_io(reference, maximum_bytes, Some(io)).await
        } else {
            self.host.read_content(reference, &self.read_grant(reference)?, maximum_bytes).await
        }
    }

    /// Lazily lists another agent's private directory under this reader's signed, owner-delegated subtree grant.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        let grant = ContentGrant::verify_directory_read(
            &self.verifier,
            &self.scope,
            volume,
            granted_prefix, &mut None,
        )?;
        self.host
            .list_private_directory(
                volume,
                &grant,
                path,
                expected_generation,
                after,
                maximum_entries,
            )
            .await
    }

    /// Requires authentic provider metadata funded by this original resource.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn supports_native_directory_list(
        &self, volume: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if !self._native_metadata.as_ref().is_some_and(|guard| guard.matches_work(work)) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "directory verifier lacks original native metadata custody",
            });
        }
        self.verifier.verify_with_native_work(&self.scope, work)?;
        volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if volume.provider() != &self.host.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "private directory belongs to another provider",
            });
        }
        Ok(())
    }

    /// Lists under both the signed provider subtree and the current dispatch owner.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "exact owner, subtree and immutable pagination")]
    pub fn list_private_directory_with_native_work<'a>(
        &'a self, context: &'a crate::runtime::TaskContext, volume: &'a VolumeRef,
        granted_prefix: &'a str, path: &'a str,
        expected_generation: Option<&'a GenerationRef>, after: Option<&'a str>,
        maximum_entries: u32, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ((GenerationRef, WorkspaceDirectoryPage), acyclic_fs::NativeAllocation)>> {
        self.supports_native_directory_list(volume, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            context.verify_original_execution_owner_with_native_work(work)?.await?;
            let grant_custody;
            let grant;
            (grant, grant_custody) = ContentGrant::verify_directory_read_with_native_work(
                &self.verifier, &self.scope, volume, granted_prefix, work,
            )?;
            grant.require_directory_path(volume, path, Some(&PreparationIo::from_native_work(work)?))?;
            if path.len() > context.scope().limits().path_bytes
                || after.is_some_and(|name| name.len() > context.scope().limits().path_bytes)
                || (after.is_some() && expected_generation.is_none())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "directory cursor or path exceeds its issued task boundary",
                });
            }
            let cursor_header = work.retain_owned(
                if after.is_some() { std::mem::size_of::<LogicalName>() as u64 } else { 0 },
            ).map_err(Error::from)?;
            let cursor_custody;
            let cursor = if let Some(name) = after {
                work.charge(acyclic_fs::WorkCounters {
                    items_examined: name.len() as u64 + 1, ..Default::default()
                }).map_err(Error::from)?;
                let (bytes, custody) = work.copy_bytes(name.as_bytes())
                    .map_err(Error::from)?;
                cursor_custody = Some(custody);
                Some(LogicalName::new(NameEncoding::Utf8, bytes, u32::MAX)
                    .map_err(|_| Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "private directory cursor is not a logical child name",
                    })?)
            } else {
                cursor_custody = None;
                None
            };
            let result = self.host.list_private_directory_guarded_with_native_work(
                volume, &grant, path, expected_generation, cursor.as_ref(), maximum_entries, work,
                || async {
                    self.supports_native_directory_list(volume, work)?;
                    grant.require_directory_path(volume, path, Some(&PreparationIo::from_native_work(work)?))?;
                    let io = PreparationIo::from_native_work(work)?;
                    let directory = io.contains_capability(
                        context.scope().grants(), &(volume, granted_prefix), "directory:read:",
                    )?;
                    let whole_volume = io.contains_capability(
                        context.scope().grants(), volume, "volume:read:",
                    )?;
                    if !directory && !whole_volume {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unauthorized,
                            reason: "issued task scope does not retain this directory read",
                        });
                    }
                    context.verify_original_execution_owner_with_native_work(work)?.await
                },
            ).await;
            drop(cursor);
            drop(cursor_custody);
            drop(cursor_header);
            drop(grant);
            drop(grant_custody);
            result
        })
    }

    /// Lazily reads a named private file and returns its pinned ref and bytes; the exact version can subsequently be shared independently of lineage.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        let grant = ContentGrant::verify_directory_read(
            &self.verifier,
            &self.scope,
            volume,
            granted_prefix, &mut None,
        )?;
        self.host.read_private_path(volume, &grant, path, expected_generation, self.maximum_bytes).await
    }

    fn require_read(&self, reference: &FileRef, io: &PreparationIo) -> Result<()> {
        reference.validate(&mut Some(&mut io.history_io()))?;
        let internal = crate::conversation::is_internal_path(reference.path());
        let inherited = crate::conversation::is_inherited_context_path(reference.path());
        let internal_owner = reference.path().starts_with(".system/") && !inherited;
        if internal && !inherited && !internal_owner {
            return Err(fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Unauthorized, "internal storage file is not public content"));
        }
        if io.contains_capability(self.scope.capabilities(), reference.volume(), "volume:read:")? { return Ok(()); }
        if internal_owner {
            return Err(fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Unauthorized, "volume operation is not granted"));
        }
        if io.contains_capability(self.scope.capabilities(), reference, "file:read:")? { return Ok(()); }
        if !internal && reference.volume().class() == VolumeClass::AgentPrivate && self.scope.agent().is_some() {
            if io.contains_capability(self.scope.capabilities(), &(reference.volume(), ""), "directory:read:")? { return Ok(()); }
            for (end, _) in reference.path().match_indices('/') {
                let prefix = &reference.path()[..end];
                if io.contains_capability(self.scope.capabilities(), &(reference.volume(), prefix), "directory:read:")? { return Ok(()); }
            }
        }
        Err(fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Unauthorized, "file read is not granted by its owner"))
    }

    fn read_grant(&self, reference: &FileRef) -> Result<ContentGrant> {
        if reference.path().starts_with(".system/")
            && !crate::conversation::is_inherited_context_path(reference.path())
        {
            ContentGrant::verify(
                &self.verifier,
                &self.scope,
                reference.volume(),
                VolumeOperation::Read, self.maximum_bytes, &mut None,
            )
        } else {
            ContentGrant::verify_read(&self.verifier, &self.scope, reference)
        }
    }


}

/// Filesystem portion of the fork-publication admission barrier.
pub struct FilesystemForkVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    maximum_inherited_bytes: u64,
    maximum_total_reference_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    native_binding: Option<NativeFilesystemVerificationBinding>,
}

struct FilesystemForkPublicationGuard<A, O> {
    workspace: Workspace<A, O>,
    reservation: Option<PublicationReservation>,
    #[cfg(not(target_arch = "wasm32"))]
    native_work: Option<acyclic_fs::NativeMaterializationWork>,
    #[cfg(not(target_arch = "wasm32"))]
    native_binding: Option<NativeFilesystemVerificationBinding>,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeFilesystemVerificationBinding {
    verifier: AuthorityVerifier,
    scope: Scope,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeFilesystemVerificationBinding {
    fn new(
        verifier: &AuthorityVerifier, scope: &Scope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        verifier.verify_with_native_work(scope, work)?;
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(Error::from)?;
        let verifier = verifier.copy(&mut Some(&mut observer))?;
        let scope = scope.copy(&mut Some(&mut observer))?;
        Ok(Self { verifier, scope, custody: observer.into_custody() })
    }

    fn authenticate(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "Filesystem verifier belongs to another original resource" });
        }
        self.verifier.verify_with_native_work(&self.scope, work)
    }

    fn require_seed(&self, seed: &ForkSeed, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.authenticate(work)?;
        let audience = self.verifier.audience();
        if audience != &seed.parent && audience != &seed.child {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork verifier belongs to another publication aggregate" });
        }
        if self.scope.agent().is_none()
            || (audience == &seed.child && self.scope.agent() != Some(seed.child_agent))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork verifier belongs to another publication agent" });
        }
        if audience == &seed.parent && !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent fork publication is not currently granted" });
        }
        Ok(())
    }

    fn require_file(&self, file: &FileRef, io: &PreparationIo) -> Result<()> {
        let work = io.original_native_work().expect("native verification IO");
        self.authenticate(work)?;
        file.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        let internal = crate::conversation::is_internal_path(file.path());
        let inherited = crate::conversation::is_inherited_context_path(file.path());
        let internal_owner = file.path().starts_with(".system/") && !inherited;
        if internal && !inherited && !internal_owner {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "internal storage file is not public content" });
        }
        if io.contains_capability(self.scope.capabilities(), file.volume(), "volume:read:")? {
            return Ok(());
        }
        if !internal_owner {
            if io.contains_capability(self.scope.capabilities(), file, "file:read:")? {
                return Ok(());
            }
            if !internal && file.volume().class() == VolumeClass::AgentPrivate && self.scope.agent().is_some() {
                if io.contains_capability(self.scope.capabilities(), &(file.volume(), ""), "directory:read:")? {
                    return Ok(());
                }
                for (end, _) in file.path().match_indices('/') {
                    if io.contains_capability(self.scope.capabilities(), &(file.volume(), &file.path()[..end]), "directory:read:")? {
                        return Ok(());
                    }
                }
            }
        }
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
            reason: "Filesystem verifier has no current read grant for this file" })
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeParentMergePublicationOwner {
    binding: NativeFilesystemVerificationBinding,
    project: VolumeRef,
    custody: acyclic_fs::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeParentMergePublicationOwner {
    fn new(
        verifier: &AuthorityVerifier, scope: &Scope, project: &VolumeRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        let io = PreparationIo::from_native_work(work)?;
        let binding = NativeFilesystemVerificationBinding::new(verifier, scope, work)?;
        let (project, custody) = project.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        io.claim(std::mem::size_of::<Self>() as u64)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(Arc::new(Self { binding, project, custody }))
    }

    fn check(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if !self.custody.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "merge publication owner belongs to another original resource" });
        }
        self.binding.authenticate(work)?;
        let io = PreparationIo::from_native_work(work)?;
        if !self.binding.scope.capabilities().contains(capability::PROJECT_MERGE)
            || !io.contains_capability(self.binding.scope.capabilities(), &self.project, "volume:write:")?
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent project publication is not currently granted" });
        }
        Ok(())
    }
}

impl<A, O> ForkPublicationGuard for FilesystemForkPublicationGuard<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    fn release_with_native_work<'a>(
        &'a self, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "fork publication guard has no original native authority" })?;
        binding.authenticate(work)?;
        let original = self.native_work.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "fork publication guard lost its original work",
        })?;
        crate::NativeFutureBundle::new(original, || async move {
            let reservation = self.reservation.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "fork publication guard has no reservation" })?;
            self.workspace.release_publication_with_native_work(reservation, original, original.cancellation())
                .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })
        })
    }

    fn release<'a>(&'a self) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.release_measured(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
        })
    }

    fn release_measured<'a>(
        &'a self,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            self.workspace
                .release_publication(
                    self.reservation.ok_or_else(|| acyclic_fs::OperationFailure::before_work(
                        Error::Invalid("fork publication guard has no reservation".into()),
                    ))?,
                    budget,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_error(map_error))
        })
    }
}

fn fork_verify_reserve<T>(values: &mut Vec<T>, additional: usize, io: &PreparationIo) -> Result<()> {
    let needed = values.len().checked_add(additional)
        .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork verification vector overflow"))?;
    if needed > values.capacity() {
        let bytes = (needed - values.capacity()).checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork verification allocation overflow"))? as u64;
        io.claim(bytes)?;
        io.allocation_attempt(bytes)?;
        values.try_reserve_exact(needed - values.len())
            .map_err(Error::Allocation)?;
    }
    Ok(())
}

fn fork_verify_insert<T>(
    values: &mut Vec<T>, value: T, compare: impl Fn(&T, &T) -> std::cmp::Ordering,
    io: &PreparationIo,
) -> Result<bool> {
    let mut low = 0;
    let mut high = values.len();
    while low < high {
        let middle = low + (high - low) / 2;
        let examined = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
        io.preflight(examined)?;
        io.charge(examined)?;
        match compare(&values[middle], &value) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                values[middle] = value;
                return Ok(false);
            }
        }
    }
    fork_verify_reserve(values, 1, io)?;
    let shifted = (values.len() - low).checked_mul(std::mem::size_of::<T>())
        .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork verification copy overflow"))? as u64;
    let copies = acyclic_fs::WorkCounters { bytes_copied: shifted, ..Default::default() };
    io.preflight(copies)?;
    io.charge(copies)?;
    values.insert(low, value);
    Ok(true)
}

fn fork_verify_contains<T: Ord>(values: &[T], value: &T, io: &PreparationIo) -> Result<bool> {
    let mut low = 0;
    let mut high = values.len();
    while low < high {
        let middle = low + (high - low) / 2;
        fork_preparer_io::item(Some(io), 0)?;
        match values[middle].cmp(value) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(true),
        }
    }
    Ok(false)
}

fn fork_verify_equal<T: Eq>(left: &[T], right: &[T], io: &PreparationIo) -> Result<bool> {
    if left.len() != right.len() { return Ok(false); }
    for (left, right) in left.iter().zip(right) {
        fork_preparer_io::item(Some(io), 0)?;
        if left != right { return Ok(false); }
    }
    Ok(true)
}

impl<A, O> FilesystemForkVerifier<A, O> {
    /// Binds one Filesystem provider and a per-file fork-reference limit.
    pub fn new(host: Arc<FilesystemHost<A, O>>, maximum_inherited_bytes: u64) -> Result<Self> {
        if maximum_inherited_bytes == 0 {
            return Err(Error::Invalid(
                "fork inherited-content limit must be positive".into(),
            ));
        }
        Ok(Self {
            host,
            maximum_inherited_bytes,
            maximum_total_reference_bytes: maximum_inherited_bytes.saturating_mul(1_024),
            #[cfg(not(target_arch = "wasm32"))]
            native_binding: None,
        })
    }

    /// Copies the authentic scoped verifier without creating a new issuer or grant.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, maximum_inherited_bytes: u64,
        verifier: &AuthorityVerifier, scope: &Scope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        if maximum_inherited_bytes == 0 {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "fork inherited-content limit must be positive" });
        }
        host.provider.validate_with_native_work(work)?;
        let binding = NativeFilesystemVerificationBinding::new(verifier, scope, work)?;
        Ok(Self { host, maximum_inherited_bytes,
            maximum_total_reference_bytes: maximum_inherited_bytes.saturating_mul(1_024),
            native_binding: Some(binding) })
    }



    /// Admits the one actual shared provider allocation before constructing it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        let binding = self.native_binding.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
        binding.authenticate(work)?;
        let mut header = work.claim(std::mem::size_of::<Self>() as u64)
            .map_err(Error::from)?;
        binding.custody.absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        Ok(Arc::new(self))
    }
    /// Narrows the aggregate bytes inspected during one fork publication.
    pub fn with_total_reference_bytes(mut self, maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "fork total reference limit must be positive".into(),
            ));
        }
        self.maximum_total_reference_bytes = maximum_bytes;
        Ok(self)
    }
}


impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkVerifier<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    async fn require_seed_file_with_io(&self, seed: &ForkSeed, file: &FileRef, io: &PreparationIo) -> Result<()> {
        let work = io.original_native_work().expect("native fork file verification");
        let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
        binding.require_seed(seed, work)?;
        file.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if file.volume().provider() != &self.host.provider {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork file belongs to another provider" });
        }
        let mut admitted = false;
        for selected in seed.inherited_context.iter()
            .chain(seed.reference_grants.iter().map(|grant| &grant.file))
            .chain(seed.attachment_manifests.iter())
            .chain(seed.summary.iter().flat_map(|summary| std::iter::once(&summary.context)
                .chain(summary.payloads.iter()).chain(summary.references.iter())))
        {
            fork_preparer_io::item(Some(io), 0)?;
            if selected == file { admitted = true; break; }
        }
        if !admitted {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "fork file is not in the original admitted seed inventory" });
        }
        self.host.verify_fork_allocation_io(seed, &seed.child_private_volume, Some(io)).await?;
        if binding.verifier.audience() == &seed.parent && file.volume() == &seed.child_private_volume {
            return Ok(());
        }
        binding.require_file(file, io)
    }

    async fn matches_selection_with_io(
        selected: &Generation<A, O>, source: &Generation<A, O>,
        paths: Option<Vec<String>>, io: &PreparationIo,
    ) -> Result<bool> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return selected.matches_fork_selection_with_native_work(source, paths, work, work.cancellation())
                .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
        }
        io.receive(selected.matches_fork_selection_measured(source, paths, io.remaining()?, io.cancellation()).await)
    }

    async fn changed_paths_with_io(
        initial: &Generation<A, O>, head: &Generation<A, O>, io: &PreparationIo,
    ) -> Result<Vec<acyclic_fs::ChangedPath>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            let _changes_custody;
            let (changes, custody) = initial.diff_to_with_native_work(head, u32::MAX, work, work.cancellation())
                .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
            _changes_custody = custody;
            let (paths, custody) = changes.changed_paths_with_native_work(u32::MAX, work, work.cancellation())
                .await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
            drop(changes);
            drop(_changes_custody);
            io.absorb_native_custody(custody)?;
            return Ok(paths);
        }
        let budget = io.remaining()?;
        let changes = match initial.diff_to_bounded(head, u32::MAX, budget, io.cancellation()).await {
            Ok(changes) => changes,
            Err(failure) => { io.spent(*failure.work)?; return Err(map_error(failure.error)); }
        };
        io.receive(changes.changed_paths_bounded(u32::MAX, budget, io.cancellation()).await)
    }

    async fn verify_with_io(&self, seed: &ForkSeed, io: &PreparationIo) -> Result<()> {
        let mut validation = io.history_io();
        seed.validate(&mut Some(&mut validation))?;
        drop(validation);
                if seed.child_private_volume.provider() == &self.host.provider {
                    self.host.verify_fork_allocation_io(seed, &seed.child_private_volume, Some(&io)).await?;
                }
                for resource in &seed.resources {
                    fork_preparer_io::item(Some(&io), 0)?;
                    if let (
                        ResourceRevision::Project { volume: source, generation: fork_point },
                        ResourceRevision::Project { volume: child, generation: initial },
                    ) = (&resource.source, &resource.revision)
                        && source.provider() == &self.host.provider
                    {
                        self.host.verify_fork_allocation_io(seed, child, Some(&io)).await?;
                        let source_ref = fork_preparer_io::content_workspace(&self.host.provider, source, Some(&io))?;
                        let child_ref = fork_preparer_io::content_workspace(&self.host.provider, child, Some(&io))?;
                        let source_workspace = fork_preparer_io::open(&self.host, &source_ref, &io).await?;
                        let child_workspace = fork_preparer_io::open(&self.host, &child_ref, &io).await?;
                        let source_generation = fork_preparer_io::generation(&self.host, &source_workspace, fork_point, &io).await?;
                        let child_generation = fork_preparer_io::generation(&self.host, &child_workspace, initial, &io).await?;
                        if !Self::matches_selection_with_io(&child_generation, &source_generation, None, io).await? {
                            return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "project capture is not forked from the selected generation"));
                        }
                    }
                }
                let mut unique_files: Vec<(String, &FileRef)> = Vec::new();
                for file in seed.inherited_context.iter()
                    .chain(seed.reference_grants.iter().map(|grant| &grant.file))
                    .chain(seed.attachment_manifests.iter())
                {
                    fork_preparer_io::item(Some(&io), 0)?;
                    if file.volume().provider() == &self.host.provider {
                        let key = fork_preparer_io::reference_key(file, Some(&io))?;
                        let owned = key.len() as u64;
                        if !fork_verify_insert(&mut unique_files, (key, file),
                            |left, right| left.0.cmp(&right.0), &io)? {
                            io.release(owned)?;
                        }
                    }
                }
                let mut total = 0_u64;
                for (_, file) in &unique_files {
                    fork_preparer_io::item(Some(&io), 0)?;
                    total = total.checked_add(file.descriptor().byte_length())
                        .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork reference bytes overflow"))?;
                    if total > self.maximum_total_reference_bytes {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork reference bytes exceed aggregate limit"));
                    }
                }
                if seed.child_private_volume.provider() == &self.host.provider {
                    let mut prefix = None;
                    for file in &seed.inherited_context {
                        fork_preparer_io::item(Some(&io), 0)?;
                        if file.path() == ".system/inherited-conversation/prefix.json" {
                            prefix = Some(file);
                            break;
                        }
                    }
                    if seed.inherited_through_sequence > 0 && prefix.is_none() {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork omitted its materialized conversation prefix"));
                    }
                    if let Some(file) = prefix {
                        if file.descriptor().media_type() != "application/vnd.acyclic.harness.inherited-conversation+json" {
                            return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "inherited conversation has the wrong media type"));
                        }
                        let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                        let inherited = io.decode_preparation::<InheritedConversationPrefix>(&bytes, crate::wire::ErrorCode::Storage)?;
                        let canonical_guard;
                        let canonical = io.encode(&*inherited, io.remaining()?.bytes_encoded, true)?;
                        canonical_guard = io.guard_bytes(&canonical)?;
                        if canonical.as_slice() != bytes.as_ref() || file.display_name() != "inherited-conversation.json" {
                            return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "inherited conversation is not canonical"));
                        }
                        if inherited.format_version != 3
                            || inherited.parent != seed.parent
                            || inherited.parent_revision != seed.parent_revision
                            || inherited.parent_agent == seed.child_agent
                            || inherited.through_sequence != seed.inherited_through_sequence
                            || !fork_verify_equal(&inherited.attached_agents, &seed.attached_agents, &io)?
                        {
                            return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "inherited conversation does not match the fork boundary"));
                        }
                        drop(inherited);
                        let bytes_owned = bytes.len() as u64;
                        drop(canonical);
                        drop(bytes);
                        drop(canonical_guard);
                        io.release(bytes_owned)?;
                    }
                }
                if let Some(summary) = &seed.summary
                    && summary.context.volume().provider() == &self.host.provider
                {
                    let _bytes_hold;
                    let bytes = self.host.read_pinned_io(&summary.context, self.maximum_inherited_bytes, Some(&io)).await?;
                    _bytes_hold = io.guard_owned(bytes.len() as u64)?;
                    let context = io.decode_preparation::<crate::context::Context>(&bytes, crate::wire::ErrorCode::Storage)?;
                    crate::context::validate_projected_context(&context, summary.selection.limits, Some(&io))?;
                    let _canonical_hold;
                    let canonical = io.encode(&*context, io.remaining()?.bytes_encoded, true)?;
                    _canonical_hold = io.guard_bytes(&canonical)?;
                    if context.current_input_index.is_some() || canonical.as_slice() != bytes.as_ref() {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "summary capture is not a canonical historical Context"));
                    }
                    let _observed_hold;
                    let mut observed = Vec::new();
                    {
                        let mut examine = |file: &FileRef| -> Result<()> {
                            fork_preparer_io::item(Some(&io), 0)?;
                            let digest = task_runtime::observed::digest(file, Some(&io), u64::MAX)?;
                            fork_verify_insert(&mut observed, digest, Ord::cmp, &io).map(|_| ())
                        };
                        for message in &context.messages {
                            for file in message.content.file_refs() { examine(file)?; }
                        }
                        for file in context.metadata.values() {
                            examine(file)?;
                        }
                    }
                    _observed_hold = io.guard_owned((observed.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
                    let _declared_hold;
                    let mut declared = Vec::new();
                    for file in summary.payloads.iter().chain(&summary.references) {
                        fork_preparer_io::item(Some(&io), 0)?;
                        let digest = task_runtime::observed::digest(file, Some(&io), u64::MAX)?;
                        fork_verify_insert(&mut declared, digest, Ord::cmp, &io)?;
                    }
                    _declared_hold = io.guard_owned((declared.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
                    if !fork_verify_equal(&observed, &declared, &io)? {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "summary capture reference inventory differs from its Context"));
                    }
                }
                if seed.child_private_volume.provider() == &self.host.provider {
                    let private = fork_preparer_io::content_workspace(&self.host.provider, &seed.child_private_volume, Some(&io))?;
                    let private_head = fork_preparer_io::resolve(&self.host, &private, Some(&io)).await?.generation;
                    if private_head != seed.child_private_generation {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Conflict, "child private generation changed before fork admission"));
                    }
                    let mut scratch = None;
                    for resource in &seed.resources {
                        fork_preparer_io::item(Some(&io), 0)?;
                        if let (
                            ResourceRevision::PrivateVolume { volume: source, generation, paths },
                            ResourceRevision::PrivateVolume { volume: child, generation: initial, .. },
                        ) = (&resource.source, &resource.revision)
                            && child == &seed.child_private_volume
                        {
                            scratch = Some((source, generation, paths, initial));
                            break;
                        }
                    }
                    let scratch = scratch.ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork has no selected parent scratch lineage"))?;
                    let source_ref = fork_preparer_io::content_workspace(&self.host.provider, scratch.0, Some(&io))?;
                    let source_workspace = fork_preparer_io::open(&self.host, &source_ref, &io).await?;
                    let child_workspace = fork_preparer_io::open(&self.host, &private, &io).await?;
                    let source = fork_preparer_io::generation(&self.host, &source_workspace, scratch.1, &io).await?;
                    let initial = fork_preparer_io::generation(&self.host, &child_workspace, scratch.3, &io).await?;
                    let mut paths = Vec::new();
                    fork_verify_reserve(&mut paths, scratch.2.len(), &io)?;
                    for path in scratch.2 {
                        fork_preparer_io::item(Some(&io), 0)?;
                        paths.push(fork_preparer_io::clone_text(path, Some(&io))?);
                    }
                    let selected_bytes = paths.capacity() * std::mem::size_of::<String>()
                        + paths.iter().map(String::len).sum::<usize>();
                    if !Self::matches_selection_with_io(&initial, &source, Some(paths), io).await? {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "scratch capture differs from its pinned selection"));
                    }
                    io.release(selected_bytes as u64)?;
                    let head = fork_preparer_io::generation(&self.host, &child_workspace, &private_head, &io).await?;
                    let changes = Self::changed_paths_with_io(&initial, &head, io).await?;
                    let mut changed_owned = (changes.capacity() * std::mem::size_of::<acyclic_fs::ChangedPath>()) as u64;
                    for change in &changes {
                        changed_owned = changed_owned.checked_add(std::mem::size_of_val(change.path.components()) as u64)
                            .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "changed path ownership overflow"))?;
                        for component in change.path.components() {
                            changed_owned = changed_owned.checked_add(component.as_bytes().len() as u64)
                                .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "changed path ownership overflow"))?;
                        }
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    if io.original_native_work().is_none() { io.retain_owned(changed_owned)?; }
                    #[cfg(target_arch = "wasm32")]
                    io.retain_owned(changed_owned)?;
                    let mut expected = Vec::new();
                    let version_bytes = private_head.as_resource().key().len().checked_mul(2)
                        .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "fork generation encoding overflow"))?;
                    io.claim(version_bytes as u64)?;
                    io.allocation_attempt(version_bytes as u64)?;
                    let version = hex::encode(private_head.as_resource().key());
                    for file in &seed.inherited_context {
                        fork_preparer_io::item(Some(&io), 0)?;
                        if file.version() != version {
                            if seed.summary.is_none() {
                                return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "inherited file is not pinned to the selected private generation"));
                            }
                            let selected = crate::executor::with_history_io(Some(&io), |work| FileRef::new(
                                file.volume().copy(work)?, fork_preparer_io::clone_text(file.path(), Some(&io))?,
                                fork_preparer_io::clone_text(&version, Some(&io))?,
                                FileDescriptor::new(*file.descriptor().sha256(), file.descriptor().byte_length(),
                                    fork_preparer_io::clone_text(file.descriptor().media_type(), Some(&io))?)?,
                                fork_preparer_io::clone_text(file.display_name(), Some(&io))?, work))?;
                            let original = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                            let final_bytes = self.host.read_pinned_io(&selected, self.maximum_inherited_bytes, Some(&io)).await?;
                            if original != final_bytes {
                                return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "summary inherited payload changed before admission"));
                            }
                            let original_owned = original.len() as u64;
                            let final_owned = final_bytes.len() as u64;
                            drop(original);
                            drop(final_bytes);
                            io.release(original_owned)?;
                            io.release(final_owned)?;
                        }
                        let path = crate::contract::preparation_format(format_args!("/{}", file.path()), Some(&io))?;
                        let owned = path.len() as u64;
                        if !fork_verify_insert(&mut expected, path, Ord::cmp, &io)? { io.release(owned)?; }
                    }
                    if expected.len() != seed.inherited_context.len() {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "inherited context has duplicate paths"));
                    }
                    let mut expected_directories = Vec::new();
                    fork_verify_insert(&mut expected_directories, fork_preparer_io::clone_text("/", Some(&io))?, Ord::cmp, &io)?;
                    for path in &expected {
                        let mut parent = path.as_str();
                        while let Some((directory, _)) = parent.rsplit_once('/') {
                            fork_preparer_io::item(Some(&io), 0)?;
                            if directory.is_empty() { break; }
                            let directory_copy = fork_preparer_io::clone_text(directory, Some(&io))?;
                            if !fork_verify_insert(&mut expected_directories, directory_copy, Ord::cmp, &io)? {
                                io.release(directory.len() as u64)?;
                            }
                            parent = directory;
                        }
                    }
                    let mut observed = Vec::new();
                    for change in &changes {
                        fork_preparer_io::item(Some(&io), 0)?;
                        let mut length = 1_usize;
                        for (index, name) in change.path.components().iter().enumerate() {
                            fork_preparer_io::item(Some(&io), 0)?;
                            std::str::from_utf8(name.as_bytes())
                                .map_err(|_| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "scratch path is not UTF-8"))?;
                            length = length.checked_add(name.as_bytes().len()).and_then(|bytes| bytes.checked_add(usize::from(index > 0)))
                                .ok_or_else(|| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "scratch path size overflow"))?;
                        }
                        io.claim(length as u64)?;
                        io.allocation_attempt(length as u64)?;
                        let copied = acyclic_fs::WorkCounters { bytes_copied: (length - 1) as u64, ..Default::default() };
                        io.preflight(copied)?;
                        io.charge(copied)?;
                        let mut path = String::with_capacity(length);
                        path.push('/');
                        for (index, name) in change.path.components().iter().enumerate() {
                            if index > 0 { path.push('/'); }
                            path.push_str(std::str::from_utf8(name.as_bytes())
                                .map_err(|_| interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "scratch path is not UTF-8"))?);
                        }
                        match change.after {
                            Some(record) if record.kind == FileKind::Directory
                                && fork_verify_contains(&expected_directories, &path, &io)? && change.before.is_none() => { io.release(length as u64)?; }
                            Some(record) if record.kind == FileKind::Regular
                                && fork_verify_contains(&expected, &path, &io)? && change.before.is_none() => {
                                if !fork_verify_insert(&mut observed, path, Ord::cmp, &io)? { io.release(length as u64)?; }
                            }
                            _ => return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "child scratch changed outside inherited context staging")),
                        }
                    }
                    if !fork_verify_equal(&observed, &expected, &io)? {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Invalid, "child private volume lacks selected inherited context"));
                    }
                    if fork_preparer_io::resolve(&self.host, &private, Some(&io)).await?.generation != private_head {
                        return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Conflict, "child private volume changed during fork admission"));
                    }
                    fork_preparer_io::retain(&head, Some(&io)).await?;
                    drop(changes);
                    drop(version);
                    io.release(changed_owned)?;
                    io.release(version_bytes as u64)?;
                    for collection in [expected, expected_directories, observed] {
                        let owned = (collection.capacity() * std::mem::size_of::<String>()) as u64
                            + collection.iter().map(|path| path.capacity() as u64).sum::<u64>();
                        drop(collection);
                        io.release(owned)?;
                    }
                }
                for resource in &seed.resources {
                    for revision in [&resource.source, &resource.revision] {
                        fork_preparer_io::item(Some(&io), 0)?;
                        if revision.provider() != &self.host.provider { continue; }
                        match revision {
                            ResourceRevision::Project { volume, generation }
                            | ResourceRevision::PrivateVolume { volume, generation, .. } => {
                                let reference = fork_preparer_io::content_workspace(&self.host.provider, volume, Some(&io))?;
                                let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
                                let generation = fork_preparer_io::generation(&self.host, &workspace, generation, &io).await?;
                                fork_preparer_io::retain(&generation, Some(&io)).await?;
                            }
                            ResourceRevision::SharedVolume(volume) => {
                                let reference = fork_preparer_io::content_workspace(&self.host.provider, volume, Some(&io))?;
                                fork_preparer_io::resolve(&self.host, &reference, Some(&io)).await?;
                            }
                            ResourceRevision::History(_) => return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Unsupported, "Filesystem cannot prove Stream history")),
                            ResourceRevision::Context(_) | ResourceRevision::Process(_)
                            | ResourceRevision::Artifact(_) | ResourceRevision::Extension { .. } => {
                                return Err(interaction_host::interaction_boundary(Some(io), crate::wire::ErrorCode::Unsupported, "fork resource requires its owning provider verifier"));
                            }
                        }
                    }
                }
                for (_, reference) in &unique_files {
                    let bytes = self.host.read_pinned_io(reference, self.maximum_inherited_bytes, Some(&io)).await?;
                    let owned = bytes.len() as u64;
                    drop(bytes);
                    io.release(owned)?;
                }
                Ok(())
    }
}

impl<A, O> ForkSeedVerifier for FilesystemForkVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn provider(&self) -> &ProviderRef {
        &self.host.provider
    }

    fn maximum_reference_bytes(&self) -> u64 {
        self.maximum_total_reference_bytes
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self, seed: &'a ForkSeed, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
            binding.require_seed(seed, work)?;
            for file in seed.inherited_context.iter()
                .chain(seed.reference_grants.iter().map(|grant| &grant.file))
                .chain(seed.attachment_manifests.iter())
                .chain(seed.summary.iter().flat_map(|summary| std::iter::once(&summary.context)
                    .chain(summary.payloads.iter()).chain(summary.references.iter())))
            {
                if file.volume().provider() == &self.host.provider { self.require_seed_file_with_io(seed, file, &io).await?; }
            }
            self.verify_with_io(seed, &io).await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_boundary_with_native_work<'a>(
        &'a self, _boundary: &'a crate::fork::AttestedBoundary,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "Filesystem provider does not attest fork boundaries" })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_file_with_native_work<'a>(
        &'a self, seed: &'a ForkSeed, file: &'a FileRef, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.require_seed_file_with_io(seed, file, &io).await?;
            let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
            drop(bytes);
            Ok(())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_manifest_with_native_work<'a>(
        &'a self, seed: &'a ForkSeed, file: &'a FileRef, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.require_seed_file_with_io(seed, file, &io).await?;
            let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
            drop(io);
            Ok(bytes)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn acquire_private_fence_with_native_work<'a>(
        &'a self, seed: &'a ForkSeed, work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Box<dyn ForkPublicationGuard>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let binding = self.native_binding.as_ref().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "Filesystem verifier has no native authority" })?;
            binding.require_seed(seed, work)?;
            if seed.child_private_volume.provider() != &self.host.provider
                || (binding.verifier.audience() != &seed.parent
                    && !io.contains_capability(binding.scope.capabilities(), &seed.child_private_volume, "volume:write:")?)
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "fork private publication fence lacks the exact current provider write grant" });
            }
            self.host.verify_fork_allocation_io(seed, &seed.child_private_volume, Some(&io)).await?;
            let reference = fork_preparer_io::content_workspace(&self.host.provider, &seed.child_private_volume, Some(&io))?;
            let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
            let native_binding = NativeFilesystemVerificationBinding::new(&binding.verifier, &binding.scope, work)?;
            let header;
            let mut guard = {
                header = work.claim(std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64)
                    .map_err(Error::from)?;
                Box::new(FilesystemForkPublicationGuard {
                    workspace, reservation: None, native_work: Some(work.clone()),
                    native_binding: Some(native_binding),
                })
            };
            let operation = acyclic_fs::OperationId::from_bytes(seed.operation_id.into_bytes());
            let reservation = match guard.workspace.reserve_publication_with_native_work(
                operation, work, work.cancellation()).await
            {
                Ok(reservation) => reservation,
                Err((source, custody)) => {
                    let primary = Error::NativeWorkspace { source, custody };
                    if let Ok(Some(reservation)) = guard.workspace.publication_reservation_with_native_work(
                        operation, work, work.cancellation()).await
                    {
                        guard.reservation = Some(reservation);
                        let _cleanup = guard.workspace.release_publication_with_native_work(
                            reservation, work, work.cancellation()).await;
                    }
                    return Err(primary);
                }
            };
            guard.reservation = Some(reservation);
            Ok((guard as Box<dyn ForkPublicationGuard>, header))
        })
    }

    fn acquire_private_fence<'a>(
        &'a self,
        seed: &'a ForkSeed,
    ) -> BoxFuture<'a, Result<Box<dyn ForkPublicationGuard>>> {
        Box::pin(async move {
            self.acquire_private_fence_measured(
                seed,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
        })
    }

    fn acquire_private_fence_measured<'a>(
        &'a self,
        seed: &'a ForkSeed,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Box<dyn ForkPublicationGuard>>, Error>> {
        Box::pin(async move {
            let io = if budget == acyclic_fs::WorkBudget::UNBOUNDED {
                PreparationIo::for_unmeasured(cancellation)
            } else {
                PreparationIo::from_budget(budget, cancellation)
            }.map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                if seed.child_private_volume.provider() != &self.host.provider {
                    return Err(Error::Unauthorized("fork private volume belongs to another provider".into()));
                }
                let reference = fork_preparer_io::content_workspace(
                    &self.host.provider, &seed.child_private_volume, Some(&io),
                )?;
                let workspace = fork_preparer_io::open(&self.host, &reference, &io).await?;
                let guard_bytes = std::mem::size_of::<FilesystemForkPublicationGuard<A, O>>() as u64;
                io.claim(guard_bytes)?;
                io.allocation_attempt(guard_bytes)?;
                let mut guard = Box::new(FilesystemForkPublicationGuard {
                    workspace, reservation: None,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_work: None,
                    #[cfg(not(target_arch = "wasm32"))]
                    native_binding: None,
                });
                let receipt = guard.workspace.reserve_publication(
                    acyclic_fs::OperationId::from_bytes(seed.operation_id.into_bytes()),
                    io.remaining()?,
                    cancellation,
                ).await;
                let receipt = match receipt {
                    Ok(receipt) => receipt,
                    Err(failure) => {
                        io.spent(*failure.work)?;
                        return Err(map_error(failure.error));
                    }
                };
                guard.reservation = Some(receipt.value);
                io.charge(receipt.work)?;
                Ok(guard as Box<dyn ForkPublicationGuard>)
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    fn read_manifest<'a>(&'a self, manifest: &'a FileRef) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>> {
        Box::pin(async move {
            self.read_manifest_measured(manifest, acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()).await
                .map(|receipt| receipt.value.0).map_err(|failure| failure.error)
        })
    }

    fn read_manifest_measured<'a>(
        &'a self, manifest: &'a FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<(acyclic_stream::OwnedBytes, u64)>, Error>> {
        Box::pin(async move {
            let io = if budget == acyclic_fs::WorkBudget::UNBOUNDED {
                PreparationIo::for_unmeasured(cancellation)
            } else {
                PreparationIo::from_budget(budget, cancellation)
            }.map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                if manifest.volume().provider() != &self.host.provider {
                    return Err(fork_preparer_io::failure(Some(&io), crate::wire::ErrorCode::Unauthorized,
                        "fork manifest belongs to another provider"));
                }
                let bytes = self.host.read_pinned_io(manifest,
                    self.maximum_inherited_bytes, Some(&io)).await?;
                let external_bytes = bytes.retained_capacity().ok_or_else(|| {
                    fork_preparer_io::failure(Some(&io), crate::wire::ErrorCode::Unsupported,
                        "manifest producer does not expose its admitted owned capacity")
                })?;
                Ok((bytes, external_bytes as u64))
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    fn verify_file<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verify_file_measured(file, acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()).await
                .map(|receipt| receipt.value).map_err(|failure| failure.error)
        })
    }

    fn verify_file_measured<'a>(
        &'a self, file: &'a FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let receipt = self.read_manifest_measured(file, budget, cancellation).await?;
            let (bytes, _) = receipt.value;
            drop(bytes);
            Ok(acyclic_fs::OperationReceipt { value: (), work: receipt.work })
        })
    }

    fn verify<'a>(&'a self, seed: &'a ForkSeed) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verify_measured(seed, acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()).await
                .map(|receipt| receipt.value).map_err(|failure| failure.error)
        })
    }

    #[allow(clippy::too_many_lines, reason = "one canonical fork admission barrier")]
    fn verify_measured<'a>(
        &'a self, seed: &'a ForkSeed, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = if budget == acyclic_fs::WorkBudget::UNBOUNDED {
                PreparationIo::for_unmeasured(cancellation)
            } else {
                PreparationIo::from_budget(budget, cancellation)
            }.map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.verify_with_io(seed, &io).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    fn verify_boundary<'a>(&'a self, boundary: &'a crate::fork::AttestedBoundary) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verify_boundary_measured(boundary, acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()).await
                .map(|receipt| receipt.value).map_err(|failure| failure.error)
        })
    }

    fn verify_boundary_measured<'a>(
        &'a self, _boundary: &'a crate::fork::AttestedBoundary,
        _budget: acyclic_fs::WorkBudget, _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async {
            Err(acyclic_fs::OperationFailure::before_work(Error::Unsupported(
                "provider does not attest fork boundaries".into(),
            )))
        })
    }
}

/// Resolved mutable workspace head and immutable generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceObservation {
    /// Provider-owned mutable workspace identity.
    pub workspace: WorkspaceRef,
    /// Exact immutable head observed by this operation.
    pub generation: GenerationRef,
}

/// One code-defined member of an atomic workspace mutation batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceMutation {
    /// Creates every missing directory on an absolute path.
    CreateDirectory {
        /// Canonical absolute path.
        path: String,
    },
    /// Creates or replaces one complete file.
    PutFile {
        /// Canonical absolute path.
        path: String,
        /// Complete file content.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Removes one file or empty directory.
    Remove {
        /// Canonical absolute path.
        path: String,
    },
}

/// Reserved host-owned file families.
#[derive(Clone, Copy)]
pub(crate) enum InternalContentClass {
    Interaction,
    Workflow,
    Execution,
    ModelPrefix,
    Conversation,
}

impl InternalContentClass {
    fn prefix(self) -> &'static str {
        match self {
            Self::Interaction => ".system/interactions/",
            Self::Workflow => ".system/workflows/",
            Self::Execution => ".system/execution/",
            Self::ModelPrefix => ".system/inherited-conversation/model-prefix/",
            Self::Conversation => "turns/.canonical/",
        }
    }
}

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
pub struct FilesystemHost<A, O> {
    filesystem: Fs<A, O>,
    provider: ProviderRef,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Provider-side proof for a parent-published project merge receipt.
pub struct FilesystemProjectMergeVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_binding: Option<NativeFilesystemVerificationBinding>,
}

impl UploadRecord for JoinCommitWitness {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(std::mem::size_of::<Self>() as u64)
    }
}

impl fork_preparer_io::PreparationRecord for JoinCommitWitness {
    fn maximum_slot_bytes() -> u64 {
        std::mem::size_of::<serde_json::Value>().max(std::mem::size_of::<Self>()) as u64
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_project_join_key(operation: crate::OperationId, work: &acyclic_fs::NativeMaterializationWork) -> Result<FilesystemKey> {
    use std::io::Write as _;
    work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })
        .map_err(Error::from)?;
    let mut buffer = [0_u8; 64];
    let mut output = buffer.as_mut_slice();
    write!(&mut output, "project-join:{operation}").map_err(|_| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason: "project join retry key exceeds its fixed encoding" })?;
    let length = 64 - output.len();
    work.charge(acyclic_fs::WorkCounters { bytes_hashed: length as u64, ..Default::default() })
        .map_err(Error::from)?;
    let digest = blake3::hash(&buffer[..length]);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] { bytes[0] = 1; }
    Ok(FilesystemKey::from_bytes(bytes))
}

impl<A, O> FilesystemProjectMergeVerifier<A, O> {
    /// Binds verification to one exact Filesystem provider identity.
    #[must_use]
    pub fn new(host: Arc<FilesystemHost<A, O>>) -> Self {
        Self {
            host,
            #[cfg(not(target_arch = "wasm32"))]
            native_binding: None,
        }
    }

    /// Binds the original parent publication MAC and current grants.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, verifier: &AuthorityVerifier,
        scope: &Scope, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        host.provider.validate_with_native_work(work)?;
        let binding = NativeFilesystemVerificationBinding::new(verifier, scope, work)?;
        Ok(Self { host, native_binding: Some(binding) })
    }



    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(mut self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        let binding = self.native_binding.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, reason: "merge verifier has no native authority" })?;
        binding.authenticate(work)?;
        let mut header = work.claim(std::mem::size_of::<Self>() as u64)
            .map_err(Error::from)?;
        binding.custody.absorb(&mut header)
            .map_err(|cause| Error::NativeWork { cause, custody: Some(header) })?;
        Ok(Arc::new(self))
    }
}

impl<A, O> ProjectMergeVerifier for FilesystemProjectMergeVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(&'a self, receipt: &'a ProjectMergeReceipt) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if receipt.source_project.provider() != &self.host.provider
                || receipt.target_project.provider() != &self.host.provider
                || receipt.provider_proof.provider != self.host.provider
                || receipt.provider_operation_id.as_slice()
                    != project_join_key(receipt.operation_id)?
                        .into_bytes()
                        .as_slice()
            {
                return Err(Error::Invalid(
                    "merge receipt belongs to another provider or retry identity".into(),
                ));
            }
            receipt.provider_proof.validate()?;
            let source = workspace_ref(
                self.host.provider.clone(),
                &receipt.source_project.storage_name()?,
            )?;
            let target = workspace_ref(
                self.host.provider.clone(),
                &receipt.target_project.storage_name()?,
            )?;
            let source_workspace = self.host.open(&source).await?;
            let target_workspace = self.host.open(&target).await?;
            if receipt.provider_proof.format != FILESYSTEM_JOIN_PROOF_FORMAT {
                return Err(Error::Invalid(
                    "merge receipt has an unsupported provider proof".into(),
                ));
            }
            let witness: JoinCommitWitness =
                serde_json::from_value(receipt.provider_proof.statement.clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
            let source_generation = self
                .host
                .generation(&source_workspace, &receipt.source_generation)
                .await?;
            let expected = self
                .host
                .generation(&target_workspace, &receipt.expected_target_generation)
                .await?;
            let result = self
                .host
                .generation(&target_workspace, &receipt.result_generation)
                .await?;
            if witness.source_workspace != source_workspace.id().volume_id()
                || witness.target_workspace != target_workspace.id().volume_id()
                || witness.source_generation != source_generation.id()
                || witness.expected_target != expected.id()
                || witness.result_generation != result.id()
                || witness.operation_id.into_bytes().as_slice()
                    != receipt.provider_operation_id.as_slice()
                || witness.history != JoinHistory::Merge
                || !target_workspace
                    .verify_join_commit(&witness)
                    .await
                    .map_err(map_error)?
            {
                return Err(Error::Invalid(
                    "merge receipt does not prove the committed Filesystem join".into(),
                ));
            }
            let normalized_source = source_generation
                .normalized_join_parent_for(&expected)
                .await
                .map_err(map_error)?;
            let parents = result.parents().await.map_err(map_error)?;
            if parents.as_slice() != [expected.id(), normalized_source] {
                return Err(Error::Invalid(
                    "merge result does not bind the inspected child and target generations".into(),
                ));
            }
            self.host
                .retain_generation(&source, &receipt.source_generation)
                .await?;
            self.host
                .retain_generation(&target, &receipt.expected_target_generation)
                .await?;
            self.host
                .retain_generation(&target, &receipt.result_generation)
                .await?;
            Ok(())
        })
    }
}

/// Parent-owned project operations.
pub struct ParentProjectController<'a, A, O> {
    host: &'a FilesystemHost<A, O>,
    parent: Authority,
    project: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Inspected project changes whose publication remains bound to the parent controller.
pub struct ParentMergePlan<A, O> {
    plan: JoinPlan<A, O>,
    child_project: VolumeRef,
    parent_project: VolumeRef,
    parent_scope: Scope,
    #[cfg(not(target_arch = "wasm32"))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Destination-authorized volume import.
pub struct VolumeImportPlan<A, O> {
    plan: JoinPlan<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> VolumeImportPlan<A, O> {
    /// Publishes only to the authorized destination using a stable retry key.
    pub async fn apply(&self, key: &IdempotencyKey) -> Result<JoinOutcome<A, O>> {
        self.plan
            .apply(ApplyOptions {
                if_target: self.plan.target_head(),
                idempotency_key: filesystem_key(key),
            })
            .await
            .map_err(map_error)
    }
}

/// Prepared child-owned prefix and the exact read grants needed by every attached reader.
pub struct InheritedContextCapture {
    /// Child-owned inherited context file.
    pub file: FileRef,
    /// Pinned child-private generation containing the file.
    pub generation: GenerationRef,
    /// Exact attached-reader grants for referenced files.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Pinned attachment-list manifests admitted with the context.
    pub attachment_manifests: Vec<FileRef>,
}

impl<A, O> ParentMergePlan<A, O> {
    /// Exact parent generation observed while planning; publication uses this CAS target.
    #[must_use]
    pub const fn target_head(&self) -> GenerationId {
        self.plan.target_head()
    }

    /// Exact child generation whose changes were inspected.
    #[must_use]
    pub const fn source_head(&self) -> GenerationId {
        self.plan.source_head()
    }


}

impl<'a, A, O> ParentProjectController<'a, A, O> {
    /// Binds the authenticated parent and project using optional original preparation IO.
    pub fn new(
        host: &'a FilesystemHost<A, O>, parent: &Reducer,
        verifier: &AuthorityVerifier, scope: &Scope, project: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if parent.authority().kind != crate::core::AggregateKind::Conversation {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                "project controller requires a parent conversation"));
        }
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        verifier.verify_audience(parent.authority(), &mut work)?;
        verifier.verify(scope, &mut work)?;
        if parent.conversation().and_then(|state| state.agent) != scope.agent()
            || scope.agent().is_none()
        {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Unauthorized,
                "project controller requires the bound parent agent"));
        }
        project.validate(&mut work)?;
        if project.provider() != &host.provider {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                "parent project belongs to another provider"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let parent = match work.as_deref_mut() {
            Some(work) => work.clone_authority(parent.authority())?,
            None => parent.authority().clone(),
        };
        let verifier = verifier.copy(&mut work)?;
        let scope = scope.copy(&mut work)?;
        let project = project.copy(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let native_custody = io.filter(|io| io.original_native_work().is_some()).map(|io| {
            io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| io.invalid("parent project controller custody underflows"))?)
        }).transpose()?;
        Ok(Self { host, parent, project, verifier, scope,
            #[cfg(not(target_arch = "wasm32"))] native_custody })
    }

    fn require(&self, capability: &str, operation: VolumeOperation) -> Result<()> {
        ContentGrant::verify(&self.verifier, &self.scope, &self.project, operation, u64::MAX, &mut None)?;
        if !self.scope.capabilities().contains(capability) {
            return Err(Error::Unauthorized(
                "parent project operation is not granted".into(),
            ));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn require_with_native_work(
        &self, capability: &str, operation: VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<PreparationIo> {
        if !self.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent project controller belongs to another original resource" });
        }
        self.verifier.verify_audience(&self.parent, &mut Some(&mut crate::store::history::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
        self.verifier.verify_with_native_work(&self.scope, work)?;
        let io = PreparationIo::from_native_work(work)?;
        let prefix = match operation {
            VolumeOperation::Read => "volume:read:",
            VolumeOperation::Write => "volume:write:",
        };
        if !self.scope.capabilities().contains(capability)
            || !io.contains_capability(self.scope.capabilities(), &self.project, prefix)?
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent project operation is not currently granted" });
        }
        Ok(io)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ParentProjectController<'_, A, O> {
    /// Freezes a completed exchange from the authoritative execution journal before retaining its shared segment.
    #[allow(
        clippy::too_many_arguments,
        reason = "one exact journal exchange and direct-parent binding"
    )]
    pub async fn stage_completed_model_prefix(
        &self,
        private: &VolumeRef,
        executor: &crate::executor::StockExecutor,
        journal: &dyn crate::executor::ExecutionJournal,
        input: &crate::executor::TurnInput,
        step: u32,
        call_id: &str,
        parent: Option<(&FileRef, &crate::model::PreparedModelRequest)>,
    ) -> Result<(FileRef, crate::model::PreparedModelRequest)> {
        let prepared = executor
            .completed_tool_prefix(journal, input, step, Some(call_id))
            .await?;
        let segment = crate::model::ModelPrefix::select(&prepared, parent)?;
        let reference = self.stage_model_prefix(private, &segment).await?;
        Ok((reference, prepared))
    }

    /// Retains one immutable model segment in the parent's private volume.
    pub async fn stage_model_prefix(
        &self,
        private: &VolumeRef,
        prefix: &crate::model::ModelPrefix,
    ) -> Result<FileRef> {
        self.require("fork:publish", VolumeOperation::Read)?;
        if private.provider() != &self.host.provider
            || private.class() != VolumeClass::AgentPrivate
            || private.owner()
                != &VolumeOwner::Agent(
                    self.scope.agent().ok_or_else(|| {
                        Error::Unauthorized("model prefix parent is unbound".into())
                    })?,
                )
        {
            return Err(Error::Unauthorized(
                "model prefix is not parent owned".into(),
            ));
        }
        let grant =
            ContentGrant::verify(&self.verifier, &self.scope, private, VolumeOperation::Write, u64::MAX, &mut None)?;
        let bytes = crate::contract::canonical_json_bytes(&prefix)?;
        let digest = blake3::hash(&bytes).to_hex();
        self.host
            .put_internal_content(
                private,
                &grant,
                &format!(".system/inherited-conversation/model-prefix/{digest}.json"),
                &bytes,
                crate::model::ModelPrefix::MEDIA_TYPE,
                "model-prefix.json",
                u64::MAX,
                &IdempotencyKey::new(format!("model-prefix:{digest}"))?,
                InternalContentClass::ModelPrefix,
            )
            .await
    }

    /// Materializes an exact bounded parent prefix into a fresh child-private workspace.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "materializes one exact bounded parent prefix"
    )]
    pub async fn materialize_inherited_conversation(
        &self,
        parent: &Reducer,
        child_private: &VolumeRef,
        initial_generation: &GenerationRef,
        child_agent: AgentId,
        attached_agents: &[AgentId],
        through_sequence: u64,
        maximum_messages: usize,
        maximum_bytes: u64,
        maximum_references: usize,
        resolver: &dyn ContentResidencyVerifier,
        idempotency_key: &IdempotencyKey,
    ) -> Result<InheritedContextCapture> {
        self.materialize_inherited_conversation_continued(
            parent,
            child_private,
            initial_generation,
            child_agent,
            attached_agents,
            through_sequence,
            maximum_messages,
            maximum_bytes,
            maximum_references,
            resolver,
            idempotency_key,
            &[],
            None,
            None,
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await
    }

    /// Projects the exact inherited reader capabilities before child admission, using the same traversal as materialization.
    #[allow(clippy::too_many_arguments, reason = "original history, readers and finite bounds remain explicit")]
    pub async fn inherited_reference_grants(
        &self, parent: &Reducer, child_agent: AgentId, attached_agents: &[AgentId],
        through_sequence: u64, maximum_messages: usize, maximum_bytes: u64,
        maximum_references: usize, resolver: &dyn ContentResidencyVerifier,
        archive: Option<&crate::fork::PreparedForkHistory>,
    ) -> Result<Vec<ReferenceGrant>> {
        self.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        if let Some(archive) = archive {
            archive.verify(&self.verifier)?;
            if archive.parent() != parent.authority() || archive.parent_revision() != parent.revision() {
                return Err(Error::Conflict("inherited archive is not the original parent cut".into()));
            }
        }
        let history = self.selected_inherited_history(parent, through_sequence, maximum_messages,
            archive.map(crate::fork::PreparedForkHistory::conversation), None)?;
        let (grants, _) = Self::collect_inherited_reference_grants(history, child_agent,
            attached_agents, through_sequence, maximum_bytes, maximum_references, resolver, None).await?;
        Ok(grants)
    }

    /// Projects inherited grants with the same collector and original payer.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original cut and authenticated readers remain explicit")]
    pub async fn inherited_reference_grants_with_native_work(
        &self, parent: &Reducer, child_agent: AgentId, attached_agents: &[AgentId],
        through_sequence: u64, maximum_messages: usize, maximum_bytes: u64,
        maximum_references: usize, resolver: &dyn ContentResidencyVerifier,
        archive: Option<&crate::fork::PreparedForkHistory>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<ReferenceGrant>, acyclic_fs::NativeAllocation)> {
        let io = self.require_with_native_work(capability::FORK_PUBLISH, VolumeOperation::Read, work)?;
        if let Some(archive) = archive {
            let mut observer = io.history_io();
            archive.verify_with_work(&self.verifier, &mut Some(&mut observer))?;
            if archive.parent() != parent.authority() || archive.parent_revision() != parent.revision() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "inherited archive is not the original parent cut" });
            }
        }
        let history = self.selected_inherited_history(parent, through_sequence, maximum_messages,
            archive.map(crate::fork::PreparedForkHistory::conversation), Some(&io))?;
        let (grants, manifests) = Self::collect_inherited_reference_grants(history, child_agent,
            attached_agents, through_sequence, maximum_bytes, maximum_references, resolver, Some(&io)).await?;
        drop(manifests);
        let mut retained = (grants.capacity() as u64).checked_mul(std::mem::size_of::<ReferenceGrant>() as u64)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "inherited grant output ownership overflows" })?;
        for grant in &grants {
            let manifest = grant.attachment_manifest.as_ref()
                .map(FileRef::logical_owned_bytes).transpose()?.unwrap_or(0);
            retained = retained.checked_add(grant.file.logical_owned_bytes()?)
                .and_then(|sum| sum.checked_add(manifest))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "inherited grant output ownership overflows" })?;
        }
        let custody = io.into_native_custody(retained)?;
        Ok((grants, custody))
    }

    fn selected_inherited_history<'p>(
        &self, parent: &'p Reducer, through_sequence: u64, maximum_messages: usize,
        archive: Option<&'p crate::conversation::ConversationState>,
        io: Option<&PreparationIo>,
    ) -> Result<&'p crate::conversation::ConversationState> {
        let rejected = |code: crate::wire::ErrorCode, reason: &'static str| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(PreparationIo::original_native_work).is_some() {
                return Error::NativeBoundary { code, reason };
            }
            match code {
                crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
                crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
                _ => Error::Invalid(reason.into()),
            }
        };
        if parent.authority() != &self.parent
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent() {
            return Err(rejected(crate::wire::ErrorCode::Unauthorized, "inherited history is outside the actual parent"));
        }
        let parent_history = parent.conversation()
            .ok_or_else(|| rejected(crate::wire::ErrorCode::Invalid, "parent is not a conversation"))?;
        let history = archive.unwrap_or(parent_history);
        if history.agent != parent_history.agent || through_sequence > parent_history.logical_revision()
            || (archive.is_some() && (history.resident_after_sequence() != 0
                || history.logical_revision() != through_sequence)) {
            return Err(rejected(crate::wire::ErrorCode::Conflict, "inherited archive differs from its original parent cut"));
        }
        let count = usize::try_from(through_sequence)
            .map_err(|_| fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited prefix is too large"))?;
        if count > maximum_messages || count > history.messages.len() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited prefix exceeds the selected history or limit"));
        }
        Ok(history)
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines, reason = "one canonical bounded reference projection")]
    async fn collect_inherited_reference_grants(
        history: &crate::conversation::ConversationState, child_agent: AgentId,
        attached_agents: &[AgentId], through_sequence: u64, maximum_bytes: u64,
        maximum_references: usize, resolver: &dyn ContentResidencyVerifier,
        io: Option<&PreparationIo>,
    ) -> Result<(Vec<ReferenceGrant>, Vec<FileRef>)> {
        let count = usize::try_from(through_sequence)
            .map_err(|_| fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited prefix is too large"))?;
        let mut direct = BTreeMap::<String, FileRef>::new();
        let mut member = BTreeMap::<(String, String), (FileRef, FileRef)>::new();
        let mut manifests = BTreeMap::<String, FileRef>::new();
        let mut manifest_bytes_total = 0_u64;
        for message in history.messages.iter().take(count) {
            fork_preparer_io::item(io, 0)?;
            fork_preparer_io::item(io, std::mem::size_of::<(String, FileRef)>() as u64)?;
            direct.insert(fork_preparer_io::reference_key(&message.content, io)?, fork_preparer_io::clone_file(&message.content, io)?);
            for reference in message.extensions.values() {
                fork_preparer_io::item(io, std::mem::size_of::<(String, FileRef)>() as u64)?;
                direct.insert(fork_preparer_io::reference_key(reference, io)?, fork_preparer_io::clone_file(reference, io)?);
            }
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for item in items {
                        fork_preparer_io::item(io, std::mem::size_of::<(String, FileRef)>() as u64)?;
                        direct.insert(fork_preparer_io::reference_key(&item.file, io)?, fork_preparer_io::clone_file(&item.file, io)?);
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    manifest_bytes_total = manifest_bytes_total
                        .checked_add(manifest.descriptor().byte_length())
                        .ok_or_else(|| {
                            fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited manifests exceed byte limit")
                        })?;
                    if manifest_bytes_total > maximum_bytes {
                        return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited attachment manifest exceeds limit"));
                    }
                    let items = match io {
                        Some(io) => {
                            #[cfg(not(target_arch = "wasm32"))]
                            if let Some(work) = io.original_native_work() {
                                let (items, custody) = resolver.load_manifest_with_native_work(manifest, *item_count, work)?.await?;
                                io.absorb_native_custody(custody)?;
                                items
                            } else {
                                let items = io.receive_content(resolver.load_manifest_measured(manifest, *item_count,
                                    io.remaining()?, io.cancellation()).await)?;
                                io.retain_owned(crate::conversation::manifest_owned_bytes(&items, items.capacity())?)?;
                                items
                            }
                            #[cfg(target_arch = "wasm32")]
                            {
                                let items = io.receive_content(resolver.load_manifest_measured(manifest, *item_count,
                                    io.remaining()?, io.cancellation()).await)?;
                                io.retain_owned(crate::conversation::manifest_owned_bytes(&items, items.capacity())?)?;
                                items
                            }
                        }
                        None => {
                            let bytes = resolver.read(manifest).await?;
                            decode_attachment_manifest(manifest, &bytes, None, None)?
                        }
                    };
                    if items.len() != *item_count as usize {
                        return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited attachment manifest count mismatch"));
                    }
                    let key = fork_preparer_io::reference_key(manifest, io)?;
                    fork_preparer_io::item(io, (2 * std::mem::size_of::<(String, FileRef)>()) as u64)?;
                    direct.insert(fork_preparer_io::clone_text(&key, io)?, fork_preparer_io::clone_file(manifest, io)?);
                    manifests.insert(fork_preparer_io::clone_text(&key, io)?, fork_preparer_io::clone_file(manifest, io)?);
                    for item in items {
                        fork_preparer_io::item(io, std::mem::size_of::<((String, String), (FileRef, FileRef))>() as u64)?;
                        member.insert(
                            (fork_preparer_io::clone_text(&key, io)?, fork_preparer_io::reference_key(&item.file, io)?),
                            (fork_preparer_io::clone_file(manifest, io)?, item.file),
                        );
                    }
                }
            }
            if direct.len().saturating_add(member.len()) > maximum_references {
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited reference count exceeds limit"));
            }
        }
        if let Some(io) = io {
            for file in direct.values().chain(member.values().map(|(_, file)| file)) {
                resolver.verify(file, maximum_bytes, Some(io))?.await?;
            }
            io.claim(((attached_agents.len() + 1) * std::mem::size_of::<AgentId>()) as u64)?;
        }
        let mut readers = BTreeSet::from([child_agent]);
        readers.extend(attached_agents.iter().copied());
        if readers.len() != attached_agents.len().saturating_add(1) {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited readers are duplicated"));
        }
        let total_grants = direct
            .len()
            .saturating_add(member.len())
            .saturating_mul(readers.len());
        if total_grants > maximum_references {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited read grants exceed limit"));
        }
        let mut reference_grants = fork_preparer_io::vector(total_grants, io)?;
        for reader in readers {
            for file in direct.values() {
                if file.volume().class() == VolumeClass::AgentPrivate
                    && file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                reference_grants.push(ReferenceGrant {
                    file: fork_preparer_io::clone_file(file, io)?,
                    reader,
                    attachment_manifest: None,
                });
            }
            for (manifest, file) in member.values() {
                if file.volume().class() == VolumeClass::AgentPrivate
                    && file.volume().owner() == &VolumeOwner::Agent(reader)
                {
                    continue;
                }
                reference_grants.push(ReferenceGrant {
                    file: fork_preparer_io::clone_file(file, io)?,
                    reader,
                    attachment_manifest: Some(fork_preparer_io::clone_file(manifest, io)?),
                });
            }
        }
        let mut attachment_manifests = fork_preparer_io::vector(manifests.len(), io)?;
        attachment_manifests.extend(manifests.into_values());
        Ok((reference_grants, attachment_manifests))
    }

    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "same inherited capture with exact original summary continuation keys"
    )]
    pub(super) async fn materialize_inherited_conversation_continued(
        &self,
        parent: &Reducer,
        child_private: &VolumeRef,
        initial_generation: &GenerationRef,
        child_agent: AgentId,
        attached_agents: &[AgentId],
        through_sequence: u64,
        maximum_messages: usize,
        maximum_bytes: u64,
        maximum_references: usize,
        resolver: &dyn ContentResidencyVerifier,
        idempotency_key: &IdempotencyKey,
        continuation_keys: &[IdempotencyKey],
        archived_history: Option<&crate::conversation::ConversationState>,
        io: Option<&PreparationIo>,
        preparation: Option<&crate::resource_preparation::TaskResourcePreparation>,
        #[cfg(not(target_arch = "wasm32"))]
        native_creation_guard: Option<&std::sync::Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<InheritedContextCapture> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(PreparationIo::original_native_work).is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            native_creation_guard.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native inherited publication requires its original issued creation guard",
            })?.verify_creation_with_native_work(work)?.await?;
        }
        if !native
            && let Some(preparation) = preparation
        { preparation.verify_creation().await?; }
        if let Some(io) = io {
            let project = io.capability(&self.project, "volume:read:")?;
            if !self.scope.capabilities().contains(&project)
                || !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
                return Err(fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Unauthorized, "parent fork preparation is not granted"));
            }
        } else {
            self.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        }
        if parent.authority() != &self.parent
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent()
            || child_private.provider() != &self.host.provider
            || child_private.class() != VolumeClass::AgentPrivate
            || child_private.owner() != &VolumeOwner::Agent(child_agent)
        {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Unauthorized, "inherited context is not parent-controlled and child-owned"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            child_private.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        } else {
            child_private.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        child_private.validate()?;
        let history = self.selected_inherited_history(
            parent, through_sequence, maximum_messages, archived_history, io,
        )?;
        let prefix = if let Some(io) = io {
            if attached_agents.len() as u64 > crate::fork::MAX_FORK_AGENTS as u64 {
                return Err(io.invalid("too many attached agents for inherited context"));
            }
            let mut readers = BTreeSet::new();
            for reader in attached_agents {
                fork_preparer_io::item(Some(io), std::mem::size_of::<AgentId>() as u64)?;
                if !readers.insert(*reader) { return Err(io.invalid("inherited attached agents are invalid")); }
            }
            let message_digest = history.history_prefix_digest(through_sequence, &mut Some(&mut io.history_io()))?;
            let inherited_parent = crate::core::Authority {
                kind: self.parent.kind,
                id: fork_preparer_io::clone_text(&self.parent.id, Some(io))?,
            };
            let copied = acyclic_fs::WorkCounters { bytes_copied: std::mem::size_of_val(attached_agents) as u64, ..Default::default() };
            io.preflight(copied)?;
            let mut attached = fork_preparer_io::vector(attached_agents.len(), Some(io))?;
            attached.extend_from_slice(attached_agents);
            io.charge(copied)?;
            InheritedConversationPrefix {
                format_version: 3, parent: inherited_parent, parent_revision: parent.revision(),
                parent_agent: history.agent.ok_or_else(|| io.invalid("inherited prefix parent is unbound"))?,
                through_sequence, attached_agents: attached, message_digest,
            }
        } else {
            InheritedConversationPrefix::select(self.parent.clone(), parent.revision(), through_sequence, attached_agents, history)?
        };
        let (reference_grants, attachment_manifests) = Self::collect_inherited_reference_grants(
            history, child_agent, attached_agents, through_sequence, maximum_bytes,
            maximum_references, resolver, io).await?;
        let bytes = match io {
            Some(io) => io.encode(&prefix, maximum_bytes, true)?,
            None => crate::contract::canonical_json_bytes(&prefix)?.into(),
        };
        if bytes.len() as u64 > maximum_bytes {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "inherited context exceeds byte limit"));
        }
        let path = ".system/inherited-conversation/prefix.json";
        let descriptor = crate::executor::with_history_io(io, |work| FileDescriptor::from_bytes(
            &bytes, "application/vnd.acyclic.harness.inherited-conversation+json", work))?;
        let workspace = fork_preparer_io::content_workspace(&self.host.provider, child_private, io)?;
        let committed = if let Some(prior) = fork_preparer_io::operation_generation(self.host, &workspace, idempotency_key, io).await? {
            let original = fork_preparer_io::generation_ref(self.host, &prior, io)?;
            let head = fork_preparer_io::resolve(self.host, &workspace, io).await?.generation;
            if head != original {
                let mut continued = false;
                for key in continuation_keys {
                    if let Some(generation) = fork_preparer_io::operation_generation(self.host, &workspace, key, io).await?
                        && fork_preparer_io::generation_ref(self.host, &generation, io)? == head {
                        continued = true;
                        break;
                    }
                }
                if !continued {
                    return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "child private volume changed after inherited-context staging"));
                }
            }
            prior
        } else {
            let initial = fork_preparer_io::resolve(self.host, &workspace, io).await?.generation;
            if &initial != initial_generation { return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "child private volume is not fresh")); }
            let copied = bytes.clone();
            let mutations = [
                WorkspaceMutation::CreateDirectory { path: fork_preparer_io::clone_text("/.system/inherited-conversation", io)? },
                WorkspaceMutation::PutFile { path: crate::contract::preparation_format(format_args!("/{path}"), io)?, bytes: copied },
            ];
            fork_preparer_io::apply_generation_task_guarded(
                self.host, &workspace, Some(&initial), &mutations, idempotency_key,
                io, preparation,
                #[cfg(not(target_arch = "wasm32"))]
                native_creation_guard,
            ).await?
        };
        let generation = fork_preparer_io::generation_ref(self.host, &committed, io)?;
        if let Some(io) = io {
            fork_preparer_io::retain(&committed, Some(io)).await?;
        } else { self.host.retain_generation(&workspace, &generation).await?; }
        let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
        let reference = crate::executor::with_history_io(io, |work| FileRef::new(
            child_private.copy(work)?, fork_preparer_io::clone_text(path, io)?, version,
            descriptor, fork_preparer_io::clone_text("inherited-conversation.json", io)?, work))?;
        let committed = self.host.read_pinned_io(&reference, maximum_bytes, io).await?;
        if committed.as_ref() != bytes.as_ref() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "inherited-context retry identity belongs to different prefix"));
        }
        Ok(InheritedContextCapture {
            file: reference,
            generation,
            reference_grants,
            attachment_manifests,
        })
    }

    /// Forks the parent's exact project generation into a child project.
    pub async fn fork_project(
        &self,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        self.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        self.host
            .fork_project(&self.project, source_generation, child, idempotency_key)
            .await
    }

    /// Inspects an exact readable source, regardless of agent ancestry.
    pub async fn prepare_project_import(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
    ) -> Result<ParentMergePlan<A, O>> {
        self.prepare_project_import_checked(source, generation, None, false,
            || std::future::ready(Ok(()))).await
    }

    fn require_selected_project_source(
        source: &VolumeRef, generation: &GenerationRef,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        context: &crate::runtime::TaskContext, operation: crate::OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let authorization = handle.authorization();
        if context.durable_task_id() != Some(authorization.caller_task) {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some_and(PreparationIo::uses_native_work) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "selected project import belongs to another original caller" });
            }
            return Err(Error::Unauthorized(
                "selected project import belongs to another original caller".into()));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            handle.require_selection(operation, authorization.source_task, source, generation, authorization.native_snapshot.as_ref(), Some(work))?;
            return handle.grant().require(source, VolumeOperation::Read, io);
        }
        handle.require_selection(operation, authorization.source_task, source, generation, authorization.native_snapshot.as_ref(), None)?;
        handle.grant().require(source, VolumeOperation::Read, io)
    }

    pub async fn prepare_project_import_from_selected_source<P: acyclic_stream::StreamProvider>(
        &self, source: &VolumeRef, generation: &GenerationRef,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        owner: &Arc<crate::durable_host::TaskJournalOwner<P>>,
        context: &crate::runtime::TaskContext, operation: crate::OperationId,
    ) -> Result<ParentMergePlan<A, O>> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        Self::require_selected_project_source(source, generation, handle, context, operation, None)?;
        self.prepare_project_import_checked(source, generation, None, true,
            || owner.verify_project_source_handle(context, handle)).await
    }

    pub async fn prepare_project_import_at_from_selected_source<P: acyclic_stream::StreamProvider>(
        &self, source: &VolumeRef, generation: &GenerationRef, target: &GenerationRef,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        owner: &Arc<crate::durable_host::TaskJournalOwner<P>>,
        context: &crate::runtime::TaskContext, operation: crate::OperationId,
    ) -> Result<ParentMergePlan<A, O>> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        Self::require_selected_project_source(source, generation, handle, context, operation, None)?;
        self.prepare_project_import_checked(source, generation, Some(target), true,
            || owner.verify_project_source_handle(context, handle)).await
    }

    pub(crate) async fn prepare_project_import_checked<F, Fut>(
        &self, source: &VolumeRef, generation: &GenerationRef,
        selected_target: Option<&GenerationRef>, selected: bool, mut guard: F,
    ) -> Result<ParentMergePlan<A, O>>
    where F: FnMut() -> Fut, Fut: Future<Output = Result<()>> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        if !selected {
            ContentGrant::verify(&self.verifier, &self.scope, source, VolumeOperation::Read, u64::MAX, &mut None)?;
        }
        guard().await?;
        source.validate(&mut None)?;
        self.host.validate_provider(source.provider())?;
        let source_workspace = self.host.open(&workspace_ref(
            self.host.provider.clone(), &source.storage_name()?)?).await?;
        guard().await?;
        let target_workspace = self.host.open(&workspace_ref(
            self.host.provider.clone(), &self.project.storage_name()?)?).await?;
        guard().await?;
        let source_generation = self.host.generation(&source_workspace, generation).await?;
        guard().await?;
        let target_generation = if let Some(target) = selected_target {
            self.host.generation(&target_workspace, target).await?
        } else {
            target_workspace.head().await.map_err(map_error)?
        };
        guard().await?;
        let plan = source_workspace.join_into(&target_workspace)
            .plan_at(&source_generation, &target_generation).await.map_err(map_error)?;
        guard().await?;
        Ok(ParentMergePlan { plan, child_project: source.clone(),
            parent_project: self.project.clone(), parent_scope: self.scope.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn prepare_project_import_with_native_work(
        &self, source: &VolumeRef, generation: &GenerationRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ParentMergePlan<A, O>, acyclic_fs::NativeAllocation)> {
        self.prepare_project_import_native(source, generation, None, false, work,
            || std::future::ready(Ok(()))).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn prepare_project_import_at_with_native_work(
        &self, source: &VolumeRef, generation: &GenerationRef,
        target: &GenerationRef, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ParentMergePlan<A, O>, acyclic_fs::NativeAllocation)> {
        self.prepare_project_import_native(source, generation, Some(target), false, work,
            || std::future::ready(Ok(()))).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn prepare_project_import_from_selected_source_with_native_work<P: acyclic_stream::StreamProvider>(
        &self, source: &VolumeRef, generation: &GenerationRef,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        owner: &Arc<crate::durable_host::TaskJournalOwner<P>>,
        context: &crate::runtime::TaskContext, operation: crate::OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ParentMergePlan<A, O>, acyclic_fs::NativeAllocation)> {
        let io = self.require_with_native_work(capability::PROJECT_MERGE, VolumeOperation::Write, work)?;
        Self::require_selected_project_source(source, generation, handle, context, operation, Some(&io))?;
        self.prepare_project_import_native(source, generation, None, true, work,
            || owner.verify_project_source_handle_with_native_work(context, handle, work)).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn prepare_project_import_at_from_selected_source_with_native_work<P: acyclic_stream::StreamProvider>(
        &self, source: &VolumeRef, generation: &GenerationRef, target: &GenerationRef,
        handle: &crate::source_authorization::ProjectSourceReadHandle,
        owner: &Arc<crate::durable_host::TaskJournalOwner<P>>,
        context: &crate::runtime::TaskContext, operation: crate::OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ParentMergePlan<A, O>, acyclic_fs::NativeAllocation)> {
        let io = self.require_with_native_work(capability::PROJECT_MERGE, VolumeOperation::Write, work)?;
        Self::require_selected_project_source(source, generation, handle, context, operation, Some(&io))?;
        self.prepare_project_import_native(source, generation, Some(target), true, work,
            || owner.verify_project_source_handle_with_native_work(context, handle, work)).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_project_import_native<F, Fut>(
        &self, source: &VolumeRef, generation: &GenerationRef,
        selected_target: Option<&GenerationRef>, selected: bool,
        work: &acyclic_fs::NativeMaterializationWork, mut guard: F,
    ) -> Result<(ParentMergePlan<A, O>, acyclic_fs::NativeAllocation)>
    where F: FnMut() -> Fut, Fut: Future<Output = Result<()>> {
        let io = self.require_with_native_work(capability::PROJECT_MERGE, VolumeOperation::Write, work)?;
        source.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if source.provider() != &self.host.provider
            || (!selected && !io.contains_capability(self.scope.capabilities(), source, "volume:read:")?)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "project import source is not currently readable by this parent" });
        }
        guard().await?;
        let source_ref = fork_preparer_io::content_workspace(&self.host.provider, source, Some(&io))?;
        let target_ref = fork_preparer_io::content_workspace(&self.host.provider, &self.project, Some(&io))?;
        let source_workspace = fork_preparer_io::open(self.host, &source_ref, &io).await?;
        guard().await?;
        let target_workspace = fork_preparer_io::open(self.host, &target_ref, &io).await?;
        guard().await?;
        let source_generation = fork_preparer_io::generation(self.host, &source_workspace, generation, &io).await?;
        guard().await?;
        let target_generation = if let Some(target) = selected_target {
            fork_preparer_io::generation(self.host, &target_workspace, target, &io).await?
        } else {
            target_workspace.head_with_native_work(work, work.cancellation()).await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
        };
        guard().await?;
        let plan_custody;
        let (plan, custody) = source_workspace.join_into(&target_workspace)
            .plan_at_with_native_work(&source_generation, &target_generation, work, work.cancellation()).await
            .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        plan_custody = custody;
        guard().await?;
        let (child_project, custody) = source.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (parent_project, custody) = self.project.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let mut observer = io.history_io();
        let parent_scope = self.scope.copy(&mut Some(&mut observer))?;
        drop(observer);
        drop(source_ref);
        drop(target_ref);
        let scope_owned = parent_scope.logical_owned_bytes()?;
        let retained = child_project.logical_owned_bytes()?
            .checked_add(parent_project.logical_owned_bytes()?)
            .and_then(|bytes| bytes.checked_add(scope_owned))
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native merge plan ownership overflows" })?;
        let custody = io.into_native_custody(retained)?;
        Ok((ParentMergePlan { plan, child_project, parent_project, parent_scope,
            native_custody: Some(custody) }, plan_custody))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn require_merge_plan_with_native_work(
        &self, plan: &ParentMergePlan<A, O>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<PreparationIo> {
        let io = self.require_with_native_work(capability::PROJECT_MERGE, VolumeOperation::Write, work)?;
        if !plan.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work))
            || plan.parent_project != self.project || plan.parent_scope != self.scope
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "merge plan belongs to another original parent controller" });
        }
        Ok(io)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_project_merge_with_native_work(
        &self, plan: &ParentMergePlan<A, O>, operation: crate::OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(JoinOutcome<A, O>, acyclic_fs::NativeAllocation)> {
        let _io = self.require_merge_plan_with_native_work(plan, work)?;
        let owner = NativeParentMergePublicationOwner::new(&self.verifier, &self.scope, &self.project, work)?;
        let mut guard = move |original: &acyclic_fs::NativeMaterializationWork|
            std::future::ready(owner.check(original));
        plan.plan.apply_with_permit_guarded_with_native_work(
            Self::merge_options(plan, native_project_join_key(operation, work)?),
            acyclic_fs::PublicationPermit::Unrestricted, work, work.cancellation(),
            &mut guard,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_project_merge_sides_with_native_work(
        &self, plan: &ParentMergePlan<A, O>, operation: crate::OperationId,
        selections: &BTreeMap<MergeConflict, ConflictSide>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(JoinOutcome<A, O>, acyclic_fs::NativeAllocation)> {
        let _io = self.require_merge_plan_with_native_work(plan, work)?;
        let owner = NativeParentMergePublicationOwner::new(&self.verifier, &self.scope, &self.project, work)?;
        let mut guard = move |original: &acyclic_fs::NativeMaterializationWork|
            std::future::ready(owner.check(original));
        plan.plan.apply_sides_with_permit_guarded_with_native_work(
            Self::merge_options(plan, native_project_join_key(operation, work)?),
            selections, acyclic_fs::PublicationPermit::Unrestricted, work, work.cancellation(),
            &mut guard,
        ).await
    }

    /// Publishes only a plan prepared under this same authenticated parent scope.
    pub async fn apply_project_merge(
        &self,
        plan: &ParentMergePlan<A, O>,
        operation_id: crate::OperationId,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply(Self::merge_options(plan, project_join_key(operation_id)?))
            .await
            .map_err(map_error)
    }

    /// Constructs the published receipt from the same durable join and original work.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn merge_receipt_with_native_work(
        &self,
        plan: &ParentMergePlan<A, O>,
        outcome: &JoinOutcome<A, O>,
        child: &Authority,
        operation_id: crate::OperationId,
        notice: &crate::conversation::ConversationMessage,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ProjectMergeReceipt, acyclic_fs::NativeAllocation)> {
        self.require_merge_plan_with_native_work(plan, work)?;
        let (JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application)) = outcome else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "project join did not publish a result",
            });
        };
        let io = PreparationIo::from_native_work(work)?;
        let filesystem_key = native_project_join_key(operation_id, work)?;
        let (_witness, witness_custody) = plan.plan.commit_witness_with_native_work(
            application, filesystem_key, work, io.cancellation(),
        ).map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        let maximum_witness_bytes = (std::mem::size_of::<JoinCommitWitness>() as u64)
            .checked_mul(16).and_then(|bytes| bytes.checked_add(1024))
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "join witness bound overflows",
            })?;
        let encoded_guard;
        let encoded = io.encode(&_witness, maximum_witness_bytes, true)?;
        encoded_guard = io.guard_bytes(&encoded)?;
        let mut statement_guard = None;
        let statement: serde_json::Value = crate::contract::json_from_slice_bounded(std::marker::PhantomData,
            &encoded,
            crate::contract::JsonBounds {
                maximum_bytes: maximum_witness_bytes,
                maximum_items: encoded.len() as u64,
                maximum_depth: 8,
            },
            0,
            Some(crate::wire::ErrorCode::Storage),
            Some(crate::contract::JsonValueSchema::Root),
            &mut |charge| match charge {
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                    let bytes = maximum_owned_bytes.max(
                        crate::contract::json_value_decode_owned_bytes(value_shape)?,
                    );
                    statement_guard = Some(io.reserve_decode(bytes)?);
                    Ok(())
                }
                charge => io.codec_charge(charge, false),
            },
        )?;
        statement_guard.take().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "join witness decoder did not admit its statement",
        })?.into_retained();
        drop(statement_guard);
        drop(encoded);
        drop(encoded_guard);
        drop(witness_custody);
        let mut observer = io.history_io();
        let child = crate::store::history::HistoryIo::clone_authority(&mut observer, child)?;
        drop(observer);
        let (source_project, custody) = plan.child_project.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (target_project, custody) = self.project.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let (source_generation, custody) = GenerationRef::new_with_native_work(
            &self.host.provider, &plan.source_head().digest().into_bytes(), None, work,
        )?;
        io.absorb_native_custody(custody)?;
        let (expected_target_generation, custody) = GenerationRef::new_with_native_work(
            &self.host.provider, &plan.target_head().digest().into_bytes(), None, work,
        )?;
        io.absorb_native_custody(custody)?;
        let result_generation = fork_preparer_io::generation_ref(
            &self.host, application.generation(), Some(&io),
        )?;
        let operation_bytes = filesystem_key.into_bytes();
        let mut provider_operation_id = Vec::new();
        fork_verify_reserve(&mut provider_operation_id, operation_bytes.len(), &io)?;
        let copy = acyclic_fs::WorkCounters {
            bytes_copied: operation_bytes.len() as u64, ..Default::default()
        };
        io.preflight(copy)?;
        io.charge(copy)?;
        provider_operation_id.extend_from_slice(&operation_bytes);
        let provider = self.host.provider.clone_with_io(&io)?;
        let format = fork_preparer_io::clone_text(FILESYSTEM_JOIN_PROOF_FORMAT, Some(&io))?;
        let (notice, custody) = notice.clone_with_native_work(work)?;
        io.absorb_native_custody(custody)?;
        let receipt = ProjectMergeReceipt {
            operation_id, child, source_project, source_generation, target_project,
            expected_target_generation, result_generation, provider_operation_id,
            provider_proof: ProviderJoinProof { provider, format, statement },
            notice,
        };
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((receipt, custody))
    }

    /// Converts only a durable successful join into an immutable Harness receipt.
    pub fn merge_receipt(
        &self,
        plan: &ParentMergePlan<A, O>,
        outcome: &JoinOutcome<A, O>,
        child: Authority,
        operation_id: crate::OperationId,
        notice: crate::conversation::ConversationMessage,
    ) -> Result<ProjectMergeReceipt> {
        self.require_merge_plan(plan)?;
        let (JoinOutcome::Applied(application) | JoinOutcome::AlreadyApplied(application)) =
            outcome
        else {
            return Err(Error::Conflict(
                "project join did not publish a result".into(),
            ));
        };
        let filesystem_key = project_join_key(operation_id)?;
        let witness = plan
            .plan
            .commit_witness(application, filesystem_key)
            .map_err(map_error)?;
        let receipt = ProjectMergeReceipt {
            operation_id,
            child,
            source_project: plan.child_project.clone(),
            source_generation: self.host.generation_ref_id(plan.source_head())?,
            target_project: self.project.clone(),
            expected_target_generation: self.host.generation_ref_id(plan.target_head())?,
            result_generation: self.host.generation_ref(application.generation())?,
            provider_operation_id: filesystem_key.into_bytes().to_vec(),
            provider_proof: ProviderJoinProof {
                provider: self.host.provider.clone(),
                format: FILESYSTEM_JOIN_PROOF_FORMAT.into(),
                statement: serde_json::to_value(witness)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            },
            notice,
        };
        Ok(receipt)
    }

    /// Describes exact three-way conflicts without yielding the publishable plan.
    pub async fn describe_project_merge_conflicts(
        &self,
        plan: &ParentMergePlan<A, O>,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan> {
        self.require_merge_plan(plan)?;
        plan.plan
            .describe_conflicts(conflicts, truncated)
            .await
            .map_err(map_error)
    }

    /// Applies an explicit, exact conflict-side selection under parent authority.
    pub async fn apply_project_merge_sides(
        &self,
        plan: &ParentMergePlan<A, O>,
        operation_id: crate::OperationId,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply_sides(
                Self::merge_options(plan, project_join_key(operation_id)?),
                selections,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }

    /// Validates and publishes an admitted custom candidate under parent authority.
    pub async fn apply_project_merge_candidate(
        &self,
        plan: &ParentMergePlan<A, O>,
        operation_id: crate::OperationId,
        candidate: &acyclic_fs::UnpublishedMergeCandidate,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply_candidate(Self::merge_options(plan, project_join_key(operation_id)?), candidate)
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }

    fn require_merge_plan(&self, plan: &ParentMergePlan<A, O>) -> Result<()> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        if plan.parent_project != self.project || plan.parent_scope != self.scope {
            return Err(Error::Unauthorized(
                "merge plan belongs to another parent controller".into(),
            ));
        }
        Ok(())
    }

    fn merge_options(plan: &ParentMergePlan<A, O>, idempotency_key: FilesystemKey) -> ApplyOptions {
        ApplyOptions {
            if_target: plan.target_head(),
            idempotency_key,
        }
    }
}

impl<A, O> FilesystemHost<A, O> {
    /// Binds one Filesystem deployment and immutable provider identity.
    pub fn new(filesystem: Fs<A, O>, provider: ProviderRef) -> Result<Self> {
        provider.validate()?;
        if provider.family() != "filesystem" {
            return Err(Error::Invalid(
                "Filesystem adapter requires the filesystem provider family".into(),
            ));
        }
        Ok(Self {
            filesystem,
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        })
    }

    /// Copies only this host's provider descriptor on the original resource.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        filesystem: Fs<A, O>, provider: &ProviderRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        provider.validate_with_native_work(work)?;
        if provider.family() != "filesystem" {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "Filesystem adapter requires the filesystem provider family" });
        }
        let io = PreparationIo::from_native_work(work)?;
        let header = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(Error::from)?;
        io.absorb_native_custody(header)?;
        let provider = provider.clone_with_io(&io)?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(Self { filesystem, provider, native_custody: Some(custody) })
    }

    /// Shares an already funded logical host Self with one actual SDK Arc.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self.native_custody.as_ref().is_some_and(|guard| guard.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "Filesystem host belongs to another original native resource" });
        }
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })
            .map_err(Error::from)?;
        Ok(Arc::new(self))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Forks a project workspace from one exact generation into a new project volume.
    async fn fork_project(
        &self,
        source: &VolumeRef,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        self.fork_volume_pinned(source, source_generation, child, None, idempotency_key)
            .await
    }

    /// Forks one explicitly selected scratch or skills volume through the Filesystem's existing allocation and retry mechanism.
    #[allow(
        clippy::too_many_arguments,
        reason = "source and destination each require an exact volume and grant; selection and retry stay explicit"
    )]
    pub async fn fork_volume(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        source_generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
        paths: Option<Vec<String>>,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        self.fork_volume_pinned(
            source,
            source_generation,
            destination,
            paths,
            idempotency_key,
        )
        .await
    }

    async fn fork_volume_pinned(
        &self,
        source: &VolumeRef,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        paths: Option<Vec<String>>,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        source.validate(&mut None)?;
        child.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(child.provider())?;
        if source == child {
            return Err(Error::Invalid(
                "volume fork requires a distinct destination".into(),
            ));
        }
        let source_name = source.storage_name()?;
        let source_workspace = self
            .open(&workspace_ref(self.provider.clone(), &source_name)?)
            .await?;
        let generation = self
            .generation(&source_workspace, source_generation)
            .await?;
        self.retain_generation(
            &workspace_ref(self.provider.clone(), &source_name)?,
            source_generation,
        )
        .await?;
        let child_name = child.storage_name()?;
        let options = ForkOptions::from_generation(generation, filesystem_key(idempotency_key));
        let options = match paths {
            Some(paths) => options.inherit_paths(paths),
            None => options,
        };
        let forked = source_workspace
            .fork(&child_name, options, acyclic_fs::PublicationPermit::Unrestricted, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new(), |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::UNCHARGED })))
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(map_error)?;
        let child_workspace = workspace_ref(self.provider.clone(), &child_name)?;
        let initial = forked.operation_generation(filesystem_key(idempotency_key), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(map_error)?
            .ok_or_else(|| Error::Storage("fork creation has no durable generation".into()))?;
        let child_generation = self.generation_ref(&initial)?;
        self.retain_generation(&child_workspace, &child_generation)
            .await?;
        Ok(WorkspaceObservation {
            workspace: child_workspace,
            generation: child_generation,
        })
    }

    /// Plans a separately requested scratch/skills integration.
    pub async fn prepare_volume_import(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
    ) -> Result<VolumeImportPlan<A, O>> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        Ok(VolumeImportPlan {
            plan: self
                .plan_volume_import(source, generation, destination)
                .await?,
        })
    }

    async fn plan_volume_import(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
        destination: &VolumeRef,
    ) -> Result<JoinPlan<A, O>> {
        source.validate(&mut None)?;
        destination.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(destination.provider())?;
        let source_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &source.storage_name()?,
            )?)
            .await?;
        let destination_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &destination.storage_name()?,
            )?)
            .await?;
        let source_generation = self.generation(&source_workspace, generation).await?;
        let target = destination_workspace.head().await.map_err(map_error)?;
        source_workspace
            .join_into(&destination_workspace)
            .plan_at(&source_generation, &target)
            .await
            .map_err(map_error)
    }

    async fn prepare_project_import(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
        target: &VolumeRef,
    ) -> Result<JoinPlan<A, O>> {
        self.plan_volume_import(source, generation, target).await
    }

    /// Creates the unique physical workspace for a logical volume.
    pub async fn create_volume(&self, volume: &VolumeRef) -> Result<WorkspaceObservation> {
        volume.validate(&mut None)?;
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "volume belongs to another Filesystem provider".into(),
            ));
        }
        let name = volume.storage_name()?;
        self.filesystem
            .create_workspace(&name, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(map_error)?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?)
            .await
    }

    /// Stages an immutable version of one volume file before conversation admission.
    #[allow(
        clippy::too_many_arguments,
        reason = "public upload names every identity, descriptor field, bound, and retry key explicitly"
    )]
    pub async fn put_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
    ) -> Result<FileRef> {
        self.put_content_impl(volume,
        grant,
        path,
        bytes,
        media_type,
        display_name,
        maximum_bytes,
        idempotency_key,
        None,
        None,
        None, None, None)
        .await
    }

    /// Publishes through the same upload engine with finite cumulative work.
    #[allow(clippy::too_many_arguments, reason = "complete upload identity and finite work contract")]
    pub async fn put_content_measured(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, maximum_bytes: u64,
        idempotency_key: &IdempotencyKey, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error> {
        let io = fork_preparer_io::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = self.put_content_impl(volume, grant, path, bytes, media_type, display_name,
            maximum_bytes, idempotency_key, None, None, Some(&io), None, None).await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Publishes under the caller's exact original key on its native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original upload identity and native ledger")]
    pub async fn put_content_with_native_work(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, maximum_bytes: u64,
        key: &IdempotencyKey, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let file = self.put_content_impl(volume, grant, path, bytes, media_type, display_name,
            maximum_bytes, key, None, None, Some(&io), None, None).await?;
        let retained = file.logical_owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((file, custody))
    }

    /// Recovers only an original immutable upload generation and its metadata.
    #[allow(clippy::too_many_arguments, reason = "original upload reconciliation binds authority, identity and bounds")]
    pub async fn content_for_operation_measured(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        path: &str, maximum_bytes: u64, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error> {
        let io = fork_preparer_io::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = self.content_for_operation_impl(volume, grant, key, Some(path), maximum_bytes, Some(&io)).await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Recovers the caller's original operation generation, path and metadata; absence is authenticated original-operation absence, never mutable head state.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original retry identity and native ledger")]
    pub async fn content_for_operation_with_native_work(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        path: &str, maximum_bytes: u64, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<(FileRef, acyclic_fs::NativeAllocation)>> {
        let io = PreparationIo::from_native_work(work)?;
        let Some(file) = self.content_for_operation_impl(
            volume, grant, key, Some(path), maximum_bytes, Some(&io)).await? else {
                return Ok(None);
            };
        let retained = file.logical_owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(Some((file, custody)))
    }

    async fn content_for_operation_impl(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        expected_path: Option<&str>, maximum_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        grant.require(volume, VolumeOperation::Write, io)?;
        self.content_for_operation_record_io(
            volume, key, expected_path, maximum_bytes, None, io).await
    }


    pub(super) async fn original_internal_content_for_operation_with_io(
        &self, volume: &VolumeRef, verifier: &AuthorityVerifier, scope: &Scope,
        key: &IdempotencyKey, path: &str, maximum_bytes: u64,
        class: InternalContentClass, io: &PreparationIo,
    ) -> Result<Option<FileRef>> {
        crate::executor::with_history_io(Some(io), |work|
            ContentGrant::authorize_volume(verifier, scope, volume, VolumeOperation::Read, u64::MAX, work))?;
        self.content_for_operation_record_io(
            volume, key, Some(path), maximum_bytes, Some(class), Some(io)).await
    }

    async fn content_for_operation_record_io(
        &self, volume: &VolumeRef, key: &IdempotencyKey,
        expected_path: Option<&str>, maximum_bytes: u64,
        internal_class: Option<InternalContentClass>, io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        } else {
            volume.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        volume.validate()?;
        if volume.provider() != &self.provider {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "resource belongs to another Filesystem provider"));
        }
        if let Some(path) = expected_path {
            fork_preparer_io::validate_content_path(path, io)?;
            if internal_class.is_some_and(|class| !path.starts_with(class.prefix()))
                || (internal_class.is_none() && (path == ".system" || path.starts_with(".system/")
                    || path == "turns/.canonical" || path.starts_with("turns/.canonical/"))) {
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, if internal_class.is_some() {
                    "content path is outside its authorized storage class"
                } else { "internal storage paths are reserved" }));
            }
        }
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let Some(original) = fork_preparer_io::operation_generation(self, &workspace, key, io).await? else { return Ok(None); };
        let receipt_path = content_identity_path_io(key.as_str(), true, ".system/harness-uploads/", io)?;
        const RECORD_BYTES: u64 = 6 * (crate::conversation::MAX_PATH_BYTES as u64
            + 32 * crate::conversation::MAX_LABEL_BYTES as u64 + 1024);
        let receipt = match io {
            Some(io) => {
                read_generation_with_io(&original, &receipt_path, RECORD_BYTES, io).await?
            }
            None => original.read(&receipt_path, RECORD_BYTES).await.map_err(map_error)?,
        };
        let (recorded, expected): (FileRef, Option<GenerationRef>) = match io {
            Some(io) => io.decode_json(&receipt)?,
            None => crate::contract::json_from_slice(&receipt).map_err(|error| Error::Storage(error.to_string()))?,
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            recorded.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        } else {
            recorded.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        recorded.validate()?;
        if recorded.volume() != volume || recorded.version() != "pending"
            || expected_path.is_some_and(|path| path != recorded.path()) {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "original upload metadata differs from requested identity"));
        }
        if internal_class.is_some_and(|class| !recorded.path().starts_with(class.prefix()))
            || (internal_class.is_none() && (recorded.path() == ".system" || recorded.path().starts_with(".system/")
                || recorded.path() == "turns/.canonical" || recorded.path().starts_with("turns/.canonical/"))) {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, if internal_class.is_some() {
                "original content is outside its authorized storage class"
            } else { "internal storage paths are reserved" }));
        }
        let canonical = match io {
            Some(io) => io.encode(&(&recorded, expected.as_ref()), RECORD_BYTES, true)?,
            None => crate::contract::canonical_json_bytes(&(&recorded, expected.as_ref()))?.into(),
        };
        if canonical.as_ref() != receipt.as_ref() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "original upload receipt is not canonical"));
        }
        let metadata_path = content_metadata_path_io(recorded.path(), true, io)?;
        let metadata = match io {
            Some(io) => {
                read_generation_with_io(&original, &metadata_path, RECORD_BYTES, io).await?
            }
            None => original.read(&metadata_path, RECORD_BYTES).await.map_err(map_error)?,
        };
        let encoded = match io {
            Some(io) => io.encode(&recorded, RECORD_BYTES, false)?,
            None => serde_json::to_vec(&recorded).map_err(|error| Error::Invalid(error.to_string()))?.into(),
        };
        if encoded.as_ref() != metadata.as_ref() {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "original upload metadata differs from its canonical receipt"));
        }
        let generation = fork_preparer_io::generation_ref(self, &original, io)?;
        match io {
            Some(io) => fork_preparer_io::retain(&original, Some(io)).await?,
            None => {
                original.pin(format!("harness-generation-{}", hex::encode(generation.as_resource().key())))
                    .await.map_err(map_error)?;
            }
        }
        let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
        let reference = crate::executor::with_history_io(io, |work| FileRef::new(
            volume.copy(work)?, fork_preparer_io::clone_text(recorded.path(), io)?, version,
            FileDescriptor::new(*recorded.descriptor().sha256(), recorded.descriptor().byte_length(),
                fork_preparer_io::clone_text(recorded.descriptor().media_type(), io)?)?,
            fork_preparer_io::clone_text(recorded.display_name(), io)?, work))?;
        self.read_pinned_io(&reference, maximum_bytes, io).await?;
        Ok(Some(reference))
    }

    /// Publishes content only at an explicitly pinned workspace generation.
    #[allow(
        clippy::too_many_arguments,
        reason = "generation-checked publication names the same complete contract as uploads"
    )]
    pub async fn put_content_at(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        expected_generation: &GenerationRef,
    ) -> Result<FileRef> {
        crate::tool::edit::validate_edit_bound(maximum_bytes)?;
        self.put_content_impl(volume,
        grant,
        path,
        bytes,
        media_type,
        display_name,
        maximum_bytes,
        idempotency_key,
        None,
        Some(expected_generation),
        None, None, None)
        .await
    }

    /// Trusted journal staging retains the same atomic metadata and retry checks as public uploads, but may write only its assigned subtree.
    #[allow(
        clippy::too_many_arguments,
        reason = "internal stage mirrors the complete file contract"
    )]
    pub(crate) async fn put_internal_content(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
        class: InternalContentClass,
    ) -> Result<FileRef> {
        self.put_content_impl(volume,
        grant,
        path,
        bytes,
        media_type,
        display_name,
        maximum_bytes,
        idempotency_key,
        Some(class),
        None,
        None, None, None)
        .await
    }

    /// Internal staging keeps the same original whole-volume authority and publisher engine; only its assigned private path class is different.
    #[allow(clippy::too_many_arguments, reason = "complete original internal publication identity")]
    pub(super) async fn put_internal_content_original_with_io(
        &self, volume: &VolumeRef, verifier: &AuthorityVerifier, scope: &Scope,
        path: &str, bytes: &[u8], media_type: &str, display_name: &str,
        maximum_bytes: u64, idempotency_key: &IdempotencyKey,
        class: InternalContentClass, io: &PreparationIo,
    ) -> Result<FileRef> {
        let grant_hold;
        let grant = ContentGrant::verify_with_io(
            verifier, scope, volume, VolumeOperation::Write, io,
        )?;
        grant_hold = io.guard_owned(grant.logical_owned_bytes()?)?;
        let result = self.put_content_impl(volume, &grant, path, bytes, media_type, display_name,
        maximum_bytes, idempotency_key, Some(class), None, Some(io), None, None).await;
        drop(grant);
        drop(grant_hold);
        result
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
        publication_lease: Option<&dyn crate::resource_preparation::CreatedTaskLease>,
        root_allocation: Option<&fork_preparer::FilesystemRootBootstrapAllocation<A, O>>,
    ) -> Result<FileRef> {
        if root_allocation.is_some_and(|allocation| &allocation.request.volume != volume) {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Unauthorized,
                "Root publication allocation names another original volume"));
        }
        #[cfg(target_arch = "wasm32")]
        if publication_lease.is_some() {
            return Err(Error::Unsupported("Root publication lease is native-platform only".into()));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(lease) = publication_lease {
            match io {
                Some(io) => io.verify_publication_lease(lease).await?,
                None => lease.verify().await?,
            }
        }
        let publication_permit = grant.require(volume, VolumeOperation::Write, io)?;
        if volume.provider() != &self.provider || bytes.len() as u64 > maximum_bytes {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "content provider or size limit mismatch"));
        }
        fork_preparer_io::validate_content_path(path, io)?;
        let descriptor = crate::executor::with_history_io(io,
            |work| FileDescriptor::from_bytes(bytes, media_type, work))?;
        let provisional = crate::executor::with_history_io(io, |work| {
            FileRef::new(volume.copy(work)?, fork_preparer_io::clone_text(path, io)?,
                fork_preparer_io::clone_text("pending", io)?,
                FileDescriptor::new(*descriptor.sha256(), descriptor.byte_length(),
                    fork_preparer_io::clone_text(descriptor.media_type(), io)?)?,
                fork_preparer_io::clone_text(display_name, io)?, work)
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
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "internal storage paths are reserved"));
            }
            None => {}
        }
        let receipt_identity = if internal_class.is_some() { provisional.path() } else { idempotency_key.as_str() };
        let receipt_path = content_identity_path_io(receipt_identity, false, ".system/harness-uploads/", io)?;
        let metadata_path = content_metadata_path_io(provisional.path(), false, io)?;
        let metadata_bytes = match io {
            Some(io) => io.encode(&provisional, io.remaining()?.bytes_encoded, false)?,
            None => acyclic_stream::OwnedBytes::from(
                serde_json::to_vec(&provisional).map_err(|error| Error::Invalid(error.to_string()))?),
        };
        let receipt_bytes = match io {
            Some(io) => io.encode(&(&provisional, expected_generation), io.remaining()?.bytes_encoded, true)?,
            None => acyclic_stream::OwnedBytes::from(
                crate::contract::canonical_json_bytes(&(&provisional, expected_generation))?),
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
                let hash = acyclic_fs::WorkCounters { bytes_hashed: idempotency_key.as_str().len() as u64, ..Default::default() };
                io.preflight(hash)?;
                let key = filesystem_key(idempotency_key);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = io.original_native_work() {
                    original.spent(hash).map_err(|cause| Error::NativeWork { cause, custody: None })?;
                    opened.operation_generation_with_native_work(key, original, io.cancellation()).await
                        .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
                } else {
                    io.charge(hash)?;
                    io.receive(opened.operation_generation(key, io.remaining()?, io.cancellation()).await)?
                }
                #[cfg(target_arch = "wasm32")]
                {
                    io.charge(hash)?;
                    io.receive(opened.operation_generation(key, io.remaining()?, io.cancellation()).await)?
                }
            }
            None => opened.operation_generation(filesystem_key(idempotency_key)).await.map_err(map_error)?,
            }
        };
        if let Some(prior) = prior {
            let generation = fork_preparer_io::generation_ref(self, &prior, io)?;
            match io {
                Some(io) => fork_preparer_io::retain(&prior, Some(io)).await?,
                None => self.retain_generation(&workspace, &generation).await?,
            }
            let version = fork_preparer_io::hex_key(generation.as_resource().key(), io)?;
            let receipt_descriptor = crate::executor::with_history_io(io,
                |work| FileDescriptor::from_bytes(&receipt_bytes, "application/json", work))?;
            let receipt = crate::executor::with_history_io(io, |work| {
                FileRef::new(volume.copy(work)?, fork_preparer_io::clone_text(&receipt_path, io)?,
                    fork_preparer_io::clone_text(&version, io)?, receipt_descriptor,
                    fork_preparer_io::clone_text("receipt.json", io)?, work)
            })?;
            self.read_pinned_io(&receipt, receipt_bytes.len() as u64, io).await?;
            let indexed = match io {
                Some(io) => {
                    let indexed_path = crate::contract::preparation_format(format_args!("/{metadata_path}"), Some(io))?;
                    read_generation_with_io(&prior, &indexed_path, metadata_bytes.len() as u64, io).await?
                }
                None => self.read(&workspace, Some(&generation), &format!("/{metadata_path}"), metadata_bytes.len() as u64).await?,
            };
            if indexed.as_ref() != metadata_bytes.as_slice() {
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "upload metadata index does not match"));
            }
            let reference = crate::conversation::FileRef::new_with_io(volume, path, &version, &descriptor, display_name, io)?;
            let committed = self
                .read_pinned_io(&reference, maximum_bytes, io)
                .await?;
            if committed.as_ref() != bytes {
                return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Conflict, "upload identity belongs to different content"));
            }
            return Ok(reference);
        }
        let absolute = crate::contract::preparation_format(format_args!("/{}", provisional.path()), io)?;
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
            path: crate::contract::preparation_format(format_args!("/{metadata_path}"), io)?,
            bytes: metadata_bytes,
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: crate::contract::preparation_format(format_args!("/{receipt_path}"), io)?,
            bytes: receipt_bytes,
        });
        let publication = async {
        let generation = match io {
            Some(io) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(lease) = publication_lease {
                    if io.original_native_work().is_some() {
                        let mut before_publication = |original| async move {
                            lease.verify_with_native_work(original)?.await
                        };
                        fork_preparer_io::apply_generation_guarded(
                            self, &workspace, expected_generation, &mutations,
                            idempotency_key, io, &mut before_publication,
                        ).await?
                    } else {
                        let mut before_publication = || io.verify_publication_lease(lease);
                        fork_preparer_io::publish_generation(
                            self, &workspace, expected_generation, &mutations,
                            idempotency_key, Some(io), &mut before_publication,
                        ).await?
                    }
                } else {
                    fork_preparer_io::apply_generation(
                        self, &workspace, expected_generation, &mutations,
                        idempotency_key, io,
                    ).await?
                }
                #[cfg(target_arch = "wasm32")]
                fork_preparer_io::apply_generation(
                    self, &workspace, expected_generation, &mutations,
                    idempotency_key, io,
                ).await?
            }
            None => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(lease) = publication_lease {
                    let mut before_publication = || lease.verify();
                    fork_preparer_io::publish_generation(
                        self, &workspace, expected_generation, &mutations,
                        idempotency_key, None, &mut before_publication,
                    ).await?
                } else {
                    self.apply_generation(
                        &workspace, expected_generation, &mutations, idempotency_key,
                    ).await?
                }
                #[cfg(target_arch = "wasm32")]
                self.apply_generation(
                    &workspace, expected_generation, &mutations, idempotency_key,
                ).await?
            }
        };
        Ok::<_, Error>(generation)
        };
        #[cfg(not(target_arch = "wasm32"))]
        let (generation, source_snapshot) = if let Some(allocation) = root_allocation {
            let failure_slot = SourcePublicationFailureSlot::prepare(io)?;
            let (generation, snapshot) = allocation.publish_lazy_candidate_with_io(
                &mutations, idempotency_key, publication_permit, io,
            ).await?;
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
                    return Err(failure_slot.finish(
                        generation.id(), source, retention.err(),
                    ));
                }
                (Ok(_), Err(source)) => {
                    return Err(failure_slot.finish(
                        generation.id(), Error::from(source), None,
                    ));
                }
            }
            true
        } else {
            false
        };
        #[cfg(target_arch = "wasm32")]
        let root_retained = false;
        let generation_reference = fork_preparer_io::generation_ref(self, &generation, io)?;
        let version = fork_preparer_io::hex_key(generation_reference.as_resource().key(), io)?;
        if !root_retained {
            match io {
                Some(io) => fork_preparer_io::retain(&generation, Some(io)).await?,
                None => { generation.pin(format!("harness-generation-{version}")).await.map_err(map_error)?; }
            }
        }
        let reference = crate::conversation::FileRef::new_with_io(volume, path, &version, &descriptor, display_name, io)?;
        self.read_pinned_io(&reference, maximum_bytes, io).await?;
        Ok(reference)
    }

    /// Stages a complete canonical attachment list for a manifest-backed event.
    pub async fn put_attachment_manifest(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        items: &[Attachment],
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
    ) -> Result<ReferencedAttachments> {
        let bytes = crate::conversation::encode_attachment_manifest(items)?;
        let manifest = self
            .put_content(
                volume,
                grant,
                path,
                &bytes,
                "application/vnd.acyclic.harness.attachments+json",
                "attachments.json",
                maximum_bytes,
                idempotency_key,
            )
            .await?;
        Ok(ReferencedAttachments::Manifest {
            manifest,
            item_count: u32::try_from(items.len())
                .map_err(|_| Error::Invalid("attachment count exceeds limit".into()))?,
        })
    }

    /// Reads a pinned version after exact-volume access verification.
    pub async fn read_content(
        &self,
        reference: &FileRef,
        grant: &ContentGrant,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        reference.validate(&mut None)?;
        grant.require_file_read(reference, None)?;
        let bytes = self.read_pinned(reference, maximum_bytes).await?;
        let generation = self.file_generation(reference)?;
        let workspace = workspace_ref(self.provider.clone(), &reference.volume().storage_name()?)?;
        self.retain_generation(&workspace, &generation).await?;
        Ok(bytes)
    }

    /// Reads and retains the exact immutable file under cumulative work limits.
    pub async fn read_content_measured(
        &self, reference: &FileRef, grant: &ContentGrant, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error> {
        let io = fork_preparer_io::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            grant.require_file_read(reference, Some(&io))?;
            reference.validate(&mut None)?;
            self.read_pinned_io(reference, maximum_bytes, Some(&io)).await
        }.await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Discovers a granted private directory at the owner's current head, or continues at the immutable generation returned by a prior page.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        self.require_private_directory(volume, grant, path, None)?;
        if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(Error::Invalid(
                "private directory page limit is invalid".into(),
            ));
        }
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        let observation = self.resolve(&workspace).await?;
        let generation = expected_generation.unwrap_or(&observation.generation);
        self.retain_generation(&workspace, generation).await?;
        let requested = if path.is_empty() {
            "/".to_owned()
        } else {
            format!("/{path}")
        };
        let mut page = self
            .list_after(
                &workspace,
                Some(generation),
                &requested,
                after,
                maximum_entries + u32::from(path.is_empty()),
            )
            .await?;
        if path.is_empty() {
            page.entries
                .retain(|entry| entry.name.unicode_text().as_deref() != Some(".system"));
            if page.entries.len() > maximum_entries as usize {
                page.entries.truncate(maximum_entries as usize);
                page.has_more = true;
            }
        }
        Ok((generation.clone(), page))
    }

    /// Uses the ordinary immutable list engine, with a fresh owner check around each SDK logical action.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "exact subtree and guarded pinned pagination")]
    async fn list_private_directory_guarded_with_native_work<F, Fut>(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str,
        expected_generation: Option<&GenerationRef>, after: Option<&LogicalName>,
        maximum_entries: u32, work: &acyclic_fs::NativeMaterializationWork,
        mut guard: F,
    ) -> Result<((GenerationRef, WorkspaceDirectoryPage), acyclic_fs::NativeAllocation)>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        volume.validate(&mut Some(&mut crate::store::history::native_work_io(work)?))?;
        if volume.provider() != &self.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "private directory belongs to another provider",
            });
        }
        if !path.is_empty() {
            crate::conversation::validate_content_path(path, &mut Some(&mut crate::store::history::native_work_io(work)?))?;
        }
        grant.require_directory_path(volume, path, Some(&PreparationIo::from_native_work(work)?))?;
        if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "private directory page limit is invalid",
            });
        }
        let scratch = PreparationIo::from_native_work(work)?;
        let output = PreparationIo::from_native_work(work)?;
        output.retain_owned(std::mem::size_of::<(GenerationRef, WorkspaceDirectoryPage)>() as u64)?;
        let workspace_ref = fork_preparer_io::content_workspace(&self.provider, volume, Some(&scratch))?;
        guard().await?;
        let workspace = fork_preparer_io::open(self, &workspace_ref, &scratch).await?;
        guard().await?;
        let generation = if let Some(expected) = expected_generation {
            fork_preparer_io::generation(self, &workspace, expected, &scratch).await?
        } else {
            workspace.head_with_native_work(work, work.cancellation()).await
                .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?
        };
        guard().await?;
        fork_preparer_io::retain(&generation, Some(&scratch)).await?;
        guard().await?;
        let (requested, requested_custody) = work.display(&format_args!("/{path}"))
            .map_err(Error::from)?;
        scratch.absorb_native_custody(requested_custody)?;
        let page_custody;
        let mut page;
        (page, page_custody) = generation.list_after_with_native_work(
            &requested, after, maximum_entries as usize + usize::from(path.is_empty()),
            work, work.cancellation(),
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        output.absorb_native_custody(page_custody)?;
        guard().await?;
        if path.is_empty() {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: page.entries.len() as u64, ..Default::default()
            }).map_err(Error::from)?;
            let mut removed_bytes = 0;
            page.entries.retain(|entry| {
                let name = &entry.name;
                let internal = match name.encoding() {
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => name.as_bytes() == b".system",
                    NameEncoding::WindowsUtf16Le =>
                        name.as_bytes() == b".\0s\0y\0s\0t\0e\0m\0",
                };
                if internal {
                    removed_bytes += name.logical_owned_bytes();
                }
                !internal
            });
            output.release(removed_bytes)?;
            while page.entries.len() > maximum_entries as usize {
                let removed = page.entries.pop().expect("overscanned page is nonempty");
                let bytes = removed.name.logical_owned_bytes();
                drop(removed);
                output.release(bytes)?;
                page.has_more = true;
            }
        }
        let reference = fork_preparer_io::generation_ref(self, &generation, Some(&output))?;
        let retained = output.owned_bytes()?;
        let custody = output.into_native_custody(retained)?;
        Ok(((reference, page), custody))
    }

    /// Lazily resolves a named private file and returns an immutable ref plus verified bytes.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.read_private_path_with_io(
            volume, grant, path, expected_generation, maximum_bytes, None,
        ).await
    }

    async fn read_private_path_with_io(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str,
        expected_generation: Option<&GenerationRef>, maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        self.require_private_directory(volume, grant, path, io)?;
        let name = path.rsplit('/').next().unwrap_or(path);
        crate::executor::with_history_io(io, |work| crate::conversation::validate_content_path(path, work))?;
        if name.len() > crate::conversation::MAX_LABEL_BYTES {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid, "file or volume label is invalid"));
        }
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        let observation = self.resolve(&workspace).await?;
        let generation = expected_generation.unwrap_or(&observation.generation);
        self.retain_generation(&workspace, generation).await?;
        let bytes = self
            .read(
                &workspace,
                Some(generation),
                &format!("/{}", path),
                maximum_bytes,
            )
            .await?;
        let metadata = self
            .read(
                &workspace,
                Some(generation),
                &format!("/{}", content_metadata_path(path)),
                u64::MAX,
            )
            .await;
        let (descriptor, display_name) = match metadata {
            Ok(metadata) => {
                let staged: FileRef = crate::contract::json_from_slice(&metadata)
                    .map_err(|_| Error::Storage("private file metadata is corrupt".into()))?;
                if staged.volume() != volume
                    || staged.path() != path
                    || staged.version() != "pending"
                {
                    return Err(Error::Storage(
                        "private file metadata does not match the file".into(),
                    ));
                }
                staged.descriptor().verify(&bytes, &mut None)?;
                (
                    staged.descriptor().clone(),
                    staged.display_name().to_owned(),
                )
            }
            Err(Error::NotFound(_)) => (
                FileDescriptor::from_bytes(&bytes, "application/octet-stream", &mut None)?,
                name.to_owned(),
            ),
            Err(error) => return Err(error),
        };
        let reference = FileRef::new(
            volume.clone(), path.to_owned(), hex::encode(generation.as_resource().key()),
            descriptor, display_name, &mut None,
        )?;
        grant.require_file_read(&reference, io)?;
        Ok((reference, bytes))
    }

    /// Reads only a finite authenticated prefix at an explicitly pinned generation.
    pub async fn read_private_prefix(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        generation: &GenerationRef,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.require_private_directory(volume, grant, path, None)?;
        crate::conversation::validate_content_path(path, &mut None)?;
        if maximum_bytes == 0 {
            return Err(Error::Invalid("prefix read limit must be positive".into()));
        }
        let reference = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        self.retain_generation(&reference, generation).await?;
        let workspace = self.open(&reference).await?;
        let pinned = self.generation(&workspace, generation).await?;
        let path = format!("/{path}");
        let stat = pinned.stat(&path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?;
        if stat.kind != FileKind::Regular {
            return Err(Error::Invalid("prefix requires a regular file".into()));
        }
        let length = stat
            .logical_bytes
            .ok_or_else(|| Error::Storage("file length is missing".into()))?;
        pinned.read_range(&path, 0, maximum_bytes.min(length), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value.bytes).map_err(|failure| failure.error)
            .map_err(map_error)
    }

    fn require_private_directory(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io { io.remaining()?; }
        crate::executor::with_history_io(io, |work| -> Result<()> {
            volume.validate(work)?;
            if !path.is_empty() { crate::conversation::validate_content_path(path, work)?; }
            Ok(())
        })?;
        if volume.provider() != &self.provider {
            return Err(fork_preparer_io::failure(io, crate::wire::ErrorCode::Invalid,
                "private directory belongs to another provider or class"));
        }
        grant.require_directory_path(volume, path, io)
    }

    async fn read_pinned(&self, reference: &FileRef, maximum_bytes: u64) -> Result<acyclic_stream::OwnedBytes> {
        self.read_pinned_io(reference, maximum_bytes, None).await
    }

    async fn read_pinned_io(
        &self, reference: &FileRef, maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((io, original)) = io.and_then(|io| {
            io.original_native_work().map(|original| (io, original))
        }) {
            use sha2::Digest as _;
            reference.validate(&mut Some(&mut crate::store::history::native_work_io(original)?))?;
            if reference.volume().provider() != &self.provider
                || reference.descriptor().byte_length() > maximum_bytes
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "content provider or size limit mismatch",
                });
            }
            let generation = self.file_generation_with_io(reference, Some(io))?;
            let workspace = fork_preparer_io::content_workspace(
                &self.provider, reference.volume(), Some(io),
            )?;
            let rendered = original.display(&format_args!("/{}", reference.path()))
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let path_custody = rendered.1;
            let path = rendered.0;
            let opened = fork_preparer_io::open(self, &workspace, io).await?;
            let pinned = fork_preparer_io::generation(self, &opened, &generation, io).await?;
            let bytes = pinned.read_with_native_work(
                &path, maximum_bytes, original, io.cancellation(),
            ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
            fork_preparer_io::retain(&pinned, Some(io)).await?;
            let hashed = acyclic_fs::WorkCounters {
                bytes_hashed: bytes.len() as u64,
                ..acyclic_fs::WorkCounters::UNCHARGED
            };
            original.admit(hashed)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            let digest = sha2::Sha256::digest(&bytes);
            original.spent(hashed)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
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
            io.claim((reference.path().len() + 1) as u64)?;
        }
        let generation = self.file_generation_with_io(reference, io)?;
        let generation_hold = io.map(|io| io.guard_owned(generation.logical_owned_bytes()?)).transpose()?;
        let workspace = fork_preparer_io::content_workspace(&self.provider, reference.volume(), io)?;
        let path = format!("/{}", reference.path());
        let bytes = match io {
            Some(io) => {
                let opened = fork_preparer_io::open(self, &workspace, io).await?;
                let pinned = fork_preparer_io::generation(self, &opened, &generation, io).await?;
                let bytes = io.receive(pinned.read(&path, maximum_bytes, io.remaining()?, io.cancellation()).await)?;
                let capacity = bytes.retained_capacity().ok_or_else(|| {
                    fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Unsupported,
                        "content producer does not expose its owned capacity")
                })?;
                io.retain_owned(capacity as u64)?;
                fork_preparer_io::retain(&pinned, Some(io)).await?;
                bytes
            }
            None => self.read(&workspace, Some(&generation), &path, maximum_bytes).await?,
        };
        reference.descriptor().verify(&bytes, io)?;
        drop(generation);
        drop(generation_hold);
        Ok(bytes)
    }

    fn file_generation(&self, reference: &FileRef) -> Result<GenerationRef> {
        self.file_generation_with_io(reference, None)
    }

    pub(crate) fn file_generation_with_io(
        &self, reference: &FileRef, io: Option<&PreparationIo>,
    ) -> Result<GenerationRef> {
        if let Some(io) = io {
            return GenerationRef::from_hex_with_io(&self.provider, reference.version(), io);
        }
        let raw = hex::decode(reference.version())
            .map_err(|_| Error::Invalid("file version is not a Filesystem generation".into()))?;
        GenerationRef::new(self.provider.clone(), raw, None)
    }

    /// Resolves the current immutable head of a named workspace.
    pub async fn resolve(&self, reference: &WorkspaceRef) -> Result<WorkspaceObservation> {
        let workspace = self.open(reference).await?;
        let head = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?;
        Ok(WorkspaceObservation {
            workspace: reference.clone(),
            generation: self.generation_ref(&head)?,
        })
    }

    /// Reads one bounded file from the current head or an exact generation.
    pub async fn read(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?.read(path, maximum_bytes, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(map_error),
            None => workspace.read(path, maximum_bytes, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error),
        }
    }

    /// Stats one path at the current head or an exact generation.
    pub async fn stat(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
    ) -> Result<WorkspaceStat> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?.stat(path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(map_error),
            None => workspace.stat(path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error),
        }
    }

    /// Lists one bounded directory page at the current head or an exact generation.
    pub async fn list(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage> {
        self.list_after(workspace, generation, path, None, maximum_entries)
            .await
    }

    /// Lists one bounded page after an exact child name cursor.
    pub async fn list_after(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?.list_directory(path, after, maximum_entries, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(map_error),
            None => workspace.list_directory(path, after, maximum_entries, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(map_error),
        }
    }

    /// Applies one atomic, idempotent mutation batch against an optional exact generation.
    pub async fn apply(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
    ) -> Result<GenerationRef> {
        let generation = self
            .apply_generation(workspace, expected_generation, mutations, idempotency_key)
            .await?;
        self.generation_ref(&generation)
    }

    async fn apply_generation(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
    ) -> Result<Generation<A, O>> {
        let mut before_publication = || std::future::ready(Ok(()));
        self.apply_generation_guarded(
            workspace, expected_generation, mutations, idempotency_key,
            &mut before_publication,
        ).await
    }

    async fn apply_generation_guarded<F, Fut>(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
        before_publication: &mut F,
    ) -> Result<Generation<A, O>>
    where
        F: FnMut() -> Fut + Send,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        before_publication().await?;
        let workspace = self.open(workspace).await?;
        let key = filesystem_key(idempotency_key);
        let mut transaction = match expected_generation {
            Some(reference) => {
                let generation = self.generation(&workspace, reference).await?;
                workspace
                    .begin_transaction_at(&generation, key)
                    .await
                    .map_err(map_error)?
            }
            None => workspace.begin_transaction(key, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?,
        };
        for mutation in mutations {
            before_publication().await?;
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction.create_dir_all(path).await.map_err(map_error)?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    transaction
                        .write(path, Bytes::copy_from_slice(bytes))
                        .await
                        .map_err(map_error)?;
                }
                WorkspaceMutation::Remove { path } => {
                    transaction.remove(path).await.map_err(map_error)?;
                }
            }
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        match async { let mut guard = before_publication; transaction.commit(None, acyclic_fs::PublicationPermit::Unrestricted, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| acyclic_fs::OperationReceipt { value, work: acyclic_fs::WorkCounters::default() }).map_err(acyclic_fs::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await? {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. } => {
                Err(Error::Conflict("workspace generation changed".into()))
            }
            TransactionCommit::Fenced => Err(Error::Conflict("workspace writer was fenced".into())),
            TransactionCommit::IdempotencyConflict => Err(Error::Conflict(
                "filesystem idempotency key belongs to another mutation".into(),
            )),
        }
    }

    async fn open(&self, reference: &WorkspaceRef) -> Result<Workspace<A, O>> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        let name = std::str::from_utf8(reference.as_resource().key())
            .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
        self.filesystem
            .open_workspace(name, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(map_error)
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn open_volume_with_native_work(
        &self,
        volume: &VolumeRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Workspace<A, O>> {
        if volume.provider() != &self.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "volume belongs to another Filesystem provider",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let reference = fork_preparer::bounded_io::content_workspace(&self.provider, volume, Some(&io))?;
        fork_preparer::bounded_io::open(self, &reference, &io).await
    }


    async fn generation(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
    ) -> Result<Generation<A, O>> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        let bytes: [u8; 32] = reference
            .as_resource()
            .key()
            .try_into()
            .map_err(|_| Error::Invalid("generation reference key must be 32 bytes".into()))?;
        workspace.generation(GenerationId::new(Digest::from_bytes(bytes)), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(map_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn generation_with_native_work(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Generation<A, O>> {
        let io = PreparationIo::from_native_work(work)?;
        fork_preparer::bounded_io::generation(self, workspace, reference, &io).await
    }


    async fn retain_generation(
        &self,
        workspace: &WorkspaceRef,
        reference: &GenerationRef,
    ) -> Result<()> {
        let workspace = self.open(workspace).await?;
        let generation = self.generation(&workspace, reference).await?;
        let identity = format!(
            "harness-generation-{}",
            hex::encode(reference.as_resource().key())
        );
        generation.pin(identity).await.map_err(map_error)?;
        Ok(())
    }

    async fn retain_file_generation(&self, file: &FileRef) -> Result<()> {
        file.validate(&mut None)?;
        if file.volume().provider() != &self.provider {
            return Err(Error::Unauthorized(
                "file belongs to another Filesystem provider".into(),
            ));
        }
        let workspace = workspace_ref(self.provider.clone(), &file.volume().storage_name()?)?;
        self.retain_generation(&workspace, &self.file_generation(file)?)
            .await
    }

    fn generation_ref(&self, generation: &Generation<A, O>) -> Result<GenerationRef> {
        GenerationRef::new(
            self.provider.clone(),
            generation.id().digest().into_bytes(),
            None,
        )
    }

    fn generation_ref_id(&self, id: GenerationId) -> Result<GenerationRef> {
        GenerationRef::new(self.provider.clone(), id.digest().into_bytes(), None)
    }

    fn validate_provider(&self, provider: &ProviderRef) -> Result<()> {
        provider.validate()?;
        if provider != &self.provider {
            return Err(Error::Invalid(
                "resource belongs to another Filesystem provider".into(),
            ));
        }
        Ok(())
    }
}

/// Creates the canonical provider-owned reference for a named workspace.
pub fn workspace_ref(provider: ProviderRef, name: &str) -> Result<WorkspaceRef> {
    acyclic_fs::WorkspaceName::new(name).map_err(|error| Error::Invalid(error.to_string()))?;
    WorkspaceRef::new(provider, name.as_bytes(), None)
}

fn filesystem_key(key: &IdempotencyKey) -> FilesystemKey {
    let digest = blake3::hash(key.as_str().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    FilesystemKey::from_bytes(bytes)
}

fn project_join_key(operation_id: crate::OperationId) -> Result<FilesystemKey> {
    Ok(filesystem_key(&IdempotencyKey::new(format!(
        "project-join:{operation_id}"
    ))?))
}

fn content_metadata_path(path: &str) -> String {
    content_metadata_path_io(path, false, None).expect("fixed metadata path encoding is infallible")
}

fn content_metadata_path_io(path: &str, absolute: bool, io: Option<&PreparationIo>) -> Result<String> {
    content_identity_path_io(path, absolute, ".system/harness-file-metadata/", io)
}

fn content_identity_path_io(path: &str, absolute: bool, prefix: &str, io: Option<&PreparationIo>) -> Result<String> {
    const SUFFIX: &str = ".json";
    let capacity = usize::from(absolute) + prefix.len() + 64 + SUFFIX.len();
    let hash_work = acyclic_fs::WorkCounters { bytes_hashed: path.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(hash_work)?; }
    let hash = blake3::hash(path.as_bytes());
    if let Some(io) = io {
        io.charge(hash_work)?;
        io.claim(capacity as u64)?;
        io.preflight(acyclic_fs::WorkCounters { bytes_copied: capacity as u64, ..Default::default() })?;
        io.allocation_attempt(capacity as u64)?;
    }
    let mut encoded = String::with_capacity(capacity);
    if absolute { encoded.push('/'); }
    encoded.push_str(prefix);
    encoded.push_str(&hash.to_hex());
    encoded.push_str(SUFFIX);
    if let Some(io) = io {
        if encoded.capacity() > capacity { io.claim((encoded.capacity() - capacity) as u64)?; }
        io.charge(acyclic_fs::WorkCounters { bytes_copied: capacity as u64, ..Default::default() })?;
    }
    Ok(encoded)
}

fn map_error(error: WorkspaceError) -> Error {
    match error {
        WorkspaceError::NotFound => Error::NotFound("workspace path".into()),
        WorkspaceError::RetentionConflict
        | WorkspaceError::StaleGeneration
        | WorkspaceError::StaleSource
        | WorkspaceError::StaleIdentity => Error::Conflict(error.to_string()),
        WorkspaceError::Name(_)
        | WorkspaceError::Path(_)
        | WorkspaceError::NamespacePath(_)
        | WorkspaceError::ReadLimitExceeded
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::ForeignGeneration
        | WorkspaceError::IncompatibleWorkspace
        | WorkspaceError::NoCommonAncestor
        | WorkspaceError::LineageLimit
        | WorkspaceError::JoinLimit
        | WorkspaceError::StaleTarget
        | WorkspaceError::InvalidMergeResolution
        | WorkspaceError::ChangeSetContinuity
        | WorkspaceError::ChangedPathLimit
        | WorkspaceError::EmptyContentSet
        | WorkspaceError::NotFork
        | WorkspaceError::ContentLengthOverflow
        | WorkspaceError::Work(_)
        | WorkspaceError::WorkMerge { .. } => Error::Invalid(error.to_string()),
        WorkspaceError::Cancelled(error) => Error::Storage(error.to_string()),
        WorkspaceError::Filesystem(error) => Error::Storage(error.to_string()),
        WorkspaceError::Engine(value) => Error::Storage(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::{AggregateKind, Authority, AuthorityIssuer};
    use crate::{
        AgentId, Capabilities,
        conversation::{VolumeClass, VolumeOwner},
    };

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_verifier_copies_authentic_scope_and_keeps_custody_until_drop() -> Result<()> {
        let provider = ProviderRef::new("native-verifier-test", "filesystem", "1")?;
        let host = Arc::new(FilesystemHost::new(Fs::memory(), provider)?);
        let issuer = AuthorityIssuer::new("native-verifier-test", [4; 32],
            Authority { kind: AggregateKind::Conversation, id: "parent".into() });
        let scope = issuer.root_for_agent(AgentId::from_bytes([8; 16]), "parent-scope",
            Capabilities::new([capability::FORK_PUBLISH, capability::PROJECT_MERGE]));
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let verifier = FilesystemForkVerifier::new_with_native_work(
            Arc::clone(&host), 4096, &issuer.verifier(&mut None).expect("verifier copy"), &scope, &work)?
            .into_native_shared(&work)?;
        let other = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(matches!(verifier.native_binding.as_ref().unwrap().authenticate(&other),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        let forged_issuer = AuthorityIssuer::new("native-verifier-test", [5; 32],
            Authority { kind: AggregateKind::Conversation, id: "parent".into() });
        assert!(FilesystemProjectMergeVerifier::new_with_native_work(
            host, &forged_issuer.verifier(&mut None).expect("verifier copy"), &scope, &work).is_err());
        drop(verifier);
        Ok(())
    }

    #[tokio::test]
    async fn resolves_exact_memory_generation_and_rejects_foreign_provider() -> Result<()> {
        let filesystem = Fs::memory();
        let workspace = filesystem
            .create_workspace("adapter-test", acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
            .map_err(map_error)?;
        let expected = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?;
        let provider = ProviderRef::new("example", "filesystem", "1")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let reference = workspace_ref(provider, "adapter-test")?;
        let observed = host.resolve(&reference).await?;
        assert_eq!(
            observed.generation.as_resource().key(),
            expected.id().digest().into_bytes()
        );

        let foreign = workspace_ref(
            ProviderRef::new("other", "filesystem", "1")?,
            "adapter-test",
        )?;
        assert!(matches!(
            host.resolve(&foreign).await,
            Err(Error::Invalid(_))
        ));

        let updated = host
            .apply(
                &reference,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/hello.txt".into(),
                    bytes: b"hello".to_vec().into(),
                }],
                &IdempotencyKey::new("write-hello")?,
            )
            .await?;
        assert_eq!(
            host.read(&reference, Some(&updated), "/hello.txt", 16)
                .await?,
            Bytes::from_static(b"hello")
        );
        assert!(matches!(
            host.apply(
                &reference,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/hello.txt".into(),
                    bytes: b"changed".to_vec().into(),
                }],
                &IdempotencyKey::new("stale-write")?,
            )
            .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }


    #[tokio::test]
    async fn staged_content_is_version_pinned_and_requires_exact_grants() -> Result<()> {
        let filesystem = Fs::memory();
        let provider = ProviderRef::new("example", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(filesystem, provider.clone())?);
        let owner = AgentId::from_bytes([3; 16]);
        let volume = VolumeRef::new(
            provider,
            "agent-private-a",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
        )?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "test",
            [4; 32],
            Authority {
                kind: AggregateKind::Conversation,
                id: "test".into(),
            },
        );
        let scope = issuer.root_for_agent(
            owner,
            "owner",
            Capabilities::new([
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let verifier = issuer.verifier(&mut None).expect("verifier copy");
        let read = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read, u64::MAX, &mut None)?;
        let write = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write, u64::MAX, &mut None)?;
        let first = host
            .put_content(
                &volume,
                &write,
                "messages/first.txt",
                b"one",
                "text/plain",
                "first.txt",
                16,
                &IdempotencyKey::new("stage-first")?,
            )
            .await?;
        let second = host
            .put_content(
                &volume,
                &write,
                "messages/first.txt",
                b"two",
                "text/plain",
                "first.txt",
                16,
                &IdempotencyKey::new("stage-second")?,
            )
            .await?;
        assert_eq!(
            host.put_content(
                &volume,
                &write,
                "messages/first.txt",
                b"one",
                "text/plain",
                "first.txt",
                16,
                &IdempotencyKey::new("stage-first")?
            )
            .await?,
            first
        );
        for (path, media_type, display_name) in [
            (
                "messages/first.txt",
                "application/octet-stream",
                "first.txt",
            ),
            ("messages/first.txt", "text/plain", "renamed.txt"),
            ("messages/other.txt", "text/plain", "first.txt"),
        ] {
            assert!(
                host.put_content(
                    &volume,
                    &write,
                    path,
                    b"one",
                    media_type,
                    display_name,
                    16,
                    &IdempotencyKey::new("stage-first")?
                )
                .await
                .is_err()
            );
        }
        assert_ne!(first.version(), second.version());
        assert_eq!(
            host.read_content(&first, &read, 16).await?,
            Bytes::from_static(b"one")
        );
        assert_eq!(
            host.read_content(&second, &read, 16).await?,
            Bytes::from_static(b"two")
        );
        #[cfg(not(target_arch = "wasm32"))]
        {
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let native = FilesystemForkVerifier::new_with_native_work(
                Arc::clone(&host), 16, &verifier, &scope, &work)?;
            let io = PreparationIo::from_native_work(&work)?;
            let binding = native.native_binding.as_ref().unwrap();
            binding.require_file(&first, &io)?;
            let bytes = host.read_pinned_io(&first, 16, Some(&io)).await?;
            assert_eq!(bytes.as_ref(), b"one");
            drop(bytes);
            binding.require_file(&second, &io)?;
            let bytes = host.read_pinned_io(&second, 16, Some(&io)).await?;
            assert_eq!(bytes.as_ref(), b"two");
            drop(bytes);
            let tampered = FileRef::new(first.volume().clone(), first.path(), first.version(),
                second.descriptor().clone(), first.display_name())?;
            assert!(host.read_pinned_io(&tampered, 16, Some(&io)).await.is_err());
            let denied_scope = issuer.root_for_agent(owner, "no-files", Capabilities::default());
            let denied = FilesystemForkVerifier::new_with_native_work(
                Arc::clone(&host), 16, &verifier, &denied_scope, &work)?;
            let before = work.receipt().backend_read_operations;
            assert!(matches!(denied.native_binding.as_ref().unwrap().require_file(&first, &io),
                Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
            assert_eq!(work.receipt().backend_read_operations, before);
            let before = work.receipt();
            cancellation.cancel();
            assert!(binding.authenticate(&work).is_err());
            assert_eq!(work.receipt().allocation_operations, before.allocation_operations);
        }
        let delegated_scope = issuer.delegate_private_directory_read(
            &scope,
            AgentId::from_bytes([8; 16]),
            "reader",
            &volume,
            "messages",
        )?;
        let delegated_reader = FilesystemContentVerifier::new(host.clone(), &verifier, &delegated_scope, 16, None)?;
        let (listed_generation, page) = delegated_reader
            .list_private_directory(&volume, "messages", "messages", None, None, 10)
            .await?;
        assert_eq!(page.entries.len(), 1);
        let erased: &dyn ContentResidencyVerifier = &delegated_reader;
        let portable_page = erased
            .list_private_directory(
                &volume,
                "messages",
                "messages",
                Some(&listed_generation),
                None,
                10,
            )
            .await?;
        assert_eq!(portable_page.generation, listed_generation);
        assert_eq!(
            portable_page.entries,
            vec![PrivateDirectoryEntry {
                name: "first.txt".into(),
                kind: PrivateDirectoryEntryKind::File,
            }]
        );
        let (portable_ref, portable_bytes) = erased
            .read_private_path(
                &volume,
                "messages",
                "messages/first.txt",
                Some(&listed_generation),
            )
            .await?;
        assert_eq!(portable_ref, second);
        assert_eq!(portable_bytes, b"two");
        let (pinned, delegated_bytes) = delegated_reader
            .read_private_path(
                &volume,
                "messages",
                "messages/first.txt",
                Some(&listed_generation),
            )
            .await?;
        assert_eq!(pinned, second);
        assert_eq!(delegated_bytes, b"two");
        host.put_content(
            &volume,
            &write,
            "messages/later.txt",
            b"later",
            "text/plain",
            "later.txt",
            16,
            &IdempotencyKey::new("stage-later")?,
        )
        .await?;
        let (old_generation, old_page) = delegated_reader
            .list_private_directory(
                &volume,
                "messages",
                "messages",
                Some(&listed_generation),
                None,
                10,
            )
            .await?;
        assert_eq!(old_generation, listed_generation);
        assert_eq!(old_page.entries, page.entries);
        let (_, current_page) = delegated_reader
            .list_private_directory(&volume, "messages", "messages", None, None, 10)
            .await?;
        assert_eq!(current_page.entries.len(), 2);
        let (old_ref, old_bytes) = delegated_reader
            .read_private_path(
                &volume,
                "messages",
                "messages/first.txt",
                Some(&listed_generation),
            )
            .await?;
        assert_eq!(old_ref, second);
        assert_eq!(old_bytes, b"two");
        assert!(
            delegated_reader
                .list_private_directory(&volume, "messages", "elsewhere", None, None, 10)
                .await
                .is_err()
        );
        let delegated =
            ContentGrant::verify_directory_read(&verifier, &delegated_scope, &volume, "messages", &mut None)?;
        let (discovered, bytes) = host
            .read_private_path(&volume, &delegated, "messages/first.txt", None, 16)
            .await?;
        assert_eq!(bytes.as_ref(), b"two");
        assert_ne!(discovered.version(), second.version());
        assert_eq!(discovered.path(), second.path());
        assert_eq!(discovered.descriptor(), second.descriptor());
        assert!(
            host.put_content(
                &volume,
                &write,
                ".system/forged.json",
                b"bad",
                "application/json",
                "forged.json",
                16,
                &IdempotencyKey::new("internal-write")?
            )
            .await
            .is_err()
        );
        let empty = host
            .put_content(
                &volume,
                &write,
                "messages/empty.txt",
                b"",
                "text/plain",
                "empty.txt",
                16,
                &IdempotencyKey::new("stage-empty")?,
            )
            .await?;
        assert!(matches!(
            delegated_reader
                .read_private_path(
                    &volume,
                    "messages",
                    "messages/empty.txt",
                    Some(&listed_generation)
                )
                .await,
            Err(Error::NotFound(_))
        ));
        assert!(host.read_content(&empty, &read, 16).await?.is_empty());
        let wrong_digest = FileRef::new(
            volume.clone(),
            first.path(),
            first.version(),
            FileDescriptor::new([0; 32], 3, "text/plain")?,
            first.display_name(),
        )?;
        assert!(host.read_content(&wrong_digest, &read, 16).await.is_err());
        assert!(
            host.put_content(
                &volume,
                &write,
                "../escape",
                b"bad",
                "text/plain",
                "escape",
                16,
                &IdempotencyKey::new("invalid-path")?,
            )
            .await
            .is_err()
        );
        assert!(host.read_content(&first, &write, 16).await.is_err());
        let other = VolumeRef::new(
            volume.provider().clone(),
            "agent-private-b",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([5; 16])),
        )?;
        assert!(ContentGrant::verify(&verifier, &scope, &other, VolumeOperation::Read, u64::MAX, &mut None).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn conversation_stream_admission_requires_resident_files() -> Result<()> {
        use crate::conversation::{Attachment, ConversationMessage, MessageKind};
        use crate::core::{Action, Command, SchemaRegistry};
        use crate::store::StreamAggregate;
        use crate::{IdempotencyKey, OperationId};
        use acyclic_stream::{MemoryStream};
        use std::{collections::BTreeMap, sync::Arc};

        let filesystem = Fs::memory();
        let provider = ProviderRef::new("example", "filesystem", "2")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let agent = AgentId::from_bytes([7; 16]);
        let volume = VolumeRef::new(
            provider,
            "scratch",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
        )?;
        host.create_volume(&volume).await?;
        let authority = Authority {
            kind: AggregateKind::Conversation,
            id: "integration".into(),
        };
        let issuer = AuthorityIssuer::new("integration", [11; 32], authority.clone());
        let scope = issuer.root_for_agent(
            agent,
            "agent",
            Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                volume.capability(VolumeOperation::Read)?,
                volume.capability(VolumeOperation::Write)?,
            ]),
        );
        let read =
            ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &volume, VolumeOperation::Read, u64::MAX, &mut None)?;
        let write =
            ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &volume, VolumeOperation::Write, u64::MAX, &mut None)?;
        let file = host
            .put_content(
                &volume,
                &write,
                "messages/input.txt",
                b"hello",
                "text/plain",
                "input.txt",
                32,
                &IdempotencyKey::new("input-upload")?,
            )
            .await?;
        let manifest = host
            .put_attachment_manifest(
                &volume,
                &write,
                "attachments/list.json",
                &[Attachment {
                    file: file.clone(),
                    label: Some("primary copy".into()),
                }],
                4_096,
                &IdempotencyKey::new("attachment-list")?,
            )
            .await?;
        let verifier = Arc::new(FilesystemContentVerifier::new(Arc::new(host), &issuer.verifier(&mut None).expect("verifier copy"), &scope, 4_096, None)?);
        let stream = Arc::new(MemoryStream::default());
        let mut aggregate =
            StreamAggregate::open(&stream, authority, issuer.verifier(&mut None).expect("verifier copy"), SchemaRegistry::new())
                .await?
                .with_content_verifier(verifier);
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([1; 16]),
                idempotency_key: IdempotencyKey::new("bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            })
            .await?;
        let message = ConversationMessage {
            id: uuid::Uuid::from_bytes([2; 16]),
            sequence: 1,
            kind: MessageKind::User,
            content: file.clone(),
            attachments: Vec::new().into(),
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let mut missing = message.clone();
        missing.content = FileRef::new(
            volume,
            "messages/missing.txt",
            file.version(),
            file.descriptor().clone(),
            "missing.txt",
        )?;
        let command = |id: u8, value: ConversationMessage| -> Result<Command> {
            Ok(Command {
                operation_id: OperationId::from_bytes([id; 16]),
                idempotency_key: IdempotencyKey::new(format!("append-{id}"))?,
                expected_revision: 1,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(value),
                },
            })
        };
        assert!(aggregate.execute(command(3, missing)?).await.is_err());
        assert_eq!(aggregate.reducer().revision(), 1);
        let mut missing_attachment = message.clone();
        missing_attachment.attachments = vec![Attachment {
            file: FileRef::new(
                file.volume().clone(),
                "attachments/missing.bin",
                file.version(),
                file.descriptor().clone(),
                "missing.bin",
            )?,
            label: None,
        }]
        .into();
        assert!(
            aggregate
                .execute(command(5, missing_attachment)?)
                .await
                .is_err()
        );
        assert_eq!(aggregate.reducer().revision(), 1);
        aggregate.execute(command(4, message.clone())?).await?;
        assert_eq!(
            aggregate
                .reducer()
                .conversation()
                .map(|state| state.messages.as_slice()),
            Some(&[message][..])
        );
        let manifest_message = ConversationMessage {
            id: uuid::Uuid::from_bytes([6; 16]),
            sequence: 2,
            kind: MessageKind::User,
            content: file.clone(),
            attachments: manifest.clone(),
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        let mut bad_count = manifest_message.clone();
        if let ReferencedAttachments::Manifest { item_count, .. } = &mut bad_count.attachments {
            *item_count += 1;
        }
        assert!(
            aggregate
                .execute(Command {
                    operation_id: OperationId::from_bytes([7; 16]),
                    idempotency_key: IdempotencyKey::new("manifest-bad-count")?,
                    expected_revision: 2,
                    scope: scope.clone(),
                    causal_parent: None,
                    action: Action::AppendConversationMessage {
                        message: Box::new(bad_count)
                    },
                })
                .await
                .is_err()
        );
        assert_eq!(aggregate.reducer().revision(), 2);
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([8; 16]),
                idempotency_key: IdempotencyKey::new("manifest-valid")?,
                expected_revision: 2,
                scope,
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(manifest_message),
                },
            })
            .await?;
        assert_eq!(
            aggregate
                .reducer()
                .conversation()
                .map(|state| state.messages.len()),
            Some(2)
        );
        assert!(read.require(file.volume(), VolumeOperation::Read, None).is_ok());
        Ok(())
    }


}

mod stock_turn;
pub use stock_turn::StockTurnMachine;

async fn read_generation_with_io<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &acyclic_fs::Generation<A, O>, path: &str, maximum: u64, io: &PreparationIo,
) -> Result<acyclic_stream::OwnedBytes> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = io.original_native_work() {
        return generation.read_with_native_work(path, maximum, original, io.cancellation()).await
            .map_err(|(source, custody)| Error::NativeWorkspace { source, custody });
    }
    let bytes = io.receive(generation.read(path, maximum, io.remaining()?, io.cancellation()).await)?;
    let capacity = bytes.retained_capacity().ok_or_else(|| {
        fork_preparer_io::failure(Some(io), crate::wire::ErrorCode::Unsupported,
            "content producer does not expose its owned capacity")
    })?;
    io.retain_owned(capacity as u64)?;
    Ok(bytes)
}
