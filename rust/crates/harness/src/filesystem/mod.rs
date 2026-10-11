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
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef, Limits,
        MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryEntry, PrivateDirectoryEntryKind,
        PrivateDirectoryPage, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef, decode_complete_attachment_manifest,
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
    JoinOutcome, JoinPlan, MergeConflict, MergeDriverRegistry, MergePlan, MergeResolutionCache,
    PublicationReservation, TransactionCommit, Workspace, WorkspaceDirectoryPage, WorkspaceError,
    WorkspaceStat,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

const FILESYSTEM_JOIN_PROOF_FORMAT: &str = "acyclic.filesystem.join-commit.v2";

mod content_publisher;
pub use content_publisher::FilesystemContentPublisher;

mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod fork_preparer;
use fork_preparer::bounded_io as fork_preparer_io;
pub(crate) use fork_preparer::bounded_io::{PreparationIo, UploadRecord, PreparationRecord, ReservedDecoded, DecodeReservation};
mod summary_fork;
pub use fork_preparer::{FilesystemForkPreparer, FilesystemResourceCreationProvider};
mod interaction_host;
pub use interaction_host::FilesystemInteractionHost;
mod project_workspaces;
pub use project_workspaces::FilesystemProjectWorkspaces;
mod workflow_journal;
pub use workflow_journal::FilesystemWorkflowJournal;
pub(crate) use workflow_journal::WorkflowReleaseWitness;
mod memory;
pub use memory::{LocalHarness, MemoryHarnessStorage};
mod text_tools;
pub use text_tools::ExactTextReplacement;
mod root_source;
pub use root_source::FsRootProjectSourceProvider;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_execution;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_process;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_process::{NativeProcessAuthority, NativeProcessProvider, NativeProcessReceipt, NativeReceiptError};
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
pub(crate) use default_tools::finite_budget;
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
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_execution::{
    BorrowedNativeVolumeBinding, NativePreparationReceipt, NativePreparationRoot,
    NativeProcessView, NativeProcessNamespaceScopes, NativePublicationFailure, NativeViewOwner, NativeVolumeView,
    NativePublicationCleanup, NativePublicationReservation, NativePublicationReservationFailure,
    NativeReservationError,
    NativePreparationJournalError, NativePreparationJournalObservation, NativeRecordedPreparation,
    NativeViewPublicationError, NativeViewPublicationReceipt,
    copy_native_mapping_with_work, copy_native_volume_with_work, copy_native_generation_with_work,
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

/// Owner-scoped scheduler result staging into one agent-private Filesystem volume.
pub struct FilesystemSchedulerPayloadStore<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    volume: VolumeRef,
    grant: ContentGrant,
    maximum_bytes: u64,
}

impl<A, O> FilesystemSchedulerPayloadStore<A, O> {
    /// Binds the exact private volume and authenticated write capability.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        maximum_bytes: u64,
    ) -> Result<Self> {
        if volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_bytes == 0
        {
            return Err(Error::Invalid(
                "scheduler payload store requires a private volume and positive limit".into(),
            ));
        }
        let grant = ContentGrant::verify(verifier, scope, &volume, VolumeOperation::Write)?;
        Ok(Self {
            host,
            volume,
            grant,
            maximum_bytes,
        })
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
    ) -> BoxFuture<'a, Result<(FileRef, acyclic_fs::NativeAllocation)>> {
        Box::pin(async move {
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
            None, None, io).await?;
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
}

impl<A, O> FilesystemContentVerifier<A, O> {
    /// Binds a verified caller scope and an upper bound for each admitted file.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_bytes: u64,
    ) -> Result<Self> {
        Self::new_with_io(host, verifier, scope, maximum_bytes, None)
    }

    pub(crate) fn new_with_io(
        host: Arc<FilesystemHost<A, O>>,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid(
                "content admission limit must be positive".into(),
            ));
        }
        if let Some(io) = io {
            let mut observer = io.history_io();
            verifier.verify_with_work(&scope, &mut Some(&mut observer))?;
        } else {
            verifier.verify(&scope)?;
        }
        Ok(Self {
            host,
            verifier,
            scope,
            maximum_bytes,
        })
    }

    /// Newly owned scalar/string payload only; the deployment/keystore Arc is
    /// borrowed host authority, not a second owned filesystem.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.scope.logical_owned_bytes()?.checked_add(self.verifier.logical_owned_bytes()?)
            .ok_or_else(|| Error::Invalid("content verifier ownership overflow".into()))
    }
}

impl<A, O> ContentResidencyVerifier for FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn read_private_prefix<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        generation: &'a GenerationRef,
        maximum_bytes: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            if maximum_bytes == 0 || maximum_bytes > self.maximum_bytes {
                return Err(Error::Invalid("prefix read exceeds provider limit".into()));
            }
            let grant = ContentGrant::verify_directory_read(
                &self.verifier,
                &self.scope,
                volume,
                granted_prefix,
            )?;
            self.host
                .read_private_prefix(volume, &grant, path, generation, maximum_bytes)
                .await
                .map(|bytes| bytes.to_vec())
        })
    }

    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32,
    ) -> BoxFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async move {
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

    fn read_private_path<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
    ) -> BoxFuture<'a, Result<(FileRef, Vec<u8>)>> {
        Box::pin(async move {
            FilesystemContentVerifier::read_private_path(
                self,
                volume,
                granted_prefix,
                path,
                expected_generation,
            )
            .await
        })
    }

    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let grant = self.read_grant(reference)?;
            self.host
                .read_content(reference, &grant, self.maximum_bytes)
                .await?;
            Ok(())
        })
    }

    fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let grant = self.read_grant(reference)?;
            Ok(self
                .host
                .read_content(reference, &grant, self.maximum_bytes)
                .await?
                .to_vec())
        })
    }

    fn supports_measured_read(
        &self, volume: &VolumeRef, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<bool>, Error> {
        let io = PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = (|| {
            let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
            io.preflight(work)?;
            let result = volume.validate().map(|()| volume.provider() == &self.host.provider);
            io.charge(work)?;
            result
        })();
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    fn read_measured<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async move {
            let io = PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = self.read_verified_with_io(reference, maximum_bytes, &io).await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn read_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self.read_verified_with_io(reference, maximum_bytes, &io).await;
            let bytes = result?;
            let custody = io.into_native_custody(bytes.capacity() as u64)?;
            Ok((bytes, custody))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = async {
                self.require_read_measured(reference, &io)?;
                self.host.read_pinned_io(reference, maximum_bytes.min(self.maximum_bytes), Some(&io)).await?;
                io.remaining()?;
                Ok(())
            }.await;
            result
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_read(&self, volume: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork) -> Result<bool> {
        let io = PreparationIo::from_native_work(work)?;
        let result = (|| {
            fork_preparer_io::item(Some(&io), 0)?;
            volume.validate()?;
            Ok(volume.provider() == &self.host.provider)
        })();
        result
    }

    fn verify_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
        limits: &'a Limits,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            crate::conversation::verified_attachment_manifest(self, reference, item_count, limits)
                .await?;
            Ok(())
        })
    }

    fn load_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
    ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move {
            FilesystemContentVerifier::load_manifest(self, reference, item_count).await
        })
    }
}

impl<A, O> FilesystemContentVerifier<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn read_verified_with_io(&self, reference: &FileRef, maximum_bytes: u64, io: &PreparationIo) -> Result<Vec<u8>> {
        self.require_read_measured(reference, io)?;
        self.host.read_pinned_owned_io(reference, maximum_bytes.min(self.maximum_bytes), io).await
    }

    /// Lazily lists another agent's private directory under this reader's
    /// signed, owner-delegated subtree grant. Pagination pins one generation.
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
            granted_prefix,
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

    /// Lazily reads a named private file and returns its pinned ref and bytes;
    /// the exact version can subsequently be shared independently of lineage.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
        expected_generation: Option<&GenerationRef>,
    ) -> Result<(FileRef, Vec<u8>)> {
        let grant = ContentGrant::verify_directory_read(
            &self.verifier,
            &self.scope,
            volume,
            granted_prefix,
        )?;
        let (reference, bytes) = self
            .host
            .read_private_path(
                volume,
                &grant,
                path,
                expected_generation,
                self.maximum_bytes,
            )
            .await?;
        Ok((reference, bytes.to_vec()))
    }

    // `new` authenticated this immutable private scope. This borrow checks the
    // same original read capabilities without reserializing caller-owned signed
    // authority, constructing a broader grant, or exposing a proof token.
    fn require_read_measured(&self, reference: &FileRef, io: &PreparationIo) -> Result<()> {
        reference.validate()?;
        let internal = crate::conversation::is_internal_path(reference.path());
        let inherited = crate::conversation::is_inherited_context_path(reference.path());
        let internal_owner = reference.path().starts_with(".system/") && !inherited;
        if internal && !inherited && !internal_owner {
            return Err(Error::Unauthorized("internal storage file is not public content".into()));
        }
        if io.contains_capability(self.scope.capabilities(), reference.volume(), "volume:read:")? { return Ok(()); }
        if internal_owner {
            return Err(Error::Unauthorized("volume operation is not granted".into()));
        }
        if io.contains_capability(self.scope.capabilities(), reference, "file:read:")? { return Ok(()); }
        if !internal && reference.volume().class() == VolumeClass::AgentPrivate && self.scope.agent().is_some() {
            if io.contains_capability(self.scope.capabilities(), &(reference.volume(), ""), "directory:read:")? { return Ok(()); }
            for (end, _) in reference.path().match_indices('/') {
                let prefix = &reference.path()[..end];
                if io.contains_capability(self.scope.capabilities(), &(reference.volume(), prefix), "directory:read:")? { return Ok(()); }
            }
        }
        Err(Error::Unauthorized("file read is not granted by its owner".into()))
    }

    fn read_grant(&self, reference: &FileRef) -> Result<ContentGrant> {
        // Execution-journal tool calls/results are intentionally stored under
        // the owner's internal namespace. They are not public exact-file
        // references, but the owning scope still needs to resolve them while
        // replaying a model context. A whole-volume grant is required here so
        // delegated exact-file scopes cannot use this escape hatch.
        if reference.path().starts_with(".system/")
            && !crate::conversation::is_inherited_context_path(reference.path())
        {
            ContentGrant::verify(
                &self.verifier,
                &self.scope,
                reference.volume(),
                VolumeOperation::Read,
            )
        } else {
            ContentGrant::verify_read(&self.verifier, &self.scope, reference)
        }
    }

    async fn load_manifest(&self, reference: &FileRef, item_count: u32) -> Result<Vec<Attachment>> {
        let grant = self.read_grant(reference)?;
        let bytes = self
            .host
            .read_content(reference, &grant, self.maximum_bytes)
            .await?;
        crate::conversation::decode_attachment_manifest(reference, &bytes, item_count)
    }
}

/// Filesystem portion of the fork-publication admission barrier.
pub struct FilesystemForkVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    maximum_inherited_bytes: u64,
    maximum_total_reference_bytes: u64,
}

struct FilesystemForkPublicationGuard<A, O> {
    workspace: Workspace<A, O>,
    reservation: Option<PublicationReservation>,
}

impl<A, O> ForkPublicationGuard for FilesystemForkPublicationGuard<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
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
                .release_publication_measured(
                    self.reservation.ok_or_else(|| acyclic_fs::OperationFailure::before_work(
                        Error::Invalid("fork publication guard has no reservation".into()),
                    ))?,
                    budget,
                    cancellation,
                )
                .await
                .map_err(|failure| failure.map_with_prior_work(
                    acyclic_fs::WorkCounters::default(),
                    map_error,
                ))
        })
    }
}

impl UploadRecord for InheritedConversationPrefix {
    fn logical_owned_bytes(&self) -> Result<u64> {
        (std::mem::size_of::<Self>() as u64)
            .checked_add(self.parent.id.len() as u64)
            .and_then(|bytes| bytes.checked_add(
                (self.attached_agents.len() as u64)
                    .checked_mul(std::mem::size_of::<crate::AgentId>() as u64)?,
            ))
            .ok_or_else(|| Error::Invalid("inherited prefix ownership overflow".into()))
    }
}

impl fork_preparer_io::PreparationRecord for InheritedConversationPrefix {
    fn maximum_slot_bytes() -> u64 {
        [
            std::mem::size_of::<Self>(),
            std::mem::size_of::<crate::core::Authority>(),
            std::mem::size_of::<crate::AgentId>(),
            std::mem::size_of::<String>(),
            std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<(String, serde_json::Value)>(),
        ].into_iter().max().unwrap_or(0) as u64
    }
}

impl fork_preparer_io::PreparationRecord for crate::context::Context {
    fn maximum_slot_bytes() -> u64 {
        [
            std::mem::size_of::<Self>(),
            std::mem::size_of::<crate::model::ModelMessage>(),
            std::mem::size_of::<crate::model::ModelContent>(),
            std::mem::size_of::<crate::model::ModelContentPart>(),
            std::mem::size_of::<crate::model::ModelDataPart>(),
            std::mem::size_of::<crate::model::ToolResultContent>(),
            std::mem::size_of::<crate::model::FileProjectionPolicy>(),
            std::mem::size_of::<crate::model::NativeMediaPolicy>(),
            std::mem::size_of::<crate::model::NativeConfigurationBinding>(),
            std::mem::size_of::<crate::core::ExtensionConfiguration>(),
            std::mem::size_of::<crate::core::EventReference>(),
            std::mem::size_of::<crate::core::Authority>(),
            std::mem::size_of::<FileRef>(),
            std::mem::size_of::<serde_json::Value>(),
            std::mem::size_of::<(String, serde_json::Value)>(),
            std::mem::size_of::<(String, FileRef)>(),
            std::mem::size_of::<String>(),
        ].into_iter().max().unwrap_or(0) as u64
    }
}

impl UploadRecord for crate::context::Context {
    fn logical_owned_bytes(&self) -> Result<u64> {
        use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart, ModelDataPart, ToolResultContent};
        fn add(total: &mut u64, bytes: u64) -> Result<()> {
            *total = total.checked_add(bytes)
                .ok_or_else(|| Error::Invalid("fork Context ownership overflow".into()))?;
            Ok(())
        }
        fn value(total: &mut u64, json: &serde_json::Value) -> Result<()> {
            match json {
                serde_json::Value::String(text) => add(total, text.len() as u64)?,
                serde_json::Value::Array(values) => {
                    for entry in values {
                        add(total, std::mem::size_of::<serde_json::Value>() as u64)?;
                        value(total, entry)?;
                    }
                }
                serde_json::Value::Object(values) => {
                    for (key, entry) in values {
                        // Logical map payload only, not opaque allocator nodes.
                        add(total, std::mem::size_of::<(String, serde_json::Value)>() as u64)?;
                        add(total, key.len() as u64)?;
                        value(total, entry)?;
                    }
                }
                _ => {}
            }
            Ok(())
        }
        fn policy(total: &mut u64, selected: &FileProjectionPolicy) -> Result<()> {
            if let FileProjectionPolicy::Native(native) = selected {
                add(total, std::mem::size_of::<crate::model::NativeMediaPolicy>() as u64)?;
                if let Some(binding) = &native.configuration {
                    add(total, binding.source.authority.id.len() as u64)?;
                    add(total, binding.configuration.extension.name.len() as u64)?;
                    add(total, binding.configuration.content.logical_owned_bytes()?)?;
                }
            }
            Ok(())
        }
        fn part(total: &mut u64, entry: &ModelContentPart) -> Result<()> {
            match entry {
                ModelContentPart::Text { text } => add(total, text.len() as u64)?,
                ModelContentPart::File { file, policy: selected } => {
                    add(total, file.logical_owned_bytes()?)?;
                    policy(total, selected)?;
                }
                ModelContentPart::ToolCall { call_id, name, arguments } => {
                    add(total, call_id.len() as u64)?;
                    add(total, name.len() as u64)?;
                    value(total, arguments)?;
                }
                ModelContentPart::ToolResult { call_id, name, content } => {
                    add(total, call_id.len() as u64)?;
                    add(total, name.len() as u64)?;
                    match content {
                        ToolResultContent::Json { value: json } => value(total, json)?,
                        ToolResultContent::Parts { parts } => {
                            for data in parts {
                                add(total, std::mem::size_of::<ModelDataPart>() as u64)?;
                                match data {
                                    ModelDataPart::Text { text } => add(total, text.len() as u64)?,
                                    ModelDataPart::File { file, policy: selected } => {
                                        add(total, file.logical_owned_bytes()?)?;
                                        policy(total, selected)?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        let mut total = std::mem::size_of::<Self>() as u64;
        for message in &self.messages {
            add(&mut total, std::mem::size_of::<crate::model::ModelMessage>() as u64)?;
            match &message.content {
                ModelContent::Text(text) => add(&mut total, text.len() as u64)?,
                ModelContent::Part(entry) => part(&mut total, entry)?,
                ModelContent::Parts(entries) => {
                    for entry in entries {
                        add(&mut total, std::mem::size_of::<ModelContentPart>() as u64)?;
                        part(&mut total, entry)?;
                    }
                }
            }
        }
        for (key, file) in &self.metadata {
            add(&mut total, std::mem::size_of::<(String, FileRef)>() as u64)?;
            add(&mut total, key.len() as u64)?;
            add(&mut total, file.logical_owned_bytes()?)?;
        }
        Ok(total)
    }
}

fn fork_verify_reserve<T>(values: &mut Vec<T>, additional: usize, io: &PreparationIo) -> Result<()> {
    let needed = values.len().checked_add(additional)
        .ok_or_else(|| Error::Invalid("fork verification vector overflow".into()))?;
    if needed > values.capacity() {
        let bytes = (needed - values.capacity()).checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| Error::Invalid("fork verification allocation overflow".into()))? as u64;
        io.claim(bytes)?;
        io.allocation_attempt(bytes)?;
        values.try_reserve_exact(needed - values.len())
            .map_err(|error| Error::Invalid(error.to_string()))?;
    }
    Ok(())
}

fn fork_verify_text(value: &str, io: &PreparationIo) -> Result<String> {
    io.claim(value.len() as u64)?;
    io.allocation_attempt(value.len() as u64)?;
    let copy = acyclic_fs::WorkCounters { bytes_copied: value.len() as u64, ..Default::default() };
    io.preflight(copy)?;
    io.charge(copy)?;
    Ok(value.to_owned())
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
        .ok_or_else(|| Error::Invalid("fork verification copy overflow".into()))? as u64;
    let copies = acyclic_fs::WorkCounters { bytes_copied: shifted, ..Default::default() };
    io.preflight(copies)?;
    io.charge(copies)?;
    values.insert(low, value);
    Ok(true)
}

fn fork_verify_digest(value: &impl serde::Serialize, io: &PreparationIo) -> Result<[u8; 32]> {
    let bytes = io.encode_json(value, io.remaining()?.bytes_encoded, true)?;
    let hashed = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
    io.preflight(hashed)?;
    io.charge(hashed)?;
    let digest = *blake3::hash(&bytes).as_bytes();
    io.release(bytes.capacity() as u64)?;
    Ok(digest)
}

fn fork_verify_path(value: &str, io: &PreparationIo) -> Result<String> {
    let length = value.len().checked_add(1)
        .ok_or_else(|| Error::Invalid("fork verification path overflow".into()))?;
    io.claim(length as u64)?;
    io.allocation_attempt(length as u64)?;
    let copies = acyclic_fs::WorkCounters { bytes_copied: value.len() as u64, ..Default::default() };
    io.preflight(copies)?;
    io.charge(copies)?;
    let mut path = String::with_capacity(length);
    path.push('/');
    path.push_str(value);
    Ok(path)
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
        })
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
                // Allocate the actual release handle before reserving durably.
                let mut guard = Box::new(FilesystemForkPublicationGuard { workspace, reservation: None });
                let receipt = guard.workspace.reserve_publication_measured(
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
                // Retain the exact proof before any caller-side accounting.
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

    fn read_manifest<'a>(&'a self, manifest: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.read_manifest_measured(manifest, acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new()).await
                .map(|receipt| receipt.value).map_err(|failure| failure.error)
        })
    }

    fn read_manifest_measured<'a>(
        &'a self, manifest: &'a FileRef, budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async move {
            let io = if budget == acyclic_fs::WorkBudget::UNBOUNDED {
                PreparationIo::for_unmeasured(cancellation)
            } else {
                PreparationIo::from_budget(budget, cancellation)
            }.map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                if manifest.volume().provider() != &self.host.provider {
                    return Err(Error::Unauthorized("fork manifest belongs to another provider".into()));
                }
                let bytes = self.host.read_pinned_io(manifest, self.maximum_inherited_bytes, Some(&io)).await?;
                io.claim(bytes.len() as u64)?;
                io.allocation_attempt(bytes.len() as u64)?;
                let copied = acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
                io.preflight(copied)?;
                io.charge(copied)?;
                let output = bytes.to_vec();
                io.release(bytes.len() as u64)?;
                Ok(output)
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
            let io = if budget == acyclic_fs::WorkBudget::UNBOUNDED {
                PreparationIo::for_unmeasured(cancellation)
            } else {
                PreparationIo::from_budget(budget, cancellation)
            }.map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                if file.volume().provider() != &self.host.provider {
                    return Err(Error::Unauthorized("fork file belongs to another provider".into()));
                }
                let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                io.release(bytes.len() as u64)?;
                Ok(())
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
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
            let result = async {
                let mut validation = crate::store::history::HistoryWork::new(io.remaining()?, cancellation);
                let validated = seed.validate_with_work(&mut Some(
                    &mut validation as &mut dyn crate::store::history::HistoryIo,
                ));
                io.receive_content(validation.finish(validated))?;
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
                        if !io.receive(child_generation.matches_fork_selection_measured(
                            &source_generation, None, io.remaining()?, cancellation,
                        ).await)? {
                            return Err(Error::Invalid("project capture is not forked from the selected generation".into()));
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
                        .ok_or_else(|| Error::Invalid("fork reference bytes overflow".into()))?;
                    if total > self.maximum_total_reference_bytes {
                        return Err(Error::Invalid("fork reference bytes exceed aggregate limit".into()));
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
                        return Err(Error::Invalid("fork omitted its materialized conversation prefix".into()));
                    }
                    if let Some(file) = prefix {
                        if file.descriptor().media_type() != "application/vnd.acyclic.harness.inherited-conversation+json" {
                            return Err(Error::Invalid("inherited conversation has the wrong media type".into()));
                        }
                        let bytes = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                        let inherited = io.decode_preparation::<InheritedConversationPrefix>(&bytes)?;
                        let canonical = io.encode_json(&*inherited, io.remaining()?.bytes_encoded, true)?;
                        if canonical.as_slice() != bytes.as_ref() || file.display_name() != "inherited-conversation.json" {
                            return Err(Error::Invalid("inherited conversation is not canonical".into()));
                        }
                        if inherited.format_version != 3
                            || inherited.parent != seed.parent
                            || inherited.parent_revision != seed.parent_revision
                            || inherited.parent_agent == seed.child_agent
                            || inherited.through_sequence != seed.inherited_through_sequence
                            || !fork_verify_equal(&inherited.attached_agents, &seed.attached_agents, &io)?
                        {
                            return Err(Error::Invalid("inherited conversation does not match the fork boundary".into()));
                        }
                        io.release(canonical.capacity() as u64)?;
                        io.release(bytes.len() as u64)?;
                    }
                }
                if let Some(summary) = &seed.summary
                    && summary.context.volume().provider() == &self.host.provider
                {
                    let _bytes_hold;
                    let bytes = self.host.read_pinned_io(&summary.context, self.maximum_inherited_bytes, Some(&io)).await?;
                    _bytes_hold = io.guard_owned(bytes.len() as u64)?;
                    let context = io.decode_preparation::<crate::context::Context>(&bytes)?;
                    crate::context::validate_projected_context_with_io(&context, summary.selection.limits, Some(&io))?;
                    let _canonical_hold;
                    let canonical = io.encode_json(&*context, io.remaining()?.bytes_encoded, true)?;
                    _canonical_hold = io.guard_owned(canonical.capacity() as u64)?;
                    if context.current_input_index.is_some() || canonical.as_slice() != bytes.as_ref() {
                        return Err(Error::Invalid("summary capture is not a canonical historical Context".into()));
                    }
                    let _observed_hold;
                    let mut observed = Vec::new();
                    {
                        let mut examine = |file: &FileRef| -> Result<()> {
                            fork_preparer_io::item(Some(&io), 0)?;
                            let digest = fork_verify_digest(file, &io)?;
                            fork_verify_insert(&mut observed, digest, Ord::cmp, &io)
                        };
                        for message in &context.messages {
                            message.content.visit_file_bindings(|file, _| examine(file))?;
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
                        let digest = fork_verify_digest(file, &io)?;
                        fork_verify_insert(&mut declared, digest, Ord::cmp, &io)?;
                    }
                    _declared_hold = io.guard_owned((declared.capacity() * std::mem::size_of::<[u8; 32]>()) as u64)?;
                    if !fork_verify_equal(&observed, &declared, &io)? {
                        return Err(Error::Invalid("summary capture reference inventory differs from its Context".into()));
                    }
                }
                if seed.child_private_volume.provider() == &self.host.provider {
                    let private = fork_preparer_io::content_workspace(&self.host.provider, &seed.child_private_volume, Some(&io))?;
                    let private_head = fork_preparer_io::resolve(&self.host, &private, Some(&io)).await?.generation;
                    if private_head != seed.child_private_generation {
                        return Err(Error::Conflict("child private generation changed before fork admission".into()));
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
                    let scratch = scratch.ok_or_else(|| Error::Invalid("fork has no selected parent scratch lineage".into()))?;
                    let source_ref = fork_preparer_io::content_workspace(&self.host.provider, scratch.0, Some(&io))?;
                    let source_workspace = fork_preparer_io::open(&self.host, &source_ref, &io).await?;
                    let child_workspace = fork_preparer_io::open(&self.host, &private, &io).await?;
                    let source = fork_preparer_io::generation(&self.host, &source_workspace, scratch.1, &io).await?;
                    let initial = fork_preparer_io::generation(&self.host, &child_workspace, scratch.3, &io).await?;
                    let mut paths = Vec::new();
                    fork_verify_reserve(&mut paths, scratch.2.len(), &io)?;
                    for path in scratch.2 {
                        fork_preparer_io::item(Some(&io), 0)?;
                        paths.push(fork_verify_text(path, &io)?);
                    }
                    let selected_bytes = paths.capacity() * std::mem::size_of::<String>()
                        + paths.iter().map(String::len).sum::<usize>();
                    if !io.receive(initial.matches_fork_selection_measured(
                        &source, Some(paths), io.remaining()?, cancellation,
                    ).await)? {
                        return Err(Error::Invalid("scratch capture differs from its pinned selection".into()));
                    }
                    io.release(selected_bytes as u64)?;
                    let head = fork_preparer_io::generation(&self.host, &child_workspace, &private_head, &io).await?;
                    let diff_budget = io.remaining()?;
                    let changes = match initial.diff_to_bounded(&head, u32::MAX, diff_budget, cancellation).await {
                        Ok(receipt) => receipt.value,
                        Err(failure) => {
                            io.spent(*failure.work)?;
                            return Err(map_error(failure.error));
                        }
                    };
                    // Path resolution continues, rather than starts another diff
                    // span. Receive its cumulative prefix exactly once.
                    let changed_paths = io.receive(changes.changed_paths_bounded(u32::MAX, diff_budget, cancellation).await)?;
                    drop(changes);
                    let changes = changed_paths;
                    let mut changed_owned = (changes.capacity() * std::mem::size_of::<acyclic_fs::ChangedPath>()) as u64;
                    for change in &changes {
                        changed_owned = changed_owned.checked_add(std::mem::size_of_val(change.path.components()) as u64)
                            .ok_or_else(|| Error::Invalid("changed path ownership overflow".into()))?;
                        for component in change.path.components() {
                            changed_owned = changed_owned.checked_add(component.as_bytes().len() as u64)
                                .ok_or_else(|| Error::Invalid("changed path ownership overflow".into()))?;
                        }
                    }
                    io.retain_owned(changed_owned)?;
                    let mut expected = Vec::new();
                    let version_bytes = private_head.as_resource().key().len().checked_mul(2)
                        .ok_or_else(|| Error::Invalid("fork generation encoding overflow".into()))?;
                    io.claim(version_bytes as u64)?;
                    io.allocation_attempt(version_bytes as u64)?;
                    let version = hex::encode(private_head.as_resource().key());
                    for file in &seed.inherited_context {
                        fork_preparer_io::item(Some(&io), 0)?;
                        if file.version() != version {
                            if seed.summary.is_none() {
                                return Err(Error::Invalid("inherited file is not pinned to the selected private generation".into()));
                            }
                            let selected = fork_preparer_io::file_ref(file.volume(), file.path(), &version,
                                file.descriptor(), file.display_name(), Some(&io))?;
                            let original = self.host.read_pinned_io(file, self.maximum_inherited_bytes, Some(&io)).await?;
                            let final_bytes = self.host.read_pinned_io(&selected, self.maximum_inherited_bytes, Some(&io)).await?;
                            if original != final_bytes {
                                return Err(Error::Invalid("summary inherited payload changed before admission".into()));
                            }
                            io.release(original.len() as u64)?;
                            io.release(final_bytes.len() as u64)?;
                        }
                        let path = fork_verify_path(file.path(), &io)?;
                        let owned = path.len() as u64;
                        if !fork_verify_insert(&mut expected, path, Ord::cmp, &io)? { io.release(owned)?; }
                    }
                    if expected.len() != seed.inherited_context.len() {
                        return Err(Error::Invalid("inherited context has duplicate paths".into()));
                    }
                    let mut expected_directories = Vec::new();
                    fork_verify_insert(&mut expected_directories, fork_verify_text("/", &io)?, Ord::cmp, &io)?;
                    for path in &expected {
                        let mut parent = path.as_str();
                        while let Some((directory, _)) = parent.rsplit_once('/') {
                            fork_preparer_io::item(Some(&io), 0)?;
                            if directory.is_empty() { break; }
                            let directory_copy = fork_verify_text(directory, &io)?;
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
                                .map_err(|_| Error::Invalid("scratch path is not UTF-8".into()))?;
                            length = length.checked_add(name.as_bytes().len()).and_then(|bytes| bytes.checked_add(usize::from(index > 0)))
                                .ok_or_else(|| Error::Invalid("scratch path size overflow".into()))?;
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
                                .map_err(|_| Error::Invalid("scratch path is not UTF-8".into()))?);
                        }
                        match change.after {
                            Some(record) if record.kind == FileKind::Directory
                                && fork_verify_contains(&expected_directories, &path, &io)? && change.before.is_none() => { io.release(length as u64)?; }
                            Some(record) if record.kind == FileKind::Regular
                                && fork_verify_contains(&expected, &path, &io)? && change.before.is_none() => {
                                if !fork_verify_insert(&mut observed, path, Ord::cmp, &io)? { io.release(length as u64)?; }
                            }
                            _ => return Err(Error::Invalid("child scratch changed outside inherited context staging".into())),
                        }
                    }
                    if !fork_verify_equal(&observed, &expected, &io)? {
                        return Err(Error::Invalid("child private volume lacks selected inherited context".into()));
                    }
                    if fork_preparer_io::resolve(&self.host, &private, Some(&io)).await?.generation != private_head {
                        return Err(Error::Conflict("child private volume changed during fork admission".into()));
                    }
                    fork_preparer_io::retain(&self.host, &head, &io).await?;
                    io.release(changed_owned)?;
                    io.release(version_bytes as u64)?;
                    for collection in [&expected, &expected_directories, &observed] {
                        io.release((collection.capacity() * std::mem::size_of::<String>()) as u64)?;
                        for path in collection { io.release(path.len() as u64)?; }
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
                                fork_preparer_io::retain(&self.host, &generation, &io).await?;
                            }
                            ResourceRevision::SharedVolume(volume) => {
                                let reference = fork_preparer_io::content_workspace(&self.host.provider, volume, Some(&io))?;
                                fork_preparer_io::resolve(&self.host, &reference, Some(&io)).await?;
                            }
                            ResourceRevision::History(_) => return Err(Error::Unsupported("Filesystem cannot prove Stream history".into())),
                            ResourceRevision::Context(_) | ResourceRevision::Process(_)
                            | ResourceRevision::Artifact(_) | ResourceRevision::Extension { .. } => {
                                return Err(Error::Unsupported("fork resource requires its owning provider verifier".into()));
                            }
                        }
                    }
                }
                for (_, reference) in &unique_files {
                    let bytes = self.host.read_pinned_io(reference, self.maximum_inherited_bytes, Some(&io)).await?;
                    io.release(bytes.len() as u64)?;
                }
                Ok(())
            }.await;
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
        bytes: Vec<u8>,
    },
    /// Removes one file or empty directory.
    Remove {
        /// Canonical absolute path.
        path: String,
    },
}

/// Reserved host-owned file families. These are never accepted by the public
/// agent upload API, even when the agent owns the private volume.
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
#[derive(Clone)]
pub struct FilesystemHost<A, O> {
    filesystem: Fs<A, O>,
    provider: ProviderRef,
}

/// Provider-side proof for a parent-published project merge receipt.
pub struct FilesystemProjectMergeVerifier<A, O> {
    host: Arc<FilesystemHost<A, O>>,
}

impl<A, O> FilesystemProjectMergeVerifier<A, O> {
    /// Binds verification to one exact Filesystem provider identity.
    #[must_use]
    pub fn new(host: Arc<FilesystemHost<A, O>>) -> Self {
        Self { host }
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
            if witness.source_workspace() != source_workspace.id().volume_id()
                || witness.target_workspace() != target_workspace.id().volume_id()
                || witness.source_generation() != source_generation.id()
                || witness.expected_target() != expected.id()
                || witness.result_generation() != result.id()
                || witness.operation_id().into_bytes().as_slice()
                    != receipt.provider_operation_id.as_slice()
                || witness.history() != JoinHistory::Merge
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

/// Parent-owned project operations. A child may report its changes, but cannot
/// fork or prepare promotion using a scope issued for its own conversation.
pub struct ParentProjectController<'a, A, O> {
    host: &'a FilesystemHost<A, O>,
    parent: Authority,
    project: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
}

/// Inspected project changes whose publication remains bound to the parent controller.
/// The underlying Filesystem plan is deliberately not exposed.
pub struct ParentMergePlan<A, O> {
    plan: JoinPlan<A, O>,
    child_project: VolumeRef,
    parent_project: VolumeRef,
    parent_scope: Scope,
}

/// Destination-authorized volume import. The source workspace handle stays
/// private so possession of a read grant cannot expose source mutation APIs.
pub struct VolumeImportPlan<A, O> {
    plan: JoinPlan<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> VolumeImportPlan<A, O> {
    /// Immutable exact conflict input for a caller's admitted policy step.
    pub async fn describe_conflicts(
        &self,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan> {
        self.plan
            .describe_conflicts(conflicts, truncated)
            .await
            .map_err(map_error)
    }
    /// Applies a retained declarative candidate through the owning publisher.
    /// The caller pins any effectful policy and observation in its ordinary journal.
    pub async fn apply_candidate_with_permit(
        &self,
        key: &IdempotencyKey,
        candidate: &acyclic_fs::UnpublishedMergeCandidate,
        permit: acyclic_fs::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>> {
        self.plan
            .apply_candidate_with_permit(
                ApplyOptions {
                    if_target: self.target_head(),
                    idempotency_key: filesystem_key(key),
                },
                candidate,
                permit,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }
    /// Pinned source generation selected at inspection.
    pub fn source_head(&self) -> GenerationId {
        self.plan.source_head()
    }
    /// Destination CAS precondition selected at inspection.
    pub fn target_head(&self) -> GenerationId {
        self.plan.target_head()
    }
    /// Publishes only to the authorized destination using a stable retry key.
    pub async fn apply(&self, key: &IdempotencyKey) -> Result<JoinOutcome<A, O>> {
        self.plan
            .apply(ApplyOptions {
                if_target: self.target_head(),
                idempotency_key: filesystem_key(key),
            })
            .await
            .map_err(map_error)
    }
    /// Publishes exact conflict choices through the same destination CAS.
    pub async fn apply_sides(
        &self,
        key: &IdempotencyKey,
        sides: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>> {
        self.plan
            .apply_sides(
                ApplyOptions {
                    if_target: self.target_head(),
                    idempotency_key: filesystem_key(key),
                },
                sides,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }
    /// Uses owning Filesystem drivers and caller-owned exact resolution cache.
    /// Effectful drivers require durable admitted observations in that cache.
    pub async fn apply_with_drivers<C: MergeResolutionCache>(
        &self,
        key: &IdempotencyKey,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>> {
        self.plan
            .apply_with_drivers(
                ApplyOptions {
                    if_target: self.target_head(),
                    idempotency_key: filesystem_key(key),
                },
                registry,
                cache,
                replanning,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
    }
}

/// Prepared child-owned prefix and the exact read grants needed by every
/// attached reader. Feed these fields into the parent-controlled `ForkReport`.
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

    /// Exact common ancestor used for the three-way merge.
    #[must_use]
    pub const fn common_ancestor(&self) -> GenerationId {
        self.plan.common_ancestor()
    }
}

impl<'a, A, O> ParentProjectController<'a, A, O> {
    /// Binds an authenticated parent conversation to its exact project volume.
    pub fn new(
        host: &'a FilesystemHost<A, O>,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        project: VolumeRef,
    ) -> Result<Self> {
        if parent.authority().kind != crate::core::AggregateKind::Conversation {
            return Err(Error::Invalid(
                "project controller requires a parent conversation".into(),
            ));
        }
        verifier.verify_audience(parent.authority())?;
        verifier.verify(scope)?;
        if parent.conversation().and_then(|state| state.agent) != scope.agent()
            || scope.agent().is_none()
        {
            return Err(Error::Unauthorized(
                "project controller requires the bound parent agent".into(),
            ));
        }
        project.validate()?;
        if project.provider() != &host.provider {
            return Err(Error::Invalid(
                "parent project belongs to another provider or class".into(),
            ));
        }
        Ok(Self {
            host,
            parent: parent.authority().clone(),
            project,
            verifier: verifier.clone(),
            scope: scope.clone(),
        })
    }

    fn require(&self, capability: &str, operation: VolumeOperation) -> Result<()> {
        ContentGrant::verify(&self.verifier, &self.scope, &self.project, operation)?;
        if !self.scope.capabilities().contains(capability) {
            return Err(Error::Unauthorized(
                "parent project operation is not granted".into(),
            ));
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ParentProjectController<'_, A, O> {
    /// Freezes a completed exchange from the authoritative execution journal
    /// before retaining its shared segment. Missing or incomplete completion
    /// writes no prefix; the caller publishes the returned ref before activation.
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
            .completed_tool_prefix(journal, input, step, call_id)
            .await?;
        let segment = crate::model::ModelPrefix::select(&prepared, parent)?;
        let reference = self.stage_model_prefix(private, &segment).await?;
        Ok((reference, prepared))
    }

    /// Retains one immutable model segment in the parent's private volume.
    /// Identical segments reuse the exact pinned reference across child forks.
    /// Publish this ref in the completed tool result's conversation extensions
    /// so ordinary fork admission binds its exact reader grants before activation.
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
            ContentGrant::verify(&self.verifier, &self.scope, private, VolumeOperation::Write)?;
        let bytes = prefix.canonical_bytes()?;
        let digest = blake3::hash(&bytes).to_hex();
        self.host
            .put_internal_content(
                private,
                &grant,
                &format!(".system/inherited-conversation/model-prefix/{digest}.json"),
                &bytes,
                crate::model::ModelPrefix::MEDIA_TYPE,
                "model-prefix.json",
                crate::model::MAX_MODEL_REQUEST_BYTES,
                &IdempotencyKey::new(format!("model-prefix:{digest}"))?,
                InternalContentClass::ModelPrefix,
            )
            .await
    }

    /// Materializes an exact bounded parent prefix into a fresh child-private
    /// workspace. Only the parent controller can write this reserved path;
    /// ordinary agent file staging rejects every `.system` path.
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
        )
        .await
    }

    /// Projects the exact inherited reader capabilities before child admission,
    /// using the same traversal as materialization. This creates no child volume,
    /// prefix or grants. An archive must be the authenticated original parent cut.
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
            archive.map(crate::fork::PreparedForkHistory::conversation))?;
        let (grants, _) = Self::collect_inherited_reference_grants(history, child_agent,
            attached_agents, through_sequence, maximum_bytes, maximum_references, resolver, None).await?;
        Ok(grants)
    }

    fn selected_inherited_history<'p>(
        &self, parent: &'p Reducer, through_sequence: u64, maximum_messages: usize,
        archive: Option<&'p crate::conversation::ConversationState>,
    ) -> Result<&'p crate::conversation::ConversationState> {
        if parent.authority() != &self.parent
            || parent.conversation().and_then(|state| state.agent) != self.scope.agent() {
            return Err(Error::Unauthorized("inherited history is outside the actual parent".into()));
        }
        let parent_history = parent.conversation()
            .ok_or_else(|| Error::Invalid("parent is not a conversation".into()))?;
        let history = archive.unwrap_or(parent_history);
        if history.agent != parent_history.agent || through_sequence > parent_history.logical_revision()
            || (archive.is_some() && (history.resident_after_sequence() != 0
                || history.logical_revision() != through_sequence)) {
            return Err(Error::Conflict("inherited archive differs from its original parent cut".into()));
        }
        let count = usize::try_from(through_sequence)
            .map_err(|_| Error::Invalid("inherited prefix is too large".into()))?;
        if count > maximum_messages || count > history.messages.len() {
            return Err(Error::Invalid("inherited prefix exceeds the selected history or limit".into()));
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
            .map_err(|_| Error::Invalid("inherited prefix is too large".into()))?;
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
                            Error::Invalid("inherited manifests exceed byte limit".into())
                        })?;
                    if manifest_bytes_total > maximum_bytes {
                        return Err(Error::Invalid(
                            "inherited attachment manifest exceeds limit".into(),
                        ));
                    }
                    let items = match io {
                        Some(io) => io.receive_content(resolver.load_manifest_measured(manifest, *item_count,
                            io.remaining()?, &io.cancellation).await)?,
                        None => {
                            let bytes = resolver.read(manifest).await?;
                            decode_complete_attachment_manifest(manifest, &bytes)?
                        }
                    };
                    if let Some(io) = io { io.retain_owned(crate::conversation::manifest_owned_bytes(&items, items.capacity())?)?; }
                    if items.len() != *item_count as usize {
                        return Err(Error::Invalid(
                            "inherited attachment manifest count mismatch".into(),
                        ));
                    }
                    let key = fork_preparer_io::reference_key(manifest, io)?;
                    fork_preparer_io::item(io, (2 * std::mem::size_of::<(String, FileRef)>()) as u64)?;
                    if let Some(io) = io { fork_preparer_io::strings(io, &[&key, &key])?; }
                    direct.insert(key.clone(), fork_preparer_io::clone_file(manifest, io)?);
                    manifests.insert(key.clone(), fork_preparer_io::clone_file(manifest, io)?);
                    for item in items {
                        fork_preparer_io::item(io, std::mem::size_of::<((String, String), (FileRef, FileRef))>() as u64)?;
                        if let Some(io) = io { fork_preparer_io::strings(io, &[&key])?; }
                        member.insert(
                            (key.clone(), fork_preparer_io::reference_key(&item.file, io)?),
                            (fork_preparer_io::clone_file(manifest, io)?, item.file),
                        );
                    }
                }
            }
            if direct.len().saturating_add(member.len()) > maximum_references {
                return Err(Error::Invalid(
                    "inherited reference count exceeds limit".into(),
                ));
            }
        }
        if let Some(io) = io {
            for file in direct.values().chain(member.values().map(|(_, file)| file)) {
                io.receive_content(resolver.verify_measured(file, maximum_bytes,
                    io.remaining()?, &io.cancellation).await)?;
            }
            io.claim(((attached_agents.len() + 1) * std::mem::size_of::<AgentId>()) as u64)?;
        }
        let mut readers = BTreeSet::from([child_agent]);
        readers.extend(attached_agents.iter().copied());
        if readers.len() != attached_agents.len().saturating_add(1) {
            return Err(Error::Invalid("inherited readers are duplicated".into()));
        }
        let total_grants = direct
            .len()
            .saturating_add(member.len())
            .saturating_mul(readers.len());
        if total_grants > maximum_references {
            return Err(Error::Invalid("inherited read grants exceed limit".into()));
        }
        if let Some(io) = io {
            let bytes = total_grants.checked_mul(std::mem::size_of::<ReferenceGrant>())
                .ok_or_else(|| Error::Invalid("inherited grants ownership overflow".into()))? as u64;
            io.claim(bytes)?;
            io.allocation_attempt(bytes)?;
        }
        let mut reference_grants = Vec::with_capacity(total_grants);
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
        if let Some(io) = io {
            let bytes = manifests.len().checked_mul(std::mem::size_of::<FileRef>())
                .ok_or_else(|| Error::Invalid("inherited manifest vector ownership overflow".into()))? as u64;
            io.claim(bytes)?;
            io.allocation_attempt(bytes)?;
        }
        Ok((reference_grants, manifests.into_values().collect()))
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
    ) -> Result<InheritedContextCapture> {
        if let Some(io) = io {
            let project = io.capability(&self.project, "volume:read:")?;
            if !self.scope.capabilities().contains(&project)
                || !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
                return Err(Error::Unauthorized("parent fork preparation is not granted".into()));
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
            return Err(Error::Unauthorized(
                "inherited context is not parent-controlled and child-owned".into(),
            ));
        }
        child_private.validate()?;
        let history = self.selected_inherited_history(
            parent, through_sequence, maximum_messages, archived_history,
        )?;
        let prefix = if let Some(io) = io {
            if attached_agents.len() as u64 > crate::fork::MAX_FORK_AGENTS as u64 {
                return Err(Error::Invalid("too many attached agents for inherited context".into()));
            }
            let mut readers = BTreeSet::new();
            for reader in attached_agents {
                fork_preparer_io::item(Some(io), std::mem::size_of::<AgentId>() as u64)?;
                if !readers.insert(*reader) { return Err(Error::Invalid("inherited attached agents are invalid".into())); }
            }
            let message_digest = history.history_prefix_digest_measured(through_sequence, io)?;
            fork_preparer_io::strings(io, &[&self.parent.id])?;
            io.claim(std::mem::size_of_val(attached_agents) as u64)?;
            io.allocation_attempt(std::mem::size_of_val(attached_agents) as u64)?;
            InheritedConversationPrefix {
                format_version: 3, parent: self.parent.clone(), parent_revision: parent.revision(),
                parent_agent: history.agent.ok_or_else(|| Error::Invalid("inherited prefix parent is unbound".into()))?,
                through_sequence, attached_agents: attached_agents.to_vec(), message_digest,
            }
        } else {
            InheritedConversationPrefix::select(self.parent.clone(), parent.revision(), through_sequence, attached_agents, history)?
        };
        let (reference_grants, attachment_manifests) = Self::collect_inherited_reference_grants(
            history, child_agent, attached_agents, through_sequence, maximum_bytes,
            maximum_references, resolver, io).await?;
        let bytes = match io {
            Some(io) => io.encode_json(&prefix, maximum_bytes, true)?,
            None => prefix.canonical_bytes()?,
        };
        if bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "inherited context exceeds byte limit".into(),
            ));
        }
        let path = ".system/inherited-conversation/prefix.json";
        if let Some(io) = io {
            fork_preparer_io::strings(io, &["application/vnd.acyclic.harness.inherited-conversation+json"])?;
            let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
            io.preflight(hash)?;
            io.charge(hash)?;
        }
        let descriptor = FileDescriptor::from_bytes(
            &bytes,
            "application/vnd.acyclic.harness.inherited-conversation+json",
        )?;
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
                    return Err(Error::Conflict("child private volume changed after inherited-context staging".into()));
                }
            }
            prior
        } else {
            let initial = fork_preparer_io::resolve(self.host, &workspace, io).await?.generation;
            if &initial != initial_generation { return Err(Error::Conflict("child private volume is not fresh".into())); }
            if let Some(io) = io {
                io.claim(bytes.len() as u64)?;
                io.claim((path.len() + 1) as u64)?;
                fork_preparer_io::strings(io, &["/.system/inherited-conversation"])?;
                io.allocation_attempt(bytes.len() as u64)?;
                io.preflight(acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() })?;
            }
            let copied = bytes.clone();
            if let Some(io) = io { io.charge(acyclic_fs::WorkCounters { bytes_copied: copied.len() as u64, ..Default::default() })?; }
            let mutations = [
                WorkspaceMutation::CreateDirectory { path: "/.system/inherited-conversation".into() },
                WorkspaceMutation::PutFile { path: format!("/{path}"), bytes: copied },
            ];
            match io {
                Some(io) => fork_preparer_io::apply_generation(self.host, &workspace, Some(&initial), &mutations, idempotency_key, io).await?,
                None => self.host.apply_generation(&workspace, Some(&initial), &mutations, idempotency_key).await?,
            }
        };
        let generation = fork_preparer_io::generation_ref(self.host, &committed, io)?;
        if let Some(io) = io {
            fork_preparer_io::retain(self.host, &committed, io).await?;
            io.claim(64)?;
        } else { self.host.retain_generation(&workspace, &generation).await?; }
        let reference = fork_preparer_io::file_ref(child_private, path,
            &hex::encode(generation.as_resource().key()), &descriptor, "inherited-conversation.json", io)?;
        let committed = self.host.read_pinned_io(&reference, maximum_bytes, io).await?;
        if committed.as_ref() != bytes {
            return Err(Error::Conflict(
                "inherited-context retry identity belongs to different prefix".into(),
            ));
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

    /// Inspects an exact readable source, regardless of agent ancestry. Only
    /// this controller's destination can be published by the returned plan.
    pub async fn prepare_project_import(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
    ) -> Result<ParentMergePlan<A, O>> {
        self.require(capability::PROJECT_MERGE, VolumeOperation::Write)?;
        ContentGrant::verify(&self.verifier, &self.scope, source, VolumeOperation::Read)?;
        Ok(ParentMergePlan {
            plan: self
                .host
                .prepare_project_import(source, generation, &self.project)
                .await?,
            child_project: source.clone(),
            parent_project: self.project.clone(),
            parent_scope: self.scope.clone(),
        })
    }

    /// Publishes only a plan prepared under this same authenticated parent scope.
    /// The exact inspected target generation is always the Filesystem CAS precondition.
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

    /// Converts only a durable successful join into an immutable Harness
    /// receipt. Stale/conflicted/fenced outcomes cannot be published as merges.
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

    /// Resolves conflicts with registered immutable-input drivers under parent authority.
    pub async fn apply_project_merge_with_drivers<C: MergeResolutionCache>(
        &self,
        plan: &ParentMergePlan<A, O>,
        operation_id: crate::OperationId,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>> {
        self.require_merge_plan(plan)?;
        plan.plan
            .apply_with_drivers(
                Self::merge_options(plan, project_join_key(operation_id)?),
                registry,
                cache,
                replanning,
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))
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
        })
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

    /// Forks one explicitly selected scratch or skills volume through the
    /// Filesystem's existing allocation and retry mechanism. Grants authorize
    /// only the source read and the destination write; they grant no source writes.
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
        source_grant.require(source, VolumeOperation::Read)?;
        destination_grant.require(destination, VolumeOperation::Write)?;
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
        source.validate()?;
        child.validate()?;
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
            .fork(&child_name, options)
            .await
            .map_err(map_error)?;
        let child_workspace = workspace_ref(self.provider.clone(), &child_name)?;
        let initial = forked
            .operation_generation(filesystem_key(idempotency_key))
            .await
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

    /// Plans a separately requested scratch/skills integration. Project
    /// integration never calls this operation or implicitly integrates skills.
    pub async fn prepare_volume_import(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
    ) -> Result<VolumeImportPlan<A, O>> {
        source_grant.require(source, VolumeOperation::Read)?;
        destination_grant.require(destination, VolumeOperation::Write)?;
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
        source.validate()?;
        destination.validate()?;
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
        volume.validate()?;
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "volume belongs to another Filesystem provider".into(),
            ));
        }
        let name = volume.storage_name()?;
        self.filesystem
            .create_workspace(&name)
            .await
            .map_err(map_error)?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?)
            .await
    }

    /// Stages an immutable version of one volume file before conversation admission.
    ///
    /// The caller must append the returned reference only after this operation succeeds.
    /// A failed later append may leave an unreferenced pinned generation. Retention
    /// remains until the provider offers an explicit, safe release operation.
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
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            idempotency_key,
            None,
            None,
            None,
        )
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
            maximum_bytes, idempotency_key, None, None, Some(&io)).await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Publishes under the caller's exact original key on its native ledger.
    /// Returned metadata custody must remain live through the reference's last use.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original upload identity and native ledger")]
    pub async fn put_content_with_native_work(
        &self, volume: &VolumeRef, grant: &ContentGrant, path: &str, bytes: &[u8],
        media_type: &str, display_name: &str, maximum_bytes: u64,
        key: &IdempotencyKey, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(FileRef, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let file = self.put_content_impl(volume, grant, path, bytes, media_type, display_name,
            maximum_bytes, key, None, None, Some(&io)).await?;
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

    /// Recovers the caller's original operation generation, path and metadata;
    /// absence is authenticated original-operation absence, never mutable head state.
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

    /// Observes the original once-publication without guessing its path or
    /// adopting a later head. A returned reference supplies no read grant.
    pub async fn original_content_for_operation_measured(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        maximum_bytes: u64, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error> {
        let io = PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = self.content_for_operation_impl(volume, grant, key, None, maximum_bytes, Some(&io)).await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Unmeasured convenience over the same original-publication engine.
    pub async fn observe_content_publication(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        maximum_bytes: u64,
    ) -> Result<Option<FileRef>> {
        self.content_for_operation_impl(volume, grant, key, None, maximum_bytes, None).await
    }

    async fn content_for_operation_impl(
        &self, volume: &VolumeRef, grant: &ContentGrant, key: &IdempotencyKey,
        expected_path: Option<&str>, maximum_bytes: u64, io: Option<&PreparationIo>,
    ) -> Result<Option<FileRef>> {
        grant.require(volume, VolumeOperation::Write)?;
        volume.validate()?;
        self.validate_provider(volume.provider())?;
        if let Some(path) = expected_path {
            crate::conversation::validate_content_path(path)?;
            if path == ".system" || path.starts_with(".system/")
                || path == "turns/.canonical" || path.starts_with("turns/.canonical/") {
                return Err(Error::Invalid("internal storage paths are reserved".into()));
            }
        }
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let Some(original) = fork_preparer_io::operation_generation(self, &workspace, key, io).await? else { return Ok(None); };
        if let Some(io) = io {
            io.claim((1 + ".system/harness-uploads/".len() + 64 + ".json".len()) as u64)?;
            let hash = acyclic_fs::WorkCounters { bytes_hashed: key.as_str().len() as u64, ..Default::default() };
            io.preflight(hash)?;
            io.charge(hash)?;
        }
        let receipt_path = format!("/.system/harness-uploads/{}.json", blake3::hash(key.as_str().as_bytes()).to_hex());
        // Every committed upload record has one protocol-bounded path and a
        // closed set of labels, numeric fields and 32-byte generation keys.
        // Six is JSON's maximum escaped bytes per source UTF-8 byte.
        const RECORD_BYTES: u64 = 6 * (crate::conversation::MAX_PATH_BYTES as u64
            + 32 * crate::conversation::MAX_LABEL_BYTES as u64 + 1024);
        let receipt = match io {
            Some(io) => {
                let bytes = io.receive(original.read_measured(&receipt_path, RECORD_BYTES, io.remaining()?, &io.cancellation).await)?;
                io.retain_owned(bytes.len() as u64)?;
                bytes
            }
            None => original.read(&receipt_path, RECORD_BYTES).await.map_err(map_error)?,
        };
        let (recorded, expected): (FileRef, Option<GenerationRef>) = match io {
            Some(io) => io.decode_json(&receipt)?,
            None => crate::contract::json_from_slice(&receipt)?,
        };
        recorded.validate()?;
        if recorded.volume() != volume || recorded.version() != "pending"
            || expected_path.is_some_and(|path| path != recorded.path()) {
            return Err(Error::Conflict("original upload metadata differs from requested identity".into()));
        }
        if recorded.path() == ".system" || recorded.path().starts_with(".system/")
            || recorded.path() == "turns/.canonical" || recorded.path().starts_with("turns/.canonical/") {
            return Err(Error::Invalid("internal storage paths are reserved".into()));
        }
        let canonical = match io {
            Some(io) => io.encode_json(&(&recorded, expected.as_ref()), RECORD_BYTES, true)?,
            None => crate::contract::canonical_json_bytes(&(&recorded, expected.as_ref()))?,
        };
        if canonical != receipt.as_ref() {
            return Err(Error::Conflict("original upload receipt is not canonical".into()));
        }
        let metadata_path = content_metadata_path_io(recorded.path(), true, io)?;
        let metadata = match io {
            Some(io) => {
                let bytes = io.receive(original.read_measured(&metadata_path, RECORD_BYTES, io.remaining()?, &io.cancellation).await)?;
                io.retain_owned(bytes.len() as u64)?;
                bytes
            }
            None => original.read(&metadata_path, RECORD_BYTES).await.map_err(map_error)?,
        };
        let encoded = match io {
            Some(io) => io.encode_json(&recorded, RECORD_BYTES, false)?,
            None => serde_json::to_vec(&recorded).map_err(|error| Error::Invalid(error.to_string()))?,
        };
        if encoded != metadata.as_ref() {
            return Err(Error::Conflict("original upload metadata differs from its canonical receipt".into()));
        }
        let generation = fork_preparer_io::generation_ref(self, &original, io)?;
        match io {
            Some(io) => fork_preparer_io::retain(self, &original, io).await?,
            None => {
                original.pin(format!("harness-generation-{}", hex::encode(generation.as_resource().key())))
                    .await.map_err(map_error)?;
            }
        }
        if let Some(io) = io { io.claim(64)?; }
        let reference = fork_preparer_io::file_ref(volume, recorded.path(),
            &hex::encode(generation.as_resource().key()), recorded.descriptor(), recorded.display_name(), io)?;
        self.read_pinned_io(&reference, maximum_bytes, io).await?;
        Ok(Some(reference))
    }

    /// Publishes content only at an explicitly pinned workspace generation.
    /// Retry binds that generation as well as the complete file contract.
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
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            idempotency_key,
            None,
            Some(expected_generation),
            None,
        )
        .await
    }

    /// Trusted journal staging retains the same atomic metadata and retry
    /// checks as public uploads, but may write only its assigned subtree.
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
        self.put_content_impl(
            volume,
            grant,
            path,
            bytes,
            media_type,
            display_name,
            maximum_bytes,
            idempotency_key,
            Some(class),
            None,
            None,
        )
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
    ) -> Result<FileRef> {
        grant.require(volume, VolumeOperation::Write)?;
        if volume.provider() != &self.provider || bytes.len() as u64 > maximum_bytes {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        if let Some(io) = io {
            let hash = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
            io.preflight(hash)?;
            io.charge(hash)?;
            fork_preparer_io::strings(io, &[media_type])?;
        }
        let descriptor = FileDescriptor::from_bytes(bytes, media_type)?;
        // Validate the path before opening a workspace or writing bytes.
        let provisional = fork_preparer_io::file_ref(volume, path, "pending", &descriptor, display_name, io)?;
        // The same operation ID must pin the entire file contract, not just
        // the byte payload. A sidecar in the atomic generation proves path,
        // media type, display name, and descriptor on every retry.
        match internal_class {
            Some(class)
                if volume.class() == VolumeClass::AgentPrivate
                    && provisional.path().starts_with(class.prefix()) => {}
            Some(_) => {
                return Err(Error::Unauthorized(
                    "journal path or volume is not assigned to this host".into(),
                ));
            }
            None if provisional.path() == ".system"
                || provisional.path().starts_with(".system/")
                || provisional.path() == "turns/.canonical"
                || provisional.path().starts_with("turns/.canonical/") =>
            {
                return Err(Error::Invalid("internal storage paths are reserved".into()));
            }
            None => {}
        }
        // Internal live slots use path identity for their sidecar; operation
        // generation lookup still uses the original retry key.
        let receipt_identity = if internal_class.is_some() { provisional.path() } else { idempotency_key.as_str() };
        if let Some(io) = io {
            io.claim((".system/harness-uploads/".len() + 64 + ".json".len()) as u64)?;
            let hash = acyclic_fs::WorkCounters {
                bytes_hashed: receipt_identity.len() as u64,
                ..Default::default()
            };
            io.preflight(hash)?;
            io.charge(hash)?;
        }
        let receipt_path = format!(
            ".system/harness-uploads/{}.json",
            blake3::hash(receipt_identity.as_bytes()).to_hex()
        );
        let metadata_path = content_metadata_path_io(provisional.path(), false, io)?;
        let metadata_bytes = match io {
            Some(io) => io.encode_json(&provisional, io.remaining()?.bytes_encoded, false)?,
            None => serde_json::to_vec(&provisional).map_err(|error| Error::Invalid(error.to_string()))?,
        };
        let receipt_bytes = match io {
            Some(io) => io.encode_json(&(&provisional, expected_generation), io.remaining()?.bytes_encoded, true)?,
            None => crate::contract::canonical_json_bytes(&(&provisional, expected_generation))?,
        };
        let workspace = fork_preparer_io::content_workspace(&self.provider, volume, io)?;
        let opened = match io {
            Some(io) => fork_preparer_io::open(self, &workspace, io).await?,
            None => self.open(&workspace).await?,
        };
        let prior = match io {
            Some(io) => {
                let hash = acyclic_fs::WorkCounters { bytes_hashed: idempotency_key.as_str().len() as u64, ..Default::default() };
                io.preflight(hash)?;
                io.charge(hash)?;
                io.receive(opened.operation_generation_measured(
                    filesystem_key(idempotency_key), io.remaining()?, &io.cancellation,
                ).await)?
            }
            None => opened.operation_generation(filesystem_key(idempotency_key)).await.map_err(map_error)?,
        };
        if let Some(prior) = prior {
            let generation = fork_preparer_io::generation_ref(self, &prior, io)?;
            match io {
                Some(io) => fork_preparer_io::retain(self, &prior, io).await?,
                None => self.retain_generation(&workspace, &generation).await?,
            }
            if let Some(io) = io { io.claim(64)?; }
            let version = hex::encode(generation.as_resource().key());
            if let Some(io) = io {
                fork_preparer_io::strings(io, &["application/json"])?;
                let hash = acyclic_fs::WorkCounters { bytes_hashed: receipt_bytes.len() as u64, ..Default::default() };
                io.preflight(hash)?;
                io.charge(hash)?;
            }
            let receipt_descriptor = FileDescriptor::from_bytes(&receipt_bytes, "application/json")?;
            let receipt = fork_preparer_io::file_ref(volume, &receipt_path, &version, &receipt_descriptor, "receipt.json", io)?;
            self.read_pinned_io(&receipt, receipt_bytes.len() as u64, io).await?;
            let indexed = match io {
                Some(io) => {
                    io.claim((metadata_path.len() + 1) as u64)?;
                    let indexed = io.receive(prior.read_measured(
                        &format!("/{metadata_path}"), metadata_bytes.len() as u64,
                        io.remaining()?, &io.cancellation,
                    ).await)?;
                    io.retain_owned(indexed.len() as u64)?;
                    indexed
                }
                None => self.read(&workspace, Some(&generation), &format!("/{metadata_path}"), metadata_bytes.len() as u64).await?,
            };
            if indexed.as_ref() != metadata_bytes.as_slice() {
                return Err(Error::Conflict(
                    "upload metadata index does not match".into(),
                ));
            }
            let reference = fork_preparer_io::file_ref(volume, path, &version, &descriptor, display_name, io)?;
            let committed = self
                .read_pinned_io(&reference, maximum_bytes, io)
                .await?;
            if committed.as_ref() != bytes {
                return Err(Error::Conflict(
                    "upload identity belongs to different content".into(),
                ));
            }
            return Ok(reference);
        }
        if let Some(io) = io {
            io.claim((provisional.path().len() + 1) as u64)?;
            io.claim((6 * std::mem::size_of::<WorkspaceMutation>()) as u64)?;
            fork_preparer_io::strings(io, &["/.system/harness-uploads", "/.system/harness-file-metadata"])?;
            io.claim((metadata_path.len() + 1) as u64)?;
            io.claim((receipt_path.len() + 1) as u64)?;
        }
        let absolute = format!("/{}", provisional.path());
        if let Some(io) = io {
            io.claim(bytes.len() as u64)?;
            let copied = acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
            io.preflight(copied)?;
        }
        if let Some(io) = io { io.allocation_attempt((6 * std::mem::size_of::<WorkspaceMutation>()) as u64)?; }
        let mut mutations = Vec::with_capacity(6);
        if let Some((parent, _)) = absolute.rsplit_once('/')
            && !parent.is_empty()
        {
            if let Some(io) = io { fork_preparer_io::strings(io, &[parent])?; }
            mutations.push(WorkspaceMutation::CreateDirectory {
                path: parent.to_owned(),
            });
        }
        if let Some(io) = io { io.allocation_attempt(bytes.len() as u64)?; }
        let owned_bytes = bytes.to_vec();
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() })?;
        }
        mutations.push(WorkspaceMutation::PutFile {
            path: absolute,
            bytes: owned_bytes,
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: "/.system/harness-uploads".into(),
        });
        mutations.push(WorkspaceMutation::CreateDirectory {
            path: "/.system/harness-file-metadata".into(),
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: format!("/{metadata_path}"),
            bytes: metadata_bytes,
        });
        mutations.push(WorkspaceMutation::PutFile {
            path: format!("/{receipt_path}"),
            bytes: receipt_bytes,
        });
        let generation = match io {
            Some(io) => fork_preparer_io::apply_generation(self, &workspace, expected_generation, &mutations, idempotency_key, io).await?,
            None => self.apply_generation(&workspace, expected_generation, &mutations, idempotency_key).await?,
        };
        let generation_reference = fork_preparer_io::generation_ref(self, &generation, io)?;
        // Keep the validated commit object in custody until its retention pin
        // is durable; reopening by identity would leave a reclamation gap.
        if let Some(io) = io { io.claim(64)?; }
        let version = hex::encode(generation_reference.as_resource().key());
        match io {
            Some(io) => fork_preparer_io::retain(self, &generation, io).await?,
            None => { generation.pin(format!("harness-generation-{version}")).await.map_err(map_error)?; }
        }
        let reference = fork_preparer_io::file_ref(volume, path, &version, &descriptor, display_name, io)?;
        // Read back the exact committed generation, not the mutable workspace head.
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
    ) -> Result<Bytes> {
        reference.validate()?;
        grant.require_file_read(reference)?;
        let bytes = self.read_pinned(reference, maximum_bytes).await?;
        let generation = self.file_generation(reference)?;
        let workspace = workspace_ref(self.provider.clone(), &reference.volume().storage_name()?)?;
        // Admission cannot publish a ref whose verified generation can be
        // reclaimed before (or immediately after) the Stream append.
        self.retain_generation(&workspace, &generation).await?;
        Ok(bytes)
    }

    pub(super) async fn read_pinned_owned_io(
        &self, reference: &FileRef, maximum_bytes: u64, io: &PreparationIo,
    ) -> Result<Vec<u8>> {
        let before = io.owned_bytes()?;
        let source_hold;
        let bytes = self.read_pinned_io(reference, maximum_bytes, Some(io)).await?;
        source_hold = io.guard_owned(io.owned_bytes()?.checked_sub(before)
            .ok_or_else(|| Error::Invalid("content read custody underflow".into()))?)?;
        io.claim(bytes.len() as u64)?;
        let copied = acyclic_fs::WorkCounters { bytes_copied: bytes.len() as u64, ..Default::default() };
        io.preflight(copied)?;
        io.allocation_attempt(bytes.len() as u64)?;
        let output = bytes.to_vec();
        io.charge(copied)?;
        drop(bytes);
        drop(source_hold);
        Ok(output)
    }

    /// Reads and retains the exact immutable file under cumulative work limits.
    pub async fn read_content_measured(
        &self, reference: &FileRef, grant: &ContentGrant, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error> {
        let io = fork_preparer_io::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = async {
            reference.validate()?;
            grant.require_file_read(reference)?;
            self.read_pinned_owned_io(reference, maximum_bytes, &io).await
        }.await;
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Discovers a granted private directory at the owner's current head, or
    /// continues at the immutable generation returned by a prior page.
    /// The physical workspace is opened only when this method is called;
    /// callers need no fork or pre-mounted copy of the owner's tree.
    pub async fn list_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<(GenerationRef, WorkspaceDirectoryPage)> {
        self.require_private_directory(volume, grant, path)?;
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

    /// Lazily resolves a named private file and returns an immutable ref plus
    /// verified bytes. This is directory authority, never write authority.
    pub async fn read_private_path(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        expected_generation: Option<&GenerationRef>,
        maximum_bytes: u64,
    ) -> Result<(FileRef, Bytes)> {
        self.require_private_directory(volume, grant, path)?;
        let name = path.rsplit('/').next().unwrap_or(path);
        let provisional = FileRef::new(
            volume.clone(),
            path,
            "pending",
            FileDescriptor::from_bytes(&[], "application/octet-stream")?,
            name,
        )?;
        let workspace = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        let observation = self.resolve(&workspace).await?;
        let generation = expected_generation.unwrap_or(&observation.generation);
        self.retain_generation(&workspace, generation).await?;
        let bytes = self
            .read(
                &workspace,
                Some(generation),
                &format!("/{}", provisional.path()),
                maximum_bytes,
            )
            .await?;
        let metadata = self
            .read(
                &workspace,
                Some(generation),
                &format!("/{}", content_metadata_path(provisional.path())),
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
                staged.descriptor().verify(&bytes)?;
                (
                    staged.descriptor().clone(),
                    staged.display_name().to_owned(),
                )
            }
            Err(Error::NotFound(_)) => (
                FileDescriptor::from_bytes(&bytes, "application/octet-stream")?,
                name.to_owned(),
            ),
            Err(error) => return Err(error),
        };
        let reference = FileRef::new(
            volume.clone(),
            path,
            hex::encode(generation.as_resource().key()),
            descriptor,
            display_name,
        )?;
        grant.require_file_read(&reference)?;
        Ok((reference, bytes))
    }

    /// Reads only a finite authenticated prefix at an explicitly pinned generation.
    /// It grants no write authority and does not resolve a live head.
    pub async fn read_private_prefix(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
        generation: &GenerationRef,
        maximum_bytes: u64,
    ) -> Result<Bytes> {
        self.require_private_directory(volume, grant, path)?;
        crate::conversation::validate_content_path(path)?;
        if maximum_bytes == 0 {
            return Err(Error::Invalid("prefix read limit must be positive".into()));
        }
        let reference = workspace_ref(self.provider.clone(), &volume.storage_name()?)?;
        self.retain_generation(&reference, generation).await?;
        let workspace = self.open(&reference).await?;
        let pinned = self.generation(&workspace, generation).await?;
        let path = format!("/{path}");
        let stat = pinned.stat(&path).await.map_err(map_error)?;
        if stat.kind != FileKind::Regular {
            return Err(Error::Invalid("prefix requires a regular file".into()));
        }
        let length = stat
            .logical_bytes
            .ok_or_else(|| Error::Storage("file length is missing".into()))?;
        pinned
            .read_range(&path, 0, maximum_bytes.min(length))
            .await
            .map_err(map_error)
    }

    fn require_private_directory(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        path: &str,
    ) -> Result<()> {
        volume.validate()?;
        if !path.is_empty() {
            crate::conversation::validate_content_path(path)?;
        }
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "private directory belongs to another provider or class".into(),
            ));
        }
        grant.require_directory_path(volume, path)
    }

    async fn read_pinned(&self, reference: &FileRef, maximum_bytes: u64) -> Result<Bytes> {
        self.read_pinned_io(reference, maximum_bytes, None).await
    }

    async fn read_pinned_io(
        &self, reference: &FileRef, maximum_bytes: u64,
        io: Option<&fork_preparer_io::PreparationIo>,
    ) -> Result<Bytes> {
        if reference.volume().provider() != &self.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        if let Some(io) = io {
            io.claim((reference.version().len() / 2) as u64)?;
            fork_preparer_io::strings(io, &[self.provider.namespace(), self.provider.family(), self.provider.version()])?;
            io.claim((reference.path().len() + 1) as u64)?;
        }
        let generation = self.file_generation(reference)?;
        let workspace = fork_preparer_io::content_workspace(&self.provider, reference.volume(), io)?;
        let path = format!("/{}", reference.path());
        let bytes = match io {
            Some(io) => {
                let opened = fork_preparer_io::open(self, &workspace, io).await?;
                let pinned = fork_preparer_io::generation(self, &opened, &generation, io).await?;
                let bytes = io.receive(pinned.read_measured(&path, maximum_bytes, io.remaining()?, &io.cancellation).await)?;
                io.retain_owned(bytes.len() as u64)?;
                fork_preparer_io::retain(self, &pinned, io).await?;
                bytes
            }
            None => self.read(&workspace, Some(&generation), &path, maximum_bytes).await?,
        };
        reference.descriptor().verify_with_io(&bytes, io)?;
        Ok(bytes)
    }

    fn file_generation(&self, reference: &FileRef) -> Result<GenerationRef> {
        let raw = hex::decode(reference.version())
            .map_err(|_| Error::Invalid("file version is not a Filesystem generation".into()))?;
        GenerationRef::new(self.provider.clone(), raw, None)
    }

    /// Resolves the current immutable head of a named workspace.
    pub async fn resolve(&self, reference: &WorkspaceRef) -> Result<WorkspaceObservation> {
        let workspace = self.open(reference).await?;
        let head = workspace.head().await.map_err(map_error)?;
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
    ) -> Result<Bytes> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .read(path, maximum_bytes)
                .await
                .map_err(map_error),
            None => workspace.read(path, maximum_bytes).await.map_err(map_error),
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
                .await?
                .stat(path)
                .await
                .map_err(map_error),
            None => workspace.stat(path).await.map_err(map_error),
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
                .await?
                .list_directory(path, after, maximum_entries)
                .await
                .map_err(map_error),
            None => workspace
                .list_directory(path, after, maximum_entries)
                .await
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
            None => workspace.begin_transaction(key).await.map_err(map_error)?,
        };
        for mutation in mutations {
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
        match transaction.commit().await.map_err(map_error)? {
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
            .open_workspace(name)
            .await
            .map_err(map_error)
    }

    async fn open_measured(
        &self,
        reference: &WorkspaceRef,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Workspace<A, O>>, Error> {
        reference.validate().map_err(acyclic_fs::OperationFailure::before_work)?;
        self.validate_provider(reference.as_resource().provider())
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let name = std::str::from_utf8(reference.as_resource().key()).map_err(|_| {
            acyclic_fs::OperationFailure::before_work(Error::Invalid(
                "workspace reference key must be UTF-8".into(),
            ))
        })?;
        self.filesystem
            .open_workspace_measured(name, budget, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), map_error))
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
        workspace
            .generation(GenerationId::new(Digest::from_bytes(bytes)))
            .await
            .map_err(map_error)
    }

    async fn generation_measured(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Generation<A, O>>, Error> {
        reference.validate().map_err(acyclic_fs::OperationFailure::before_work)?;
        self.validate_provider(reference.as_resource().provider())
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let bytes: [u8; 32] = reference.as_resource().key().try_into().map_err(|_| {
            acyclic_fs::OperationFailure::before_work(Error::Invalid(
                "generation reference key must be 32 bytes".into(),
            ))
        })?;
        workspace
            .generation_measured(GenerationId::new(Digest::from_bytes(bytes)), budget, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(acyclic_fs::WorkCounters::default(), map_error))
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
        file.validate()?;
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
    const PREFIX: &str = ".system/harness-file-metadata/";
    const SUFFIX: &str = ".json";
    let capacity = usize::from(absolute) + PREFIX.len() + 64 + SUFFIX.len();
    let hash_work = acyclic_fs::WorkCounters { bytes_hashed: path.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(hash_work)?; }
    let hash = blake3::hash(path.as_bytes());
    if let Some(io) = io {
        io.charge(hash_work)?;
        io.claim(capacity as u64)?;
        io.allocation_attempt(capacity as u64)?;
    }
    let mut encoded = String::with_capacity(capacity);
    if absolute { encoded.push('/'); }
    encoded.push_str(PREFIX);
    encoded.push_str(&hash.to_hex());
    encoded.push_str(SUFFIX);
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
        | WorkspaceError::Work(_) => Error::Invalid(error.to_string()),
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

    #[tokio::test]
    async fn resolves_exact_memory_generation_and_rejects_foreign_provider() -> Result<()> {
        let filesystem = Fs::memory();
        let workspace = filesystem
            .create_workspace("adapter-test")
            .await
            .map_err(map_error)?;
        let expected = workspace.head().await.map_err(map_error)?;
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
                    bytes: b"hello".to_vec(),
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
                    bytes: b"changed".to_vec(),
                }],
                &IdempotencyKey::new("stale-write")?,
            )
            .await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn publication_custody_retains_original_commit_after_head_advances() -> Result<()> {
        let filesystem = Fs::memory();
        filesystem.create_workspace("publication-custody").await.map_err(map_error)?;
        let provider = ProviderRef::new("custody", "filesystem", "1")?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        let workspace = workspace_ref(provider, "publication-custody")?;
        let initial = host.resolve(&workspace).await?;
        let key = IdempotencyKey::new("custody-original")?;
        let mutations = [WorkspaceMutation::PutFile {
            path: "/original".into(),
            bytes: b"original".to_vec(),
        }];
        let committed = host
            .apply_generation(&workspace, Some(&initial.generation), &mutations, &key)
            .await?;
        let original = host.generation_ref(&committed)?;
        committed
            .pin(format!("harness-generation-{}", hex::encode(original.as_resource().key())))
            .await
            .map_err(map_error)?;
        host.apply(
            &workspace,
            Some(&original),
            &[WorkspaceMutation::PutFile {
                path: "/original".into(),
                bytes: b"later".to_vec(),
            }],
            &IdempotencyKey::new("custody-later")?,
        ).await?;
        assert_eq!(committed.read("/original", 16).await.map_err(map_error)?.as_ref(), b"original");
        assert_eq!(host.read(&workspace, Some(&original), "/original", 16).await?.as_ref(), b"original");
        assert_eq!(host.read(&workspace, None, "/original", 16).await?.as_ref(), b"later");
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
        let verifier = issuer.verifier();
        let read = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read)?;
        let write = ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write)?;
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
        let delegated_scope = issuer.delegate_private_directory_read(
            &scope,
            AgentId::from_bytes([8; 16]),
            "reader",
            &volume,
            "messages",
        )?;
        let delegated_reader = FilesystemContentVerifier::new(
            host.clone(),
            verifier.clone(),
            delegated_scope.clone(),
            16,
        )?;
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
            ContentGrant::verify_directory_read(&verifier, &delegated_scope, &volume, "messages")?;
        let (discovered, bytes) = host
            .read_private_path(&volume, &delegated, "messages/first.txt", None, 16)
            .await?;
        assert_eq!(bytes, Bytes::from_static(b"two"));
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
        assert!(ContentGrant::verify(&verifier, &scope, &other, VolumeOperation::Read).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn conversation_stream_admission_requires_resident_files() -> Result<()> {
        use crate::conversation::{Attachment, ConversationMessage, MessageKind};
        use crate::core::{Action, Command, SchemaRegistry};
        use crate::store::StreamAggregate;
        use crate::{IdempotencyKey, OperationId};
        use acyclic_stream::{MemoryStream, StreamClient};
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
            ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Read)?;
        let write =
            ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
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
        let verifier = Arc::new(FilesystemContentVerifier::new(
            Arc::new(host),
            issuer.verifier(),
            scope.clone(),
            4_096,
        )?);
        let stream = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut aggregate =
            StreamAggregate::open(&stream, authority, issuer.verifier(), SchemaRegistry::new())
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
        assert!(read.require(file.volume(), VolumeOperation::Read).is_ok());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn execution_journal_emits_spans_without_content_fields() -> Result<()> {
        use crate::executor::{ExecutionEvent, ExecutionJournal as _};
        use crate::obs::capture;
        let (seen, _guard) = capture::install();
        let provider = ProviderRef::new("example", "filesystem", "3")?;
        let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
        let owner = AgentId::from_bytes([5; 16]);
        let volume = VolumeRef::new(
            provider,
            "agent-private-traced",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
        )?;
        host.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "test",
            [6; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "task".into(),
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
        let journal = FilesystemExecutionJournal::new(
            acyclic_stream::StreamClient::new(Arc::new(acyclic_stream::MemoryStream::default())),
            host,
            volume,
            issuer.verifier(),
            scope,
            65_536,
        )?;
        let operation = crate::OperationId::from_bytes([7; 16]);
        let request = journal
            .stage(
                operation,
                "request".into(),
                b"{\"secret\":1}".to_vec(),
                "application/json",
            )
            .await?;
        assert_eq!(journal.load(&request).await?.len(), 12);
        let prepared_request = crate::model::PreparedModelRequest::prepare(
            crate::model::ModelRequest {
                model: crate::model::Model::new(
                    "synthetic",
                    "journal-test",
                    "1",
                    serde_json::json!({}),
                )?,
                messages: vec![crate::model::ModelMessage {
                    role: crate::model::ModelRole::User,
                    content: crate::model::ModelContent::Text("fixture request".into()),
                }],
                tools: Vec::new(),
                max_output_tokens: Some(32),
            },
            Limits::default(),
        )?;
        let model_digest = prepared_request.manifest().request_digest;
        let request = journal
            .stage(
                operation,
                "prepared-request".into(),
                prepared_request.bytes().to_vec(),
                "application/json",
            )
            .await?;
        journal
            .append(
                operation,
                "start".into(),
                ExecutionEvent::Started {
                    request_digest: model_digest,
                },
            )
            .await?;
        assert!(
            journal
                .append_if_tail(
                    operation,
                    1,
                    "model".into(),
                    ExecutionEvent::ModelStarted {
                        purpose: crate::executor::ModelPurpose::Response,
                        step: 0,
                        request_digest: model_digest,
                        request
                    },
                )
                .await?
        );
        assert_eq!(journal.replay(operation, 0, 8).await?.len(), 2);
        let unkeyed = ExecutionEvent::Started {
            request_digest: [2; 32],
        };
        assert!(
            journal
                .append(operation, String::new(), unkeyed)
                .await
                .is_err()
        );

        let has = |span, field, value| capture::has(&seen, span, field, value);
        assert!(has("acyclic.harness.journal.stage", "bytes", "12"));
        assert!(has("acyclic.harness.journal.load", "bytes", "12"));
        assert!(has("acyclic.harness.journal.append", "outcome", "ok"));
        assert!(has(
            "acyclic.harness.journal.append",
            "error.kind",
            "ERROR_CODE_INVALID"
        ));
        assert!(has("acyclic.harness.journal.append_if_tail", "rev", "1"));
        assert!(has("acyclic.harness.journal.replay", "items", "2"));
        capture::assert_clean(&seen, &["secret", "agent-private-traced", ".system"]);
        Ok(())
    }
}

mod stock_turn;
pub use stock_turn::StockTurnMachine;
