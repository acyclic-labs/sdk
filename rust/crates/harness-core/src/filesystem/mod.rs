#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        reason = "adapter tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../../docs/filesystem.md")]
use crate::{
    Error, IdempotencyKey, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef,
        MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryPage, ReferencedAttachments, VolumeClass,
        VolumeOperation, VolumeRef,
    },
    core::{AuthorityVerifier, Scope},
    resources::{GenerationRef, ProviderRef, WorkspaceRef},
};
use acyclic_fs::kernel::{AttributeClass, AttributeName, FileKind, LogicalName};
use acyclic_fs_work::performance::WorkCounters;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};
use {
    acyclic_fs::{
        AsyncAuthorityStore, AsyncObjectStore, DirectoryPage, Fs, Generation,
        IdempotencyKey as FilesystemKey, JoinPlan, TransactionCommit, Workspace, WorkspaceError,
        WorkspaceStat,
    },
    acyclic_fs_format::object_reference::GenerationId,
};
mod content_input;
use content_input::{ContentInput, ContentPublication, HashedSource, PublicationInput};
mod content_publisher;
mod content_replacement;
pub use content_publisher::FilesystemContentPublisher;
mod execution_journal;
#[cfg(test)]
pub(crate) use execution_journal::ExecutionReplayObservation;
pub use execution_journal::FilesystemExecutionJournal;
#[cfg(feature = "test-support")]
pub use execution_journal::codec_fuzz as execution_codec_fuzz;
#[cfg(test)]
pub(crate) use execution_journal::test_support::journal as test_execution_journal;
#[cfg(not(target_arch = "wasm32"))]
pub use execution_journal::{
    ContextCheckpoint, ContextPublicationReceipt, ContextPublicationRequest,
};
mod agent_effects;
mod agent_goal;
pub use agent_goal::HOME_VOLUME;
mod agent_internal;
pub mod attestation;
mod history;
pub use history::read as history_checkpoint;
pub mod hosting;
mod spawn_volumes;
pub use spawn_volumes::{
    FilesystemSpawnVolumes, ForkCutError, VolumeResources, spawned_binding, spawned_operation,
};
mod fs_tools;
pub use fs_tools::FilesystemTools;
mod templates;
pub use templates::Templates;
mod stock_turns;
pub use stock_turns::{Grants, StockExecutors, Tasks, TurnContext};
mod agent_steering;
mod agent_turns;
pub use agent_turns::{
    AgentAuthority, FilesystemTurnHost, PRIVATE_VOLUME, TurnAdmission, TurnAuthority,
    TurnExecutors, TurnLimits, agent_authority, agent_journal_authority, private_volume,
};
pub(crate) use agent_turns::{ceiling_grants, inherited_workload_grants};
pub(crate) mod bounded_io;
mod tool_resource;
pub use tool_resource::TOOL_OUTPUT;
#[cfg(not(target_arch = "wasm32"))]
mod tool_output;
#[doc(hidden)]
pub use bounded_io::PreparationRecord;
pub(crate) use bounded_io::PreparationScope;
#[doc(hidden)]
pub use bounded_io::UploadRecord;
use bounded_io::decode_record;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use bounded_io::native_decoded;
pub use bounded_io::{Charged, PreparationCustody, PreparationIo};
pub(crate) use bounded_io::{DecodeReservation, retain_decoded};
#[cfg(not(target_arch = "wasm32"))]
pub use tool_output::{AgentObservation, session_authority, session_trust};
mod memory;
mod run_admission;
pub use memory::MemoryHarnessStorage;
mod text_tools;
pub use text_tools::ExactTextReplacement;
#[cfg(not(target_arch = "wasm32"))]
mod native_access;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod native_process;
#[cfg(not(target_arch = "wasm32"))]
pub use native_process::NativeProcessObserver;
mod workspace_access;
pub use workspace_access::{
    WorkspaceAccess, require_unrestricted_volume_access, require_workspace_subtree_read,
};
mod native_capture_bounds;
mod native_contract;
#[cfg(not(target_arch = "wasm32"))]
pub use native_access::NativeMountAuthority;
#[cfg(not(target_arch = "wasm32"))]
pub use native_access::{
    local_owner_mount_options, local_owner_workspace, local_owner_workspace_access,
};
pub use native_contract::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeViewManifest, NativeViewOptions, NativeVolumeMapping,
    NativeVolumeNamespace,
};
mod config;
use config::*;
pub use config::{FilesystemJournalBinding, WorkspaceMutation, WorkspaceObservation};
pub(crate) mod host;
pub(crate) use host::content_path;
pub(crate) use host::map_error;
pub use host::reads::media_type::captured as captured_media_type;
use host::*;
pub use host::{FilesystemHost, VolumeImportPlan, filesystem_key, workspace_ref};
pub(crate) mod access;
pub use access::INITIAL_POLICY as INITIAL_ACCESS_POLICY;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use access::check_access_record;
mod content_verifier;
pub use content_verifier::FilesystemContentVerifier;
/// Pins a generation that a published file version names, under the one
/// label every unmeasured publication uses, so the version stays readable.
#[doc(hidden)]
pub async fn retain_published_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    generation: &Generation<A, O>,
) -> Result<()> {
    bounded_io::retain(generation, None)
        .await
        .map_err(|error| match error {
            acyclic_fs::LazyWorkspaceError::WorkspaceCause(error) => map_error(error),
            error => error.into(),
        })
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    #[allow(
        clippy::too_many_arguments,
        reason = "one implementation pins the complete staged file contract"
    )]
    fn put_content_impl<'a, B: ContentInput + ?Sized>(
        &'a self,
        volume: &'a VolumeRef,
        grant: &'a ContentGrant,
        path: &'a str,
        input: &'a B,
        media_type: &'a str,
        display_name: &'a str,
        maximum_bytes: Option<u64>,
        idempotency_key: &'a IdempotencyKey,
        internal_class: Option<InternalContentClass>,
        expected_generation: Option<&'a GenerationRef>,
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a bounded_io::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>>
    where
        A: 'static,
        O: 'static,
    {
        self.put_content_input(
            ContentPublication {
                volume,
                grant,
                path,
                media_type,
                display_name,
                maximum_bytes,
                idempotency_key,
                internal_class,
                expected_generation,
                ceiling,
                io,
            },
            input.publication_input(),
        )
    }

    fn put_content_input<'a>(
        &'a self,
        request: ContentPublication<'a>,
        mut input: PublicationInput<'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>>
    where
        A: 'static,
        O: 'static,
    {
        let ContentPublication {
            volume,
            grant,
            path,
            media_type,
            display_name,
            maximum_bytes,
            idempotency_key,
            internal_class,
            expected_generation,
            ceiling,
            io,
        } = request;
        // Refuse invalid or unauthorized input before consuming any source.
        grant.require(volume, VolumeOperation::Write, io)?;
        // A conditional write commits to the authored workspace. A private
        // namespace cut holds unpublished private writes that no authored
        // commit can be conditioned on, so it is never a write precondition.
        if expected_generation.is_some_and(|cut| cut.as_resource().version().is_some()) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                PRIVATE_CUT_PRECONDITION,
            ));
        }
        if let Some(grants) = ceiling
            && !crate::executor::with_history_io(io, |work| {
                crate::core::volume_operation_granted_with_work(
                    grants,
                    volume,
                    VolumeOperation::Write,
                    work,
                )
            })?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a filesystem write grant",
            ));
        }
        if volume.provider() != &self.provider
            || input.bytes().is_some_and(|bytes| {
                maximum_bytes.is_some_and(|maximum| bytes.len() as u64 > maximum)
            })
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "content provider or size limit mismatch",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            crate::conversation::validate_content_path(path, work)
        })?;
        InternalContentClass::require_path(volume, internal_class, path, io)?;
        if internal_class.is_some() && volume.class() != VolumeClass::AgentPrivate {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "internal publication requires its assigned private volume",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(&input, PublicationInput::Stream(_))
            && io.and_then(PreparationIo::original_native_work).is_some()
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "stream source lacks original native allocation custody; recovery: use ContentPublisher::stage_once with already owned bytes",
            ));
        }
        // Construction returns before polling this concrete publication graph.
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let unmeasured;
            let active_io = if let Some(io) = io {
                io
            } else {
                unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &unmeasured
            };
            let io = Some(active_io);
            // Metadata is admitted once before workspace or source IO.
            let descriptor = if let Some(bytes) = input.bytes() {
                crate::executor::with_history_io(io, |work| {
                    FileDescriptor::from_bytes(bytes, media_type, work)
                })?
            } else {
                FileDescriptor::new([0; 32], 0, stock_string_copy(media_type, io)?)?
            };
            let operation = bounded_io::operation_key(idempotency_key, io)?;
            let provisional = crate::executor::with_history_io(io, |work| {
                FileRef::new(
                    volume.copy(work)?,
                    stock_string_copy(path, io)?,
                    bounded_io::hex_key(&operation.into_bytes(), io)?,
                    descriptor,
                    stock_string_copy(display_name, io)?,
                    work,
                )
            })?;
            let workspace = bounded_io::content_workspace(&self.provider, volume, io)?;
            let root = self.open(&workspace, io).await?;
            // Host records stay with the same volume owner as their readers
            // and journal. User content follows the selected project branch.
            let binding = self
                .content_operation_workspace(&root, operation, internal_class.is_some(), active_io)
                .await?;
            let needs_binding = binding.is_none();
            let (opened, binding) = match binding {
                Some(binding) => binding,
                None => (self.select_current_workspace(&root, active_io).await?, None),
            };
            if needs_binding {
                let (generation, fence) = self
                    .authorize_content_write(&root, grant, path, false, ceiling, io)?
                    .await?;
                let _hold = active_io.guard_owned(generation.logical_owned_bytes()?)?;
                self.bind_content_operation(
                    &root,
                    opened.id(),
                    operation,
                    (fence, grant),
                    active_io,
                )?
                .await?;
            }
            let prior = active_io.sdk(
                opened
                    .operation_publication(operation, None, active_io, active_io.cancellation())
                    .await,
            )?;
            let root_cut = if opened.id() != root.id() && prior.is_none() {
                Some(
                    active_io.sdk(
                        root.capture_policy_cut(active_io, active_io.cancellation())
                            .await,
                    )?,
                )
            } else {
                None
            };
            let root_authorization = if opened.id() != root.id() {
                let (generation, fence) = self
                    .authorize_content_write(&root, grant, path, prior.is_none(), ceiling, io)?
                    .await?;
                let hold = active_io.guard_owned(generation.logical_owned_bytes()?)?;
                Some((generation, hold, fence))
            } else {
                None
            };
            let root_fence = root_authorization.as_ref().map(|(_, _, fence)| *fence);
            let (authorized_generation, policy_fence) = self
                .authorize_content_write(&opened, grant, path, prior.is_none(), ceiling, io)?
                .await?;
            let generation_hold = io
                .map(|io| io.guard_owned(authorized_generation.logical_owned_bytes()?))
                .transpose()?;
            let authorized_generation = (authorized_generation, generation_hold);
            if let Some(binding) = binding {
                bounded_io::retain(&binding, io).await?;
            }
            // The original transaction stages immutable chunks. Its one
            // publication below binds the resulting digest and metadata.
            let mut staged = None;
            let mut provisional = if input.bytes().is_some() {
                provisional
            } else {
                let prepared = crate::conversation::BoundedContentFuture::new(io, || async {
                use sha2::Digest;
                let PublicationInput::Stream(source) = &mut input else {
                    return Err(stock_boundary(io, crate::wire::ErrorCode::Invalid,
                        "content input is not a stream; recovery: supply the admitted publication source"));
                };
                let mut staged = None;
                let mut source = HashedSource {
                    source: &mut **source, io: active_io, hash: sha2::Sha256::new(), length: 0,
                };
                if prior.is_some() {
                    // Replay needs the request digest, never new content puts.
                    let mut chunk = [0; 8192];
                    loop {
                        let remaining = maximum_bytes.map(|maximum| maximum.saturating_sub(source.length));
                        let maximum = remaining.map_or(chunk.len() as u64, |remaining| remaining.saturating_add(1));
                        let read = source.read(&mut chunk, maximum, active_io.cancellation())
                            .await.map_err(|error| {
                                if active_io.cancellation().is_cancelled() { Error::Cancelled }
                                else { Error::Storage(format!("content source read failed: {error}; recovery: recreate the complete original source and replay the same publication")) }
                            })?;
                        active_io.charge(acyclic_fs_work::performance::WorkCounters { source_bytes_read: read as u64, ..Default::default() })?;
                        if maximum_bytes.is_some_and(|maximum| source.length > maximum) {
                            return Err(stock_boundary(io, crate::wire::ErrorCode::Invalid,
                                "content source exceeds admitted size; recovery: supply content within the publisher's byte credit"));
                        }
                        if read == 0 { break; }
                    }
                } else {
                    let transaction = active_io.sdk(opened.begin_transaction(
                        operation, active_io, active_io.cancellation()).await)?;
                    // The publisher's independent byte credit funds staging.
                    let staging_bytes = maximum_bytes.map(|maximum| maximum.max(1));
                    let content = active_io.sdk(transaction.stage_content(
                        &mut source, staging_bytes, active_io, active_io.cancellation()).await)
                        .map_err(|error| if active_io.cancellation().is_cancelled() { Error::Cancelled } else { error })?;
                    if maximum_bytes.is_some_and(|maximum| source.length > maximum) {
                        return Err(stock_boundary(io, crate::wire::ErrorCode::Invalid,
                            "content source exceeds admitted size; recovery: supply content within the publisher's byte credit"));
                    }
                    staged = Some((transaction, content));
                }
                let provisional = provisional.with_content_identity(source.hash.finalize().into(), source.length)?;
                Ok((provisional, staged))
                })?.await?;
                staged = prepared.1;
                prepared.0
            };
            let metadata_key = content_metadata_key_io(volume, provisional.path(), io)?;
            let receipt_bytes = match io {
                Some(io) => io.encode(
                    &(1_u8, (&provisional, expected_generation)),
                    io.remaining()?.bytes_encoded,
                    true,
                )?,
                None => acyclic_stream::OwnedBytes::from(crate::contract::canonical_json_bytes(
                    &(1_u8, (&provisional, expected_generation)),
                )?),
            };
            if let Some(prior) = prior {
                if host::publication_evidence(prior.evidence(), io)? != receipt_bytes.as_ref() {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "upload identity belongs to a different request",
                    ));
                }
                let prior = prior.into_generation();
                let generation = (self).generation_ref_id(prior.id(), io)?;
                bounded_io::retain(&prior, io).await?;
                let version = bounded_io::hex_key(generation.as_resource().key(), io)?;
                let indexed = read_file_annotation(
                    &prior,
                    provisional.path(),
                    &metadata_key,
                    Some(receipt_bytes.len() as u64),
                    io,
                )
                .await?;
                if indexed.as_deref() != Some(receipt_bytes.as_ref()) {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "upload metadata index does not match",
                    ));
                }
                crate::executor::with_history_io(io, |work| {
                    provisional.pin_version(version, work)
                })?;
                grant.recheck(io)?;
                return self.seal_publication(provisional, io);
            }
            let absolute = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("/{}", provisional.path()), work)
            })?;
            if let Some(expected) = expected_generation
                && expected != &authorized_generation.0
            {
                self.require_unchanged_content(
                    &opened,
                    expected,
                    &authorized_generation.0,
                    (&absolute, &metadata_key),
                    io,
                )?
                .await?;
            }
            let policy_fence = if expected_generation.is_some() {
                policy_fence.pin_generation()
            } else {
                policy_fence
            };
            let mut mutations = crate::model::vec_with_capacity(2, io)?;
            if let Some((parent, _)) = absolute.rsplit_once('/')
                && !parent.is_empty()
            {
                mutations.push(WorkspaceMutation::CreateDirectory {
                    path: stock_string_copy(parent, io)?,
                });
            }
            let staged = match staged {
                Some((transaction, content)) => Some((transaction, absolute, content)),
                None => {
                    mutations.push(WorkspaceMutation::PutFile {
                        path: absolute,
                        bytes: input.mutation_bytes(io)?,
                    });
                    None
                }
            };
            let generation = self
                .apply_authorized_generation(
                    &opened,
                    (policy_fence, grant, root_fence),
                    root_cut,
                    &mutations,
                    IdempotencyKey(stock_string_copy(idempotency_key.as_str(), io)?),
                    active_io,
                    receipt_bytes,
                    metadata_key,
                    staged,
                )?
                .await?;
            let generation_reference = (self).generation_ref_id(generation.id(), io)?;
            let version = bounded_io::hex_key(generation_reference.as_resource().key(), io)?;
            bounded_io::retain(&generation, io).await?;
            crate::executor::with_history_io(io, |work| provisional.pin_version(version, work))?;
            // Carry the verified immutable publication through runtime admission.
            grant.recheck(io)?;
            self.seal_publication(provisional, io)
        })
    }
    #[inline(never)]
    fn read_retained_file<'a>(
        &'a self,
        generation: &'a Generation<A, O>,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, crate::conversation::Custody>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(all(test, not(target_arch = "wasm32")))]
            pinned_read_tests::retained::sample_items(false, io)?;
            #[cfg(all(test, not(target_arch = "wasm32")))]
            if pinned_read_tests::retained::reopen() {
                let result = self.read_pinned_io(reference, maximum_bytes, io).await;
                pinned_read_tests::retained::sample_items(true, io)?;
                return result.map(Charged::into_custody);
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
            #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
            acyclic_differential::record(("read", maximum_bytes));
            let unmeasured;
            let read_io = if let Some(io) = io {
                io
            } else {
                unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &unmeasured
            };
            let bytes = read_generation(generation, &path, Some(maximum_bytes), read_io).await?;
            crate::executor::with_history_io(io, |work| {
                reference.descriptor().verify(&bytes, work)
            })?;
            #[cfg(all(test, not(target_arch = "wasm32")))]
            pinned_read_tests::retained::sample_items(true, io)?;
            Ok(bytes.into_custody())
        })
    }
    async fn read_pinned_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&bounded_io::PreparationIo>,
    ) -> Result<Charged<acyclic_stream::OwnedBytes>> {
        // Keep the Result temporary across await for the original admitted caller frame.
        (match io {
            None => crate::conversation::BoundedContentFuture::new(None, || {
                self.read_pinned_unmetered(reference, maximum_bytes, pinned_read::bytes_only)
            }),
            Some(io) => {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = io.original_native_work() {
                    return crate::conversation::BoundedContentFuture::new(Some(io), || {
                        self.read_pinned_native(reference, maximum_bytes, io, original)
                    })?
                    .await;
                }
                crate::conversation::BoundedContentFuture::new(Some(io), || {
                    self.read_pinned_finite(reference, maximum_bytes, io)
                })
            }
        })?
        .await
    }

    async fn read_pinned_receipt_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&bounded_io::PreparationIo>,
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
        F: FnOnce(Charged<acyclic_stream::OwnedBytes>, Option<Generation<A, O>>) -> T
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
                bounded_io::content_workspace(&self.provider, reference.volume(), None)?;
            let path = format!("/{}", reference.path());
            // Keep the authenticated immutable handle from the read. A
            // retaining caller can pin it without opening the workspace
            // and authenticating this same generation a second time.
            #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
            acyclic_differential::record(("read", maximum_bytes));
            let owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            let opened = self.open(&workspace, Some(&owned)).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if self.selects_private_cut(&opened, &generation) {
                let (bytes, cut) = Box::pin(self.read_pinned_private(
                    &opened,
                    &generation,
                    reference,
                    maximum_bytes,
                    false,
                    &owned,
                ))
                .await?;
                reference.descriptor().verify(&bytes, &mut None)?;
                return Ok(project(bytes, Some(cut)));
            }
            let pinned = self.generation(&opened, &generation, Some(&owned)).await?;
            let bytes = owned.sdk(
                pinned
                    .read(&path, maximum_bytes, &owned, owned.cancellation())
                    .await,
            )?;
            // The bytes carry their charge on this read's own ledger.
            let bytes = owned.retain_read_bytes(bytes)?;
            reference.descriptor().verify(&bytes, &mut None)?;
            Ok(project(bytes, Some(pinned)))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_pinned_native(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: &bounded_io::PreparationIo,
        original: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Charged<acyclic_stream::OwnedBytes>> {
        let ((path, path_custody), pinned) = {
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
            let workspace =
                bounded_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
            let path = original
                .display(&format_args!("/{}", reference.path()))
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
            let opened = self.open(&workspace, Some(io)).await?;
            if self.selects_private_cut(&opened, &generation) {
                let (bytes, _) = Box::pin(self.read_pinned_private(
                    &opened,
                    &generation,
                    reference,
                    maximum_bytes,
                    true,
                    io,
                ))
                .await?;
                crate::executor::with_history_io(Some(io), |work| {
                    reference.descriptor().verify(&bytes, work)
                })?;
                return Ok(bytes);
            }
            let pinned = self.generation(&opened, &generation, Some(io)).await?;
            // The authenticated generation owns its proof. Retire the
            // temporary selectors before admitting the file data.
            (path, pinned)
        };
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("read", maximum_bytes));
        let bytes = pinned
            .read(&path, maximum_bytes, original, io.cancellation())
            .await
            .map_err(|source| Error::NativeWorkspace {
                source: Box::new(source),
                custody: None,
            })?;
        drop(path);
        drop(path_custody);
        bounded_io::retain(&pinned, Some(io)).await?;
        crate::executor::with_history_io(Some(io), |work| {
            reference.descriptor().verify(&bytes, work)
        })?;
        // Native bytes already carry their original allocation.
        io.retain_read_bytes(bytes)
    }

    async fn read_pinned_finite(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: &bounded_io::PreparationIo,
    ) -> Result<Charged<acyclic_stream::OwnedBytes>> {
        if reference.volume().provider() != &self.provider
            || reference.descriptor().byte_length() > maximum_bytes
        {
            return Err(Error::Invalid(
                "content provider or size limit mismatch".into(),
            ));
        }
        let path_bytes = (reference.path().len() + 1) as u64;
        io.retain_owned(path_bytes)?;
        let path_hold = io.guard_owned(path_bytes)?;
        let generation = self.file_generation(reference, Some(io))?;
        let generation_hold = io.guard_owned(generation.logical_owned_bytes()?)?;
        let workspace =
            bounded_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
        let workspace_hold = io.guard_owned(workspace.logical_owned_bytes()?)?;
        let path = format!("/{}", reference.path());
        let opened = self.open(&workspace, Some(io)).await?;
        drop(workspace);
        drop(workspace_hold);
        #[cfg(not(target_arch = "wasm32"))]
        if self.selects_private_cut(&opened, &generation) {
            let (bytes, _) = Box::pin(self.read_pinned_private(
                &opened,
                &generation,
                reference,
                maximum_bytes,
                true,
                io,
            ))
            .await?;
            crate::executor::with_history_io(Some(io), |work| {
                reference.descriptor().verify(&bytes, work)
            })?;
            return Ok(bytes);
        }
        let pinned = self.generation(&opened, &generation, Some(io)).await?;
        drop(generation);
        drop(generation_hold);
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("read", maximum_bytes));
        let bytes = io.receive(
            pinned
                .read(&path, maximum_bytes, io.remaining()?, io.cancellation())
                .await,
        )?;
        drop(path);
        drop(path_hold);
        // The charge lives exactly as long as the returned bytes.
        let bytes = io.retain_read_bytes(bytes)?;
        bounded_io::retain(&pinned, Some(io)).await?;
        crate::executor::with_history_io(Some(io), |work| {
            reference.descriptor().verify(&bytes, work)
        })?;
        Ok(bytes)
    }
}

mod pinned_read;
use pinned_read::PinnedRead;

#[cfg(test)]
mod tests;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod pinned_read_tests;

use crate::executor::{stock_boundary, stock_string_copy};

/// Refusal of a conditional write whose precondition names a private
/// namespace cut. Conditional writes commit to the authored workspace.
pub(crate) const PRIVATE_CUT_PRECONDITION: &str = "a private cut is not a write precondition";

pub(crate) mod workspace_policy;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod workspace_publication;
#[cfg(not(target_arch = "wasm32"))]
pub use workspace_publication::{
    CheckoutRoot, WorkspacePublicationCoordinator, WorkspacePublicationPublisher,
    WorkspaceRootMaterializer, workspace_publication_coordinator,
};

#[cfg(not(target_arch = "wasm32"))]
mod agent_publication;
#[cfg(not(target_arch = "wasm32"))]
pub use agent_publication::{plan_agent_merge, record_agent_publication};

#[cfg(not(target_arch = "wasm32"))]
mod checkout_publication;
#[cfg(not(target_arch = "wasm32"))]
pub use checkout_publication::{AuthorizedCheckout, CheckoutApproval, CheckoutPublication};

pub use host::{DirectoryCredit, DirectoryOptions};
