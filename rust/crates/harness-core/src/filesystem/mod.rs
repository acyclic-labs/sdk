#![cfg_attr(
    test,
    allow(
        clippy::too_many_lines,
        clippy::indexing_slicing,
        reason = "adapter tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../../docs/filesystem.md")]
use crate::{
    Error, IdempotencyKey, Result,
    conversation::{
        Attachment, ContentGrant, ContentResidencyVerifier, FileDescriptor, FileRef,
        MAX_PRIVATE_DIRECTORY_PAGE, PrivateDirectoryEntry, PrivateDirectoryEntryKind,
        PrivateDirectoryPage, ReferencedAttachments, VolumeClass, VolumeOperation, VolumeRef,
    },
    core::{AuthorityVerifier, Scope},
    resources::{GenerationRef, ProviderRef, WorkspaceRef},
};
use acyclic_fs::WorkCounters;
use acyclic_fs::kernel::{FileKind, LogicalName, NameEncoding};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Digest, DirectoryPage, Fs, Generation, GenerationId,
    IdempotencyKey as FilesystemKey, JoinPlan, TransactionCommit, Workspace, WorkspaceError,
    WorkspaceStat,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};
mod content_input;
use content_input::ContentInput;
mod content_publisher;
pub use content_publisher::FilesystemContentPublisher;
mod execution_journal;
pub use execution_journal::FilesystemExecutionJournal;
mod agent_effects;
mod agent_goal;
pub use agent_goal::GOAL_VOLUME;
mod agent_internal;
mod spawn_volumes;
pub use spawn_volumes::{FilesystemSpawnVolumes, spawned_volume};
mod agent_turns;
pub use agent_turns::{
    AgentAuthority, FilesystemTurnHost, PRIVATE_VOLUME, TurnAdmission, TurnAuthority,
    TurnExecutors, TurnLimits, agent_authority,
};
pub(crate) mod bounded_io;
#[doc(hidden)]
pub use bounded_io::PreparationRecord;
pub(crate) use bounded_io::PreparationScope;
#[doc(hidden)]
pub use bounded_io::UploadRecord;
use bounded_io::decode_record;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use bounded_io::native_decoded;
pub(crate) use bounded_io::{DecodeReservation, retain_decoded};
pub use bounded_io::{PreparationCustody, PreparationIo};
mod interaction_host;
pub use interaction_host::FilesystemInteractionHost;
mod memory;
pub use memory::MemoryHarnessStorage;
mod text_tools;
pub use text_tools::ExactTextReplacement;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
mod native_access;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod native_process;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_process::NativeProcessObserver;
mod native_capture_bounds;
mod native_contract;
#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
mod native_directory;
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
pub use native_access::{
    NativeMountAuthority, WorkspaceAccess, require_unrestricted_volume_access,
    require_unrestricted_workspace_read,
};
#[cfg(all(
    feature = "native-execution",
    feature = "filesystem-local",
    not(target_arch = "wasm32")
))]
pub use native_access::{
    local_owner_mount_options, local_owner_workspace, local_owner_workspace_access,
};
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
mod config;
use config::*;
pub use config::{FilesystemJournalBinding, WorkspaceMutation, WorkspaceObservation};
mod host;
pub(crate) use host::map_error;
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
    generation
        .retain_if_unpublished(
            format!(
                "harness-generation-{}",
                hex::encode(generation.id().digest().into_bytes())
            ),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .await
        .map(|_| ())
        .map_err(|failure| map_error(failure.error))
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
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
        maximum_bytes: u64,
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
        // Refuse invalid or unauthorized input before admitting an allocated future.
        let bytes = input.as_ref();
        grant.require(volume, VolumeOperation::Write, io)?;
        if let Some(grants) = ceiling
            && !crate::executor::with_history_io(io, |work| {
                crate::core::volume_write_granted_with_work(grants, volume, work)
            })?
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "content caller lacks a filesystem write grant",
            ));
        }
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
        match internal_class {
            Some(class)
                if volume.class() == VolumeClass::AgentPrivate
                    && path.starts_with(class.prefix()) => {}
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
            None if path == ".system"
                || path.starts_with(".system/")
                || path == "turns/.canonical"
                || path.starts_with("turns/.canonical/") =>
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "internal storage paths are reserved",
                ));
            }
            None => {}
        }
        // Construction returns before polling this concrete publication graph.
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let descriptor = crate::executor::with_history_io(io, |work| {
                FileDescriptor::from_bytes(bytes, media_type, work)
            })?;
            let operation = bounded_io::operation_key(idempotency_key, io)?;
            let provisional = crate::executor::with_history_io(io, |work| {
                FileRef::new(
                    volume.copy(work)?,
                    stock_string_copy(path, io)?,
                    bounded_io::hex_key(&operation.into_bytes(), io)?,
                    FileDescriptor::new(
                        *descriptor.sha256(),
                        descriptor.byte_length(),
                        stock_string_copy(descriptor.media_type(), io)?,
                    )?,
                    stock_string_copy(display_name, io)?,
                    work,
                )
            })?;
            let workspace = bounded_io::content_workspace(&self.provider, volume, io)?;
            let receipt_identity = if internal_class.is_some() {
                provisional.path()
            } else {
                idempotency_key.as_str()
            };
            let receipt_path =
                content_receipt_path_io(receipt_identity, internal_class.is_some(), false, io)?;
            let metadata_path = content_metadata_path_io(provisional.path(), false, io)?;
            let receipt_bytes = match io {
                Some(io) => io.encode(
                    &(&provisional, expected_generation),
                    io.remaining()?.bytes_encoded,
                    true,
                )?,
                None => acyclic_stream::OwnedBytes::from(crate::contract::canonical_json_bytes(
                    &(&provisional, expected_generation),
                )?),
            };
            let (opened, prior) = crate::conversation::BoundedContentFuture::new(io, || async {
                let owned;
                let io = if let Some(io) = io {
                    io
                } else {
                    owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &owned
                };
                let opened = self.open(&workspace, Some(io)).await?;
                let prior = io.sdk(
                    opened
                        .operation_generation(operation, io, io.cancellation())
                        .await,
                )?;
                Ok::<_, Error>((opened, prior))
            })?
            .await?;
            let (authorized_generation, policy_fence) = self
                .authorize_content_write(
                    &opened,
                    grant,
                    provisional.path(),
                    prior.is_some(),
                    ceiling,
                    io,
                )?
                .await?;
            let generation_hold = io
                .map(|io| io.guard_owned(authorized_generation.logical_owned_bytes()?))
                .transpose()?;
            let authorized_generation = (authorized_generation, generation_hold);
            if let Some(prior) = prior {
                let generation = (self).generation_ref_id(prior.id(), io)?;
                bounded_io::retain(&prior, io).await?;
                let version = bounded_io::hex_key(generation.as_resource().key(), io)?;
                let receipt_path = crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
                })?;
                const RECORD_BYTES: u64 = 6
                    * (crate::conversation::MAX_PATH_BYTES as u64
                        + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                        + 1024);
                let original_receipt =
                    read_generation(&prior, &receipt_path, RECORD_BYTES, io).await?;
                let original_receipt = if internal_class.is_some() {
                    recover_internal_upload_receipt(&prior, receipt_identity, original_receipt, io)
                        .await?
                } else {
                    original_receipt
                };
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
                    read_generation(&prior, &indexed_path, receipt_bytes.len() as u64, io).await?;
                if !content_metadata_matches(&indexed, &provisional, &receipt_bytes, io)? {
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
                    .read_retained_file(&prior, &reference, maximum_bytes, io)?
                    .await?;
                if committed.as_ref() != bytes {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "upload identity belongs to different content",
                    ));
                }
                grant.recheck(io)?;
                return self.seal_publication(reference, io);
            }
            if expected_generation.is_some_and(|expected| expected != &authorized_generation.0) {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "workspace generation changed before content authorization",
                ));
            }
            let policy_fence = if expected_generation.is_some() {
                policy_fence.pin_generation()
            } else {
                policy_fence
            };
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
                path: stock_string_copy("/.system/harness-file-metadata", io)?,
            });
            if internal_class.is_none() {
                mutations.push(WorkspaceMutation::CreateDirectory {
                    path: stock_string_copy("/.system/harness-uploads", io)?,
                });
                mutations.push(WorkspaceMutation::PutFile {
                    path: crate::executor::with_history_io(io, |work| {
                        crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
                    })?,
                    bytes: receipt_bytes.mutation_bytes(io)?,
                });
            }
            mutations.push(WorkspaceMutation::PutFile {
                path: crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(format_args!("/{receipt_path}"), work)
                })?,
                bytes: receipt_bytes,
            });
            let generation = self
                .apply_authorized_generation(
                    &workspace,
                    (policy_fence, grant),
                    &mutations,
                    IdempotencyKey(stock_string_copy(idempotency_key.as_str(), io)?),
                    io,
                )?
                .await?;
            let generation_reference = (self).generation_ref_id(generation.id(), io)?;
            let version = bounded_io::hex_key(generation_reference.as_resource().key(), io)?;
            {
                match io {
                    Some(io) => bounded_io::retain(&generation, Some(io)).await?,
                    None => retain_published_generation(&generation).await?,
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
            self.read_retained_file(&generation, &reference, maximum_bytes, io)?
                .await?;
            // Carry the verified immutable publication through runtime admission.
            grant.recheck(io)?;
            self.seal_publication(reference, io)
        })
    }
    #[inline(never)]
    fn read_retained_file<'a>(
        &'a self,
        generation: &'a Generation<A, O>,
        reference: &'a FileRef,
        maximum_bytes: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
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
            #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
            acyclic_differential::record(("read", maximum_bytes));
            let bytes = read_generation(generation, &path, maximum_bytes, io).await?;
            crate::executor::with_history_io(io, |work| {
                reference.descriptor().verify(&bytes, work)
            })?;
            #[cfg(all(test, not(target_arch = "wasm32")))]
            pinned_read_tests::retained::sample_items(true, io);
            Ok(bytes)
        })
    }
    async fn read_pinned_io(
        &self,
        reference: &FileRef,
        maximum_bytes: u64,
        io: Option<&bounded_io::PreparationIo>,
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
                bounded_io::content_workspace(&self.provider, reference.volume(), None)?;
            let path = format!("/{}", reference.path());
            // Keep the authenticated immutable handle from the read. A
            // retaining caller can pin it without opening the workspace
            // and authenticating this same generation a second time.
            #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
            acyclic_differential::record(("read", maximum_bytes));
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
        io: Option<&'a bounded_io::PreparationIo>,
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
                let workspace =
                    bounded_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
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
                #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
                acyclic_differential::record(("read", maximum_bytes));
                let bytes = pinned
                    .read(&path, maximum_bytes, original, io.cancellation())
                    .await
                    .map_err(|source| Error::NativeWorkspace {
                        source: Box::new(source),
                        custody: None,
                    })?;
                bounded_io::retain(&pinned, Some(io)).await?;
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
            let workspace = bounded_io::content_workspace(&self.provider, reference.volume(), io)?;
            let _workspace_hold = io
                .map(|io| io.guard_owned(workspace.logical_owned_bytes()?))
                .transpose()?;
            let path = format!("/{}", reference.path());
            let bytes = match io {
                Some(io) => {
                    let opened = (self).open(&workspace, Some(io)).await?;
                    let pinned = self.generation(&opened, &generation, Some(io)).await?;
                    #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
                    acyclic_differential::record(("read", maximum_bytes));
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
                    bounded_io::retain(&pinned, Some(io)).await?;
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

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod pinned_read_tests;

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
pub use checkout_publication::{AuthorizedCheckout, CheckoutApproval, CheckoutPublication};

pub use host::DirectoryOptions;
