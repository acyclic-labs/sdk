//! Native commands over the existing task and SDK workspace owners.
use super::{
    FilesystemHost, NativeViewManifest, NativeViewOptions, NativeVolumeMapping,
    NativeVolumeNamespace, PreparationIo,
};
use crate::{
    Error, Result,
    conversation::{ContentGrant, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    resources::GenerationRef,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, NativeAllocation, NativeMaterializationWork,
    NativeWorkingSet, PublicationPermit,
};
use acyclic_stream::StreamProvider;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
mod access;
#[cfg(feature = "filesystem-local")]
mod local_owner;
pub use access::{NativeAccessAuthority, authenticated_mount_options, owner_mount_options};
#[cfg(feature = "filesystem-local")]
pub use local_owner::{local_owner_mount_options, local_owner_workspace};
mod allocation;
mod completion;
pub(super) mod journal;
mod journal_allocation;
mod journal_encoding;
#[cfg(test)]
mod journal_properties;
#[cfg(test)]
mod journal_verification;
mod mounted;
mod preparation;
mod preparation_record;
#[cfg(fuzzing)]
pub use preparation_record::{fuzz_native_preparation_intent, fuzz_native_preparation_journal};
mod publication;
pub use allocation::NativePreparationRoot;
pub(crate) use completion::NativePreparationCompletionTemplate;
pub use mounted::NativeMountedVolume;
pub use publication::NativeViewPublicationError;

/// Selection of one exact generation and publication permit.
pub struct BorrowedNativeVolumeBinding<'a, A, O> {
    /// Filesystem provider.
    pub host: &'a FilesystemHost<A, O>,
    /// Authorized volume.
    pub volume: &'a VolumeRef,
    /// Immutable generation.
    pub generation: &'a GenerationRef,
    /// Declared namespace.
    pub namespace: &'a NativeVolumeNamespace,
    /// Existing owner when this binding selects a demand-backed mount.
    pub mounted: Option<&'a Arc<dyn NativeMountedVolume>>,
    /// Relative directory below the namespace root.
    pub path: &'a str,
    /// Whether edits may publish.
    pub writable: bool,
    /// Authority verifier.
    pub verifier: &'a AuthorityVerifier,
    /// Original read and destination write authority.
    pub scope: &'a Scope,
    /// SDK publication permit.
    pub publication: PublicationPermit,
}

struct PreparedVolume<A, O> {
    view: VolumeView<A, O>,
    publication: PublicationPermit,
    work: NativeMaterializationWork,
    callback: Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>,
}

enum VolumeView<A, O> {
    Directory(Box<NativeWorkingSet<A, O>>),
    Mounted {
        owner: Arc<dyn NativeMountedVolume>,
        _work_scope: acyclic_fs::native_mount::NativeMountWorkScope,
    },
}

/// SDK namespace and its protected original preparation.
pub struct NativeVolumeView<A, O> {
    volumes: Vec<PreparedVolume<A, O>>,
    preparation: preparation_record::Intent,
    custody: NativeAllocation,
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Original task work ledger.
    pub fn work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }

    /// Exact request and approval manifest.
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }

    /// Returns the original publication permit for an authorized writable volume.
    pub fn publication_permit(&self, volume: &VolumeRef) -> Option<PublicationPermit> {
        self.manifest()
            .volumes
            .iter()
            .zip(&self.volumes)
            .find(|(mapping, _)| &mapping.volume == volume && mapping.writable)
            .map(|(_, prepared)| prepared.publication)
    }

    async fn require_volume<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
        volume: &VolumeRef,
        operation: VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<&PreparedVolume<A, O>> {
        let io = PreparationIo::from_native_work(work)?;
        if !self.work().shares_ledger(work)
            || !owner.native_shared_work_matches(work)
            || owner.task_binding() != (self.preparation.task, &self.preparation.original_fence)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "working volume changed its original owner",
            });
        }
        owner.require_volume_grant(volume, operation, &mut Some(&mut &io))?;
        for (mapping, prepared) in self.manifest().volumes.iter().zip(&self.volumes) {
            if &mapping.volume == volume && (operation == VolumeOperation::Read || mapping.writable)
            {
                prepared.callback.check(work)?.await?;
                return Ok(prepared);
            }
        }
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "volume is outside its original working view",
        })
    }

    /// Runs bounded physical I/O through the selected volume's original owner.
    pub async fn file_io<P: StreamProvider, T>(
        &self,
        owner: &TaskJournalOwner<P>,
        volume: &VolumeRef,
        operation: VolumeOperation,
        path: &Path,
        write: bool,
        parents: bool,
        work: &NativeMaterializationWork,
        io: impl FnOnce(
            &mut std::fs::File,
            &NativeMaterializationWork,
        ) -> std::result::Result<T, acyclic_fs::NativeTreeMaterializationError>
        + Send,
    ) -> Result<T> {
        if write && operation != VolumeOperation::Write {
            return Err(preparation_record::invalid());
        }
        let prepared = self.require_volume(owner, volume, operation, work).await?;
        let volume_work = &prepared.work;
        let result = match &prepared.view {
            VolumeView::Directory(view) => view
                .file_io(path, write, parents, volume_work, io)
                .await
                .map_err(Error::from),
            VolumeView::Mounted { owner: mount, .. } => {
                let work = volume_work.with_source_io_lease(mount.io_lease()?)?;
                let result = acyclic_fs::native_mount::native_file_io(
                    mount.path(),
                    mount.identity()?,
                    path,
                    write,
                    parents,
                    &work,
                    io,
                )
                .map_err(Error::from);
                if write {
                    mount.capture_host_path(path, volume_work).and(result)
                } else {
                    result
                }
            }
        };
        self.require_volume(owner, volume, operation, work).await?;
        result
    }

    /// Rechecks the physical views and immutable source selections.
    pub async fn validate_for_dispatch(&self) -> Result<()> {
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest().volumes) {
            volume.callback.check(&volume.work)?.await?;
            match &volume.view {
                VolumeView::Directory(view) => {
                    view.validate_for_presentation(&volume.work).await?;
                    view.verify_unchanged(&volume.work).await?;
                }
                VolumeView::Mounted { owner, .. } => {
                    mounted::verify_snapshot(owner.as_ref(), &mapping.namespace, &volume.work)
                        .await?;
                }
            }
            volume.callback.check(&volume.work)?.await?;
        }
        Ok(())
    }

    /// Publishes authorized destinations, retaining SDK acknowledgements through failure.
    pub async fn publish<P: StreamProvider>(
        &self,
        owner: &TaskJournalOwner<P>,
    ) -> Result<(
        Vec<(usize, acyclic_fs::NativePublicationSnapshot)>,
        NativeAllocation,
    )> {
        use acyclic_fs::native_mount::NativeMountPublicationFailure;
        use publication::failure;
        let work = self.work();
        if !owner.native_shared_work_matches(work)
            || owner.task_binding() != (self.preparation.task, &self.preparation.original_fence)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native publication requires its original owner",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut published = crate::model::vec_with_capacity(self.volumes.len(), Some(&io))?;
        let failure_slot = work.claim(
            (std::mem::size_of::<NativeViewPublicationError>()
                + std::mem::size_of::<acyclic_fs::NativePublicationSnapshot>()) as u64,
            2,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let result = async {
            // All immutable sources pass before any destination publishes.
            for writable in [false, true] {
                for (index, (volume, mapping)) in self
                    .volumes
                    .iter()
                    .zip(&self.manifest().volumes)
                    .enumerate()
                    .filter(|(_, (_, mapping))| mapping.writable == writable)
                {
                    let snapshot = if writable {
                        owner
                            .verify(false, &mut Some(&mut io.history_io()))
                            .await
                            .map_err(failure)?;
                        let callback = Arc::clone(&volume.callback);
                        match &volume.view {
                            VolumeView::Mounted { owner, .. } => {
                                mounted::require_root(owner.as_ref(), &volume.work)
                                    .map_err(failure)?;
                                let snapshot = owner
                                    .publish(volume.publication, callback, &volume.work)
                                    .map_err(failure)?
                                    .await
                                    .map_err(failure)??;
                                mounted::require_root(owner.as_ref(), &volume.work).map_err(
                                    |cause| NativeMountPublicationFailure {
                                        cause,
                                        accounting_failure: None,
                                        source_acknowledged: true,
                                        publication: Some(Box::new(snapshot)),
                                    },
                                )?;
                                snapshot
                            }
                            VolumeView::Directory(view) => {
                                view.sync_with_permit(
                                    volume.publication,
                                    &volume.work,
                                    move |actual: &NativeMaterializationWork| {
                                        let actual = actual.clone();
                                        let callback = Arc::clone(&callback);
                                        async move {
                                            callback.check(&actual)?.await.map_err(Error::from)
                                        }
                                    },
                                )
                                .await?
                            }
                        }
                    } else {
                        match &volume.view {
                            VolumeView::Directory(view) => view
                                .verify_unchanged(&volume.work)
                                .await
                                .map_err(|cause| failure(cause.into()))?,
                            VolumeView::Mounted { owner, .. } => {
                                mounted::require_root(owner.as_ref(), &volume.work)
                                    .map_err(failure)?;
                                let snapshot = owner
                                    .snapshot(&volume.work)
                                    .map_err(failure)?
                                    .await
                                    .map_err(failure)??;
                                mounted::require_snapshot(
                                    &snapshot,
                                    &mapping.namespace,
                                    &volume.work,
                                )
                                .map_err(failure)?;
                                mounted::require_root(owner.as_ref(), &volume.work)
                                    .map_err(failure)?;
                                snapshot
                            }
                        }
                    };
                    published.push((index, snapshot));
                }
            }
            Ok::<_, NativeMountPublicationFailure<Error>>(())
        }
        .await;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        match result {
            Ok(()) => Ok((published, custody)),
            Err(cause) => Err(Error::NativePublication {
                failure: Box::new(NativeViewPublicationError {
                    cause,
                    published,
                    custody,
                }),
                custody: Some(failure_slot),
            }),
        }
    }
}
