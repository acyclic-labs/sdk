//! Optional native command composition over existing task, effect and workspace owners.

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

mod lazy;
pub use lazy::NativeExistingLazyVolume;
mod allocation;
mod preparation;
pub use allocation::NativePreparationRoot;
mod completion;
pub(crate) use completion::NativePreparationCompletionTemplate;
pub(super) mod journal;
mod preparation_record;
mod publication;
pub use publication::NativeViewPublicationError;

/// Borrowed selection of one exact generation and publication permit.
pub struct BorrowedNativeVolumeBinding<'a, A, O> {
    /// Filesystem provider.
    pub host: &'a FilesystemHost<A, O>,
    /// Authorized volume.
    pub volume: &'a VolumeRef,
    /// Immutable generation.
    pub generation: &'a GenerationRef,
    /// Declared namespace.
    pub namespace: &'a NativeVolumeNamespace,
    /// Relative path below the namespace root.
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

enum PreparedVolume<A, O> {
    Materialized {
        view: NativeWorkingSet<A, O>,
        publication: PublicationPermit,
        work: NativeMaterializationWork,
        callback: Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>,
    },
    Lazy(Arc<NativeExistingLazyVolume>),
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

    /// Exact request/approval manifest.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }

    /// Rechecks the original physical views and immutable source selections.
    pub async fn validate_for_dispatch(&self) -> Result<()> {
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest().volumes) {
            match volume {
                PreparedVolume::Materialized { view, work, .. } => {
                    view.validate_for_presentation(work).await?;
                    view.verify_unchanged(work).await?;
                }
                PreparedVolume::Lazy(lazy) => {
                    let actual = lazy.backend.snapshot(lazy.custody.work())?.await?;
                    if mapping.namespace != (NativeVolumeNamespace::LazyMount { snapshot: actual })
                    {
                        return Err(
                            acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into()
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Publishes authorized destinations, retaining every SDK result through failure.
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
            std::mem::size_of::<NativeViewPublicationError>() as u64,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let result = async {
            // Validate all immutable sources before any destination publishes.
            let volumes = self
                .volumes
                .iter()
                .zip(&self.manifest().volumes)
                .enumerate();
            for (index, (volume, mapping)) in volumes
                .clone()
                .filter(|(_, (_, mapping))| !mapping.writable)
                .chain(volumes.filter(|(_, (_, mapping))| mapping.writable))
            {
                if mapping.writable {
                    owner
                        .verify(false, &mut Some(&mut io.history_io()))
                        .await
                        .map_err(failure)?;
                }
                let snapshot = match volume {
                    PreparedVolume::Materialized { view, work, .. } if !mapping.writable => view
                        .verify_unchanged(work)
                        .await
                        .map_err(|cause| failure(cause.into()))?,
                    PreparedVolume::Materialized {
                        view,
                        work,
                        callback,
                        publication,
                        ..
                    } => {
                        let callback = Arc::clone(callback);
                        view.sync_with_permit(
                            *publication,
                            work,
                            move |actual: &NativeMaterializationWork| {
                                let actual = actual.clone();
                                let callback = Arc::clone(&callback);
                                async move {
                                    callback
                                        .check(&actual)
                                        .map_err(Error::from)?
                                        .await
                                        .map_err(Error::from)
                                }
                            },
                        )
                        .await?
                    }
                    PreparedVolume::Lazy(lazy) => lazy
                        .backend
                        .publish(lazy)
                        .map_err(failure)?
                        .await
                        .map_err(failure)??,
                };
                published.push((index, snapshot));
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
                    diagnostic: None,
                    custody: Some(custody),
                }),
                custody: Some(failure_slot),
            }),
        }
    }
}
