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
mod preparation_record;
pub use preparation_record::NativeRecordedPreparation;
mod inspection;
pub(super) mod journal;
pub use inspection::observe_preparation;
mod publication;
pub use publication::NativeViewPublicationError;
mod root_output;
pub use root_output::NativeRootOutputSource;
mod working_files;
pub use working_files::NativeTaskWorkingView;

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
        callback: Arc<dyn crate::runtime::NativeVolumePublicationGuard>,
    },
    Lazy(Arc<NativeExistingLazyVolume>),
}

/// SDK namespace and its protected original preparation.
pub struct NativeVolumeView<A, O> {
    volumes: Vec<PreparedVolume<A, O>>,
    preparation: NativeRecordedPreparation,
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Original task work ledger.
    pub fn work(&self) -> &NativeMaterializationWork {
        self.preparation.custody.work()
    }

    /// Exact request/approval manifest.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.preparation.manifest
    }

    /// Original protected journal and selection.
    pub fn preparation(&self) -> &NativeRecordedPreparation {
        &self.preparation
    }

    /// Rechecks the original physical views and immutable source selections.
    pub async fn validate_for_dispatch(&self) -> Result<()> {
        for (volume, mapping) in self.volumes.iter().zip(&self.manifest().volumes) {
            match volume {
                PreparedVolume::Materialized { view, work, .. } => {
                    view.validate_for_presentation_with_work(work, work.cancellation())
                        .await?;
                    view.verify_unchanged_with_work(work, work.cancellation())
                        .await?;
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
        Vec<super::native_process::NativeProcessPublishedVolume>,
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
        let mut published = super::fork_preparer_io::vector(self.volumes.len(), Some(&io))?;
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
                        .verify_unchanged_with_work(work, work.cancellation())
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
                        view.sync_with_permit_with_work(
                            *publication,
                            work,
                            work.cancellation(),
                            move |actual: &NativeMaterializationWork| {
                                let actual = actual.clone();
                                let callback = Arc::clone(&callback);
                                async move { callback.check(&actual)?.await }
                            },
                        )
                        .await?
                    }
                    PreparedVolume::Lazy(lazy) => {
                        let snapshot = if mapping.writable {
                            lazy.backend.publish(lazy.publication, lazy.custody.work())
                        } else {
                            lazy.backend.qualify(lazy.custody.work())
                        };
                        snapshot.map_err(failure)?.await.map_err(failure)??
                    }
                };
                published
                    .push(super::native_process::NativeProcessPublishedVolume { index, snapshot });
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
