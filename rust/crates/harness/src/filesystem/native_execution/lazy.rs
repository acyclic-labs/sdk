use super::preparation_record::invalid;
use super::*;
use crate::{NativeFutureBundle, runtime::NativeVolumePublicationGuard};
use acyclic_fs::native_mount::NativeMountPublicationFailure;
use acyclic_fs::{LazyMount, LazySnapshotRef, LazyWorkspaceStore, NativePublicationSnapshot};

/// Existing SDK mount and its original task selection.
pub struct NativeExistingLazyVolume {
    pub(super) mapping: NativeVolumeMapping,
    pub(super) publication: PublicationPermit,
    pub(super) backend: Box<dyn Backend>,
    pub(super) custody: NativeAllocation,
}

pub(super) trait Backend: Send + Sync {
    fn matches_owner(&self, owner: &dyn std::any::Any) -> bool;
    fn snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, LazySnapshotRef>>;
    fn publish<'a>(
        &'a self,
        volume: &'a NativeExistingLazyVolume,
    ) -> Result<
        NativeFutureBundle<
            'a,
            std::result::Result<NativePublicationSnapshot, NativeMountPublicationFailure<Error>>,
        >,
    >;
}

struct Mount<P: StreamProvider, A, O, D, S> {
    mount: Arc<LazyMount<A, O, D, S>>,
    owner: Arc<TaskJournalOwner<P>>,
    callback: Arc<dyn NativeVolumePublicationGuard>,
}

impl NativeExistingLazyVolume {
    /// Captures a mounted selection on the original metadata and volume ledgers.
    pub fn capture<'a, P, A, O, D, S>(
        mount: &'a Arc<LazyMount<A, O, D, S>>,
        owner: &'a Arc<TaskJournalOwner<P>>,
        binding: &'a BorrowedNativeVolumeBinding<'a, A, O>,
        guard: &'a crate::runtime::NativeVolumeCallbackGuard<P>,
        metadata: &'a NativeMaterializationWork,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, Self>>
    where
        P: StreamProvider + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: acyclic_fs::demand::DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        NativeFutureBundle::new(work, || async move {
            if !owner.native_shared_work_matches(metadata) || !work.shares_ledger(metadata) {
                return Err(invalid());
            }
            super::preparation::verify_binding(owner, binding, guard, work).await?;
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(invalid());
            };
            let workspace = binding
                .host
                .open_volume_with_native_work(binding.volume, work)
                .await?;
            let generation = binding
                .host
                .generation_with_native_work(&workspace, binding.generation, work)
                .await?;
            let actual = mount.snapshot(work, work.cancellation()).await?;
            if &actual != snapshot
                || actual.workspace_id != workspace.id()
                || actual.authored_generation != generation.id()
                || mount.native_root_identity().is_none()
            {
                return Err(invalid());
            }
            let io = PreparationIo::from_native_work(work)?;
            let callback = guard
                .for_volume(owner, binding.volume, binding.writable, work)?
                .1;
            let (path, custody) = work.path(mount.path(), None)?;
            io.absorb_native_custody(custody)?;
            let mapping = NativeVolumeMapping {
                volume: binding.volume.copy(&mut Some(&mut io.history_io()))?,
                generation: binding.generation.clone_with_io(&io)?,
                namespace: binding.namespace.clone(),
                path,
                writable: binding.writable,
                authority_revision: crate::contract::canonical_json_digest(
                    &(binding.verifier.audience(), binding.scope),
                    owner.input_limits().file_bytes,
                    &mut Some(&mut io.history_io()),
                )?,
            };
            let custody = work.claim(
                std::mem::size_of::<Mount<P, A, O, D, S>>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let backend = Box::new(Mount {
                mount: mount.clone(),
                owner: owner.clone(),
                callback,
            });
            backend.callback.check(work)?.await?;
            io.absorb_native_custody(custody)?;
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            let retained = io.owned_bytes()?;
            Ok(Self {
                mapping,
                publication: binding.publication,
                backend,
                custody: io.into_native_custody(retained)?,
            })
        })
    }
}

impl<P, A, O, D, S> Backend for Mount<P, A, O, D, S>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: acyclic_fs::demand::DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    fn matches_owner(&self, owner: &dyn std::any::Any) -> bool {
        owner
            .downcast_ref::<TaskJournalOwner<P>>()
            .is_some_and(|owner| std::ptr::eq(owner, self.owner.as_ref()))
    }

    fn snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, LazySnapshotRef>> {
        NativeFutureBundle::new(work, || async move {
            self.callback.check(work)?.await?;
            let snapshot = self.mount.snapshot(work, work.cancellation()).await?;
            self.callback.check(work)?.await?;
            Ok(snapshot)
        })
    }

    fn publish<'a>(
        &'a self,
        volume: &'a NativeExistingLazyVolume,
    ) -> Result<
        NativeFutureBundle<
            'a,
            std::result::Result<NativePublicationSnapshot, NativeMountPublicationFailure<Error>>,
        >,
    > {
        let work = volume.custody.work();
        NativeFutureBundle::new(work, || async move {
            self.callback.check(work)?.await?;
            if volume.mapping.writable {
                let callback = self.callback.clone();
                return Ok(self
                    .mount
                    .sync_with_permit(
                        volume.publication,
                        work,
                        move |actual: &NativeMaterializationWork| {
                            let actual = actual.clone();
                            let callback = callback.clone();
                            async move { callback.check(&actual)?.await }
                        },
                    )
                    .await);
            }
            let snapshot = match self
                .mount
                .publication_snapshot(work, work.cancellation())
                .await
            {
                Ok(snapshot) => snapshot,
                Err((cause, accounting_failure)) => {
                    return Ok(Err(NativeMountPublicationFailure {
                        cause: cause.into(),
                        accounting_failure,
                        source_acknowledged: false,
                        publication: None,
                    }));
                }
            };
            if let Err(cause) = async { self.callback.check(work)?.await }.await {
                return Ok(Err(NativeMountPublicationFailure {
                    cause,
                    accounting_failure: None,
                    source_acknowledged: false,
                    publication: Some(snapshot),
                }));
            }
            Ok(Ok(snapshot))
        })
    }
}
