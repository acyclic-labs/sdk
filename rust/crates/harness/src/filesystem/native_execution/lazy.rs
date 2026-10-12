use super::preparation_record::invalid;
use super::*;
use acyclic_fs::native_mount::NativeMountPublicationFailure;
use acyclic_fs::{LazyMount, LazyWorkspaceStore, NativePublicationSnapshot};

/// Existing SDK mount and the selection captured under its original task.
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
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>>;
    fn publish<'a>(
        &'a self,
        permit: PublicationPermit,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<NativePublicationSnapshot, NativeMountPublicationFailure<Error>>,
        >,
    >;
    fn qualify<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<NativePublicationSnapshot, NativeMountPublicationFailure<Error>>,
        >,
    >;
}

struct Mount<P: StreamProvider, A, O, D, S> {
    mount: Arc<LazyMount<A, O, D, S>>,
    owner: Arc<TaskJournalOwner<P>>,
    callback: Arc<dyn crate::runtime::NativeVolumePublicationGuard>,
}

impl NativeExistingLazyVolume {
    /// Captures a real mounted selection on the original metadata and volume ledgers.
    pub fn capture<'a, P, A, O, D, S>(
        mount: &'a Arc<LazyMount<A, O, D, S>>,
        owner: &'a Arc<TaskJournalOwner<P>>,
        binding: &'a BorrowedNativeVolumeBinding<'a, A, O>,
        guard: &'a crate::runtime::NativeVolumeCallbackGuard<P>,
        metadata: &'a NativeMaterializationWork,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>>
    where
        P: StreamProvider + 'static,
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
        D: acyclic_fs::demand::DemandSource + 'static,
        S: LazyWorkspaceStore,
    {
        crate::NativeFutureBundle::new(work, || async move {
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
            let actual = mount.snapshot_with_work(work, work.cancellation()).await?;
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
            let (path, path_custody) = work.path(mount.path(), None)?;
            io.absorb_native_custody(path_custody)?;
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
            let backend_custody = work.claim(
                std::mem::size_of::<Mount<P, A, O, D, S>>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let backend = Box::new(Mount {
                mount: Arc::clone(mount),
                owner: Arc::clone(owner),
                callback,
            });
            backend.callback.check(work)?.await?;
            io.absorb_native_custody(backend_custody)?;
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
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>> {
        crate::NativeFutureBundle::new(work, || async move {
            self.callback.check(work)?.await?;
            let snapshot = self
                .mount
                .snapshot_with_work(work, work.cancellation())
                .await?;
            self.callback.check(work)?.await?;
            Ok(snapshot)
        })
    }
    fn publish<'a>(
        &'a self,
        permit: PublicationPermit,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<NativePublicationSnapshot, NativeMountPublicationFailure<Error>>,
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            self.callback.check(work)?.await?;
            let callback = Arc::clone(&self.callback);
            Ok(self
                .mount
                .sync_with_permit_work_guarded(
                    permit,
                    work,
                    work.cancellation(),
                    move |actual: &NativeMaterializationWork| {
                        let actual = actual.clone();
                        let callback = Arc::clone(&callback);
                        async move { callback.check(&actual)?.await }
                    },
                )
                .await)
        })
    }
    fn qualify<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<NativePublicationSnapshot, NativeMountPublicationFailure<Error>>,
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            self.callback.check(work)?.await?;
            let snapshot = match self
                .mount
                .publication_snapshot_with_work(work, work.cancellation())
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
