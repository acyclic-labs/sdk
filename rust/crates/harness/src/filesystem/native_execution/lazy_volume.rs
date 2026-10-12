use super::*;

/// A sealed reference to an existing native lazy mount, not permission to mount or append a namespace to an already prepared operation.
pub struct NativeExistingLazyVolume {
    pub(super) held: Arc<dyn NativeHeldLazyVolume>,
}

pub(super) trait NativeHeldLazyVolume: Send + Sync {
    fn matches_owner(&self, owner: &dyn std::any::Any) -> bool;
    fn matches_binding(
        &self,
        volume: &VolumeRef,
        generation: &GenerationRef,
        writable: bool,
        publication: PublicationPermit,
        authority_revision: &[u8; 32],
    ) -> bool;
    fn path(&self) -> &Path;
    fn identity(&self) -> acyclic_fs::NativeRootIdentity;
    fn original_work(&self) -> &NativeMaterializationWork;
    fn snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>>;
    fn publication_snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                acyclic_fs::native_mount::NativeMountPublicationFailure<Error>,
            >,
        >,
    >;
    fn begin_scope(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::native_mount::NativeMountWorkScope>;
    fn read<'a>(
        &'a self,
        relative: &'a Path,
        maximum: usize,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    fn write<'a>(
        &'a self,
        relative: &'a Path,
        content: &'a str,
        parents: bool,
        expected: Option<[u8; 32]>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    fn sync<'a>(
        &'a self,
        permit: PublicationPermit,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                acyclic_fs::native_mount::NativeMountPublicationFailure<Error>,
            >,
        >,
    >;
    fn retire(
        &self,
        wait: &acyclic_fs::CancellationToken,
    ) -> Result<acyclic_fs::native_mount::NativeMountDrainProof>;
}

pub(super) struct HeldLazyVolume<P: StreamProvider, A, O, D, S> {
    pub(super) mount: Arc<acyclic_fs::LazyMount<A, O, D, S>>,
    pub(super) owner: Arc<TaskJournalOwner<P>>,
    pub(super) callback: Arc<dyn crate::runtime::NativeVolumePublicationGuard>,
    pub(super) identity: acyclic_fs::NativeRootIdentity,
    pub(super) volume: VolumeRef,
    pub(super) generation: GenerationRef,
    pub(super) writable: bool,
    pub(super) publication: PublicationPermit,
    pub(super) authority_revision: [u8; 32],
    pub(super) custody: acyclic_fs::NativeAllocation,
}

impl NativeExistingLazyVolume {
    /// Captures only the actual existing mount and its immutable declared snapshot.
    pub fn capture_with_native_work<'a, P, A, O, D, S>(
        mount: &'a Arc<acyclic_fs::LazyMount<A, O, D, S>>,
        owner: &'a Arc<TaskJournalOwner<P>>,
        binding: &'a BorrowedNativeVolumeBinding<'a, A, O>,
        guard: &'a crate::runtime::NativeVolumeCallbackGuard<P>,
        metadata_work: &'a NativeMaterializationWork,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Self>>
    where
        P: StreamProvider,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
        D: acyclic_fs::demand::DemandSource + 'static,
        S: acyclic_fs::LazyWorkspaceStore,
    {
        crate::NativeFutureBundle::new(work, || async move {
            if !owner.native_shared_work_matches(metadata_work)
                || !work.shares_ledger(metadata_work)
            {
                return Err(Error::from(
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                ));
            }
            owner
                .verify(
                    false,
                    &mut Some(&mut crate::store::history::native_work_io(metadata_work)?),
                )
                .await?;
            verify_binding(owner, binding, work)?;
            guard.require_volume_with_native_work(owner, binding.volume, binding.writable, work)?;
            let NativeVolumeNamespace::LazyMount { snapshot } = binding.namespace else {
                return Err(Error::from(
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                ));
            };
            let identity = mount.native_root_identity().ok_or_else(|| {
                Error::from(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)
            })?;
            let workspace = binding
                .host
                .open_volume_with_native_work(binding.volume, work)
                .await?;
            let generation = binding
                .host
                .generation_with_native_work(&workspace, binding.generation, work)
                .await?;
            let actual = mount.snapshot(&work, work.cancellation()).await?;
            if &actual != snapshot
                || snapshot.workspace_id != workspace.id()
                || snapshot.authored_generation != generation.id()
            {
                return Err(Error::from(
                    acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                ));
            }
            let callback = guard
                .for_volume(owner, binding.volume, binding.writable, work)?
                .1;
            let mut holder_custody = work.claim(
                (std::mem::size_of::<Self>() + std::mem::size_of::<HeldLazyVolume<P, A, O, D, S>>())
                    as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let (volume, mut volume_custody) = allocate(work, |io| {
                binding.volume.copy(&mut Some(&mut io.history_io()))
            })?;
            holder_custody.absorb(&mut volume_custody)?;
            let (generation, mut generation_custody) =
                allocate(work, |io| binding.generation.clone_with_io(io))?;
            holder_custody.absorb(&mut generation_custody)?;
            let (canonical, _canonical_custody) =
                encode_record(&(binding.verifier.audience(), binding.scope), work)?;
            work.charge(acyclic_fs::WorkCounters {
                bytes_hashed: canonical.len() as u64,
                ..Default::default()
            })?;
            let authority_revision = *blake3::hash(&canonical).as_bytes();
            work.charge(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..Default::default()
            })?;
            let held = Arc::new(HeldLazyVolume {
                mount: Arc::clone(mount),
                owner: Arc::clone(owner),
                callback,
                identity,
                volume,
                generation,
                writable: binding.writable,
                publication: binding.publication,
                authority_revision,
                custody: holder_custody,
            });
            owner
                .verify(
                    false,
                    &mut Some(&mut crate::store::history::native_work_io(metadata_work)?),
                )
                .await?;
            Ok(Self { held })
        })
    }
}

impl<P, A, O, D, S> HeldLazyVolume<P, A, O, D, S>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: acyclic_fs::demand::DemandSource + 'static,
    S: acyclic_fs::LazyWorkspaceStore,
{
    pub(super) fn require_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.custody.work().shares_allocation_context(work) {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        work.admit(Default::default()).map_err(Error::from)
    }
    pub(super) async fn check(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_work(work)?;
        self.callback.check(work)?.await
    }
}

impl<P, A, O, D, S> NativeHeldLazyVolume for HeldLazyVolume<P, A, O, D, S>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: acyclic_fs::demand::DemandSource + 'static,
    S: acyclic_fs::LazyWorkspaceStore,
{
    fn matches_owner(&self, owner: &dyn std::any::Any) -> bool {
        owner
            .downcast_ref::<TaskJournalOwner<P>>()
            .is_some_and(|owner| std::ptr::eq(owner, self.owner.as_ref()))
    }
    fn matches_binding(
        &self,
        volume: &VolumeRef,
        generation: &GenerationRef,
        writable: bool,
        publication: PublicationPermit,
        authority_revision: &[u8; 32],
    ) -> bool {
        &self.volume == volume
            && &self.generation == generation
            && self.writable == writable
            && self.publication == publication
            && &self.authority_revision == authority_revision
    }
    fn path(&self) -> &Path {
        self.mount.path()
    }
    fn identity(&self) -> acyclic_fs::NativeRootIdentity {
        self.identity
    }
    fn original_work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }
    fn snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_fs::LazySnapshotRef>> {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let snapshot = self.mount.snapshot(&work, work.cancellation()).await?;
            self.check(work).await?;
            Ok(snapshot)
        })
    }
    fn publication_snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                acyclic_fs::native_mount::NativeMountPublicationFailure<Error>,
            >,
        >,
    > {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let snapshot = match self
                .mount
                .publication_snapshot(&work, work.cancellation())
                .await
            {
                Ok(snapshot) => snapshot,
                Err((cause, accounting)) => {
                    return Ok(Err(
                        acyclic_fs::native_mount::NativeMountPublicationFailure {
                            cause: Error::from(cause),
                            accounting_failure: accounting,
                            source_acknowledged: false,
                            publication: None,
                        },
                    ));
                }
            };
            if let Err(cause) = self.check(work).await {
                return Ok(Err(
                    acyclic_fs::native_mount::NativeMountPublicationFailure {
                        cause,
                        accounting_failure: None,
                        source_acknowledged: false,
                        publication: Some(snapshot),
                    },
                ));
            }
            Ok(Ok(snapshot))
        })
    }
    fn begin_scope(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::native_mount::NativeMountWorkScope> {
        self.require_work(work)?;
        self.mount.begin_original_work(&work).map_err(Error::from)
    }
    fn read<'a>(
        &'a self,
        relative: &'a Path,
        maximum: usize,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        if !work.shares_ledger(self.custody.work()) {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let lease = self
                .mount
                .native_source_io_lease()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
            let physical = work.with_source_io_lease(lease)?;
            self.check(&physical).await?;
            let scope = self.mount.begin_original_work(&physical)?;
            let _io_scope = scope
                .try_clone_for_io()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
            physical
                .read_working_file(self.path(), self.identity, relative, maximum, || {
                    self.check(&physical)
                })
                .await
        })
    }
    fn write<'a>(
        &'a self,
        relative: &'a Path,
        content: &'a str,
        parents: bool,
        expected: Option<[u8; 32]>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        if !work.shares_ledger(self.custody.work()) {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        crate::NativeFutureBundle::new(work, || async move {
            let lease = self
                .mount
                .native_source_io_lease()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
            let physical = work.with_source_io_lease(lease)?;
            self.check(&physical).await?;
            let scope = self.mount.begin_original_work(&physical)?;
            let _io_scope = scope
                .try_clone_for_io()
                .map_err(acyclic_fs::native_mount::MountLifecycleError::Native)?;
            physical
                .write_working_file(
                    self.path(),
                    self.identity,
                    relative,
                    content,
                    parents,
                    expected,
                    || self.check(&physical),
                )
                .await
        })
    }
    fn sync<'a>(
        &'a self,
        permit: PublicationPermit,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                acyclic_fs::native_mount::NativeMountPublicationFailure<Error>,
            >,
        >,
    > {
        self.require_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.check(work).await?;
            let callback = Arc::clone(&self.callback);
            let result = self
                .mount
                .sync_with_permit(permit, &work, move |actual: &NativeMaterializationWork| {
                    let callback = Arc::clone(&callback);
                    let actual = actual.clone();
                    async move { callback.check(&actual)?.await }
                })
                .await;
            Ok(result)
        })
    }
    fn retire(
        &self,
        wait: &acyclic_fs::CancellationToken,
    ) -> Result<acyclic_fs::native_mount::NativeMountDrainProof> {
        self.mount.retire(wait).map_err(Error::from)
    }
}
