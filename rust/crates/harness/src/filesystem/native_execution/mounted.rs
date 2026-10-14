use super::*;
use acyclic_fs::native_mount::{
    NativeMountCallbackGuard, NativeMountPublicationFailure, NativeMountWorkScope,
    NativeSourceIoLease,
};

pub(super) fn require_snapshot(
    actual: &acyclic_fs::NativePublicationSnapshot,
    namespace: &NativeVolumeNamespace,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let NativeVolumeNamespace::LazyMount { snapshot } = namespace else {
        return Err(super::preparation_record::invalid());
    };
    if actual
        .lazy_snapshot(&mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        })?
        .as_ref()
        != Some(snapshot)
    {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "mounted volume changed its pinned snapshot",
        });
    }
    Ok(())
}

pub(super) async fn verify_snapshot(
    owner: &dyn NativeMountedVolume,
    namespace: &NativeVolumeNamespace,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let _scope = owner.enter(work)?;
    require_root(owner, work)?;
    let slot = work.claim(
        (std::mem::size_of::<NativeViewPublicationError>()
            + std::mem::size_of::<acyclic_fs::native_mount::MountSourceError>()) as u64,
        2,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let snapshot = owner
        .snapshot(work)?
        .await?
        .map_err(|cause| Error::NativePublication {
            failure: Box::new(NativeViewPublicationError {
                cause,
                published: Vec::new(),
                custody: slot,
            }),
            custody: None,
        })?;
    require_snapshot(&snapshot, namespace, work)?;
    require_root(owner, work)
}

pub(super) fn require_root(
    owner: &dyn NativeMountedVolume,
    work: &NativeMaterializationWork,
) -> Result<()> {
    if work.root_identity(owner.path())? != owner.identity()? {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Conflict,
            reason: "mounted volume root was replaced",
        });
    }
    Ok(())
}

/// The existing live mount retained by a native command.
pub trait NativeMountedVolume: Send + Sync {
    /// Actual mounted directory.
    fn path(&self) -> &Path;
    /// Original finite callback allowance.
    fn work(&self) -> Result<NativeMaterializationWork>;
    /// Holds this invocation through mounted callbacks.
    fn enter(&self, work: &NativeMaterializationWork) -> Result<NativeMountWorkScope>;
    /// Holds physical I/O against source retirement.
    fn io_lease(&self) -> Result<NativeSourceIoLease>;
    /// Captures a provider-side edit before publication.
    fn capture_host_path(&self, path: &Path, work: &NativeMaterializationWork) -> Result<()>;
    /// Actual mounted directory identity.
    fn identity(&self) -> Result<acyclic_fs::NativeRootIdentity>;
    /// Recognizes held SDK resources beside this mounted directory.
    fn owns_parent_entry(
        &self,
        entry: &acyclic_fs::native_host::HostListedEntry,
        work: &NativeMaterializationWork,
    ) -> std::result::Result<bool, acyclic_fs::NativeTreeMaterializationError>;
    /// Observes the actual mounted snapshot without copying its tree.
    fn snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                NativeMountPublicationFailure<Error>,
            >,
        >,
    >;
    /// Publishes through the actual mounted checkout and original owner guard.
    fn publish<'a>(
        &'a self,
        permit: PublicationPermit,
        guard: Arc<dyn NativeMountCallbackGuard>,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                NativeMountPublicationFailure<Error>,
            >,
        >,
    >;
}

impl<A, O, D, S> NativeMountedVolume for acyclic_fs::LazyMount<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: acyclic_fs::demand::DemandSource + 'static,
    S: acyclic_fs::LazyWorkspaceStore,
{
    fn path(&self) -> &Path {
        self.path()
    }
    fn work(&self) -> Result<NativeMaterializationWork> {
        self.native_work().map_err(Into::into)
    }
    fn enter(&self, work: &NativeMaterializationWork) -> Result<NativeMountWorkScope> {
        self.begin_original_work(work).map_err(Into::into)
    }
    fn io_lease(&self) -> Result<NativeSourceIoLease> {
        self.native_source_io_lease().map_err(Into::into)
    }
    fn capture_host_path(&self, path: &Path, work: &NativeMaterializationWork) -> Result<()> {
        self.capture_host_path(path, work).map_err(Into::into)
    }
    fn identity(&self) -> Result<acyclic_fs::NativeRootIdentity> {
        self.native_root_identity()
            .ok_or_else(super::preparation_record::invalid)
    }
    fn owns_parent_entry(
        &self,
        entry: &acyclic_fs::native_host::HostListedEntry,
        work: &NativeMaterializationWork,
    ) -> std::result::Result<bool, acyclic_fs::NativeTreeMaterializationError> {
        self.owns_native_parent_entry(entry, work)
    }
    fn snapshot<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                NativeMountPublicationFailure<Error>,
            >,
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let mut accounting_failure = None;
            Ok(self
                .publication_snapshot(work, work.cancellation(), &mut accounting_failure)
                .await
                .map_err(|cause| NativeMountPublicationFailure {
                    cause: cause.into(),
                    accounting_failure: accounting_failure.map(Box::new),
                    source_acknowledged: false,
                    publication: None,
                }))
        })
    }
    fn publish<'a>(
        &'a self,
        permit: PublicationPermit,
        guard: Arc<dyn NativeMountCallbackGuard>,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<
                acyclic_fs::NativePublicationSnapshot,
                NativeMountPublicationFailure<Error>,
            >,
        >,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            Ok(self
                .sync_with_permit(permit, work, move |actual: &NativeMaterializationWork| {
                    let actual = actual.clone();
                    let guard = Arc::clone(&guard);
                    async move { guard.check(&actual)?.await.map_err(Error::from) }
                })
                .await)
        })
    }
}
