use super::*;

/// Original process custody and the exact physically retired mount prefix.
pub struct NativeTaskViewDrainReceipt {
    pub(super) process: crate::filesystem::native_process::NativeProcessDrainProof,
    pub(super) mounts: Vec<acyclic_fs::native_mount::NativeMountDrainProof>,
    pub(super) custody: acyclic_fs::NativeAllocation,
}

impl NativeTaskViewDrainReceipt {
    /// Borrows the original process proof without discarding mounted custody.
    pub fn process_drain_proof(
        &self,
    ) -> &crate::filesystem::native_process::NativeProcessDrainProof {
        &self.process
    }
}

pub(super) trait NativeTaskWorkingViewSeal {}

/// An SDK-issued working view bound to its original owner and ledger.
pub trait NativeTaskWorkingView: NativeTaskWorkingViewSeal + std::any::Any + Send + Sync {
    /// Compares the exact held view and owner.
    fn same_identity(&self, other: &dyn NativeTaskWorkingView) -> bool;
    /// Selects an authorized mounted volume.
    fn volume_for_builtin_path<'a>(
        &'a self,
        path: &'a Path,
        operation: VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<&'a VolumeRef>;
    /// Returns the original task and creation fence.
    fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence);
    /// Returns its immutable namespace declaration.
    fn manifest(&self) -> &NativeViewManifest;
    /// Returns its original ledger.
    fn original_native_work(&self) -> &NativeMaterializationWork;
    /// Authenticates the view on that ledger.
    fn verify_with_native_work<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Reads verified physical bytes.
    fn read_builtin_file<'a>(
        &'a self,
        operation: crate::OperationId,
        path: &'a Path,
        maximum: usize,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    /// Writes an authorized mounted path.
    fn write_builtin_file<'a>(
        &'a self,
        operation: crate::OperationId,
        path: &'a Path,
        content: &'a str,
        parents: bool,
        expected: Option<[u8; 32]>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}

pub(super) struct HeldNativeTaskWorkingView<P, A, O> {
    pub(super) view: Arc<NativeVolumeView<A, O>>,
    pub(super) owner: Arc<TaskJournalOwner<P>>,
    pub(super) custody: acyclic_fs::NativeAllocation,
}

impl<P, A, O> HeldNativeTaskWorkingView<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) fn require_original(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self.custody.work().shares_allocation_context(work)
            || !self
                .view
                .preparation
                ._custody
                .work()
                .shares_allocation_context(work)
            || !self.owner.native_shared_work_matches(work)
            || self.owner.task_binding()
                != (
                    self.view.preparation.task,
                    &self.view.preparation.original_fence,
                )
        {
            return Err(Error::from(
                acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        work.admit(Default::default()).map_err(Error::from)
    }
    pub(super) async fn authenticate(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.require_original(work)?;
        self.view
            .authenticate_builtin_view(&self.owner, work)
            .await?;
        self.view.validate_dispatch_with_work(work).await?;
        self.owner
            .verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(work)?),
            )
            .await
    }
}

impl<P: StreamProvider, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    NativeTaskWorkingViewSeal for HeldNativeTaskWorkingView<P, A, O>
{
}

impl<P, A, O> NativeTaskWorkingView for HeldNativeTaskWorkingView<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn same_identity(&self, other: &dyn NativeTaskWorkingView) -> bool {
        (other as &dyn std::any::Any)
            .downcast_ref::<Self>()
            .is_some_and(|other| {
                Arc::ptr_eq(&self.view, &other.view) && Arc::ptr_eq(&self.owner, &other.owner)
            })
    }
    fn volume_for_builtin_path<'a>(
        &'a self,
        path: &'a Path,
        operation: VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<&'a VolumeRef> {
        self.require_original(work)?;
        self.view
            .builtin_volume(path, operation, work)
            .map(|(mapping, _, _)| &mapping.volume)
    }
    fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence) {
        (
            self.view.preparation.task,
            &self.view.preparation.original_fence,
        )
    }
    fn manifest(&self) -> &NativeViewManifest {
        &self.view.preparation.manifest
    }
    fn original_native_work(&self) -> &NativeMaterializationWork {
        self.view.work()
    }
    fn verify_with_native_work<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || self.authenticate(work))
    }
    fn read_builtin_file<'a>(
        &'a self,
        operation: crate::OperationId,
        path: &'a Path,
        maximum: usize,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.view
                .read_builtin_file(&self.owner, operation, path, maximum, work)
                .await
        })
    }
    fn write_builtin_file<'a>(
        &'a self,
        operation: crate::OperationId,
        path: &'a Path,
        content: &'a str,
        parents: bool,
        expected: Option<[u8; 32]>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        self.require_original(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.view
                .write_builtin_file(
                    &self.owner,
                    operation,
                    path,
                    content,
                    parents,
                    expected,
                    work,
                )
                .await
        })
    }
}

/// One prepared namespace used by the process provider.
pub trait NativeProcessView<P: StreamProvider>: Send + Sync {
    /// Immutable original request/approval selection.
    fn manifest(&self) -> &NativeViewManifest;
    /// The original preparation/lifetime token, never a replacement request token.
    fn operation_cancellation(&self) -> &acyclic_fs::CancellationToken;
    /// A genuine prepared caller family; legacy preparation carries no such proof.
    fn original_native_work(&self) -> &NativeMaterializationWork;
    /// Uses the caller's actual original ledger and the held view's cancellation.
    fn validate_for_dispatch<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Checks the same held root without constructing a replacement work span.
    fn validate_working_directory(
        &self,
        cwd: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<()>;
    /// Retains genuine mounted-namespace scopes through process quiescence.
    fn retain_native_namespace_scopes<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            (
                Vec<acyclic_fs::native_mount::NativeMountWorkScope>,
                acyclic_fs::NativeAllocation,
            ),
        >,
    >;
    /// Retains actual completed destinations and original typed publication cause.
    fn publish<'a>(
        &'a self,
        owner: &'a TaskJournalOwner<P>,
        work: &'a NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            std::result::Result<NativeViewPublicationReceipt, NativeViewPublicationError>,
        >,
    >;
}
