use super::*;

/// SDK capture and publication owner for one pinned native directory.
pub struct NativeWorkingSet<A, O> {
    pub(super) source: Arc<CheckoutMountSource<A, O>>,
    pub(super) workspace: Workspace<A, O>,
    pub(super) source_root: PathBuf,
    pub(super) source_identity: NativeRootIdentity,
    pub(super) selected_root: PathBuf,
    pub(super) expected_generation: GenerationId,
    pub(super) baseline: NativeViewBaseline,
    pub(super) writable: bool,
    pub(super) _custody: NativeAllocation,
}

impl<A, O> NativeWorkingSet<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Exact generation admitted into the physical view.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        self.expected_generation
    }

    /// Physical volume-root directory owned by this view.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.source_root
    }

    /// Accepts physical I/O on this source's paid retirement gate.
    pub fn native_source_io_lease(
        &self,
    ) -> Result<super::super::adapter::NativeSourceIoLease, NativeMountError> {
        self.source.native_source_io_lease().map_err(Into::into)
    }

    /// Revalidates the selected root and generation before presentation.
    pub async fn validate_for_presentation(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountLifecycleError> {
        self.check_head(self.expected_generation, work).await?;
        validate_native_working_set_root(
            &self.source_root,
            &self.selected_root,
            Some(self.source_identity),
            work,
        )?;
        self.check_head(self.expected_generation, work).await
    }

    async fn check_head(
        &self,
        expected: GenerationId,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountLifecycleError> {
        work.cancellation()
            .check()
            .map_err(|_| MountSourceError::Cancelled)?;
        if self.writable && self.workspace.head(work, work.cancellation()).await?.id() != expected {
            return Err(WorkspaceError::StaleGeneration.into());
        }
        Ok(())
    }

    /// Captures exactly the supplied changed paths.
    pub async fn capture_host_paths(
        &self,
        paths: &[MountPath],
        work: &NativeMaterializationWork,
    ) -> Result<(), MountLifecycleError> {
        self.capture(paths, HostCaptureScope::ExactPaths, work)
            .await
    }

    /// Reconciles all host and checkout descendants of a selected subtree.
    pub async fn capture_host_subtree(
        &self,
        root: &MountPath,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountLifecycleError> {
        self.capture(std::slice::from_ref(root), HostCaptureScope::Subtree, work)
            .await
    }

    async fn capture(
        &self,
        paths: &[MountPath],
        scope: HostCaptureScope,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountLifecycleError> {
        work.cancellation()
            .check()
            .map_err(|_| MountSourceError::Cancelled)?;
        self.source
            .capture_host(
                &self.source_root,
                paths,
                self.source_identity,
                scope,
                Some(self.baseline),
                work,
            )
            .await?;
        work.cancellation()
            .check()
            .map_err(|_| MountSourceError::Cancelled)?;
        Ok(())
    }

    /// Reconciles and publishes under the operation permit and owner guard.
    pub async fn sync_with_permit<F, Fut, E>(
        &self,
        permit: PublicationPermit,
        work: &NativeMaterializationWork,
        mut before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, NativeMountPublicationFailure<E>>
    where
        E: From<MountLifecycleError> + From<MountSourceError> + Send + 'static,
        F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut acknowledged = None;
        let result = async {
            if !self.writable {
                return Err((
                    E::from(MountLifecycleError::NativePolicy(
                        "read-only native view cannot publish",
                    )),
                    None,
                ));
            }
            let cancellation = work.cancellation();
            let current = super::super::adapter::await_lock(
                self.source.shared_checkout().lock(),
                &mut super::super::adapter::NativeDemandCarrier::Native(work),
                cancellation,
                Some(self.source.cancellation()),
            )
            .await
            .map_err(|cause| (E::from(cause), None))?
            .generation_id();
            let head = self
                .workspace
                .head(work, cancellation)
                .await
                .map_err(|cause| (E::from(MountLifecycleError::Workspace(cause)), None))?;
            if head.id() != current {
                return Err((E::from(MountSourceError::Stale), None));
            }
            self.capture_host_subtree(&MountPath::root(), work)
                .await
                .map_err(|cause| (E::from(cause), None))?;
            self.source
                .sync_async_with_permit_carrier_guarded_context_with_ack(
                    permit,
                    false,
                    &mut super::super::adapter::NativeDemandCarrier::Native(work),
                    &mut super::super::view_ledger::ViewLedgerAdmission::default(),
                    cancellation,
                    &mut before_publication,
                    Some(self.snapshot_context()),
                    &mut acknowledged,
                )
                .await
        }
        .await;
        result.map_err(
            |(cause, accounting_failure)| NativeMountPublicationFailure {
                cause,
                accounting_failure,
                source_acknowledged: acknowledged.is_some(),
                publication: acknowledged,
            },
        )
    }

    /// Verifies the full baseline and returns its immutable SDK qualification.
    pub async fn verify_unchanged(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<crate::NativePublicationSnapshot, MountLifecycleError> {
        self.capture_host_subtree(&MountPath::root(), work).await?;
        let pending = super::super::adapter::await_lock(
            self.source.shared_checkout().lock(),
            &mut super::super::adapter::NativeDemandCarrier::Native(work),
            work.cancellation(),
            Some(self.source.cancellation()),
        )
        .await?
        .has_pending_mutations();
        if pending {
            return Err(MountLifecycleError::NativePolicy(
                "native source view changed after admission",
            ));
        }
        work.cancellation()
            .check()
            .map_err(|_| MountSourceError::Cancelled)?;
        Ok(crate::NativePublicationSnapshot::unchanged(
            self.snapshot_context(),
            self.expected_generation,
        ))
    }

    fn snapshot_context(&self) -> crate::lazy_workspace::NativeSnapshotContext {
        crate::lazy_workspace::NativeSnapshotContext::Authored {
            workspace_id: self.workspace.id(),
            config: self.workspace.volume_config(),
        }
    }

    /// Retires accepted captures using this source's birth-time credit.
    pub fn retire_with_work(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<NativeMountDrainProof, MountLifecycleError> {
        self.source.cancel_cleanup(Some(wait))?;
        if !self.source.capture_retired() {
            return Err(NativeMountError::ControlPending.into());
        }
        Ok(NativeMountDrainProof {
            _source: self.source.clone(),
            #[cfg(windows)]
            namespace_outcome: None,
        })
    }
}

pub(super) fn validate_native_working_set_root(
    source_root: &Path,
    selected_root: &Path,
    expected: Option<NativeRootIdentity>,
    work: &NativeMaterializationWork,
) -> Result<NativeRootIdentity, MountLifecycleError> {
    let root = crate::native_host::HostRoot::open(source_root, &mut &*work)?;
    if expected.is_some_and(|identity| identity != root.identity()) {
        return Err(MountSourceError::Stale.into());
    }
    if !root.stat(selected_root, &mut &*work)?.is_dir() {
        return Err(MountLifecycleError::NativePolicy(
            "selected native working-set directory is absent",
        ));
    }
    Ok(root.identity())
}
