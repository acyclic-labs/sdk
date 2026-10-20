use super::super::session::NativeSessionControl;
use super::*;

/// Process-owned native mount over a sparse workspace.
pub struct LazyMount<A, O, D, S> {
    pub(super) source: Arc<LazyMountSource<A, O, D, S>>,
    pub(super) session: Mutex<Option<NativeMountSession>>,
    pub(super) destination: PathBuf,
    pub(super) _destination_custody: NativeAllocation,
}

impl<A, O, D, S> LazyMount<A, O, D, S> {
    /// Takes the first original fatal callback cause and its completed work.
    pub fn take_callback_failure(&self) -> Option<(NativeMountError, crate::WorkCounters)> {
        self.session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()?
            .take_callback_failure()
    }

    /// Exact mounted host path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.destination
    }
}

impl<A, O, D, S> LazyMount<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Checks an observed parent entry against this active mount's held resources.
    pub fn owns_native_parent_entry(
        &self,
        entry: &crate::native_host::HostListedEntry,
        work: &NativeMaterializationWork,
    ) -> Result<bool, crate::NativeTreeMaterializationError> {
        self.session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or(crate::NativeTreeMaterializationError::ExternalMutation)?
            .owns_parent_entry(entry, work)
    }

    /// Returns this mount's original finite callback allowance.
    pub fn native_work(&self) -> Result<NativeMaterializationWork, MountLifecycleError> {
        self.source
            .native_original_work()?
            .ok_or(MountLifecycleError::NativePolicy(
                "mounted volume has no original native work",
            ))
    }

    /// Accepts physical I/O on the mounted source's retirement gate.
    pub fn native_source_io_lease(
        &self,
    ) -> Result<super::super::adapter::NativeSourceIoLease, NativeMountError> {
        self.source.native_source_io_lease().map_err(Into::into)
    }

    /// Captures one provider-side host edit through the original mounted owner.
    pub fn capture_host_path(
        &self,
        relative: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<(), MountLifecycleError> {
        let _scope = self.begin_original_work(work)?;
        let work = work.with_source_io_lease(self.native_source_io_lease()?)?;
        let config = crate::model::VolumeConfig::native(crate::model::Lifecycle::Ephemeral);
        let mut path = crate::native_host::buffer(relative.components().count(), &mut &work)?;
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            };
            let mut name = crate::native_name::host_name_bytes_with_native_work(
                name,
                config.profile,
                config.limits.maximum_component_bytes,
                &work,
            )?;
            path.1.absorb(&mut name.1)?;
            path.0.push(name.0.1);
        }
        if path.0.is_empty() {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
        }
        let path = (MountPath { components: path.0 }, path.1);
        with_live_session(&self.session, |session| {
            session.capture_host_paths(std::slice::from_ref(&path.0), &work)
        })?;
        Ok(())
    }

    /// Retains already-paid container capacity until its payload drops.
    pub fn retain_payload_allocation(
        &mut self,
        incoming: &mut NativeAllocation,
    ) -> Result<(), MountLifecycleError> {
        self._destination_custody
            .absorb(incoming)
            .map_err(Into::into)
    }

    /// Authenticated post-mount native root witness.
    pub fn native_root_identity(&self) -> Option<NativeRootIdentity> {
        self.session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(NativeMountSession::native_root_identity)
    }

    /// Binds mounted callbacks to the original physical I/O owner.
    pub fn begin_original_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<super::super::NativeMountWorkScope, MountLifecycleError> {
        let owner = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owner
            .as_ref()
            .ok_or(MountSourceError::Stale)?
            .begin_original_work(work)
            .map_err(Into::into)
    }

    /// Publishes pending authored effects with an unrestricted permit.
    pub async fn sync(&self) -> Result<(), MountLifecycleError> {
        let work = self.native_work()?;
        self.sync_with_permit(PublicationPermit::Unrestricted, &work, |_| {
            std::future::ready(Ok::<(), MountLifecycleError>(()))
        })
        .await
        .map(|_| ())
        .map_err(|failure| failure.cause)
    }

    /// Publishes through the original allowance and retains the genuine SDK ACK.
    pub async fn sync_with_permit<F, Fut, E>(
        &self,
        permit: PublicationPermit,
        work: &NativeMaterializationWork,
        before_publication: F,
    ) -> Result<crate::NativePublicationSnapshot, NativeMountPublicationFailure<E>>
    where
        E: From<MountLifecycleError>
            + From<MountSourceError>
            + From<crate::FsError>
            + Send
            + 'static,
        F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut acknowledged = None;
        let result = async {
            work.admit(crate::WorkCounters::default())
                .map_err(MountLifecycleError::from)
                .map_err(|cause| (E::from(cause), None))?;
            with_live_session_async(&self.session, work, NativeSessionControl::FlushCallbacks)
                .await
                .map_err(|cause| (E::from(cause), None))?;
            self.source
                .sync_async_with_permit_qualified_native_work_guarded_with_ack(
                    permit,
                    work,
                    work.cancellation(),
                    &mut acknowledged,
                    before_publication,
                )
                .await
        }
        .await;
        let publication =
            result.map_err(
                |(cause, accounting_failure)| NativeMountPublicationFailure {
                    cause,
                    accounting_failure: accounting_failure.map(Box::new),
                    source_acknowledged: acknowledged.is_some(),
                    publication: acknowledged.map(Box::new),
                },
            )?;
        self.revalidate_async()
            .await
            .map_err(|cause| NativeMountPublicationFailure {
                cause: E::from(cause),
                accounting_failure: None,
                source_acknowledged: true,
                publication: Some(Box::new(publication)),
            })?;
        Ok(publication)
    }

    /// Imports one sealed selection through this mount's actual SDK owner.
    pub fn import_selected<'a, F, Fut, E>(
        &'a self,
        destination: crate::LazySnapshotRef,
        input: (crate::SelectedImportPlan, NativeAllocation),
        operation: crate::OperationId,
        permit: PublicationPermit,
        accounting: &crate::kernel::KernelAccounting<'a>,
        before_publication: F,
    ) -> impl Future<Output = Result<crate::LazySnapshotRef, E>>
    + Send
    + 'a
    + use<'a, A, O, D, S, E, F, Fut>
    where
        E: From<MountLifecycleError>
            + From<MountSourceError>
            + From<crate::FsError>
            + Send
            + 'static,
        F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let invocation = match accounting {
            crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } => {
                Ok((*work, *cancellation))
            }
            _ => Err(MountSourceError::OriginalSnapshotUnavailable),
        };
        async move {
            let (work, cancellation) = invocation.map_err(E::from)?;
            let allocation = input.1;
            let plan = input.0;
            let result = async {
                work.admit(crate::WorkCounters::default())
                    .map_err(MountLifecycleError::from)
                    .map_err(E::from)?;
                if !allocation.work().shares_ledger(work)
                    || !allocation.work().shares_resource_allowance(work)
                    || allocation.bytes() < plan.logical_owned_bytes()
                {
                    return Err(E::from(MountLifecycleError::Materialization(
                        crate::NativeTreeMaterializationError::InvalidLayout,
                    )));
                }
                with_live_session_async(&self.session, work, NativeSessionControl::FlushCallbacks)
                    .await
                    .map_err(E::from)?;
                let snapshot = self
                    .source
                    .import_selected(
                        destination,
                        plan,
                        operation,
                        permit,
                        crate::kernel::KernelAccounting::native(work, cancellation),
                        before_publication,
                    )
                    .await?;
                self.revalidate_async().await.map_err(E::from)?;
                Ok(snapshot)
            }
            .await;
            drop(allocation);
            result
        }
    }

    /// Returns only the original SDK import ACK after native visibility settles.
    pub async fn selected_import_snapshot(
        &self,
        operation: crate::OperationId,
        work: &NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::LazySnapshotRef>, NativeMountError> {
        work.admit(crate::WorkCounters::default())?;
        async {
            with_live_session_async(&self.session, work, NativeSessionControl::FlushCallbacks)
                .await?;
            self.revalidate_async().await
        }
        .await
        .map_err(|cause| match cause {
            MountLifecycleError::Native(cause) => cause,
            _ => unreachable!("session operation only returns native errors"),
        })?;
        self.source
            .selected_import_snapshot(operation, work, cancellation)
            .await
            .map_err(Into::into)
    }

    /// Advances a clean checkout and settles its native visibility.
    pub async fn advance_to_head(&self) -> Result<(), MountLifecycleError> {
        let advanced = self.source.advance_to_head_async().await;
        self.revalidate_async().await?;
        advanced.map_err(Into::into)
    }

    /// Makes source-side changes beneath the supplied paths visible.
    pub fn changed_outside(&self, paths: &[String]) -> Result<(), MountLifecycleError> {
        self.source.changed_outside(paths)?;
        self.revalidate()
    }

    /// Settles all source changes reported before this call. It runs on the
    /// source's original meter whatever meter the caller supplied, since
    /// the invalidator refuses to run while any other meter is active.
    pub fn revalidate(&self) -> Result<(), MountLifecycleError> {
        with_session_work(
            &self.session,
            self.source.as_ref(),
            NativeMountSession::revalidate,
        )
    }

    async fn revalidate_async(&self) -> Result<(), MountLifecycleError> {
        with_session_work_async(
            &self.session,
            self.source.as_ref(),
            NativeSessionControl::Revalidate,
        )
        .await
    }

    async fn settle(&self, work: &NativeMaterializationWork) -> Result<(), MountLifecycleError> {
        work.admit(crate::WorkCounters::default())?;
        with_live_session_async(&self.session, work, NativeSessionControl::FlushCallbacks).await?;
        self.revalidate_async().await
    }

    /// Reads the actual lazy namespace through its current publication owner.
    pub async fn snapshot(
        &self,
        work: &NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::LazySnapshotRef, MountLifecycleError> {
        self.settle(work).await?;
        self.source
            .snapshot(
                super::super::adapter::NativeDemandCarrier::Native(work),
                cancellation,
            )
            .await
            .map_err(Into::into)
    }

    /// Captures immutable readonly SDK qualification from the mounted namespace.
    pub async fn publication_snapshot(
        &self,
        work: &NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<crate::NativePublicationSnapshot, MountLifecycleError> {
        *accounting_failure = None;
        self.settle(work).await?;
        self.source
            .publication_snapshot(
                super::super::adapter::NativeDemandCarrier::Native(work),
                cancellation,
                accounting_failure,
            )
            .await
            .map_err(Into::into)
    }

    /// Publishes and detaches, retaining the owner on any failure.
    pub async fn unmount(&self) -> Result<(), MountLifecycleError> {
        self.sync().await?;
        self.retire(&crate::CancellationToken::new()).map(drop)
    }

    /// Retires physical work on birth-time credit and preserves uncaptured data.
    pub fn retire(
        &self,
        wait: &crate::CancellationToken,
    ) -> Result<NativeMountDrainProof, MountLifecycleError> {
        let mut owner = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = (|| {
            if let Some(session) = owner.as_mut() {
                session.stop_with_cleanup_wait(wait)?;
                if !session.cleanup_state().cleanup_complete() {
                    return Err(NativeMountError::ControlPending.into());
                }
            } else {
                self.source.cancel_with_cleanup_wait(wait)?;
            }
            if !self.source.physically_retired()? {
                return Err(NativeMountError::ControlPending.into());
            }
            let proof = NativeMountDrainProof {
                _source: self.source.clone(),
                #[cfg(windows)]
                namespace_outcome: owner
                    .as_mut()
                    .and_then(NativeMountSession::take_retired_namespace_outcome),
            };
            drop(owner.take());
            Ok(proof)
        })();
        if result.is_err()
            && let Some(session) = owner.as_mut()
        {
            session.record_measured_cleanup_failure();
        }
        result
    }
}
