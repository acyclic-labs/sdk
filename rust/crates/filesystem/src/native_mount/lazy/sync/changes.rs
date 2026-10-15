use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    pub(super) async fn publish_clean<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        before_publication: &mut F,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = &*accounting
        else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (work, cancellation) = (*work, *cancellation);

        let context = NativeSnapshotContext::Lazy(
            self.lazy
                .publication_cut(None, accounting)
                .await
                .map_err(lazy_error)?,
        );
        self.authored
            .shared_checkout()
            .publish(
                permit,
                crate::kernel::KernelAccounting::native(work, cancellation),
                Some(self.authored.cancellation()),
                before_publication,
                Some(context),
                acknowledged,
            )
            .await
            .map_err(|(error, accounting)| {
                *accounting_failure = accounting;
                error
            })?;
        let published = acknowledged.ok_or(MountSourceError::Indeterminate)?;
        self.prune_detached()?;
        Ok(published)
    }
    pub(super) async fn complete_publication<F, Fut, E>(
        &self,
        permit: crate::PublicationPermit,
        publication: (
            NativeSnapshotContext,
            crate::OperationId,
            Option<SelectedImportProof>,
        ),
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        before_publication: &mut F,
        acknowledged: &mut Option<crate::NativePublicationSnapshot>,
        accounting_failure: &mut Option<MountSourceError>,
    ) -> Result<crate::NativePublicationSnapshot, E>
    where
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<MountSourceError> + From<crate::FsError> + Send + 'static,
    {
        let crate::kernel::KernelAccounting::NativeOriginal { work, cancellation } = &*accounting
        else {
            return Err(MountSourceError::OriginalSnapshotUnavailable.into());
        };
        let (work, cancellation) = (*work, *cancellation);
        let (context, operation, selected_proof) = publication;
        self.authored
            .shared_checkout()
            .publish(
                permit,
                crate::kernel::KernelAccounting::native(work, cancellation),
                Some(self.authored.cancellation()),
                before_publication,
                Some(context),
                acknowledged,
            )
            .await
            .map_err(|(error, accounting)| {
                *accounting_failure = accounting;
                error
            })?;
        let published = acknowledged.ok_or(MountSourceError::Indeterminate)?;
        self.finish_publication(
            operation,
            published.generation(),
            selected_proof,
            accounting,
        )
        .await?;
        self.finish_detached_publication(work)?;
        self.clear_mount_removals()?;
        Ok(published)
    }
    pub(super) async fn publication_context(
        &self,
        operation: crate::OperationId,
        committed: Option<crate::GenerationId>,
        proof: Option<SelectedImportProof>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<NativeSnapshotContext, MountSourceError> {
        if let (Some(generation), Some(SelectedImportProof { input, .. })) = (committed, proof) {
            let (snapshot, original_input) = self
                .lazy
                .selected_snapshot(operation, generation, accounting)
                .await
                .map_err(lazy_error)?;
            if original_input != input {
                return Err(MountSourceError::Stale);
            }
            let cut = self
                .lazy
                .publication_cut(Some(operation), accounting)
                .await
                .map_err(lazy_error)?;
            if cut.workspace_id != snapshot.workspace_id
                || cut.source != snapshot.source
                || cut.overlay != snapshot.overlay
                || cut.shadows != snapshot.shadows
            {
                return Err(MountSourceError::Stale);
            }
            return Ok(NativeSnapshotContext::Selected { cut, input });
        }
        let cut = self
            .lazy
            .publication_cut(Some(operation), accounting)
            .await
            .map_err(lazy_error)?;
        Ok(match proof {
            Some(SelectedImportProof { input, .. }) => {
                NativeSnapshotContext::Selected { cut, input }
            }
            None => NativeSnapshotContext::Lazy(cut),
        })
    }
    pub(super) async fn finish_publication(
        &self,
        operation: crate::OperationId,
        generation: crate::GenerationId,
        proof: Option<SelectedImportProof>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), MountSourceError> {
        if let Some(SelectedImportProof {
            destination,
            source,
            config,
            input,
        }) = proof
        {
            self.lazy
                .finish_selected_import(
                    operation,
                    generation,
                    destination,
                    source,
                    config,
                    input,
                    accounting,
                )
                .await
                .map_err(lazy_error)?;
        } else if !self
            .lazy
            .finish_mount_removals(accounting)
            .await
            .map_err(lazy_error)?
        {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }
    pub(super) async fn publication_changes(
        &self,
        candidate: &CheckoutCandidate<'_, A, O>,
        paths: &[String],
        changes: Option<&[NamespacePath]>,
        scratch: &mut adapter::InputBuffers<'_, '_>,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        carrier: &mut adapter::NativeDemandCarrier<'_>,
    ) -> Result<
        (
            Vec<crate::NativeAllocation>,
            Vec<NamespacePath>,
            ViewEffect,
            Option<crate::NativeAllocation>,
        ),
        MountSourceError,
    > {
        let crate::kernel::KernelAccounting::NativeOriginal { cancellation, .. } = &*accounting
        else {
            return Err(MountSourceError::OriginalSnapshotUnavailable);
        };
        let cancellation = *cancellation;
        let mut namespace_allocations = scratch.vec(paths.len())?;
        let mut namespaces = scratch.vec(paths.len())?;
        for path in paths {
            let namespace = self
                .lazy
                .namespace_path(path, accounting)
                .map_err(lazy_error)?;
            let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
                namespace.allocation
            else {
                return Err(MountSourceError::NativeWork(
                    crate::NativeTreeMaterializationError::InvalidLayout,
                ));
            };
            namespace_allocations.push(allocation);
            namespaces.push(namespace.value);
        }
        let scope = match changes {
            Some(changes) => InstallScope::CombinedPaths(&namespaces, changes),
            None => InstallScope::Paths(&namespaces),
        };
        let (installed, _allocation) = adapter::installed_changes(
            &candidate.base,
            &candidate.checkout,
            scope,
            carrier,
            cancellation,
        )
        .await?;
        Ok((namespace_allocations, namespaces, installed, _allocation))
    }
    pub(super) async fn install_publication(
        &self,
        candidate: CheckoutCandidate<'_, A, O>,
        installed: &ViewEffect,
        carrier: &mut adapter::NativeDemandCarrier<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), MountSourceError> {
        if self
            .authored
            .shared_checkout()
            .install_candidate(
                (candidate, None),
                carrier,
                cancellation,
                Some(self.authored.cancellation()),
                |carrier, stamp| {
                    if self.authored.shared_checkout().ledger.stamp() != stamp {
                        return Err(MountSourceError::Stale);
                    }
                    self.authored
                        .shared_checkout()
                        .ledger
                        .record_deferred(&ViewChange::Effect(installed), carrier)
                },
                |checkout, candidate, prepared| {
                    cancellation
                        .commit_if_both_live(self.authored.cancellation(), || {
                            checkout.adopt_prepared_candidate(
                                &candidate.base,
                                candidate.checkout,
                                prepared,
                            )
                        })
                        .map_err(|_| MountSourceError::Cancelled)
                },
            )
            .await?
            .is_none()
        {
            return Err(MountSourceError::Stale);
        }
        Ok(())
    }
}
