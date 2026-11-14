use super::*;
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    /// Returns the authenticated selected-import snapshot recorded by this operation.
    pub async fn selected_import_snapshot(
        &self,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<LazySnapshotRef>, LazyWorkspaceError> {
        let Some(publication) = self.original_publication(operation, ctx).await? else {
            return Ok(None);
        };
        self.selected_publication_snapshot(publication, ctx)
            .map(|(snapshot, _)| Some(snapshot))
    }

    pub(crate) async fn selected_snapshot(
        &self,
        operation: crate::OperationId,
        generation: crate::GenerationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazySnapshotRef, crate::Digest), LazyWorkspaceError> {
        let (publication, _, _) = self
            .load_original_publication(operation, Some(generation), ctx)
            .await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        self.selected_publication_snapshot(publication, ctx)
    }

    fn selected_publication_snapshot(
        &self,
        publication: NativePublicationSnapshot,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazySnapshotRef, crate::Digest), LazyWorkspaceError> {
        let NativeSnapshotContext::Selected { cut, input } = publication.context else {
            return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
        };
        ctx.charge(WorkCounters {
            bytes_hashed: LazySnapshotRef::hash_bytes(publication.generation, &cut.source),
            ..WorkCounters::UNCHARGED
        })?;
        Ok((
            LazySnapshotRef::new(
                cut.workspace_id,
                publication.generation,
                cut.source,
                cut.overlay,
                cut.shadows,
            ),
            input,
        ))
    }

    /// Read one operation's actual native DATA receipt and its authenticated
    /// context. Later head or companion advancement cannot replace this cut.
    pub async fn original_publication(
        &self,
        operation: crate::OperationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<NativePublicationSnapshot>, LazyWorkspaceError> {
        Ok(self
            .load_original_publication(operation, None, ctx)
            .await?
            .map(|(publication, _, _)| publication))
    }

    pub(super) async fn load_original_publication(
        &self,
        operation: crate::OperationId,
        expected: Option<crate::GenerationId>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<
        Option<(NativePublicationSnapshot, crate::Sequence, crate::Digest)>,
        LazyWorkspaceError,
    > {
        let (record, mut guard) = self
            .workspace
            .volume
            .fs
            .workspace_operation_generation(
                &self.workspace.volume,
                operation,
                Some(self.workspace.limits().maximum_object_bytes),
                Some(self.workspace.limits().maximum_object_bytes),
                ctx,
                cancellation(ctx),
            )
            .await?;
        let Some((actual, record, evidence)) = record else {
            crate::kernel::retire_allocation(guard, 0, ctx)?;
            return Ok(None);
        };
        let bytes = if self
            .workspace
            .volume
            .fs
            .authority()
            .retains_operation_payload()
        {
            0
        } else {
            record.payload.len() as u64
        };
        if bytes != 0 && matches!(guard, KernelOwnedAllocation::Standalone) {
            let (funding, admitted) = ctx.record_actual_owned(bytes)?;
            guard = funding;
            if let Err(error) = admitted {
                drop(record);
                crate::kernel::retire_allocation(guard, bytes, ctx)?;
                return Err(error.into());
            }
        }
        let record = Owned::new(record, guard, bytes);
        let result = (async {
            if expected.is_some_and(|expected| actual != expected) {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            }
            let reference = evidence.ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let evidence = self
                .workspace
                .volume
                .fs
                .read_original_publication_evidence(
                    &self.workspace.volume,
                    reference,
                    ctx,
                    cancellation(ctx),
                )
                .await?;
            let context = publication_codec::decode_publication_context(&evidence, operation, ctx)?;
            if context.workspace_id() != self.workspace.id()
                || context.config() != self.workspace.volume.config
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            if let NativeSnapshotContext::Lazy(cut) | NativeSnapshotContext::Selected { cut, .. } =
                &context
                && cut.source.identity != self.source.identity()
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            let prior = record
                .value
                .sequence
                .get()
                .checked_sub(1)
                .filter(|position| *position != 0)
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            Ok(Some((
                NativePublicationSnapshot {
                    context,
                    operation: Some(operation),
                    generation: actual,
                },
                crate::Sequence::new(prior),
                record.value.previous_digest,
            )))
        })
        .await;
        record.retire(ctx)?;
        result
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn finish_selected_import(
        &self,
        operation: crate::OperationId,
        generation: crate::GenerationId,
        destination: LazySnapshotRef,
        source: LazySnapshotRef,
        config: VolumeConfig,
        input: crate::Digest,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        if destination.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let (snapshot, saved) = self.selected_snapshot(operation, generation, ctx).await?;
        if saved != input {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let state = self
            .store
            .load_lazy_workspace(self.workspace.id(), ctx)
            .await?
            .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
        let pending = state.pending_remove.as_ref().filter(|pending|
            matches!(pending.kind,PendingLazyRemoveKind::Mounted { idempotency_key } if idempotency_key.operation_id() == operation));
        let Some(pending) = pending else {
            return Ok(snapshot);
        };
        if pending.prepared_overlay != snapshot.overlay
            || state.overlay != snapshot.overlay
            || state.shadows != snapshot.shadows
            || state.source != snapshot.source
        {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let revision = state.revision;
        let next = LazyWorkspaceState {
            revision: revision.checked_add(1).ok_or(crate::WorkError::Overflow)?,
            pending_remove: None,
            selected_import_roots: Some([
                (snapshot.clone(), self.workspace.volume.config),
                (destination, self.workspace.volume.config),
                (source, config),
            ]),
            ..state
        };
        if self
            .store
            .compare_and_swap_lazy_workspace(self.workspace.id(), revision, next, ctx)
            .await?
        {
            return Ok(snapshot);
        }
        Err(LazyWorkspaceError::Concurrent)
    }
}
