use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(crate) async fn selected_snapshot(
        &self,
        operation: crate::OperationId,
        generation: crate::GenerationId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(LazySnapshotRef, crate::Digest), LazyWorkspaceError> {
        let (record, mut guard) = self
            .workspace
            .volume
            .fs
            .workspace_operation_generation(
                &self.workspace.volume,
                operation,
                ctx,
                cancellation(ctx),
            )
            .await?;
        let Some((actual, record)) = record else {
            crate::kernel::retire_allocation(guard, 0, ctx)?;
            return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
        };
        let bytes = record.payload.len() as u64;
        if matches!(guard, KernelOwnedAllocation::Standalone) {
            let (funding, admitted) = ctx.record_actual_owned(bytes)?;
            guard = funding;
            if let Err(error) = admitted {
                drop(record);
                crate::kernel::retire_allocation(guard, bytes, ctx)?;
                return Err(error.into());
            }
        }
        let record = Owned::new(record, guard, bytes);
        let result = (|| {
            if actual != generation {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            }
            let decoded = crate::kernel::decode_publication_evidence_with_accounting(
                &record.value.payload,
                self.workspace.limits().maximum_object_bytes,
                ctx,
            )?;
            let evidence = decoded
                .original_evidence
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let context = publication_codec::decode_publication_context(evidence, operation, ctx)?;
            let NativeSnapshotContext::Selected { cut, input } = context else {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            };
            if cut.workspace_id != self.workspace.id()
                || cut.config != self.workspace.volume.config
                || cut.source.identity != self.source.reference().identity
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            ctx.charge(WorkCounters {
                bytes_hashed: LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
                ..WorkCounters::UNCHARGED
            })?;
            Ok((
                LazySnapshotRef::new(
                    cut.workspace_id,
                    generation,
                    cut.source,
                    cut.overlay,
                    cut.shadows,
                ),
                input,
            ))
        })();
        record.retire(ctx)?;
        result
    }
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
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
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
                    (snapshot, self.workspace.volume.config),
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
        }
        Err(LazyWorkspaceError::Concurrent)
    }
}
