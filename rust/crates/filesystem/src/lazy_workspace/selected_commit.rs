use super::*;
use crate::kernel::{KernelAccounting, Owned, allocation::KernelOwnedAllocation};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Resolves the immutable selected publication, independently of the current head and hints.
    pub(crate) async fn selected_snapshot(
        &self,
        operation: crate::OperationId,
        generation: crate::GenerationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(LazySnapshotRef, crate::Digest), LazyWorkspaceError> {
        let (value, mut allocation) = self
            .workspace
            .volume
            .fs
            .workspace_operation_generation(
                &self.workspace.volume,
                operation,
                accounting,
                cancellation(accounting),
            )
            .await?;
        let Some((actual, record)) = value else {
            crate::kernel::retire_allocation(allocation, 0, accounting)?;
            return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
        };
        let bytes = record.payload.len() as u64;
        if matches!(allocation, KernelOwnedAllocation::Standalone) {
            let (adopted, admission) = accounting.record_actual_owned(bytes)?;
            allocation = adopted;
            if let Err(error) = admission {
                drop(record);
                crate::kernel::retire_allocation(allocation, bytes, accounting)?;
                return Err(error.into());
            }
        }
        let record = Owned::new(record, allocation, bytes);
        let result = (|| {
            if actual != generation {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            }
            let decoded = crate::kernel::decode_publication_evidence_with_accounting(
                &record.value.payload,
                self.workspace.limits().maximum_object_bytes,
                accounting,
            )?;
            let evidence = decoded
                .original_evidence
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let context = super::publication_codec::decode_publication_context(
                evidence, operation, accounting,
            )?;
            let NativeSnapshotContext::Selected { cut, input } = context else {
                return Err(LazyWorkspaceError::OriginalSnapshotUnavailable);
            };
            if cut.workspace_id != self.workspace.id()
                || cut.config != self.workspace.volume.config
                || cut.source.identity != self.source.reference().identity
            {
                return Err(LazyWorkspaceError::StaleIdentity);
            }
            accounting.charge(WorkCounters {
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
        record.retire(accounting)?;
        result
    }

    pub(crate) async fn finish_selected_import(
        &self,
        operation: crate::OperationId,
        generation: crate::GenerationId,
        destination: LazySnapshotRef,
        source: LazySnapshotRef,
        source_config: VolumeConfig,
        input: crate::Digest,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<LazySnapshotRef, LazyWorkspaceError> {
        if destination.workspace_id != self.workspace.id() {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        let (snapshot, original_input) = self
            .selected_snapshot(operation, generation, accounting)
            .await?;
        if original_input != input {
            return Err(LazyWorkspaceError::StaleIdentity);
        }
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            let state = self
                .store
                .load_lazy_workspace(self.workspace.id(), accounting)
                .await?
                .ok_or(LazyWorkspaceError::OriginalSnapshotUnavailable)?;
            let pending = state.pending_remove.as_ref().filter(|pending| {
                matches!(pending.kind, PendingLazyRemoveKind::Mounted { idempotency_key }
                    if idempotency_key.operation_id() == operation)
            });
            if let Some(pending) = pending {
                if pending.prepared_overlay != snapshot.overlay
                    || state.overlay != snapshot.overlay
                    || state.shadows != snapshot.shadows
                    || state.source != snapshot.source
                {
                    return Err(LazyWorkspaceError::StaleIdentity);
                }
            } else {
                return Ok(snapshot);
            }
            let replacement = LazyWorkspaceState {
                revision: state
                    .revision
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?,
                pending_remove: None,
                completed_selected_import: Some(CompletedSelectedImport {
                    operation,
                    snapshot,
                    destination,
                    source,
                    input,
                    config: self.workspace.volume.config,
                    source_config,
                }),
                ..state.clone()
            };
            if self
                .store
                .compare_and_swap_lazy_workspace(
                    self.workspace.id(),
                    state.revision,
                    replacement,
                    accounting,
                )
                .await?
            {
                return Ok(snapshot);
            }
        }
        Err(LazyWorkspaceError::Concurrent)
    }
}
