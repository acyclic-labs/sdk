use super::*;

pub(super) fn clone_native_lazy_state(
    state: &LazyWorkspaceState,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<LazyWorkspaceState, LocalCoreStateStoreError> {
    work.charge(crate::WorkCounters::UNCHARGED)?;
    // This schema contains only inline identifiers, scalars and fixed metadata.
    Ok(state.clone())
}

impl LocalCoreStateStore {
    pub(super) async fn load_lazy_logged(
        &self,
        id: WorkspaceId,
        work: crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<LazyWorkspaceState>, LocalCoreStateStoreError> {
        work.charge(crate::WorkCounters::default())?;
        if let Ownership::Owned(owner) = &self.ownership {
            let residents = owner.residents.read().unwrap_or_else(PoisonError::into_inner);
            for record in &residents.0 {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                if record.key == RecordKey::LazyWorkspace(id) {
                    return record.value.as_ref().and_then(LazyWorkspaceState::from_value)
                        .map(|state| clone_native_lazy_state(state, &work)).transpose();
                }
            }
        }
        self.materialization(work).transaction(move |root| {
            let work = root._allocation.as_ref().expect("bound native work").work();
            root.with_log( |log| log.get_lazy_measured(&root, id, work))
        }).await
    }

    pub(super) async fn swap_lazy_logged(
        &self,
        id: WorkspaceId,
        expected: u64,
        replacement: LazyWorkspaceState,
        work: crate::materializer::NativeMaterializationWork,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.materialization(work).transaction(move |root| {
            let work = root._allocation.as_ref().expect("bound native work").work();
            root.with_log( |log| {
                let current = log.get_lazy_measured(&root, id, work)?;
                if current.as_ref().map_or(0, |state| state.revision) != expected {
                    return Ok(false);
                }
                let prior_origin = current.as_ref().and_then(|state| state.fork_origin);
                if prior_origin.is_some() || replacement.fork_origin.is_some() {
                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                    let Some(origin) = replacement.fork_origin else { return Ok(false); };
                    if replacement.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
                        || replacement.workspace_id != id
                        || replacement.parent_workspace_id != Some(origin.snapshot.workspace_id)
                        || origin.snapshot.workspace_id == id
                    {
                        return Ok(false);
                    }
                    if let Some(prior) = prior_origin {
                        let current = current.as_ref().expect("retained origin has a binding");
                        if current.schema_version != replacement.schema_version
                            || current.workspace_id != id
                            || current.parent_workspace_id != replacement.parent_workspace_id
                            || prior.operation_id != origin.operation_id
                            || prior.config != origin.config
                            || prior.snapshot != origin.snapshot
                            || (prior.phase == crate::LazyForkPhase::Committed
                                && origin.phase != crate::LazyForkPhase::Committed)
                        {
                            return Ok(false);
                        }
                    } else if current.is_some() || expected != 0 || replacement.revision != 1
                        || origin.phase != crate::LazyForkPhase::Prepared
                    {
                        return Ok(false);
                    }
                    if prior_origin.is_none_or(|prior| prior.phase == crate::LazyForkPhase::Prepared)
                        && (replacement.source != origin.snapshot.source
                            || replacement.overlay != origin.snapshot.overlay
                            || replacement.shadows != origin.snapshot.shadows
                            || replacement.pending_remove.is_some())
                    {
                        return Ok(false);
                    }
                    if prior_origin.is_none() {
                        // Authenticate only the first freeze while companion collection
                        // and source binding updates share this same log transaction.
                        let Some(parent) = log.get_lazy_measured(
                            &root, origin.snapshot.workspace_id, work,
                        )? else { return Ok(false); };
                        if parent.schema_version != replacement.schema_version
                            || parent.workspace_id != origin.snapshot.workspace_id
                            || parent.source != origin.snapshot.source
                            || parent.overlay != origin.snapshot.overlay
                            || parent.shadows != origin.snapshot.shadows
                            || parent.fork_origin.is_some_and(|origin|
                                origin.phase == crate::LazyForkPhase::Prepared)
                        {
                            return Ok(false);
                        }
                    }
                }
                let mut change = Change::new(work)?;
                change.put_scalar(RecordKey::LazyWorkspace(id), Some(RecordValue::LazyWorkspace(replacement)), work)?;
                log.commit(&root, change)?;
                Ok(true)
            })
        }).await
    }
}
