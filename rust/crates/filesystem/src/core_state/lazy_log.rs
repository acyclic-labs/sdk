use super::*;

impl LocalCoreStateStore {
    pub(super) async fn swap_lazy_logged(
        &self,
        id: WorkspaceId,
        expected: u64,
        replacement: LazyWorkspaceState,
        work: crate::materializer::NativeMaterializationWork,
    ) -> Result<bool, LocalCoreStateStoreError> {
        native_records::transaction!(self, work, |root, log, work| {
            let current = log
                .get::<LazyWorkspaceState>(&root, id, None, None)?
                .map(|(value, _)| value);
            if current.as_ref().map_or(0, |state| state.revision) != expected {
                return Ok(false);
            }
            let parent = match &replacement.fork_origin {
                Some(origin)
                    if current
                        .as_ref()
                        .is_none_or(|state| state.fork_origin.is_none()) =>
                {
                    log.get::<LazyWorkspaceState>(&root, origin.snapshot.workspace_id, None, None)?
                        .map(|(state, _)| state)
                }
                _ => None,
            };
            work.charge(crate::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if !replacement.accepts_fork_transition(current.as_ref(), parent.as_ref()) {
                return Ok(false);
            }
            let mut change = Change::new(work)?;
            change.put(
                (
                    RecordKey::LazyWorkspace(id),
                    Some(RecordValue::LazyWorkspace(replacement)),
                )
                    .into(),
            )?;
            log.commit(&root, change)?;
            Ok(true)
        })
        .await
    }
}
