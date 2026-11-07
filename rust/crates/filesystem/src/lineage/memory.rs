use super::*;
use crate::record_store::MemoryRecords;

/// Process-local lineage adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryWorkspaceLineageStore {
    records: MemoryRecords<WorkspaceId, Retained<WorkspaceLineageRecord>>,
}

impl MemoryWorkspaceLineageStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local lineage synchronization failure.
#[derive(Debug, Error)]
#[error("workspace-lineage memory store is unavailable")]
pub struct MemoryWorkspaceLineageStoreError;

impl WorkspaceLineageStore for MemoryWorkspaceLineageStore {
    type Error = MemoryWorkspaceLineageStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Retained<WorkspaceLineageRecord>>, WorkspaceLineageError<Self::Error>> {
        accounting
            .charge(crate::WorkCounters {
                backend_read_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(WorkspaceError::from)?;
        self.records
            .load_with(&workspace_id, |record| {
                let owned = copy_record(record, accounting)?;
                Retained::from_owned(owned, accounting)
            })
            .map_err(|_| WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError))?
            .transpose()
            .map_err(WorkspaceError::from)
            .map_err(Into::into)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: &WorkspaceLineageRecord,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<bool, WorkspaceLineageError<Self::Error>> {
        let owned = copy_record(replacement, accounting).map_err(WorkspaceError::from)?;
        let replacement = Retained::from_owned(owned, accounting).map_err(WorkspaceError::from)?;
        accounting
            .charge(crate::WorkCounters {
                backend_write_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            })
            .map_err(WorkspaceError::from)?;
        self.records
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .map_err(|_| WorkspaceLineageError::Store(MemoryWorkspaceLineageStoreError))
    }
}
