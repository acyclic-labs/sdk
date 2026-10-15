use super::*;
use crate::record_store::MemoryRecords;

/// Process-local lineage adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryWorkspaceLineageStore {
    records: MemoryRecords<WorkspaceId, WorkspaceLineageRecord>,
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
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.records
            .load(&workspace_id)
            .map_err(|_| MemoryWorkspaceLineageStoreError)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.records
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .map_err(|_| MemoryWorkspaceLineageStoreError)
    }
}
