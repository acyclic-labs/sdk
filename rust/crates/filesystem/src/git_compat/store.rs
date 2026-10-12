use super::*;

/// Durable optimistic-concurrency adapter for private compatibility state.
pub trait GitCompatStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads state for one repository/workspace identity.
    fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> impl Future<Output = Result<Option<GitCompatState>, Self::Error>> + Send;

    /// Atomically replaces one expected revision. Revision zero creates state.
    fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Atomically removes one exact compatibility-state revision.
    fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;
}

/// Process-local compatibility state adapter for tests and embedded callers.
#[derive(Default)]
pub struct MemoryGitCompatStore {
    pub(super) states: MemoryRecords<WorkspaceId, GitCompatState>,
}

impl MemoryGitCompatStore {
    /// Creates an empty adapter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Process-local compatibility store synchronization failure.
#[derive(Debug, Error)]
#[error("Git compatibility memory store is unavailable")]
pub struct MemoryGitCompatStoreError;

impl GitCompatStore for MemoryGitCompatStore {
    type Error = MemoryGitCompatStoreError;

    async fn load(&self, workspace_id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.states.load(&workspace_id).map_err(|_| MemoryGitCompatStoreError)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.states
            .compare_and_swap(workspace_id, expected_revision, replacement)
            .map_err(|_| MemoryGitCompatStoreError)
    }

    async fn compare_and_delete(&self, workspace_id: WorkspaceId, expected_revision: u64) -> Result<bool, Self::Error> {
        self.states.compare_and_delete(&workspace_id, expected_revision).map_err(|_| MemoryGitCompatStoreError)
    }
}
