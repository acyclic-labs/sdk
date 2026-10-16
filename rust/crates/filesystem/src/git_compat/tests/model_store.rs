use super::*;
use crate::{
    OperationWindowSnapshot, OperationWindowStore,
    operation_window::MemoryOperationWindowStoreError,
    record_store::{MemoryRecords, Poisoned, Revisioned},
};

// Negative control: same instrumented record lock, deliberately omitting the
// expected-revision check. Both real coordinators must detect the lost fence.
pub(super) struct UncheckedStore<V>(MemoryRecords<WorkspaceId, V>);

impl<V> Default for UncheckedStore<V> {
    fn default() -> Self {
        Self(MemoryRecords::default())
    }
}

impl<V: Clone + Revisioned> UncheckedStore<V> {
    fn replace(&self, workspace: WorkspaceId, replacement: V) -> Result<bool, Poisoned> {
        self.0
            .0
            .lock()
            .map(|mut values| {
                values.insert(workspace, replacement);
                true
            })
            .map_err(|_| Poisoned)
    }
}

impl GitCompatStore for UncheckedStore<GitCompatState> {
    type Error = MemoryGitCompatStoreError;

    async fn load(&self, workspace: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.0
            .load(&workspace)
            .map_err(|_| MemoryGitCompatStoreError)
    }

    async fn compare_and_swap(
        &self,
        workspace: WorkspaceId,
        _expected: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.replace(workspace, replacement)
            .map_err(|_| MemoryGitCompatStoreError)
    }

    async fn compare_and_delete(
        &self,
        workspace: WorkspaceId,
        expected: u64,
    ) -> Result<bool, Self::Error> {
        self.0
            .compare_and_delete(&workspace, expected)
            .map_err(|_| MemoryGitCompatStoreError)
    }
}

impl OperationWindowStore for UncheckedStore<OperationWindowSnapshot> {
    type Error = MemoryOperationWindowStoreError;

    async fn load(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        self.0
            .load(&workspace)
            .map_err(|_| MemoryOperationWindowStoreError)
    }

    async fn compare_and_swap(
        &self,
        workspace: WorkspaceId,
        _expected: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        self.replace(workspace, replacement)
            .map_err(|_| MemoryOperationWindowStoreError)
    }
}
