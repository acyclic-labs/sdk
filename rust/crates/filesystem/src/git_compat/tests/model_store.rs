use super::*;
use crate::{OperationWindowSnapshot, OperationWindowStore};
use loom::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct CountedStore<S> {
    inner: S,
    attempts: AtomicUsize,
}

impl<S> CountedStore<S> {
    pub(super) fn new(inner: S) -> Self {
        Self {
            inner,
            attempts: AtomicUsize::new(0),
        }
    }

    pub(super) fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Relaxed)
    }
}

impl<S: GitCompatStore> GitCompatStore for CountedStore<S> {
    type Error = S::Error;

    async fn load(&self, workspace: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.inner.load(workspace).await
    }

    async fn compare_and_swap(
        &self,
        workspace: WorkspaceId,
        expected: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .compare_and_swap(workspace, expected, replacement)
            .await
    }

    async fn compare_and_delete(
        &self,
        workspace: WorkspaceId,
        expected: u64,
    ) -> Result<bool, Self::Error> {
        self.inner.compare_and_delete(workspace, expected).await
    }
}

impl<S: OperationWindowStore> OperationWindowStore for CountedStore<S> {
    type Error = S::Error;

    async fn load(
        &self,
        workspace: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        self.inner.load(workspace).await
    }

    async fn compare_and_swap(
        &self,
        workspace: WorkspaceId,
        expected: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .compare_and_swap(workspace, expected, replacement)
            .await
    }
}
