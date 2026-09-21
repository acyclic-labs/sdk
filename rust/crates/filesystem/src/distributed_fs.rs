//! Small composition root for one distributed-filesystem deployment.

use crate::demand::DemandSource;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, GitCompatRepository, LazyWorkspace,
    LazyWorkspaceError, LazyWorkspaceStore, MultiRootPublicationAuthorizer,
    MultiRootPublicationCoordinator, MultiRootPublicationStore, MultiRootPublisher,
    OperationWindowCoordinator, OperationWindowStore, Workspace, WorkspaceContextRegistry,
    WorkspaceContextStore, WorkspaceGraph, WorkspaceId, WorkspaceLineageError,
    WorkspaceLineageStore, WorkspaceResolver,
};
use std::sync::Arc;

/// Shared constructors for the semantic handles of one filesystem deployment.
pub struct DistributedFs<A, O, S> {
    fs: Fs<A, O>,
    store: S,
}

impl<A, O, S: Clone> Clone for DistributedFs<A, O, S> {
    fn clone(&self) -> Self {
        Self::new(self.fs.clone(), self.store.clone())
    }
}

impl<A, O, S> DistributedFs<A, O, S> {
    /// Composes an existing filesystem and durable semantic-state store.
    #[must_use]
    pub const fn new(fs: Fs<A, O>, store: S) -> Self {
        Self { fs, store }
    }

    /// Resolves one workspace by authenticated durable identity.
    pub async fn workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Workspace<A, O>, WorkspaceLineageError<S::Error>>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        S: WorkspaceLineageStore + Clone,
    {
        let record = self.lineage().resolve(workspace_id).await?;
        let workspace = self.fs.open_workspace(&record.workspace_name).await?;
        if workspace.id() != workspace_id {
            return Err(WorkspaceLineageError::IncompatibleState);
        }
        Ok(workspace)
    }

    /// Constructs the durable recursive context handle.
    pub fn contexts(&self) -> WorkspaceContextRegistry<S>
    where
        S: WorkspaceContextStore + Clone,
    {
        WorkspaceContextRegistry::new(self.store.clone())
    }

    /// Constructs the authenticated direct-workspace lineage handle.
    pub fn lineage(&self) -> WorkspaceGraph<S>
    where
        S: WorkspaceLineageStore + Clone,
    {
        WorkspaceGraph::new(self.store.clone())
    }

    /// Constructs the durable overlapping-operation coordinator.
    pub fn operations(&self) -> OperationWindowCoordinator<S>
    where
        S: OperationWindowStore + Clone,
    {
        OperationWindowCoordinator::new(self.store.clone())
    }

    /// Constructs one root's Git-shaped compatibility view.
    pub fn git(&self, workspace_id: WorkspaceId) -> GitCompatRepository<S>
    where
        S: Clone,
    {
        GitCompatRepository::new(workspace_id, self.store.clone())
    }

    /// Attaches an unresolved source without enumerating or reading its tree.
    pub async fn attach_lazy<D: DemandSource + 'static>(
        &self,
        name: impl AsRef<str>,
        source: Arc<D>,
    ) -> Result<LazyWorkspace<A, O, D, S>, LazyWorkspaceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        S: LazyWorkspaceStore + Clone,
    {
        LazyWorkspace::attach(&self.fs, name, source, self.store.clone()).await
    }

    /// Reopens an existing workspace against its unresolved source.
    pub async fn open_lazy<D: DemandSource + 'static>(
        &self,
        workspace: Workspace<A, O>,
        source: Arc<D>,
    ) -> Result<LazyWorkspace<A, O, D, S>, LazyWorkspaceError>
    where
        A: AsyncAuthorityStore,
        O: AsyncObjectStore,
        S: LazyWorkspaceStore + Clone,
    {
        LazyWorkspace::open(workspace, source, self.store.clone()).await
    }

    /// Composes the SDK-owned all-roots publication state machine.
    pub fn publications<P: MultiRootPublisher, R: MultiRootPublicationAuthorizer>(
        &self,
        publisher: P,
        authorizer: R,
    ) -> MultiRootPublicationCoordinator<S, P, R>
    where
        S: MultiRootPublicationStore + Clone,
    {
        MultiRootPublicationCoordinator::new(self.store.clone(), publisher, authorizer)
    }
}

impl<A, O, S> WorkspaceResolver<A, O> for DistributedFs<A, O, S>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    S: WorkspaceLineageStore + Clone,
{
    type Error = WorkspaceLineageError<S::Error>;

    async fn resolve(&self, workspace_id: WorkspaceId) -> Result<Workspace<A, O>, Self::Error> {
        self.workspace(workspace_id).await
    }
}
