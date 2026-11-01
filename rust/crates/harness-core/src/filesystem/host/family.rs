//! Immutable project refs remain readable across selected branch views.
use super::*;
use acyclic_fs::{
    GitCompatRepository, GitCompatStore, WorkspaceGraph, WorkspaceId, WorkspaceLineageStore,
};

pub(super) trait ProjectFamily<A: 'static, O: 'static>:
    acyclic_stream::ProviderPlatform
{
    fn applies(&self, root: WorkspaceId) -> bool;
    fn selects_content(&self) -> bool;
    fn workspace<'a>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        root: &'a Workspace<A, O>,
        owner: WorkspaceId,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Workspace<A, O>>>;
    fn current<'a>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        root: &'a Workspace<A, O>,
        io: &'a PreparationIo,
    ) -> Result<Option<crate::conversation::BoundedContentFuture<'a, Workspace<A, O>>>>;
    fn generation<'a>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        workspace: &'a Workspace<A, O>,
        generation: GenerationId,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>>;
}

struct Family<S> {
    root: Option<WorkspaceId>,
    store: S,
}

impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    S: WorkspaceLineageStore + GitCompatStore + Clone,
> ProjectFamily<A, O> for Family<S>
{
    fn applies(&self, root: WorkspaceId) -> bool {
        self.root.is_none_or(|bound| bound == root)
    }

    fn selects_content(&self) -> bool {
        self.root.is_none()
    }

    fn workspace<'a>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        root: &'a Workspace<A, O>,
        owner: WorkspaceId,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Workspace<A, O>>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            io.check_cancellation()?;
            if owner == root.id() {
                return Ok(root.clone());
            }
            if io.original_native_work().is_some() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "project selection requires ordinary workspace execution",
                ));
            }
            let graph = WorkspaceGraph::new(self.store.clone());
            authorize(&graph, root.id(), owner).await?;
            let workspace =
                acyclic_fs::WorkspaceResolver::resolve(&(host.filesystem.clone(), graph), owner)
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
            io.check_cancellation()?;
            Ok(workspace)
        })
    }

    fn current<'a>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        root: &'a Workspace<A, O>,
        io: &'a PreparationIo,
    ) -> Result<Option<crate::conversation::BoundedContentFuture<'a, Workspace<A, O>>>> {
        // Physical hosts already bind their selected workspace. Only provider
        // clients obtain the current view from the shared durable repository.
        if self.root.is_some() {
            return Ok(None);
        }
        Ok(Some(crate::conversation::BoundedContentFuture::new(
            Some(io),
            || async move {
                io.check_cancellation()?;
                if io.original_native_work().is_some() {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Unsupported,
                        "project selection requires ordinary workspace execution",
                    ));
                }
                let repository = GitCompatRepository::new(root.id(), self.store.clone());
                let (selected, pending) = repository
                    .checkout_selection()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                io.check_cancellation()?;
                if pending.is_some() {
                    return Err(Error::Conflict(
                        "filesystem checkout requires its original publication reconciliation"
                            .into(),
                    ));
                }
                self.workspace(host, root, selected, io)?.await
            },
        )?))
    }

    fn generation<'a>(
        &'a self,
        host: &'a FilesystemHost<A, O>,
        workspace: &'a Workspace<A, O>,
        generation: GenerationId,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        crate::conversation::BoundedContentFuture::new(Some(io), || {
            host.project_generation(workspace, generation, workspace.id(), &self.store, io)
        })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Binds the original project's durable ancestry before sharing its host.
    #[doc(hidden)]
    pub fn with_project_family<S: WorkspaceLineageStore + GitCompatStore + Clone + 'static>(
        mut self: Arc<Self>,
        root: WorkspaceId,
        store: S,
    ) -> Result<Arc<Self>> {
        Arc::get_mut(&mut self)
            .ok_or_else(|| Error::Conflict("bind the project before sharing its host".into()))?
            .project_family = Some(Arc::new(Family {
            root: Some(root),
            store,
        }));
        Ok(self)
    }

    /// Binds stream-backed project selection and ancestry before sharing.
    /// Ordinary provider reads and writes follow each root's selected descendant;
    /// the root still supplies the original project authorization boundary.
    pub fn with_project_families<S: WorkspaceLineageStore + GitCompatStore + Clone + 'static>(
        mut self: Arc<Self>,
        store: S,
    ) -> Result<Arc<Self>> {
        Arc::get_mut(&mut self)
            .ok_or_else(|| Error::Conflict("bind project ancestry before sharing its host".into()))?
            .project_family = Some(Arc::new(Family { root: None, store }));
        Ok(self)
    }
}

async fn authorize<S: WorkspaceLineageStore>(
    graph: &WorkspaceGraph<S>,
    root: WorkspaceId,
    owner: WorkspaceId,
) -> Result<()> {
    graph
        .authorize_descendant(owner, root)
        .await
        .map_err(|error| match error {
            acyclic_fs::WorkspaceLineageError::Store(error) => Error::Storage(error.to_string()),
            _ => Error::Unauthorized("generation has no valid project ancestry".into()),
        })
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn select_current_workspace(
        &self,
        root: &Workspace<A, O>,
        io: &PreparationIo,
    ) -> Result<Workspace<A, O>> {
        if let Some(family) = &self.project_family
            && family.applies(root.id())
            && let Some(selected) = family.current(self, root, io)?
        {
            return selected.await;
        }
        Ok(root.clone())
    }

    pub(in crate::filesystem) async fn current_generation(
        &self,
        root: &Workspace<A, O>,
        io: &PreparationIo,
    ) -> Result<Generation<A, O>> {
        let workspace = self.select_current_workspace(root, io).await?;
        io.sdk(workspace.head(io, io.cancellation()).await)
    }

    async fn project_generation<S: WorkspaceLineageStore + Clone>(
        &self,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        root: WorkspaceId,
        store: &S,
        io: &PreparationIo,
    ) -> Result<Generation<A, O>> {
        let owner = io.sdk(
            self.filesystem
                .generation_workspace_id(
                    generation,
                    workspace.volume_config(),
                    io,
                    io.cancellation(),
                )
                .await,
        )?;
        let related;
        let workspace = if owner == root {
            workspace
        } else {
            if io.original_native_work().is_some() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unsupported,
                    "related project reads require ordinary workspace execution",
                ));
            }
            let graph = WorkspaceGraph::new(store.clone());
            authorize(&graph, root, owner).await?;
            related =
                acyclic_fs::WorkspaceResolver::resolve(&(self.filesystem.clone(), graph), owner)
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
            &related
        };
        io.sdk(
            workspace
                .generation(generation, io, io.cancellation())
                .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::{MemoryWorkspaceLineageStore, WorkspaceLineageRecord};
    use proptest::prelude::*;

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn stream_ancestry_preserves_pinned_branch_generations_and_refuses_foreign_roots()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let filesystem = acyclic_fs::Fs::memory();
        let cancellation = acyclic_fs::CancellationToken::new();
        let root = filesystem
            .create_workspace("project", acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let unrelated = filesystem
            .create_workspace(
                "unrelated",
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let store = acyclic_fs::StreamGitCompatStore::new(acyclic_fs::StreamAuthorityStore::new(
            filesystem.stream_provider(),
        ));
        let graph = WorkspaceGraph::new(store.clone());
        graph.register_root(&root).await?;
        graph.register_root(&unrelated).await?;
        let child = graph
            .fork(&root, "topic", acyclic_fs::IdempotencyKey::new())
            .await?;
        let pinned = child
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let unrelated_head = unrelated
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let provider =
            ProviderRef::new("memory".into(), "filesystem".into(), "2".into(), &mut None)?;
        let repository = GitCompatRepository::new(root.id(), store.clone());
        let host =
            FilesystemHost::new(filesystem, &provider, None)?.with_project_families(store)?;
        let io = PreparationIo::for_unmeasured(&cancellation)?;
        assert_eq!(
            host.current_generation(&root, &io).await?.workspace_id(),
            root.id()
        );
        repository
            .register_branch_workspace("topic", child.id(), None, true)
            .await?;
        assert_eq!(host.current_generation(&root, &io).await?.id(), pinned.id());
        repository
            .register_branch_workspace("main", root.id(), None, true)
            .await?;
        assert_eq!(
            host.current_generation(&root, &io).await?.workspace_id(),
            root.id()
        );
        let reopened = host.select_generation(&root, pinned.id(), &io).await?;
        assert_eq!(reopened.workspace_id(), child.id());
        assert_eq!(reopened.id(), pinned.id());
        assert!(matches!(
            host.select_generation(&root, unrelated_head.id(), &io)
                .await,
            Err(Error::Unauthorized(_))
        ));
        repository
            .register_branch_workspace("foreign", unrelated.id(), None, true)
            .await?;
        assert!(
            matches!(
                host.current_generation(&root, &io).await,
                Err(Error::Unauthorized(_))
            ),
            "a durable branch name cannot grant foreign project authority"
        );
        cancellation.cancel();
        assert!(host.current_generation(&root, &io).await.is_err());
        assert!(
            host.select_generation(&root, pinned.id(), &io)
                .await
                .is_err(),
            "cancelled ancestry resolution must end rather than wait"
        );
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // A pinned generation is authorized iff its owner is the project root
        // or a proven descendant. Sibling projects never inherit that grant.
        #[test]
        fn project_read_authority_follows_only_durable_ancestry(depth in 0u8..8, foreign in any::<bool>(), missing in any::<bool>(), seed in any::<[u8; 16]>()) {
            futures::executor::block_on(async {
                let store = MemoryWorkspaceLineageStore::new();
                let id = |index: u8| { let mut bytes = seed; bytes[0] = index; WorkspaceId::from_bytes(bytes) };
                let generation = GenerationId::new(Digest::from_bytes([1; 32]));
                for index in 0..=depth + 1 {
                    let parent = (index > 1).then(|| id(index - 1));
                    let record = WorkspaceLineageRecord {
                        version: 1, revision: 1, ready: true,
                        workspace_id: id(index), workspace_name: format!("workspace-{index}"),
                        parent_workspace_id: parent,
                        parent_workspace_name: parent.map(|_| format!("workspace-{}", index - 1)),
                        fork_generation: generation, initial_generation: generation,
                    };
                    assert!(store.compare_and_swap(id(index), 0, record).await.unwrap());
                }
                let graph = WorkspaceGraph::new(store);
                let owner = if missing { id(9) } else if foreign { id(0) } else { id(depth + 1) };
                prop_assert_eq!(authorize(&graph, id(1), owner).await.is_ok(), !foreign && !missing);
                Ok(())
            })?;
        }
    }
}
