//! Immutable project refs remain readable across selected branch views.
use super::*;
use acyclic_fs::{WorkspaceGraph, WorkspaceId, WorkspaceLineageStore};

async fn authorize<S: WorkspaceLineageStore>(
    graph: &WorkspaceGraph<S>,
    root: WorkspaceId,
    owner: WorkspaceId,
) -> Result<()> {
    if owner == root {
        return Ok(());
    }
    let ancestors = graph
        .ancestors(owner, u32::MAX)
        .await
        .map_err(|error| match error {
            acyclic_fs::WorkspaceLineageError::Store(error) => Error::Storage(error.to_string()),
            _ => Error::Unauthorized("generation has no valid project ancestry".into()),
        })?;
    if ancestors
        .iter()
        .any(|ancestor| ancestor.workspace_id == root)
    {
        Ok(())
    } else {
        Err(Error::Unauthorized(
            "generation belongs to another project family".into(),
        ))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) async fn project_generation(
        &self,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        root: WorkspaceId,
        store: &acyclic_fs::LocalCoreStateStore,
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
