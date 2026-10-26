//! A project volume's view related to this checkout.
use super::*;

impl LocalWorkspace {
    pub(in crate::local) async fn project_view(&self, project: &VolumeRef) -> Result<View> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let project_workspace = self
            .filesystem
            .open_workspace(
                project.storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        self.view()
            .open_related(
                project_workspace,
                &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await
            .map_err(storage)
    }
}

impl LocalWorkspace {
    /// The cut of lazy `source` that spawned fork `destination` forks under
    /// creation `operation` (step one of the external-creator fork): the cut
    /// a previous attempt retained, else `source`'s current cut, retained
    /// now. `None` when `source` keeps no lazy state.
    async fn spawned_fork_cut(
        &self,
        source: &VolumeRef,
        destination: &VolumeRef,
        operation: acyclic_fs::OperationId,
    ) -> Result<Option<acyclic_fs::GenerationId>> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        let destination = destination.storage_name(&mut None)?;
        let retained = self
            .store
            .load_lazy_workspace(
                self.filesystem
                    .workspace_id(&destination)
                    .map_err(storage)?,
                &mut accounting,
            )
            .await
            .map_err(storage)?
            .and_then(|state| state.fork_origin)
            .filter(|origin| origin.operation_id == operation);
        if let Some(origin) = retained {
            return Ok(Some(origin.snapshot.authored_generation));
        }
        let lazy = self
            .store
            .load_lazy_workspace(
                self.filesystem
                    .workspace_id(&source.storage_name(&mut None)?)
                    .map_err(storage)?,
                &mut accounting,
            )
            .await
            .map_err(storage)?
            .is_some();
        if !lazy {
            return Ok(None);
        }
        let view = self.project_view(source).await?;
        let cut = view.snapshot(&mut accounting).await.map_err(storage)?;
        view.prepare_fork_snapshot(&destination, cut, operation, &mut accounting)
            .await
            .map_err(storage)?;
        Ok(Some(cut.authored_generation))
    }

    /// The lazy view of spawned project `project`, first binding its
    /// prepared fork to the spawn's creating generation (step two) if no
    /// open has yet, and registering it in the workspace graph; idempotent
    /// and O(1).
    pub(in crate::local) async fn spawned_project_view(&self, project: &VolumeRef) -> Result<View> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let name = project.storage_name(&mut None)?;
        let child = self
            .filesystem
            .open_workspace(&name, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        let state = self
            .store
            .load_lazy_workspace(child.id(), &mut accounting)
            .await
            .map_err(storage)?
            .ok_or_else(|| Error::NotFound("spawned project has no prepared fork".into()))?;
        let origin = state
            .fork_origin
            .ok_or_else(|| Error::Conflict("spawned project has no fork origin".into()))?;
        let parent = state
            .parent_workspace_id
            .ok_or_else(|| Error::Conflict("spawned project fork has no parent".into()))?;
        let parent = acyclic_fs::WorkspaceResolver::resolve(
            &(
                self.filesystem.clone(),
                acyclic_fs::WorkspaceGraph::new(self.store.clone()),
            ),
            parent,
        )
        .await
        .map_err(storage)?;
        let source = self
            .view()
            .open_related(parent, &mut accounting)
            .await
            .map_err(storage)?;
        let original = self
            .filesystem
            .workspace_for_creation_operation(
                &name,
                child.volume_config(),
                origin.operation_id,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
            .ok_or_else(|| Error::Conflict("spawned project has no creating generation".into()))?
            .id();
        // Binding and registration are each idempotent; an open after a
        // stop between them completes both.
        let view = if origin.phase == acyclic_fs::LazyForkPhase::Prepared {
            source
                .bind_prepared_fork(
                    origin.snapshot,
                    child,
                    original,
                    origin.operation_id,
                    &mut accounting,
                )
                .await
                .map_err(storage)?
                .0
        } else {
            self.view()
                .open_related(child, &mut accounting)
                .await
                .map_err(storage)?
        };
        acyclic_fs::WorkspaceGraph::new(self.store.clone())
            .register_existing_child_at_generation(
                source.workspace(),
                view.workspace(),
                origin.snapshot.authored_generation,
                original,
            )
            .await
            .map_err(storage)?;
        Ok(view)
    }
}

/// Spawned forks of this checkout's lazy volumes fork their retained cut.
pub(in crate::local) struct LazyForkCuts(pub(in crate::local) Arc<LocalWorkspace>);

impl crate::filesystem::ForkCuts for LazyForkCuts {
    fn cut<'a>(
        &'a self,
        source: &'a VolumeRef,
        destination: &'a VolumeRef,
        operation: acyclic_fs::OperationId,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<acyclic_fs::GenerationId>>> {
        Box::pin(self.0.spawned_fork_cut(source, destination, operation))
    }
}
