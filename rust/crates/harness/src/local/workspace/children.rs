//! A project volume's view related to this checkout.
use super::*;

impl LocalWorkspace {
    async fn project_view(&self, project: &str) -> Result<View> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let project_workspace = self
            .filesystem
            .open_workspace(project, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
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
        source: &str,
        destination: &str,
        operation: acyclic_fs::OperationId,
        generation: Option<acyclic_fs::GenerationId>,
    ) -> std::result::Result<Option<acyclic_fs::GenerationId>, crate::filesystem::ForkCutError>
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        let retained = self
            .store
            .load_lazy_workspace(
                self.filesystem.workspace_id(destination).map_err(storage)?,
                &mut accounting,
            )
            .await
            .map_err(storage)?
            .and_then(|state| state.fork_origin)
            .filter(|origin| origin.operation_id == operation);
        if let Some(origin) = retained {
            require_selected_generation(generation, origin.snapshot.authored_generation)?;
            return Ok(Some(origin.snapshot.authored_generation));
        }
        let Some(state) = self
            .store
            .load_lazy_workspace(
                self.filesystem.workspace_id(source).map_err(storage)?,
                &mut accounting,
            )
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        // A spawned fork serves no reads until bound, so a child's own spawn
        // binds it (idempotently) and forks what that child sees.
        let view = match state.fork_origin {
            Some(_) => self.spawned_project_view_named(source).await?,
            None => self.project_view(source).await?,
        };
        let cut = view.snapshot(&mut accounting).await.map_err(storage)?;
        require_selected_generation(generation, cut.authored_generation)?;
        view.prepare_fork_snapshot(destination, cut, operation, &mut accounting)
            .await
            .map_err(storage)?;
        Ok(Some(cut.authored_generation))
    }

    /// The lazy view of spawned project `project`, first binding its
    /// prepared fork to the spawn's creating generation (step two) if no
    /// open has yet, and registering it in the workspace graph; idempotent
    /// and O(1).
    pub(in crate::local) async fn spawned_project_view(&self, project: &VolumeRef) -> Result<View> {
        let name = project.storage_name(&mut None)?;
        self.spawned_project_view_named(&name).await
    }

    async fn spawned_project_view_named(&self, name: &str) -> Result<View> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let child = self
            .filesystem
            .open_workspace(name, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
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
                name,
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

fn require_selected_generation(
    selected: Option<acyclic_fs::GenerationId>,
    actual: acyclic_fs::GenerationId,
) -> std::result::Result<(), crate::filesystem::ForkCutError> {
    if selected.is_some_and(|selected| selected != actual) {
        return Err(crate::filesystem::ForkCutError::SourceChanged);
    }
    Ok(())
}

/// Spawned forks of this checkout's lazy volumes fork their retained cut.
pub(in crate::local) struct LazyForkCuts(pub(in crate::local) Arc<LocalWorkspace>);

impl crate::filesystem::ForkCuts for LazyForkCuts {
    fn cut<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
        operation: acyclic_fs::OperationId,
        generation: Option<acyclic_fs::GenerationId>,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        std::result::Result<Option<acyclic_fs::GenerationId>, crate::filesystem::ForkCutError>,
    > {
        Box::pin(
            self.0
                .spawned_fork_cut(source, destination, operation, generation),
        )
    }
}

impl LocalWorkspace {
    /// Registers each `agents/<name>` that `arguments` name as the branch of
    /// `agent`'s spawned child `name`, when `agent` binds this checkout's
    /// project: that child's project is a lazy fork of this checkout, bound
    /// into the workspace graph (if nothing bound it earlier) and registered
    /// the first time its parent names it, in the same turn that spawned it.
    /// A registered branch is left as it is; one discarded and named again is
    /// registered again, since discarding retires only this repository's
    /// reference to the child's fork.
    pub(in crate::local) async fn register_named_children(
        &self,
        agent: &crate::agent::AgentPath,
        volumes: &[crate::agent::VolumeBinding],
        arguments: &mut [String],
    ) -> Result<()> {
        let Some(child) =
            crate::tools::resolve_child_branch(agent, arguments, crate::tools::spawned_child)?
        else {
            return Ok(());
        };
        let branch = child.as_str();
        let repository = self.view().workspace().id();
        let mut project = None;
        for binding in volumes {
            let id = self
                .filesystem
                .workspace_id(&binding.volume.storage_name(&mut None)?)
                .map_err(storage)?;
            if id == repository {
                project = Some(binding);
                break;
            }
        }
        let Some(project) = project else {
            return Ok(());
        };
        let registered = acyclic_fs::GitCompatStore::load(&self.store, repository)
            .await
            .map_err(storage)?
            .map(|state| state.branches)
            .unwrap_or_default();
        let cancellation = acyclic_fs::CancellationToken::new();
        let volume = crate::filesystem::spawned_binding(&child, project).map_err(storage)?;
        let id = self
            .filesystem
            .workspace_id(&volume.storage_name(&mut None)?)
            .map_err(storage)?;
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        let forked = self
            .store
            .load_lazy_workspace(id, &mut accounting)
            .await
            .map_err(storage)?
            .is_some_and(|state| {
                state.fork_origin.is_some() && state.parent_workspace_id == Some(repository)
            });
        if !forked {
            return Ok(());
        }
        if let Some(existing) = registered.get(branch) {
            if existing.workspace_id != id {
                return Err(Error::Conflict(
                    "child branch belongs to another workspace".into(),
                ));
            }
            return Ok(());
        }
        // Bound first, so the branch names a workspace that serves reads;
        // a stop between the two steps repeats both idempotently.
        self.spawned_project_view(&volume).await?;
        acyclic_fs::GitCompatRepository::new(repository, self.store.clone())
            .register_branch_workspace(branch, id, None, false)
            .await
            .map_err(storage)?;
        Ok(())
    }
}
