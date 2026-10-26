//! Authenticated child allocation shared by workspace clients and executors.
use super::*;
use crate::runtime::{RuntimeScope, TaskContext};

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
    /// Retains the current cut of the lazy `source` project for the fork
    /// `destination` that a spawn commit creates under `operation`: step one
    /// of the external-creator fork. Call it right before that spawn, after
    /// capturing `source`, so the commit forks the cut's head.
    pub(in crate::local) async fn prepare_spawned_project(
        &self,
        source: &VolumeRef,
        destination: &VolumeRef,
        operation: acyclic_fs::OperationId,
    ) -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let view = self.project_view(source).await?;
        let mut accounting = acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation);
        let cut = view.snapshot(&mut accounting).await.map_err(storage)?;
        view.prepare_fork_snapshot(
            &destination.storage_name(&mut None)?,
            cut,
            operation,
            &mut accounting,
        )
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// The lazy view of spawned project `project`, first binding its
    /// prepared fork to the spawn's creating generation (step two) if no
    /// open has yet; idempotent, and O(1) once bound.
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
        let Some(origin) = state
            .fork_origin
            .filter(|origin| origin.phase == acyclic_fs::LazyForkPhase::Prepared)
        else {
            return self
                .view()
                .open_related(child, &mut accounting)
                .await
                .map_err(storage);
        };
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
        let (view, _) = source
            .bind_prepared_fork(
                origin.snapshot,
                child,
                original,
                origin.operation_id,
                &mut accounting,
            )
            .await
            .map_err(storage)?;
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

pub(in crate::local) fn created_project<'a>(
    task: &'a TaskContext,
    scope: &RuntimeScope,
) -> Result<&'a VolumeRef> {
    let owner = task
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("child workspace has no original owner".into()))?;
    let (child, fence) = owner.task_binding();
    if !task.original_task_binding_matches(child, fence) {
        return Err(Error::Unauthorized("child workspace owner differs".into()));
    }
    let access = task.scope().created_access.as_ref().ok_or_else(|| {
        Error::Unauthorized("workspace has no authenticated child allocation".into())
    })?;
    if access.operation.into_bytes() != child.into_bytes() {
        return Err(Error::Unauthorized(
            "child workspace seal belongs to another task".into(),
        ));
    }
    let mut selected = None;
    for binding in &access.content {
        if binding.volume.class() == VolumeClass::Project
            && [VolumeOperation::Read, VolumeOperation::Write]
                .into_iter()
                .map(|operation| binding.volume.capability(operation, &mut None))
                .collect::<Result<Vec<_>>>()?
                .iter()
                .all(|grant| scope.grants().contains(grant))
            && selected.replace(&binding.volume).is_some()
        {
            return Err(Error::Unauthorized(
                "child project binding is ambiguous".into(),
            ));
        }
    }
    selected.ok_or_else(|| Error::Unauthorized("child project is not writable".into()))
}
