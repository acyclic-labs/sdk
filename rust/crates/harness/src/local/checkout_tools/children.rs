use super::*;
use crate::conversation::VolumeClass;
use acyclic_fs::facade::FsWork as _;

impl CheckoutTools {
    pub(super) fn child_project<'a>(
        &self,
        context: &'a ToolContext,
    ) -> Result<Option<&'a VolumeRef>> {
        let scope = context.effective_scope();
        if scope.grants().contains(&self.read_grant) && scope.grants().contains(&self.write_grant) {
            return Ok(None);
        }
        let task = context.task();
        let owner = task
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("child workspace has no original owner".into()))?;
        let (child, fence) = owner.task_binding();
        if !task.original_task_binding_matches(child, fence) {
            return Err(Error::Unauthorized("child workspace owner differs".into()));
        }
        let access = context
            .task()
            .scope()
            .created_access
            .as_ref()
            .ok_or_else(|| {
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
            {
                if selected.replace(&binding.volume).is_some() {
                    return Err(Error::Unauthorized(
                        "child project binding is ambiguous".into(),
                    ));
                }
            }
        }
        selected
            .map(Some)
            .ok_or_else(|| Error::Unauthorized("child project is not writable".into()))
    }

    pub(super) async fn child_checkout(
        &self,
        context: &ToolContext,
        project: &VolumeRef,
    ) -> Result<Arc<Self>> {
        let (task, _) = context.task().owned_task_binding(None)?;
        let owner =
            context.task().owned_mailbox.as_ref().ok_or_else(|| {
                Error::Unauthorized("child checkout has no original owner".into())
            })?;
        // The shared cache guard pins construction and publication of one
        // native binding. Owner and mount callbacks never reacquire this guard.
        serialized(&self.state, async |mut state| {
            task_views::cached_task(&mut state.children, task, None, &(), async |_| {
                owner.verify_owner(None)?.await?;
                let cancellation = acyclic_fs::CancellationToken::new();
                let project_workspace = self
                    .workspace
                    .filesystem
                    .open_workspace(
                        project.storage_name(&mut None)?,
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(storage)?
                    .value;
                let view = self
                    .workspace
                    .view()
                    .open_related(
                        project_workspace,
                        &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
                    )
                    .await
                    .map_err(storage)?;
                let (mounts, signed) = self.workspace.created_mounts(project, context).await?;
                access::require_mount_access(
                    view.workspace(),
                    &mounts,
                    &signed,
                    context.effective_scope().grants(),
                    "child checkout",
                )
                .await?;
                let mut tools = self
                    .mount_task_view(view, owner, Some(mounts), &signed)
                    .await?;
                tools.read_grant = project.capability(VolumeOperation::Read, &mut None)?;
                tools.write_grant = project.capability(VolumeOperation::Write, &mut None)?;
                tools.task_files = Arc::new(
                    self.task_files
                        .for_created_task(context.task(), context.effective_scope())
                        .await?
                        .ok_or_else(|| {
                            Error::Unauthorized("child private files are absent".into())
                        })?,
                );
                owner.verify_owner(None)?.await?;
                Ok(tools)
            })
            .await
        })
        .await
    }
}
