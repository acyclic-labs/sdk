use super::*;

impl CheckoutTools {
    pub(super) fn child_project<'a>(
        &self,
        context: &'a ToolContext,
    ) -> Result<Option<&'a VolumeRef>> {
        let scope = context.effective_scope();
        if scope.grants().contains(&self.read_grant) && scope.grants().contains(&self.write_grant) {
            return Ok(None);
        }
        workspace::created_project(context.task(), scope).map(Some)
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
                let view = self.workspace.project_view(project).await?;
                let mounts = self
                    .workspace
                    .created_mounts(project, context.task(), context.effective_scope())
                    .await?;
                let mut tools = self.mount_task_view(view, owner, Some(mounts)).await?;
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
