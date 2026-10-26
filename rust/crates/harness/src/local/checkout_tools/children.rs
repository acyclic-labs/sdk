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
    ) -> Result<child_views::ChildLease<Self>> {
        let (task, _) = context.task().owned_task_binding(None)?;
        let owner =
            context.task().owned_mailbox.as_ref().ok_or_else(|| {
                Error::Unauthorized("child checkout has no original owner".into())
            })?;
        // The child's own construction gate pins construction and publication
        // of one native binding; the parent's checkout guard is never taken.
        // Owner and mount callbacks never reacquire either.
        let create = async || {
            owner.verify_owner(None)?.await?;
            let view = self.workspace.project_view(project).await?;
            let (mounts, signed) = self
                .workspace
                .created_mounts(project, context.task(), context.effective_scope())
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
                    .ok_or_else(|| Error::Unauthorized("child private files are absent".into()))?,
            );
            owner.verify_owner(None)?.await?;
            Ok(tools)
        };
        child_views::ChildViews::acquire(
            &self.workspace.children,
            task,
            create,
            async |tools: &Self| tools.retire_view().await,
        )
        .await
    }
}

impl LocalWorkspace {
    /// Retires `task`'s child view once its turn settles, so an idle child
    /// holds no mount, callback threads or descriptors. A view still in use
    /// retires when its last call releases it, at the next settlement.
    pub(in crate::local) async fn settle_child(&self, task: TaskId) -> Result<()> {
        child_views::ChildViews::settle(&self.children, task, async |tools| {
            tools.retire_view().await
        })
        .await
    }

    // Detaches this view's native mount on birth-time credit, then removes
    // the directory it was mounted on, which no later mount reuses. Both
    // steps are idempotent, so a retry resumes wherever a failed one stopped.
    // Retirement preserves uncaptured data in the view's store, which the
    // next mount of this view recovers.
    fn retire_mount(&self) -> Result<()> {
        let Some(mount) = &self.mounted else {
            return Ok(());
        };
        mount.prepare_unmount_cleanup().map_err(storage)?;
        mount
            .retire(&acyclic_fs::CancellationToken::new())
            .map_err(storage)?;
        Ok(())
    }
}

impl CheckoutTools {
    // The caller holds this view's construction gate and no call holds the
    // view, so neither its state nor its mounts have another user. A failed
    // step leaves what it had not retired owned here, for the retry.
    pub(super) async fn retire_view(&self) -> Result<()> {
        let mut state = lock_owned(StateArc::clone(&self.state)).await;
        if let Some((_, selected)) = &state.workspace {
            selected.retire_mount()?;
        }
        state.workspace = None;
        drop(state);
        self.workspace.retire_mount()
    }
}
