//! Both model and direct calls select the admitted task's sparse fork.
use super::*;
use acyclic_fs::{
    CancellationToken, LazyWorkspaceStore as _, WorkBudget, kernel::KernelAccounting,
};

impl CheckoutTools {
    pub(super) async fn select(
        &self,
        context: &ToolContext,
    ) -> Result<Option<child_views::ChildLease<Self>>> {
        // An agent turn has no task owner: it runs in the selected checkout.
        let Some(owner) = context.task().owned_mailbox.as_ref() else {
            return Ok(None);
        };
        let (task, _) = context.task().owned_task_binding(None)?;
        owner.verify_owner(None)?.await?;
        let admission = owner.admission().0;
        let task = if admission.task.name == "acyclic.direct_tool" {
            admission.parent.unwrap_or(task)
        } else {
            task
        };
        if admission.parent.is_none() && task == owner.task_binding().0 {
            return Ok(None);
        }
        self.task_view(context, task, owner).await
    }

    // A task's view is constructed once, under its own gate in this owner's
    // index. Concurrent selectors of one task share it; other tasks and the
    // parent's running command never wait for it.
    async fn task_view(
        &self,
        context: &ToolContext,
        task: TaskId,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
    ) -> Result<Option<child_views::ChildLease<Self>>> {
        let views = &self.workspace.children;
        if let Some(view) = child_views::ChildViews::get(views, task) {
            return Ok(Some(view));
        }
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let mut accounting = KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let ancestors = context.task().ancestors(task)?;
        futures::pin_mut!(ancestors);
        let (mut found, mut root) = (None, task);
        while let Some(candidate) = futures::TryStreamExt::try_next(&mut ancestors).await? {
            root = candidate;
            let id = self
                .workspace
                .filesystem
                .workspace_id(format!("task-{candidate}"))
                .map_err(storage)?;
            if views.contains(candidate)
                || self
                    .workspace
                    .store
                    .load_lazy_workspace(id, &mut accounting)
                    .await
                    .map_err(storage)?
                    .is_some()
            {
                found = Some(candidate);
                break;
            }
        }
        let parent = match found {
            None if root == task => return Ok(None),
            Some(found) if found == task => {
                // A durable fork of this task exists: reopen it.
                return child_views::ChildViews::acquire(
                    views,
                    task,
                    async || {
                        let view = self
                            .workspace
                            .fork_task(task, self.workspace.view(), owner)
                            .await?;
                        self.mount_task_view(view, owner, None, &self.signed).await
                    },
                    async |tools: &Self| tools.retire_view().await,
                )
                .await
                .map(Some);
            }
            Some(found) => Some(
                Box::pin(self.task_view(context, found, owner))
                    .await?
                    .ok_or_else(|| Error::Conflict("task ancestor has no view".into()))?,
            ),
            None => None,
        };
        // Only this fork pins the parent's selected branch, under that branch
        // owner's guard, through the durable, idempotent fork.
        let create = async || {
            let view = if let Some(parent) = &parent {
                serialized(&parent.state, async |state| {
                    from_selected(
                        state.active.as_ref(),
                        parent.workspace.view(),
                        async |view| self.workspace.fork_task(task, view, owner).await,
                    )
                    .await
                })
                .await?
            } else {
                serialized(&self.state, async |state| {
                    from_selected(state.active.as_ref(), self.workspace.view(), async |view| {
                        self.workspace.fork_task(task, view, owner).await
                    })
                    .await
                })
                .await?
            };
            self.mount_task_view(view, owner, None, &self.signed).await
        };
        child_views::ChildViews::acquire(views, task, create, async |tools: &Self| {
            tools.retire_view().await
        })
        .await
        .map(Some)
    }

    pub(super) async fn mount_task_view(
        &self,
        view: workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
        mounts: Option<Arc<workspace::Mounts>>,
        signed: &Scope,
    ) -> Result<Self> {
        let workspace = self
            .workspace
            .mount_task_view(view, owner, mounts, signed)
            .await?;
        let (capture_options, capture_policy) = capture::configuration(&workspace)?;
        Ok(Self {
            signed: signed.clone(),
            workspace: Arc::clone(&workspace),
            read_grant: self.read_grant.clone(),
            write_grant: self.write_grant.clone(),
            capture_options,
            capture_policy,
            clock: Arc::clone(&self.clock),
            expiry_unix_ms: self.expiry_unix_ms,
            state: StateArc::clone(&workspace.state),
            selected: true,
        })
    }
    pub(super) async fn selected_workspace(
        &self,
        state: &mut State,
        owner: &dyn workspace::owner::ViewOwner,
    ) -> Result<Arc<LocalWorkspace>> {
        self.workspace.selected_workspace(state, owner).await
    }
}

pub(super) async fn from_selected<T, V>(
    active: Option<&V>,
    original: &V,
    create: impl AsyncFnOnce(&V) -> Result<T>,
) -> Result<T> {
    create(active.unwrap_or(original)).await
}
