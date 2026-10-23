//! Both model and direct calls select the admitted task's sparse fork.
use super::*;
use acyclic_fs::{
    CancellationToken, LazyWorkspaceStore as _, WorkBudget, kernel::KernelAccounting,
};

impl CheckoutTools {
    pub(super) async fn select(&self, context: &ToolContext) -> Result<Option<Arc<Self>>> {
        let (task, _) = context.task().owned_task_binding(None)?;
        let owner = context
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("tool task owner missing".into()))?;
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
        let (ancestor, binding) = serialized(&self.state, async |mut state| {
            if let Some(tools) = state.children.get(&task) {
                return Ok((task, Some(Arc::clone(tools))));
            }
            let cancellation = CancellationToken::new();
            let work =
                acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let mut accounting = KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            };
            let ancestors = context.task().ancestors(task)?;
            futures::pin_mut!(ancestors);
            let mut root = task;
            while let Some(candidate) = futures::TryStreamExt::try_next(&mut ancestors).await? {
                root = candidate;
                if let Some(tools) = state.children.get(&candidate) {
                    return Ok((candidate, Some(Arc::clone(tools))));
                }
                let id = self
                    .workspace
                    .filesystem
                    .workspace_id(&format!("task-{candidate}"))
                    .map_err(storage)?;
                if self
                    .workspace
                    .store
                    .load_lazy_workspace(id, &mut accounting)
                    .await
                    .map_err(storage)?
                    .is_none()
                {
                    continue;
                }
                let view = self
                    .workspace
                    .fork_task(candidate, self.workspace.view(), owner)
                    .await?;
                let tools = Arc::new(self.mount_task_view(view, owner, None).await?);
                state.children.insert(candidate, Arc::clone(&tools));
                return Ok((candidate, Some(tools)));
            }
            Ok::<_, Error>((root, None))
        })
        .await?;
        if ancestor == task {
            return Ok(binding);
        }
        // Release the owner index before acquiring the ancestor's state. Its
        // guard pins the active branch through the durable, idempotent fork.
        let view = if let Some(parent) = binding {
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
        // Recheck under the owner index: concurrent selectors may have opened
        // this durable fork already. Only the winner constructs a native mount.
        serialized(&self.state, async |mut state| {
            cached_task(&mut state.children, task, None, &(), async |_| {
                self.mount_task_view(view, owner, None).await
            })
            .await
            .map(Some)
        })
        .await
    }
    pub(super) async fn mount_task_view(
        &self,
        view: workspace::View,
        owner: &Arc<dyn crate::runtime::OwnedTaskMailbox>,
        mounts: Option<Arc<workspace::Mounts>>,
    ) -> Result<Self> {
        let workspace = self
            .workspace
            .mount_task_view(view, owner, mounts, &self.workspace.mount_directory)
            .await?;
        let (capture_options, capture_policy) = capture::configuration(&workspace)?;
        Ok(Self {
            workspace: Arc::clone(&workspace),
            read_grant: self.read_grant.clone(),
            write_grant: self.write_grant.clone(),
            capture_options,
            capture_policy,
            clock: Arc::clone(&self.clock),
            task_files: Arc::clone(&self.task_files),
            state: StateArc::clone(&workspace.state),
            selected: true,
        })
    }
    pub(super) async fn selected_workspace(
        &self,
        state: &mut State,
        owner: &dyn crate::runtime::OwnedTaskMailbox,
    ) -> Result<Arc<LocalWorkspace>> {
        self.workspace.selected_workspace(state, owner).await
    }
}

// The caller's owned guard keeps lookup, construction and installation atomic.
pub(super) async fn cached_task<T, V>(
    cache: &mut std::collections::BTreeMap<TaskId, Arc<T>>,
    task: TaskId,
    active: Option<&V>,
    original: &V,
    create: impl AsyncFnOnce(&V) -> Result<T>,
) -> Result<Arc<T>> {
    if let Some(value) = cache.get(&task) {
        return Ok(Arc::clone(value));
    }
    let value = Arc::new(from_selected(active, original, create).await?);
    cache.insert(task, Arc::clone(&value));
    Ok(value)
}

async fn from_selected<T, V>(
    active: Option<&V>,
    original: &V,
    create: impl AsyncFnOnce(&V) -> Result<T>,
) -> Result<T> {
    create(active.unwrap_or(original)).await
}
