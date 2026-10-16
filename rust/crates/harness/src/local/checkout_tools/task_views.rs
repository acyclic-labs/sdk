//! Both model and direct calls select the admitted task's sparse fork.
use super::*;
use acyclic_fs::{
    CancellationToken, IdempotencyKey, MountOptions, WorkBudget, kernel::KernelAccounting,
};

impl CheckoutTools {
    pub(super) async fn select(&self, context: &ToolContext) -> Result<Option<Arc<Self>>> {
        let owner = context.task().ordinary_journal_owner::<LocalStream>()?;
        owner.verify(false, &mut None).await?;
        let task = if owner.admission().task.name == "acyclic.direct_tool" {
            owner.admission().parent.unwrap_or_else(|| owner.task_id())
        } else {
            owner.task_id()
        };
        let mut lineage = context.task().lineage(task).await?;
        lineage.reverse();
        // A root task edits the owner's checkout. Every descendant has its own
        // durable lazy fork. This cache owns mounts, never authoritative state.
        let mut parent = Arc::clone(&self.workspace);
        serialized(&self.state, async |mut state| {
            let mut selected = None;
            for task in lineage.into_iter().skip(1) {
                let tools = cached_task(&mut state.children, task, async || {
                    let cancellation = CancellationToken::new();
                    let work = acyclic_fs::NativeMaterializationWork::new(
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                    let mut accounting = KernelAccounting::NativeOriginal {
                        work: &work,
                        cancellation: &cancellation,
                    };
                    let name = format!("task-{task}");
                    let (view, _) = parent
                        .view()
                        .fork(
                            &name,
                            IdempotencyKey::from_bytes(task.into_bytes()),
                            &mut accounting,
                        )
                        .await
                        .map_err(storage)?;
                    let path = self.workspace.mount_directory.join(task.to_string());
                    std::fs::create_dir_all(&path).map_err(storage)?;
                    let mounted = Arc::new(
                        view.mount(
                            &path,
                            MountOptions::read_write()
                                .publication(acyclic_fs::MountPublication::Manual),
                        )
                        .await
                        .map_err(storage)?,
                    );
                    let identity = acyclic_fs::capture_root_identity(&path).map_err(storage)?;
                    let volume = self
                        .mounts
                        .mounts()
                        .next()
                        .ok_or_else(|| Error::Invalid("project mount missing".into()))?;
                    let workspace = Arc::new(LocalWorkspace {
                        path: path.clone(),
                        mount_directory: self.workspace.mount_directory.clone(),
                        view,
                        mounted: Some(mounted),
                        identity,
                        filesystem: parent.filesystem.clone(),
                        store: parent.store.clone(),
                        mounts: Arc::new(workspace::mounts::owner(&path, &volume.1.view.volume)?),
                    });
                    let (capture_options, capture_policy) = capture::configuration(&workspace)?;
                    Ok(Self {
                        workspace: Arc::clone(&workspace),
                        read_grant: self.read_grant.clone(),
                        write_grant: self.write_grant.clone(),
                        capture_options,
                        capture_policy,
                        clock: Arc::clone(&self.clock),
                        mounts: Arc::clone(&workspace.mounts),
                        task_files: Arc::clone(&self.task_files),
                        state: StateArc::new(AsyncMutex::new(State {
                            active: None,
                            watcher: None,
                            children: Default::default(),
                        })),
                        selected: true,
                    })
                })
                .await?;
                parent = Arc::clone(&tools.workspace);
                selected = Some(tools);
            }
            Ok(selected)
        })
        .await
    }
}

// The caller's owned guard keeps lookup, construction and installation atomic.
pub(super) async fn cached_task<T>(
    cache: &mut std::collections::BTreeMap<TaskId, Arc<T>>,
    task: TaskId,
    create: impl AsyncFnOnce() -> Result<T>,
) -> Result<Arc<T>> {
    if let Some(value) = cache.get(&task) {
        return Ok(Arc::clone(value));
    }
    let value = Arc::new(create().await?);
    cache.insert(task, Arc::clone(&value));
    Ok(value)
}
