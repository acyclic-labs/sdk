//! One authenticated frozen Project selection.
use super::*;

impl PiControlHost<'_> {
    pub(crate) async fn selected(
        &self,
        context: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        task: TaskId,
        requested: Option<&str>,
        pinned: Option<LazySnapshotRef>,
        operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<PiNativeSelectedSource> {
        let root = self.protected.policy().root_task;
        authorize_selected_source(
            &self.runtime.runtime,
            root,
            context,
            owner,
            task,
            self.protected.policy().session_limits.depth,
            work,
        )
        .await?;
        let input = self.admission_input(task, work).await?;
        let mut key = work.display(self.project_key(&input.0.binding, requested)?)?;
        let project = &input.0.binding.project_volumes[&key.0];
        let workspace = if task == root {
            Arc::clone(
                &self
                    .roots
                    .get(&key.0)
                    .ok_or_else(|| denied("selected Root lost its creator workspace"))?
                    .workspace,
            )
        } else {
            self.native()?
                .live_workspace(task)
                .ok_or_else(|| denied("selected child lost its creator workspace"))?
        };
        let (snapshot, frozen_custody) = if let Some(snapshot) = pinned {
            (snapshot, None)
        } else if task != root {
            let (snapshot, custody) = self.native()?.selection_snapshot(task, work).await?;
            (snapshot, Some(custody))
        } else if owner.task_id() == task {
            let (snapshot, custody) = crate::pi_fork_host::freeze_created_root_source(
                Arc::clone(&self.protected),
                self.resources,
                Arc::clone(&self.filesystem),
                self.stream.clone(),
                context,
                owner,
                &input.0,
                &key.0,
                &self.roots[&key.0],
                work,
            )
            .await?;
            (snapshot, Some(custody))
        } else {
            (
                self.root_source
                    .filter(|source| source.root_key == key.0)
                    .ok_or_else(|| {
                        denied("foreign Root source needs its original observe-source ACK")
                    })?
                    .snapshot,
                None,
            )
        };
        let handle = self
            .coordinator(work)?
            .authorize_selected_project_with_native_work(
                context,
                owner,
                task,
                Arc::clone(&workspace),
                snapshot,
                operation,
                work.remaining()?.items_examined,
                self.protected.policy().limits.file_bytes,
                work,
            )
            .await?;
        let mut project_cut = crate::pi_fork::project_snapshot(
            project,
            snapshot,
            self.protected.policy().maximum_payload_bytes,
            work,
        )?;
        let inspection = self
            .runtime
            .runtime
            .inspect_project_source_with_native_work(
                context,
                Arc::clone(owner),
                handle,
                operation,
                task,
                &project_cut.0,
                work,
            )
            .await?;
        if let Some(view) = self.native()?.live_view(task) {
            let mut observed = view
                .inspect_project_snapshot_with_native_work(&inspection, snapshot, work)?
                .await?;
            if observed.1 != snapshot {
                return Err(denied("inspection changed its authorized frozen source"));
            }
            project_cut.1.absorb(&mut observed.2)?;
        } else if task != root {
            let mut custody = self
                .native()?
                .validate_published_selection_snapshot(task, &inspection, project, snapshot, work)
                .await?;
            project_cut.1.absorb(&mut custody)?;
        }
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        let name = project.storage_name(&mut Some(&mut io))?;
        if name != workspace.workspace().name().as_str()
            || snapshot.workspace_id != workspace.workspace().id()
        {
            return Err(denied("selected snapshot names another allocation"));
        }
        project_cut.1.absorb(&mut key.1)?;
        Ok(PiNativeSelectedSource {
            key: key.0,
            workspace,
            context: Arc::clone(context),
            owner: Arc::clone(owner),
            volume: project_cut.0.project,
            snapshot,
            inspection,
            _custody: (project_cut.1, frozen_custody),
        })
    }
}
