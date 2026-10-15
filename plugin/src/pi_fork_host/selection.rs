use super::*;

impl crate::pi_host::PiControlHost<'_> {
    pub(super) async fn source(
        &self,
        shared: &Arc<TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        caller: &PiTaskInput,
        command: &PiForkCommand,
        plan: Option<&PiForkPlan>,
        work: &NativeMaterializationWork,
    ) -> Result<(
        Arc<crate::LocalLazyWorkspace>,
        LazySnapshotRef,
        Option<acyclic_fs::NativeAllocation>,
    )> {
        let root_key = command
            .commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi fork has no selected project"))?;
        if command.commit.child.project_volumes.len() != 1 {
            return Err(invalid(
                "Pi fork requires exactly its actually selected project",
            ));
        }
        crate::pi_inspection::authorize_selected_source(
            &self.runtime.runtime,
            self.protected.policy().root_task,
            shared.as_ref(),
            owner,
            command.commit.source_task,
            self.protected.policy().session_limits.depth,
            work,
        )
        .await?;
        let root = command.commit.source_task == self.protected.policy().root_task;
        let actual =
            if root {
                Some(self.roots.get(root_key).ok_or_else(|| {
                    invalid("Pi original selected root has no actual lazy handle")
                })?)
            } else {
                None
            };
        let source = if let Some(actual) = actual {
            Arc::clone(&actual.workspace)
        } else {
            self.native()?
                .live_workspace(command.commit.source_task)
                .ok_or_else(|| {
                    invalid("Pi selected child has no actual reopened creator workspace")
                })?
        };
        if let Some(frozen) = plan
            .map(|plan| plan.frozen)
            .or(command.root_source.filter(|_| root))
        {
            source
                .validate_snapshot(
                    frozen,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: work.cancellation(),
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            return Ok((source, frozen, None));
        }
        let (frozen, custody) = if let Some(actual) = actual {
            if owner.task_id() != self.protected.policy().root_task {
                return Err(invalid(
                    "Pi selected Root requires its real original Root observe control receipt",
                ));
            }
            self.freeze_root_source(shared, owner, caller, root_key, actual, work)
                .await?
        } else {
            let frozen = self
                .native()?
                .selection_snapshot(command.commit.source_task, work)
                .await?;
            owner
                .verify(
                    false,
                    &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                        work,
                    )?)),
                )
                .await?;
            frozen
        };
        Ok((source, frozen, Some(custody)))
    }
}
