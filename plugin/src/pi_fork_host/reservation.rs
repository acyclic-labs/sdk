use super::*;

impl crate::pi_host::PiControlHost<'_> {
    pub(super) fn parent_alias(&self, task: TaskId) -> Result<&str> {
        if task == self.protected.policy().root_task {
            return Ok(&self.retained.root_session_id);
        }
        self.retained
            .routes
            .values()
            .find_map(|route| match &route.task_binding {
                PluginTaskBinding::Pi(binding) if binding.task_id == task => {
                    Some(route.agent_id.as_str())
                }
                _ => None,
            })
            .ok_or_else(|| unauthorized("Pi original caller has no actual persisted route alias"))
    }

    pub(super) fn reservation_coordinates(
        &self,
        plan: &PiForkPlan,
        work: &NativeMaterializationWork,
    ) -> Result<(
        [u8; 16],
        (
            crate::pi_native::PiNativePaths,
            acyclic_fs::NativeAllocation,
        ),
        (String, acyclic_fs::NativeAllocation),
    )> {
        let (root_key, project) = plan
            .commit
            .child
            .project_volumes
            .iter()
            .next()
            .ok_or_else(|| invalid("Pi project missing"))?;
        let mut root = [0; 16];
        hex::decode_to_slice(root_key, &mut root)
            .map_err(|_| invalid("Pi pending root identity is not canonical"))?;
        let paths = crate::pi_native::planned_native_paths(
            self.resources,
            &self.retained.root_session_id,
            plan.commit.child.task_id,
            root_key,
            work,
        )?;
        if paths.0.cwd != plan.commit.launch.invocation.cwd
            || paths.0.session_file != plan.commit.launch.invocation.session_file
        {
            return Err(unauthorized(
                "Pi SDK retained native paths differ from its original planned namespace",
            ));
        }
        let workspace = {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = project.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        Ok((root, paths, workspace))
    }

    pub(crate) fn reservation(
        &self,
        plan: &PiForkPlan,
        work: &NativeMaterializationWork,
    ) -> Result<(PendingSpawn, acyclic_fs::NativeAllocation)> {
        let (root, paths, workspace) = self.reservation_coordinates(plan, work)?;
        #[derive(Serialize)]
        struct Pending<'b> {
            task_binding: PluginTaskBinding<&'b crate::PiTaskBinding>,
            agent_id: Option<&'b str>,
            parent_agent_id: &'b str,
            tool_use_id: &'b str,
            active_root_id: Option<[u8; 16]>,
            expires_at_millis: u64,
            workspace_name: &'b str,
            fork_key: [u8; 16],
            roots: BTreeMap<String, crate::RouteRoot>,
            mount_path: &'b std::path::Path,
            lifecycle: PendingSpawnLifecycle,
        }
        let pending = Pending {
            task_binding: PluginTaskBinding::Pi(&plan.commit.child),
            agent_id: Some(&plan.commit.alias),
            parent_agent_id: self.parent_alias(plan.commit.caller)?,
            tool_use_id: &plan.commit.sdk_operation,
            active_root_id: Some(root),
            expires_at_millis: crate::now_millis().saturating_add(2 * 60 * 1_000),
            workspace_name: &workspace.0,
            fork_key: PiForkOperations::new(plan.original, work)?
                .fork
                .into_bytes(),
            roots: BTreeMap::new(),
            mount_path: &paths.0.view_root,
            lifecycle: PendingSpawnLifecycle::Preparing,
        };
        let (value, custody) = self.decode(&pending, PENDING_DIAGNOSTIC_LABEL_BYTES, work)?;
        Ok((value, custody))
    }
}
