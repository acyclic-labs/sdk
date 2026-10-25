use super::*;

impl crate::pi_host::PiControlHost<'_> {
    pub(crate) async fn fork(
        &self,
        shared: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let mut supplied: (PiForkCommand, acyclic_fs::NativeAllocation) =
            self.decode(&payload, FORK_COMMAND_DIAGNOSTIC_LABEL_BYTES, work)?;
        supplied
            .0
            .commit
            .launch
            .invocation
            .environment
            .retain(|key, _| !crate::pi_caller::is_environment_key(key));
        let input = supplied.0;
        let actual = self.context(shared, &fence, work).await?;
        let (shared, owner, caller, _) = &actual;
        let context = shared.as_ref();
        let original = original_operation(
            self.protected.policy().root_task,
            owner.task_id(),
            &input.commit.sdk_operation,
            work,
        )?;
        let operations = PiForkOperations::new(original, work)?;
        if command.operation_id != original
            || input.commit.caller != owner.task_id()
            || input.commit.launch.source.parent_session_id != caller.binding.native_session_id
        {
            return Err(denied(
                "Pi fork control changed its original caller/SDK operation",
            ));
        }
        if self.retained.pending.iter().any(|pending| {
            pending.agent_id.as_deref() == Some(input.commit.alias.as_str())
                && pending.fork_key != operations.fork.into_bytes()
        }) {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi requested alias is already reserved by another original fork",
            });
        }
        if self.retained.routes.values().any(|route| route.agent_id == input.commit.alias
            && !matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if crate::pi_native_host::validate_child_binding(binding, &input.commit.child).is_ok())) {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi requested alias already belongs to another admitted child" });
        }
        let coordinator = self.coordinator(work)?;
        coordinator.require_input_read(context)?;
        let existing = coordinator.load_original_plan(original).await?;
        if reconcile && existing.is_none() {
            return Ok(TaskCommandProgress::Pending);
        }
        let (source, frozen, _source_custody) = self
            .source(shared, owner, caller, &input, existing.as_ref(), work)
            .await?;
        let plan = coordinator
            .begin(
                shared,
                owner,
                &fence,
                source.clone(),
                frozen,
                input.commit,
                ForkPreparationLimits {
                    resources: 128,
                    work: 1_000_000,
                    bytes: self.protected.policy().maximum_payload_bytes,
                },
            )
            .await?;
        let saved = self
            .retained
            .pending
            .iter()
            .find(|pending| pending.fork_key == operations.fork.into_bytes());
        if let Some(saved) = saved {
            let PluginTaskBinding::Pi(binding) = &saved.task_binding else {
                return Err(denied("Pi pending substituted host-managed admission"));
            };
            let (root, paths, workspace) = self.reservation_coordinates(&plan, work)?;
            if crate::pi_native_host::validate_child_binding(binding, &plan.commit.child).is_err()
                || saved.tool_use_id != plan.commit.sdk_operation
                || saved.agent_id.as_deref() != Some(plan.commit.alias.as_str())
                || saved.parent_agent_id != self.parent_alias(plan.commit.caller)?
                || saved.workspace_name != workspace.0
                || saved.active_root_id != Some(root)
                || saved.mount_path != paths.0.view_root
                || !saved.roots.is_empty()
                || !matches!(&saved.lifecycle, PendingSpawnLifecycle::Preparing)
                || saved.expires_at_millis <= crate::now_millis()
            {
                return Err(denied(
                    "Pi pending changed or expired its original preparation identity",
                ));
            }
        } else if let Some(route) = self
            .retained
            .routes
            .values()
            .find(|route| route.agent_id == plan.commit.alias)
        {
            let PluginTaskBinding::Pi(binding) = &route.task_binding else {
                return Err(denied(
                    "Pi existing route is not its original admitted child",
                ));
            };
            if crate::pi_native_host::validate_child_binding(binding, &plan.commit.child).is_err()
                || route.turn_id != plan.commit.sdk_operation
                || route.parent_agent_id != self.parent_alias(plan.commit.caller)?
            {
                return Err(denied(
                    "Pi existing route changed its original preparation identity",
                ));
            }
        } else {
            let reservation = self.reservation(&plan, work)?;
            coordinator
                .admit_preparation(context, owner, &fence, &plan)
                .await?;
            *self
                .pending
                .lock()
                .map_err(|_| invalid("Pi pending control lock poisoned"))? = Some(reservation);
            return Ok(TaskCommandProgress::Pending);
        }
        let Some(prepared) = coordinator
            .advance(shared, owner, &fence, &plan, source)
            .await?
        else {
            return Ok(TaskCommandProgress::Pending);
        };
        let task = prepared.binding.task_id;
        let reply = ready(
            &PiPreparedReply {
                task,
                binding: &prepared.binding,
                state: "prepared",
            },
            work,
        )?;
        *self
            .prepared
            .lock()
            .map_err(|_| invalid("Pi prepared fork lock poisoned"))? = Some(prepared);
        Ok(reply)
    }
}
