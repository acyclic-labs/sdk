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
        let cancel = work.cancellation().clone();
        let actual = self.context(shared, &fence, work).await?;
        let (shared, owner, caller, _) = &actual;
        let context = shared.as_ref();
        let original = original_operation(
            self.protected.policy().root_task,
            owner.task_id(),
            &input.commit.sdk_operation,
            &work,
        )?;
        let operations = PiForkOperations::new(original, &work)?;
        if command.operation_id != original
            || input.commit.caller != owner.task_id()
            || input.commit.launch.source.parent_session_id != caller.binding.native_session_id
        {
            return Err(unauthorized(
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
        let coordinator = self.coordinator(&work)?;
        let existing = coordinator.load_plan(&context, original).await?;
        if reconcile && existing.is_none() {
            return Ok(TaskCommandProgress::Pending);
        }
        let (source, frozen, _source_custody) = self
            .source(shared, owner, caller, &input, existing.as_ref(), work)
            .await?;
        let conversation = self.protected.conversation_authority_with_native_work(
            owner.task_id(),
            caller.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let mut authority = (conversation.0, conversation.1);
        let mut verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let parent = {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            io.preflight()?;
            let aggregate = StreamAggregate::open(
                &self.stream,
                authority.0.0,
                verifier.0,
                SchemaRegistry::new(),
                None,
                acyclic_harness::store::default_projection_read_limits(),
                acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            (aggregate, io.into_custody()?)
        };
        let mut parent_custody = parent.1;
        drop(authority.0.1);
        drop(authority.0.2);
        parent_custody.absorb(&mut authority.1)?;
        parent_custody.absorb(&mut verifier.1)?;
        let mut parent = parent.0;
        parent.set_limits(context.scope().limits())?;
        let plan = coordinator
            .begin(
                shared,
                owner,
                &fence,
                parent,
                source.clone(),
                frozen,
                input.commit,
                ForkPreparationLimits {
                    resources: 128,
                    work: 1_000_000,
                    bytes: self.protected.policy().maximum_payload_bytes,
                },
                work.remaining()?,
                cancel.clone(),
            )
            .await?;
        let saved = self
            .retained
            .pending
            .iter()
            .find(|pending| pending.fork_key == operations.fork.into_bytes());
        if let Some(saved) = saved {
            let PluginTaskBinding::Pi(binding) = &saved.task_binding else {
                return Err(unauthorized(
                    "Pi pending substituted host-managed admission",
                ));
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
                return Err(unauthorized(
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
                return Err(unauthorized(
                    "Pi existing route is not its original admitted child",
                ));
            };
            if crate::pi_native_host::validate_child_binding(binding, &plan.commit.child).is_err()
                || route.turn_id != plan.commit.sdk_operation
                || route.parent_agent_id != self.parent_alias(plan.commit.caller)?
            {
                return Err(unauthorized(
                    "Pi existing route changed its original preparation identity",
                ));
            }
        } else {
            let reservation = self.reservation(&plan, work)?;
            coordinator
                .admit_preparation(&context, &owner, &fence, &plan)
                .await?;
            *self
                .pending
                .lock()
                .map_err(|_| invalid("Pi pending control lock poisoned"))? = Some(reservation);
            return Ok(TaskCommandProgress::Pending);
        }
        let Some(prepared) = coordinator
            .advance(
                shared,
                &owner,
                &fence,
                &plan,
                source,
                work.remaining()?,
                cancel,
            )
            .await?
        else {
            return Ok(TaskCommandProgress::Pending);
        };
        let task = prepared.binding.task_id;
        let result = ready(
            &PiPreparedReply {
                task,
                binding: &prepared.binding,
                state: "prepared",
            },
            &work,
        )?;
        Ok(result)
    }
}
