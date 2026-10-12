//! Authenticated admissions and original effect bindings.
use super::*;

impl PiNativeCommandHost {
    pub(super) async fn child_admission(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, PiTaskInput, NativeAllocation)> {
        self.runtime
            .verify_owner_with_native_work(&self.protected, work)?;
        let mut admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(task, work)
            .await?;
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(admission.0.limits.file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value(&admission.0.input, maximum, work)?;
        let mut input = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        if task == self.protected.policy().root_task
            || admission.0.parent.is_none()
            || admission.0.task != *self.runtime.definition.identity()
            || input.0.binding.task_id != task
            || !matches!(input.0.execution, PiTaskExecution::Child { .. })
        {
            return Err(unauthorized(
                "Pi native command requires its original registered child admission",
            ));
        }
        input.0.binding.validate_with_native_work(
            None,
            input.0.binding.project_volumes.keys().map(String::as_str),
            true,
            work,
        )?;
        admission.1.absorb(&mut input.1)?;
        Ok((admission.0, input.0, admission.1))
    }

    pub(super) async fn original_never_started(
        &self,
        task: TaskId,
        meter: &NativeMaterializationWork,
    ) -> Result<bool> {
        let admission = self.child_admission(task, meter).await?;
        let input = &admission.1;
        let PiTaskExecution::Child { launch } = &input.execution else {
            unreachable!()
        };
        let entry = self.registered_entry(task)?;
        {
            let handles = entry.handles.lock().await;
            if handles.effect.is_some()
                || handles.preparation_owner.is_some()
                || handles.root.is_some()
                || handles.project.is_some()
                || handles.project_allocation.is_some()
                || handles.project_startup_owner.is_some()
                || !handles.scratch_gate.is_settled()
                || !handles.project_gate.is_settled()
                || self.live_view(task).is_some()
            {
                return Ok(false);
            }
        }
        let observed = {
            let mut io = native_work_io(meter)?;
            HistoryIo::retain_owned(
                &mut io,
                std::mem::size_of::<WorkPull>().max(std::mem::size_of::<TaskWorkerAttempt>())
                    as u64,
            )?;
            let value = self
                .runtime
                .runtime
                .task_host()
                .recover_work(task, &mut Some(&mut io))
                .await?;
            (value, io.into_custody())
        };
        let lease = match observed.0 {
            WorkPull::Claimed(lease) => lease,
            WorkPull::Unresolved { error, .. } => return Err(error),
            WorkPull::Idle => return Ok(false),
        };
        let fence = LeaseFence::from(&lease.reservation);
        let owner = self
            .runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(task, &fence, meter)
            .await?;
        match self
            .observe_preparation_journal(task, &owner, meter)
            .await?
        {
            Some(_) => Ok(false),
            None => Err(Error::Indeterminate(launch.operation_id)),
        }
    }

    pub(super) async fn authenticated_command(
        &self,
        context: &TaskContext,
        fence: &LeaseFence,
        command: &WorkflowCommand,
        payload: Value,
        recovery: bool,
        work: &NativeMaterializationWork,
    ) -> Result<(
        (PiTaskInput, NativeAllocation),
        Arc<TaskJournalOwner<LocalStream>>,
        (PiNativeLaunchIntent, NativeAllocation),
    )> {
        let task = context
            .durable_task_id()
            .ok_or_else(|| unauthorized("Pi command has no durable child owner"))?;
        let admitted = self.child_admission(task, work).await?;
        let admission = &admitted.0;
        let input = &admitted.1;
        let PiTaskExecution::Child { launch } = &input.execution else {
            unreachable!()
        };
        if launch != command
            || command.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND
            || context.scope().grants() != &admission.grants
            || context.scope().limits() != admission.limits
            || context.scope().run_limits() != admission.run_limits
            || context.owned_task_fence()? != *fence
        {
            return Err(unauthorized(
                "Pi native command, scope or fence differs from its original admission",
            ));
        }
        let original = PiNativeLaunchIntent::load(context, &command.payload, work).await?;
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(context.scope().limits().file_bytes);
        let supplied_bytes = crate::pi_root_provider::canonical_pi_value(&payload, maximum, work)?;
        let supplied = crate::pi_root_provider::guarded_pi_decode::<PiNativeLaunchIntent>(
            &supplied_bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        if original.0 != supplied.0 {
            return Err(unauthorized(
                "Pi resolved native input differs from its immutable original file",
            ));
        }
        let owner = self
            .runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(task, fence, work)
            .await?
            .into_native_shared(work)?;
        owner
            .verify(recovery, &mut Some(&mut native_work_io(work)?))
            .await?;
        Ok(((admitted.1, admitted.2), owner, original))
    }

    pub(super) fn result_bindings(
        &self,
        context: &TaskContext,
        input: &PiTaskInput,
    ) -> Result<ContentBindings> {
        let result = input
            .binding
            .results
            .as_ref()
            .ok_or_else(|| unauthorized("Pi child has no original result selection"))?;
        context.created_content_bindings(&result.volume)
    }

    pub(super) async fn journal(
        &self,
        owner: &TaskJournalOwner<LocalStream>,
        context: &TaskContext,
        input: &PiTaskInput,
        command: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Journal>> {
        let storage = self.protected.authority(work)?;
        let conversation = self.protected.conversation_authority_with_native_work(
            owner.task_id(),
            input.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let interaction_verifier = {
            let mut io = native_work_io(work)?;
            let value = conversation.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        let result = input
            .binding
            .results
            .as_ref()
            .ok_or_else(|| unauthorized("Pi approval has no original result volume"))?;
        let journal_owner = self
            .runtime
            .runtime
            .task_host()
            .journal_owner_with_native_work(owner.task_id(), &context.owned_task_fence()?, work)
            .await?;
        Journal::for_task_with_native_work(
            journal_owner,
            command,
            self.filesystem.clone(),
            &self.protected.policy().journal,
            &storage.verifier,
            &storage.signed,
            self.protected
                .policy()
                .maximum_payload_bytes
                .min(context.scope().limits().file_bytes),
            work,
        )?
        .with_interaction_owner_with_native_work(
            &interaction_verifier.0,
            &SchemaRegistry::new(),
            &conversation.0.2,
            &result.volume,
            work,
        )?
        .into_native_shared(work)
    }

    pub(super) fn authority(
        &self,
        context: &TaskContext,
        input: &PiTaskInput,
        journal: Arc<Journal>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeProcessAuthority> {
        let bindings = self.result_bindings(context, input)?;
        let conversation = self.protected.conversation_authority_with_native_work(
            input.binding.task_id,
            input.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let verifier = {
            let mut io = native_work_io(work)?;
            let value = conversation.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        Ok(NativeProcessAuthority {
            verifier: verifier.0,
            history_limits: history_limits(context)?,
            // Both stock artifacts and their results use the original unmounted RESULT binding.
            content: bindings.reader.clone(),
            result_content: bindings.reader,
            results: bindings
                .writer
                .ok_or_else(|| unauthorized("Pi sealed result has no original publisher"))?,
            approvals: journal,
            authority_custody: Some(verifier.1),
        })
    }

    pub(super) fn effects(
        &self,
        context: &TaskContext,
        input: &PiTaskInput,
        provider: &Arc<NativeProcessProvider<LocalStream>>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Effects>> {
        let content = self.result_bindings(context, input)?.reader;
        let authorization = self.protected.conversation_authority_with_native_work(
            input.binding.task_id,
            input.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let mut registry = EffectRegistry::new(Some(work))?
            .with_result_resolver_with_native_work(content.clone(), work)?;
        registry.register_with_native_work(provider.clone(), work)?;
        let ((audience, issuer, scope), custody) = authorization;
        Effects::new_with_native_work(
            self.stream.clone(),
            audience,
            issuer,
            scope,
            SchemaRegistry::new(),
            content,
            registry,
            custody,
            work,
        )?
        .into_arc_with_native_work(work)
    }

    pub(super) async fn original_effect(
        &self,
        context: &TaskContext,
        input: &PiTaskInput,
        command: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<(Option<acyclic_harness::core::EffectState>, NativeAllocation)> {
        let conversation = self.protected.conversation_authority_with_native_work(
            input.binding.task_id,
            input.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let verifier = {
            let mut io = native_work_io(work)?;
            let value = conversation.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        let history = HistoryReader::new_with_native_work(
            &self.stream,
            &conversation.0.0,
            &verifier.0,
            work,
        )?;
        let cursor = history.0.pin_with_native_work(0, work).await?;
        let effect = acyclic_harness::effect_host::task_effect_id(
            input.binding.task_id,
            command,
            &mut Some(&mut native_work_io(work)?),
        )?;
        history
            .0
            .effect_with_native_work(&cursor.0, effect, history_limits(context)?, work)
            .await
    }

    pub(super) async fn bind_recovery(
        &self,
        shared: Arc<TaskContext>,
        input: &PiTaskInput,
        owner: Arc<TaskJournalOwner<LocalStream>>,
        command: OperationId,
        effect: &acyclic_harness::core::EffectState,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<BoundEffect>> {
        let context = shared.as_ref();
        let borrowed = BorrowedEffectPlan {
            provider: &effect.provider,
            guarantee: &effect.guarantee,
            effect_kind: &effect.effect_kind,
            request: &effect.request,
            result_schema: &effect.result_schema,
        };
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(context.scope().limits().file_bytes);
        let bytes = crate::pi_root_provider::canonical_pi_value(&borrowed, maximum, work)?;
        let plan = crate::pi_root_provider::guarded_pi_decode::<TaskEffectPlan>(
            &bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        if plan.0.provider != pi_native::PI_NATIVE_PROVIDER {
            return Err(unauthorized(
                "Pi original native command retained another effect provider",
            ));
        }
        let journal = self.journal(&owner, context, input, command, work).await?;
        let authority = self.authority(context, input, journal.clone(), work)?;
        let provider = NativeProcessProvider::recover(
            owner.clone(),
            command,
            &plan.0,
            task_interaction_id(input.binding.task_id, command),
            authority,
            work,
        )
        .await?
        .into_arc_with_native_work(work)?;
        let effects = self.effects(context, input, &provider, work)?;
        let bound = Arc::new(BoundEffect {
            provider,
            work: work.clone(),
            effects,
            owner,
            journal,
            context: shared,
            command,
            staged: None,
            plan,
        });
        self.registered_entry(input.binding.task_id)?
            .handles
            .lock()
            .await
            .effect = Some(bound.clone());
        Ok(bound)
    }

    /// Reuses the original live provider and SDK gates.
    pub(super) async fn retained_planned(
        &self,
        task: TaskId,
        command: OperationId,
        effect: &acyclic_harness::core::EffectState,
    ) -> Result<Option<Arc<BoundEffect>>> {
        if effect.status != EffectStatus::Planned {
            return Ok(None);
        }
        let Some(entry) = self.find_entry(task) else {
            return Ok(None);
        };
        let retained = {
            let handles = entry.handles.lock().await;
            if handles.scratch_gate.reservation.is_none()
                || handles.project_gate.reservation.is_none()
                || handles.mounts_drained
            {
                return Ok(None);
            }
            handles.effect.clone()
        };
        let Some(bound) = retained else {
            return Ok(None);
        };
        if bound.staged.is_none() {
            return Ok(None);
        }
        let plan = &bound.plan.0;
        if bound.command != command
            || plan.provider != effect.provider
            || plan.guarantee != effect.guarantee
            || plan.effect_kind != effect.effect_kind
            || plan.request != effect.request
            || plan.result_schema != effect.result_schema
        {
            return Err(unauthorized(
                "Pi Planned effect differs from its original retained SDK launch",
            ));
        }
        if bound
            .provider
            .receipt(bound.provider.original_attempt(), &bound.work, None)
            .await?
            .is_some()
        {
            return Ok(None);
        }
        Ok(Some(bound))
    }
}
