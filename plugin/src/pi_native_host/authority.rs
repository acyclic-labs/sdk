//! Authenticated admissions and original effect bindings.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

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
            .observe_admission_with_native_work(task, work)?
            .await?;
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(admission.0.limits.file_bytes);
        let mut input = crate::pi_recovery::decode_recovery_value::<PiTaskInput>(
            std::borrow::Cow::Borrowed(&admission.0.input),
            acyclic_harness::JsonValueSchema::None,
            maximum,
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
        input.0.binding.validate(
            None,
            input
                .0
                .binding
                .project_volumes
                .keys()
                .map(|key| key.as_str()),
            true,
        )?;
        admission.1.absorb(&mut input.1)?;
        Ok((admission.0, input.0, admission.1))
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
            || context
                .owned_task_binding(Some(&PreparationIo::from_native_work(work)?))?
                .1
                != fence
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
        let supplied = crate::pi_recovery::decode_recovery_value::<PiNativeLaunchIntent>(
            std::borrow::Cow::Owned(payload),
            acyclic_harness::JsonValueSchema::None,
            maximum,
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
            .journal_owner(task, fence, Some(&PreparationIo::from_native_work(work)?))
            .await?
            .into_native_shared(work)?;
        owner
            .verify(
                recovery,
                &mut Some(&mut &PreparationIo::from_native_work(work)?),
            )
            .await?;
        Ok(((admitted.1, admitted.2), owner, original))
    }

    pub(super) fn result_bindings(
        &self,
        context: &TaskContext,
        input: &PiTaskInput,
        work: &NativeMaterializationWork,
    ) -> Result<ContentBindings> {
        let result = input
            .binding
            .results
            .as_ref()
            .ok_or_else(|| unauthorized("Pi child has no original result selection"))?;
        let volume = &result.volume;
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        volume.validate(&mut Some(&mut io))?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !context
                .scope()
                .grants()
                .contains(&volume.capability(operation, &mut Some(&mut io))?)
            {
                return Err(unauthorized(
                    "Pi result lacks its original read/write grant",
                ));
            }
        }
        context
            .scope()
            .created_content(Some(volume))
            .filter(|binding| {
                binding
                    .writer
                    .as_ref()
                    .is_some_and(|writer| writer.volume() == volume)
            })
            .cloned()
            .ok_or_else(|| unauthorized("Pi result has no authenticated created writer"))
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
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let value = conversation.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
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
            .journal_owner(
                owner.task_id(),
                context
                    .owned_task_binding(Some(&PreparationIo::from_native_work(work)?))?
                    .1,
                Some(&PreparationIo::from_native_work(work)?),
            )
            .await?;
        let io = PreparationIo::from_native_work(work)?;
        Journal::for_task(
            journal_owner,
            command,
            context.id(),
            self.filesystem.clone(),
            acyclic_harness::filesystem::FilesystemJournalBinding {
                volume: &self.protected.policy().journal,
                verifier: &storage.verifier,
                scope: &storage.signed,
                maximum_bytes: self
                    .protected
                    .policy()
                    .maximum_payload_bytes
                    .min(context.scope().limits().file_bytes),
                io: Some(&io),
            },
        )?
        .with_interaction_owner(
            &interaction_verifier.0,
            &SchemaRegistry::new(),
            &conversation.0.2,
            &result.volume,
            Some(&io),
        )?
        .into_native_shared(work)
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
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let value = conversation.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let mut reader_io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let history = HistoryReader::new(
            &self.stream,
            &conversation.0.0,
            &verifier.0,
            &mut Some(&mut reader_io),
        )?;
        let cursor = history.pin(0, Some(&mut reader_io)).await?;
        let effect = acyclic_harness::effect_host::task_effect_id(
            input.binding.task_id,
            command,
            &mut Some(&mut &PreparationIo::from_native_work(work)?),
        )?;
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let effect = history
            .effect(&cursor, effect, history_limits(context)?, Some(&mut io))
            .await?;
        Ok((effect.map(|(effect, _)| effect), io.into_custody()?))
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
        let plan = effect_plan(
            &effect.provider,
            effect.guarantee,
            &effect.effect_kind,
            &effect.request,
            &effect.result_schema,
            work,
        )?;
        if plan.0.provider != pi_native::PI_NATIVE_PROVIDER {
            return Err(unauthorized(
                "Pi original native command retained another effect provider",
            ));
        }
        let journal = self.journal(&owner, context, input, command, work).await?;
        let approval = task_interaction_id(
            input.binding.task_id,
            command,
            &mut Some(&mut &PreparationIo::from_native_work(work)?),
        )?;
        let bound = self
            .bind_effect(
                shared, input, owner, command, plan, journal, None, approval, None, work,
            )
            .await?;
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
            if !matches!(&handles.scratch_gate, Some((_, _, Some(_))))
                || !matches!(&handles.project_gate, Some((_, _, Some(_))))
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
            || &plan.request != effect.request.as_ref()
            || plan.result_schema != effect.result_schema
        {
            return Err(unauthorized(
                "Pi Planned effect differs from its original retained SDK launch",
            ));
        }
        Ok(Some(bound))
    }
}
