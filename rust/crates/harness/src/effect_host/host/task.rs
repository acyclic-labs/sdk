use super::*;

impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Runs one task command through the original conversation effect lifecycle.
    #[cfg(feature = "filesystem")]
    pub async fn run_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: TaskEffectPlan,
    ) -> Result<EffectStatus> {
        self.run_task_effect_with_work(owner, command_id, &plan, &mut None)
            .await
    }

    /// Runs that same lifecycle, retaining the actual returned status metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn run_task_effect_with_native_work(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        self.require_native_metadata(work)?;
        self.providers.require_native_metadata(work)?;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let status = self
            .run_task_effect_with_work(owner, command_id, plan, &mut Some(&mut io))
            .await?;
        use crate::store::history::HistoryIo as _;
        io.return_output(status_clone_bytes(&status)?, 1)?;
        let custody = io.take_native_custody(status_owned_bytes(&status)?)?;
        Ok((status, custody))
    }

    #[cfg(feature = "filesystem")]
    async fn run_task_effect_with_work(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStatus> {
        use crate::{EffectAttemptId, core::ApplyResult};
        validate_task_plan(owner, plan, true, work)?;
        let effect_id = task_effect_id(owner.task_binding().0, command_id, work)?;
        self.task_action(owner, effect_id, "plan", TaskEffectAction::Plan(plan), work)
            .await?;
        let stream = owner.stream();
        let aggregate = self.aggregate(&stream, work).await?;
        let effect = self.effect_for_host(&aggregate.0, effect_id, work).await?;
        if !matches!(
            effect.status,
            EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            return clone_status(&effect.status, work);
        }
        self.providers.resolve(&effect, work)?;
        let dispatched = if effect.status == EffectStatus::Planned {
            matches!(
                self.task_action(
                    owner,
                    effect_id,
                    "dispatch",
                    TaskEffectAction::Dispatch(EffectAttemptId::from_bytes(effect_id.into_bytes())),
                    work
                )
                .await?,
                ApplyResult::Applied { .. }
            )
        } else {
            false
        };
        drop(effect);
        drop(aggregate);
        let aggregate = self.aggregate(&stream, work).await?;
        let effect = self.effect_for_host(&aggregate.0, effect_id, work).await?;
        if !matches!(
            effect.status,
            EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            verify_owner(owner, true, work).await?;
            return clone_status(&effect.status, work);
        }
        let attestation = if dispatched {
            verify_owner(owner, false, work).await?;
            Some(
                self.providers
                    .dispatch_state_and_attest(&self.issuer, effect_id, &effect, work)
                    .await?,
            )
        } else {
            self.providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect, work)
                .await?
        };
        match attestation {
            Some(attestation) => {
                self.settle(effect_id, &effect, attestation, Some(owner), work)
                    .await
            }
            None => Ok(EffectStatus::Indeterminate),
        }
    }

    /// Observes the exact retained command, including after task cancellation.
    #[cfg(feature = "filesystem")]
    pub async fn reconcile_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
    ) -> Result<EffectStatus> {
        self.reconcile_task_effect_with_work(owner, command_id, plan, &mut None)
            .await
    }

    /// Reconciles without redispatching and retains actual returned metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn reconcile_task_effect_with_native_work(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(EffectStatus, acyclic_fs::NativeAllocation)> {
        self.require_native_metadata(work)?;
        self.providers.require_native_metadata(work)?;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let status = self
            .reconcile_task_effect_with_work(owner, command_id, plan, &mut Some(&mut io))
            .await?;
        use crate::store::history::HistoryIo as _;
        io.return_output(status_clone_bytes(&status)?, 1)?;
        let custody = io.take_native_custody(status_owned_bytes(&status)?)?;
        Ok((status, custody))
    }

    #[cfg(feature = "filesystem")]
    async fn reconcile_task_effect_with_work(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStatus> {
        verify_owner(owner, true, work).await?;
        validate_task_plan(owner, plan, false, work)?;
        let effect_id = task_effect_id(owner.task_binding().0, command_id, work)?;
        let stream = owner.stream();
        let aggregate = self.aggregate(&stream, work).await?;
        let effect = self.effect_for_host(&aggregate.0, effect_id, work).await?;
        if effect.provider != plan.provider
            || effect.guarantee != plan.guarantee
            || effect.effect_kind != plan.effect_kind
            || effect.request != plan.request
            || effect.result_schema != plan.result_schema
        {
            return Err(effect_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "task effect differs from retained command",
            ));
        }
        if !matches!(
            effect.status,
            EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            return clone_status(&effect.status, work);
        }
        match self
            .providers
            .reconcile_and_attest(&self.issuer, effect_id, &effect, work)
            .await?
        {
            Some(attestation) => {
                self.settle(effect_id, &effect, attestation, Some(owner), work)
                    .await
            }
            None => Ok(EffectStatus::Indeterminate),
        }
    }

    #[cfg(feature = "filesystem")]
    async fn task_action(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        effect_id: EffectId,
        phase: &str,
        action: TaskEffectAction<'_>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::core::ApplyResult> {
        let digest = crate::contract::canonical_json_digest(
            &("task-effect", effect_id, phase),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(16)?;
        }
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        if let Some(io) = work.as_deref_mut() {
            io.copy_bytes(16)?;
        }
        let operation_id = OperationId::from_bytes(identity);
        loop {
            let stream = owner.stream();
            let mut aggregate = self.aggregate(&stream, work).await?;
            let expected_revision = aggregate
                .0
                .reducer()
                .operation_revision(operation_id)
                .map_or(aggregate.0.reducer().revision(), |revision| revision - 1);
            let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
            let scope = self.scope.copy(custody.work())?;
            custody.adopt(scope.logical_owned_bytes()?)?;
            let copied_action = prepare_action(effect_id, &action, custody.work())?;
            custody.adopt(action_owned_bytes(&copied_action)?)?;
            let (key, key_bytes) =
                effect_key("task-effect:", effect_id, Some(phase), custody.work())?;
            custody.adopt(key_bytes)?;
            let command = Command {
                operation_id,
                idempotency_key: key,
                expected_revision,
                scope,
                causal_parent: None,
                action: copied_action,
            };
            match aggregate
                .0
                .execute(command, Some(owner), custody.work())
                .await
            {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => {
                    if aggregate
                        .0
                        .reducer()
                        .operation_revision(operation_id)
                        .is_some()
                    {
                        return Err(effect_error(
                            custody.work(),
                            crate::wire::ErrorCode::Conflict,
                            "task effect command changed after admission",
                        ));
                    }
                    verify_owner(owner, false, custody.work()).await?;
                }
                result => return result,
            }
        }
    }

    async fn settle(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        attestation: EffectAttestation,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectStatus> {
        let mut retained = crate::wire_codec::CodecQuota::new(work, 0)?;
        let attestation = attestation;
        retained.adopt(attestation_owned_bytes(&attestation)?)?;
        let digest = crate::contract::canonical_json_digest(
            &(effect_id, &attestation),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            retained.work(),
        )?;
        if let Some(io) = retained.work().as_deref_mut() {
            io.admit_copy(16)?;
        }
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        if let Some(io) = retained.work().as_deref_mut() {
            io.copy_bytes(16)?;
        }
        let operation_id = OperationId::from_bytes(identity);
        loop {
            #[cfg(feature = "filesystem")]
            if let Some(owner) = owner {
                verify_owner(owner, true, retained.work()).await?;
            }
            #[cfg(feature = "filesystem")]
            let stream = owner.map_or_else(
                || self.stream.clone(),
                crate::durable_host::TaskJournalOwner::stream,
            );
            #[cfg(not(feature = "filesystem"))]
            let stream = self.stream.clone();
            let mut aggregate = self.aggregate(&stream, retained.work()).await?;
            let current = self
                .effect_for_host(&aggregate.0, effect_id, retained.work())
                .await?;
            if current.status == attestation.status {
                return clone_status(&attestation.status, retained.work());
            }
            if current.attempts != effect.attempts
                || !matches!(
                    current.status,
                    EffectStatus::Dispatched | EffectStatus::Indeterminate
                )
            {
                return Err(effect_error(
                    retained.work(),
                    crate::wire::ErrorCode::Conflict,
                    "effect attempt changed during reconciliation",
                ));
            }
            drop(current);
            let mut command_custody = crate::wire_codec::CodecQuota::new(retained.work(), 0)?;
            let scope = self.scope.copy(command_custody.work())?;
            command_custody.adopt(scope.logical_owned_bytes()?)?;
            let observation = attestation.copy(command_custody.work())?.0;
            command_custody.adopt(attestation_owned_bytes(&observation)?)?;
            let (key, key_bytes) = effect_key(
                "effect-reconcile:",
                operation_id,
                None,
                command_custody.work(),
            )?;
            command_custody.adopt(key_bytes)?;
            let command = Command {
                operation_id,
                idempotency_key: key,
                expected_revision: aggregate.0.reducer().revision(),
                scope,
                causal_parent: None,
                action: Action::ResolveEffect { observation },
            };
            #[cfg(feature = "filesystem")]
            let result = match owner {
                Some(owner) => {
                    aggregate
                        .0
                        .execute(command, Some(owner), command_custody.work())
                        .await
                }
                None => {
                    if command_custody.work().is_some() {
                        return Err(effect_error(
                            command_custody.work(),
                            crate::wire::ErrorCode::Unsupported,
                            "observed effect settlement requires its task owner",
                        ));
                    }
                    aggregate
                        .0
                        .execute(command, None, command_custody.work())
                        .await
                }
            };
            #[cfg(not(feature = "filesystem"))]
            let result = {
                if command_custody.work().is_some() {
                    return Err(effect_error(
                        command_custody.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "native effect settlement requires filesystem task ownership",
                    ));
                }
                aggregate.0.execute(command, command_custody.work()).await
            };
            drop(command_custody);
            match result {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Ok(_) => return clone_status(&attestation.status, retained.work()),
                Err(error) if error.code() == crate::wire::ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl<P: StreamProvider> DurableEffectObserver for ConversationEffectHost<P> {
    fn reconcile<'a>(&'a self, effect_id: EffectId) -> BoxFuture<'a, Result<EffectStatus>> {
        Box::pin(async move {
            let aggregate = self.aggregate(&self.stream, &mut None).await?;
            let effect = self
                .effect_for_host(&aggregate.0, effect_id, &mut None)
                .await?;
            if !matches!(
                effect.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return clone_status(&effect.status, &mut None);
            }
            let Some(attestation) = self
                .providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect, &mut None)
                .await?
            else {
                return Ok(EffectStatus::Indeterminate);
            };
            self.settle(
                effect_id,
                &effect,
                attestation,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await
        })
    }
}
