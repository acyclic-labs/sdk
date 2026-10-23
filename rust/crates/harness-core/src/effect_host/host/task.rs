use super::*;
impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Runs or observes the exact retained task effect on its original ledger.
    #[cfg(feature = "filesystem")]
    pub async fn run_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command: OperationId,
        plan: &TaskEffectPlan,
        work: NativeWork<'_>,
        reconcile_only: bool,
    ) -> Result<(EffectStatus, NativeCustody)> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            self.require_native_metadata(work)?;
            self.providers.require_native_metadata(work)?;
        }
        let mut io = history(work)?;
        let observed = &mut io.as_mut().map(|io| io as &mut dyn HistoryIo);
        let status = async {
            if reconcile_only {
                owner.verify(true, observed).await?;
            }
            owner.require_effect_grants(&plan.provider, !reconcile_only, observed)?;
            #[cfg(not(target_arch = "wasm32"))]
            let input = observed
                .as_deref()
                .and_then(HistoryIo::original_native_work)
                .map(crate::executor::PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(target_arch = "wasm32")]
            let input = None;
            owner.validate_input_file(&plan.request, input.as_ref())?;
            owner.validate_input_file(&plan.result_schema, input.as_ref())?;
            let effect_id = task_effect_id(owner.task_binding().0, command, observed)?;
            if !reconcile_only {
                self.task_action(owner, effect_id, Some(plan), observed)
                    .await?;
            }
            self.resolve_task_effect(owner, effect_id, plan, reconcile_only, observed)
                .await
        }
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let custody = if let Some(mut io) = io {
            let copied = match &status {
                EffectStatus::Succeeded { result } => result.logical_clone_owned_bytes()?,
                EffectStatus::Failed { message } => message.len() as u64,
                _ => 0,
            };
            io.return_output(copied, 1)?;
            Some(io.take_native_custody(status_owned(&status)?)?)
        } else {
            None
        };
        #[cfg(target_arch = "wasm32")]
        let custody = None;
        Ok((status, custody))
    }
    #[cfg(feature = "filesystem")]
    async fn task_action(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        effect_id: EffectId,
        plan: Option<&TaskEffectPlan>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<crate::core::ApplyResult> {
        let phase = if plan.is_some() { "plan" } else { "dispatch" };
        let operation_id =
            OperationId::from_bytes(plan::identity(&("task-effect", effect_id, phase), work)?);
        loop {
            let stream = owner.stream();
            let mut aggregate = self.aggregate(&stream, work).await?;
            let revision = aggregate
                .0
                .reducer()
                .operation_revision(operation_id)
                .map_or(aggregate.0.reducer().revision(), |revision| revision - 1);
            let mut quota = CodecQuota::new(work, 0)?;
            let scope = self.scope.copy(quota.work())?;
            quota.adopt(scope.logical_owned_bytes()?)?;
            let action = if let Some(plan) = plan {
                let request = plan.request.copy(quota.work())?;
                quota.adopt(request.logical_owned_bytes()?)?;
                let schema = plan.result_schema.copy(quota.work())?;
                quota.adopt(schema.logical_owned_bytes()?)?;
                Action::PlanEffect {
                    effect_id,
                    provider: crate::core::copy_text(&plan.provider, &mut quota)?,
                    guarantee: plan.guarantee,
                    effect_kind: crate::core::copy_text(&plan.effect_kind, &mut quota)?,
                    request: Box::new(request),
                    result_schema: Box::new(schema),
                }
            } else {
                Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id: crate::EffectAttemptId::from_bytes(effect_id.into_bytes()),
                }
            };
            let key = crate::executor::preparation_format(
                format_args!("task-effect:{effect_id}:{phase}"),
                quota.work(),
            )?;
            quota.adopt(key.capacity() as u64)?;
            let command = Command {
                operation_id,
                idempotency_key: IdempotencyKey::new(key)?,
                expected_revision: revision,
                scope,
                causal_parent: None,
                action,
            };
            match aggregate
                .0
                .execute(command, Some(owner), quota.work())
                .await
            {
                #[cfg(not(target_arch = "wasm32"))]
                Err(error) if error.is_native_failure() => return Err(error),
                Err(error) if error.code() == Conflict => {
                    if aggregate
                        .0
                        .reducer()
                        .operation_revision(operation_id)
                        .is_some()
                    {
                        return Err(boundary(
                            quota.work(),
                            Conflict,
                            "admitted effect command changed",
                        ));
                    }
                    owner.verify(false, quota.work()).await?;
                }
                result => return result,
            }
        }
    }
    async fn settle(
        &self,
        id: EffectId,
        effect: &EffectState,
        attestation: EffectAttestation,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<EffectStatus> {
        let mut retained = CodecQuota::new(work, 0)?;
        retained.adopt(attestation_owned(&attestation)?)?;
        let operation_id =
            OperationId::from_bytes(plan::identity(&(id, &attestation), retained.work())?);
        loop {
            #[cfg(feature = "filesystem")]
            if let Some(owner) = owner {
                owner.verify(true, retained.work()).await?;
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
                .effect_for_host(&aggregate.0, id, retained.work())
                .await?;
            if current.0.status == attestation.status {
                return copy_status(&attestation.status, retained.work());
            }
            if current.0.attempts != effect.attempts
                || !matches!(
                    current.0.status,
                    EffectStatus::Dispatched | EffectStatus::Indeterminate
                )
            {
                return Err(boundary(
                    retained.work(),
                    Conflict,
                    "effect attempt changed",
                ));
            }
            drop(current);
            let mut quota = CodecQuota::new(retained.work(), 0)?;
            let scope = self.scope.copy(quota.work())?;
            quota.adopt(scope.logical_owned_bytes()?)?;
            let (observation, paid) = attestation.copy(quota.work())?;
            quota.adopt(paid)?;
            let key = crate::executor::preparation_format(
                format_args!("effect-reconcile:{operation_id}"),
                quota.work(),
            )?;
            quota.adopt(key.capacity() as u64)?;
            let command = Command {
                operation_id,
                idempotency_key: IdempotencyKey::new(key)?,
                expected_revision: aggregate.0.reducer().revision(),
                scope,
                causal_parent: None,
                action: Action::ResolveEffect {
                    observation: (observation).into(),
                },
            };
            if quota.work().is_some() {
                #[cfg(feature = "filesystem")]
                if owner.is_none() {
                    return Err(boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "observed settlement requires task owner",
                    ));
                }
                #[cfg(not(feature = "filesystem"))]
                return Err(boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Unsupported,
                    "observed settlement requires filesystem",
                ));
            }
            let result = aggregate
                .0
                .execute(
                    command,
                    #[cfg(feature = "filesystem")]
                    owner,
                    quota.work(),
                )
                .await;
            drop(quota);
            match result {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(error) if error.is_native_failure() => return Err(error),
                Ok(_) => return copy_status(&attestation.status, retained.work()),
                Err(error) if error.code() == Conflict => continue,
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(feature = "filesystem")]
    async fn resolve_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        effect_id: EffectId,
        plan: &TaskEffectPlan,
        reconcile_only: bool,
        observed: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<EffectStatus> {
        let stream = owner.stream();
        let mut dispatch = reconcile_only.then_some(false);
        loop {
            let aggregate = self.aggregate(&stream, observed).await?;
            let effect = self
                .effect_for_host(&aggregate.0, effect_id, observed)
                .await?;
            if reconcile_only
                && (effect.0.provider != plan.provider
                    || effect.0.guarantee != plan.guarantee
                    || effect.0.effect_kind != plan.effect_kind
                    || effect.0.request != plan.request
                    || effect.0.result_schema != plan.result_schema)
            {
                return Err(boundary(
                    observed,
                    Conflict,
                    "effect differs from retained command",
                ));
            }
            if dispatch.is_none() {
                if !matches!(
                    effect.0.status,
                    EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate
                ) {
                    return copy_status(&effect.0.status, observed);
                }
                self.providers.resolve(&effect.0, observed)?;
                dispatch = Some(
                    effect.0.status == EffectStatus::Planned
                        && matches!(
                            self.task_action(owner, effect_id, None, observed).await?,
                            crate::core::ApplyResult::Applied { .. }
                        ),
                );
                continue;
            }
            if !matches!(
                effect.0.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                if !reconcile_only {
                    owner.verify(true, observed).await?;
                }
                return copy_status(&effect.0.status, observed);
            }
            let attestation = if dispatch == Some(true) {
                owner.verify(false, observed).await?;
                Some(
                    self.providers
                        .dispatch_and_attest(&self.issuer, effect_id, &effect.0, observed)
                        .await?,
                )
            } else {
                self.providers
                    .reconcile_and_attest(&self.issuer, effect_id, &effect.0, observed)
                    .await?
            };
            return match attestation {
                Some(attestation) => {
                    self.settle(effect_id, &effect.0, attestation, Some(owner), observed)
                        .await
                }
                None => Ok(EffectStatus::Indeterminate),
            };
        }
    }
}
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: StreamProvider> DurableEffectObserver for ConversationEffectHost<P> {
    async fn reconcile(&self, id: EffectId) -> Result<EffectStatus> {
        let aggregate = self.aggregate(&self.stream, &mut None).await?;
        let effect = self.effect_for_host(&aggregate.0, id, &mut None).await?;
        if !matches!(
            effect.0.status,
            EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            return copy_status(&effect.0.status, &mut None);
        }
        match self
            .providers
            .reconcile_and_attest(&self.issuer, id, &effect.0, &mut None)
            .await?
        {
            Some(attestation) => {
                self.settle(
                    id,
                    &effect.0,
                    attestation,
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await
            }
            None => Ok(EffectStatus::Indeterminate),
        }
    }
}
