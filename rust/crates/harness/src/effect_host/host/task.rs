use super::*;

impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Runs the admitted task effect and returns its original output custody.
    #[cfg(feature = "filesystem")]
    pub async fn run_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
        work: crate::NativeWork<'_>,
    ) -> Result<(EffectStatus, crate::NativeCustody)> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut io = match work {
            Some(work) => {
                self.require_native_metadata(work)?;
                self.providers.require_native_metadata(work)?;
                Some(crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                ))
            }
            None => None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let observed = &mut io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let observed = &mut None;
        let status = async {
            use crate::core::ApplyResult;
            validate_task_plan(owner, plan, true)?;
            let effect_id = task_effect_id(owner.task_binding().0, command_id, observed)?;
            self.task_action(owner, effect_id, Some(plan), observed)
                .await?;
            let stream = owner.stream();
            let aggregate = self.aggregate(&stream, observed).await?;
            let effect = self
                .effect_for_host(&aggregate.0, effect_id, observed)
                .await?;
            if !matches!(
                effect.0.status,
                EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return clone_status(&effect.0.status, observed);
            }
            self.providers.resolve(&effect.0, observed)?;
            let dispatched = if effect.0.status == EffectStatus::Planned {
                matches!(
                    self.task_action(owner, effect_id, None, observed).await?,
                    ApplyResult::Applied { .. }
                )
            } else {
                false
            };
            drop(effect);
            drop(aggregate);
            let aggregate = self.aggregate(&stream, observed).await?;
            let effect = self
                .effect_for_host(&aggregate.0, effect_id, observed)
                .await?;
            if !matches!(
                effect.0.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                owner.verify(true).await?;
                return clone_status(&effect.0.status, observed);
            }
            let attestation = if dispatched {
                owner.verify(false).await?;
                Some(
                    self.providers
                        .dispatch_state_and_attest(&self.issuer, effect_id, &effect.0, observed)
                        .await?,
                )
            } else {
                self.providers
                    .reconcile_and_attest(&self.issuer, effect_id, &effect.0, observed)
                    .await?
            };
            match attestation {
                Some(attestation) => {
                    self.settle(effect_id, &effect.0, attestation, Some(owner), observed)
                        .await
                }
                None => Ok(EffectStatus::Indeterminate),
            }
        }
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let custody = match io {
            Some(mut io) => {
                use crate::store::history::HistoryIo as _;
                io.return_output(status_clone_bytes(&status)?, 1)?;
                Some(io.take_native_custody(status_owned_bytes(&status)?)?)
            }
            None => None,
        };
        #[cfg(target_arch = "wasm32")]
        let custody = {
            let _ = work;
            None
        };
        Ok((status, custody))
    }

    /// Observes the retained command without dispatching and returns its output custody.
    #[cfg(feature = "filesystem")]
    pub async fn reconcile_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
        work: crate::NativeWork<'_>,
    ) -> Result<(EffectStatus, crate::NativeCustody)> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut io = match work {
            Some(work) => {
                self.require_native_metadata(work)?;
                self.providers.require_native_metadata(work)?;
                Some(crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                ))
            }
            None => None,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let observed = &mut io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let observed = &mut None;
        let status = async {
            owner.verify(true).await?;
            validate_task_plan(owner, plan, false)?;
            let effect_id = task_effect_id(owner.task_binding().0, command_id, observed)?;
            let stream = owner.stream();
            let aggregate = self.aggregate(&stream, observed).await?;
            let effect = self
                .effect_for_host(&aggregate.0, effect_id, observed)
                .await?;
            if effect.0.provider != plan.provider
                || effect.0.guarantee != plan.guarantee
                || effect.0.effect_kind != plan.effect_kind
                || effect.0.request != plan.request
                || effect.0.result_schema != plan.result_schema
            {
                return Err(effect_error(
                    observed,
                    crate::wire::ErrorCode::Conflict,
                    "task effect differs from retained command",
                ));
            }
            if !matches!(
                effect.0.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return clone_status(&effect.0.status, observed);
            }
            match self
                .providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect.0, observed)
                .await?
            {
                Some(attestation) => {
                    self.settle(effect_id, &effect.0, attestation, Some(owner), observed)
                        .await
                }
                None => Ok(EffectStatus::Indeterminate),
            }
        }
        .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let custody = match io {
            Some(mut io) => {
                use crate::store::history::HistoryIo as _;
                io.return_output(status_clone_bytes(&status)?, 1)?;
                Some(io.take_native_custody(status_owned_bytes(&status)?)?)
            }
            None => None,
        };
        #[cfg(target_arch = "wasm32")]
        let custody = {
            let _ = work;
            None
        };
        Ok((status, custody))
    }

    #[cfg(feature = "filesystem")]
    async fn task_action(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        effect_id: EffectId,
        plan: Option<&TaskEffectPlan>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::core::ApplyResult> {
        let phase = if plan.is_some() { "plan" } else { "dispatch" };
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
            let copied_action = match plan {
                Some(plan) => {
                    let provider = match custody.work().as_deref_mut() {
                        Some(io) => io.clone_text(&plan.provider)?,
                        None => plan.provider.clone(),
                    };
                    let effect_kind = match custody.work().as_deref_mut() {
                        Some(io) => io.clone_text(&plan.effect_kind)?,
                        None => plan.effect_kind.clone(),
                    };
                    Action::PlanEffect {
                        effect_id,
                        provider,
                        guarantee: plan.guarantee,
                        effect_kind,
                        request: plan.request.copy(custody.work())?,
                        result_schema: plan.result_schema.copy(custody.work())?,
                    }
                }
                None => Action::MarkEffectDispatched {
                    effect_id,
                    attempt_id: crate::EffectAttemptId::from_bytes(effect_id.into_bytes()),
                },
            };
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
                    owner.verify(false).await?;
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
                owner.verify(true).await?;
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
            if current.0.status == attestation.status {
                return clone_status(&attestation.status, retained.work());
            }
            if current.0.attempts != effect.attempts
                || !matches!(
                    current.0.status,
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
                effect.0.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return clone_status(&effect.0.status, &mut None);
            }
            let Some(attestation) = self
                .providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect.0, &mut None)
                .await?
            else {
                return Ok(EffectStatus::Indeterminate);
            };
            self.settle(
                effect_id,
                &effect.0,
                attestation,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await
        })
    }
}
