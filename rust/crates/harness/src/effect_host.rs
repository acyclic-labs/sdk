//! Conversation-owned effect admission, dispatch and reconciliation.

use crate::contract::capability;
use crate::{
    EffectId, Error, IdempotencyKey, OperationId, Result,
    conversation::ContentResidencyVerifier,
    core::{
        Action, Authority, AuthorityIssuer, Command, EffectAttestation, EffectState, EffectStatus,
        SchemaRegistry, Scope,
    },
    effects::EffectRegistry,
    runtime::DurableEffectObserver,
    store::{
        DEFAULT_TERMINAL_EFFECTS, HistoryReadLimits, StreamAggregate,
        default_projection_read_limits,
    },
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use acyclic_stream::{StreamClient, StreamProvider};
use std::sync::Arc;

/// Immutable provider selection and ref-only request for one task command.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskEffectPlan {
    /// Exact registered provider identity.
    pub provider: String,
    /// Pinned delivery strength supported by that provider.
    pub guarantee: crate::core::EffectGuarantee,
    /// Versioned provider operation kind.
    pub effect_kind: String,
    /// Immutable request, readable under the retained task grants.
    pub request: crate::conversation::FileRef,
    /// Immutable result schema, readable under the same grants.
    pub result_schema: crate::conversation::FileRef,
}

#[cfg(feature = "filesystem")]
pub(crate) fn task_effect_id(task: crate::TaskId, command: OperationId) -> Result<EffectId> {
    let digest =
        crate::contract::canonical_json_digest(&("harness/v2/task-effect", task, command))?;
    let mut identity = [0; 16];
    identity.copy_from_slice(&digest[..16]);
    Ok(EffectId::from_bytes(identity))
}

/// Resolves a pinned provider attempt into its owning conversation history.
/// Callers must separately authenticate which tasks may ask about that history.
pub struct ConversationEffectHost<P> {
    stream: StreamClient<P>,
    authority: Authority,
    issuer: AuthorityIssuer,
    scope: Scope,
    schemas: SchemaRegistry,
    content: Arc<dyn ContentResidencyVerifier>,
    providers: EffectRegistry,
    effect_history_limits: HistoryReadLimits,
    terminal_effect_limit: usize,
}

impl<P: StreamProvider> ConversationEffectHost<P> {
    /// Binds one conversation, its signed effect-run scope and provider catalog.
    pub fn new(
        stream: StreamClient<P>,
        authority: Authority,
        issuer: AuthorityIssuer,
        scope: Scope,
        schemas: SchemaRegistry,
        content: Arc<dyn ContentResidencyVerifier>,
        providers: EffectRegistry,
    ) -> Result<Self> {
        if authority.kind != crate::core::AggregateKind::Conversation {
            return Err(Error::Invalid(
                "effect host requires a conversation aggregate".into(),
            ));
        }
        let verifier = issuer.verifier();
        verifier.verify_audience(&authority)?;
        verifier.verify(&scope)?;
        if !scope.capabilities().contains(capability::EFFECT_RUN) {
            return Err(Error::Unauthorized(
                "effect host scope lacks effect:run".into(),
            ));
        }
        Ok(Self {
            stream,
            authority,
            issuer,
            scope,
            schemas,
            content,
            providers,
            effect_history_limits: default_projection_read_limits(),
            terminal_effect_limit: DEFAULT_TERMINAL_EFFECTS,
        })
    }

    /// Configures bounded original-effect lookup for archived result/retry reads.
    pub fn with_effect_history_read_limits(mut self, limits: HistoryReadLimits) -> Result<Self> {
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "effect history bounds must be positive".into(),
            ));
        }
        self.effect_history_limits = limits;
        Ok(self)
    }

    /// Configures only the resident success/failure cache; active uncertainty
    /// stays resident and every archived observation keeps its original proof.
    pub fn with_resident_terminal_effect_limit(mut self, maximum: usize) -> Result<Self> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "terminal effect cache limit must be positive".into(),
            ));
        }
        self.terminal_effect_limit = maximum;
        Ok(self)
    }

    async fn aggregate(&self) -> Result<StreamAggregate<P>> {
        self.aggregate_on(&self.stream).await
    }

    async fn aggregate_on(&self, stream: &StreamClient<P>) -> Result<StreamAggregate<P>> {
        StreamAggregate::open_with_projection_limits(
            stream,
            self.authority.clone(),
            self.issuer.verifier(),
            self.schemas.clone(),
            default_projection_read_limits(),
            self.terminal_effect_limit,
        )
        .await?
        .with_content_verifier(Arc::clone(&self.content))
        .with_effect_history_read_limits(self.effect_history_limits)
    }

    /// Runs one task command through the existing conversation effect lifecycle.
    /// Only the winner of the durable dispatch append calls the provider. Every
    /// later invocation reconciles that attempt; even an idempotent provider is
    /// never implicitly retried here. Policy/approval precedes this admission.
    /// Task history uses the owner's Stream client for both reads and commits,
    /// keeping the lease condition and effect append in one provider transaction.
    #[cfg(feature = "filesystem")]
    pub async fn run_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: TaskEffectPlan,
    ) -> Result<EffectStatus> {
        use crate::{EffectAttemptId, core::ApplyResult, effects::EffectDispatch};
        owner.require_effect_grants(&plan.provider, true)?;
        owner.validate_input_file(&plan.request)?;
        owner.validate_input_file(&plan.result_schema)?;
        let effect_id = task_effect_id(owner.task_binding().0, command_id)?;
        self.task_action(
            owner,
            effect_id,
            "plan",
            Action::PlanEffect {
                effect_id,
                provider: plan.provider,
                guarantee: plan.guarantee,
                effect_kind: plan.effect_kind,
                request: plan.request,
                result_schema: plan.result_schema,
            },
        )
        .await?;
        let mut effect = self
            .aggregate_on(&owner.stream())
            .await?
            .effect(effect_id, self.effect_history_limits)
            .await?
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
        if !matches!(
            effect.status,
            EffectStatus::Planned | EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            return Ok(effect.status);
        }
        self.providers.resolve(&effect)?;
        let dispatched = if effect.status == EffectStatus::Planned {
            matches!(
                self.task_action(
                    owner,
                    effect_id,
                    "dispatch",
                    Action::MarkEffectDispatched {
                        effect_id,
                        attempt_id: EffectAttemptId::from_bytes(effect_id.into_bytes()),
                    }
                )
                .await?,
                ApplyResult::Applied { .. }
            )
        } else {
            false
        };
        effect = self
            .aggregate_on(&owner.stream())
            .await?
            .effect(effect_id, self.effect_history_limits)
            .await?
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
        if !matches!(
            effect.status,
            EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            owner.verify(true).await?;
            return Ok(effect.status);
        }
        let attestation = if dispatched {
            owner.verify(false).await?;
            Some(
                self.providers
                    .dispatch_and_attest(
                        &self.issuer,
                        effect_id,
                        &effect,
                        EffectDispatch::from_state(effect_id, &effect)?,
                    )
                    .await?,
            )
        } else {
            self.providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect)
                .await?
        };
        match attestation {
            Some(attestation) => {
                self.settle(effect_id, &effect, attestation, Some(owner))
                    .await
            }
            None => Ok(EffectStatus::Indeterminate),
        }
    }

    /// Observes an exact retained task command, including after cancellation.
    /// It cannot plan or dispatch work and cannot adopt another command's effect.
    #[cfg(feature = "filesystem")]
    pub async fn reconcile_task_effect(
        &self,
        owner: &crate::durable_host::TaskJournalOwner<P>,
        command_id: OperationId,
        plan: &TaskEffectPlan,
    ) -> Result<EffectStatus> {
        owner.verify(true).await?;
        owner.require_effect_grants(&plan.provider, false)?;
        owner.validate_input_file(&plan.request)?;
        owner.validate_input_file(&plan.result_schema)?;
        let effect_id = task_effect_id(owner.task_binding().0, command_id)?;
        let effect = self
            .aggregate_on(&owner.stream())
            .await?
            .effect(effect_id, self.effect_history_limits)
            .await?
            .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
        if effect.provider != plan.provider
            || effect.guarantee != plan.guarantee
            || effect.effect_kind != plan.effect_kind
            || effect.request != plan.request
            || effect.result_schema != plan.result_schema
        {
            return Err(Error::Conflict(
                "task effect differs from retained command".into(),
            ));
        }
        if !matches!(
            effect.status,
            EffectStatus::Dispatched | EffectStatus::Indeterminate
        ) {
            return Ok(effect.status);
        }
        match self
            .providers
            .reconcile_and_attest(&self.issuer, effect_id, &effect)
            .await?
        {
            Some(attestation) => {
                self.settle(effect_id, &effect, attestation, Some(owner))
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
        action: Action,
    ) -> Result<crate::core::ApplyResult> {
        let digest = crate::contract::canonical_json_digest(&("task-effect", effect_id, phase))?;
        let mut identity = [0; 16];
        identity.copy_from_slice(&digest[..16]);
        let operation_id = OperationId::from_bytes(identity);
        loop {
            let mut aggregate = self.aggregate_on(&owner.stream()).await?;
            let expected_revision = aggregate
                .reducer()
                .operation_revision(operation_id)
                .map_or(aggregate.reducer().revision(), |revision| revision - 1);
            let command = Command {
                operation_id,
                idempotency_key: IdempotencyKey::new(format!("task-effect:{effect_id}:{phase}"))?,
                expected_revision,
                scope: self.scope.clone(),
                causal_parent: None,
                action: action.clone(),
            };
            match aggregate.execute_task_command(command, owner).await {
                Err(Error::Conflict(_)) => {
                    // A changed immutable operation must fail instead of spinning.
                    if aggregate
                        .reducer()
                        .operation_revision(operation_id)
                        .is_some()
                    {
                        return Err(Error::Conflict(
                            "task effect command changed after admission".into(),
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
    ) -> Result<EffectStatus> {
        let observed = attestation.status.clone();
        let digest = crate::contract::canonical_json_digest(&(effect_id, &attestation))?;
        let mut operation_bytes = [0_u8; 16];
        operation_bytes.copy_from_slice(&digest[..16]);
        let operation_id = OperationId::from_bytes(operation_bytes);
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
            let mut aggregate = self.aggregate_on(&stream).await?;
            let current = aggregate
                .effect(effect_id, self.effect_history_limits)
                .await?
                .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
            if current.status == observed {
                return Ok(observed);
            }
            if current.attempts != effect.attempts
                || !matches!(
                    current.status,
                    EffectStatus::Dispatched | EffectStatus::Indeterminate
                )
            {
                return Err(Error::Conflict(
                    "effect attempt changed during reconciliation".into(),
                ));
            }
            let command = Command {
                operation_id,
                idempotency_key: IdempotencyKey::new(format!("effect-reconcile:{operation_id}"))?,
                expected_revision: aggregate.reducer().revision(),
                scope: self.scope.clone(),
                causal_parent: None,
                action: Action::ResolveEffect {
                    observation: attestation.clone(),
                },
            };
            #[cfg(feature = "filesystem")]
            let result = match owner {
                Some(owner) => aggregate.execute_task_command(command, owner).await,
                None => aggregate.execute(command).await,
            };
            #[cfg(not(feature = "filesystem"))]
            let result = aggregate.execute(command).await;
            match result {
                Ok(_) => return Ok(observed),
                Err(Error::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

impl<P: StreamProvider> DurableEffectObserver for ConversationEffectHost<P> {
    fn reconcile<'a>(&'a self, effect_id: EffectId) -> BoxFuture<'a, Result<EffectStatus>> {
        Box::pin(async move {
            let aggregate = self.aggregate().await?;
            let effect = aggregate
                .effect(effect_id, self.effect_history_limits)
                .await?
                .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
            if !matches!(
                effect.status,
                EffectStatus::Dispatched | EffectStatus::Indeterminate
            ) {
                return Ok(effect.status);
            }
            let Some(attestation) = self
                .providers
                .reconcile_and_attest(&self.issuer, effect_id, &effect)
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
            )
            .await
        })
    }
}
