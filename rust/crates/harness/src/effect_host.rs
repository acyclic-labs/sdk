//! Conversation-owned reconciliation of already dispatched provider effects.

use crate::{
    EffectId, Error, IdempotencyKey, OperationId, Result,
    conversation::ContentResidencyVerifier,
    core::{Action, Authority, AuthorityIssuer, Command, EffectStatus, SchemaRegistry, Scope},
    effects::EffectRegistry,
    runtime::DurableEffectObserver,
    store::StreamAggregate,
};
use acyclic_stream::{StreamClient, StreamProvider};
use futures::future::BoxFuture;
use std::sync::Arc;

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
        if !scope.capabilities().contains("effect:run") {
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
        })
    }

    async fn aggregate(&self) -> Result<StreamAggregate<P>> {
        Ok(StreamAggregate::open(
            &self.stream,
            self.authority.clone(),
            self.issuer.verifier(),
            self.schemas.clone(),
        )
        .await?
        .with_content_verifier(Arc::clone(&self.content)))
    }
}

impl<P: StreamProvider> DurableEffectObserver for ConversationEffectHost<P> {
    fn reconcile<'a>(&'a self, effect_id: EffectId) -> BoxFuture<'a, Result<EffectStatus>> {
        Box::pin(async move {
            let aggregate = self.aggregate().await?;
            let effect = aggregate
                .reducer()
                .effect(effect_id)
                .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?
                .clone();
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
            let observed = attestation.status.clone();
            if effect.status == observed {
                return Ok(observed);
            }
            let digest = crate::contract::canonical_json_digest(&(effect_id, &attestation))?;
            let mut operation_bytes = [0_u8; 16];
            operation_bytes.copy_from_slice(&digest[..16]);
            let operation_id = OperationId::from_bytes(operation_bytes);
            for _ in 0..3 {
                let mut aggregate = self.aggregate().await?;
                let current = aggregate
                    .reducer()
                    .effect(effect_id)
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
                    idempotency_key: IdempotencyKey::new(format!(
                        "effect-reconcile:{operation_id}"
                    ))?,
                    expected_revision: aggregate.reducer().revision(),
                    scope: self.scope.clone(),
                    causal_parent: None,
                    action: Action::ResolveEffect {
                        observation: attestation.clone(),
                    },
                };
                match aggregate.execute(command).await {
                    Ok(_) => return Ok(observed),
                    Err(Error::Conflict(_)) => continue,
                    Err(error) => return Err(error),
                }
            }
            Err(Error::Indeterminate(operation_id))
        })
    }
}
