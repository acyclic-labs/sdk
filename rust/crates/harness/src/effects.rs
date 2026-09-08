//! Replaceable external effect providers with explicit guarantees and reconciliation.

use crate::{
    EffectAttemptId, EffectId, Error, Result,
    core::{AuthorityIssuer, EffectAttestation},
    core::{EffectGuarantee, EffectState, EffectStatus},
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Immutable dispatch request reconstructed from durable effect state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectDispatch {
    /// Provider selected by durable state.
    pub provider: String,
    /// Semantic effect identity shared across attempts.
    pub effect_id: EffectId,
    /// Unique dispatch attempt identity.
    pub attempt_id: EffectAttemptId,
    /// Namespaced provider operation kind.
    pub effect_kind: String,
    /// Stable provider request.
    pub request: Value,
    /// Pinned delivery guarantee.
    pub guarantee: EffectGuarantee,
    /// Canonical digest of the immutable provider request.
    pub request_digest: [u8; 32],
}

impl EffectDispatch {
    /// Reconstructs the only valid provider request from committed effect state.
    pub fn from_state(effect_id: EffectId, effect: &EffectState) -> Result<Self> {
        if effect.status != EffectStatus::Dispatched {
            return Err(Error::Conflict("effect is not awaiting dispatch".into()));
        }
        let attempt_id = effect
            .attempts
            .last()
            .copied()
            .ok_or_else(|| Error::Conflict("effect has no durable dispatch attempt".into()))?;
        Ok(Self {
            provider: effect.provider.clone(),
            effect_id,
            attempt_id,
            effect_kind: effect.effect_kind.clone(),
            request: effect.request.clone(),
            guarantee: effect.guarantee,
            request_digest: effect.request_digest,
        })
    }
}

/// Observation returned by dispatch or later reconciliation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectObservation {
    /// Provider that produced the observation.
    pub provider: String,
    /// Semantic effect being observed.
    pub effect_id: EffectId,
    /// Attempt being observed.
    pub attempt_id: EffectAttemptId,
    /// Canonical request identity echoed by the provider.
    pub request_digest: [u8; 32],
    /// Delivery guarantee under which the attempt ran.
    pub guarantee: EffectGuarantee,
    /// Known terminal result or explicit uncertainty.
    pub status: EffectStatus,
}

/// Provider-owned effect capability boundary.
pub trait EffectProvider: Send + Sync {
    /// Stable provider identity.
    fn id(&self) -> &str;

    /// Delivery guarantees actually supported for an effect kind.
    fn guarantees(&self, effect_kind: &str) -> BTreeSet<EffectGuarantee>;

    /// Whether exactly-once claims are backed by linearizable apply/reconcile semantics.
    fn linearizable_reconciliation(&self) -> bool;

    /// Dispatches one already-durable attempt.
    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>>;

    /// Queries an existing attempt without redispatching it.
    fn reconcile<'a>(
        &'a self,
        attempt_id: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<EffectObservation>>>;
}

/// Explicit registry; no ambient or process-global provider catalog exists.
#[derive(Clone, Default)]
pub struct EffectRegistry(BTreeMap<String, Arc<dyn EffectProvider>>);

impl EffectRegistry {
    /// Registers one provider identity.
    pub fn register(&mut self, provider: Arc<dyn EffectProvider>) -> Result<()> {
        let id = provider.id().to_owned();
        if id.trim().is_empty() {
            return Err(Error::Invalid("effect provider identity is empty".into()));
        }
        if self.0.contains_key(&id) {
            return Err(Error::Conflict(format!(
                "effect provider {id} is already registered"
            )));
        }
        self.0.insert(id, provider);
        Ok(())
    }

    /// Resolves and validates the provider-owned guarantee pinned by durable state.
    pub fn resolve(&self, effect: &EffectState) -> Result<&Arc<dyn EffectProvider>> {
        let provider = self
            .0
            .get(&effect.provider)
            .ok_or_else(|| Error::Unsupported(format!("effect provider {}", effect.provider)))?;
        if !provider
            .guarantees(&effect.effect_kind)
            .contains(&effect.guarantee)
        {
            return Err(Error::Unsupported(format!(
                "provider {} does not support {:?} for {}",
                effect.provider, effect.guarantee, effect.effect_kind
            )));
        }
        if effect.guarantee == EffectGuarantee::ExactlyOnce
            && !provider.linearizable_reconciliation()
        {
            return Err(Error::Unsupported(
                "exactly-once requires linearizable provider reconciliation".into(),
            ));
        }
        Ok(provider)
    }

    /// Validates that a provider observation belongs exactly to durable state.
    pub fn validate_observation(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        observation: &EffectObservation,
    ) -> Result<()> {
        self.resolve(effect)?;
        if observation.provider != effect.provider
            || observation.effect_id != effect_id
            || effect.attempts.last() != Some(&observation.attempt_id)
            || observation.request_digest != effect.request_digest
            || observation.guarantee != effect.guarantee
            || !observation.status.is_terminal()
        {
            return Err(Error::Conflict(
                "effect observation does not match durable dispatch".into(),
            ));
        }
        Ok(())
    }

    /// Dispatches through the pinned provider and attests its exact terminal observation.
    pub async fn dispatch_and_attest(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        dispatch: EffectDispatch,
    ) -> Result<EffectAttestation> {
        let provider = self.resolve(effect)?;
        self.validate_dispatch(effect_id, effect, &dispatch)?;
        let observation = provider.dispatch(dispatch).await?;
        self.attest_provider_observation(issuer, effect_id, effect, observation)
    }

    /// Reconciles through the pinned provider without redispatching the attempt.
    pub async fn reconcile_and_attest(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
    ) -> Result<Option<EffectAttestation>> {
        let provider = self.resolve(effect)?;
        let attempt_id = effect
            .attempts
            .last()
            .copied()
            .ok_or_else(|| Error::Conflict("effect has no durable dispatch attempt".into()))?;
        provider
            .reconcile(attempt_id)
            .await?
            .map(|observation| {
                self.attest_provider_observation(issuer, effect_id, effect, observation)
            })
            .transpose()
    }

    fn validate_dispatch(
        &self,
        effect_id: EffectId,
        effect: &EffectState,
        dispatch: &EffectDispatch,
    ) -> Result<()> {
        if dispatch.provider != effect.provider
            || dispatch.effect_id != effect_id
            || effect.attempts.last() != Some(&dispatch.attempt_id)
            || dispatch.effect_kind != effect.effect_kind
            || dispatch.request != effect.request
            || dispatch.request_digest != effect.request_digest
            || dispatch.guarantee != effect.guarantee
        {
            return Err(Error::Conflict(
                "effect dispatch does not match durable state".into(),
            ));
        }
        Ok(())
    }

    fn attest_provider_observation(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
        observation: EffectObservation,
    ) -> Result<EffectAttestation> {
        self.validate_observation(effect_id, effect, &observation)?;
        if let EffectStatus::Succeeded { result } = &observation.status {
            jsonschema::validator_for(&effect.result_schema)
                .map_err(|error| Error::Invalid(format!("invalid effect result schema: {error}")))?
                .validate(result)
                .map_err(|error| {
                    Error::Invalid(format!("effect result failed validation: {error}"))
                })?;
        }
        issuer.attest_effect(
            effect_id,
            effect,
            observation.attempt_id,
            observation.status,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AggregateKind, Authority};
    use futures::FutureExt as _;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Provider(AtomicUsize);

    impl EffectProvider for Provider {
        fn id(&self) -> &str {
            "example.provider"
        }

        fn guarantees(&self, _: &str) -> BTreeSet<EffectGuarantee> {
            BTreeSet::from([EffectGuarantee::IdempotentRetry])
        }

        fn linearizable_reconciliation(&self) -> bool {
            false
        }

        fn dispatch<'a>(
            &'a self,
            request: EffectDispatch,
        ) -> BoxFuture<'a, Result<EffectObservation>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(EffectObservation {
                    provider: request.provider,
                    effect_id: request.effect_id,
                    attempt_id: request.attempt_id,
                    request_digest: request.request_digest,
                    guarantee: request.guarantee,
                    status: EffectStatus::Succeeded {
                        result: json!({"receipt": "ok"}),
                    },
                })
            }
            .boxed()
        }

        fn reconcile<'a>(
            &'a self,
            _: EffectAttemptId,
        ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
            async { Ok(None) }.boxed()
        }
    }

    #[tokio::test]
    async fn only_an_exact_provider_dispatch_can_be_attested() -> Result<()> {
        let provider = Arc::new(Provider(AtomicUsize::new(0)));
        let mut registry = EffectRegistry::default();
        registry.register(provider.clone())?;
        let effect_id = EffectId::from_bytes([1; 16]);
        let attempt_id = EffectAttemptId::from_bytes([2; 16]);
        let schema = json!({"type": "object", "required": ["receipt"]});
        let state = EffectState {
            provider: "example.provider".into(),
            guarantee: EffectGuarantee::IdempotentRetry,
            effect_kind: "example.write".into(),
            request: json!({"value": 1}),
            request_digest: [3; 32],
            result_schema_digest: *blake3::hash(
                &serde_json::to_vec(&schema).map_err(|error| Error::Invalid(error.to_string()))?,
            )
            .as_bytes(),
            result_schema: schema,
            status: EffectStatus::Dispatched,
            attempts: vec![attempt_id],
        };
        let issuer = AuthorityIssuer::new(
            "host",
            [4; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "task-1".into(),
            },
        );
        let dispatch = EffectDispatch {
            provider: state.provider.clone(),
            effect_id,
            attempt_id,
            effect_kind: state.effect_kind.clone(),
            request: state.request.clone(),
            guarantee: state.guarantee,
            request_digest: state.request_digest,
        };

        let mut changed = dispatch.clone();
        changed.request = json!({"value": 2});
        assert!(matches!(
            registry
                .dispatch_and_attest(&issuer, effect_id, &state, changed)
                .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(provider.0.load(Ordering::SeqCst), 0);

        let attestation = registry
            .dispatch_and_attest(&issuer, effect_id, &state, dispatch)
            .await?;
        assert_eq!(provider.0.load(Ordering::SeqCst), 1);
        assert!(matches!(attestation.status, EffectStatus::Succeeded { .. }));
        Ok(())
    }
}
