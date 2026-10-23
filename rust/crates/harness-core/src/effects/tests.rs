use super::*;
use crate::core::{AggregateKind, Authority};
use crate::{
    AgentId,
    conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};
use futures::FutureExt as _;
use serde_json::json;

fn request_file(bytes: &[u8]) -> Result<FileRef> {
    FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "filesystem".into(), "2".into(), &mut None)?,
            "effects".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
            &mut None,
        )?,
        "requests/request.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(bytes, "application/json", &mut None)?,
        "request.json".into(),
        &mut None,
    )
}
fn schema_file(schema: &Value) -> Result<FileRef> {
    let bytes = serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?;
    FileRef::new(
        request_file(b"null")?.volume().clone(),
        "schemas/result.json".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(&bytes, "application/schema+json", &mut None)?,
        "result.json".into(),
        &mut None,
    )
}

struct ResultResolver;
impl ContentResidencyVerifier for ResultResolver {
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        _ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "fixture has no finite content reader".into(),
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let bytes = if reference.descriptor().media_type() == "application/schema+json" {
                serde_json::to_vec(&json!({"type":"object","required":["receipt"]}))
                    .map_err(|error| Error::Invalid(error.to_string()))?
            } else {
                br#"{"receipt":"ok"}"#.to_vec()
            };
            if bytes.len() as u64 > maximum_bytes {
                return Err(Error::Invalid("content byte allowance exceeded".into()));
            }
            reference.descriptor().verify(&bytes, &mut None)?;
            Ok(bytes.into())
        })
    }
}
use std::sync::atomic::{AtomicUsize, Ordering};

struct Provider(
    AtomicUsize,
    std::sync::Mutex<Option<(AuthorityIssuer, crate::core::Scope)>>,
);
impl Provider {
    fn revoke(&self) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        if let Some((issuer, scope)) = self
            .1
            .lock()
            .map_err(|_| Error::Storage("test provider lock poisoned".into()))?
            .take()
        {
            issuer.revoke_workload(&scope)?;
        }
        Ok(())
    }
}

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

    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>> {
        let admitted = self.revoke();
        async move {
            admitted?;
            Ok(EffectObservation {
                provider: request.provider,
                effect_id: request.effect_id,
                attempt_id: request.attempt_id,
                request_digest: request.request_digest,
                guarantee: request.guarantee,
                status: EffectStatus::Succeeded {
                    result: (request_file(br#"{"receipt":"ok"}"#)?).into(),
                },
            })
        }
        .boxed()
    }

    fn reconcile<'a>(
        &'a self,
        _: EffectAttemptId,
    ) -> BoxFuture<'a, Result<Option<EffectObservation>>> {
        let admitted = self.revoke();
        async move {
            admitted?;
            Ok(None)
        }
        .boxed()
    }
}

struct Fixture {
    registry: EffectRegistry,
    provider: Arc<Provider>,
    issuer: AuthorityIssuer,
    scope: crate::core::Scope,
    state: EffectState,
    effect_id: EffectId,
}
fn fixture() -> Result<Fixture> {
    let provider = Arc::new(Provider(AtomicUsize::new(0), std::sync::Mutex::new(None)));
    let mut registry = EffectRegistry::new(None, Some(Arc::new(ResultResolver)))?;
    registry.register(provider.clone(), None)?;
    let effect_id = EffectId::from_bytes([1; 16]);
    let attempt_id = EffectAttemptId::from_bytes([2; 16]);
    let schema = json!({"type": "object", "required": ["receipt"]});
    let issuer = AuthorityIssuer::new(
        "host",
        [4; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "task-1".into(),
        },
    );
    let scope = issuer.issue(
        "planner",
        &crate::Capabilities::new([
            "event:append",
            "effect:plan",
            "effect:run",
            "effect:provider:example.provider",
        ]),
        None,
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let mut reducer = crate::core::Reducer::new(
        verifier.audience().clone(),
        verifier,
        crate::core::SchemaRegistry::new(),
    );
    for (ordinal, action) in [
        crate::core::Action::PlanEffect {
            effect_id,
            provider: "example.provider".into(),
            guarantee: EffectGuarantee::IdempotentRetry,
            effect_kind: "example.write".into(),
            request: request_file(br#"{"value":1}"#)?.into(),
            result_schema: schema_file(&schema)?.into(),
        },
        crate::core::Action::MarkEffectDispatched {
            effect_id,
            attempt_id,
        },
    ]
    .into_iter()
    .enumerate()
    {
        reducer.apply(&crate::core::Command {
            operation_id: crate::OperationId::from_bytes(
                [u8::try_from(ordinal).map_err(|error| Error::Invalid(error.to_string()))? + 1; 16],
            ),
            idempotency_key: crate::IdempotencyKey(format!("{ordinal}")),
            expected_revision: ordinal as u64,
            scope: scope.clone(),
            causal_parent: None,
            action,
        })?;
    }
    let state = reducer.effect(effect_id).unwrap().clone();
    Ok(Fixture {
        registry,
        provider,
        issuer,
        scope,
        state,
        effect_id,
    })
}

#[tokio::test]
async fn durable_dispatch_produces_a_validated_attestation() -> Result<()> {
    let Fixture {
        registry,
        provider,
        issuer,
        state,
        effect_id,
        ..
    } = fixture()?;
    let attestation = registry
        .dispatch_and_attest(&issuer, effect_id, &state, &mut None)
        .await?;
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    assert!(matches!(attestation.status, EffectStatus::Succeeded { .. }));
    Ok(())
}

#[tokio::test]
async fn revoked_plans_never_enter_dispatch_or_reconciliation() -> Result<()> {
    let f = fixture()?;
    f.issuer.revoke_workload(&f.scope)?;
    assert!(
        f.registry
            .dispatch_and_attest(&f.issuer, f.effect_id, &f.state, &mut None)
            .await
            .is_err()
    );
    assert!(
        f.registry
            .reconcile_and_attest(&f.issuer, f.effect_id, &f.state, &mut None)
            .await
            .is_err()
    );
    assert_eq!(f.provider.0.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn revocation_during_a_provider_callback_rejects_its_observation() -> Result<()> {
    for reconcile in [false, true] {
        let f = fixture()?;
        *f.provider.1.lock().unwrap() = Some((f.issuer.clone(), f.scope));
        if reconcile {
            assert!(
                f.registry
                    .reconcile_and_attest(&f.issuer, f.effect_id, &f.state, &mut None)
                    .await
                    .is_err()
            );
        } else {
            assert!(
                f.registry
                    .dispatch_and_attest(&f.issuer, f.effect_id, &f.state, &mut None)
                    .await
                    .is_err()
            );
        }
        assert_eq!(f.provider.0.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn a_valid_planning_workload_cannot_be_reused_for_another_request() -> Result<()> {
    let mut f = fixture()?;
    f.state.effect_kind = "example.widened".into();
    assert!(
        f.registry
            .dispatch_and_attest(&f.issuer, f.effect_id, &f.state, &mut None)
            .await
            .is_err()
    );
    assert!(
        f.registry
            .reconcile_and_attest(&f.issuer, f.effect_id, &f.state, &mut None)
            .await
            .is_err()
    );
    assert_eq!(f.provider.0.load(Ordering::SeqCst), 0);
    Ok(())
}
