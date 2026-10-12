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

    fn dispatch<'a>(&'a self, request: EffectDispatch) -> BoxFuture<'a, Result<EffectObservation>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        async move {
            Ok(EffectObservation {
                provider: request.provider,
                effect_id: request.effect_id,
                attempt_id: request.attempt_id,
                request_digest: request.request_digest,
                guarantee: request.guarantee,
                status: EffectStatus::Succeeded {
                    result: request_file(br#"{"receipt":"ok"}"#)?,
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
    let mut registry = EffectRegistry::new(None, Some(Arc::new(ResultResolver)))?;
    registry.register(provider.clone(), None)?;
    let effect_id = EffectId::from_bytes([1; 16]);
    let attempt_id = EffectAttemptId::from_bytes([2; 16]);
    let schema = json!({"type": "object", "required": ["receipt"]});
    let state = EffectState {
        provider: "example.provider".into(),
        guarantee: EffectGuarantee::IdempotentRetry,
        effect_kind: "example.write".into(),
        request: request_file(br#"{"value":1}"#)?,
        request_digest: [3; 32],
        result_schema: schema_file(&schema)?,
        result_schema_digest: *schema_file(&schema)?.descriptor().sha256(),
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
    changed.request = request_file(br#"{"value":2}"#)?;
    assert!(matches!(
        registry
            .dispatch_and_attest(&issuer, effect_id, &state, changed, &mut None)
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(provider.0.load(Ordering::SeqCst), 0);

    let attestation = registry
        .dispatch_and_attest(&issuer, effect_id, &state, dispatch, &mut None)
        .await?;
    assert_eq!(provider.0.load(Ordering::SeqCst), 1);
    assert!(matches!(attestation.status, EffectStatus::Succeeded { .. }));
    Ok(())
}
