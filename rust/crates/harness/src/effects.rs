//! Replaceable external effect providers with explicit guarantees and reconciliation.

use crate::{
    EffectAttemptId, EffectId, Error, Result,
    conversation::{ContentResidencyVerifier, FileRef},
    core::{AuthorityIssuer, EffectAttestation},
    core::{EffectGuarantee, EffectState, EffectStatus},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
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
    pub request: FileRef,
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
pub trait EffectProvider: acyclic_stream::ProviderPlatform {
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

    /// Dispatches the same durable attempt on the supplied original ledger.
    /// Unsupported providers reject before any provider work. The native future
    /// owns its real box custody until after the physical box is dropped.
    #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
    fn dispatch_with_native_work<'a>(
        &'a self, _request: EffectDispatch,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (EffectObservation, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unsupported, reason:"effect provider has no native dispatch observer" })
    }

    /// Observes only the original attempt, retaining actual returned metadata.
    #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
    fn reconcile_with_native_work<'a>(
        &'a self, _attempt_id: EffectAttemptId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, Option<(EffectObservation, acyclic_fs::NativeAllocation)>>> {
        Err(Error::NativeBoundary { code:crate::wire::ErrorCode::Unsupported, reason:"effect provider has no native reconciliation observer" })
    }

    /// Tests the pinned guarantee without constructing an unbounded catalog.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_guarantee_with_native_work(
        &self, _effect_kind: &str, _guarantee: EffectGuarantee,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "effect provider has no native guarantee observer",
        })
    }
}

/// Explicit registry; no ambient or process-global provider catalog exists.
#[derive(Default)]
pub struct EffectRegistry {
    providers: BTreeMap<String, Arc<dyn EffectProvider>>,
    result_resolver: Option<Arc<dyn ContentResidencyVerifier>>,
    // Payload fields drop before the original ledger's custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_metadata: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for EffectRegistry {
    fn clone(&self) -> Self {
        Self {
            providers: self.providers.clone(),
            result_resolver: self.result_resolver.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata: None,
        }
    }
}

/// The provider observation is dropped before its original allocation custody.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct NativeEffectObservation {
    observation: EffectObservation,
    custody: acyclic_fs::NativeAllocation,
}

/// Single admission check for a pinned successful JSON result.
pub(crate) fn validate_result_bytes(schema: &Value, result: &FileRef, bytes: &[u8]) -> Result<()> {
    result.validate()?;
    if result.descriptor().media_type() != "application/json" {
        return Err(Error::Invalid("effect result must be JSON content".into()));
    }
    result.descriptor().verify(bytes)?;
    let value: Value = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("effect result is not JSON: {error}")))?;
    crate::contract::validate_json_schema_value(schema, &value, "effect result")?;
    Ok(())
}

/// Parses one pinned schema and rejects invalid or mismatched bytes before publication.
pub(crate) fn validate_schema_bytes(reference: &FileRef, bytes: &[u8]) -> Result<Value> {
    reference.validate()?;
    if reference.descriptor().media_type() != "application/schema+json" {
        return Err(Error::Invalid(
            "effect result schema must be JSON Schema content".into(),
        ));
    }
    reference.descriptor().verify(bytes)?;
    let schema: Value = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("effect result schema is not JSON: {error}")))?;
    crate::contract::compile_json_schema(&schema, "effect result")?;
    Ok(schema)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_effect_reference_with_work(
    reference: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
    }
    // All fields are private and immutable; new and the guarded typed serde
    // constructor already enforce this original metadata predicate. Rechecking
    // the same predicate on an existing typed reference allocates nothing.
    reference.validate()
}

/// Validates the original schema without letting an unguarded Value escape.
pub(crate) fn validate_effect_schema_bytes_with_work(
    reference: &FileRef, bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        return validate_schema_bytes(reference, bytes).map(drop);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        validate_effect_reference_with_work(reference, work)?;
        if reference.descriptor().media_type() != "application/schema+json" {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                "effect result schema must be JSON Schema content"));
        }
        reference.descriptor().verify_with_work(bytes, work)?;
        let original = work.as_deref().and_then(|io| io.original_native_work())
            .ok_or_else(|| Error::Unsupported("effect schema observer has no original native ledger".into()))?;
        let decode = crate::filesystem::PreparationIo::from_native_work(original)?;
        let schema = decode.decode_preparation::<Value>(bytes)?;
        drop(crate::contract::compile_json_schema_with_io(
            &schema, "effect result", Some(&decode))?);
        return Ok(());
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    Err(Error::Unsupported("native effect schema validation is unavailable".into()))
}

/// Shared store/registry result admission. Source bytes stay caller-guarded;
/// decoded schema and result own their original reservations through validation.
pub(crate) fn validate_effect_result_bytes_with_work(
    schema_reference: &FileRef, schema_bytes: &[u8], result: &FileRef, bytes: &[u8],
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        let schema = validate_schema_bytes(schema_reference, schema_bytes)?;
        return validate_result_bytes(&schema, result, bytes);
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        validate_effect_reference_with_work(schema_reference, work)?;
        validate_effect_reference_with_work(result, work)?;
        if schema_reference.descriptor().media_type() != "application/schema+json" {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Invalid,
                "effect result schema must be JSON Schema content"));
        }
        schema_reference.descriptor().verify_with_work(schema_bytes, work)?;
        let original = work.as_deref().and_then(|io| io.original_native_work())
            .ok_or_else(|| Error::Unsupported("effect result observer has no original native ledger".into()))?;
        let decode = crate::filesystem::PreparationIo::from_native_work(original)?;
        let schema = decode.decode_preparation::<Value>(schema_bytes)?;
        // Preserve schema admission before result parsing/allocation, as in the
        // original validate_schema_bytes + validate_result_bytes sequence.
        drop(crate::contract::compile_json_schema_with_io(
            &schema, "effect result", Some(&decode))?);
        if result.descriptor().media_type() != "application/json" {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect result must be JSON content",
            });
        }
        result.descriptor().verify_with_native_work(bytes, original)?;
        let value = decode.decode_preparation::<Value>(bytes)?;
        return crate::contract::validate_json_schema_value_with_io(
            &schema, &value, "effect result", Some(&decode));
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    Err(Error::Unsupported("native effect result validation is unavailable".into()))
}

impl EffectRegistry {
    /// Creates an empty ordinary provider catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates the original catalog with intrinsic logical metadata custody.
    /// BTree allocator internals and provider-owned backing are not claimed.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        let native_metadata = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Self {
            providers: BTreeMap::new(),
            result_resolver: None,
            native_metadata: Some(native_metadata),
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_native_metadata(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        if self.native_metadata.as_ref().is_some_and(|guard| guard.work().shares_ledger(work)) {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect catalog does not retain the original native metadata",
            })
        }
    }

    /// Installs the same owner-mediated resolver without copying its backing.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn with_result_resolver_with_native_work(
        mut self, resolver: Arc<dyn ContentResidencyVerifier>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        self.require_native_metadata(work)?;
        let io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.check_cancellation()?;
        self.result_resolver = Some(resolver);
        Ok(self)
    }

    /// Copies only SDK-owned identity strings and logical map entry slots.
    /// Provider and resolver Arcs continue to share their existing backing.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut cloned = Self::new_with_native_work(work)?;
        for provider in self.providers.values() {
            cloned.register_with_native_work(Arc::clone(provider), work)?;
        }
        cloned.result_resolver = self.result_resolver.clone();
        Ok(cloned)
    }

    /// Registers on the original catalog, admitting its identity before growth.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn register_with_native_work(
        &mut self, provider: Arc<dyn EffectProvider>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        use crate::store::history::HistoryIo as _;
        self.require_native_metadata(work)?;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.check_cancellation()?;
        let source = provider.id();
        if source.trim().is_empty() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "effect provider identity is empty",
            });
        }
        if self.providers.contains_key(source) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "effect provider is already registered",
            });
        }
        let slot = std::mem::size_of::<(String, Arc<dyn EffectProvider>)>() as u64;
        io.admit_owned(slot)?;
        io.retain_owned(slot)?;
        let mut id = String::new();
        io.reserve_text(&mut id, source.len())?;
        io.admit_copy(source.len() as u64)?;
        id.push_str(source);
        io.copy_bytes(source.len() as u64)?;
        let mut custody = io.into_custody();
        self.native_metadata.as_mut().expect("validated native catalog")
            .absorb(&mut custody)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.providers.insert(id, provider);
        Ok(())
    }

    /// Installs the owner-mediated byte boundary required to validate successful results.
    #[must_use]
    pub fn with_result_resolver(mut self, resolver: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.result_resolver = Some(resolver);
        self
    }

    /// Registers one provider identity.
    pub fn register(&mut self, provider: Arc<dyn EffectProvider>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(guard) = &self.native_metadata {
            let work = guard.work().clone();
            return self.register_with_native_work(provider, &work);
        }
        let id = provider.id().to_owned();
        if id.trim().is_empty() {
            return Err(Error::Invalid("effect provider identity is empty".into()));
        }
        if self.providers.contains_key(&id) {
            return Err(Error::Conflict(format!(
                "effect provider {id} is already registered"
            )));
        }
        self.providers.insert(id, provider);
        Ok(())
    }

    /// Resolves and validates the provider-owned guarantee pinned by durable state.
    pub fn resolve(&self, effect: &EffectState) -> Result<&Arc<dyn EffectProvider>> {
        self.resolve_with_work(effect, &mut None)
    }

    pub(crate) fn resolve_with_work(
        &self, effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&Arc<dyn EffectProvider>> {
        self.resolve_fields_with_work(
            &effect.provider, &effect.effect_kind, effect.guarantee, work)
    }

    fn resolve_fields_with_work(
        &self, provider_name: &str, effect_kind: &str, guarantee: EffectGuarantee,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&Arc<dyn EffectProvider>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            return self.resolve_fields_with_native_work(provider_name, effect_kind, guarantee, native);
        }
        if work.is_some() {
            return Err(Error::Unsupported("effect observer has no original native ledger".into()));
        }
        let provider = self.providers.get(provider_name).ok_or_else(||
            crate::effect_host::effect_error(work, crate::wire::ErrorCode::Unsupported,
                "effect provider is not registered"))?;
        if !provider.guarantees(effect_kind).contains(&guarantee) {
            return Err(Error::Unsupported(format!(
                "provider {provider_name} does not support {guarantee:?} for {effect_kind}")));
        }
        if guarantee == EffectGuarantee::ExactlyOnce
            && !provider.linearizable_reconciliation()
        {
            return Err(Error::Unsupported(
                "exactly-once requires linearizable provider reconciliation".into()));
        }
        Ok(provider)
    }

    /// Checks the original provider SPI before any native future allocation or IO.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn resolve_fields_with_native_work(
        &self, provider_name: &str, effect_kind: &str, guarantee: EffectGuarantee,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&Arc<dyn EffectProvider>> {
        self.require_native_metadata(work)?;
        work.visit(1).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let provider = self.providers.get(provider_name).ok_or_else(|| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "effect provider is not registered",
        })?;
        if !provider.supports_guarantee_with_native_work(effect_kind, guarantee, work)? {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "effect provider does not support the pinned guarantee",
            });
        }
        if guarantee == EffectGuarantee::ExactlyOnce && !provider.linearizable_reconciliation() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "exactly-once requires linearizable provider reconciliation",
            });
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
        self.validate_observation_with_work(effect_id, effect, observation, &mut None)
    }

    fn validate_observation_with_work(
        &self, effect_id: EffectId, effect: &EffectState, observation: &EffectObservation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.resolve_with_work(effect, work)?;
        if observation.provider != effect.provider || observation.effect_id != effect_id
            || effect.attempts.last() != Some(&observation.attempt_id)
            || observation.request_digest != effect.request_digest
            || observation.guarantee != effect.guarantee || !observation.status.is_terminal()
        {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Conflict,
                "effect observation does not match durable dispatch"));
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
        self.dispatch_and_attest_with_work(issuer, effect_id, effect, dispatch, &mut None).await
    }

    pub(crate) async fn dispatch_state_and_attest_with_work(
        &self, issuer: &AuthorityIssuer, effect_id: EffectId, effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        if effect.status != EffectStatus::Dispatched {
            return Err(crate::effect_host::effect_error(custody.work(),
                crate::wire::ErrorCode::Conflict, "effect is not awaiting dispatch"));
        }
        let attempt_id = effect.attempts.last().copied().ok_or_else(||
            crate::effect_host::effect_error(custody.work(), crate::wire::ErrorCode::Conflict,
                "effect has no durable dispatch attempt"))?;
        let provider = match custody.work().as_deref_mut() {
            Some(io) => io.clone_text(&effect.provider)?, None => effect.provider.clone(),
        };
        custody.adopt(provider.capacity() as u64)?;
        let effect_kind = match custody.work().as_deref_mut() {
            Some(io) => io.clone_text(&effect.effect_kind)?, None => effect.effect_kind.clone(),
        };
        custody.adopt(effect_kind.capacity() as u64)?;
        let request = crate::effect_host::clone_file_with_work(&effect.request, custody.work())?;
        custody.adopt(request.logical_owned_bytes()?)?;
        let dispatch = EffectDispatch {
            provider, effect_id, attempt_id, effect_kind, request,
            guarantee: effect.guarantee, request_digest: effect.request_digest,
        };
        self.dispatch_and_attest_with_work(issuer, effect_id, effect, dispatch, custody.work()).await
    }

    async fn dispatch_and_attest_with_work(
        &self, issuer: &AuthorityIssuer, effect_id: EffectId, effect: &EffectState,
        dispatch: EffectDispatch,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        let provider = self.resolve_with_work(effect, work)?;
        self.validate_dispatch_with_work(effect_id, effect, &dispatch, work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            let future = provider.dispatch_with_native_work(dispatch, native)?;
            let (observation, custody) = future.await?;
            return self.attest_native_observation(issuer, effect_id, effect,
                NativeEffectObservation { observation, custody }, work).await;
        }
        if work.is_some() {
            return Err(Error::Unsupported("effect observer has no original native ledger".into()));
        }
        let observation = provider.dispatch(dispatch).await?;
        self.attest_provider_observation_with_work(
            issuer, effect_id, effect, observation, work).await
    }

    /// Reconciles through the pinned provider without redispatching the attempt.
    pub async fn reconcile_and_attest(
        &self,
        issuer: &AuthorityIssuer,
        effect_id: EffectId,
        effect: &EffectState,
    ) -> Result<Option<EffectAttestation>> {
        self.reconcile_and_attest_with_work(issuer, effect_id, effect, &mut None).await
    }

    pub(crate) async fn reconcile_and_attest_with_work(
        &self, issuer: &AuthorityIssuer, effect_id: EffectId, effect: &EffectState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<EffectAttestation>> {
        let provider = self.resolve_with_work(effect, work)?;
        let attempt_id = effect.attempts.last().copied().ok_or_else(||
            crate::effect_host::effect_error(work, crate::wire::ErrorCode::Conflict,
                "effect has no durable dispatch attempt"))?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            let future = provider.reconcile_with_native_work(attempt_id, native)?;
            return match future.await? {
                Some((observation, custody)) => Ok(Some(self.attest_native_observation(
                    issuer, effect_id, effect, NativeEffectObservation { observation, custody },
                    work).await?)),
                None => Ok(None),
            };
        }
        if work.is_some() {
            return Err(Error::Unsupported("effect observer has no original native ledger".into()));
        }
        match provider.reconcile(attempt_id).await? {
            Some(observation) => Ok(Some(self.attest_provider_observation_with_work(
                issuer, effect_id, effect, observation, work).await?)),
            None => Ok(None),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn attest_native_observation(
        &self, issuer: &AuthorityIssuer, effect_id: EffectId, effect: &EffectState,
        incoming: NativeEffectObservation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        if !work.as_deref().and_then(|io| io.original_native_work())
            .is_some_and(|native| incoming.custody.work().shares_ledger(native))
        {
            let NativeEffectObservation { observation, custody } = incoming;
            drop(observation);
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(custody),
            });
        }
        let provider_bytes = incoming.observation.provider.capacity() as u64;
        let NativeEffectObservation { observation, custody } = incoming;
        let result = self.attest_provider_observation_with_work(
            issuer, effect_id, effect, observation, work).await;
        match result {
            Ok(attestation) => {
                let io = work.as_deref_mut().expect("original observer");
                io.absorb_native_custody(custody)?;
                // Signing moved the status, but dropped the original echoed
                // provider label. Only its new attestation label remains live.
                io.release_owned(provider_bytes)?;
                Ok(attestation)
            },
            Err(error) => {
                drop(custody);
                Err(error)
            },
        }
    }

    fn validate_dispatch_with_work(
        &self, effect_id: EffectId, effect: &EffectState, dispatch: &EffectDispatch,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if dispatch.provider != effect.provider || dispatch.effect_id != effect_id
            || effect.attempts.last() != Some(&dispatch.attempt_id)
            || dispatch.effect_kind != effect.effect_kind || dispatch.request != effect.request
            || dispatch.request_digest != effect.request_digest || dispatch.guarantee != effect.guarantee
        {
            return Err(crate::effect_host::effect_error(work, crate::wire::ErrorCode::Conflict,
                "effect dispatch does not match durable state"));
        }
        Ok(())
    }

    async fn attest_provider_observation_with_work(
        &self, issuer: &AuthorityIssuer, effect_id: EffectId, effect: &EffectState,
        observation: EffectObservation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<EffectAttestation> {
        self.validate_observation_with_work(effect_id, effect, &observation, work)?;
        if let EffectStatus::Succeeded { result } = &observation.status {
            let resolver = self.result_resolver.as_ref().ok_or_else(||
                crate::effect_host::effect_error(work, crate::wire::ErrorCode::Unsupported,
                    "effect result content resolver is not bound"))?;
            if work.is_some() {
                let mut content = crate::wire_codec::CodecQuota::new(work, 0)?;
                let bytes = content.work().as_deref_mut().expect("observer")
                    .read_content(resolver.as_ref(), result)?.await?;
                content.adopt(bytes.capacity() as u64)?;
                let schema_bytes = content.work().as_deref_mut().expect("observer")
                    .read_content(resolver.as_ref(), &effect.result_schema)?.await?;
                content.adopt(schema_bytes.capacity() as u64)?;
                validate_effect_result_bytes_with_work(
                    &effect.result_schema, &schema_bytes, result, &bytes, content.work())?;
            } else {
                let bytes = resolver.read(result).await?;
                let schema_bytes = resolver.read(&effect.result_schema).await?;
                validate_effect_result_bytes_with_work(
                    &effect.result_schema, &schema_bytes, result, &bytes, work)?;
            }
        }
        issuer.attest_effect_with_work(
            effect_id, effect, observation.attempt_id, observation.status, work)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AggregateKind, Authority};
    use crate::{
        AgentId,
        conversation::{FileDescriptor, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
    };
    use futures::FutureExt as _;
    use serde_json::json;
    use std::{future::Future, pin::Pin};

    fn request_file(bytes: &[u8]) -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "effects",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([8; 16])),
            )?,
            "requests/request.json",
            "generation-1",
            FileDescriptor::from_bytes(bytes, "application/json")?,
            "request.json",
        )
    }
    fn schema_file(schema: &Value) -> Result<FileRef> {
        let bytes =
            serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?;
        FileRef::new(
            request_file(b"null")?.volume().clone(),
            "schemas/result.json",
            "generation-1",
            FileDescriptor::from_bytes(&bytes, "application/schema+json")?,
            "result.json",
        )
    }

    struct ResultResolver;
    impl ContentResidencyVerifier for ResultResolver {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                let bytes = if reference.descriptor().media_type() == "application/schema+json" {
                    serde_json::to_vec(&json!({"type": "object", "required": ["receipt"]}))
                        .map_err(|error| Error::Invalid(error.to_string()))?
                } else {
                    br#"{"receipt":"ok"}"#.to_vec()
                };
                reference.descriptor().verify(&bytes)
            })
        }
        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
            Box::pin(async move {
                let bytes = if reference.descriptor().media_type() == "application/schema+json" {
                    serde_json::to_vec(&json!({"type": "object", "required": ["receipt"]}))
                        .map_err(|error| Error::Invalid(error.to_string()))?
                } else {
                    br#"{"receipt":"ok"}"#.to_vec()
                };
                reference.descriptor().verify(&bytes)?;
                Ok(bytes)
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
        let mut registry = EffectRegistry::default().with_result_resolver(Arc::new(ResultResolver));
        registry.register(provider.clone())?;
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
