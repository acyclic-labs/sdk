//! effects semantics.
use super::*;
/// Strength advertised by an external effect provider.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectGuarantee {
    /// Provider proves one externally observable application.
    ExactlyOnce,
    /// Provider accepts a stable retry identity and deduplicates retries.
    IdempotentRetry,
    /// Provider will not retry after an uncertain dispatch.
    AtMostOnce,
}

/// Current durable status of an effect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "public ref-valued status keeps its stable serde and Rust shape"
)]
pub enum EffectStatus {
    /// Effect is durable but has not been dispatched.
    Planned,
    /// Dispatch began and its external outcome may still be unknown.
    Dispatched,
    /// Effect completed successfully.
    Succeeded {
        /// Pinned schema-defined JSON result file.
        result: FileRef,
    },
    /// Effect completed with a known failure.
    Failed {
        /// Stable failure description.
        message: String,
    },
    /// Provider cannot determine the externally visible outcome.
    Indeterminate,
}

impl EffectStatus {
    pub(crate) fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded { .. } | Self::Failed { .. } | Self::Indeterminate
        )
    }
}

/// Host-attested provider observation accepted by the pure reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectAttestation {
    /// Provider that produced the observation.
    pub provider: String,
    /// Effect being observed.
    pub effect_id: EffectId,
    /// Exact attempt being observed.
    pub attempt_id: EffectAttemptId,
    /// Digest of the immutable provider request.
    pub request_digest: [u8; 32],
    /// Pinned delivery guarantee.
    pub guarantee: EffectGuarantee,
    /// Pinned successful-result schema digest.
    pub result_schema_digest: [u8; 32],
    /// Terminal or explicitly unknown result.
    pub status: EffectStatus,
    pub(in crate::core) proof: [u8; 32],
}

impl EffectAttestation {
    pub(crate) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<(Self, u64)> {
        crate::wire_codec::CodecQuota::produce(work, |quota| copying::copy_observation(self, quota))
    }
}

/// Complete durable projection of one external effect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EffectState {
    /// Provider selected before dispatch.
    pub provider: String,
    /// Provider-advertised delivery strength pinned for this effect.
    pub guarantee: EffectGuarantee,
    /// Namespaced effect kind.
    pub effect_kind: String,
    /// Immutable provider request file; the event carries no request body.
    pub request: FileRef,
    /// Canonical digest binding provider, kind, guarantee, and request.
    pub request_digest: [u8; 32],
    /// Immutable JSON Schema file for successful results.
    pub result_schema: FileRef,
    /// SHA-256 digest pinning the result schema bytes.
    pub result_schema_digest: [u8; 32],
    /// Current semantic completion state.
    pub status: EffectStatus,
    /// Ordered unique attempts made against the provider.
    pub attempts: Vec<EffectAttemptId>,
}

pub(crate) fn settled_effect(status: &EffectStatus) -> bool {
    matches!(
        status,
        EffectStatus::Succeeded { .. } | EffectStatus::Failed { .. }
    )
}

pub(crate) fn event_effect_id(payload: &EventPayload) -> Option<EffectId> {
    match payload {
        EventPayload::EffectPlanned { effect_id, .. }
        | EventPayload::EffectDispatched { effect_id, .. } => Some(*effect_id),
        EventPayload::EffectResolved { observation } => Some(observation.effect_id),
        _ => None,
    }
}

pub(in crate::core) fn effect_request_digest_with_work(
    provider: &str,
    guarantee: EffectGuarantee,
    effect_kind: &str,
    request: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    crate::contract::canonical_json_digest(
        &(provider, guarantee, effect_kind, request),
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )
}

fn validate_effect_result(status: &EffectStatus) -> Result<()> {
    if let EffectStatus::Succeeded { result } = status {
        result.validate(&mut None)?;
        if result.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("effect result must be JSON content".into()));
        }
    }
    Ok(())
}

pub(in crate::core) fn validate_effect_dispatch(
    effect: &EffectState,
    attempt: &EffectAttemptId,
) -> Result<()> {
    let may_retry = effect.status == EffectStatus::Indeterminate
        && effect.guarantee == EffectGuarantee::IdempotentRetry;
    if effect.status != EffectStatus::Planned && !may_retry {
        return Err(Error::Conflict("invalid effect dispatch transition".into()));
    }
    if effect.attempts.contains(attempt) {
        return Err(Error::Conflict(
            "effect attempt identity already exists".into(),
        ));
    }
    Ok(())
}

pub(in crate::core) fn validate_effect_observation(
    effect: &EffectState,
    observation: &EffectAttestation,
) -> Result<()> {
    if observation.provider != effect.provider
        || observation.request_digest != effect.request_digest
        || observation.guarantee != effect.guarantee
        || observation.result_schema_digest != effect.result_schema_digest
    {
        return Err(Error::Conflict(
            "effect observation does not match durable state".into(),
        ));
    }
    if !observation.status.is_terminal()
        || effect.attempts.last() != Some(&observation.attempt_id)
        || !matches!(
            effect.status,
            EffectStatus::Dispatched | EffectStatus::Indeterminate
        )
    {
        return Err(Error::Conflict(
            "invalid effect resolution transition".into(),
        ));
    }
    validate_effect_result(&observation.status)
}
