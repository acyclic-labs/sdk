use super::*;

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
