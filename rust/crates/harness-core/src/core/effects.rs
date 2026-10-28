use super::*;
/// Strength advertised by an external effect provider.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, crate::HeapLayout,
)]
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum EffectStatus {
    /// Effect is durable but has not been dispatched.
    Planned,
    /// Dispatch began and its external outcome may still be unknown.
    Dispatched,
    /// Effect completed successfully.
    Succeeded {
        /// Pinned schema-defined JSON result file.
        result: Box<FileRef>,
    },
    /// Effect completed with a known failure.
    Failed {
        /// Stable failure description.
        message: String,
    },
    /// Provider cannot determine the externally visible outcome.
    Indeterminate,
}

impl EffectStatus {}

/// Host-attested provider observation accepted by the pure reducer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
    #[serde(with = "super::capability_bytes")]
    #[heap_layout(as = crate::heap_layout::markers::Base64Bytes)]
    pub(in crate::core) proof: Vec<u8>,
}

/// Complete durable projection of one external effect.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
pub struct EffectState {
    /// Provider selected before dispatch.
    pub provider: String,
    /// Provider-advertised delivery strength pinned for this effect.
    pub guarantee: EffectGuarantee,
    /// Namespaced effect kind.
    pub effect_kind: String,
    /// Immutable provider request file; the event carries no request body.
    pub request: Box<FileRef>,
    /// Canonical digest binding provider, kind, guarantee, and request.
    pub request_digest: [u8; 32],
    /// Immutable JSON Schema file for successful results.
    pub result_schema: FileRef,
    /// SHA-256 digest pinning the result schema bytes.
    pub result_schema_digest: [u8; 32],
    /// Current semantic completion state.
    pub status: EffectStatus,
    /// Authenticated planning event; dispatch and reconciliation recheck its live workload.
    pub(crate) planning: Option<Box<Event>>,
    /// Ordered unique attempts made against the provider.
    pub attempts: Vec<EffectAttemptId>,
}

crate::core::ownership::owned_enum!(EffectGuarantee {
    ExactlyOnce,
    IdempotentRetry,
    AtMostOnce,
});
crate::core::ownership::owned_enum!(EffectStatus {
    Planned,
    Dispatched,
    Succeeded { result },
    Failed { message },
    Indeterminate,
});
crate::core::ownership::owned_struct!(EffectAttestation {
    provider,
    effect_id,
    attempt_id,
    request_digest,
    guarantee,
    result_schema_digest,
    status,
    proof
});
crate::core::ownership::owned_struct!(EffectState {
    provider,
    guarantee,
    effect_kind,
    request,
    request_digest,
    result_schema,
    result_schema_digest,
    status,
    planning,
    attempts
});
