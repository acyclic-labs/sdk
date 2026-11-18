//! Shared Actors client diagnostic projection.

/// Lossless Rust-owned diagnostic projection shared by both JavaScript bridges.
#[derive(Clone, Debug, Default, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", optional_fields = nullable)]
pub struct ErrorMetadata {
    /// Stable SDK error category.
    pub code: String,
    /// Original status message or canonical admission diagnostic.
    pub message: String,
    /// Numeric gRPC status reported by the maintained transport.
    pub grpc_code: Option<i32>,
    /// Canonical gRPC category.
    pub grpc_name: Option<String>,
    /// Authenticated remote credential rejection, distinct from local freshness
    /// refusal. Only this verdict can invalidate an exact request lease.
    pub credential_rejected: bool,
    /// Decoded int32 service code, including future values unknown to this SDK version.
    pub service_code: Option<i32>,
    /// Decoded service diagnostic, separate from the gRPC status message.
    pub service_message: Option<String>,
    /// Original producer attribution, independent of the service code or message.
    pub cause: Option<acyclic_auth::refusal::Cause>,
    /// Original status and product metadata, including duplicate and opaque values.
    pub refusal: Option<acyclic_auth::refusal::Parts>,
    /// Canonical admission category, when applicable.
    pub contract_code: Option<String>,
    /// Exact detail bytes for service failures, including empty details.
    #[serde(with = "serde_bytes")]
    #[ts(optional, type = "Uint8Array | null")]
    pub raw_details: Option<Vec<u8>>,
}
