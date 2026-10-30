use crate::Adapter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Authentication required by a registered endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub enum Authentication {
    /// A provider API key is required.
    ApiKey,
    /// Prefer an API key, otherwise reuse native Codex CLI credentials on the built-in Responses route.
    ApiKeyOrCodexLogin,
    /// The endpoint accepts anonymous requests; use an API key when configured.
    Optional,
    /// Resolve a bearer through a host-protected secret binding on each request.
    Bearer {
        /// Canonical logical reference, resolved outside guest memory.
        secret_reference: String,
    },
    /// Resolve the adapter's API-key header through the same protected binding.
    ApiKeySecret {
        /// Canonical logical reference, resolved outside guest memory.
        secret_reference: String,
    },
}

impl Authentication {
    pub(crate) fn secret_reference(&self) -> Option<&str> {
        match self {
            Self::Bearer { secret_reference } | Self::ApiKeySecret { secret_reference } => {
                Some(secret_reference)
            }
            _ => None,
        }
    }
}

/// Endpoint configuration independent of immutable model identity.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct ProviderSpec {
    /// Protocol dialect.
    pub adapter: Adapter,
    /// Credential policy supplied by the catalog, independent of its provider ID.
    pub authentication: Authentication,
    /// HTTPS API root, or loopback HTTP API root.
    pub base_url: String,
    /// Explicit endpoint headers; adapter authentication remains authoritative.
    #[serde(default)]
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "browser"),
        tsify(type = "Record<string, string>")
    )]
    pub headers: BTreeMap<String, String>,
    /// Exact model capacities, including local server configurations.
    #[serde(default)]
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "browser"),
        tsify(type = "Record<string, ModelSpec>")
    )]
    pub models: BTreeMap<String, crate::ModelSpec>,
    /// Verified admission counter. Required for automatic compaction; generic
    /// compatible endpoints must configure their own tokenizer contract.
    #[serde(default)]
    pub token_counter: Option<crate::TokenCounterSpec>,
    /// Maximum bytes in one SSE event and aggregate retained protocol state.
    pub max_event_bytes: usize,
    /// Bounded redelivery of a request the endpoint refused before generating anything.
    #[serde(default, skip_serializing_if = "RetrySpec::is_default")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub retry: RetrySpec,
    /// Optional caller-selected deadline for each request to the endpoint.
    #[serde(default, skip_serializing_if = "TimeoutSpec::is_default")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub timeouts: TimeoutSpec,
}

/// Caller-selected request deadline, shared by response headers and the body.
/// Requests are unbounded when no deadline is provided. A deadline never
/// restarts when another body chunk arrives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct TimeoutSpec {
    /// Maximum request duration in milliseconds, from send through body completion.
    /// `None` is unbounded; zero expires before dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub total_ms: Option<u64>,
}

impl TimeoutSpec {
    /// Requests have no implicit deadline.
    pub const DEFAULT: Self = Self { total_ms: None };
    fn is_default(&self) -> bool {
        *self == Self::DEFAULT
    }
}

impl Default for TimeoutSpec {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Redelivery of a model request refused with HTTP 408, 429 or 5xx, or never connected, before
/// any output. Waits use the provider's `Retry-After` when present, otherwise exponential
/// backoff with deterministic per-dispatch jitter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct RetrySpec {
    /// Attempts after the first; zero disables redelivery.
    pub attempts: u8,
    /// Backoff before the first redelivery; each later one doubles it.
    pub base_delay_ms: u32,
    /// Longest single wait. A `Retry-After` beyond it ends redelivery immediately.
    pub max_delay_ms: u32,
}

impl RetrySpec {
    /// Three redeliveries from 500 ms, each wait at most 8 s.
    pub const DEFAULT: Self = Self {
        attempts: 3,
        base_delay_ms: 500,
        max_delay_ms: 8_000,
    };
    fn is_default(&self) -> bool {
        *self == Self::DEFAULT
    }
}

impl Default for RetrySpec {
    fn default() -> Self {
        Self::DEFAULT
    }
}
