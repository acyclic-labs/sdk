use crate::Adapter;
use acyclic_auth::secrets::SecretRef;
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
        /// Canonical versioned reference, resolved outside guest memory.
        #[cfg_attr(
            all(target_arch = "wasm32", feature = "browser"),
            tsify(type = "SecretRef")
        )]
        secret_reference: SecretRef,
    },
    /// Resolve the adapter's API-key header through the same protected binding.
    ApiKeySecret {
        /// Canonical versioned reference, resolved outside guest memory.
        #[cfg_attr(
            all(target_arch = "wasm32", feature = "browser"),
            tsify(type = "SecretRef")
        )]
        secret_reference: SecretRef,
    },
}

impl Authentication {
    pub(crate) fn secret_reference(&self) -> Option<&SecretRef> {
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
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "browser"),
        tsify(type = "Family")
    )]
    pub adapter: Adapter,
    /// Credential policy supplied by the catalog, independent of its provider ID.
    pub authentication: Authentication,
    /// HTTPS API root, or HTTP on loopback/an explicitly permitted host.
    pub base_url: String,
    /// Explicitly permit HTTP to this exact canonical host, without wildcards.
    /// Other hosts retain the HTTPS or loopback-only policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub allow_http_host: Option<String>,
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
    /// Verified admission counter. Without one, Chat measures the same input
    /// through a one-token completion's reported `usage.prompt_tokens`.
    /// Other protocols require their own tokenizer contract.
    #[serde(default)]
    pub token_counter: Option<crate::TokenCounterSpec>,
    /// Caller-selected capacity for one SSE event and retained protocol state.
    /// An omitted capacity is unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub max_event_bytes: Option<usize>,
    /// Redelivery of a request the endpoint refused before generating anything.
    #[serde(default, skip_serializing_if = "RetrySpec::is_default")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub retry: RetrySpec,
    /// Optional caller-selected deadline for each request to the endpoint.
    #[serde(default, skip_serializing_if = "TimeoutSpec::is_default")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub timeouts: TimeoutSpec,
    /// Request-side prompt caching form this endpoint accepts.
    #[serde(default, skip_serializing_if = "PromptCache::is_native")]
    #[cfg_attr(all(target_arch = "wasm32", feature = "browser"), tsify(optional))]
    pub prompt_cache: PromptCache,
}

/// How a request asks the endpoint to cache its stable prefix. Every form keeps
/// the prefix byte-identical; only the cache fields it adds differ.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub enum PromptCache {
    /// The protocol's own form: Anthropic breakpoints, the Responses
    /// `prompt_cache_key`, and implicit caching on Gemini and compatible Chat
    /// endpoints (for example Fireworks). Generic Chat has no universal cache flag.
    #[default]
    Native,
    /// A routing Chat gateway (OpenRouter): the automatic `cache_control`
    /// breakpoint for the routed model plus `prompt_cache_key` sticky routing.
    Routed,
}

impl PromptCache {
    fn is_native(&self) -> bool {
        *self == Self::Native
    }
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

/// Transient provider failures wait with uncapped redelivery. Permanent authentication
/// and invalid-request failures return immediately. The original request and turn identity
/// are preserved. Retry-After is honored; absent a hint, waits use exponential backoff
/// with deterministic per-dispatch jitter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct RetrySpec {
    /// Backoff before the first redelivery; each later one doubles it.
    pub base_delay_ms: u32,
    /// Longest computed backoff; provider Retry-After waits are never shortened.
    pub max_delay_ms: u32,
}

impl RetrySpec {
    /// Uncapped redelivery from 500 ms, with computed backoff at most 8 s.
    pub const DEFAULT: Self = Self {
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
