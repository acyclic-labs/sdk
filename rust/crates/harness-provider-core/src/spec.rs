use crate::{Adapter, invalid};
use acyclic_auth::secrets::SecretRef;
use acyclic_harness_core::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Parse a provider base URL, admitting HTTPS or loopback HTTP only.
pub fn validate_url(base: &str, allow_http_host: Option<&str>) -> Result<url::Url> {
    let url = url::Url::parse(base).map_err(|_| invalid("invalid provider URL"))?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    let permitted = allow_http_host.is_some_and(|host| Some(host) == url.host_str());
    if !(url.scheme() == "https" || (url.scheme() == "http" && (local || permitted)))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid(
            "provider URL requires HTTPS or loopback HTTP without userinfo, query or fragment; to permit HTTP to one non-loopback host, set that provider's allow_http_host to its exact canonical host",
        ));
    }
    Ok(url)
}

/// Declarative provider selected by a registered provider/model address.
#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub enum ProviderConfig {
    /// Workspace-backed provider. It declares no transport or credential policy.
    Scripted(ScriptedProviderSpec),
    /// A streaming protocol endpoint.
    Endpoint(ProviderSpec),
}

/// The local adapter discriminator.
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptedAdapter {
    /// Replay an immutable script in the executing agent's volume.
    Scripted,
}

/// Configuration for one volume-backed scripted provider.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct ScriptedProviderSpec {
    /// Provider adapter name.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "browser"),
        tsify(type = "'scripted'")
    )]
    pub adapter: ScriptedAdapter,
    /// Authenticated, digest-pinned file in the workspace.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "browser"),
        tsify(type = "{ path: string; sha256: string }")
    )]
    pub script: acyclic_harness_core::model::scripted::VolumeScriptSource,
}

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
    /// The secret this authentication reads, when it reads one.
    pub fn secret_reference(&self) -> Option<&SecretRef> {
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
    /// Optional verified input tokenizer. Without one, generation admits its own input.
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
    /// The endpoint manages caching implicitly and accepts no request cache fields.
    Implicit,
    /// The protocol's own form: Anthropic breakpoints, the Responses
    /// and Chat `prompt_cache_key`, and implicit caching on Gemini. Chat's key
    /// requests stable placement; cache admission and retention remain automatic.
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
/// are preserved. Retry-After and reset headers are honored unchanged; absent a hint,
/// waits use exponential backoff
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

/// Declarative counting endpoint on the provider's own origin.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct TokenCounterSpec {
    /// Relative path; {model} expands to the selected model name.
    pub path: String,
    /// Input dialect understood by the count endpoint. Must match generation:
    /// counting borrows that exact projected input rather than translating it.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "browser"),
        tsify(type = "Family")
    )]
    pub adapter: Adapter,
    /// Typed response field containing the nonnegative integral count.
    pub response_field: TokenCountField,
    /// Configured bound for input framing not included in that endpoint's count.
    pub framing_tokens: u32,
}

/// Count response fields decoded directly as integers, without Value normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub enum TokenCountField {
    /// OpenAI/Anthropic input token count.
    #[serde(rename = "input_tokens")]
    InputTokens,
    /// Gemini prompt token count.
    #[serde(rename = "totalTokens")]
    TotalTokens,
    /// Tokenizer endpoints such as vLLM.
    #[serde(rename = "count")]
    Count,
}
