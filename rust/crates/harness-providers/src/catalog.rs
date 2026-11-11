use crate::{Adapter, Authentication, ModelFeatures, ProviderSpec, TokenCounterSpec};
use acyclic_harness_core::context::ModelContextCapacity;
use std::collections::BTreeMap;

pub(crate) const DEFAULT_MODEL: &str = "openai/gpt-6.1-sol";

const ENDPOINTS: &[(&str, Adapter, Authentication, &str)] = &[
    (
        "recon",
        Adapter::Chat,
        Authentication::ApiKey,
        "https://api.morphllm.com/v1",
    ),
    (
        "anthropic",
        Adapter::Anthropic,
        Authentication::ApiKey,
        "https://api.anthropic.com/v1",
    ),
    (
        "openai",
        Adapter::Responses,
        Authentication::ApiKeyOrCodexLogin,
        "https://api.openai.com/v1",
    ),
    (
        "openrouter",
        Adapter::Chat,
        Authentication::ApiKey,
        "https://openrouter.ai/api/v1",
    ),
    (
        "ollama",
        Adapter::Chat,
        Authentication::Optional,
        "http://localhost:11434/v1",
    ),
    (
        "lmstudio",
        Adapter::Chat,
        Authentication::Optional,
        "http://localhost:1234/v1",
    ),
    (
        "vllm",
        Adapter::Chat,
        Authentication::Optional,
        "http://localhost:8000/v1",
    ),
    (
        "gemini",
        Adapter::Gemini,
        Authentication::ApiKey,
        "https://generativelanguage.googleapis.com/v1beta",
    ),
];

const TOOLS_AND_IMAGES: ModelFeatures = ModelFeatures {
    tools: true,
    images: true,
};

const MODELS: &[(&str, &str, u32, u32, ModelFeatures, bool)] = &[
    (
        "recon",
        "morph-kimik31",
        1_048_576,
        8_192,
        ModelFeatures {
            tools: true,
            images: false,
        },
        false,
    ),
    (
        "recon",
        "morph-dsv41flash",
        1_048_576,
        8_192,
        ModelFeatures {
            tools: true,
            images: false,
        },
        false,
    ),
    // https://platform.claude.com/docs/en/models/overview
    (
        "anthropic",
        "claude-fable-5-1",
        1_000_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "anthropic",
        "claude-opus-5-5",
        1_000_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "anthropic",
        "claude-sonnet-5-5",
        1_000_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "anthropic",
        "claude-haiku-5-5",
        1_000_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    // https://developers.openai.com/api/docs/models
    (
        "openai",
        "gpt-6-astra",
        1_050_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "openai",
        "gpt-6.1-sol",
        1_050_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "openai",
        "gpt-6.1-terra",
        1_050_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "openai",
        "gpt-6-luna",
        1_050_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "anthropic",
        "claude-sonnet-4-5",
        200_000,
        64_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "anthropic",
        "claude-sonnet-4-5-20250929",
        200_000,
        64_000,
        TOOLS_AND_IMAGES,
        false,
    ),
    // https://ai.google.dev/gemini-api/docs/models (verified 2026-10-31).
    // Thinking-level support: https://ai.google.dev/gemini-api/docs/generate-content/thinking
    // and https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini/3-1-pro
    // (verified 2026-11-01). Gemini 2.5 uses token budgets, not levels.
    // Argon long decode is a separate asynchronous mode; streamGenerateContent
    // retains the documented synchronous output capacity.
    (
        "gemini",
        "gemini-4-argon",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-3.8-flash",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-3.6-flash",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-3.5-flash-lite",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-3.1-flash-lite",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-3.1-pro-preview",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-3-flash-preview",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        true,
    ),
    (
        "gemini",
        "gemini-2.5-pro",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "gemini",
        "gemini-2.5-flash",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "openai",
        "gpt-4.1",
        1_047_576,
        32_768,
        TOOLS_AND_IMAGES,
        false,
    ),
    (
        "openai",
        "gpt-4.1-2025-04-14",
        1_047_576,
        32_768,
        TOOLS_AND_IMAGES,
        false,
    ),
    ("openai", "gpt-5", 400_000, 128_000, TOOLS_AND_IMAGES, false),
    (
        "openai",
        "gpt-5-2025-08-07",
        400_000,
        128_000,
        TOOLS_AND_IMAGES,
        false,
    ),
];

/// Standard endpoint policies, model capacities and capabilities as ordinary data.
pub fn default_catalog() -> BTreeMap<String, ProviderSpec> {
    let counters: BTreeMap<Adapter, Option<TokenCounterSpec>> =
        serde_json::from_str(include_str!("token_counters.json"))
            .expect("embedded native token counters");
    ENDPOINTS
        .iter()
        .map(|(id, adapter, authentication, base_url)| {
            let (id, adapter, base_url) = (*id, *adapter, *base_url);
            let authentication = authentication.clone();
            let models = MODELS
                .iter()
                .filter(|(provider, ..)| *provider == id)
                .map(
                    |&(_, name, context_tokens, output_tokens, features, thinking_levels)| {
                        (
                            name.into(),
                            crate::ModelSpec {
                                tool_profile: match id {
                                    "anthropic" => Some(
                                        acyclic_harness_core::model::NativeToolProfile::Anthropic,
                                    ),
                                    "openai" => {
                                        Some(acyclic_harness_core::model::NativeToolProfile::Openai)
                                    }
                                    "gemini" => {
                                        Some(acyclic_harness_core::model::NativeToolProfile::Google)
                                    }
                                    _ => None,
                                },
                                thinking_levels,
                                max_context_tokens: None,
                                capacity: ModelContextCapacity {
                                    context_tokens,
                                    output_tokens: Some(output_tokens),
                                },
                                features,
                            },
                        )
                    },
                )
                .collect();
            (
                id.into(),
                ProviderSpec {
                    adapter,
                    authentication,
                    base_url: base_url.into(),
                    allow_http_host: None,
                    headers: BTreeMap::new(),
                    models,
                    token_counter: counters.get(&adapter).cloned().flatten(),
                    max_event_bytes: None,
                    retry: crate::RetrySpec::DEFAULT,
                    timeouts: crate::TimeoutSpec::DEFAULT,
                    prompt_cache: if id == "openrouter" {
                        crate::PromptCache::Routed
                    } else {
                        crate::PromptCache::Native
                    },
                },
            )
        })
        .collect()
}
