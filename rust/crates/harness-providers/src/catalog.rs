use crate::{
    Adapter, Authentication, ModelFeatures, ProviderSpec, TokenCountField, TokenCounterSpec,
};
use acyclic_harness::context::ModelContextCapacity;
use std::collections::BTreeMap;

const ENDPOINTS: &[(&str, Adapter, Authentication, &str)] = &[
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

const COUNTERS: &[(&str, &str, Adapter, TokenCountField)] = &[
    (
        "openai",
        "responses/input_tokens",
        Adapter::Responses,
        TokenCountField::InputTokens,
    ),
    (
        "anthropic",
        "messages/count_tokens",
        Adapter::Anthropic,
        TokenCountField::InputTokens,
    ),
    (
        "gemini",
        "models/{model}:countTokens",
        Adapter::Gemini,
        TokenCountField::TotalTokens,
    ),
];

const TOOLS_AND_IMAGES: ModelFeatures = ModelFeatures {
    tools: true,
    images: true,
};

const MODELS: &[(&str, &str, u32, u32, ModelFeatures)] = &[
    (
        "anthropic",
        "claude-sonnet-4-5",
        200_000,
        64_000,
        TOOLS_AND_IMAGES,
    ),
    (
        "anthropic",
        "claude-sonnet-4-5-20250929",
        200_000,
        64_000,
        TOOLS_AND_IMAGES,
    ),
    (
        "gemini",
        "gemini-2.5-pro",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
    ),
    (
        "gemini",
        "gemini-2.5-flash",
        1_048_576,
        65_536,
        TOOLS_AND_IMAGES,
    ),
    ("openai", "gpt-4.1", 1_047_576, 32_768, TOOLS_AND_IMAGES),
    (
        "openai",
        "gpt-4.1-2025-04-14",
        1_047_576,
        32_768,
        TOOLS_AND_IMAGES,
    ),
    ("openai", "gpt-5", 400_000, 128_000, TOOLS_AND_IMAGES),
    (
        "openai",
        "gpt-5-2025-08-07",
        400_000,
        128_000,
        TOOLS_AND_IMAGES,
    ),
];

/// Standard endpoint policies, model capacities and capabilities as ordinary data.
pub fn default_catalog() -> BTreeMap<String, ProviderSpec> {
    ENDPOINTS
        .iter()
        .map(|&(id, adapter, authentication, base_url)| {
            let models = MODELS
                .iter()
                .filter(|(provider, ..)| *provider == id)
                .map(|&(_, name, context_tokens, output_tokens, features)| {
                    (
                        name.into(),
                        crate::ModelSpec {
                            capacity: ModelContextCapacity {
                                context_tokens,
                                output_tokens,
                            },
                            features,
                        },
                    )
                })
                .collect();
            (
                id.into(),
                ProviderSpec {
                    adapter,
                    authentication,
                    base_url: base_url.into(),
                    headers: BTreeMap::new(),
                    models,
                    token_counter: COUNTERS.iter().find(|(provider, ..)| *provider == id).map(
                        |&(_, path, adapter, response_field)| TokenCounterSpec {
                            path: path.into(),
                            adapter,
                            response_field,
                            framing_tokens: 0,
                        },
                    ),
                    max_event_bytes: 1024 * 1024,
                    retry: crate::RetrySpec::DEFAULT,
                    timeouts: crate::TimeoutSpec::DEFAULT,
                },
            )
        })
        .collect()
}
