use crate::{
    Adapter, Authentication, ModelFeatures, ProviderSpec, TokenCountField, TokenCounterSpec,
};
use acyclic_harness_core::{
    Result,
    config::{
        ProviderEntry, ProviderProfile,
        provider::{
            CacheBreakpoint, CacheTtl, Compaction, NativeTool, ProviderCache, ProviderEffort,
            ProviderSampling, SummaryHeader,
        },
    },
    context::ModelContextCapacity,
    model::NativeToolProfile,
};
use serde_json::Map;
use std::collections::BTreeMap;

pub(crate) const DEFAULT_MODEL: &str = "openai/gpt-6.1-sol";

struct Endpoint {
    id: &'static str,
    adapter: Adapter,
    authentication: Authentication,
    base_url: &'static str,
    settings: Option<(&'static str, NativeToolProfile)>,
}

const ENDPOINTS: &[Endpoint] = &[
    Endpoint {
        id: "anthropic",
        adapter: Adapter::Anthropic,
        authentication: Authentication::ApiKey,
        base_url: "https://api.anthropic.com/v1",
        settings: Some((
            "plugins/anthropic/provider.ts",
            NativeToolProfile::Anthropic,
        )),
    },
    Endpoint {
        id: "openai",
        adapter: Adapter::Responses,
        authentication: Authentication::ApiKeyOrCodexLogin,
        base_url: "https://api.openai.com/v1",
        settings: Some(("plugins/openai/provider.ts", NativeToolProfile::Openai)),
    },
    Endpoint {
        id: "openrouter",
        adapter: Adapter::Chat,
        authentication: Authentication::ApiKey,
        base_url: "https://openrouter.ai/api/v1",
        settings: None,
    },
    Endpoint {
        id: "ollama",
        adapter: Adapter::Chat,
        authentication: Authentication::Optional,
        base_url: "http://localhost:11434/v1",
        settings: None,
    },
    Endpoint {
        id: "lmstudio",
        adapter: Adapter::Chat,
        authentication: Authentication::Optional,
        base_url: "http://localhost:1234/v1",
        settings: None,
    },
    Endpoint {
        id: "vllm",
        adapter: Adapter::Chat,
        authentication: Authentication::Optional,
        base_url: "http://localhost:8000/v1",
        settings: None,
    },
    Endpoint {
        id: "gemini",
        adapter: Adapter::Gemini,
        authentication: Authentication::ApiKey,
        base_url: "https://generativelanguage.googleapis.com/v1beta",
        settings: Some(("plugins/gemini/provider.ts", NativeToolProfile::Google)),
    },
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

const MODELS: &[(&str, &str, u32, u32, ModelFeatures, bool)] = &[
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
///
/// # Errors
/// An embedded native declaration cannot be admitted as provider settings.
pub fn default_catalog() -> Result<BTreeMap<String, ProviderSpec>> {
    ENDPOINTS
        .iter()
        .map(
            |Endpoint {
                 id,
                 adapter,
                 authentication,
                 base_url,
                 settings,
             }| {
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
                                    tool_profile: settings.map(|(_, profile)| profile),
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
                Ok((
                    id.into(),
                    ProviderSpec {
                        settings: settings
                            .map(|(path, profile)| stock_settings(path, profile))
                            .transpose()?,
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
                ))
            },
        )
        .collect()
}

fn stock_settings(path: &str, profile: NativeToolProfile) -> Result<ProviderEntry> {
    let tools = acyclic_harness_core::tools::native::declaration_data(profile)?
        .into_iter().map(|definition| {
            let name = definition.get("name").and_then(serde_json::Value::as_str).ok_or_else(|| crate::invalid(format!(
                "stock provider settings admission failed: {path} has a nameless declaration; recovery: restore the original sys/native declaration source"
            )))?.to_owned();
            Ok(NativeTool { name, definition })
        }).collect::<Result<_>>()?;
    let entry = ProviderEntry {
        path: path.into(),
        base: ProviderProfile {
            tools,
            cache: ProviderCache {
                breakpoints: vec![CacheBreakpoint::Header, CacheBreakpoint::Segments],
                ttl: CacheTtl::FiveMinutes,
            },
            compaction: Compaction::NativeWhenAvailable,
            summary_header: SummaryHeader::StripExecutableBuiltins,
            // No explicit effort field means the selected model's provider default.
            effort: ProviderEffort {
                low: Map::new(),
                medium: Map::new(),
                high: Map::new(),
            },
            sampling: ProviderSampling {
                temperature: None,
                top_p: None,
                max_output_tokens: None,
            },
        },
    };
    entry.validate()?;
    Ok(entry)
}
