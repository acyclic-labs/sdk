use crate::{Adapter, invalid};
use acyclic_harness_core::Result;
use serde::Deserialize;

pub use crate::{TokenCountField, TokenCounterSpec};

pub(crate) fn endpoint(
    config: &crate::ProviderSpec,
    model: &str,
    spec: &TokenCounterSpec,
) -> Result<String> {
    let mut base = crate::spec::validate_url(&config.base_url, config.allow_http_host.as_deref())?;
    let root = format!("{}/", base.path().trim_end_matches('/'));
    base.set_path(&root);
    let mut path = spec.path.clone();
    if path.contains("{model}") {
        if model
            .chars()
            .any(|c| matches!(c, '/' | ':' | '?' | '#' | '%'))
        {
            return Err(invalid("invalid count model name"));
        }
        path = path.replace("{model}", model);
    }
    let resolved = base
        .join(&path)
        .map_err(|_| invalid("invalid count endpoint"))?;
    if resolved.origin() != base.origin()
        || !resolved.username().is_empty()
        || resolved.password().is_some()
        || resolved.query().is_some()
        || resolved.fragment().is_some()
    {
        return Err(invalid("count endpoint must remain on the provider origin"));
    }
    Ok(resolved.into())
}

/// Both native admission and browser preparation count the same projected
/// request, rendered once more by the one renderer with the count subset.
pub(crate) fn body(
    generation: crate::projection::wire::Request<'_>,
    model: &str,
    spec: &TokenCounterSpec,
) -> Result<Vec<u8>> {
    let adapter = spec.adapter;
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let model =
        if crate::tests::catalog_identity_tests::SUBSTITUTE_WIRE_MODEL.with(std::cell::Cell::get) {
            "unselected-model"
        } else {
            model
        };
    let input = generation.into_count(model, spec);
    let mut bytes = Vec::with_capacity(128);
    if adapter == Adapter::Gemini {
        bytes.extend_from_slice(br#"{"generateContentRequest":"#);
        input.render(&mut bytes)?;
        bytes.push(b'}');
    } else {
        input.render(&mut bytes)?;
    }
    Ok(bytes)
}

pub(crate) fn fields(adapter: Adapter) -> &'static [&'static str] {
    match adapter {
        // OpenAI's count endpoint accepts the same input items, tools and text schema.
        Adapter::Responses => &[
            "model",
            "input",
            "instructions",
            "tools",
            "tool_choice",
            "text",
            "parallel_tool_calls",
            "reasoning",
            "personality",
            "conversation",
            "previous_response_id",
            "truncation",
        ],
        Adapter::Anthropic => &[
            "model",
            "messages",
            "system",
            "tools",
            "tool_choice",
            "thinking",
            "output_config",
            "cache_control",
        ],
        Adapter::Chat => &[
            "model",
            "messages",
            "tools",
            "tool_choice",
            "add_generation_prompt",
        ],
        Adapter::Gemini => &[],
    }
}

pub(crate) fn response(bytes: &[u8], spec: &TokenCounterSpec) -> Result<u64> {
    #[derive(Deserialize)]
    struct Input {
        input_tokens: u64,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Total {
        total_tokens: u64,
    }
    #[derive(Deserialize)]
    struct Count {
        count: u64,
    }
    // Typed integer decoding cannot reinterpret a literal serde private-marker map.
    let reported = match spec.response_field {
        TokenCountField::InputTokens => {
            serde_json::from_slice::<Input>(bytes).map(|v| v.input_tokens)
        }
        TokenCountField::TotalTokens => {
            serde_json::from_slice::<Total>(bytes).map(|v| v.total_tokens)
        }
        TokenCountField::Count => serde_json::from_slice::<Count>(bytes).map(|v| v.count),
    }
    .map_err(|_| invalid("invalid token count response"))?;
    reported
        .checked_add(u64::from(spec.framing_tokens))
        .filter(|count| *count <= acyclic_harness_core::conversation::MAX_EXACT_JS_INTEGER)
        .ok_or_else(|| invalid("token count exceeds portable arithmetic"))
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "wire_tests.rs"]
mod tests;
