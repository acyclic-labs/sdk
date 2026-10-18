use crate::{Adapter, invalid};
use acyclic_harness::Result;
use serde::{Deserialize, Serialize};

/// Declarative counting endpoint on the provider's own origin.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(all(target_arch = "wasm32", feature = "browser"), derive(tsify::Tsify))]
pub struct TokenCounterSpec {
    /// Relative path; {model} expands to the selected model name.
    pub path: String,
    /// Input dialect understood by the count endpoint.
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

pub(crate) fn endpoint(base: &str, model: &str, spec: &TokenCounterSpec) -> Result<String> {
    let mut base = crate::registry::validate_url(base)?;
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

/// Both native admission and browser preparation borrow the same projected input.
#[derive(Serialize)]
#[serde(untagged)]
pub(crate) enum CountBody<'a, 'b> {
    Direct(CountInput<'a, 'b>),
    Gemini {
        #[serde(rename = "generateContentRequest")]
        request: CountInput<'a, 'b>,
    },
}

pub(crate) struct CountInput<'a, 'b> {
    generation: &'a crate::projection::wire::Body<'b>,
    model: &'a str,
    adapter: Adapter,
}

pub(crate) fn body<'a, 'b>(
    generation: &'a crate::projection::wire::Body<'b>,
    model: &'a str,
    adapter: Adapter,
) -> CountBody<'a, 'b> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let model =
        if crate::tests::catalog_identity_tests::SUBSTITUTE_WIRE_MODEL.with(std::cell::Cell::get) {
            "unselected-model"
        } else {
            model
        };
    let input = CountInput {
        generation,
        model,
        adapter,
    };
    if adapter == Adapter::Gemini {
        CountBody::Gemini { request: input }
    } else {
        CountBody::Direct(input)
    }
}

impl Serialize for CountInput<'_, '_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if self.adapter == Adapter::Gemini {
            self.generation.serialize_fields(
                serializer,
                None,
                Some(&format!("models/{}", self.model)),
            )
        } else {
            self.generation
                .serialize_fields(serializer, Some(fields(self.adapter)), None)
        }
    }
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
        .filter(|count| *count <= acyclic_harness::conversation::MAX_EXACT_JS_INTEGER)
        .ok_or_else(|| invalid("token count exceeds portable arithmetic"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "wire_tests.rs"]
mod tests;
