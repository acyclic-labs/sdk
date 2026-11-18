//! Provider refusal classification without transport or diagnostic ownership.

use acyclic_model_render::Family;
use serde_json::Value;

/// The protocol information a provider refusal is allowed to contribute.
pub enum Refusal {
    /// The request exceeded the provider's context window.
    ContextOverflow,
    /// A recognized HTTP-equivalent status.
    Http(u16),
    /// The refusal did not carry a recognized protocol discriminator.
    Unclassified,
}

/// Only recognized protocol discriminants affect classification. Remote messages,
/// headers and unknown codes never enter the returned classification.
pub fn classify(adapter: Family, event: &Value) -> Refusal {
    if context_overflow(event) {
        return Refusal::ContextOverflow;
    }
    let error = event
        .get("error")
        .filter(|v| !v.is_null())
        .or_else(|| event["response"].get("error"))
        .unwrap_or(event);
    let numeric = error["code"].as_u64().and_then(|n| u16::try_from(n).ok());
    let status = numeric.or_else(|| {
        if adapter == Family::Gemini {
            match error["status"].as_str()? {
                "UNAUTHENTICATED" => Some(401),
                "PERMISSION_DENIED" => Some(403),
                "RESOURCE_EXHAUSTED" => Some(429),
                "INTERNAL" => Some(500),
                "UNAVAILABLE" => Some(503),
                "DEADLINE_EXCEEDED" => Some(504),
                "INVALID_ARGUMENT" => Some(400),
                "NOT_FOUND" => Some(404),
                _ => None,
            }
        } else {
            [error["type"].as_str(), error["code"].as_str()]
                .into_iter()
                .flatten()
                .find_map(|code| match code {
                    "authentication_error" | "invalid_api_key" => Some(401),
                    "permission_error" | "permission_denied" => Some(403),
                    "rate_limit_error" | "rate_limit_exceeded" => Some(429),
                    "api_error" | "server_error" => Some(500),
                    "overloaded_error" => Some(529),
                    "timeout_error" => Some(504),
                    _ => None,
                })
        }
    });
    match status {
        Some(status) => Refusal::Http(status),
        None => Refusal::Unclassified,
    }
}

/// Only protocol discriminants and Anthropic's documented length refusal affect this class.
fn context_overflow(event: &Value) -> bool {
    let error = event
        .get("error")
        .filter(|error| !error.is_null())
        .or_else(|| event["response"].get("error"))
        .unwrap_or(event);
    is_context_overflow(
        error["code"].as_str(),
        error["type"].as_str(),
        error["message"].as_str(),
    )
}

/// Checks the context-window discriminants shared by streamed events and HTTP refusals.
pub fn is_context_overflow(code: Option<&str>, kind: Option<&str>, message: Option<&str>) -> bool {
    [code, kind].into_iter().flatten().any(|code| {
        matches!(
            code,
            "context_length_exceeded"
                | "context_window_exceeded"
                | "exceed_context_size_error"
                | "prompt_too_long"
        )
    }) || (kind == Some("invalid_request_error")
        && message.is_some_and(|message| message.starts_with("prompt is too long:")))
}
