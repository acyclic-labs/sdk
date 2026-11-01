use super::{Adapter, Value, invalid};
use acyclic_harness_core::Error;

// Only recognized protocol discriminants affect classification. Remote messages,
// headers and unknown codes never enter the returned diagnostic.
pub(super) fn classify(adapter: Adapter, event: &Value) -> Error {
    if context_overflow(event) {
        return Error::ModelContextOverflow;
    }
    let error = event
        .get("error")
        .filter(|v| !v.is_null())
        .or_else(|| event["response"].get("error"))
        .unwrap_or(event);
    let numeric = error["code"].as_u64().and_then(|n| u16::try_from(n).ok());
    let status = numeric.or_else(|| {
        if adapter == Adapter::Gemini {
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
        Some(status) => crate::http_error(status),
        None => invalid("model provider reported a stream error"),
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

fn is_context_overflow(code: Option<&str>, kind: Option<&str>, message: Option<&str>) -> bool {
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

mod http;
pub(crate) use http::context_overflow_bytes;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "error_tests.rs"]
mod tests;
