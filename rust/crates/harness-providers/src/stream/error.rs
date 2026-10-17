use super::{Adapter, Value, invalid};
use acyclic_harness::Error;

// Only recognized protocol discriminants affect classification. Remote messages,
// headers and unknown codes never enter the returned diagnostic.
pub(super) fn classify(adapter: Adapter, event: &Value) -> Error {
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

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "error_tests.rs"]
mod tests;
