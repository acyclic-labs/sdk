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

/// Borrow only the known refusal fields from the already bounded HTTP prefix.
/// Building an arbitrary JSON tree here would escape the native diagnostic ledger.
pub(crate) fn context_overflow_bytes(bytes: &[u8]) -> bool {
    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        error: Option<&'a serde_json::value::RawValue>,
        #[serde(borrow)]
        response: Option<&'a serde_json::value::RawValue>,
    }
    #[derive(serde::Deserialize)]
    struct Fields<'a> {
        #[serde(borrow)]
        code: Option<&'a serde_json::value::RawValue>,
        #[serde(rename = "type", borrow)]
        kind: Option<&'a str>,
        #[serde(borrow)]
        message: Option<&'a str>,
    }
    let Ok(envelope) = serde_json::from_slice::<Envelope<'_>>(bytes) else {
        return false;
    };
    let error = envelope.error.or_else(|| {
        serde_json::from_str::<Envelope<'_>>(envelope.response?.get())
            .ok()?
            .error
    });
    let bytes = error.map_or(bytes, |raw| raw.get().as_bytes());
    let Ok(fields) = serde_json::from_slice::<Fields<'_>>(bytes) else {
        return false;
    };
    let code = fields.code.and_then(|raw| {
        raw.get()
            .starts_with('"')
            .then(|| serde_json::from_str::<&str>(raw.get()).ok())
            .flatten()
    });
    is_context_overflow(code, fields.kind, fields.message)
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "error_tests.rs"]
mod tests;
