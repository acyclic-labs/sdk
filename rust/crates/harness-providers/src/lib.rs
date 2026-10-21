//! Shared streaming adapters and immutable model selection.

mod accounting;
mod adapter;
mod alloy;
mod alloy_spec;
mod catalog;
#[cfg(not(target_arch = "wasm32"))]
mod catalog_document;
#[cfg(not(target_arch = "wasm32"))]
mod codex;
#[cfg(not(target_arch = "wasm32"))]
mod codex_count;
#[cfg(not(target_arch = "wasm32"))]
mod credential_file;
mod model_spec;
#[cfg(not(target_arch = "wasm32"))]
mod refresh;
#[cfg(not(target_arch = "wasm32"))]
mod usage_snapshot;
#[cfg(not(target_arch = "wasm32"))]
pub use codex::CodexLogin;
mod call_ids;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
mod cancellation;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod codex_dispatch_tests;
mod counting;
mod credentials;
mod deadline;
mod diagnostic;
mod dispatch;
mod files;
#[cfg(test)]
mod fixtures;
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub mod fuzzing;
mod http;
mod input;
pub use input::validate_input;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(any(all(target_arch = "wasm32", feature = "browser"), all(test, not(loom))))]
mod prepared;
mod projection;
mod protocol;
mod reasoning;
mod registry;
mod resolved;
mod retry;
mod spec;
mod sse;
mod stream;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
mod tool_names;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
mod wasm;

pub use adapter::Adapter;
pub use alloy::ResolvedAlloy;
pub use alloy_spec::{AlloySelection, AlloySpec, AlloyTurn};
pub use catalog::default_catalog;
pub use counting::wire::{TokenCountField, TokenCounterSpec};
pub use credentials::Credentials;
#[cfg(target_arch = "wasm32")]
pub use http::BrowserTransport;
#[cfg(not(target_arch = "wasm32"))]
pub use http::ReqwestTransport;
pub use http::{HttpRequest, HttpResponse, HttpTransport};
pub use model_spec::{ModelFeatures, ModelSpec};
pub use reasoning::{ProjectedReasoning, project_reasoning};
pub use registry::Registry;
pub use resolved::ResolvedModel;
pub use spec::{Authentication, ProviderSpec, RetrySpec, TimeoutSpec};

use acyclic_harness_core::{Error, Result};
use serde_json::Value;
mod sync;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod accounting_properties;

fn invalid(reason: impl Into<String>) -> Error {
    Error::Invalid(reason.into())
}

/// The one status classification for dispatch, counting, refresh and stream error events.
/// 408, 429 and 5xx are a typed refusal before generation that redelivery may clear.
fn http_error(status: u16) -> Error {
    unavailable(status, &[]).unwrap_or_else(|| {
        let message = format!("model HTTP status {status}");
        match status {
            401 | 403 => Error::Unauthorized(message),
            _ => Error::Invalid(message),
        }
    })
}

/// Most bytes of a refusal body kept as its excerpt.
const EXCERPT_BYTES: usize = 256;

fn unavailable(status: u16, body: &[u8]) -> Option<Error> {
    matches!(status, 408 | 429 | 500..=599).then(|| Error::ModelUnavailable {
        failure: acyclic_harness_core::ModelTransportFailure::Status(status),
        excerpt: excerpt(body),
    })
}

/// Transport failures are semantic provider failures on both paths. The native path uses an
/// empty excerpt, retaining the typed cause without allocating before ledger admission.
fn transport_failure(
    native: bool,
    failure: acyclic_harness_core::ModelTransportFailure,
    cause: impl std::fmt::Display,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if native {
        return Error::ModelUnavailable {
            failure,
            excerpt: String::new(),
        };
    }
    let _ = native;
    Error::ModelUnavailable {
        failure,
        excerpt: diagnostic::excerpt(cause),
    }
}

/// Classifies a non-success response, reading at most [`EXCERPT_BYTES`] of a refusal's body.
async fn http_failure(response: HttpResponse) -> Error {
    use futures::StreamExt as _;
    let HttpResponse {
        status, mut body, ..
    } = response;
    if unavailable(status, &[]).is_none() {
        return http_error(status);
    }
    let mut prefix = Vec::new();
    while prefix.len() < EXCERPT_BYTES {
        let Some(Ok(chunk)) = body.next().await else {
            break;
        };
        let room = EXCERPT_BYTES - prefix.len();
        prefix.extend_from_slice(chunk.get(..room).unwrap_or(&chunk));
    }
    unavailable(status, &prefix).unwrap_or_else(|| http_error(status))
}

/// Lossy UTF-8 with control characters as spaces, bounded on a character boundary; a character
/// the bound cut is dropped rather than replaced.
fn excerpt(body: &[u8]) -> String {
    let bounded = body.get(..EXCERPT_BYTES).unwrap_or(body);
    let text = match std::str::from_utf8(bounded) {
        Ok(text) => std::borrow::Cow::Borrowed(text),
        Err(cut) if cut.error_len().is_none() => std::borrow::Cow::Borrowed(
            bounded
                .get(..cut.valid_up_to())
                .and_then(|valid| std::str::from_utf8(valid).ok())
                .unwrap_or_default(),
        ),
        Err(_) => String::from_utf8_lossy(bounded),
    };
    let mut excerpt = String::new();
    for character in text.chars() {
        if excerpt.len() + character.len_utf8() > EXCERPT_BYTES {
            break;
        }
        excerpt.push(if character.is_control() {
            ' '
        } else {
            character
        });
    }
    excerpt.trim().to_owned()
}

fn json(bytes: &[u8]) -> Result<Value> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = acyclic_harness_core::deserialize_json_value(&mut decoder)
        .map_err(|error| invalid(error.to_string()))?;
    decoder.end().map_err(|error| invalid(error.to_string()))?;
    Ok(value)
}
