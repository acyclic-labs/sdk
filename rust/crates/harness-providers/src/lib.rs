//! Shared streaming adapters and immutable model selection.
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

mod accounting;
mod admission;
mod alloy;
mod alloy_spec;
mod authorization;
mod catalog;
#[cfg(not(target_arch = "wasm32"))]
mod catalog_document;
#[cfg(not(target_arch = "wasm32"))]
mod codex;
#[cfg(not(target_arch = "wasm32"))]
mod credential_file;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod length_continuation_tests;
mod model_spec;
#[cfg(not(target_arch = "wasm32"))]
mod refresh;
mod usage_count;
mod usage_snapshot;
#[cfg(not(target_arch = "wasm32"))]
pub use codex::CodexLogin;
mod call_ids;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
mod cancellation;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod codex_dispatch_tests;
#[cfg(all(not(target_arch = "wasm32"), feature = "local"))]
mod configured;
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
/// Configuration for OpenAI-compatible hosted and local endpoints.
#[cfg(feature = "endpoint-presets")]
pub mod endpoints;
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
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;
mod tool_names;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
mod wasm;

pub use acyclic_model_render::Family as Adapter;
pub use alloy::ResolvedAlloy;
pub use alloy_spec::{AlloySelection, AlloySpec, AlloyTurn};
pub use authorization::{
    AuthorizationScheme, CredentialBinding, CredentialResolver, CredentialSource,
};
pub use catalog::default_catalog;
pub use counting::wire::{TokenCountField, TokenCounterSpec};
pub use credentials::Credentials;
#[cfg(target_arch = "wasm32")]
pub use http::BrowserTransport;
#[cfg(not(target_arch = "wasm32"))]
pub use http::ReqwestTransport;
pub use http::{HttpRequest, HttpResponse, HttpTransport};
pub use model_spec::{ModelFeatures, ModelSpec};
pub use reasoning::{ProjectedReasoning, project_native, project_reasoning};
pub use registry::Registry;
pub use resolved::ResolvedModel;
pub use spec::{Authentication, PromptCache, ProviderSpec, RetrySpec, TimeoutSpec};

use acyclic_harness_core::{Error, Result};
use serde_json::Value;
mod sync;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod accounting_properties;

fn invalid(reason: impl Into<String>) -> Error {
    Error::Invalid(reason.into())
}

/// The one status classification for dispatch, counting, refresh and stream error events.
/// 408, 429 and 5xx are a typed refusal before generation that redelivery may clear.
fn http_error(status: u16) -> Error {
    unavailable(status).unwrap_or_else(|| {
        let message = format!("model HTTP status {status}");
        match status {
            401 | 403 => Error::Unauthorized(message),
            _ => Error::Invalid(message),
        }
    })
}

/// Admission for a locally rendered status diagnostic. Remote text is never retained.
const EXCERPT_BYTES: usize = 256;

fn unavailable(status: u16) -> Option<Error> {
    matches!(status, 408 | 429 | 500..=599).then(|| Error::ModelUnavailable {
        failure: acyclic_harness_core::ModelTransportFailure::Status(status),
        excerpt: String::new(),
    })
}

/// Keep the transport class without formatting an untrusted cause. Transport errors
/// may include request URLs, headers or response text containing credentials.
fn transport_failure(
    _native: bool,
    failure: acyclic_harness_core::ModelTransportFailure,
    _cause: impl std::fmt::Display,
) -> Error {
    Error::ModelUnavailable {
        failure,
        excerpt: String::new(),
    }
}

/// A retrying operation needs only refusal headers. Reading a transient diagnostic
/// could wait forever and cannot change the HTTP recovery classification.
async fn recovery_failure(
    response: HttpResponse,
    maximum: impl Into<Option<usize>>,
    work: acyclic_fs::NativeWork<'_>,
) -> Error {
    if let Some(error) = unavailable(response.status) {
        return error;
    }
    http_failure(response, maximum, work).await
}

/// Context refusals use the same admitted JSON budget as token-count responses.
/// All other refusals retain only their status. In particular, echoed credentials
/// in a proxy's error body must never enter a journal, volume or log.
async fn http_failure(
    response: HttpResponse,
    maximum: impl Into<Option<usize>>,
    work: acyclic_fs::NativeWork<'_>,
) -> Error {
    if let Some(error) = unavailable(response.status) {
        return error;
    }
    #[cfg(not(target_arch = "wasm32"))]
    let mut custody = match work
        .map(|work| {
            work.claim(
                4 * EXCERPT_BYTES as u64 + 64,
                4,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
        })
        .transpose()
    {
        Ok(custody) => custody,
        Err(cause) => return cause.into(),
    };
    let error = read_http_failure(response, maximum, work).await;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(custody) = custody.as_mut() {
        return native::diagnostic_error(diagnostic::rejection(error), custody);
    }
    error
}

async fn read_http_failure(
    response: HttpResponse,
    maximum: impl Into<Option<usize>>,
    work: acyclic_fs::NativeWork<'_>,
) -> Error {
    let HttpResponse { status, body, .. } = response;
    // Only context classification consumes a body; it emits a typed result.
    if matches!(status, 400 | 413) {
        return match http::bounded::read(body, maximum, work).await {
            Ok(body) => match stream::context_overflow_bytes(&body.bytes, work) {
                Ok(true) => Error::ModelContextOverflow,
                Ok(false) => http_error(status),
                Err(error) => error,
            },
            Err(error) => error,
        };
    }
    http_error(status)
}

fn json(bytes: &[u8]) -> Result<Value> {
    decode_json(bytes).map_err(|error| invalid(error.to_string()))
}

fn decode_json(bytes: &[u8]) -> serde_json::Result<Value> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = acyclic_harness_core::deserialize_json_value(&mut decoder)?;
    decoder.end()?;
    Ok(value)
}
