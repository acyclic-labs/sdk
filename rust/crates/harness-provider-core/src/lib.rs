//! Provider foundation: endpoint specs, credentials, transports and shared wire helpers.
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

/// Per-adapter token estimation calibrated from reported usage.
pub mod accounting;
#[cfg(not(target_arch = "wasm32"))]
/// Original-ledger admission for provider wire work.
pub mod allocation;
/// Immutable alloy configuration and its deterministic selection policy.
pub mod alloy_spec;
/// The built-in provider catalog and its model entries.
pub mod catalog;
#[cfg(not(target_arch = "wasm32"))]
/// Parsing and serialising provider catalog files.
pub mod catalog_document;
#[cfg(not(target_arch = "wasm32"))]
/// The Codex login flow and its token refresh.
pub mod codex;
#[cfg(not(target_arch = "wasm32"))]
mod credential_file;
mod model_spec;
#[cfg(not(target_arch = "wasm32"))]
mod refresh;
/// The last usage a provider reported, shared across its clones.
pub mod usage_snapshot;
#[cfg(not(target_arch = "wasm32"))]
pub use codex::CodexLogin;
#[cfg(all(target_arch = "wasm32", feature = "browser"))]
pub mod cancellation;
/// Credential sources, their live refresh and their watched files.
pub mod credentials;
/// Caller-selected request deadlines over one shared event.
pub mod deadline;
/// Classification of provider failures into the shared diagnostic classes.
pub mod diagnostic;
/// Drives admitted response frames through the provider event parser.
pub mod event_driver;
/// Resolution of a request's files into the text an adapter renders.
pub mod files;
/// Recorded provider streams every adapter is compared against.
pub mod fixtures;
/// The platform HTTP transport and its bounded body reader.
pub mod http;
/// Provider, model and timeout specifications.
pub mod spec;
/// Server-sent-event framing for provider streams.
pub mod sse;
/// The portable and native mutexes the crate shares state through.
pub mod sync;
/// Tool-name canonicalisation and the adversarial name generator.
pub mod tool_names;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod accounting_properties;

pub use acyclic_model_render::Family as Adapter;
pub use alloy_spec::{AlloySelection, AlloySpec, AlloyTurn};
pub use catalog::default_catalog;
pub use credentials::Credentials;
#[cfg(target_arch = "wasm32")]
pub use http::BrowserTransport;
#[cfg(not(target_arch = "wasm32"))]
pub use http::ReqwestTransport;
pub use http::{HttpRequest, HttpResponse, HttpTransport};
pub use model_spec::{ModelFeatures, ModelSpec};
pub use spec::{
    Authentication, PromptCache, ProviderConfig, ProviderSpec, RetrySpec, ScriptedAdapter,
    ScriptedProviderSpec, TimeoutSpec, TokenCountField, TokenCounterSpec,
};

use acyclic_harness_core::{Error, Result};
use acyclic_harness_provider_response::{http_error, unavailable};

/// Constructs an invalid-input refusal.
pub fn invalid(reason: impl Into<String>) -> Error {
    Error::Invalid(reason.into())
}

/// A refusal raised while native work is admitted keeps its native boundary class.
pub fn boundary(work: acyclic_fs::NativeWork<'_>, reason: &'static str) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_some() {
        return Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Invalid,
            reason,
        };
    }
    let _ = work;
    invalid(reason)
}

/// Admission for a locally rendered status diagnostic. Remote text is never retained.
pub const EXCERPT_BYTES: usize = 256;

/// A retrying operation needs only refusal headers. Reading a transient diagnostic
/// could wait forever and cannot change the HTTP recovery classification.
pub async fn recovery_failure(
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
pub async fn http_failure(
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
                acyclic_fs_work::performance::WorkCounters::UNCHARGED,
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
        return allocation::diagnostic_error(diagnostic::rejection(error), custody);
    }
    error
}

/// Classifies a refusal body: only a context overflow consumes it.
async fn read_http_failure(
    response: HttpResponse,
    maximum: impl Into<Option<usize>>,
    work: acyclic_fs::NativeWork<'_>,
) -> Error {
    let HttpResponse { status, body, .. } = response;
    // Only context classification consumes a body; it emits a typed result.
    if matches!(status, 400 | 413) {
        return match http::bounded::read(body, maximum, work).await {
            Ok(body) => {
                match acyclic_harness_provider_response::context_overflow_bytes(&body.bytes, work) {
                    Ok(true) => Error::ModelContextOverflow,
                    Ok(false) => http_error(status),
                    Err(error) => error,
                }
            }
            Err(error) => error,
        };
    }
    http_error(status)
}
