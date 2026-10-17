//! Shared streaming adapters and immutable model selection.

mod accounting;
mod adapter;
mod alloy;
mod alloy_spec;
mod catalog;
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
#[cfg(any(target_arch = "wasm32", loom))]
mod cancellation;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod codex_dispatch_tests;
mod counting;
mod credentials;
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
pub use spec::{Authentication, ProviderSpec};

use acyclic_harness::{Error, Result};
use serde_json::Value;
mod sync;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod accounting_properties;

fn invalid(reason: impl Into<String>) -> Error {
    Error::Invalid(reason.into())
}

fn http_error(status: u16) -> Error {
    let message = format!("model HTTP status {status}");
    match status {
        401 | 403 => Error::Unauthorized(message),
        408 | 429 | 500..=599 => Error::Storage(message),
        _ => Error::Invalid(message),
    }
}

fn json(bytes: &[u8]) -> Result<Value> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = acyclic_harness::deserialize_json_value(&mut decoder)
        .map_err(|error| invalid(error.to_string()))?;
    decoder.end().map_err(|error| invalid(error.to_string()))?;
    Ok(value)
}
