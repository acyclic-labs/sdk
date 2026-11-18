use super::{Adapter, Value, invalid};
use acyclic_harness_core::Error;
use acyclic_harness_provider_protocol::refusal::{self, Refusal};

pub(super) fn classify(adapter: Adapter, event: &Value) -> Error {
    match refusal::classify(adapter, event) {
        Refusal::ContextOverflow => Error::ModelContextOverflow,
        Refusal::Http(status) => crate::http_error(status),
        Refusal::Unclassified => invalid("model provider reported a stream error"),
    }
}

mod http;
pub(crate) use http::context_overflow_bytes;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "error_tests.rs"]
mod tests;
