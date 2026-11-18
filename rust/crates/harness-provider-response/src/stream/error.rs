use super::{Adapter, Value, invalid};
use acyclic_harness_core::Error;
use acyclic_harness_provider_protocol::refusal::{self, Refusal};

/// Classifies an admitted stream error using the provider wire rules.
pub fn classify(adapter: Adapter, event: &Value) -> Error {
    match refusal::classify(adapter, event) {
        Refusal::ContextOverflow => Error::ModelContextOverflow,
        Refusal::Http(status) => crate::http_error(status),
        Refusal::Unclassified => invalid("model provider reported a stream error"),
    }
}
