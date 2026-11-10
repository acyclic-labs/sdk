#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) use acyclic_harness_core::sse::decode;
pub(crate) use acyclic_harness_core::sse::{Decoder, frames};

pub(crate) fn decoder(adapter: crate::Adapter, decoder: Decoder) -> Decoder {
    if adapter == crate::Adapter::Responses {
        decoder.with_json_document()
    } else {
        decoder
    }
}
