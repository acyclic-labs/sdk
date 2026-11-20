#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub use acyclic_harness_core::sse::decode;
pub use acyclic_harness_core::sse::{Decoder, frames};

/// The stream decoder ADAPTER's responses are framed with.
pub fn decoder(adapter: crate::Adapter, decoder: Decoder) -> Decoder {
    if adapter == crate::Adapter::Responses {
        decoder.with_json_document()
    } else {
        decoder
    }
}
