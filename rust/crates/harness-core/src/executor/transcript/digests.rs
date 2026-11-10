//! Hash the prefix and complete messages together without a transcript Value.
use super::*;

pub(super) fn messages(
    messages: &[ModelMessage],
    prefix: Option<usize>,
    io: Option<&PreparationIo>,
) -> Result<([u8; 32], Option<[u8; 32]>)> {
    #[cfg(not(target_arch = "wasm32"))]
    let _span = tracing::debug_span!("acyclic.harness.transcript.digest").entered();
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return io.canonical_sequence_digests(messages, prefix);
    }
    let _ = io;
    crate::executor::json::canonical_sequence_digests(
        messages,
        prefix,
        crate::executor::json::JsonBounds {
            maximum_bytes: u64::MAX,
            maximum_items: u64::MAX,
            maximum_depth: 256,
        },
        None,
        &mut None,
    )
}
