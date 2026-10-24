use super::*;

pub(super) fn bytes_only<A, O>(
    bytes: acyclic_stream::OwnedBytes,
    _generation: Option<Generation<A, O>>,
) -> acyclic_stream::OwnedBytes {
    bytes
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod retention_tests;
