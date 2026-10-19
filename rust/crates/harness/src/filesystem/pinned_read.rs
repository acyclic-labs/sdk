use super::*;

// A successful authenticated read owns either its exact immutable generation
// or proof that the original accounting context has already retained it. No
// mutable head is consulted when the caller requests retention.
pub(super) struct PinnedRead<A, O> {
    bytes: acyclic_stream::OwnedBytes,
    pending: Option<Generation<A, O>>,
}

pub(super) fn bytes_only<A, O>(
    bytes: acyclic_stream::OwnedBytes,
    _generation: Option<Generation<A, O>>,
) -> acyclic_stream::OwnedBytes {
    bytes
}

impl<A, O> PinnedRead<A, O> {
    pub(super) fn from_read(
        bytes: acyclic_stream::OwnedBytes,
        generation: Option<Generation<A, O>>,
    ) -> Self {
        match generation {
            Some(generation) => Self::pending(bytes, generation),
            None => Self::retained(bytes),
        }
    }
    pub(super) fn pending(bytes: acyclic_stream::OwnedBytes, generation: Generation<A, O>) -> Self {
        Self {
            bytes,
            pending: Some(generation),
        }
    }
    pub(super) fn retained(bytes: acyclic_stream::OwnedBytes) -> Self {
        Self {
            bytes,
            pending: None,
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PinnedRead<A, O> {
    pub(super) async fn retain(self) -> Result<acyclic_stream::OwnedBytes> {
        if let Some(generation) = self.pending {
            fork_preparer_io::retain(&generation, None).await?;
        }
        Ok(self.bytes)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod retention_tests;
