//! Canonical retry digest.
use crate::{CommitRequest, memory};

/// Computes the canonical digest of a normalized, validated request.
#[must_use]
pub fn commit_digest(request: &CommitRequest) -> [u8; 32] {
    memory::commit_digest(request, &mut memory::DigestWork::Live(&memory::MemoryObserver::default()))
        .expect("ordinary hashing has no fallible observer")
}
