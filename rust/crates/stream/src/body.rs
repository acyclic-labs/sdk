//! Body fragments retaining the original transport or allocation custody.

use bytes::Bytes;

/// One original body fragment and the owner that permits it to remain live.
///
/// Borrowing exposes no independently owned buffer: transport credit and any
/// allocation claim stay attached until the fragment is dropped. Construction
/// moves the original owners without allocating or copying payload bytes.
pub struct BodyChunk<C> {
    // Retire the physical backing before returning its original capacity.
    bytes: Bytes,
    _custody: C,
}

impl<C> BodyChunk<C> {
    /// Transfers existing bytes and their original custody together.
    #[must_use]
    pub const fn new(bytes: Bytes, custody: C) -> Self {
        Self {
            bytes,
            _custody: custody,
        }
    }
}

impl<C> AsRef<[u8]> for BodyChunk<C> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
