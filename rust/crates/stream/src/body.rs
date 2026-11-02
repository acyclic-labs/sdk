//! Body fragments retaining the original transport or allocation custody.

use bytes::{Buf, Bytes};

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

impl<C: Send + 'static> BodyChunk<C> {
    /// Transfers this fragment to an HTTP body without releasing its credit.
    ///
    /// The issuing operation admits the one fixed owner-control allocation
    /// before it is made. Its returned guard and the original fragment custody
    /// remain attached through transport clones and nonempty slices. Failure
    /// returns the original admission error without allocating or copying.
    /// This grants no authority and creates no capacity or cancellation source.
    pub fn try_into_bytes<A: Send + 'static, E>(
        self,
        admit: impl FnOnce(usize) -> Result<A, E>,
    ) -> Result<Bytes, E> {
        let control = admit(acyclic_auth::operation::bytes_owner_allocation_bytes::<
            BodyChunk<(C, A)>,
        >())?;
        Ok(Bytes::from_owner(BodyChunk {
            bytes: self.bytes,
            _custody: (self._custody, control),
        }))
    }
}

impl<C> Buf for BodyChunk<C> {
    fn remaining(&self) -> usize {
        self.bytes.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.bytes.chunk()
    }

    fn advance(&mut self, count: usize) {
        self.bytes.advance(count);
    }
}

impl<C> AsRef<[u8]> for BodyChunk<C> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
