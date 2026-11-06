//! Capacity-preserving transfer of a vector and its original custody.
use super::*;

// Backing retires before the owner that pays for it and this shared header.
struct VectorOwner<G> {
    bytes: Vec<u8>,
    _owner: G,
}

impl<G> AsRef<[u8]> for VectorOwner<G> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl OwnedBytes {
    /// The one shared header allocated by [`Self::from_vec_with_owner`].
    /// Payload capacity remains separately funded by the original owner.
    #[must_use]
    pub const fn vec_owner_allocation_bytes<G>() -> usize {
        acyclic_auth::operation::bytes_owner_allocation_bytes::<VectorOwner<G>>()
    }

    /// Transfers a vector and its original custody without copying bytes.
    /// `admit` funds the shared header through `owner` before it is allocated.
    /// On refusal, the vector is destroyed before its owner. The carrier records
    /// actual vector capacity, including backing outside a clone's visible range.
    /// This supplies allocation identity, not authority or ledger provenance.
    pub fn from_vec_with_owner<G: Send + 'static, E>(
        bytes: Vec<u8>,
        owner: G,
        admit: impl FnOnce(&mut G, usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut storage = VectorOwner {
            bytes,
            _owner: owner,
        };
        admit(&mut storage._owner, Self::vec_owner_allocation_bytes::<G>())?;
        let (length, capacity) = (storage.bytes.len(), storage.bytes.capacity());
        Ok(Self {
            data: Data::Plain(Bytes::from_owner(storage)),
            range: 0..length,
            retained_capacity: Some(capacity),
        })
    }
}
