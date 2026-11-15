use super::*;

#[cfg(test)]
mod tests;

/// Caller-owned lineage sealing. Implementations authenticate the plaintext,
/// physical name, original generation and root directory identity together.
/// Keys and transport identity remain outside filesystem objects.
pub trait ObjectSealer: acyclic_stream::ProviderPlatform {
    /// Maximum canonical reference width produced by this sealer.
    fn maximum_reference_bytes(&self) -> usize {
        ObjectId::ENCODED_LEN
    }
    /// Prepares immutable CAS bytes before a parent authenticates the reference.
    /// Return the original object or bind sealed bytes with [`crate::HashedObject::seal`].
    fn prepare(
        &self,
        object: crate::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::HashedObject, ObjectStoreError>;

    /// Validates the association and bounds its stored representation.
    /// Reject unsealed references when this composition requires sealing.
    /// This admission runs before any bytes are published. The plaintext bound
    /// may be `u64::MAX`; clamp representation overhead to the representable
    /// byte count rather than rejecting a bound larger than the actual object.
    fn stored_limit(&self, id: ObjectId, plaintext_limit: u64) -> Result<u64, ObjectStoreError>;

    /// Authenticates the seal against every association field and returns plaintext.
    /// Views preserve the input retention evidence; new allocations retain
    /// the caller's original work custody and report their exact capacity.
    fn open(
        &self,
        id: ObjectId,
        stored: ObjectRead,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError>;
}

/// Direct plaintext CAS storage.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlainObjects;

impl ObjectSealer for PlainObjects {
    fn maximum_reference_bytes(&self) -> usize {
        33
    }
    fn prepare(
        &self,
        object: crate::HashedObject,
        _: &mut AuthorityWork<'_>,
    ) -> Result<crate::HashedObject, ObjectStoreError> {
        Ok(object)
    }
    fn stored_limit(&self, id: ObjectId, maximum: u64) -> Result<u64, ObjectStoreError> {
        if id.storage.is_some() {
            return Err(ObjectStoreError::Corrupt);
        }
        Ok(maximum)
    }
    fn open(
        &self,
        id: ObjectId,
        stored: ObjectRead,
        _: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        self.stored_limit(id, stored.len() as u64)?;
        Ok(stored)
    }
}

impl<P: ?Sized, S: ObjectSealer> LogicalObjectStore<P, S> {
    pub(super) fn open_stored(
        &self,
        id: ObjectId,
        bytes: ObjectRead,
        maximum: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<ObjectRead, crate::FsError> {
        self.open_prehashed(id, bytes, maximum, None, work)
    }

    pub(super) fn open_prehashed(
        &self,
        id: ObjectId,
        bytes: ObjectRead,
        maximum: u64,
        authenticated: Option<&acyclic_stream::SealedBytes>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<ObjectRead, crate::FsError> {
        self.open_prehashed_proof(id, bytes, maximum, authenticated, work)
            .map(|(bytes, _)| bytes)
    }

    fn open_prehashed_proof(
        &self,
        id: ObjectId,
        bytes: ObjectRead,
        maximum: u64,
        authenticated: Option<&acyclic_stream::SealedBytes>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(ObjectRead, acyclic_stream::SealedBytes), crate::FsError> {
        if let Some(sealed) = authenticated {
            if acyclic_objects::Digest::from_bytes(*sealed.digest()) != blob(id) {
                return Err(ObjectStoreError::Corrupt.into());
            }
        } else if let Some(storage) = id.storage {
            let physical = ObjectId {
                kind: id.kind,
                digest: storage.locator,
                storage: None,
            };
            read::hash(physical, &bytes.bytes, bytes.len() as u64, work)?;
        }
        let bytes = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = work.native_work() {
                self.sealer.open(
                    id,
                    bytes,
                    &mut AuthorityWork::native(original, work.cancellation()),
                )?
            } else {
                self.open_standalone(id, bytes, work)?
            }
            #[cfg(target_arch = "wasm32")]
            self.open_standalone(id, bytes, work)?
        };
        if bytes.len() as u64 > maximum {
            return Err(ObjectStoreError::TooLarge {
                observed: bytes.len() as u64,
                maximum,
            }
            .into());
        }
        let proof = if let Some(sealed) = authenticated.filter(|sealed| {
            std::ptr::eq(sealed.bytes().as_ref(), bytes.as_ref())
                && crate::Digest::from_bytes(*sealed.digest()) == id.digest
        }) {
            sealed.clone()
        } else {
            read::seal(id, &bytes.bytes, bytes.len() as u64, work)?
        };
        Ok((bytes, proof))
    }

    pub(super) fn restore_prepared(
        &self,
        id: ObjectId,
        stored: OwnedBytes,
        maximum: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<crate::HashedObject, crate::FsError> {
        let limit = self.sealer.stored_limit(id, maximum)?;
        if stored.len() as u64 > limit {
            return Err(ObjectStoreError::TooLarge {
                observed: stored.len() as u64,
                maximum: limit,
            }
            .into());
        }
        let stored = write::seal(stored, work)?;
        let (_, plaintext) = self.open_prehashed_proof(
            id,
            ObjectRead {
                bytes: stored.bytes().clone(),
                retention: ObjectReadRetention::Shared,
            },
            maximum,
            Some(&stored),
            work,
        )?;
        crate::HashedObject::from_proofs(id, plaintext, id.storage.map(|_| stored))
            .ok_or_else(|| ObjectStoreError::Corrupt.into())
    }

    fn open_standalone(
        &self,
        id: ObjectId,
        bytes: ObjectRead,
        work: &mut KernelAccounting<'_>,
    ) -> Result<ObjectRead, crate::FsError> {
        let mut context = crate::async_storage::AuthorityFrame::borrow(work)?;
        let result = self.sealer.open(id, bytes, &mut context);
        context.spent().verify(context.budget)?;
        result.map_err(Into::into)
    }
}
