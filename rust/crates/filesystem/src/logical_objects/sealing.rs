use super::*;

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
    /// This admission runs before any bytes are published.
    fn stored_limit(&self, id: ObjectId, plaintext_limit: u64) -> Result<u64, ObjectStoreError>;

    /// Authenticates the seal against every association field and returns plaintext.
    /// Returned allocations retain the caller's original work custody.
    fn open(
        &self,
        id: ObjectId,
        stored: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<OwnedBytes, ObjectStoreError>;
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
        stored: OwnedBytes,
        _: &mut AuthorityWork<'_>,
    ) -> Result<OwnedBytes, ObjectStoreError> {
        self.stored_limit(id, stored.len() as u64)?;
        Ok(stored)
    }
}

impl<P: ?Sized, S: ObjectSealer> LogicalObjectStore<P, S> {
    pub(super) fn open_stored(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        maximum: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<OwnedBytes, crate::FsError> {
        self.open_prehashed(id, bytes, maximum, None, work)
    }

    pub(super) fn open_prehashed(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        maximum: u64,
        authenticated: Option<&acyclic_stream::SealedBytes>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<OwnedBytes, crate::FsError> {
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
            read::hash(physical, &bytes, bytes.len() as u64, work)?;
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
        if !authenticated.is_some_and(|sealed| {
            std::ptr::eq(sealed.bytes().as_ref(), bytes.as_ref())
                && crate::Digest::from_bytes(*sealed.digest()) == id.digest
        }) {
            read::hash(id, &bytes, bytes.len() as u64, work)?;
        }
        Ok(bytes)
    }

    fn open_standalone(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut KernelAccounting<'_>,
    ) -> Result<OwnedBytes, crate::FsError> {
        let mut context = AuthorityWork::new(work.remaining()?, work.cancellation());
        let result = self.sealer.open(id, bytes, &mut context);
        crate::kernel::complete_standalone_backend(work, context.spent(), 0)?;
        result.map_err(Into::into)
    }
}
