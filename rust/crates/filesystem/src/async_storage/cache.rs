use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_read_batch(
    reads: Vec<ObjectRead>, allocation: crate::materializer::NativeAllocation,
) -> Result<(Vec<ObjectRead>, crate::materializer::NativeAllocation), crate::storage::ObjectStoreError> {
    let bytes = reads.capacity().checked_mul(size_of::<ObjectRead>())
        .and_then(|bytes| u64::try_from(bytes).ok());
    if bytes != Some(allocation.bytes()) {
        drop(reads);
        drop(allocation);
        return Err(crate::storage::ObjectStoreError::Corrupt);
    }
    Ok((reads, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn owned_read(
    read: ObjectRead, original: &crate::materializer::NativeMaterializationWork,
) -> Result<ObjectRead, crate::storage::ObjectStoreError> {
    let valid = match read.retention {
        crate::storage::ObjectReadRetention::Owned { logical_bytes } => {
            read.bytes.retained_capacity().and_then(|capacity| u64::try_from(capacity).ok())
                == Some(logical_bytes)
                && logical_bytes >= read.bytes.len() as u64
                && read.bytes.matches_meter(&original.runtime_meter())
        }
        crate::storage::ObjectReadRetention::Shared => false,
    };
    if !valid { return Err(crate::storage::ObjectStoreError::Corrupt); }
    Ok(read)
}

pub(crate) fn observe_read(custody: &crate::kernel::ProofDataCustody, result: ObjectResult<ObjectRead>) -> ObjectResult<ObjectRead> {
    let work = match &result { Ok(receipt) => receipt.work, Err(failure) => *failure.work };
    custody.record_completed_leaf_work(work).map_err(|error| crate::storage::ObjectFailure::new(error.into(), work))?;
    result
}

/// Exact identity of one authenticated decoded-object representation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[doc(hidden)]
pub struct DecodedCacheKey {
    pub(crate) object_id: ObjectId,
    decoder: TypeId,
    limits: crate::kernel::DecodeLimits,
}

impl DecodedCacheKey {
    pub(crate) fn new<T: 'static>(
        object_id: ObjectId,
        limits: crate::kernel::DecodeLimits,
    ) -> Self {
        Self {
            object_id,
            decoder: TypeId::of::<T>(),
            limits,
        }
    }
}

/// Type-erased immutable decoded value retained by a disposable accelerator.
#[derive(Clone)]
#[doc(hidden)]
pub struct DecodedCacheValue {
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) logical_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<Arc<crate::materializer::NativeAllocation>>,
}

/// Whether a decoded value became shared cache state or remains operation-owned.
#[doc(hidden)]
pub enum DecodedCacheAdmission {
    Uncached(DecodedCacheValue),
    Shared(DecodedCacheValue),
}

