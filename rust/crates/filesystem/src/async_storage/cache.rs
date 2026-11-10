use super::*;

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
    pub(crate) native_custody: Option<Arc<crate::NativeAllocation>>,
}

/// The original cache observation made before provider I/O or decoding.
#[doc(hidden)]
pub enum DecodedCacheLookup<'a> {
    Hit(DecodedCacheValue),
    Miss(DecodedCacheMiss<'a>),
}

/// Admission permission from one original miss. It borrows its issuing cache,
/// so completion cannot rediscover a different namespace or resident slot.
#[doc(hidden)]
pub struct DecodedCacheMiss<'a> {
    key: DecodedCacheKey,
    epoch: u64,
    origin: Option<&'a dyn DecodedCacheOrigin>,
}

pub(crate) trait DecodedCacheOrigin: ProviderPlatform {
    fn admit(
        &self,
        key: DecodedCacheKey,
        epoch: u64,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::ObjectStoreError>;
}

impl<'a> DecodedCacheMiss<'a> {
    pub(crate) fn new(
        key: DecodedCacheKey,
        epoch: u64,
        origin: &'a dyn DecodedCacheOrigin,
    ) -> Self {
        Self {
            key,
            epoch,
            origin: Some(origin),
        }
    }

    pub(crate) fn uncached(key: DecodedCacheKey) -> Self {
        Self {
            key,
            epoch: 0,
            origin: None,
        }
    }

    /// Offers the completed value only to the cache that issued this miss.
    /// A retired observation leaves the original immutable value uncached.
    pub fn admit(
        self,
        value: DecodedCacheValue,
    ) -> Result<DecodedCacheAdmission, crate::ObjectStoreError> {
        if value.value.as_ref().type_id() != self.key.decoder {
            return Err(crate::ObjectStoreError::Corrupt);
        }
        match self.origin {
            Some(origin) => origin.admit(self.key, self.epoch, value),
            None => Ok(DecodedCacheAdmission::Uncached(value)),
        }
    }
}

/// Whether a decoded value became shared cache state or remains operation-owned.
#[doc(hidden)]
pub enum DecodedCacheAdmission {
    Uncached(DecodedCacheValue),
    Shared(DecodedCacheValue),
}

#[cfg(all(any(test, feature = "distributed"), not(target_arch = "wasm32")))]
pub(crate) fn owned_read(
    read: ObjectRead,
    original: &crate::materializer::NativeMaterializationWork,
) -> Result<ObjectRead, crate::storage::ObjectStoreError> {
    let valid = match read.retention {
        crate::storage::ObjectReadRetention::Owned { logical_bytes } => {
            read.bytes
                .retained_capacity()
                .and_then(|capacity| u64::try_from(capacity).ok())
                == Some(logical_bytes)
                && logical_bytes >= read.bytes.len() as u64
                && read.bytes.matches_meter(&original.runtime_meter())
        }
        crate::storage::ObjectReadRetention::Shared => false,
    };
    if !valid {
        return Err(crate::storage::ObjectStoreError::Corrupt);
    }
    Ok(read)
}

#[cfg(test)]
pub(crate) fn cached_value<S: AsyncObjectStore>(
    store: &S,
    key: DecodedCacheKey,
) -> Result<Option<DecodedCacheValue>, crate::ObjectStoreError> {
    Ok(match store.decoded_cache_lookup(key)? {
        DecodedCacheLookup::Hit(value) => Some(value),
        DecodedCacheLookup::Miss(_) => None,
    })
}

#[cfg(test)]
pub(crate) fn publish_decoded<S: AsyncObjectStore>(
    store: &S,
    key: DecodedCacheKey,
    value: DecodedCacheValue,
) -> Result<DecodedCacheAdmission, crate::ObjectStoreError> {
    match store.decoded_cache_lookup(key)? {
        DecodedCacheLookup::Hit(cached) => Ok(DecodedCacheAdmission::Shared(cached)),
        DecodedCacheLookup::Miss(miss) => miss.admit(value),
    }
}
