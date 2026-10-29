//! Filesystem immutable content over account-scoped content-addressed blobs.
//!
//! Authenticated references distinguish plaintext identity from sealed CAS location.
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError, ObjectWrite,
    object_digest,
};
use crate::{AsyncObjectStore, AuthorityWork, WorkCounters, WorkError};
use acyclic_objects::{Blobs, ByteRange, Error, Object, Put, wire};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

/// Immutable filesystem-object storage over one account's blobs.
#[derive(Clone)]
pub struct LogicalObjectStore<P: ?Sized, S = PlainObjects> {
    sealer: S,
    provider: Arc<P>,
    maximum_object_bytes: u64,
    decoded: Arc<crate::cache::CachedObjectStore<()>>,
    epoch: Arc<Epoch>,
}

impl<P: ?Sized> LogicalObjectStore<P> {
    /// Binds an account-scoped blob provider.
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            sealer: PlainObjects,
            maximum_object_bytes: 5 * 1024 * 1024 * 1024,
            decoded: Arc::new(crate::cache::CachedObjectStore::with_default_options(())),
            epoch: Arc::default(),
        }
    }
}
impl<P: ?Sized, S> LogicalObjectStore<P, S> {
    /// Uses caller-owned lineage sealing without introducing a locator cache.
    pub fn with_sealer<T: ObjectSealer>(self, sealer: T) -> LogicalObjectStore<P, T> {
        LogicalObjectStore {
            provider: self.provider,
            maximum_object_bytes: self.maximum_object_bytes,
            decoded: self.decoded,
            sealer,
            epoch: self.epoch,
        }
    }

    /// Retains publications under `epoch`, which a collection advances.
    #[must_use]
    pub fn with_epoch(mut self, epoch: Epoch) -> Self {
        self.epoch = Arc::new(epoch);
        self
    }

    /// The collection epoch publications retain under.
    pub fn epoch(&self) -> &Arc<Epoch> {
        &self.epoch
    }

    /// Applies the composition's existing positive per-object bound.
    pub fn with_object_limit(mut self, maximum: u64) -> Result<Self, ObjectStoreError> {
        if maximum == 0 || usize::try_from(maximum).is_err() {
            return Err(ObjectStoreError::Rejected(
                "invalid object size limit".to_owned(),
            ));
        }
        if self.maximum_object_bytes != maximum {
            // A decoded hit must not bypass a newly narrowed backend byte limit.
            self.decoded = Arc::new(crate::cache::CachedObjectStore::with_default_options(()));
        }
        self.maximum_object_bytes = maximum;
        Ok(self)
    }

    /// Returns the exact public provider used by this stateless adapter.
    pub fn provider(&self) -> &Arc<P> {
        &self.provider
    }
}

fn accounting<'a>(
    context: &'a mut AuthorityWork<'_>,
) -> Result<KernelAccounting<'a>, ObjectStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = context.original {
        return Ok(KernelAccounting::native(original, context.cancellation));
    }
    Ok(KernelAccounting::standalone(
        context.read_budget()?,
        context.cancellation,
    ))
}
macro_rules! operate {
    ($context:ident, $work:ident, $body:expr) => {{
        let (result, spent) = {
            let mut $work = accounting($context)?;
            let result = $body;
            (result, $work.spent())
        };
        #[cfg(not(target_arch = "wasm32"))]
        let native = $context.original.is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        if !native {
            $context.work = $context.work.checked_add(spent)?;
        }
        result.map_err(crate::async_storage::object_error)
    }};
}
impl<P: Blobs + ?Sized, S: ObjectSealer> LogicalObjectStore<P, S> {
    async fn read_object<const STORED: bool>(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let read = operate!(
            context,
            work,
            self.fetch::<STORED>(id, maximum, &mut work)
                .await
                .map(|(read, _)| read)
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original
            && matches!(read.retention, ObjectReadRetention::Owned { .. })
        {
            return crate::async_storage::owned_read(read, original);
        }
        Ok(read)
    }
    async fn read_objects<const STORED: bool>(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let (reads, allocation) = operate!(
            context,
            work,
            self.fetch_batch::<STORED>(requests, &mut work).await
        )?;
        match allocation {
            KernelOwnedAllocation::Standalone => Ok((reads, None)),
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(allocation) => {
                Ok::<_, crate::ObjectStoreError>((reads, allocation))
                    .map(|(reads, allocation)| (reads, Some(allocation)))
            }
        }
    }
}
impl<P: Blobs + ?Sized, S: ObjectSealer> AsyncObjectStore for LogicalObjectStore<P, S> {
    fn requires_child_first_publication(&self) -> bool {
        true
    }
    fn maximum_reference_bytes(&self) -> usize {
        self.sealer.maximum_reference_bytes()
    }
    async fn prepare_hashed(
        &self,
        object: crate::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::HashedObject, ObjectStoreError> {
        let original = object.object_id();
        let prepared = self.sealer.prepare(object, context)?;
        if prepared.object_id().encoded_len() > self.maximum_reference_bytes() {
            return Err(ObjectStoreError::Corrupt);
        }
        if prepared.object_id().kind != original.kind
            || prepared.object_id().digest != original.digest
        {
            return Err(ObjectStoreError::DigestMismatch);
        }
        Ok(prepared)
    }

    fn decoded_cache_get(
        &self,
        key: crate::async_storage::DecodedCacheKey,
    ) -> Result<Option<crate::async_storage::DecodedCacheValue>, ObjectStoreError> {
        self.decoded.decoded_get(key)
    }

    fn decoded_cache_admit(
        &self,
        key: crate::async_storage::DecodedCacheKey,
        value: crate::async_storage::DecodedCacheValue,
    ) -> Result<crate::async_storage::DecodedCacheAdmission, ObjectStoreError> {
        self.decoded.decoded_admit(key, value)
    }

    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        operate!(
            context,
            work,
            self.publish(id, bytes.clone(), &bytes, None, &mut work)
                .await
        )
    }
    async fn put_stored(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        operate!(
            context,
            work,
            async {
                let maximum = self.sealer.stored_limit(id, self.maximum_object_bytes)?;
                if bytes.len() as u64 > maximum {
                    return Err(ObjectStoreError::TooLarge {
                        observed: bytes.len() as u64,
                        maximum,
                    }
                    .into());
                }
                let sealed = write::seal(bytes.clone(), &mut work)?;
                let plaintext = self.open_prehashed(
                    id,
                    bytes.clone(),
                    self.maximum_object_bytes,
                    Some(&sealed),
                    &mut work,
                )?;
                self.publish(id, bytes, &plaintext, Some(&sealed), &mut work)
                    .await
            }
            .await
        )
    }
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let id = object.object_id();
        let bytes = object.stored_bytes().clone();
        operate!(
            context,
            work,
            self.publish(id, bytes, object.bytes(), Some(object.sealed()), &mut work)
                .await
        )
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        operate!(context, work, self.publish_batch(writes, &mut work).await)
    }
    async fn put_many_hashed(
        &self,
        objects: &[crate::storage::HashedObject],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        operate!(context, work, self.publish_batch(objects, &mut work).await)
    }
    async fn flush_before_publish(
        &self,
        scope: crate::PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        accounting(context)?
            .check()
            .map_err(crate::async_storage::object_error)?;
        // The stream entry is the commit: its volume retains the closure,
        // which also makes it durable, first.
        operate!(context, work, self.retain_scope(scope, &mut work).await)?;
        Ok(crate::PublicationHold::none())
    }
    fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl std::future::Future<Output = Result<ObjectRead, ObjectStoreError>>
    + acyclic_stream::ProviderTask {
        self.read_object::<false>(id, maximum, context)
    }
    fn read_stored(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut AuthorityWork<'_>,
    ) -> impl std::future::Future<Output = Result<ObjectRead, ObjectStoreError>>
    + acyclic_stream::ProviderTask {
        self.read_object::<true>(id, maximum, context)
    }
    fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl std::future::Future<
        Output = Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError>,
    > + acyclic_stream::ProviderTask {
        self.read_objects::<false>(requests, context)
    }
    fn read_many_stored(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> impl std::future::Future<
        Output = Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError>,
    > + acyclic_stream::ProviderTask {
        self.read_objects::<true>(requests, context)
    }
    async fn contains(
        &self,
        id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        operate!(
            context,
            work,
            async {
                work.charge(WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                match self.provider.head(blob(id)).await {
                    Ok(_) => Ok(true),
                    Err(error) if error.code == wire::ErrorCode::NotFound => Ok(false),
                    Err(error) => Err(ObjectStoreError::LogicalObjects(error).into()),
                }
            }
            .await
        )
    }
}
pub(super) fn blob(id: ObjectId) -> acyclic_objects::Digest {
    acyclic_objects::Digest::from_bytes(
        *id.storage
            .map_or(id.digest, |storage| storage.locator)
            .as_bytes(),
    )
}
fn service_error(error: Error) -> ObjectStoreError {
    match error.code {
        wire::ErrorCode::NotFound => ObjectStoreError::Missing,
        _ => ObjectStoreError::LogicalObjects(error),
    }
}
#[cfg(not(target_arch = "wasm32"))]
fn native_error(
    error: acyclic_objects::NativeObjectsError,
    original: &crate::NativeMaterializationWork,
) -> ObjectStoreError {
    use acyclic_objects::NativeObjectsCause;
    if error.has_payload() {
        return ObjectStoreError::NativeObjects(error);
    }
    match error.into_cause() {
        NativeObjectsCause::Service(service) => service_error(service),
        NativeObjectsCause::Admission(error) => {
            ObjectStoreError::Native(original.runtime_error(error))
        }
        NativeObjectsCause::Unsupported => ObjectStoreError::NativeUnavailable,
    }
}
mod requests;
use requests::*;
pub(crate) mod epoch;
mod read;
mod write;
pub use epoch::Epoch;

#[cfg(test)]
#[cfg(feature = "memory")]
mod tests;

mod sealing;
pub use sealing::{ObjectSealer, PlainObjects};
