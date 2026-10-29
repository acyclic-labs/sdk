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
    publication_root: Option<acyclic_objects::RootId>,
    /// Stored blobs some publication root has not retained yet, shared by
    /// every clone whatever its root.
    staged: Arc<std::sync::Mutex<Staged>>,
}

/// Stored blobs in staging order, with each publication root's position:
/// a root has retained every blob before its position. Positions only
/// advance, each by its own root's retention, so one root's retention never
/// covers another's. Blobs every root has passed are dropped.
#[derive(Default)]
struct Staged {
    /// The position of the oldest kept blob.
    base: u64,
    digests: std::collections::VecDeque<acyclic_objects::Digest>,
    roots: std::collections::HashMap<acyclic_objects::RootId, u64>,
}

impl Staged {
    fn end(&self) -> u64 {
        self.base + self.digests.len() as u64
    }

    /// Records that `root` retained every blob before `end`.
    fn retained(&mut self, root: &acyclic_objects::RootId, end: u64) {
        if let Some(position) = self.roots.get_mut(root) {
            *position = (*position).max(end);
        }
        let oldest = self.roots.values().copied().min().unwrap_or(self.end());
        while self.base < oldest && self.digests.pop_front().is_some() {
            self.base += 1;
        }
    }
}

impl<P: ?Sized> LogicalObjectStore<P> {
    /// Binds an account-scoped blob provider.
    pub fn new(provider: Arc<P>) -> Self {
        Self {
            provider,
            sealer: PlainObjects,
            maximum_object_bytes: 5 * 1024 * 1024 * 1024,
            decoded: Arc::new(crate::cache::CachedObjectStore::with_default_options(())),
            publication_root: None,
            staged: Arc::default(),
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
            publication_root: self.publication_root,
            staged: self.staged,
        }
    }

    /// Retains every published closure under `root` before its record is
    /// written, so no number of collections removes a live generation.
    ///
    /// Clones share staged blobs, and each root retains them independently:
    /// a root starts from the oldest blob some root still owes.
    #[must_use]
    pub fn with_publication_root(mut self, root: acyclic_objects::RootId) -> Self {
        let mut staged = self.staged();
        let base = staged.base;
        staged.roots.entry(root.clone()).or_insert(base);
        drop(staged);
        self.publication_root = Some(root);
        self
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

    fn staged(&self) -> std::sync::MutexGuard<'_, Staged> {
        self.staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Notes stored objects for each root's next publication that cannot
    /// enumerate its closure.
    fn published(&self, ids: impl IntoIterator<Item = ObjectId>) {
        let mut staged = self.staged();
        if !staged.roots.is_empty() {
            staged.digests.extend(ids.into_iter().map(blob));
        }
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
impl<P: Blobs + ?Sized> LogicalObjectStore<P> {
    /// The stream entry is the commit: its closure is durable first, in one
    /// durability round.
    async fn flush_scope(
        &self,
        scope: crate::PublicationScope<'_>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.charge(WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let flushed = match scope {
            crate::PublicationScope::Closure { objects, .. } => {
                let blobs: Vec<_> = objects.iter().copied().map(blob).collect();
                self.provider.flush(Some(&blobs)).await
            }
            crate::PublicationScope::Everything => self.provider.flush(None).await,
        };
        flushed.map_err(ObjectStoreError::LogicalObjects)?;
        work.check()
    }
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
        let Some(root) = &self.publication_root else {
            operate!(context, work, self.flush_scope(scope, &mut work).await)?;
            return Ok(crate::PublicationHold::none());
        };
        let root = root.clone();
        operate!(
            context,
            work,
            self.retain_scope(root, scope, &mut work).await
        )?;
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
fn native_error(error: acyclic_objects::NativeObjectsError) -> ObjectStoreError {
    use acyclic_objects::NativeObjectsCause;
    match &error.cause {
        NativeObjectsCause::Service(service) if !error.has_payload() => service_error(*service),
        NativeObjectsCause::Unsupported if !error.has_payload() => {
            ObjectStoreError::NativeUnavailable
        }
        _ => ObjectStoreError::NativeObjects(error),
    }
}
mod requests;
use requests::*;
mod read;
mod write;

#[cfg(test)]
#[cfg(feature = "memory")]
mod tests;

mod sealing;
pub use sealing::{ObjectSealer, PlainObjects};
