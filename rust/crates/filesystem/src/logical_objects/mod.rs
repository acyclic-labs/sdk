//! Filesystem immutable content over canonical logical Objects v1.
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError, ObjectWrite,
    object_digest,
};
use crate::{AsyncObjectStore, AuthorityWork, CancellationToken, WorkCounters, WorkError};
use acyclic_objects::v1::{Error, Object, ObjectsProvider, wire};
use acyclic_stream::OwnedBytes;
use std::{fmt::Write as _, sync::Arc};

/// Immutable filesystem-object storage over one exact public Objects bucket.
#[derive(Clone)]
pub struct LogicalObjectStore<P> {
    provider: Arc<P>,
    bucket: wire::BucketRef,
    maximum_object_bytes: u64,
}

impl<P> LogicalObjectStore<P> {
    /// Binds an authenticated provider to the account's dedicated filesystem bucket.
    pub fn new(provider: Arc<P>, bucket: wire::BucketRef) -> Self {
        Self {
            provider,
            bucket,
            maximum_object_bytes: 5 * 1024 * 1024 * 1024,
        }
    }

    /// Applies the composition's existing positive per-object bound.
    pub fn with_object_limit(mut self, maximum: u64) -> Result<Self, ObjectStoreError> {
        if maximum == 0 || usize::try_from(maximum).is_err() {
            return Err(ObjectStoreError::Rejected(
                "invalid object size limit".to_owned(),
            ));
        }
        self.maximum_object_bytes = maximum;
        Ok(self)
    }

    /// Returns the exact backing bucket identity used by this adapter.
    pub fn bucket(&self) -> &wire::BucketRef {
        &self.bucket
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
        let mut $work = accounting($context)?;
        let result = $body;
        let spent = $work.spent();
        drop($work);
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
impl<P: ObjectsProvider> AsyncObjectStore for LogicalObjectStore<P> {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        operate!(
            context,
            work,
            async {
                self.validate_write(id, &bytes, &mut work)?;
                self.publish(id, bytes, &mut work).await
            }
            .await
        )
    }
    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let (id, bytes) = object.into_parts();
        operate!(context, work, self.publish(id, bytes, &mut work).await)
    }
    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        operate!(context, work, self.publish_batch(writes, &mut work).await)
    }
    async fn flush_before_publish(
        &self,
        _scope: crate::PublicationScope<'_>,
        context: &mut AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        accounting(context)?
            .check()
            .map_err(crate::async_storage::object_error)?;
        Ok(crate::PublicationHold::none())
    }
    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        context: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let read = operate!(
            context,
            work,
            self.fetch(id, maximum, &mut work)
                .await
                .map(|(read, _)| read)
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = context.original {
            if matches!(read.retention, ObjectReadRetention::Owned { .. }) {
                return crate::async_storage::owned_read(read, original);
            }
        }
        Ok(read)
    }
    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let (reads, allocation) =
            operate!(context, work, self.fetch_batch(requests, &mut work).await)?;
        match allocation {
            KernelOwnedAllocation::Standalone => Ok((reads, None)),
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(allocation) => {
                crate::async_storage::native_read_batch(reads, allocation)
                    .map(|(reads, allocation)| (reads, Some(allocation)))
            }
        }
    }
    async fn contains(
        &self,
        id: ObjectId,
        context: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        #[cfg(not(target_arch = "wasm32"))]
        if context.original.is_some() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        let maximum = context.read_budget()?.object_bytes_read;
        match self.read(id, maximum, context).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::Missing) => Ok(false),
            Err(error) => Err(error),
        }
    }
}
fn service_error(error: Error) -> ObjectStoreError {
    match error.code {
        wire::ErrorCode::NotFound => ObjectStoreError::Missing,
        _ => ObjectStoreError::LogicalObjects(error),
    }
}
#[cfg(not(target_arch = "wasm32"))]
fn native_error(error: acyclic_objects::v1::NativeObjectsError) -> ObjectStoreError {
    use acyclic_objects::v1::NativeObjectsCause;
    match &error.cause {
        NativeObjectsCause::Service(service) if !error.has_payload() => service_error(*service),
        NativeObjectsCause::Unsupported if !error.has_payload() => {
            ObjectStoreError::NativeUnavailable
        }
        _ => ObjectStoreError::NativeObjects(error),
    }
}
fn precondition(error: &ObjectStoreError) -> bool {
    match error {
        ObjectStoreError::LogicalObjects(error) => {
            error.code == wire::ErrorCode::PreconditionFailed
        }
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::NativeObjects(error) => {
            use acyclic_objects::v1::NativeObjectsCause;
            let mut error = error;
            while let NativeObjectsCause::SharedFailure(inner) = &error.cause {
                error = inner.error();
            }
            matches!(&error.cause, NativeObjectsCause::Service(service) if service.code == wire::ErrorCode::PreconditionFailed)
        }
        _ => false,
    }
}
mod requests;
use requests::*;
mod read;
#[cfg(test)]
mod tests;
mod write;
