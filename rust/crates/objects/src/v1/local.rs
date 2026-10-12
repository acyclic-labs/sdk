//! Durable logical Objects v1, backed by private state deltas and immutable bodies.
pub use super::memory::persistence::LocalOpenError;
use super::{Error, MemoryObjects, Object, ObjectsProvider, wire};
use acyclic_stream::OwnedBytes;
use futures::executor::block_on;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
use super::{NativeObjectsCause, NativeObjectsError, ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt};

fn spawn_native_read<F>(
    future: F,
    meter: &Arc<dyn NativeReadMeter>,
) -> Result<tokio::task::JoinHandle<F::Output>, NativeObjectsError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let mut future = super::Guarded {
        value: tracing::Instrument::in_current_span(future),
        _allocation: None,
    };
    future._allocation = Some(ReadAllocation::new(meter, std::mem::size_of_val(&future))?);
    // Admit the actual private task refcount control, without estimating its layout.
    meter.allocate_refcount_header()?;
    Ok(tokio::spawn(future))
}

use crate::{LocalObjectsLimits, LocalObjectsGarbageCollection};

fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
    meter: Option<Arc<dyn NativeReadMeter>>,
) -> Result<acyclic_native_runtime::NativeCompletion<T>, NativeObjectsError> {
    let span = tracing::Span::current();
    Ok(acyclic_native_runtime::run_blocking_io(move || span.in_scope(work),
        meter.map(|meter| (meter, false)))?)
}

/// One owned logical store. Clones share admission, retry receipts and ownership.
#[derive(Clone)]
pub struct LocalObjects {
    core: MemoryObjects,
    body_io: Arc<RwLock<()>>,
}
impl LocalObjects {
    /// Opens and validates a store, retaining an optional external ownership anchor.
    pub async fn open(
        root: impl AsRef<Path>,
        options: LocalObjectsLimits,
        anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    ) -> Result<Self, LocalOpenError> {
        let root = root.as_ref().to_path_buf();
        blocking(move || {
            super::memory::persistence::open(root, options, anchor).map(|core| Self {
                core,
                body_io: Arc::new(RwLock::new(())),
            })
        }, None).map_err(|_| LocalOpenError::Unavailable)?
        .await
        .map_err(|_| LocalOpenError::Unavailable)?
    }
    async fn mutate<T: Send + 'static>(
        &self,
        action: impl FnOnce(&MemoryObjects) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        self.mutate_with_meter(None, move |core| action(core).map_err(Into::into)).await
            .map_err(|error| match error.into_cause() {
                NativeObjectsCause::Service(error) => error,
                _ => wire::ErrorCode::Unavailable.into(),
            })
    }

    async fn mutate_with_meter<T: Send + 'static>(
        &self,
        meter: Option<Arc<dyn NativeReadMeter>>,
        action: impl FnOnce(&MemoryObjects) -> Result<T, NativeObjectsError> + Send + 'static,
    ) -> Result<T, NativeObjectsError> {
        if let Some(meter) = &meter { meter.check()?; }
        if self.core.local_maintenance_due()? {
            let lease = Arc::clone(&self.body_io).write_owned().await;
            let core = self.core.clone();
            let worker_meter = meter.clone();
            blocking(move || {
                let _lease = lease;
                let native = worker_meter.is_some();
                core.compact_local_if_due(worker_meter).map_err(|error| {
                    if native { error } else { Error::from(wire::ErrorCode::Unavailable).into() }
                })
            }, meter.clone())?.await
                .map_err(|_| Error::from(wire::ErrorCode::Unavailable))??;
        }
        let core = self.core.clone();
        let lease = Arc::clone(&self.body_io).read_owned().await;
        blocking(move || {
            let _lease = lease;
            action(&core)
        }, meter)?.await
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
    }

    /// Authenticates retained bodies and reclaims unreachable private segments.
    pub async fn collect_garbage(
        &self,
        maximum_candidates: u64,
    ) -> Result<LocalObjectsGarbageCollection, LocalOpenError> {
        if maximum_candidates == 0 {
            return Err(LocalOpenError::Invalid);
        }
        let lease = self.body_io.clone().write_owned().await;
        let core = self.core.clone();
        blocking(move || {
            let _lease = lease;
            core.collect_local_garbage(maximum_candidates)
        }, None).map_err(|_| LocalOpenError::Unavailable)?
        .await
        .map_err(|_| LocalOpenError::Unavailable)?
    }
}

#[async_trait::async_trait]
impl ObjectsProvider for LocalObjects {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        self.mutate(move |core| Ok(block_on(core.put_batch(requests))))
            .await
            .unwrap_or_else(|error| vec![Err(error); count])
    }

    fn put_batch_native(
        &self,
        input: ObjectsNativeRequest<Vec<(wire::PutObjectHeader, OwnedBytes)>>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            if input.data().is_empty() {
                return ObjectsBatch::new(Vec::new(), ReadAllocation::empty(&meter));
            }
            let worker_meter = Arc::clone(&meter);
            self.mutate_with_meter(Some(meter), move |core| {
                block_on(core.put_batch_native(input, worker_meter)?)
            }).await
        })
    }
    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        let core = self.core.clone();
        let body_io = self.body_io.clone();
        tokio::spawn(tracing::Instrument::in_current_span(async move {
            let _lease = body_io.read_owned().await;
            core.get_batch(requests).await
        }))
        .await
        .unwrap_or_else(|_| (0..count).map(|_| Err(wire::ErrorCode::Unavailable.into())).collect())
    }

    fn get_batch_native(
        &self,
        input: ObjectsNativeRequest<Vec<(wire::GetObjectRequest, u64)>>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            if input.data().is_empty() {
                return ObjectsBatch::new(Vec::new(), ReadAllocation::empty(&meter));
            }
            let core = self.core.clone();
            let body_io = Arc::clone(&self.body_io);
            let task_meter = Arc::clone(&meter);
            spawn_native_read(async move {
                let input = input;
                // Detached work retains the original input custody and store root.
                let _lease = body_io.read_owned().await;
                core.get_batch_native(input, task_meter)?.await
            }, &meter)?.await
                .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
        })
    }

    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        self.mutate(move |core| block_on(core.create_bucket(query)))
            .await
    }
    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        self.core.head_bucket(query).await
    }
    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        self.mutate(move |core| block_on(core.delete_bucket(query)))
            .await
    }
    async fn put(
        &self,
        query: wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        self.mutate(move |core| block_on(core.put(query, body)))
            .await
    }

    fn put_native(
        &self,
        input: ObjectsNativeRequest<(wire::PutObjectHeader, OwnedBytes)>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            let worker_meter = Arc::clone(&meter);
            self.mutate_with_meter(Some(meter), move |core| {
                block_on(core.put_native(input, worker_meter)?)
            }).await
        })
    }
    async fn get(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        // The detached read retains the store root until physical completion.
        let core = self.core.clone();
        let body_io = self.body_io.clone();
        tokio::spawn(tracing::Instrument::in_current_span(async move {
            let _lease = body_io.read_owned().await;
            core.get(query, maximum_bytes).await
        }))
        .await
        .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
    }

    fn get_native(
        &self,
        input: ObjectsNativeRequest<wire::GetObjectRequest>,
        maximum_bytes: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        super::native_input_meter(&meter, &input)?;
        super::native_future(Arc::clone(&meter), async move {
            let input = input;
            let core = self.core.clone();
            let body_io = Arc::clone(&self.body_io);
            let task_meter = Arc::clone(&meter);
            spawn_native_read(async move {
                // Detached work retains the original input custody and store root.
                let input = input;
                let _lease = body_io.read_owned().await;
                core.get_native(input, maximum_bytes, task_meter)?.await
            }, &meter)?.await
                .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
        })
    }
    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        self.core.head(query).await
    }
    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        self.mutate(move |core| block_on(core.delete(query))).await
    }
    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        self.core.list(query).await
    }
    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        self.mutate(move |core| block_on(core.create_multipart(query)))
            .await
    }
    async fn upload_part(
        &self,
        query: wire::UploadPartHeader,
        body: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.mutate(move |core| block_on(core.upload_part(query, body)))
            .await
    }
    async fn list_parts(
        &self,
        query: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        self.core.list_parts(query).await
    }
    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        self.mutate(move |core| block_on(core.complete_multipart(query)))
            .await
    }
    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        self.mutate(move |core| block_on(core.abort_multipart(query)))
            .await
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    mod collection;
    fn bucket() -> Option<wire::BucketRef> {
        Some(wire::BucketRef {
            name: "customer.inputs".into(),
        })
    }
    fn identity(key: &str) -> Option<wire::MutationIdentity> {
        Some(wire::MutationIdentity {
            idempotency_key: key.into(),
        })
    }
    fn put(key: &str) -> wire::PutObjectHeader {
        wire::PutObjectHeader {
            bucket: bucket(),
            object_key: key.into(),
            mutation: identity(key),
            ..Default::default()
        }
    }
    fn get(key: &str) -> wire::GetObjectRequest {
        wire::GetObjectRequest {
            bucket: bucket(),
            object_key: key.into(),
            ..Default::default()
        }
    }
    async fn create(root: &Path) -> Result<LocalObjects, LocalOpenError> {
        LocalObjects::open(root, LocalObjectsLimits::default(), None).await
    }
    #[tokio::test]
    async fn every_logical_operation_passes_and_the_empty_store_reopens()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let provider = create(root.path()).await?;
        super::super::conformance::verify(&provider, "durable-v1").await?;
        drop(provider);
        let reopened = create(root.path()).await?;
        super::super::conformance::verify(&reopened, "durable-v1-reopened").await?;
        Ok(())
    }
    #[tokio::test]
    async fn reopen_keeps_exact_receipts_current_keys_and_native_batch_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let provider = create(root.path()).await?;
        provider
            .create_bucket(wire::CreateBucketRequest {
                name: "customer.inputs".into(),
                mutation: None,
            })
            .await?;
        let first = provider
            .put(put("value"), OwnedBytes::from_static(b"before"))
            .await?;
        let mut replacement = put("value");
        replacement.mutation = identity("replacement");
        let last = provider
            .put(replacement, OwnedBytes::from_static(b"after"))
            .await?;
        drop(provider);
        let provider = create(root.path()).await?;
        assert_eq!(
            provider
                .put(put("value"), OwnedBytes::from_static(b"before"))
                .await?,
            first
        );
        assert_eq!(
            provider.get(get("value"), 5).await?.into_body(),
            OwnedBytes::from_static(b"after")
        );
        assert_eq!(
            provider
                .head(wire::HeadObjectRequest {
                    bucket: bucket(),
                    object_key: "value".into(),
                    ..Default::default()
                })
                .await?
                .object,
            Some(last)
        );
        let results = provider
            .put_batch(vec![
                (put("second"), OwnedBytes::from_static(b"two")),
                (put("third"), OwnedBytes::from_static(b"three")),
            ])
            .await;
        assert!(results.iter().all(Result::is_ok));
        let reads = provider
            .get_batch(vec![
                (get("third"), 5),
                (get("missing"), 10),
                (get("second"), 3),
            ])
            .await;
        assert_eq!(
            reads
                .first()
                .ok_or("missing result")?
                .as_ref()
                .map_err(|error| *error)?
                .body().clone(),
            OwnedBytes::from_static(b"three")
        );
        assert_eq!(
            reads
                .get(1)
                .ok_or("missing result")?
                .as_ref()
                .err()
                .map(|error| error.code),
            Some(wire::ErrorCode::NotFound)
        );
        assert_eq!(
            reads
                .get(2)
                .ok_or("missing result")?
                .as_ref()
                .map_err(|error| *error)?
                .body().clone(),
            OwnedBytes::from_static(b"two")
        );
        Ok(())
    }
    #[tokio::test]
    async fn ownership_capacity_and_v1_store_boundaries_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let provider = create(root.path()).await?;
        assert!(matches!(
            create(root.path()).await,
            Err(LocalOpenError::AlreadyOwned)
        ));
        drop(provider);
        let options = LocalObjectsLimits {
            maximum_bytes: 123,
            ..LocalObjectsLimits::default()
        };
        assert!(matches!(
            LocalObjects::open(root.path(), options, None).await,
            Err(LocalOpenError::Invalid)
        ));
        let legacy = tempfile::tempdir()?;
        // Historical journal rejection fixture.
        let limits = LocalObjectsLimits::default();
        let mut header = b"ACYCLIC-OBJECTS-LOCAL\0\x04".to_vec();
        for limit in [
            limits.maximum_object_bytes,
            limits.maximum_bytes,
            limits.maximum_journal_operations,
            limits.maximum_journal_bytes,
        ] {
            header.extend_from_slice(&limit.to_le_bytes());
        }
        std::fs::write(legacy.path().join("mutations.log"), &header)?;
        let before = std::fs::read(legacy.path().join("mutations.log"))?;
        assert!(matches!(
            create(legacy.path()).await,
            Err(LocalOpenError::Corrupt)
        ));
        assert_eq!(std::fs::read(legacy.path().join("mutations.log"))?, before);
        Ok(())
    }
}
