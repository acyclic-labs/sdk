//! Durable logical Objects v2, backed by private state deltas and immutable bodies.
pub use super::memory::persistence::LocalOpenError;
use super::{Error, MemoryObjects, NativeBatchObjects, Object, ObjectsProvider, wire};
use bytes::Bytes;
use futures::executor::block_on;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Durable capacities and explicit host synchronization policy.
pub type LocalOptions = crate::LocalObjectsLimits;
/// Private physical storage reclamation counters; no public Objects history.
pub type GarbageCollection = crate::LocalObjectsGarbageCollection;

/// One owned logical store. Clones share admission, retry receipts and ownership.
#[derive(Clone)]
pub struct LocalObjects {
    core: MemoryObjects,
    body_io: Arc<RwLock<()>>,
}
impl LocalObjects {
    /// Opens a v2 store and validates every replayed record and referenced segment.
    ///
    /// The v1 journal is not silently converted or overwritten.
    pub async fn open(
        root: impl AsRef<Path>,
        options: LocalOptions,
    ) -> Result<Self, LocalOpenError> {
        Self::open_inner(root, options, None).await
    }
    /// Opens while retaining the native composition's external ownership anchor.
    #[doc(hidden)]
    pub async fn open_with_ownership_anchor(
        root: impl AsRef<Path>,
        options: LocalOptions,
        anchor: acyclic_native_runtime::OwnershipAnchor,
    ) -> Result<Self, LocalOpenError> {
        Self::open_inner(root, options, Some(anchor)).await
    }
    async fn open_inner(
        root: impl AsRef<Path>,
        options: LocalOptions,
        anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
    ) -> Result<Self, LocalOpenError> {
        let root = root.as_ref().to_path_buf();
        acyclic_native_runtime::run_blocking_io(move || {
            super::memory::persistence::open(root, options, anchor).map(|core| Self {
                core,
                body_io: Arc::new(RwLock::new(())),
            })
        })
        .await
        .map_err(|_| LocalOpenError::Unavailable)?
    }
    async fn mutate<T: Send + 'static>(
        &self,
        action: impl FnOnce(&MemoryObjects) -> Result<T, Error> + Send + 'static,
    ) -> Result<T, Error> {
        if self.core.local_maintenance_due()? {
            let lease = self.body_io.clone().write_owned().await;
            let core = self.core.clone();
            acyclic_native_runtime::run_blocking_io(move || {
                let _lease = lease;
                core.compact_local_if_due()
            })
            .await
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
        }
        let core = self.core.clone();
        let lease = self.body_io.clone().read_owned().await;
        acyclic_native_runtime::run_blocking_io(move || {
            let _lease = lease;
            action(&core)
        })
        .await
        .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
    }

    /// Authenticates retained bodies and reclaims unreachable private segments.
    ///
    /// Collection waits for admitted physical reads and mutations. Cancellation
    /// detaches observation while the owned worker keeps the lease and root.
    pub async fn collect_garbage(
        &self,
        maximum_candidates: u64,
    ) -> Result<GarbageCollection, LocalOpenError> {
        if maximum_candidates == 0 {
            return Err(LocalOpenError::Invalid);
        }
        let lease = self.body_io.clone().write_owned().await;
        let core = self.core.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            let _lease = lease;
            core.collect_local_garbage(maximum_candidates)
        })
        .await
        .map_err(|_| LocalOpenError::Unavailable)?
    }
}

#[async_trait::async_trait]
impl ObjectsProvider for LocalObjects {
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
        body: Bytes,
    ) -> Result<wire::ObjectInfo, Error> {
        self.mutate(move |core| block_on(core.put(query, body)))
            .await
    }
    async fn get(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        // Caller cancellation detaches observation, not the already owned read.
        // Its core clone keeps the root/anchor alive until physical work ends.
        let core = self.core.clone();
        let body_io = self.body_io.clone();
        tokio::spawn(async move {
            let _lease = body_io.read_owned().await;
            core.get(query, maximum_bytes).await
        })
        .await
        .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
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
        body: Bytes,
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
#[async_trait::async_trait]
impl NativeBatchObjects for LocalObjects {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, Bytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        self.mutate(move |core| Ok(block_on(core.put_batch(requests))))
            .await
            .unwrap_or_else(|error| vec![Err(error); count])
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
        tokio::spawn(async move {
            let _lease = body_io.read_owned().await;
            core.get_batch(requests).await
        })
        .await
        .unwrap_or_else(|_| vec![Err(wire::ErrorCode::Unavailable.into()); count])
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
        LocalObjects::open(root, LocalOptions::default()).await
    }
    #[tokio::test]
    async fn every_logical_operation_passes_and_the_empty_store_reopens()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let provider = create(root.path()).await?;
        super::super::conformance::verify(&provider, "durable-v2").await?;
        drop(provider);
        let reopened = create(root.path()).await?;
        super::super::conformance::verify(&reopened, "durable-v2-reopened").await?;
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
            .put(put("value"), Bytes::from_static(b"before"))
            .await?;
        let mut replacement = put("value");
        replacement.mutation = identity("replacement");
        let last = provider
            .put(replacement, Bytes::from_static(b"after"))
            .await?;
        drop(provider);
        let provider = create(root.path()).await?;
        assert_eq!(
            provider
                .put(put("value"), Bytes::from_static(b"before"))
                .await?,
            first
        );
        assert_eq!(
            provider.get(get("value"), 5).await?.body,
            Bytes::from_static(b"after")
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
                (put("second"), Bytes::from_static(b"two")),
                (put("third"), Bytes::from_static(b"three")),
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
                .body,
            Bytes::from_static(b"three")
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
                .body,
            Bytes::from_static(b"two")
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
        let options = LocalOptions {
            maximum_bytes: 123,
            ..LocalOptions::default()
        };
        assert!(matches!(
            LocalObjects::open(root.path(), options).await,
            Err(LocalOpenError::Invalid)
        ));
        let legacy = tempfile::tempdir()?;
        // Exact empty v1 journal header, retained as a rejection fixture after
        // retiring the v1 engine. No legacy provider is needed to create it.
        let limits = LocalOptions::default();
        let mut header = b"ACYCLIC-OBJECTS-LOCAL\0\x04".to_vec();
        for limit in [
            limits.maximum_object_bytes,
            limits.maximum_bytes,
            limits.maximum_journal_operations,
            limits.maximum_journal_bytes,
        ] {
            header.extend_from_slice(&limit.to_le_bytes());
        }
        assert_eq!(header.len(), 55);
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
