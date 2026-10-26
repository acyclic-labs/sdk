//! S3-compatible durable publication over the canonical logical Objects core.
//!
//! Immutable commands and their bodies are published by a conditional head PUT.
//! Logical bucket names are isolated inside a caller-provisioned physical bucket.

use super::{Error, MemoryObjects, MemoryOptions, Object, ObjectsProvider, request, wire};
use acyclic_stream::OwnedBytes;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path,
};
use prost::Message;
use std::sync::Arc;
use tokio::sync::Mutex;

const PART: usize = 5 * 1024 * 1024;
const MAX_FRAME: usize = 80 * 1024 * 1024;

/// Endpoint capabilities, configured by the host rather than a vendor name.
#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
    /// Atomic If-None-Match: * and If-Match conditional PUTs.
    pub conditional_put: bool,
    /// Successful PUTs are immediately visible to subsequent GETs.
    pub read_after_write: bool,
    /// Multipart uploads with at least 5 MiB nonfinal parts.
    pub multipart: bool,
    /// HTTP byte-range GET support.
    pub range_reads: bool,
}

#[derive(Clone, PartialEq, Message)]
struct Head {
    #[prost(string, tag = "1")]
    node: String,
    #[prost(bytes = "vec", tag = "2")]
    cursor_key: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct Frame {
    #[prost(string, tag = "1")]
    previous: String,
    #[prost(string, tag = "2")]
    route: String,
    #[prost(bytes = "vec", tag = "3")]
    request: Vec<u8>,
    #[prost(bytes = "bytes", tag = "4")]
    body: bytes::Bytes,
    #[prost(message, optional, tag = "5")]
    at: Option<prost_types::Timestamp>,
}

struct Cache {
    core: MemoryObjects,
    node: String,
}
struct Inner {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    capabilities: Capabilities,
    cache: Mutex<Cache>,
}

/// Logical Objects provider backed by an S3-compatible physical bucket.
#[derive(Clone)]
pub struct S3Objects {
    inner: Arc<Inner>,
}

fn backend(error: impl std::fmt::Display) -> Error {
    tracing::error!(%error, "S3 Objects operation failed; reopen S3Objects::open with the same physical bucket and prefix");
    wire::ErrorCode::Unavailable.into()
}

impl S3Objects {
    /// Opens a private prefix in an existing physical bucket. Configure endpoint,
    /// credentials, region and addressing on `AmazonS3Builder`; no vendor name is
    /// accepted or inspected by this adapter.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        capabilities: Capabilities,
    ) -> Result<Self, Error> {
        if !capabilities.conditional_put || !capabilities.read_after_write {
            return Err(wire::ErrorCode::Unsupported.into());
        }
        let core = MemoryObjects::new(MemoryOptions::default())?;
        let provider = Self {
            inner: Arc::new(Inner {
                store,
                prefix,
                capabilities,
                cache: Mutex::new(Cache {
                    core,
                    node: String::new(),
                }),
            }),
        };
        let mut cursor_key = [0; 32];
        getrandom::fill(&mut cursor_key).map_err(backend)?;
        let head = Head {
            node: String::new(),
            cursor_key: cursor_key.to_vec(),
        };
        match provider
            .inner
            .store
            .put_opts(
                &provider.path("head"),
                head.encode_to_vec().into(),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_)
            | Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {}
            Err(error) => return Err(backend(error)),
        }
        let mut cache = provider.inner.cache.lock().await;
        let (head, _) = provider.read_head().await?;
        provider.refresh(&mut cache, &head).await?;
        drop(cache);
        Ok(provider)
    }

    fn path(&self, key: &str) -> Path {
        self.inner.prefix.clone().join(key)
    }

    async fn read_head(&self) -> Result<(Head, UpdateVersion), Error> {
        let result = self
            .inner
            .store
            .get(&self.path("head"))
            .await
            .map_err(backend)?;
        if result.meta.size > 1024 {
            return Err(backend("oversized head"));
        }
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        if version.e_tag.is_none() {
            return Err(backend("conditional publication requires an ETag"));
        }
        let bytes = result.bytes().await.map_err(backend)?;
        let head = Head::decode(bytes).map_err(backend)?;
        if head.cursor_key.len() != 32 {
            return Err(backend("invalid cursor key"));
        }
        Ok((head, version))
    }

    async fn frame(&self, node: &str) -> Result<Frame, Error> {
        if node.len() != 64 || !node.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(backend("invalid command identity"));
        }
        let options = GetOptions {
            range: self
                .inner
                .capabilities
                .range_reads
                .then_some(GetRange::Bounded(0..MAX_FRAME as u64)),
            ..Default::default()
        };
        let result = self
            .inner
            .store
            .get_opts(&self.path(&format!("commands/{node}")), options)
            .await
            .map_err(backend)?;
        if result.meta.size > MAX_FRAME as u64 {
            return Err(backend("oversized command"));
        }
        let bytes = result.bytes().await.map_err(backend)?;
        if blake3::hash(&bytes).to_hex().as_str() != node {
            return Err(backend("command checksum mismatch"));
        }
        Frame::decode(bytes).map_err(backend)
    }

    async fn refresh(&self, cache: &mut Cache, head: &Head) -> Result<(), Error> {
        cache
            .core
            .replay_cursor_key(head.cursor_key.as_slice().try_into().map_err(backend)?)?;
        let mut next = head.node.clone();
        // Retain only fixed-size command identities while walking backwards.
        // Bodies are read and applied one at a time in publication order.
        let mut nodes = Vec::new();
        while next != cache.node {
            if next.is_empty() {
                return Err(backend("published history was replaced"));
            }
            let frame = self.frame(&next).await?;
            nodes.push(next);
            next = frame.previous;
        }
        for node in nodes.into_iter().rev() {
            let frame = self.frame(&node).await?;
            if let Err(error) = Self::apply(&cache.core, frame).await
                && error.code == wire::ErrorCode::Unavailable
            {
                return Err(error);
            }
            cache.node = node;
        }
        Ok(())
    }

    async fn apply(core: &MemoryObjects, frame: Frame) -> Result<Vec<u8>, Error> {
        core.replay_context(
            frame
                .at
                .ok_or_else(|| backend("command omitted publication time"))?,
        )?;
        macro_rules! execute {
            ($method:ident, $request:ident) => {{
                let request = wire::$request::decode(frame.request.as_slice()).map_err(backend)?;
                core.$method(request)
                    .await
                    .map(|value| value.encode_to_vec())
            }};
        }
        match frame.route.as_str() {
            "buckets/create" => execute!(create_bucket, CreateBucketRequest),
            "buckets/delete" => execute!(delete_bucket, DeleteBucketRequest),
            "objects/delete" => execute!(delete, DeleteObjectRequest),
            "objects/restore" => execute!(restore, RestoreObjectsRequest),
            "multipart/create" => execute!(create_multipart, CreateMultipartRequest),
            "multipart/complete" => execute!(complete_multipart, CompleteMultipartRequest),
            "multipart/abort" => execute!(abort_multipart, AbortMultipartRequest),
            "objects/put" => {
                let header =
                    wire::PutObjectHeader::decode(frame.request.as_slice()).map_err(backend)?;
                core.put(header, frame.body.into())
                    .await
                    .map(|value| value.encode_to_vec())
            }
            "multipart/upload-part" => {
                let header =
                    wire::UploadPartHeader::decode(frame.request.as_slice()).map_err(backend)?;
                core.upload_part(header, frame.body.into())
                    .await
                    .map(|value| value.encode_to_vec())
            }
            _ => Err(backend("unknown durable command")),
        }
    }

    async fn write_frame(&self, node: &str, bytes: bytes::Bytes) -> Result<(), Error> {
        let path = self.path(&format!("commands/{node}"));
        if !self.inner.capabilities.multipart || bytes.len() < PART {
            self.inner
                .store
                .put(&path, bytes.into())
                .await
                .map_err(backend)?;
            return Ok(());
        }
        let mut upload = self
            .inner
            .store
            .put_multipart(&path)
            .await
            .map_err(backend)?;
        let result = async {
            let mut start = 0;
            while start < bytes.len() {
                let end = (start + PART).min(bytes.len());
                upload
                    .put_part(bytes.slice(start..end).into())
                    .await
                    .map_err(backend)?;
                start = end;
            }
            upload.complete().await.map_err(backend)?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = upload.abort().await;
        }
        result
    }

    async fn mutate<R: Message + Default>(
        &self,
        route: &str,
        request: Vec<u8>,
        body: OwnedBytes,
    ) -> Result<R, Error> {
        request::validate_binary(route, &request, body.len() as u64)?;
        if request.len().saturating_add(body.len()) > MAX_FRAME - 1024 {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let provider = self.clone();
        let route = route.to_owned();
        let body = bytes::Bytes::copy_from_slice(&body);
        // Publication is owned across caller cancellation; cache publication
        // follows a successful durable CAS, never a speculative core mutation.
        let bytes = tokio::spawn(async move { provider.publish(route, request, body).await })
            .await
            .map_err(backend)??;
        R::decode(bytes.as_slice()).map_err(backend)
    }

    async fn publish(
        &self,
        route: String,
        request: Vec<u8>,
        body: bytes::Bytes,
    ) -> Result<Vec<u8>, Error> {
        let mut cache = self.inner.cache.lock().await;
        for _ in 0..8 {
            let (mut head, version) = self.read_head().await?;
            self.refresh(&mut cache, &head).await?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(backend)?;
            let frame = Frame {
                previous: head.node.clone(),
                route: route.clone(),
                request: request.clone(),
                body: body.clone(),
                at: Some(prost_types::Timestamp {
                    seconds: i64::try_from(now.as_secs()).map_err(backend)?,
                    nanos: i32::try_from(now.subsec_nanos()).map_err(backend)?,
                }),
            };
            let bytes = frame.encode_to_vec();
            let node = blake3::hash(&bytes).to_hex().to_string();
            self.write_frame(&node, bytes.into()).await?;
            head.node = node.clone();
            match self
                .inner
                .store
                .put_opts(
                    &self.path("head"),
                    head.encode_to_vec().into(),
                    PutMode::Update(version).into(),
                )
                .await
            {
                Ok(_) => {
                    let result = Self::apply(&cache.core, frame).await;
                    if !matches!(
                        result,
                        Err(Error {
                            code: wire::ErrorCode::Unavailable
                        })
                    ) {
                        cache.node = node;
                    }
                    return result;
                }
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => continue,
                Err(error) => return Err(backend(error)),
            }
        }
        Err(backend(
            "conditional publication contention; retry the same mutation identity",
        ))
    }

    async fn read<T>(
        &self,
        operation: impl AsyncFnOnce(&MemoryObjects) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut cache = self.inner.cache.lock().await;
        let (head, _) = self.read_head().await?;
        self.refresh(&mut cache, &head).await?;
        operation(&cache.core).await
    }
}

#[async_trait::async_trait]
impl ObjectsProvider for S3Objects {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        // Batch items are ordered and individually atomic at the public seam.
        // Each item uses the same durable publication path as an ordinary PUT.
        let mut results = Vec::with_capacity(requests.len());
        for (request, body) in requests {
            results.push(self.put(request, body).await);
        }
        results
    }

    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        let count = requests.len();
        match self
            .read(async |core| Ok(core.get_batch(requests).await))
            .await
        {
            Ok(results) => results,
            Err(error) => (0..count).map(|_| Err(error)).collect(),
        }
    }

    async fn create_bucket(
        &self,
        request: wire::CreateBucketRequest,
    ) -> Result<wire::Bucket, Error> {
        self.mutate("buckets/create", request.encode_to_vec(), OwnedBytes::new())
            .await
    }
    async fn head_bucket(&self, request: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        self.read(async |core| core.head_bucket(request).await)
            .await
    }
    async fn delete_bucket(
        &self,
        request: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        self.mutate("buckets/delete", request.encode_to_vec(), OwnedBytes::new())
            .await
    }
    async fn put(
        &self,
        request: wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        self.mutate("objects/put", request.encode_to_vec(), body)
            .await
    }
    async fn get(&self, request: wire::GetObjectRequest, maximum: u64) -> Result<Object, Error> {
        self.read(async |core| core.get(request, maximum).await)
            .await
    }
    async fn head(
        &self,
        request: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        self.read(async |core| core.head(request).await).await
    }
    async fn delete(
        &self,
        request: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        self.mutate("objects/delete", request.encode_to_vec(), OwnedBytes::new())
            .await
    }
    async fn restore(
        &self,
        request: wire::RestoreObjectsRequest,
    ) -> Result<wire::RestoreObjectsResponse, Error> {
        self.mutate(
            "objects/restore",
            request.encode_to_vec(),
            OwnedBytes::new(),
        )
        .await
    }
    async fn list(
        &self,
        request: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        self.read(async |core| core.list(request).await).await
    }
    async fn create_multipart(
        &self,
        request: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        self.mutate(
            "multipart/create",
            request.encode_to_vec(),
            OwnedBytes::new(),
        )
        .await
    }
    async fn upload_part(
        &self,
        request: wire::UploadPartHeader,
        body: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.mutate("multipart/upload-part", request.encode_to_vec(), body)
            .await
    }
    async fn list_parts(
        &self,
        request: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        self.read(async |core| core.list_parts(request).await).await
    }
    async fn complete_multipart(
        &self,
        request: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        self.mutate(
            "multipart/complete",
            request.encode_to_vec(),
            OwnedBytes::new(),
        )
        .await
    }
    async fn abort_multipart(
        &self,
        request: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        self.mutate(
            "multipart/abort",
            request.encode_to_vec(),
            OwnedBytes::new(),
        )
        .await
    }
}
