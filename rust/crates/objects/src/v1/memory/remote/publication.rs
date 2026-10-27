//! Shared conditional-root publication over canonical Objects state.
use super::*;
use futures::future::BoxFuture;
use tokio::sync::Mutex;

/// Physical IO only. Versions are opaque conditional-write tokens.
pub trait Store: ImmutableBodies + Send + Sync + 'static {
    /// Opaque conditional-write version of a head.
    type Version: Send + Sync;
    /// Read the head and its version.
    fn head(&self) -> BoxFuture<'_, Result<(Vec<u8>, Self::Version), Error>>;
    /// Create or conditionally replace a head; false means a lost race.
    fn publish<'a>(
        &'a self,
        version: Option<&'a Self::Version>,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<bool, Error>>;
    /// Read bounded immutable metadata.
    fn get<'a>(
        &'a self,
        kind: &'a str,
        id: &'a str,
        maximum: usize,
    ) -> BoxFuture<'a, Result<Vec<u8>, Error>>;
    /// Write immutable metadata.
    fn put<'a>(
        &'a self,
        kind: &'a str,
        id: &'a str,
        bytes: bytes::Bytes,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

#[derive(Clone, PartialEq, Message)]
struct Head {
    #[prost(string, tag = "1")]
    node: String,
    #[prost(bytes = "vec", tag = "2")]
    cursor_key: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct Root {
    #[prost(uint64, tag = "1")]
    sequence: u64,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pages: Vec<Vec<u8>>,
}

struct Command {
    route: String,
    request: Vec<u8>,
    body: OwnedBytes,
    at: prost_types::Timestamp,
}
struct Cache {
    core: MemoryObjects,
    node: String,
}
struct Inner<T> {
    transport: Arc<T>,
    cache: Mutex<Cache>,
}
/// Durable Objects core instantiated by a physical transport adapter.
pub struct Publisher<T> {
    inner: Arc<Inner<T>>,
}
impl<T> Clone for Publisher<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
fn backend(error: impl std::fmt::Display) -> Error {
    tracing::error!(%error, "Objects root publication failed; reopen the same storage namespace");
    Unavailable.into()
}

impl<T: Store> Publisher<T> {
    pub(in crate::v1) async fn attach(transport: Arc<T>) -> Result<Self, Error> {
        let core = MemoryObjects::new(MemoryOptions::default())?;
        let provider = Self {
            inner: Arc::new(Inner {
                transport,
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
        provider
            .inner
            .transport
            .publish(None, head.encode_to_vec())
            .await?;
        let mut cache = provider.inner.cache.lock().await;
        let (head, _) = provider.read_head().await?;
        provider.refresh(&mut cache, &head).await?;
        drop(cache);
        Ok(provider)
    }
    async fn read_head(&self) -> Result<(Head, T::Version), Error> {
        let (bytes, version) = self.inner.transport.head().await?;
        if bytes.len() > 1024 {
            return Err(backend("oversized head"));
        }
        let head = Head::decode(bytes.as_slice()).map_err(backend)?;
        if head.cursor_key.len() != 32 {
            return Err(backend("invalid cursor key"));
        }
        Ok((head, version))
    }
    async fn metadata(&self, kind: &str, id: &str) -> Result<Vec<u8>, Error> {
        let bytes = Store::get(self.inner.transport.as_ref(), kind, id, RECORD_BYTES).await?;
        if bytes.len() > RECORD_BYTES || blake3::hash(&bytes).to_hex().as_str() != id {
            return Err(backend("invalid metadata size or checksum"));
        }
        Ok(bytes)
    }
    async fn refresh(&self, cache: &mut Cache, head: &Head) -> Result<(), Error> {
        if cache.node == head.node {
            cache
                .core
                .replay_cursor_key(head.cursor_key.as_slice().try_into().map_err(backend)?)?;
            return Ok(());
        }
        let bytes = self.metadata("roots", &head.node).await?;
        let root = Root::decode(bytes.as_slice()).map_err(backend)?;
        if root.pages.len() > MemoryOptions::default().maximum_entries {
            return Err(backend("current-state root exceeds entry bound"));
        }
        let mut pages = Vec::with_capacity(root.pages.len());
        for page in root.pages {
            if page.len() != 32 {
                return Err(backend("invalid metadata page identity"));
            }
            pages.push(self.metadata("pages", &hex::encode(page)).await?);
        }
        let source: Arc<dyn ImmutableBodies> = self.inner.transport.clone();
        let core = MemoryObjects::load_snapshot(
            pages,
            root.sequence,
            head.cursor_key.as_slice().try_into().map_err(backend)?,
            source,
        )?;
        cache.core = core;
        cache.node = head.node.clone();
        Ok(())
    }

    async fn apply(core: &MemoryObjects, frame: Command) -> Result<Vec<u8>, Error> {
        core.replay_context(frame.at)?;
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

    pub(in crate::v1) async fn mutate<R: Message + Default + Send + 'static>(
        &self,
        route: &str,
        request: Vec<u8>,
        body: OwnedBytes,
    ) -> Result<R, Error> {
        request::validate_binary(route, &request, body.len() as u64)?;
        let route = route.to_owned();
        let bytes = self
            .transaction(move |core, at| {
                Box::pin(Self::apply(
                    core,
                    Command {
                        route: route.clone(),
                        request: request.clone(),
                        body: body.clone(),
                        at,
                    },
                ))
            })
            .await?;
        R::decode(bytes.as_slice()).map_err(backend)
    }
    pub(in crate::v1) async fn publish_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        match self
            .transaction(move |core, at| {
                let requests = requests.clone();
                Box::pin(async move {
                    core.replay_context(at)?;
                    Ok(core.put_batch(requests).await)
                })
            })
            .await
        {
            Ok(results) => results,
            Err(error) => vec![Err(error); count],
        }
    }
    async fn transaction<R, F>(&self, operation: F) -> Result<R, Error>
    where
        R: Send + 'static,
        F: for<'a> Fn(&'a MemoryObjects, prost_types::Timestamp) -> BoxFuture<'a, Result<R, Error>>
            + Send
            + Sync
            + 'static,
    {
        let provider = self.clone();
        // One owned publication survives cancellation; no speculative state escapes.
        tokio::spawn(async move { provider.publish(operation).await })
            .await
            .map_err(backend)?
    }
    async fn publish<R, F>(&self, operation: F) -> Result<R, Error>
    where
        F: for<'a> Fn(&'a MemoryObjects, prost_types::Timestamp) -> BoxFuture<'a, Result<R, Error>>,
    {
        let mut cache = self.inner.cache.lock().await;
        for _ in 0..8 {
            let (mut head, version) = self.read_head().await?;
            self.refresh(&mut cache, &head).await?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(backend)?;
            let staged = cache.core.stage()?;
            let result = operation(
                &staged,
                prost_types::Timestamp {
                    seconds: i64::try_from(now.as_secs()).map_err(backend)?,
                    nanos: i32::try_from(now.subsec_nanos()).map_err(backend)?,
                },
            )
            .await?;
            let source: Arc<dyn ImmutableBodies> = self.inner.transport.clone();
            let (sequence, pages) = staged.snapshot(source).await?;
            let mut root = Root {
                sequence,
                pages: Vec::with_capacity(pages.len()),
            };
            for bytes in pages {
                let id = blake3::hash(&bytes);
                Store::put(
                    self.inner.transport.as_ref(),
                    "pages",
                    &id.to_hex().to_string(),
                    bytes.into(),
                )
                .await?;
                root.pages.push(id.as_bytes().to_vec());
            }
            let bytes = root.encode_to_vec();
            if bytes.len() > RECORD_BYTES {
                return Err(backend("current-state root exceeds metadata bound"));
            }
            let node = blake3::hash(&bytes).to_hex().to_string();
            Store::put(self.inner.transport.as_ref(), "roots", &node, bytes.into()).await?;
            head.node = node.clone();
            if self
                .inner
                .transport
                .publish(Some(&version), head.encode_to_vec())
                .await?
            {
                cache.core = staged;
                cache.node = node;
                return Ok(result);
            }
        }
        Err(backend(
            "conditional publication contention; retry the same mutation identity",
        ))
    }
    pub(in crate::v1) async fn read<R>(
        &self,
        operation: impl AsyncFnOnce(&MemoryObjects) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut cache = self.inner.cache.lock().await;
        let (head, _) = self.read_head().await?;
        self.refresh(&mut cache, &head).await?;
        operation(&cache.core).await
    }
}

#[async_trait::async_trait]
impl<T: Store> ObjectsProvider for Publisher<T> {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        self.publish_batch(requests).await
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
