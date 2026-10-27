//! Counts bytes actually received from the real emulator, without synthetic replies.
use futures::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct ObservedStore {
    pub inner: Arc<dyn ObjectStore>,
    pub body_bytes: AtomicUsize,
    pub body_gets: AtomicUsize,
    pub metadata_bytes: AtomicUsize,
}

impl std::fmt::Display for ObservedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "observed {}", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(path, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        let result = self.inner.get_opts(path, opts).await?;
        let is_body = path
            .parent()
            .is_some_and(|parent| parent.filename() == Some("bodies"));
        // Consume the actual response to count transferred bytes, then pass
        // those same bytes to the adapter. Never infer bytes from the request.
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let bytes = result.bytes().await?;
        if is_body {
            println!(
                "MinIO observed body GET path={path}, received bytes={}",
                bytes.len()
            );
        }
        if is_body {
            self.body_bytes.fetch_add(bytes.len(), Ordering::Relaxed);
            self.body_gets.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metadata_bytes
                .fetch_add(bytes.len(), Ordering::Relaxed);
        }
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(bytes) },
            ))),
            meta,
            range,
            attributes,
            extensions,
        })
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
