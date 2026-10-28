//! Counts bytes actually received from the real emulator, without synthetic replies.
use futures::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub struct ObservedStore {
    pub inner: Arc<dyn acyclic_objects::s3::Bucket>,
    pub body_bytes: AtomicUsize,
    pub body_gets: AtomicUsize,
    pub metadata_bytes: AtomicUsize,
    pub envelope_creates: AtomicUsize,
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
        // Envelopes live at `<account>/e/...`; creating one is a blob's commit point.
        let commit =
            path.parts().any(|part| part.as_ref() == "e") && matches!(&opts.mode, PutMode::Create);
        let started = std::time::Instant::now();
        let result = self.inner.put_opts(path, payload, opts).await;
        if std::env::var_os("STORAGE_IO_TRACE").is_some() {
            println!(
                "MinIO PUT {path} {:?} ok={}",
                started.elapsed(),
                result.is_ok()
            );
        }
        if commit && result.is_ok() {
            self.envelope_creates.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    async fn get_opts(&self, path: &Path, opts: GetOptions) -> Result<GetResult> {
        let started = std::time::Instant::now();
        let result = self.inner.get_opts(path, opts).await?;
        // Payloads live at `<account>/p/<shard>/<digest>`; envelopes and roots elsewhere.
        let is_body = path.parts().any(|part| part.as_ref() == "p");
        // Consume the actual response to count transferred bytes, then pass
        // those same bytes to the adapter. Never infer bytes from the request.
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        let bytes = result.bytes().await?;
        if std::env::var_os("STORAGE_IO_TRACE").is_some() {
            println!(
                "MinIO GET {path} {:?} bytes={}",
                started.elapsed(),
                bytes.len()
            );
        }
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

#[async_trait::async_trait]
impl signer::Signer for ObservedStore {
    async fn signed_url(
        &self,
        method: acyclic_objects::s3::Method,
        path: &Path,
        expires_in: std::time::Duration,
    ) -> Result<acyclic_objects::s3::Url> {
        self.inner.signed_url(method, path, expires_in).await
    }
    async fn signed_url_opts(
        &self,
        method: acyclic_objects::s3::Method,
        path: &Path,
        expires_in: std::time::Duration,
        options: &signer::SignedUrlOptions,
    ) -> Result<acyclic_objects::s3::Url> {
        self.inner
            .signed_url_opts(method, path, expires_in, options)
            .await
    }
}
