use super::*;
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::multipart::PartId;
use object_store::{
    CopyOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Retains the actual submitted bodies while the real provider handles writes,
/// conditions and multipart storage. Signing is unavailable, so multipart
/// controls exercise the original abort path after all parts were accepted.
#[derive(Debug, Default)]
pub(super) struct Recording {
    pub(super) inner: InMemory,
    uploads: Mutex<Vec<PutPayload>>,
    pub(super) starts: AtomicUsize,
    pub(super) aborts: AtomicUsize,
}

impl Recording {
    pub(super) fn take(&self) -> Vec<PutPayload> {
        std::mem::take(&mut *self.uploads.lock().unwrap())
    }

    fn record(&self, payload: &PutPayload) {
        self.uploads.lock().unwrap().push(payload.clone());
    }
}

impl std::fmt::Display for Recording {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, formatter)
    }
}

#[async_trait]
impl ObjectStore for Recording {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.record(&payload);
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[async_trait]
impl MultipartStore for Recording {
    async fn create_multipart(&self, path: &Path) -> object_store::Result<MultipartId> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.inner.create_multipart(path).await
    }

    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        index: usize,
        payload: PutPayload,
    ) -> object_store::Result<PartId> {
        self.record(&payload);
        self.inner.put_part(path, id, index, payload).await
    }

    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> object_store::Result<PutResult> {
        self.inner.complete_multipart(path, id, parts).await
    }

    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> object_store::Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        self.inner.abort_multipart(path, id).await
    }
}

#[async_trait]
impl Signer for Recording {
    async fn signed_url(&self, _: Method, _: &Path, _: Duration) -> object_store::Result<Url> {
        Err(object_store::Error::NotSupported {
            source: std::io::Error::from(std::io::ErrorKind::Unsupported).into(),
        })
    }
}
