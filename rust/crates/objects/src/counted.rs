//! Request accounting over the exact public blob boundary. Native arguments
//! and results keep their original work and custody owners.
use crate::{
    Blobs, ByteRange, Collection, Delete, Digest, Error, Head, Manifest, Object, Page, Put,
    Retained, RootId, Usage,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    NativeObjectsError, ObjectsBatch, ObjectsNativeFuture, ObjectsNativeRequest, ObjectsPutReceipt,
};
use acyclic_stream::OwnedBytes;
use acyclic_stream::testing::{Counted, RequestClass};

#[async_trait::async_trait]
impl<P: Blobs> Blobs for Counted<P> {
    async fn put(&self, body: OwnedBytes, references: Vec<Digest>) -> Result<Put, Error> {
        self.record(RequestClass::Data);
        self.inner().put(body, references).await
    }
    async fn put_manifest(&self, manifest: Manifest) -> Result<Put, Error> {
        self.record(RequestClass::Data);
        self.inner().put_manifest(manifest).await
    }
    async fn get(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        self.record(RequestClass::Data);
        self.inner().get(digest, range, maximum_bytes).await
    }
    async fn head(&self, digest: Digest) -> Result<Head, Error> {
        self.record(RequestClass::Head);
        self.inner().head(digest).await
    }
    async fn delete(&self, digest: Digest) -> Result<Delete, Error> {
        self.record(RequestClass::Data);
        self.inner().delete(digest).await
    }
    async fn list(&self, after: Option<Digest>, limit: u32) -> Result<Page, Error> {
        self.record(RequestClass::List);
        self.inner().list(after, limit).await
    }
    async fn retain(&self, root: &RootId, references: Vec<Digest>) -> Result<Retained, Error> {
        self.record(RequestClass::Data);
        self.inner().retain(root, references).await
    }
    async fn release(&self, root: &RootId) -> Result<bool, Error> {
        self.record(RequestClass::Data);
        self.inner().release(root).await
    }
    async fn collect(&self) -> Result<Collection, Error> {
        self.record(RequestClass::Data);
        self.inner().collect().await
    }
    async fn usage(&self) -> Result<Usage, Error> {
        self.record(RequestClass::Metadata);
        self.inner().usage().await
    }
    async fn put_batch(&self, requests: Vec<(OwnedBytes, Vec<Digest>)>) -> Vec<Result<Put, Error>> {
        self.record(RequestClass::Data);
        self.inner().put_batch(requests).await
    }
    async fn get_batch(
        &self,
        requests: Vec<(Digest, ByteRange, u64)>,
    ) -> Vec<Result<Object, Error>> {
        self.record(RequestClass::Data);
        self.inner().get_batch(requests).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        request: ObjectsNativeRequest<(Digest, ByteRange, u64)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().get_native(request, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        requests: ObjectsNativeRequest<Vec<(Digest, ByteRange, u64)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().get_batch_native(requests, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        request: ObjectsNativeRequest<(OwnedBytes, Vec<Digest>)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().put_native(request, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        requests: ObjectsNativeRequest<Vec<(OwnedBytes, Vec<Digest>)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().put_batch_native(requests, meter)
    }
}
