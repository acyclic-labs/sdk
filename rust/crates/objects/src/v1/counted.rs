//! Request accounting over the exact public Objects provider boundary.
//! Native arguments and results retain their original work and custody owners.
use super::*;
use acyclic_stream::testing::{Counted, RequestClass};

#[async_trait::async_trait]
impl<P: ObjectsProvider> ObjectsProvider for Counted<P> {
    async fn put_batch(
        &self,
        requests: Vec<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        self.record(RequestClass::Data);
        self.inner().put_batch(requests).await
    }
    async fn get_batch(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        self.record(RequestClass::Data);
        self.inner().get_batch(requests).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn get_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(wire::GetObjectRequest, u64)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<Object>>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().get_batch_native(_requests, _meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_batch_native(
        &self,
        _requests: ObjectsNativeRequest<Vec<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsBatch<ObjectsPutReceipt>>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().put_batch_native(_requests, _meter)
    }
    async fn create_bucket(
        &self,
        request: wire::CreateBucketRequest,
    ) -> Result<wire::Bucket, Error> {
        self.record(RequestClass::Data);
        self.inner().create_bucket(request).await
    }
    async fn head_bucket(&self, request: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        self.record(RequestClass::Head);
        self.inner().head_bucket(request).await
    }
    async fn delete_bucket(
        &self,
        request: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        self.record(RequestClass::Data);
        self.inner().delete_bucket(request).await
    }
    async fn put(
        &self,
        header: wire::PutObjectHeader,
        body: acyclic_stream::OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        self.record(RequestClass::Data);
        self.inner().put(header, body).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn put_native(
        &self,
        _request: ObjectsNativeRequest<(wire::PutObjectHeader, acyclic_stream::OwnedBytes)>,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, ObjectsPutReceipt>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().put_native(_request, _meter)
    }
    async fn get(
        &self,
        request: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        self.record(RequestClass::Data);
        self.inner().get(request, maximum_bytes).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn get_native(
        &self,
        _request: ObjectsNativeRequest<wire::GetObjectRequest>,
        _maximum_bytes: u64,
        _meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        self.record(RequestClass::Data);
        self.inner().get_native(_request, _maximum_bytes, _meter)
    }
    async fn head(
        &self,
        request: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        self.record(RequestClass::Head);
        self.inner().head(request).await
    }
    async fn delete(
        &self,
        request: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        self.record(RequestClass::Data);
        self.inner().delete(request).await
    }
    async fn restore(
        &self,
        _request: wire::RestoreObjectsRequest,
    ) -> Result<wire::RestoreObjectsResponse, Error> {
        self.record(RequestClass::Data);
        self.inner().restore(_request).await
    }
    async fn list(
        &self,
        request: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        self.record(RequestClass::List);
        self.inner().list(request).await
    }
    async fn create_multipart(
        &self,
        request: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        self.record(RequestClass::Data);
        self.inner().create_multipart(request).await
    }
    async fn upload_part(
        &self,
        header: wire::UploadPartHeader,
        body: acyclic_stream::OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.record(RequestClass::Data);
        self.inner().upload_part(header, body).await
    }
    async fn list_parts(
        &self,
        request: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        self.record(RequestClass::List);
        self.inner().list_parts(request).await
    }
    async fn complete_multipart(
        &self,
        request: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        self.record(RequestClass::Data);
        self.inner().complete_multipart(request).await
    }
    async fn abort_multipart(
        &self,
        request: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        self.record(RequestClass::Data);
        self.inner().abort_multipart(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn counted_objects_preserve_values_errors_and_budget_classes()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = Counted::new(MemoryObjects::new(Default::default())?);
        let bucket = provider
            .create_bucket(wire::CreateBucketRequest {
                name: "counted".into(),
                mutation: None,
            })
            .await?;
        let bucket_ref = bucket.bucket;
        acyclic_stream::assert_requests!(
            provider,
            provider
                .put(
                    wire::PutObjectHeader {
                        bucket: bucket_ref.clone(),
                        object_key: "value".into(),
                        ..Default::default()
                    },
                    acyclic_stream::OwnedBytes::from_static(b"payload")
                )
                .await,
            control = 0,
            data = 1
        )?;
        let object = acyclic_stream::assert_requests!(
            provider,
            provider
                .get(
                    wire::GetObjectRequest {
                        bucket: bucket_ref.clone(),
                        object_key: "value".into(),
                        ..Default::default()
                    },
                    7
                )
                .await,
            control = 0,
            data = 1
        )?;
        assert_eq!(object.body().as_ref(), b"payload");
        let missing = acyclic_stream::assert_requests!(
            provider,
            provider
                .get(
                    wire::GetObjectRequest {
                        bucket: bucket_ref.clone(),
                        object_key: "missing".into(),
                        ..Default::default()
                    },
                    7
                )
                .await,
            control = 0,
            data = 1
        );
        assert_eq!(missing.unwrap_err().code, wire::ErrorCode::NotFound);
        acyclic_stream::assert_requests!(
            provider,
            provider
                .head(wire::HeadObjectRequest {
                    bucket: bucket_ref,
                    object_key: "value".into(),
                    ..Default::default()
                })
                .await,
            control = 1,
            data = 0
        )?;
        assert_eq!(provider.requests().class(RequestClass::Head), 1);
        // Every logical operation, including multipart and conditional errors,
        // also passes through the same forwarding wrapper.
        super::super::conformance::verify(&provider, "counted-suite").await?;
        Ok(())
    }
}
