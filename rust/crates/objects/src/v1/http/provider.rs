use super::*;

#[async_trait::async_trait]
impl ObjectsProvider for HttpObjects {
    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        request::create_bucket_digest(&query)?;
        self.call("buckets/create", query, |query, result| {
            response::bucket(&result, &wire::BucketRef { name: query.name })?;
            Ok(result)
        })
        .await
    }

    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        request::bucket(&query.bucket)?;
        self.call("buckets/head", query, |query, result| {
            response::bucket(
                &result,
                query
                    .bucket
                    .as_ref()
                    .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?,
            )?;
            Ok(result)
        })
        .await
    }

    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        request::delete_bucket_digest(&query)?;
        self.call("buckets/delete", query, |_, result| Ok(result))
            .await
    }

    async fn put(
        &self,
        query: wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        self.put_stream(query, stream::once(async { Ok(body) }).boxed().into())
            .await
    }

    fn put_native(
        &self,
        request: super::ObjectsNativeRequest<(wire::PutObjectHeader, OwnedBytes)>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<super::ObjectsNativeFuture<'_, super::ObjectsPutReceipt>, NativeObjectsError> {
        crate::v1::native_input_meter(&meter, &request)?;
        super::native_future(meter.clone(), async move {
            obs::traced(
                obs::span!(
                    INFO,
                    "acyclic.objects.http.call",
                    route = "objects/put",
                    rpc.code = obs::Empty
                ),
                async {
                    let errors = super::NativeTransportErrors::new(&meter)?;
                    let mut body = None;
                    request::put(&request.data().0)?;
                    let request = request.map(|(header, carrier)| {
                        body = Some(carrier);
                        wire::PutObjectRequest {
                            frame: Some(wire::put_object_request::Frame::Header(header)),
                        }
                    });
                    let header = line("PutObjectRequest", request.data(), Some(&errors))
                        .map_err(|error| errors.original(error))?;
                    drop(request);
                    let source = upload::chunks_native(
                        body.expect("native publication owns its canonical body"),
                        errors.clone(),
                    )?;
                    let (info, allocation) = self
                        .send_upload(header, source, Some(errors.clone()))
                        .await
                        .map_err(|error| errors.original(error))?;
                    Ok(super::ObjectsPutReceipt::new(info, allocation))
                },
            )
            .await
        })
    }

    async fn get(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        self.get_stream(query, maximum_bytes)
            .await?
            .collect(maximum_bytes)
            .await
    }

    fn get_native(
        &self,
        request: super::ObjectsNativeRequest<wire::GetObjectRequest>,
        maximum_bytes: u64,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<super::ObjectsNativeFuture<'_, Object>, NativeObjectsError> {
        crate::v1::native_input_meter(&meter, &request)?;
        super::native_future(meter.clone(), async move {
            let mut prepared = Download::prepare_native(maximum_bytes, meter.clone())?;
            let download = self
                .get_stream_controlled(
                    request.data(),
                    maximum_bytes,
                    Some(prepared.errors.clone()),
                    &mut prepared.header_allocation,
                )
                .await
                .map_err(|error| prepared.errors.original(error))?;
            download.collect_native(prepared).await
        })
    }

    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        request::head(&query)?;
        self.call(
            "objects/head",
            query,
            |_, result: wire::HeadObjectResponse| {
                response::object_info(result.object.as_ref().ok_or_else(response::invalid)?)?;
                Ok(result)
            },
        )
        .await
    }

    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        request::delete_digest(&query)?;
        self.call("objects/delete", query, |_, result| Ok(result))
            .await
    }

    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        request::list(&query)?;
        self.call("objects/list", query, |query, result| {
            response::listing(&query, &result)?;
            Ok(result)
        })
        .await
    }

    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        request::create_multipart_digest(&query)?;
        self.call(
            "multipart/create",
            query,
            |_, result: wire::MultipartUpload| {
                request::upload_id(&result.upload_id).map_err(|_| response::invalid())?;
                Ok(result)
            },
        )
        .await
    }

    async fn upload_part(
        &self,
        query: wire::UploadPartHeader,
        body: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.upload_part_stream(query, stream::once(async { Ok(body) }).boxed().into())
            .await
    }

    async fn list_parts(
        &self,
        query: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        request::list_parts(&query)?;
        self.call("multipart/list-parts", query, |query, result| {
            response::parts(&query, &result)?;
            Ok(result)
        })
        .await
    }

    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        request::complete_multipart_digest(&query)?;
        self.call("multipart/complete", query, |_, result| {
            response::object_info(&result)?;
            Ok(result)
        })
        .await
    }

    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        request::abort_multipart_digest(&query)?;
        self.call("multipart/abort", query, |_, result| Ok(result))
            .await
    }
}
