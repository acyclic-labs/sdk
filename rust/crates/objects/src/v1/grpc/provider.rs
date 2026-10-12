use super::*;

#[async_trait::async_trait]
impl ObjectsProvider for GrpcObjects {
    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.create_bucket",
                rpc.code = obs::Empty
            ),
            async {
                request::bucket_name(&query.name)?;
                request::identity(&query.mutation)?;
                bounded(&query)?;
                let expected = wire::BucketRef {
                    name: query.name.clone(),
                };
                let result = self
                    .buckets()
                    .create_bucket(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))?;
                response::bucket(&result, &expected)?;
                Ok(result)
            },
        )
        .await
    }

    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.head_bucket",
                rpc.code = obs::Empty
            ),
            async {
                request::bucket(&query.bucket)?;
                bounded(&query)?;
                let expected = query
                    .bucket
                    .clone()
                    .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
                let result = self
                    .buckets()
                    .head_bucket(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))?;
                response::bucket(&result, &expected)?;
                Ok(result)
            },
        )
        .await
    }

    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.delete_bucket",
                rpc.code = obs::Empty
            ),
            async {
                request::bucket(&query.bucket)?;
                request::identity(&query.mutation)?;
                bounded(&query)?;
                self.buckets()
                    .delete_bucket(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))
            },
        )
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
        let request_input = response::RequestInput::native(request, &meter)?;
        super::native_future(meter.clone(), async move {
            let errors = super::NativeTransportErrors::new(&meter)?;
            errors.prepare_codec_failure()?;
            let controls = errors.clone();
            let (header, carrier) = request_input.value();
            let wire_bytes = native_put_wire_bytes(header, carrier.len())?;
            let owners = carrier
                .len()
                .div_ceil(65_536)
                .checked_add(2)
                .ok_or_else(response::invalid)?;
            let wire = PreparedWireBody::new(controls.clone(), wire_bytes, owners)
                .map_err(|error| errors.original(error))?;
            let mut body = None;
            let request_input = request_input.map(|(header, carrier)| {
                body = Some(carrier);
                header
            });
            let source = upload::chunks_native(
                body.expect("native publication owns its canonical body"),
                errors.clone(),
            )?;
            let (info, allocation) = Self::put_stream_controlled(
                self.clone(),
                request_input,
                source,
                Some(controls),
                Some(wire),
            )
            .await
            .map_err(|error| errors.original(error))?;
            Ok(super::ObjectsPutReceipt::new(info, allocation))
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
        let request_input = response::RequestInput::native(request, &meter)?;
        super::native_future(meter.clone(), async move {
            let mut prepared = Download::prepare_native(maximum_bytes, meter.clone())?;
            let errors = prepared.errors.clone();
            errors.prepare_codec_failure()?;
            let controls = errors.clone();
            let mut header_allocation = Some(acyclic_native_runtime::ReadAllocation::adopt(
                &meter,
                MESSAGE_BYTES * 2,
            )?);
            let wire_bytes = request_input
                .value()
                .encoded_len()
                .checked_add(5)
                .ok_or_else(response::invalid)?;
            let wire = PreparedWireBody::new(controls.clone(), wire_bytes, 1)
                .map_err(|error| prepared.errors.original(error))?;
            let download = self
                .get_stream_controlled(
                    request_input,
                    maximum_bytes,
                    Some(controls),
                    &mut header_allocation,
                    Some(wire),
                )
                .await
                .map_err(|error| prepared.errors.original(error))?;
            if let Some(allocation) = header_allocation {
                prepared.header_allocation = Some(allocation);
            }
            download.collect_native(prepared).await
        })
    }

    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.grpc.head", rpc.code = obs::Empty),
            async {
                request::head(&query)?;
                let response = self
                    .objects()
                    .head_object(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))?;
                info(response.object.as_ref().ok_or_else(protocol)?)?;
                Ok(response)
            },
        )
        .await
    }

    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.grpc.delete", rpc.code = obs::Empty),
            async {
                bounded(&query)?;
                self.objects()
                    .delete_object(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))
            },
        )
        .await
    }

    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.grpc.list", rpc.code = obs::Empty),
            async {
                request::list(&query)?;
                let response = self
                    .objects()
                    .list_objects(self.authenticated(query.clone(), REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))?;
                response::listing(&query, &response)?;
                Ok(response)
            },
        )
        .await
    }

    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.create_multipart",
                rpc.code = obs::Empty
            ),
            async {
                request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                request::metadata(&query.metadata)?;
                request::identity(&query.mutation)?;
                bounded(&query)?;
                self.multipart()
                    .create_multipart(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))
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
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.list_parts",
                rpc.code = obs::Empty
            ),
            async {
                request::list_parts(&query)?;
                let response = self
                    .multipart()
                    .list_parts(self.authenticated(query.clone(), REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))?;
                response::parts(&query, &response)?;
                Ok(response)
            },
        )
        .await
    }

    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.complete_multipart",
                rpc.code = obs::Empty
            ),
            async {
                bounded(&query)?;
                let response = self
                    .multipart()
                    .complete_multipart(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))?;
                info(&response)?;
                Ok(response)
            },
        )
        .await
    }

    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.abort_multipart",
                rpc.code = obs::Empty
            ),
            async {
                request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                request::identity(&query.mutation)?;
                bounded(&query)?;
                self.multipart()
                    .abort_multipart(self.authenticated(query, REQUEST_TIMEOUT))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, None))
            },
        )
        .await
    }
}
