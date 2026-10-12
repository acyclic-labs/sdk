use super::*;

macro_rules! unary {
    ($( $method:ident($input:ident) -> $output:ident, $client:ident.$rpc:ident,
        |$query:ident, $result:ident| {$($prepare:tt)*}, $validate:block; )*
        { $($stream:tt)* }) => {
        #[async_trait::async_trait]
        impl ObjectsProvider for GrpcObjects {
            $(async fn $method(&self, $query: wire::$input) -> Result<wire::$output, Error> {
                obs::traced(
                    obs::span!(INFO, concat!("acyclic.objects.grpc.", stringify!($method)), rpc.code = obs::Empty),
                    async {
                        $($prepare)*
                        let $result = self.$client()
                            .$rpc(self.authenticated($query, REQUEST_TIMEOUT))
                            .await.map(ok).map_err(|status| error_from_status(&status, None))?;
                        $validate;
                        Ok($result)
                    },
                ).await
            })*
            $($stream)*
        }
    };
}

unary! {
create_bucket(CreateBucketRequest) -> Bucket, buckets.create_bucket,
    |query, result| {
        request::bucket_name(&query.name)?;
        request::identity(&query.mutation)?;
        bounded(&query)?;
        let expected = wire::BucketRef { name: query.name.clone() };
    }, {
        response::bucket(&result, &expected)?;
    };
head_bucket(HeadBucketRequest) -> Bucket, buckets.head_bucket,
    |query, result| {
        request::bucket(&query.bucket)?;
        bounded(&query)?;
        let expected = query.bucket.clone().ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
    }, {
        response::bucket(&result, &expected)?;
    };
delete_bucket(DeleteBucketRequest) -> DeleteBucketResponse, buckets.delete_bucket,
        |query, result| {
            request::bucket(&query.bucket)?;
            request::identity(&query.mutation)?;
            bounded(&query)?;
        }, {};
head(HeadObjectRequest) -> HeadObjectResponse, objects.head_object,
    |query, result| {
        request::head(&query)?;
    }, {
        info(result.object.as_ref().ok_or_else(protocol)?)?;
    };
delete(DeleteObjectRequest) -> DeleteObjectResponse, objects.delete_object,
        |query, result| {
            bounded(&query)?;
        }, {};
list(ListObjectsRequest) -> ListObjectsResponse, objects.list_objects,
    |query, result| {
        request::list(&query)?;
        let expected = query.clone();
    }, {
        response::listing(&expected, &result)?;
    };
create_multipart(CreateMultipartRequest) -> MultipartUpload, multipart.create_multipart,
        |query, result| {
            request::bucket(&query.bucket)?;
            request::key(&query.object_key)?;
            request::metadata(&query.metadata)?;
            request::identity(&query.mutation)?;
            bounded(&query)?;
        }, {};
list_parts(ListPartsRequest) -> ListPartsResponse, multipart.list_parts,
    |query, result| {
        request::list_parts(&query)?;
        let expected = query.clone();
    }, {
        response::parts(&expected, &result)?;
    };
complete_multipart(CompleteMultipartRequest) -> ObjectInfo, multipart.complete_multipart,
    |query, result| {
        bounded(&query)?;
    }, {
        info(&result)?;
    };
abort_multipart(AbortMultipartRequest) -> AbortMultipartResponse, multipart.abort_multipart,
        |query, result| {
            bounded(&query)?;
        }, {};
{
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






    async fn upload_part(
        &self,
        query: wire::UploadPartHeader,
        body: OwnedBytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.upload_part_stream(query, stream::once(async { Ok(body) }).boxed().into())
            .await
    }
}
}
