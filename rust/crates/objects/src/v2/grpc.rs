//! Authenticated, bounded gRPC client for logical Objects v2.
use super::response::object_info as info;
use super::{
    Download, Error, Object, ObjectsProvider, UploadBody, request, response, upload, wire,
};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream};
use prost::Message;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

const MESSAGE_BYTES: usize = 16 * 1024 * 1024;
fn put_frame(chunk: Result<Bytes, Error>) -> wire::PutObjectRequest {
    wire::PutObjectRequest {
        frame: chunk
            .ok()
            .map(|chunk| wire::put_object_request::Frame::Body(chunk.to_vec())),
    }
}
fn part_frame(chunk: Result<Bytes, Error>) -> wire::UploadPartRequest {
    wire::UploadPartRequest {
        frame: chunk
            .ok()
            .map(|chunk| wire::upload_part_request::Frame::Body(chunk.to_vec())),
    }
}

/// Client configuration or connection failure.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Endpoint, credential, or private CA violates the client contract.
    #[error("invalid Objects gRPC configuration")]
    InvalidConfiguration,
    /// Connection could not be established.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
}

/// Logical Objects client. Every request carries a sensitive bearer credential.
#[derive(Clone)]
pub struct GrpcObjects {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
}
impl GrpcObjects {
    /// Publishes a caller-owned stream; a source failure cancels the RPC before valid EOF.
    pub fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> impl std::future::Future<Output = Result<wire::ObjectInfo, Error>> + Send + 'static {
        Self::put_stream_owned(self.clone(), query, body)
    }
    /// Streams one staged multipart part without publishing the final object.
    pub fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> impl std::future::Future<Output = Result<wire::UploadedPart, Error>> + Send + 'static {
        Self::upload_part_stream_owned(self.clone(), query, body)
    }
    /// Publishes a caller-owned stream; a source failure cancels the RPC before valid EOF.
    async fn put_stream_owned(
        owner: Self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        request::put_stream_digest(&query)?;
        let original = query.encode_to_vec();
        let header = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Header(query)),
        };
        let (chunks, state, failure) = upload::chunks(body);
        // An invalid frame also prevents valid EOF if the transport polls a failing source.
        let frames = chunks.map(put_frame as fn(Result<Bytes, Error>) -> wire::PutObjectRequest);
        let mut client = owner.objects();
        let complete = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Complete(true)),
        };
        let input = owner.authenticated(
            stream::iter([header])
                .chain(frames)
                .chain(stream::iter([complete]))
                .boxed(),
        );
        let result = upload::run(
            client
                .put_object(input)
                .map(|reply| {
                    reply
                        .map(|reply| reply.into_inner())
                        .map_err(|status| error_from_status(&status))
                })
                .boxed(),
            failure,
        )
        .await?;
        response::validate_binary(
            "objects/put",
            &original,
            &result.encode_to_vec(),
            state.finish()?,
        )?;
        Ok(result)
    }
    /// Streams one staged multipart part without publishing the final object.
    async fn upload_part_stream_owned(
        owner: Self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> Result<wire::UploadedPart, Error> {
        request::upload_part_stream_digest(&query)?;
        let original = query.encode_to_vec();
        let header = wire::UploadPartRequest {
            frame: Some(wire::upload_part_request::Frame::Header(query)),
        };
        let (chunks, state, failure) = upload::chunks(body);
        let frames = chunks.map(part_frame as fn(Result<Bytes, Error>) -> wire::UploadPartRequest);
        let mut client = owner.multipart();
        let complete = wire::UploadPartRequest {
            frame: Some(wire::upload_part_request::Frame::Complete(true)),
        };
        let input = owner.authenticated(
            stream::iter([header])
                .chain(frames)
                .chain(stream::iter([complete]))
                .boxed(),
        );
        let result = upload::run(
            client
                .upload_part(input)
                .map(|reply| {
                    reply
                        .map(|reply| reply.into_inner())
                        .map_err(|status| error_from_status(&status))
                })
                .boxed(),
            failure,
        )
        .await?;
        response::validate_binary(
            "multipart/upload-part",
            &original,
            &result.encode_to_vec(),
            state.finish()?,
        )?;
        Ok(result)
    }
    /// Connects over authenticated TLS with an optional additional private CA.
    ///
    /// # Errors
    /// Rejects invalid credentials, non-HTTPS endpoints, oversized CA bundles, or failed connections.
    pub async fn connect(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_tls(endpoint, token, ca, None).await
    }
    /// Connects with a caller-supplied PEM client certificate chain and private key.
    ///
    /// Server certificate verification, bearer authentication, message bounds, and
    /// operation deadlines are the same as [`Self::connect`]. The server owns client
    /// identity authorization; presenting a certificate does not grant access.
    ///
    /// # Errors
    /// Rejects empty or oversized identity inputs, invalid configuration, malformed
    /// or mismatched certificate/key material, or failed connections.
    pub async fn connect_with_identity(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, ConnectError> {
        if certificate_pem.is_empty()
            || certificate_pem.len() > 64 * 1024
            || private_key_pem.is_empty()
            || private_key_pem.len() > 64 * 1024
        {
            return Err(ConnectError::InvalidConfiguration);
        }
        Self::connect_tls(
            endpoint,
            token,
            ca,
            Some(Identity::from_pem(certificate_pem, private_key_pem)),
        )
        .await
    }
    async fn connect_tls(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
        identity: Option<Identity>,
    ) -> Result<Self, ConnectError> {
        let endpoint = Endpoint::from_shared(endpoint.to_owned())?;
        if endpoint.uri().scheme_str() != Some("https")
            || endpoint
                .uri()
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
            || endpoint.uri().query().is_some()
            || endpoint.uri().path() != "/"
            || token.trim().is_empty()
            || token.len() > 8192
        {
            return Err(ConnectError::InvalidConfiguration);
        }
        let mut authorization: MetadataValue<Ascii> = format!("Bearer {token}")
            .parse()
            .map_err(|_| ConnectError::InvalidConfiguration)?;
        authorization.set_sensitive(true);
        let mut tls = ClientTlsConfig::new().with_webpki_roots();
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > 64 * 1024 {
                return Err(ConnectError::InvalidConfiguration);
            }
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let Some(identity) = identity {
            tls = tls.identity(identity);
        }
        let channel = endpoint
            .tls_config(tls)?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .connect()
            .await?;
        Ok(Self {
            channel,
            authorization,
        })
    }
    fn authenticated<T>(&self, value: T) -> Request<T> {
        let mut request = Request::new(value);
        request
            .metadata_mut()
            .insert("authorization", self.authorization.clone());
        request
    }
    fn buckets(&self) -> wire::buckets_service_client::BucketsServiceClient<Channel> {
        wire::buckets_service_client::BucketsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }
    fn objects(&self) -> wire::objects_service_client::ObjectsServiceClient<Channel> {
        wire::objects_service_client::ObjectsServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }
    fn multipart(&self) -> wire::multipart_service_client::MultipartServiceClient<Channel> {
        wire::multipart_service_client::MultipartServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }
    /// Starts a validated download without buffering the selected representation.
    /// Dropping the returned stream releases the underlying gRPC response.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        request::validate_binary("objects/get", &query.encode_to_vec(), 0)?;
        let range = query.range;
        let mut frames = self
            .objects()
            .get_object(self.authenticated(query))
            .await
            .map_err(|status| error_from_status(&status))?
            .into_inner();
        let first = frames
            .message()
            .await
            .map_err(|status| error_from_status(&status))?
            .ok_or_else(protocol)?;
        let header = match first.frame {
            Some(wire::get_object_response::Frame::Header(header)) => header,
            Some(wire::get_object_response::Frame::Error(detail)) => {
                return Err(frame_error(detail.code));
            }
            _ => return Err(protocol()),
        };
        let remaining = response::get_header(&header, &range, maximum_bytes)?;
        let body = stream::try_unfold((frames, remaining), |(mut frames, remaining)| async move {
            let Some(frame) = frames
                .message()
                .await
                .map_err(|status| error_from_status(&status))?
            else {
                return if remaining == 0 {
                    Ok(None)
                } else {
                    Err(protocol())
                };
            };
            match frame.frame {
                Some(wire::get_object_response::Frame::Body(bytes)) => {
                    let remaining = response::validate_get_body(bytes.len() as u64, remaining)?;
                    Ok(Some((Bytes::from(bytes), (frames, remaining))))
                }
                Some(wire::get_object_response::Frame::Error(detail)) => {
                    Err(frame_error(detail.code))
                }
                _ => Err(protocol()),
            }
        })
        .boxed();
        Ok(Download { header, body })
    }
}
fn frame_error(code: i32) -> Error {
    wire::ErrorCode::try_from(code)
        .ok()
        .filter(|code| *code != wire::ErrorCode::Unspecified)
        .map_or_else(protocol, Error::from)
}

/// Maps canonical service details, falling back to stable transport categories.
#[must_use]
pub fn error_from_status(status: &tonic::Status) -> Error {
    if let Ok(detail) = wire::ErrorDetail::decode(status.details())
        && let Ok(code) = wire::ErrorCode::try_from(detail.code)
        && code != wire::ErrorCode::Unspecified
    {
        return code.into();
    }
    match status.code() {
        tonic::Code::InvalidArgument => wire::ErrorCode::InvalidArgument,
        tonic::Code::NotFound => wire::ErrorCode::NotFound,
        tonic::Code::AlreadyExists => wire::ErrorCode::AlreadyExists,
        tonic::Code::FailedPrecondition => wire::ErrorCode::PreconditionFailed,
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            wire::ErrorCode::AccessDenied
        }
        tonic::Code::ResourceExhausted => wire::ErrorCode::QuotaExceeded,
        tonic::Code::Unimplemented => wire::ErrorCode::Unsupported,
        _ => wire::ErrorCode::Unavailable,
    }
    .into()
}
fn bounded<T: Message>(query: &T) -> Result<(), Error> {
    if query.encoded_len() > MESSAGE_BYTES {
        Err(wire::ErrorCode::QuotaExceeded.into())
    } else {
        Ok(())
    }
}
fn protocol() -> Error {
    wire::ErrorCode::Unavailable.into()
}

#[async_trait::async_trait]
impl ObjectsProvider for GrpcObjects {
    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        request::bucket_name(&query.name)?;
        request::identity(&query.mutation)?;
        bounded(&query)?;
        let expected = wire::BucketRef {
            name: query.name.clone(),
        };
        let result = self
            .buckets()
            .create_bucket(self.authenticated(query))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))?;
        response::bucket(&result, &expected)?;
        Ok(result)
    }
    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        request::bucket(&query.bucket)?;
        bounded(&query)?;
        let expected = query
            .bucket
            .clone()
            .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
        let result = self
            .buckets()
            .head_bucket(self.authenticated(query))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))?;
        response::bucket(&result, &expected)?;
        Ok(result)
    }
    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        request::bucket(&query.bucket)?;
        request::identity(&query.mutation)?;
        bounded(&query)?;
        self.buckets()
            .delete_bucket(self.authenticated(query))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))
    }
    async fn put(
        &self,
        query: wire::PutObjectHeader,
        body: Bytes,
    ) -> Result<wire::ObjectInfo, Error> {
        self.put_stream(query, stream::once(async { Ok(body) }).boxed())
            .await
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
    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        request::validate_binary("objects/head", &query.encode_to_vec(), 0)?;
        request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        bounded(&query)?;
        let response = self
            .objects()
            .head_object(self.authenticated(query))
            .await
            .map_err(|status| error_from_status(&status))?
            .into_inner();
        info(response.object.as_ref().ok_or_else(protocol)?)?;
        Ok(response)
    }
    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        bounded(&query)?;
        self.objects()
            .delete_object(self.authenticated(query))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))
    }
    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        request::validate_binary("objects/list", &query.encode_to_vec(), 0)?;
        request::page_size(query.page_size)?;
        bounded(&query)?;
        let response = self
            .objects()
            .list_objects(self.authenticated(query.clone()))
            .await
            .map_err(|status| error_from_status(&status))?
            .into_inner();
        response::listing(&query, &response)?;
        Ok(response)
    }
    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        request::metadata(&query.metadata)?;
        request::identity(&query.mutation)?;
        bounded(&query)?;
        self.multipart()
            .create_multipart(self.authenticated(query))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))
    }
    async fn upload_part(
        &self,
        query: wire::UploadPartHeader,
        body: Bytes,
    ) -> Result<wire::UploadedPart, Error> {
        self.upload_part_stream(query, stream::once(async { Ok(body) }).boxed())
            .await
    }
    async fn list_parts(
        &self,
        query: wire::ListPartsRequest,
    ) -> Result<wire::ListPartsResponse, Error> {
        request::validate_binary("multipart/list-parts", &query.encode_to_vec(), 0)?;
        request::page_size(query.page_size)?;
        bounded(&query)?;
        let response = self
            .multipart()
            .list_parts(self.authenticated(query.clone()))
            .await
            .map_err(|status| error_from_status(&status))?
            .into_inner();
        response::parts(&query, &response)?;
        Ok(response)
    }
    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        bounded(&query)?;
        let response = self
            .multipart()
            .complete_multipart(self.authenticated(query))
            .await
            .map_err(|status| error_from_status(&status))?
            .into_inner();
        info(&response)?;
        Ok(response)
    }
    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        request::identity(&query.mutation)?;
        bounded(&query)?;
        self.multipart()
            .abort_multipart(self.authenticated(query))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| error_from_status(&status))
    }
}
