//! Authenticated, bounded gRPC client for logical Objects v1.
use super::response::object_info as info;
use super::{
    Download, Error, Object, ObjectsProvider, UploadBody, request, response, upload, wire,
};
use crate::obs;
use acyclic_stream::OwnedBytes;
use futures::{FutureExt, StreamExt, stream};
use prost::Message;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

const MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_TRANSFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1800);

fn put_frame(chunk: Result<OwnedBytes, Error>) -> wire::PutObjectRequest {
    wire::PutObjectRequest {
        frame: chunk
            .ok()
            .map(|chunk| wire::put_object_request::Frame::Body(chunk.to_vec())),
    }
}
fn part_frame(chunk: Result<OwnedBytes, Error>) -> wire::UploadPartRequest {
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
    /// The platform certificate verifier could not be initialized.
    #[error("Objects platform certificate verifier: {0}")]
    PlatformVerifier(#[source] rustls::Error),
}

/// Logical Objects client. Every request carries a sensitive bearer credential.
#[derive(Clone)]
pub struct GrpcObjects {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
    transfer_timeout: std::time::Duration,
}
impl GrpcObjects {
    /// Sets the finite deadline for streamed PUT, multipart parts and complete GET bodies.
    pub fn with_transfer_timeout(
        mut self,
        timeout: std::time::Duration,
    ) -> Result<Self, ConnectError> {
        if timeout.is_zero() || timeout > MAX_TRANSFER_TIMEOUT {
            return Err(ConnectError::InvalidConfiguration);
        }
        self.transfer_timeout = timeout;
        Ok(self)
    }

    /// Connects with optional private CA trust and PEM client certificate/key identity.
    pub async fn connect(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
        identity: Option<(&[u8], &[u8])>,
    ) -> Result<Self, ConnectError> {
        let identity = identity
            .map(|(certificate, key)| {
                if certificate.is_empty()
                    || certificate.len() > super::MAX_PEM_BYTES
                    || key.is_empty()
                    || key.len() > super::MAX_PEM_BYTES
                {
                    return Err(ConnectError::InvalidConfiguration);
                }
                Ok(Identity::from_pem(certificate, key))
            })
            .transpose()?;
        let endpoint = Endpoint::from_shared(endpoint.to_owned())?;
        if endpoint.uri().scheme_str() != Some("https")
            || endpoint
                .uri()
                .authority()
                .is_none_or(|authority| authority.as_str().contains('@'))
            || endpoint.uri().query().is_some()
            || endpoint.uri().path() != "/"
            || token.trim().is_empty()
            || token.len() > super::MAX_BEARER_TOKEN_BYTES
        {
            return Err(ConnectError::InvalidConfiguration);
        }
        let mut authorization: MetadataValue<Ascii> = format!("Bearer {token}")
            .parse()
            .map_err(|_| ConnectError::InvalidConfiguration)?;
        authorization.set_sensitive(true);
        let mut tls = ClientTlsConfig::new();
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > super::MAX_PEM_BYTES {
                return Err(ConnectError::InvalidConfiguration);
            }
            tls = tls.ca_certificate(Certificate::from_pem(ca));
        }
        if let Some(identity) = identity {
            tls = tls.identity(identity);
        }
        let endpoint = if ca.is_some() {
            endpoint.tls_config(tls)?
        } else {
            let verifier = rustls_platform_verifier::Verifier::new(std::sync::Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .map_err(ConnectError::PlatformVerifier)?;
            endpoint.tls_config_with_verifier(tls, std::sync::Arc::new(verifier))?
        };
        let channel = endpoint
            .connect_timeout(std::time::Duration::from_secs(10))
            // Requests carry their own deadlines; this ceiling admits the largest transfer budget.
            .timeout(MAX_TRANSFER_TIMEOUT)
            .connect()
            .await?;
        Ok(Self {
            channel,
            authorization,
            transfer_timeout: REQUEST_TIMEOUT,
        })
    }

    fn authenticated<T>(&self, value: T, timeout: std::time::Duration) -> Request<T> {
        let mut request = Request::new(value);
        request.set_timeout(timeout);
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

    /// The generated client is unary-only, so object body RPCs are routed here.
    async fn streaming(
        &self,
        path: &'static str,
    ) -> Result<
        (
            tonic::client::Grpc<Channel>,
            tonic::codegen::http::uri::PathAndQuery,
        ),
        Error,
    > {
        let mut client = tonic::client::Grpc::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES);
        client.ready().await.map_err(|_| {
            obs::record!("rpc.code" = tonic::Code::Unknown as u64);
            protocol()
        })?;
        Ok((
            client,
            tonic::codegen::http::uri::PathAndQuery::from_static(path),
        ))
    }

    fn multipart(&self) -> wire::multipart_service_client::MultipartServiceClient<Channel> {
        wire::multipart_service_client::MultipartServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }

    /// Publishes a caller-owned stream; a source failure cancels the RPC before valid EOF.
    pub fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> impl Future<Output = Result<wire::ObjectInfo, Error>> + Send + 'static {
        let owner = self.clone();
        async move {
            obs::traced(
                obs::span!(INFO, "acyclic.objects.grpc.put", rpc.code = obs::Empty),
                async {
                    request::put(&query)?;
                    let header = wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Header(query)),
                    };
                    let (chunks, state) = upload::chunks(body);
                    // An invalid frame also prevents valid EOF if the transport polls a failing source.
                    let frames = chunks
                        .map(put_frame as fn(Result<OwnedBytes, Error>) -> wire::PutObjectRequest);
                    let complete = wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Complete(true)),
                    };
                    let mut input = owner.authenticated(
                        stream::iter([header])
                            .chain(frames)
                            .chain(stream::iter([complete])),
                        owner.transfer_timeout,
                    );
                    input.extensions_mut().insert(tonic::GrpcMethod::new(
                        "acyclic.objects.v1.ObjectsService",
                        "PutObject",
                    ));
                    let result = upload::run(
                        async {
                            let (mut client, path) = owner
                                .streaming("/acyclic.objects.v1.ObjectsService/PutObject")
                                .await?;
                            client
                                .client_streaming(input, path, tonic_prost::ProstCodec::default())
                                .await
                                .map(ok)
                                .map_err(|status| error_from_status(&status))
                        }
                        .boxed(),
                        &state,
                    )
                    .await?;
                    info(&result)?;
                    if result.size != state.finish()? {
                        return Err(response::invalid());
                    }
                    Ok(result)
                },
            )
            .await
        }
    }

    /// Streams one staged multipart part without publishing the final object.
    pub fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> impl Future<Output = Result<wire::UploadedPart, Error>> + Send + 'static {
        let owner = self.clone();
        async move {
            obs::traced(
                obs::span!(
                    INFO,
                    "acyclic.objects.grpc.upload_part",
                    rpc.code = obs::Empty
                ),
                async {
                    request::upload_part(&query)?;
                    let part_number = query.part_number;
                    let header = wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Header(query)),
                    };
                    let (chunks, state) = upload::chunks(body);
                    let frames = chunks.map(
                        part_frame as fn(Result<OwnedBytes, Error>) -> wire::UploadPartRequest,
                    );
                    let complete = wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Complete(true)),
                    };
                    let input = owner.authenticated(
                        stream::iter([header])
                            .chain(frames)
                            .chain(stream::iter([complete]))
                            .boxed(),
                        owner.transfer_timeout,
                    );
                    let result = upload::run(
                        owner
                            .multipart()
                            .upload_part(input)
                            .map(|reply| reply.map(ok).map_err(|status| error_from_status(&status)))
                            .boxed(),
                        &state,
                    )
                    .await?;
                    response::part(&result, part_number, state.finish()?)?;
                    Ok(result)
                },
            )
            .await
        }
    }

    /// Starts a validated download without buffering the selected representation.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        obs::download(
            obs::span!(INFO, "acyclic.objects.grpc.get", rpc.code = obs::Empty),
            async {
                request::get(&query)?;
                let range = query.range;
                let deadline = tokio::time::Instant::now() + self.transfer_timeout;
                let mut request = self.authenticated(query, self.transfer_timeout);
                request.extensions_mut().insert(tonic::GrpcMethod::new(
                    "acyclic.objects.v1.ObjectsService",
                    "GetObject",
                ));
                let (mut client, path) = self
                    .streaming("/acyclic.objects.v1.ObjectsService/GetObject")
                    .await?;
                let mut frames: tonic::Streaming<wire::GetObjectResponse> = client
                    .server_streaming(request, path, tonic_prost::ProstCodec::default())
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
                let first = tokio::time::timeout_at(deadline, frames.message())
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
                    .map_err(|status| error_from_status(&status))?
                    .ok_or_else(protocol)?;
                let header = match first.frame {
                    Some(wire::get_object_response::Frame::Header(header)) => header,
                    frame => return Err(response::reject_frame(frame)),
                };
                let remaining = response::get_header(&header, &range, maximum_bytes)?;
                let body = stream::try_unfold(
                    (frames, remaining),
                    move |(mut frames, remaining)| async move {
                        let Some(frame) = tokio::time::timeout_at(deadline, frames.message())
                            .await
                            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
                            .map_err(|status| error_from_status(&status))?
                        else {
                            return if remaining == 0 {
                                Ok(None)
                            } else {
                                Err(protocol())
                            };
                        };
                        match frame.frame {
                            Some(wire::get_object_response::Frame::Body(frame)) => {
                                let bytes = response::body(frame, remaining)?;
                                let remaining = remaining - bytes.len() as u64;
                                Ok(Some((bytes::Bytes::from(bytes), (frames, remaining))))
                            }
                            frame => Err(response::reject_frame(frame)),
                        }
                    },
                )
                .boxed();
                Ok(Download {
                    header,
                    body: body.into(),
                })
            },
        )
        .await
    }
}

/// Unwraps a successful reply, recording its gRPC code on the current span.
fn ok<T>(reply: tonic::Response<T>) -> T {
    obs::record!("rpc.code" = tonic::Code::Ok as u64);
    reply.into_inner()
}

/// Maps canonical service details, falling back to stable transport categories.
fn error_from_status(status: &tonic::Status) -> Error {
    obs::record!("rpc.code" = status.code() as u64);
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
                            .await.map(ok).map_err(|status| error_from_status(&status))?;
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
