//! Authenticated, bounded gRPC client for logical Objects v1.
use super::response::object_info as info;
use super::{
    Download, Error, NativeObjectsError, Object, ObjectsProvider, UploadBody,
    request, response, upload, wire,
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

struct NativeWireChunk {
    data: bytes::Bytes,
    _wire: acyclic_native_runtime::ReadAllocation,
    _owner: acyclic_native_runtime::ReadAllocation,
}
impl AsRef<[u8]> for NativeWireChunk {
    fn as_ref(&self) -> &[u8] { self.data.as_ref() }
}
pin_project_lite::pin_project! {
    struct WireBodySlot {
        #[pin]
        source: Option<tonic::body::Body>,
        controls: response::NativeGetControls,
        owners: (Vec<acyclic_native_runtime::ReadAllocationReservation>, acyclic_native_runtime::ReadAllocation),
        failed: bool,
    }
}
impl http_body::Body for WireBodySlot {
    type Data = bytes::Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        let mut this = self.project();
        if *this.failed { return Poll::Ready(None); }
        if this.controls.check().is_err() {
            *this.failed = true;
            return Poll::Ready(Some(Err(this.controls.errors.codec_failure()
                .expect("native codec failure is prepared before RPC"))));
        }
        let frame = match this.source.as_mut().as_pin_mut()
            .expect("only an initialized transport slot is published").poll_frame(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => {
                *this.failed = true;
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(Some(Ok(frame))) => frame,
        };
        let data = match frame.into_data() {
            Ok(data) => data,
            Err(frame) => return Poll::Ready(Some(Ok(frame))),
        };
        let wire = match this.controls.errors.take_wire(data.len()) {
            Ok(allocation) => allocation,
            Err(error) => {
                this.controls.errors.record(error);
                *this.failed = true;
                return Poll::Ready(Some(Err(this.controls.errors.codec_failure()
                    .expect("native codec failure is prepared before RPC"))));
            }
        };
        let Some(credit) = this.owners.0.pop() else {
            this.controls.errors.record(std::io::Error::from(std::io::ErrorKind::InvalidData).into());
            *this.failed = true;
            return Poll::Ready(Some(Err(this.controls.errors.codec_failure()
                .expect("native codec failure is prepared before RPC"))));
        };
        let owner = match credit.allocate() {
            Ok(allocation) => allocation,
            Err(error) => {
                this.controls.errors.record(error.into());
                *this.failed = true;
                return Poll::Ready(Some(Err(this.controls.errors.codec_failure()
                    .expect("native codec failure is prepared before RPC"))));
            }
        };
        // Ready frames retain their original claims through every alias.
        let data = bytes::Bytes::from_owner(NativeWireChunk { data, _wire: wire, _owner: owner });
        Poll::Ready(Some(Ok(http_body::Frame::data(data))))
    }
    fn is_end_stream(&self) -> bool {
        self.failed || self.source.as_ref().is_some_and(http_body::Body::is_end_stream)
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.source.as_ref().expect("only an initialized transport slot is published").size_hint()
    }
}
struct CustodiedWireBody {
    source: std::pin::Pin<Box<WireBodySlot>>,
    _allocation: acyclic_native_runtime::ReadAllocation,
    _erasure: acyclic_native_runtime::ReadAllocation,
}
impl http_body::Body for CustodiedWireBody {
    type Data = bytes::Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.source.as_mut().poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool { self.source.is_end_stream() }
    fn size_hint(&self) -> http_body::SizeHint { self.source.size_hint() }
}
struct PreparedWireBody {
    source: std::pin::Pin<Box<WireBodySlot>>,
    erasure: acyclic_native_runtime::ReadAllocationReservation,
    _allocation: acyclic_native_runtime::ReadAllocation,
}
impl PreparedWireBody {
    fn new(
        controls: response::NativeGetControls,
        wire_bytes: usize,
        owner_count: usize,
    ) -> Result<Self, Error> {
        use acyclic_native_runtime::{ReadAllocation, ReadAllocationReservation};
        controls.check()?;
        let bytes = owner_count.checked_mul(std::mem::size_of::<ReadAllocationReservation>())
            .ok_or_else(response::invalid)?;
        let mut credits = (Vec::new(), ReadAllocation::new(&controls.meter, bytes)
            .map_err(|error| controls.fail(error))?);
        credits.0.try_reserve_exact(owner_count)
            .map_err(|error| controls.fail(controls.meter.allocation_failed(error)))?;
        let capacity = credits.0.capacity().checked_mul(std::mem::size_of::<ReadAllocationReservation>())
            .ok_or_else(response::invalid)?;
        credits.1.reconcile_capacity(capacity).map_err(|error| controls.fail(error))?;
        for _ in 0..owner_count {
            credits.0.push(ReadAllocationReservation::new(
                &controls.meter, std::mem::size_of::<NativeWireChunk>() as u64,
            ).map_err(|error| controls.fail(error))?);
        }
        let wire = ReadAllocation::adopt(&controls.meter, wire_bytes)
            .map_err(|error| controls.fail(error))?;
        controls.errors.retain_wire(wire).map_err(|error| controls.fail(error))?;
        let erasure = ReadAllocationReservation::new(
            &controls.meter, std::mem::size_of::<CustodiedWireBody>() as u64,
        ).map_err(|error| controls.fail(error))?;
        let allocation = ReadAllocation::new(&controls.meter, std::mem::size_of::<WireBodySlot>())
            .map_err(|error| controls.fail(error))?;
        Ok(Self {
            source: Box::pin(WireBodySlot {
                source: None, controls,
                owners: credits,
                failed: false,
            }),
            erasure, _allocation: allocation,
        })
    }
    fn initialize(mut self, source: tonic::body::Body) -> Result<tonic::body::Body, NativeObjectsError> {
        self.source.as_mut().project().source.set(Some(source));
        let erasure = self.erasure.allocate()?;
        let body = CustodiedWireBody {
            source: self.source, _allocation: self._allocation, _erasure: erasure,
        };
        // UnsyncBoxBody downcasts directly in tonic Body::new.
        Ok(tonic::body::Body::new(http_body_util::combinators::UnsyncBoxBody::new(body)))
    }
}
struct ObjectsChannel {
    channel: Channel,
    wire: Option<PreparedWireBody>,
}
#[derive(Debug, thiserror::Error)]
enum ObjectsChannelError {
    #[error("{0}")]
    Transport(#[source] tonic::transport::Error),
    #[error("{0}")]
    Native(#[source] tonic::Status),
}
pin_project_lite::pin_project! {
    struct ObjectsChannelFuture {
        #[pin]
        active: Option<<Channel as tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>>::Future>,
        failure: Option<ObjectsChannelError>,
    }
}
impl Future for ObjectsChannelFuture {
    type Output = Result<tonic::codegen::http::Response<tonic::body::Body>, ObjectsChannelError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        if let Some(error) = this.failure.take() { return std::task::Poll::Ready(Err(error)); }
        this.active.as_mut().as_pin_mut().expect("an accepted channel future is present")
            .poll(cx).map_err(ObjectsChannelError::Transport)
    }
}
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for ObjectsChannel {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = ObjectsChannelError;
    type Future = ObjectsChannelFuture;
    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        tonic::codegen::Service::poll_ready(&mut self.channel, cx).map_err(ObjectsChannelError::Transport)
    }
    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        let request = if let Some(wire) = self.wire.take() {
            let controls = wire.source.controls.clone();
            let (parts, source) = request.into_parts();
            match wire.initialize(source) {
                Ok(source) => tonic::codegen::http::Request::from_parts(parts, source),
                Err(error) => {
                    controls.errors.record(error);
                    return ObjectsChannelFuture { active: None, failure: Some(ObjectsChannelError::Native(
                        controls.errors.codec_failure().expect("native codec failure is prepared before RPC"),
                    )) };
                }
            }
        } else { request };
        ObjectsChannelFuture {
            active: Some(tonic::codegen::Service::call(&mut self.channel, request)),
            failure: None,
        }
    }
}

fn native_put_wire_bytes(header: &wire::PutObjectHeader, length: usize) -> Result<usize, Error> {
    let header_length = header.encoded_len();
    let full = length / 65_536;
    let remainder = length % 65_536;
    let mut bytes = header_length.checked_add(prost::encoding::encoded_len_varint(header_length as u64))
        .and_then(|value| value.checked_add(6))
        .and_then(|value| full.checked_mul(65_545).and_then(|body| value.checked_add(body)))
        .ok_or_else(response::invalid)?;
    if remainder != 0 {
        bytes = bytes.checked_add(remainder)
            .and_then(|value| value.checked_add(prost::encoding::encoded_len_varint(remainder as u64)))
            .and_then(|value| value.checked_add(6)).ok_or_else(response::invalid)?;
    }
    bytes.checked_add(7).ok_or_else(response::invalid)
}

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

fn put_frames(
    chunks: UploadBody,
    controls: Option<response::NativeGetControls>,
    header: response::RequestInput<wire::PutObjectRequest>,
) -> impl futures::Stream<Item = response::RequestInput<wire::PutObjectRequest>> + Send {
    stream::unfold((Some(header), chunks, controls, false), |(mut first, mut chunks, controls, failed)| async move {
        if failed { return None; }
        if let Some(frame) = first.take() {
            return Some((frame, (None, chunks, controls, false)));
        }
        let chunk = chunks.next().await?;
        let (frame, failed) = if let Some(controls) = &controls {
            let frame = (|| {
                let chunk = chunk?;
                let (bytes, allocation) = controls.copy(chunk.as_ref())?;
                let packet = super::ObjectsNativeRequest::new(wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Body(bytes)),
                }, allocation, &controls.meter).map_err(|error| controls.fail(error))?;
                Ok::<_, Error>(response::RequestInput::native(packet, &controls.meter)
                    .map_err(|error| controls.fail(error))?.with_controls(Some(controls.clone()))?)
            })();
            match frame {
                Ok(frame) => (frame, false),
                Err(_) => (response::RequestInput::CodecFailure(
                    controls.errors.codec_failure().expect("one prepared native source failure"),
                ), true),
            }
        } else { (response::RequestInput::ordinary(put_frame(chunk)), false) };
        Some((frame, (None, chunks, controls, failed)))
    })
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
    /// Publishes a caller-owned stream; a source failure cancels the RPC before valid EOF.
    pub fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> impl Future<Output = Result<wire::ObjectInfo, Error>> + Send + 'static {
        Self::put_stream_owned(self.clone(), query, body)
    }
    /// Streams one staged multipart part without publishing the final object.
    pub fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> impl Future<Output = Result<wire::UploadedPart, Error>> + Send + 'static {
        Self::upload_part_stream_owned(self.clone(), query, body)
    }
    /// Publishes a caller-owned stream; a source failure cancels the RPC before valid EOF.
    async fn put_stream_owned(
        owner: Self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        Ok(Self::put_stream_controlled(owner, response::RequestInput::ordinary(query), body, None, None).await?.0)
    }
    async fn put_stream_controlled(
        owner: Self,
        request_input: response::RequestInput<wire::PutObjectHeader>,
        body: UploadBody,
        controls: Option<response::NativeGetControls>,
        wire: Option<PreparedWireBody>,
    ) -> Result<(wire::ObjectInfo, Option<acyclic_native_runtime::ReadAllocation>), Error> {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.grpc.put", rpc.code = obs::Empty),
            async {
                let query = request_input.value();
                let response_allocation = controls.as_ref().map(|controls| {
                    acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, MESSAGE_BYTES * 4)
                        .map_err(|error| controls.fail(error))
                }).transpose()?;
                let validation_allocation = controls.as_ref().map(|controls| controls.request(query)).transpose()?;
                request::put_stream_digest(query)?;
                drop(validation_allocation);
                let header = request_input.map(|query| wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Header(query)),
                }).with_controls(controls.clone())?;
                let (chunks, state) = upload::chunks(body, controls.clone())?;
                let frames = put_frames(chunks, controls.clone(), header);
                let mut client = tonic::client::Grpc::new(ObjectsChannel { channel: owner.channel.clone(), wire })
                    .max_decoding_message_size(MESSAGE_BYTES)
                    .max_encoding_message_size(MESSAGE_BYTES);
                client.ready().await.map_err(|_| {
                    obs::record!("rpc.code" = tonic::Code::Unknown as u64);
                    response::invalid()
                })?;
                let complete = wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Complete(true)),
                };
                let complete = if let Some(controls) = &controls {
                    let packet = super::ObjectsNativeRequest::new(complete, (), &controls.meter)
                        .map_err(|error| controls.fail(error))?;
                    response::RequestInput::native(packet, &controls.meter)
                        .map_err(|error| controls.fail(error))?.with_controls(Some(controls.clone()))?
                } else { response::RequestInput::ordinary(complete) };
                let mut input = owner.authenticated_transfer(
                    frames.chain(stream::iter([complete])),
                );
                input.extensions_mut().insert(tonic::GrpcMethod::new(
                    "acyclic.objects.v1.ObjectsService", "PutObject",
                ));
                let invoke = || client.client_streaming(
                    input,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/acyclic.objects.v1.ObjectsService/PutObject",
                    ),
                    response::CustodyProstCodec::<wire::PutObjectRequest, wire::ObjectInfo>::new(),
                );
                let (rpc, dispatch_failed) = if let Some(controls) = &controls {
                    controls.dispatch(acyclic_native_runtime::NativeOperationKind::Write, invoke)?
                } else { (invoke(), false) };
                let result = upload::run(
                    rpc.map(|reply| reply.map(ok).map_err(|status| error_from_status_controlled(&status, controls.as_ref()))),
                    &state,
                ).await?;
                let length = if dispatch_failed { Err(protocol()) } else { state.finish() };
                response::validate_put_controlled(result, response_allocation, length, controls.as_ref())
            },
        )
        .await
    }
    /// Streams one staged multipart part without publishing the final object.
    async fn upload_part_stream_owned(
        owner: Self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> Result<wire::UploadedPart, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.grpc.upload_part",
                rpc.code = obs::Empty
            ),
            async {
                request::upload_part_stream_digest(&query)?;
                let original = query.encode_to_vec();
                let header = wire::UploadPartRequest {
                    frame: Some(wire::upload_part_request::Frame::Header(query)),
                };
                let (chunks, state) = upload::chunks(body, None)?;
                let frames =
                    chunks.map(part_frame as fn(Result<OwnedBytes, Error>) -> wire::UploadPartRequest);
                let mut client = owner.multipart();
                let complete = wire::UploadPartRequest {
                    frame: Some(wire::upload_part_request::Frame::Complete(true)),
                };
                let input = owner.authenticated_transfer(
                    stream::iter([header])
                        .chain(frames)
                        .chain(stream::iter([complete]))
                        .boxed(),
                );
                let result = upload::run(
                    client
                        .upload_part(input)
                        .map(|reply| reply.map(ok).map_err(|status| error_from_status(&status)))
                        .boxed(),
                    &state,
                )
                .await?;
                response::validate_binary(
                    "multipart/upload-part",
                    &original,
                    &result.encode_to_vec(),
                    state.finish()?,
                )?;
                Ok(result)
            },
        )
        .await
    }
    /// Connects trusting the platform's roots, or exclusively the supplied private CA.
    pub async fn connect(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
    ) -> Result<Self, ConnectError> {
        Self::connect_tls(endpoint, token, ca, None).await
    }
    /// Connects with a caller-supplied PEM client certificate chain and private key.
    pub async fn connect_with_identity(
        endpoint: &str,
        token: &str,
        ca: Option<&[u8]>,
        certificate_pem: &[u8],
        private_key_pem: &[u8],
    ) -> Result<Self, ConnectError> {
        if certificate_pem.is_empty()
            || certificate_pem.len() > super::MAX_PEM_BYTES
            || private_key_pem.is_empty()
            || private_key_pem.len() > super::MAX_PEM_BYTES
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
            // Allow caller-selected transfer deadlines through the channel.
            .timeout(MAX_TRANSFER_TIMEOUT)
            .connect()
            .await?;
        Ok(Self {
            channel,
            authorization,
            transfer_timeout: REQUEST_TIMEOUT,
        })
    }
    fn authenticated<T>(&self, value: T) -> Request<T> {
        self.authenticated_with_timeout(value, REQUEST_TIMEOUT)
    }
    fn authenticated_transfer<T>(&self, value: T) -> Request<T> {
        self.authenticated_with_timeout(value, self.transfer_timeout)
    }
    fn authenticated_with_timeout<T>(&self, value: T, timeout: std::time::Duration) -> Request<T> {
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
    fn multipart(&self) -> wire::multipart_service_client::MultipartServiceClient<Channel> {
        wire::multipart_service_client::MultipartServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MESSAGE_BYTES)
            .max_encoding_message_size(MESSAGE_BYTES)
    }
    /// Starts a validated download without buffering the selected representation.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        self.get_stream_controlled(response::RequestInput::ordinary(query), maximum_bytes, None, &mut None, None).await
    }
    async fn get_stream_controlled(
        &self,
        request_input: response::RequestInput<wire::GetObjectRequest>,
        maximum_bytes: u64,
        controls: Option<response::NativeGetControls>,
        header_allocation: &mut Option<acyclic_native_runtime::ReadAllocation>,
        wire: Option<PreparedWireBody>,
    ) -> Result<Download, Error> {
        obs::download(
            obs::span!(INFO, "acyclic.objects.grpc.get", rpc.code = obs::Empty),
            async {
                let query = request_input.value();
                request::get(query)?;
                let range = query.range;
                let deadline = tokio::time::Instant::now() + self.transfer_timeout;
                let stream_controls = controls.clone();
                let body_factory = |frames: tonic::Streaming<wire::GetObjectResponse>, remaining: u64| stream::try_unfold(
                    (frames, remaining, stream_controls),
                    move |(mut frames, remaining, controls)| async move {
                        let allocation = if let Some(controls) = &controls {
                            controls.check()?;
                            Some(acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, MESSAGE_BYTES)
                                .map_err(|error| controls.fail(error))?)
                        } else { None };
                        let Some(frame) = tokio::time::timeout_at(deadline, frames.message())
                            .await
                            .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
                            .map_err(|status| error_from_status_controlled(&status, controls.as_ref()))?
                        else {
                            return if remaining == 0 {
                                Ok(None)
                            } else {
                                Err(protocol())
                            };
                        };
                        match frame.frame {
                            Some(wire::get_object_response::Frame::Body(frame)) => {
                                let bytes = if let Some(controls) = &controls {
                                    controls.body(frame, remaining, allocation.expect("native frame preclaim"))?
                                } else {
                                    bytes::Bytes::from(response::body(frame, remaining)?)
                                };
                                let remaining = remaining - bytes.len() as u64;
                                Ok(Some((bytes, (frames, remaining, controls))))
                            }
                            frame => Err(response::reject_frame(frame, allocation, controls.as_ref())),
                        }
                    },
                );
                let prepared_body = response::prepare_body_stream(&body_factory, controls.as_ref())?;
                let mut client = tonic::client::Grpc::new(ObjectsChannel { channel: self.channel.clone(), wire })
                    .max_decoding_message_size(MESSAGE_BYTES)
                    .max_encoding_message_size(MESSAGE_BYTES);
                client.ready().await.map_err(|_| {
                    obs::record!("rpc.code" = tonic::Code::Unknown as u64);
                    response::invalid()
                })?;
                let mut request = self.authenticated_transfer(request_input.with_controls(controls.clone())?);
                request.extensions_mut().insert(tonic::GrpcMethod::new(
                    "acyclic.objects.v1.ObjectsService", "GetObject",
                ));
                let invoke = || client.server_streaming(
                    request,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/acyclic.objects.v1.ObjectsService/GetObject",
                    ),
                    response::CustodyProstCodec::<wire::GetObjectRequest, wire::GetObjectResponse>::new(),
                );
                let (rpc, dispatch_failed) = if let Some(controls) = &controls {
                    controls.dispatch(acyclic_native_runtime::NativeOperationKind::Read, invoke)?
                } else { (invoke(), false) };
                let mut frames = rpc.await.map(ok).map_err(|status| error_from_status_controlled(&status, controls.as_ref()))?;
                let first = tokio::time::timeout_at(deadline, frames.message())
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
                    .map_err(|status| error_from_status_controlled(&status, controls.as_ref()))?
                    .ok_or_else(protocol)?;
                let header = match first.frame {
                    Some(wire::get_object_response::Frame::Header(header)) => header,
                    frame => return Err(response::reject_frame(frame, header_allocation.take(), controls.as_ref())),
                };
                if let Some(mut allocation) = header_allocation.take() {
                    if let Err(error) = allocation.reconcile_capacity(response::header_capacity(&header)) {
                        return Err(controls.as_ref().expect("native header custody")
                            .fail(NativeObjectsError::from(error).with_header(header, allocation)));
                    }
                    *header_allocation = Some(allocation);
                }
                if dispatch_failed {
                    let controls = controls.as_ref().expect("native RPC attempt");
                    let original = controls.errors.take().expect("recorded native RPC error");
                    return Err(controls.fail(original.with_header(
                        header, header_allocation.take().expect("native header custody"),
                    )));
                }
                let remaining = match response::get_header(&header, &range, maximum_bytes) {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        if let Some(allocation) = header_allocation.take() {
                            return Err(controls.as_ref().expect("native header custody")
                                .fail(NativeObjectsError::from(error).with_header(header, allocation)));
                        }
                        return Err(error);
                    }
                };
                let body = response::finish_body_stream(body_factory, frames, remaining, prepared_body);
                Ok(Download { header, body })
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
#[must_use]
pub fn error_from_status(status: &tonic::Status) -> Error {
    error_from_status_controlled(status, None)
}
fn error_from_status_controlled(
    status: &tonic::Status, controls: Option<&response::NativeGetControls>,
) -> Error {
    let _detail_custody = if let Some(controls) = controls {
        let Some(maximum) = status.details().len().checked_mul(4) else { return response::invalid(); };
        match acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, maximum) {
            Ok(allocation) => Some(allocation),
            Err(error) => return controls.fail(error),
        }
    } else { None };
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
                    .create_bucket(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
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
                    .head_bucket(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
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
                    .delete_bucket(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))
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
            let prepared = Download::prepare_native(0, meter.clone())?;
            let errors = prepared.errors.clone();
            errors.prepare_codec_failure(&meter)?;
            let controls = response::NativeGetControls { meter: meter.clone(), errors: errors.clone() };
            let (header, carrier) = request_input.value();
            let wire_bytes = native_put_wire_bytes(header, carrier.len())?;
            let owners = carrier.len().div_ceil(65_536).checked_add(2).ok_or_else(response::invalid)?;
            let wire = match PreparedWireBody::new(controls.clone(), wire_bytes, owners) {
                Ok(wire) => wire,
                Err(error) => {
                    let original = prepared.errors.take().unwrap_or_else(|| error.into());
                    return Err(prepared.fail(original, None, 0, None));
                }
            };
            let mut body = None;
            let request_input = request_input.map(|(header, carrier)| {
                body = Some(carrier);
                header
            });
            let source = upload::chunks_native(body.expect("native publication owns its canonical body"), meter.clone(), errors.clone())?;
            match Self::put_stream_controlled(self.clone(), request_input, source, Some(controls), Some(wire)).await {
                Ok((info, allocation)) => Ok(super::ObjectsPutReceipt::new(info, allocation)),
                Err(error) => {
                    let original = prepared.errors.take().unwrap_or_else(|| error.into());
                    Err(prepared.fail(original, None, 0, None))
                }
            }
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
            errors.prepare_codec_failure(&meter)?;
            let controls = response::NativeGetControls { meter: meter.clone(), errors: errors.clone() };
            let mut header_allocation = Some(acyclic_native_runtime::ReadAllocation::adopt(&meter, MESSAGE_BYTES * 2)?);
            let wire_bytes = request_input.value().encoded_len().checked_add(5).ok_or_else(response::invalid)?;
            let wire = match PreparedWireBody::new(controls.clone(), wire_bytes, 1) {
                Ok(wire) => wire,
                Err(error) => {
                    let original = prepared.errors.take().unwrap_or_else(|| error.into());
                    return Err(prepared.fail(original, None, 0, None));
                }
            };
            let download = match self.get_stream_controlled(request_input, maximum_bytes, Some(controls), &mut header_allocation, Some(wire)).await {
                Ok(download) => download,
                Err(error) => {
                    let original = prepared.errors.take().unwrap_or_else(|| error.into());
                    return Err(prepared.fail(original, None, 0, None));
                }
            };
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
                request::validate_binary("objects/head", &query.encode_to_vec(), 0)?;
                request::bucket(&query.bucket)?;
                request::key(&query.object_key)?;
                bounded(&query)?;
                let response = self
                    .objects()
                    .head_object(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
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
                    .delete_object(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))
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
                request::validate_binary("objects/list", &query.encode_to_vec(), 0)?;
                request::page_size(query.page_size)?;
                bounded(&query)?;
                let response = self
                    .objects()
                    .list_objects(self.authenticated(query.clone()))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
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
                    .create_multipart(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))
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
                request::validate_binary("multipart/list-parts", &query.encode_to_vec(), 0)?;
                request::page_size(query.page_size)?;
                bounded(&query)?;
                let response = self
                    .multipart()
                    .list_parts(self.authenticated(query.clone()))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
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
                    .complete_multipart(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))?;
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
                    .abort_multipart(self.authenticated(query))
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status))
            },
        )
        .await
    }
}

#[cfg(test)]
mod native_wire_tests {
    use super::*;
    use acyclic_native_runtime::NativeReadMeter;
    use std::sync::{Arc, atomic::Ordering::SeqCst};
    use std::task::{Context, Poll};
    use tonic::codec::Codec;

    fn encoded_body<S>(source: S) -> tonic::body::Body
    where S: futures::Stream<Item = Result<response::RequestInput<wire::GetObjectRequest>, tonic::Status>> + Send + 'static {
        let encoder = response::CustodyProstCodec::<wire::GetObjectRequest, wire::GetObjectResponse>::new().encoder();
        tonic::body::Body::new(tonic::codec::EncodeBody::new_client(encoder, source, None, Some(MESSAGE_BYTES)))
    }

    #[test]
    fn actual_ready_codec_frame_keeps_original_claim_to_last_wire_alias()
        -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let observed = Arc::new(super::super::tests::NativeTransportMeter::new());
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let prepared = Download::prepare_native(0, meter.clone())?;
        let errors = prepared.errors.clone();
        errors.prepare_codec_failure(&meter)?;
        let controls = response::NativeGetControls { meter: meter.clone(), errors };
        let packet = super::super::ObjectsNativeRequest::new(wire::GetObjectRequest::default(), (), &meter)?;
        let input = response::RequestInput::native(packet, &meter)?.with_controls(Some(controls.clone()))?;
        let ready = PreparedWireBody::new(controls.clone(), input.value().encoded_len() + 5, 1)?;
        let mut body = ready.initialize(encoded_body(stream::iter([Ok(input)])))?;
        let before = observed.allocations.load(SeqCst);
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let frame = match http_body::Body::poll_frame(std::pin::Pin::new(&mut body), &mut context) {
            Poll::Ready(Some(Ok(frame))) => frame,
            _ => panic!("the actual CPU-only native query encoder must produce a ready frame"),
        };
        assert_eq!(observed.allocations.load(SeqCst), before + 1);
        let wire = frame.into_data().expect("the actual codec frame owns encoded bytes");
        assert_eq!(wire.as_ref(), &[0, 0, 0, 0, 0]);
        let alias = wire.slice(1..4);
        drop(body);
        drop(controls);
        drop(prepared);
        drop(meter);
        let retained = observed.memory.load(SeqCst);
        assert_eq!(retained, (5 + std::mem::size_of::<NativeWireChunk>()) as u64);
        let attempts = observed.allocations.load(SeqCst);
        observed.cancelled.store(true, SeqCst);
        observed.limit.store(0, SeqCst);
        let last = alias.clone();
        drop(wire);
        drop(alias);
        assert_eq!(last.as_ref(), &[0, 0, 0]);
        assert_eq!(observed.memory.load(SeqCst), retained);
        assert_eq!(observed.allocations.load(SeqCst), attempts);
        drop(last);
        assert_eq!(observed.memory.load(SeqCst), 0);
        Ok(())
    }

}
