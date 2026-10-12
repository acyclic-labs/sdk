//! Authenticated logical Objects HTTP gateway with bounded Protobuf JSON/NDJSON.
use super::{
    Download, Error, HTTP_ROUTES, NativeObjectsError, Object, ObjectsProvider,
    UploadBody, json, request, response, upload, wire,
};
use crate::obs;
use bytes::Bytes;
use acyclic_stream::OwnedBytes;
use futures::{StreamExt, stream};
use prost::Message;
use reqwest::{
    Body, Client, Response, Url,
    header::{AUTHORIZATION, HeaderValue},
};

/// One NDJSON line, bounded by the base64 of one compressed 8 MiB body frame.
const JSON_FRAME_BYTES: usize = 12 * 1024 * 1024;
const REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// Authenticated native HTTP client for the logical Objects v1 gateway.
#[derive(Clone)]
pub struct HttpObjects {
    transport: Client,
    endpoint: Url,
    authorization: HeaderValue,
    maximum: usize,
}
impl HttpObjects {
    /// Publishes a header-first streamed value. Source failure aborts the HTTP request.
    pub async fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        Ok(self.put_stream_controlled(response::RequestInput::ordinary(query), body, None).await?.0)
    }
    async fn put_stream_controlled(
        &self,
        request_input: response::RequestInput<wire::PutObjectHeader>,
        body: UploadBody,
        controls: Option<response::NativeGetControls>,
    ) -> Result<(wire::ObjectInfo, Option<acyclic_native_runtime::ReadAllocation>), Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/put",
                rpc.code = obs::Empty
            ),
            async {
                let query = request_input.value();
                let _request_allocation = controls.as_ref().map(|controls| controls.request(query)).transpose()?;
                let mut response_allocation = controls.as_ref().map(|controls| {
                    acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, self.maximum.saturating_mul(4))
                        .map_err(|error| controls.fail(error))
                }).transpose()?;
                request::put_stream_digest(query)?;
                let original = response::binary_controlled(query, controls.as_ref())?;
                let header_frame = request_input.map(|query| wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Header(query)),
                });
                let header = line_controlled("PutObjectRequest", header_frame.value(), controls.as_ref())?;
                drop(header_frame);
                let (chunks, state, failure) = upload::chunks_controlled(body, controls.clone())?;
                let frame_controls = controls.clone();
                let frames = chunks.map(move |chunk| {
                    let chunk = chunk?;
                    let (bytes, allocation) = if let Some(controls) = &frame_controls {
                        let (bytes, allocation) = controls.copy(chunk.as_ref())?;
                        (bytes, Some(allocation))
                    } else { (chunk.to_vec(), None) };
                    let frame = wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Body(bytes)),
                    };
                    let encoded = line_controlled("PutObjectRequest", &frame, frame_controls.as_ref());
                    drop(frame);
                    drop(allocation);
                    encoded
                });
                let complete = line_controlled(
                    "PutObjectRequest",
                    &wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Complete(true)),
                    }, controls.as_ref(),
                )?;
                let terminal_controls = controls.clone();
                let header_controls = controls.clone();
                let (result, retained_response): (wire::ObjectInfo, _) = upload::run(
                    async {
                        let reply = self
                            .post_controlled(
                                "objects/put",
                                "application/x-ndjson",
                                response::http_body(
                                    stream::once(async move {
                                        if let Some(controls) = header_controls { controls.check()?; }
                                        Ok::<_, Error>(header)
                                    }).chain(frames)
                                        .chain(stream::once(async move {
                                            if let Some(controls) = terminal_controls { controls.check()?; }
                                            Ok::<_, Error>(complete)
                                        })), controls.as_ref(),
                                )?, controls.as_ref(),
                            )
                            .await?;
                        self.decode_put_controlled(reply, controls.as_ref(), response_allocation.take()).await
                    },
                    failure,
                )
                .await?;
                response::validate_put_controlled(&original.0, result, retained_response, state.finish(), controls.as_ref())
            },
        )
        .await
    }
    /// Streams one independently staged multipart part.
    pub async fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> Result<wire::UploadedPart, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "multipart/upload-part",
                rpc.code = obs::Empty
            ),
            async {
                request::upload_part_stream_digest(&query)?;
                let original = query.encode_to_vec();
                let header = line(
                    "UploadPartRequest",
                    &wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Header(query)),
                    },
                )?;
                let (chunks, state, failure) = upload::chunks(body);
                let frames = chunks.map(|chunk| {
                    line(
                        "UploadPartRequest",
                        &wire::UploadPartRequest {
                            frame: Some(wire::upload_part_request::Frame::Body(chunk?.to_vec())),
                        },
                    )
                });
                let complete = line(
                    "UploadPartRequest",
                    &wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Complete(true)),
                    },
                )?;
                let result: wire::UploadedPart = upload::run(
                    async {
                        let reply = self
                            .post(
                                "multipart/upload-part",
                                "application/x-ndjson",
                                Body::wrap_stream(
                                    stream::once(async { Ok::<_, Error>(header) })
                                        .chain(frames)
                                        .chain(stream::once(async { Ok::<_, Error>(complete) })),
                                ),
                            )
                            .await?;
                        self.decode("UploadedPart", reply).await
                    },
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
            },
        )
        .await
    }
    /// Creates a bounded client using HTTPS or loopback HTTP for local development.
    pub fn new(endpoint: &str, token: &str, maximum_response_bytes: usize) -> Result<Self, Error> {
        Self::with_ca_certificate(endpoint, token, maximum_response_bytes, None)
    }
    /// Adds one bounded PEM private CA without modifying process-wide trust.
    pub fn with_ca_certificate(
        endpoint: &str,
        token: &str,
        maximum_response_bytes: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, Error> {
        let invalid = || Error::from(wire::ErrorCode::InvalidArgument);
        let mut endpoint = Url::parse(endpoint).map_err(|_| invalid())?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || token.trim().is_empty()
            || token.len() > super::MAX_BEARER_TOKEN_BYTES
            || maximum_response_bytes == 0
        {
            return Err(invalid());
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| invalid())?;
        authorization.set_sensitive(true);
        let mut transport = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30));
        if let Some(ca) = ca {
            if ca.is_empty() || ca.len() > super::MAX_PEM_BYTES {
                return Err(invalid());
            }
            transport = transport
                .add_root_certificate(reqwest::Certificate::from_pem(ca).map_err(|_| invalid())?);
        }
        Ok(Self {
            transport: transport.build().map_err(|_| invalid())?,
            endpoint,
            authorization,
            maximum: maximum_response_bytes,
        })
    }
    async fn post(
        &self,
        route: &'static str,
        content_type: &str,
        body: Body,
    ) -> Result<Response, Error> {
        self.post_controlled(route, content_type, body, None).await
    }
    async fn post_controlled(
        &self,
        route: &'static str,
        content_type: &str,
        body: Body,
        controls: Option<&response::NativeGetControls>,
    ) -> Result<Response, Error> {
        if let Some(controls) = controls { controls.check()?; }
        let prefix = "v1/objects/";
        let mut path = (String::new(), None);
        let maximum = prefix.len().checked_add(route.len()).ok_or_else(response::invalid)?;
        if let Some(controls) = controls {
            path.1 = Some(acyclic_native_runtime::ReadAllocation::new(&controls.meter, maximum)
                .map_err(|error| controls.fail(error))?);
        }
        path.0.try_reserve_exact(maximum).map_err(|error| {
            controls.map_or_else(response::invalid, |controls| controls.fail(controls.meter.allocation_failed(error)))
        })?;
        if let Some(controls) = controls {
            path.1.as_mut().expect("native route string custody")
                .reconcile_capacity(path.0.capacity()).map_err(|error| controls.fail(error))?;
            let url_maximum = self.endpoint.as_str().len().checked_add(maximum).ok_or_else(response::invalid)?;
            let url_allocation = acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, url_maximum)
                .map_err(|error| controls.fail(error))?;
            controls.errors.retain_wire(url_allocation).map_err(|error| controls.fail(error))?;
            controls.meter.admit_codec(0, maximum as u64, 0).map_err(|error| controls.fail(error))?;
        }
        path.0.push_str(prefix);
        path.0.push_str(route);
        if let Some(controls) = controls {
            controls.meter.record_codec(0, maximum as u64, 0).map_err(|error| controls.fail(error))?;
        }
        let url = self.endpoint.join(&path.0)
            .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
        drop(path);
        let builder = self.transport.post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("content-type", content_type)
            .body(body);
        let (rpc, dispatch_failed) = if let Some(controls) = controls {
            let kind = if route == "objects/get" {
                acyclic_native_runtime::NativeOperationKind::Read
            } else {
                acyclic_native_runtime::NativeOperationKind::Write
            };
            controls.dispatch(kind, || builder.send())?
        } else { (builder.send(), false) };
        let response = rpc.await.map_err(|_| response::invalid())?;
        if dispatch_failed { return Err(response::invalid()); }
        obs::record!("rpc.code" = response.status().as_u16());
        if response.status().as_u16() != 200 {
            return Err(self.failure_controlled(response, controls).await);
        }
        Ok(response)
    }
    async fn bytes(&self, response: Response) -> Result<Vec<u8>, Error> {
        Ok(self.bytes_controlled(response, None).await?.0)
    }
    async fn bytes_controlled(
        &self, mut response: Response, controls: Option<&response::NativeGetControls>,
    ) -> Result<(Vec<u8>, Option<acyclic_native_runtime::ReadAllocation>), Error> {
        if response
            .content_length()
            .is_some_and(|size| size > self.maximum as u64)
        {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let mut bytes = Vec::new();
        let mut allocation = controls.map(|controls| acyclic_native_runtime::ReadAllocation::empty(&controls.meter));
        let result = async {
            while let Some(chunk) = response.chunk().await.map_err(|_| response::invalid())? {
                if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                if let Some(controls) = controls {
                    controls.check()?;
                    let needed = bytes.len() + chunk.len();
                    if needed > bytes.capacity() {
                        let allocation = allocation.as_mut().expect("native response custody");
                        allocation.grow(needed - bytes.capacity()).map_err(|error| controls.fail(error))?;
                        bytes.try_reserve_exact(needed - bytes.len())
                            .map_err(|error| controls.fail(controls.meter.allocation_failed(error)))?;
                        allocation.reconcile_capacity(bytes.capacity()).map_err(|error| controls.fail(error))?;
                    }
                    controls.meter.admit_codec(0, chunk.len() as u64, 0).map_err(|error| controls.fail(error))?;
                }
                bytes.extend_from_slice(&chunk);
                if let Some(controls) = controls {
                    controls.meter.record_codec(0, chunk.len() as u64, 0).map_err(|error| controls.fail(error))?;
                }
            }
            Ok::<(), Error>(())
        }.await;
        if let Err(error) = result {
            if let Some(controls) = controls {
                let original = controls.errors.take().unwrap_or_else(|| error.into());
                let mut allocation = allocation.expect("native response wire custody");
                let _ = allocation.reconcile_capacity(bytes.capacity());
                return Err(controls.fail(original.with_wire(bytes, allocation)));
            }
            return Err(error);
        }
        Ok((bytes, allocation))
    }
    async fn failure_controlled(&self, response: Response, controls: Option<&response::NativeGetControls>) -> Error {
        let status = response.status().as_u16();
        if status == 304 {
            return wire::ErrorCode::NotModified.into();
        }
        let received = match self.bytes_controlled(response, controls).await {
            Ok(value) => value,
            Err(error) => return error,
        };
        let decoded_allocation = if let Some(controls) = controls {
            match acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, received.0.len().saturating_mul(4)) {
                Ok(allocation) => Some(allocation),
                Err(error) => {
                    let (bytes, allocation) = received;
                    return controls.fail(NativeObjectsError::from(error).with_wire(bytes, allocation.expect("native failure wire custody")));
                }
            }
        } else { None };
        let code = if let Ok(detail) = json::decode::<wire::ErrorDetail>("ErrorDetail", &received.0, self.maximum)
            && let Ok(code) = wire::ErrorCode::try_from(detail.code)
            && code != wire::ErrorCode::Unspecified {
            code
        } else {
            match status {
                400 => wire::ErrorCode::InvalidArgument,
                401 | 403 => wire::ErrorCode::AccessDenied,
                404 => wire::ErrorCode::NotFound,
                409 => wire::ErrorCode::AlreadyExists,
                412 => wire::ErrorCode::PreconditionFailed,
                413 | 429 => wire::ErrorCode::QuotaExceeded,
                416 => wire::ErrorCode::RangeNotSatisfiable,
                501 => wire::ErrorCode::Unsupported,
                _ => wire::ErrorCode::Unavailable,
            }
        };
        drop(decoded_allocation);
        if let Some(controls) = controls {
            let (bytes, allocation) = received;
            controls.fail(NativeObjectsError::from(Error::from(code)).with_wire(bytes, allocation.expect("native failure wire custody")))
        } else {
            code.into()
        }
    }
    async fn decode<O: Message + Default>(
        &self,
        name: &str,
        response: Response,
    ) -> Result<O, Error> {
        media_type(&response, "application/json")?;
        json::decode(name, &self.bytes(response).await?, self.maximum)
            .map_err(|_| response::invalid())
    }
    async fn decode_put_controlled(
        &self,
        response: Response,
        controls: Option<&response::NativeGetControls>,
        allocation: Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Result<(wire::ObjectInfo, Option<acyclic_native_runtime::ReadAllocation>), Error> {
        media_type(&response, "application/json")?;
        let received = self.bytes_controlled(response, controls).await?;
        let _codec_allocation = if let Some(controls) = controls {
            match acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, received.0.len().saturating_mul(4)) {
                Ok(allocation) => Some(allocation),
                Err(error) => {
                    let (bytes, allocation) = received;
                    return Err(controls.fail(NativeObjectsError::from(error).with_wire(bytes, allocation.expect("native response wire custody"))));
                }
            }
        } else { None };
        let value = match json::decode("ObjectInfo", &received.0, self.maximum) {
            Ok(value) => value,
            Err(_) => {
                if let Some(controls) = controls {
                    let (bytes, allocation) = received;
                    return Err(controls.fail(NativeObjectsError::from(response::invalid()).with_wire(
                        bytes, allocation.expect("native response wire custody"),
                    )));
                }
                return Err(response::invalid());
            }
        };
        Ok((value, allocation))
    }
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &'static str,
        value: I,
        validate: impl FnOnce(I, O) -> Result<O, Error>,
    ) -> Result<O, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route,
                rpc.code = obs::Empty
            ),
            async {
                let (_, input, output) = HTTP_ROUTES
                    .iter()
                    .find(|(path, _, _)| *path == route)
                    .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
                let bytes = json::encode(input, &value)?;
                if bytes.len() > REQUEST_BYTES {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                let response = self.post(route, "application/json", bytes.into()).await?;
                validate(value, self.decode(output, response).await?)
            },
        )
        .await
    }
    /// Starts a bounded header-first download without collecting its body.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        self.get_stream_controlled(response::RequestInput::ordinary(query), maximum_bytes, None, &mut None).await
    }
    async fn get_stream_controlled(
        &self,
        request_input: response::RequestInput<wire::GetObjectRequest>,
        maximum_bytes: u64,
        controls: Option<response::NativeGetControls>,
        header_allocation: &mut Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Result<Download, Error> {
        obs::download(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/get",
                rpc.code = obs::Empty
            ),
            async {
                let query = request_input.value();
                let _request_allocation = controls.as_ref().map(|controls| controls.request(query)).transpose()?;
                let request_binary = response::binary_controlled(query, controls.as_ref())?;
                request::validate_binary("objects/get", &request_binary.0, 0)?;
                let body_factory = |reader: Frames, remaining: u64|
                    stream::try_unfold((reader, remaining), |(mut reader, remaining)| async move {
                        let Some((frame, allocation)) = reader.next().await? else {
                            return if remaining == 0 {
                                Ok(None)
                            } else {
                                Err(response::invalid())
                            };
                        };
                        match frame.frame {
                            Some(wire::get_object_response::Frame::Body(frame)) => {
                                let bytes = if let Some(controls) = &reader.controls {
                                    controls.body(frame, remaining, allocation.expect("native frame preclaim"))?
                                } else {
                                    Bytes::from(response::body(frame, remaining)?)
                                };
                                let remaining = remaining - bytes.len() as u64;
                                Ok(Some((bytes, (reader, remaining))))
                            }
                            frame => Err(response::reject_frame(frame, allocation, reader.controls.as_ref())),
                        }
                    });
                let prepared_body = response::prepare_body_stream(&body_factory, controls.as_ref())?;
                drop(request_binary);
                let bytes = if let Some(controls) = &controls {
                    let (bytes, allocation) = json::encode_native("GetObjectRequest", query, controls)?;
                    controls.wire_bytes(bytes, allocation)?
                } else {
                    Bytes::from(json::encode("GetObjectRequest", query)?)
                };
                if bytes.len() > REQUEST_BYTES {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                let reply = self
                    .post_controlled("objects/get", "application/json", bytes.into(), controls.as_ref())
                    .await?;
                media_type(&reply, "application/x-ndjson")?;
                if reply
                    .content_length()
                    .is_some_and(|size| size > self.maximum as u64)
                {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                let mut reader = Frames {
                    reply,
                    chunk: Bytes::new(),
                    pending: Vec::new(),
                    total: 0,
                    maximum: self.maximum,
                    pending_allocation: controls.as_ref().map(|controls| acyclic_native_runtime::ReadAllocation::empty(&controls.meter)),
                    controls: controls.clone(),
                };
                let (first, first_allocation) = reader.next().await?.ok_or_else(response::invalid)?;
                let header = match first.frame {
                    Some(wire::get_object_response::Frame::Header(header)) => header,
                    frame => return Err(response::reject_frame(frame, first_allocation, controls.as_ref())),
                };
                if let Some(mut allocation) = header_allocation.take() {
                    if let Err(error) = allocation.reconcile_capacity(response::header_capacity(&header)) {
                        return Err(controls.as_ref().expect("native header custody")
                            .fail(NativeObjectsError::from(error).with_header(header, allocation)));
                    }
                    *header_allocation = Some(allocation);
                }
                let remaining = match response::get_header(&header, &query.range, maximum_bytes) {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        if let Some(allocation) = header_allocation.take() {
                            return Err(controls.as_ref().expect("native header custody")
                                .fail(NativeObjectsError::from(error).with_header(header, allocation)));
                        }
                        return Err(error);
                    }
                };
                let body = response::finish_body_stream(body_factory, reader, remaining, prepared_body);
                Ok(Download { header, body })
            },
        )
        .await
    }
}
fn media_type(response: &Response, expected: &str) -> Result<(), Error> {
    if response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_none_or(|value| !value.trim().eq_ignore_ascii_case(expected))
    {
        return Err(response::invalid());
    }
    Ok(())
}
fn line(name: &str, frame: &impl Message) -> Result<Bytes, Error> {
    let mut bytes = json::encode(name, frame)?;
    bytes.push(b'\n');
    if bytes.len() > JSON_FRAME_BYTES {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(bytes.into())
}

fn line_controlled(
    name: &str, frame: &impl Message, controls: Option<&response::NativeGetControls>,
) -> Result<Bytes, Error> {
    let Some(controls) = controls else { return line(name, frame); };
    let (mut bytes, mut allocation) = json::encode_native(name, frame, controls)?;
    if bytes.len() == bytes.capacity() {
        allocation.grow(1).map_err(|error| controls.fail(error))?;
        bytes.try_reserve_exact(1).map_err(|error| controls.fail(controls.meter.allocation_failed(error)))?;
        allocation.reconcile_capacity(bytes.capacity()).map_err(|error| controls.fail(error))?;
    }
    bytes.push(b'\n');
    if bytes.len() > JSON_FRAME_BYTES {
        return Err(controls.fail(NativeObjectsError::from(Error::from(wire::ErrorCode::QuotaExceeded)).with_wire(bytes, allocation)));
    }
    controls.wire_bytes(bytes, allocation)
}
struct Frames {
    reply: Response,
    chunk: Bytes,
    pending: Vec<u8>,
    total: usize,
    maximum: usize,
    controls: Option<response::NativeGetControls>,
    pending_allocation: Option<acyclic_native_runtime::ReadAllocation>,
}
impl Frames {
    async fn next(&mut self) -> Result<Option<(wire::GetObjectResponse, Option<acyclic_native_runtime::ReadAllocation>)>, Error> {
        let result = self.next_inner().await;
        match result {
            Err(error) if self.controls.is_some() => {
                let controls = self.controls.as_ref().expect("native frame controls");
                let original = controls.errors.take().unwrap_or_else(|| error.into());
                let bytes = std::mem::take(&mut self.pending);
                let allocation = self.pending_allocation.take()
                    .unwrap_or_else(|| acyclic_native_runtime::ReadAllocation::empty(&controls.meter));
                Err(controls.fail(original.with_wire(bytes, allocation)))
            }
            result => result,
        }
    }
    async fn next_inner(&mut self) -> Result<Option<(wire::GetObjectResponse, Option<acyclic_native_runtime::ReadAllocation>)>, Error> {
        let mut frame_allocation = if let Some(controls) = &self.controls {
            controls.check()?;
            Some(acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, JSON_FRAME_BYTES * 4)
                .map_err(|error| controls.fail(error))?)
        } else { None };
        loop {
            if self.chunk.is_empty() {
                if let Some(controls) = &self.controls { controls.check()?; }
                let Some(chunk) = self.reply.chunk().await.map_err(|_| response::invalid())? else {
                    return if self.pending.is_empty() {
                        Ok(None)
                    } else {
                        Err(response::invalid())
                    };
                };
                if chunk.len() > self.maximum.saturating_sub(self.total) {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                self.total += chunk.len();
                self.chunk = chunk;
            }
            let newline = self.chunk.iter().position(|byte| *byte == b'\n');
            let size = newline.map_or(self.chunk.len(), |index| index + 1);
            if size > JSON_FRAME_BYTES.saturating_sub(self.pending.len()) {
                return Err(response::invalid());
            }
            if let Some(controls) = &self.controls {
                let needed = self.pending.len() + size;
                if needed > self.pending.capacity() {
                    let allocation = self.pending_allocation.as_mut().expect("native pending custody");
                    allocation.grow(needed - self.pending.capacity()).map_err(|error| controls.fail(error))?;
                    self.pending.try_reserve_exact(needed - self.pending.len())
                        .map_err(|error| controls.fail(controls.meter.allocation_failed(error)))?;
                    allocation.reconcile_capacity(self.pending.capacity()).map_err(|error| controls.fail(error))?;
                }
                controls.meter.admit_codec(0, size as u64, 0).map_err(|error| controls.fail(error))?;
            }
            self.pending.extend_from_slice(&self.chunk.split_to(size));
            if let Some(controls) = &self.controls {
                controls.meter.record_codec(0, size as u64, 0).map_err(|error| controls.fail(error))?;
            }
            if newline.is_some() {
                self.pending.pop();
                let bytes = self.pending.strip_suffix(b"\r").unwrap_or(&self.pending);
                let (frame, allocation) = if let Some(controls) = &self.controls {
                    let (frame, allocation) = json::decode_get_native(
                        bytes, JSON_FRAME_BYTES, controls, frame_allocation.take().expect("native frame preclaim"),
                    ).map_err(|_| response::invalid())?;
                    (frame, Some(allocation))
                } else {
                    (json::decode("GetObjectResponse", bytes, JSON_FRAME_BYTES).map_err(|_| response::invalid())?, None)
                };
                self.pending.clear();
                return Ok(Some((frame, allocation)));
            }
        }
    }
}
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
        let request_input = response::RequestInput::native(request, &meter)?;
        super::native_future(meter.clone(), async move {
            let prepared = Download::prepare_native(0, meter.clone())?;
            let errors = prepared.errors();
            let mut body = None;
            let request_input = request_input.map(|(header, carrier)| {
                body = Some(carrier);
                header
            });
            let source = upload::chunks_native(body.expect("native publication owns its canonical body"), meter.clone(), errors.clone())?;
            let controls = response::NativeGetControls { meter, errors };
            match self.put_stream_controlled(request_input, source, Some(controls)).await {
                Ok((info, allocation)) => Ok(super::ObjectsPutReceipt::new(info, allocation)),
                Err(error) => {
                    let original = prepared.take_error().unwrap_or_else(|| error.into());
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
            let errors = prepared.errors();
            let controls = response::NativeGetControls { meter: meter.clone(), errors: errors.clone() };
            let mut header_allocation = Some(acyclic_native_runtime::ReadAllocation::adopt(&meter, JSON_FRAME_BYTES * 4)?);
            let download = match self.get_stream_controlled(request_input, maximum_bytes, Some(controls), &mut header_allocation).await {
                Ok(download) => download,
                Err(error) => {
                    let original = prepared.take_error().unwrap_or_else(|| error.into());
                    return Err(prepared.fail(original, None, 0, None));
                }
            };
            if let Some(allocation) = header_allocation {
                prepared.retain_header_allocation(allocation)?;
            }
            download.collect_native(prepared).await
        })
    }
    async fn head(
        &self,
        query: wire::HeadObjectRequest,
    ) -> Result<wire::HeadObjectResponse, Error> {
        request::validate_binary("objects/head", &query.encode_to_vec(), 0)?;
        request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        request::read_filters(&query.if_match, &query.if_none_match)?;
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
        request::validate_binary("objects/list", &query.encode_to_vec(), 0)?;
        request::bucket(&query.bucket)?;
        request::page_size(query.page_size)?;
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
        request::validate_binary("multipart/list-parts", &query.encode_to_vec(), 0)?;
        request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        request::upload_id(&query.upload_id)?;
        request::page_size(query.page_size)?;
        if query.after_part_number > 10_000 {
            return Err(wire::ErrorCode::InvalidArgument.into());
        }
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
