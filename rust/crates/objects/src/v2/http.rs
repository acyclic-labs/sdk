//! Authenticated logical Objects HTTP gateway with bounded Protobuf JSON/NDJSON.
use super::{
    Download, Error, HTTP_JSON_FRAME_BYTES, HTTP_ROUTES, Object, ObjectsProvider, UploadBody, json,
    request, response, upload, wire,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use reqwest::{
    Body, Client, Response, Url,
    header::{AUTHORIZATION, HeaderValue},
};

const REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// Authenticated HTTP client for the logical Objects v2 gateway.
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
        request::put_stream_digest(&query)?;
        let original = query.encode_to_vec();
        let header = line(
            "PutObjectRequest",
            &wire::PutObjectRequest {
                frame: Some(wire::put_object_request::Frame::Header(query)),
            },
        )?;
        let (chunks, state, failure) = upload::chunks(body);
        let frames = chunks.map(|chunk| {
            line(
                "PutObjectRequest",
                &wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Body(chunk?.to_vec())),
                },
            )
        });
        let complete = line(
            "PutObjectRequest",
            &wire::PutObjectRequest {
                frame: Some(wire::put_object_request::Frame::Complete(true)),
            },
        )?;
        let result: wire::ObjectInfo = upload::run(
            async {
                #[cfg(not(target_arch = "wasm32"))]
                let request_body = Body::wrap_stream(
                    stream::once(async { Ok::<_, Error>(header) })
                        .chain(frames)
                        .chain(stream::once(async { Ok::<_, Error>(complete) })),
                );
                #[cfg(target_arch = "wasm32")]
                let request_body = {
                    let frames = stream::once(async { Ok::<_, Error>(header) })
                        .chain(frames)
                        .chain(stream::once(async { Ok::<_, Error>(complete) }));
                    futures::pin_mut!(frames);
                    let mut bytes = Vec::new();
                    while let Some(frame) = frames.next().await {
                        let frame = frame?;
                        if frame.len() > REQUEST_BYTES.saturating_sub(bytes.len()) {
                            return Err(wire::ErrorCode::QuotaExceeded.into());
                        }
                        bytes.extend_from_slice(&frame);
                    }
                    Body::from(bytes)
                };
                let reply = self
                    .post(
                        "objects/put",
                        "application/x-ndjson",
                        request_body,
                    )
                    .await?;
                self.decode("ObjectInfo", reply).await
            },
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
    /// Streams one independently staged multipart part.
    pub async fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> Result<wire::UploadedPart, Error> {
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
                #[cfg(not(target_arch = "wasm32"))]
                let request_body = Body::wrap_stream(
                    stream::once(async { Ok::<_, Error>(header) })
                        .chain(frames)
                        .chain(stream::once(async { Ok::<_, Error>(complete) })),
                );
                #[cfg(target_arch = "wasm32")]
                let request_body = {
                    let frames = stream::once(async { Ok::<_, Error>(header) })
                        .chain(frames)
                        .chain(stream::once(async { Ok::<_, Error>(complete) }));
                    futures::pin_mut!(frames);
                    let mut bytes = Vec::new();
                    while let Some(frame) = frames.next().await {
                        let frame = frame?;
                        if frame.len() > REQUEST_BYTES.saturating_sub(bytes.len()) {
                            return Err(wire::ErrorCode::QuotaExceeded.into());
                        }
                        bytes.extend_from_slice(&frame);
                    }
                    Body::from(bytes)
                };
                let reply = self
                    .post(
                        "multipart/upload-part",
                        "application/x-ndjson",
                        request_body,
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
        response::validate_http_endpoint(endpoint)?;
        let mut endpoint = Url::parse(endpoint).map_err(|_| invalid())?;
        if token.trim().is_empty() || token.len() > 8192 || maximum_response_bytes == 0 {
            return Err(invalid());
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let mut authorization =
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| invalid())?;
        authorization.set_sensitive(true);
        let mut transport = Client::builder();
        #[cfg(not(target_arch = "wasm32"))]
        {
            transport = transport
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(30));
        }
        if let Some(ca) = ca {
            #[cfg(target_arch = "wasm32")]
            let _ = ca;
            #[cfg(not(target_arch = "wasm32"))]
            {
                if ca.is_empty() || ca.len() > 65536 {
                    return Err(invalid());
                }
                transport = transport.add_root_certificate(
                    reqwest::Certificate::from_pem(ca).map_err(|_| invalid())?,
                );
            }
        }
        Ok(Self {
            transport: transport.build().map_err(|_| invalid())?,
            endpoint,
            authorization,
            maximum: maximum_response_bytes,
        })
    }

    /// Verify the authenticated Rust-owned Objects identity using the HTTP control route.
    pub async fn verify_handshake(&self) -> Result<bool, Error> {
        use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
        let family = BindingFamily::Objects;
        let version = control::control_protocol_version(family);
        let route = control::handshake_http_route(family.name())
            .ok_or_else(|| Error::from(wire::ErrorCode::InvalidArgument))?;
        let url = self
            .endpoint
            .join(route.trim_start_matches('/'))
            .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
        let response = self
            .transport
            .get(url.clone())
            .header(AUTHORIZATION, self.authorization.clone())
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|_| response::invalid())?;
        if response.url() != &url {
            return Err(response::invalid());
        }
        let status = response.status();
        if matches!(status.as_u16(), 404 | 405) {
            return Ok(false);
        }
        if !status.is_success() {
            return Err(self.failure(response).await);
        }
        media_type(&response, "application/json")?;
        let bytes = self.bytes(response).await?;
        let pool = DescriptorPool::decode(control::control_descriptor().as_slice())
            .map_err(|_| response::invalid())?;
        let descriptor = pool
            .get_message_by_name("acyclic.protocol.v1.HandshakeResponse")
            .ok_or_else(response::invalid)?;
        let mut json = serde_json::Deserializer::from_slice(&bytes);
        let decoded =
            DynamicMessage::deserialize(descriptor, &mut json).map_err(|_| response::invalid())?;
        json.end().map_err(|_| response::invalid())?;
        control::validate_handshake_response(
            family,
            version,
            &[control::RequiredCapability {
                name: family.name(),
                version,
            }],
            &decoded.encode_to_vec(),
            control::MAXIMUM_HANDSHAKE_RESPONSE_BYTES.min(self.maximum),
        )
        .map_err(|_| response::invalid())?;
        Ok(true)
    }
    async fn post(&self, route: &str, content_type: &str, body: Body) -> Result<Response, Error> {
        let url = self
            .endpoint
            .join(&format!("v2/objects/{route}"))
            .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
        let response = self
            .transport
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .map_err(|_| response::invalid())?;
        if response.status().as_u16() != 200 {
            return Err(self.failure(response).await);
        }
        Ok(response)
    }
    async fn bytes(&self, mut response: Response) -> Result<Vec<u8>, Error> {
        if response
            .content_length()
            .is_some_and(|size| size > self.maximum as u64)
        {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let mut bytes = Vec::new();
        #[cfg(not(target_arch = "wasm32"))]
        while let Some(chunk) = response.chunk().await.map_err(|_| response::invalid())? {
            if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                return Err(wire::ErrorCode::QuotaExceeded.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        #[cfg(target_arch = "wasm32")]
        {
            let chunk = response.bytes().await.map_err(|_| response::invalid())?;
            if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                return Err(wire::ErrorCode::QuotaExceeded.into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
    async fn failure(&self, response: Response) -> Error {
        let status = response.status().as_u16();
        if status == 304 {
            return wire::ErrorCode::NotModified.into();
        }
        let bytes = match self.bytes(response).await {
            Ok(bytes) => bytes,
            Err(error) => return error,
        };
        if let Ok(detail) = json::decode::<wire::ErrorDetail>("ErrorDetail", &bytes, self.maximum)
            && let Ok(code) = wire::ErrorCode::try_from(detail.code)
            && code != wire::ErrorCode::Unspecified
        {
            return code.into();
        }
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
        .into()
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
    async fn call<I: Message, O: Message + Default>(
        &self,
        route: &str,
        value: &I,
    ) -> Result<O, Error> {
        let (_, input, output) = HTTP_ROUTES
            .iter()
            .find(|(path, _, _)| *path == route)
            .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
        let bytes = json::encode(input, value)?;
        if bytes.len() > REQUEST_BYTES {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let response = self.post(route, "application/json", bytes.into()).await?;
        self.decode(output, response).await
    }
    /// Starts a bounded header-first download without collecting its body.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        request::validate_binary("objects/get", &query.encode_to_vec(), 0)?;
        let bytes = json::encode("GetObjectRequest", &query)?;
        if bytes.len() > REQUEST_BYTES {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let reply = self
            .post("objects/get", "application/json", bytes.into())
            .await?;
        media_type(&reply, "application/x-ndjson")?;
        if reply
            .content_length()
            .is_some_and(|size| size > self.maximum as u64)
        {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        #[cfg(target_arch = "wasm32")]
        let buffered = Bytes::from(self.bytes(reply).await?);
        let mut reader = Frames {
            #[cfg(not(target_arch = "wasm32"))]
            reply,
            chunk: Bytes::new(),
            pending: Vec::new(),
            total: 0,
            maximum: self.maximum,
            #[cfg(target_arch = "wasm32")]
            buffered: Some(buffered),
        };
        let first = reader.next().await?.ok_or_else(response::invalid)?;
        let header = match first.frame {
            Some(wire::get_object_response::Frame::Header(header)) => header,
            Some(wire::get_object_response::Frame::Error(detail)) => {
                return Err(frame_error(detail.code));
            }
            _ => return Err(response::invalid()),
        };
        let remaining = response::get_header(&header, &query.range, maximum_bytes)?;
        let body = stream::try_unfold((reader, remaining), |(mut reader, remaining)| async move {
            let Some(frame) = reader.next().await? else {
                return if remaining == 0 {
                    Ok(None)
                } else {
                    Err(response::invalid())
                };
            };
            match frame.frame {
                Some(wire::get_object_response::Frame::Body(bytes)) => {
                    let remaining = response::validate_get_body(bytes.len() as u64, remaining)?;
                    Ok(Some((Bytes::from(bytes), (reader, remaining))))
                }
                Some(wire::get_object_response::Frame::Error(detail)) => {
                    Err(frame_error(detail.code))
                }
                _ => Err(response::invalid()),
            }
        });
        #[cfg(not(target_arch = "wasm32"))]
        let body = body.boxed();
        #[cfg(target_arch = "wasm32")]
        let body = body.boxed_local();
        Ok(Download { header, body })
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
    if bytes.len() > HTTP_JSON_FRAME_BYTES {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(bytes.into())
}
struct Frames {
    #[cfg(not(target_arch = "wasm32"))]
    reply: Response,
    chunk: Bytes,
    pending: Vec<u8>,
    total: usize,
    maximum: usize,
    #[cfg(target_arch = "wasm32")]
    buffered: Option<Bytes>,
}
impl Frames {
    async fn next(&mut self) -> Result<Option<wire::GetObjectResponse>, Error> {
        loop {
            if self.chunk.is_empty() {
                #[cfg(not(target_arch = "wasm32"))]
                let Some(chunk) = self.reply.chunk().await.map_err(|_| response::invalid())? else {
                    return if self.pending.is_empty() {
                        Ok(None)
                    } else {
                        Err(response::invalid())
                    };
                };
                #[cfg(target_arch = "wasm32")]
                let Some(chunk) = self.buffered.take().filter(|chunk| !chunk.is_empty()) else {
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
            if size > HTTP_JSON_FRAME_BYTES.saturating_sub(self.pending.len()) {
                return Err(response::invalid());
            }
            self.pending.extend_from_slice(&self.chunk.split_to(size));
            if newline.is_some() {
                self.pending.pop();
                let bytes = self.pending.strip_suffix(b"\r").unwrap_or(&self.pending);
                let frame = json::decode("GetObjectResponse", bytes, HTTP_JSON_FRAME_BYTES)
                    .map_err(|_| response::invalid())?;
                self.pending.clear();
                return Ok(Some(frame));
            }
        }
    }
}
fn frame_error(code: i32) -> Error {
    wire::ErrorCode::try_from(code)
        .ok()
        .filter(|code| *code != wire::ErrorCode::Unspecified)
        .map_or_else(response::invalid, Error::from)
}
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl ObjectsProvider for HttpObjects {
    async fn create_bucket(&self, query: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> {
        request::create_bucket_digest(&query)?;
        let result = self.call("buckets/create", &query).await?;
        response::bucket(&result, &wire::BucketRef { name: query.name })?;
        Ok(result)
    }
    async fn head_bucket(&self, query: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> {
        request::bucket(&query.bucket)?;
        let result = self.call("buckets/head", &query).await?;
        response::bucket(
            &result,
            query
                .bucket
                .as_ref()
                .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?,
        )?;
        Ok(result)
    }
    async fn delete_bucket(
        &self,
        query: wire::DeleteBucketRequest,
    ) -> Result<wire::DeleteBucketResponse, Error> {
        request::delete_bucket_digest(&query)?;
        self.call("buckets/delete", &query).await
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
        request::read_filters(&query.if_match, &query.if_none_match)?;
        let result: wire::HeadObjectResponse = self.call("objects/head", &query).await?;
        response::object_info(result.object.as_ref().ok_or_else(response::invalid)?)?;
        Ok(result)
    }
    async fn delete(
        &self,
        query: wire::DeleteObjectRequest,
    ) -> Result<wire::DeleteObjectResponse, Error> {
        request::delete_digest(&query)?;
        self.call("objects/delete", &query).await
    }
    async fn list(
        &self,
        query: wire::ListObjectsRequest,
    ) -> Result<wire::ListObjectsResponse, Error> {
        request::validate_binary("objects/list", &query.encode_to_vec(), 0)?;
        request::bucket(&query.bucket)?;
        request::page_size(query.page_size)?;
        let result = self.call("objects/list", &query).await?;
        response::listing(&query, &result)?;
        Ok(result)
    }
    async fn create_multipart(
        &self,
        query: wire::CreateMultipartRequest,
    ) -> Result<wire::MultipartUpload, Error> {
        request::create_multipart_digest(&query)?;
        let result: wire::MultipartUpload = self.call("multipart/create", &query).await?;
        request::upload_id(&result.upload_id).map_err(|_| response::invalid())?;
        Ok(result)
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
        request::bucket(&query.bucket)?;
        request::key(&query.object_key)?;
        request::upload_id(&query.upload_id)?;
        request::page_size(query.page_size)?;
        if query.after_part_number > 10_000 {
            return Err(wire::ErrorCode::InvalidArgument.into());
        }
        let result = self.call("multipart/list-parts", &query).await?;
        response::parts(&query, &result)?;
        Ok(result)
    }
    async fn complete_multipart(
        &self,
        query: wire::CompleteMultipartRequest,
    ) -> Result<wire::ObjectInfo, Error> {
        request::complete_multipart_digest(&query)?;
        let result = self.call("multipart/complete", &query).await?;
        response::object_info(&result)?;
        Ok(result)
    }
    async fn abort_multipart(
        &self,
        query: wire::AbortMultipartRequest,
    ) -> Result<wire::AbortMultipartResponse, Error> {
        request::abort_multipart_digest(&query)?;
        self.call("multipart/abort", &query).await
    }
}
