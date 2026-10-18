//! Authenticated logical Objects HTTP gateway with bounded Protobuf JSON/NDJSON.
use super::{
    Download, Error, HTTP_ROUTES, Object, ObjectsProvider, UploadBody, json, request, response,
    upload, wire,
};
use crate::obs;
pub use acyclic_auth::credential::CredentialProvider;
use acyclic_stream::OwnedBytes;
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use reqwest::{
    Body, Client, Response, Url,
    header::{AUTHORIZATION, HeaderValue},
};

fn credential_header(token: &str) -> Result<HeaderValue, Error> {
    let invalid = || Error::from(wire::ErrorCode::InvalidArgument);
    if !acyclic_auth::credential::valid_credential(token) {
        return Err(invalid());
    }
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| invalid())?;
    value.set_sensitive(true);
    Ok(value)
}

/// One NDJSON line, bounded by the base64 of one compressed 8 MiB body frame.
const JSON_FRAME_BYTES: usize = 12 * 1024 * 1024;
const REQUEST_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
mod credential_tests;

/// Authenticated native HTTP client for the logical Objects v1 gateway.
#[derive(Clone)]
pub struct HttpObjects {
    transport: Client,
    endpoint: Url,
    authorization: Option<HeaderValue>,
    credential: Option<CredentialProvider>,
    maximum: usize,
}
impl HttpObjects {
    /// Sends the canonical header, indexed chunks and completion markers as NDJSON.
    pub async fn put_objects(
        &self,
        frames: impl futures::Stream<Item = wire::PutObjectsRequest> + Send + 'static,
    ) -> Result<wire::PutObjectsResponse, Error> {
        let reply = self
            .post(
                "objects/put-batch",
                "application/x-ndjson",
                Body::wrap_stream(frames.map(|frame| batch_line(&frame))),
            )
            .await?;
        self.decode("PutObjectsResponse", reply).await
    }

    /// Returns the bounded canonical indexed download frames in wire order.
    pub async fn get_objects(
        &self,
        query: wire::GetObjectsRequest,
    ) -> Result<stream::BoxStream<'static, Result<wire::GetObjectsResponse, Error>>, Error> {
        request::get_batch(&query)?;
        let bytes = json::encode("GetObjectsRequest", &query)?;
        if bytes.len() > REQUEST_BYTES {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let reply = self
            .post("objects/get-batch", "application/json", bytes.into())
            .await?;
        media_type(&reply, "application/x-ndjson")?;
        let reader = Frames {
            reply,
            chunk: Bytes::new(),
            pending: Vec::new(),
            total: 0,
            maximum: self.maximum,
        };
        Ok(stream::try_unfold(reader, |mut reader| async move {
            Ok(reader
                .next("GetObjectsResponse")
                .await?
                .map(|frame| (frame, reader)))
        })
        .boxed())
    }

    /// Publishes a header-first streamed value. Source failure aborts the HTTP request.
    pub async fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/put",
                rpc.code = obs::Empty
            ),
            async {
                request::put(&query)?;
                let header = line(
                    "PutObjectRequest",
                    &wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Header(query)),
                    },
                )?;
                let (chunks, state) = upload::chunks(body);
                let frames = chunks.map(|chunk| {
                    line(
                        "PutObjectRequest",
                        &wire::PutObjectRequest {
                            frame: Some(wire::put_object_request::Frame::Body(upload::body_bytes(
                                chunk?,
                            ))),
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
                        let reply = self
                            .post(
                                "objects/put",
                                "application/x-ndjson",
                                Body::wrap_stream(
                                    stream::once(async { Ok::<_, Error>(header) })
                                        .chain(frames)
                                        .chain(stream::once(async { Ok::<_, Error>(complete) })),
                                ),
                            )
                            .await?;
                        self.decode("ObjectInfo", reply).await
                    },
                    &state,
                )
                .await?;
                response::object_info(&result)?;
                if result.size != state.finish()? {
                    return Err(response::invalid());
                }
                Ok(result)
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
                request::upload_part(&query)?;
                let part_number = query.part_number;
                let header = line(
                    "UploadPartRequest",
                    &wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Header(query)),
                    },
                )?;
                let (chunks, state) = upload::chunks(body);
                let frames = chunks.map(|chunk| {
                    line(
                        "UploadPartRequest",
                        &wire::UploadPartRequest {
                            frame: Some(wire::upload_part_request::Frame::Body(
                                upload::body_bytes(chunk?),
                            )),
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
                    &state,
                )
                .await?;
                response::part(&result, part_number, state.finish()?)?;
                Ok(result)
            },
        )
        .await
    }
    /// Creates a bounded client with optional exclusive PEM trust.
    pub fn new(
        endpoint: &str,
        token: &str,
        maximum_response_bytes: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, Error> {
        Self::build(endpoint, Some(token), None, maximum_response_bytes, ca)
    }

    /// Resolve the callback for every physical request, including streamed transfers.
    pub fn with_credential_provider(
        endpoint: &str,
        credential: CredentialProvider,
        maximum: usize,
        ca: Option<&[u8]>,
    ) -> Result<Self, Error> {
        Self::build(endpoint, None, Some(credential), maximum, ca)
    }

    fn build(
        endpoint: &str,
        token: Option<&str>,
        credential: Option<CredentialProvider>,
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
            || maximum_response_bytes == 0
        {
            return Err(invalid());
        }
        if !endpoint.path().ends_with('/') {
            endpoint.set_path(&format!("{}/", endpoint.path()));
        }
        let authorization = token.map(credential_header).transpose()?;
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
            endpoint: endpoint.join("v1/objects/").map_err(|_| invalid())?,
            authorization,
            credential,
            maximum: maximum_response_bytes,
        })
    }
    async fn authorization(&self) -> Result<HeaderValue, Error> {
        if let Some(provider) = &self.credential {
            let token = provider
                .resolve()
                .await
                .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            credential_header(&token).map_err(|_| Error::from(wire::ErrorCode::AccessDenied))
        } else {
            self.authorization
                .clone()
                .ok_or_else(|| Error::from(wire::ErrorCode::AccessDenied))
        }
    }

    async fn post(
        &self,
        route: &'static str,
        content_type: &str,
        body: Body,
    ) -> Result<Response, Error> {
        let url = self
            .endpoint
            .join(route)
            .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
        let response = self
            .transport
            .post(url)
            .header(AUTHORIZATION, self.authorization().await?)
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .map_err(|_| response::invalid())?;
        obs::record!("rpc.code" = response.status().as_u16());
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
        while let Some(chunk) = response.chunk().await.map_err(|_| response::invalid())? {
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
        obs::download(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/get",
                rpc.code = obs::Empty
            ),
            async {
                request::get(&query)?;
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
                let mut reader = Frames {
                    reply,
                    chunk: Bytes::new(),
                    pending: Vec::new(),
                    total: 0,
                    maximum: self.maximum,
                };
                let first = reader
                    .next::<wire::GetObjectResponse>("GetObjectResponse")
                    .await?
                    .ok_or_else(response::invalid)?;
                let header = match first.frame {
                    Some(wire::get_object_response::Frame::Header(header)) => header,
                    frame => return Err(response::reject_frame(frame)),
                };
                let remaining = response::get_header(&header, &query.range, maximum_bytes)?;
                let body =
                    stream::try_unfold((reader, remaining), |(mut reader, remaining)| async move {
                        let Some(frame) = reader
                            .next::<wire::GetObjectResponse>("GetObjectResponse")
                            .await?
                        else {
                            return if remaining == 0 {
                                Ok(None)
                            } else {
                                Err(response::invalid())
                            };
                        };
                        match frame.frame {
                            Some(wire::get_object_response::Frame::Body(frame)) => {
                                let bytes = response::body(frame, remaining)?;
                                let remaining = remaining - bytes.len() as u64;
                                Ok(Some((Bytes::from(bytes), (reader, remaining))))
                            }
                            frame => Err(response::reject_frame(frame)),
                        }
                    })
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
    line_limit(name, frame, JSON_FRAME_BYTES)
}
fn batch_line(frame: &wire::PutObjectsRequest) -> Result<Bytes, Error> {
    use wire::put_objects_request::Frame;
    let limit = match &frame.frame {
        Some(Frame::Header(header)) => {
            request::put_batch(header)?;
            // JSON escaping expands one UTF-8 byte to at most six bytes.
            6 * REQUEST_BYTES
        }
        Some(Frame::Chunk(chunk)) => {
            if matches!(&chunk.frame, Some(wire::put_objects_chunk::Frame::Body(body)) if body.len() > 65536)
            {
                return Err(wire::ErrorCode::QuotaExceeded.into());
            }
            4 * 65536_usize.div_ceil(3) + 128
        }
        _ => 128,
    };
    line_limit("PutObjectsRequest", frame, limit)
}
fn line_limit(name: &str, frame: &impl Message, limit: usize) -> Result<Bytes, Error> {
    let mut bytes = json::encode(name, frame)?;
    bytes.push(b'\n');
    if bytes.len() > limit {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(bytes.into())
}
struct Frames {
    reply: Response,
    chunk: Bytes,
    pending: Vec<u8>,
    total: usize,
    maximum: usize,
}
impl Frames {
    async fn next<T: Message + Default>(&mut self, name: &str) -> Result<Option<T>, Error> {
        loop {
            if self.chunk.is_empty() {
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
            self.pending.extend_from_slice(&self.chunk.split_to(size));
            if newline.is_some() {
                self.pending.pop();
                let bytes = self.pending.strip_suffix(b"\r").unwrap_or(&self.pending);
                let frame =
                    json::decode(name, bytes, JSON_FRAME_BYTES).map_err(|_| response::invalid())?;
                self.pending.clear();
                return Ok(Some(frame));
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
    async fn restore(
        &self,
        query: wire::RestoreObjectsRequest,
    ) -> Result<wire::RestoreObjectsResponse, Error> {
        request::restore(&query)?;
        self.call("objects/restore", query, |_, result| Ok(result))
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

#[cfg(test)]
mod batch_bounds {
    use super::*;
    #[test]
    fn batch_http_frame_bounds_follow_the_canonical_frame_type() {
        let object = wire::PutObjectHeader {
            bucket: Some(wire::BucketRef {
                name: "batch".into(),
            }),
            object_key: "key".into(),
            metadata: Some(wire::ObjectMetadata {
                content_type: "x".repeat(8192),
                ..Default::default()
            }),
            preconditions: Some(wire::Preconditions {
                condition: Some(wire::preconditions::Condition::IfMatch("x".repeat(8192))),
            }),
            ..Default::default()
        };
        let mut frame = wire::PutObjectsRequest {
            frame: Some(wire::put_objects_request::Frame::Header(
                wire::PutObjectsHeader {
                    objects: vec![object; 1000],
                },
            )),
        };
        assert!(frame.encoded_len() < REQUEST_BYTES);
        let encoded = batch_line(&frame).unwrap();
        assert!(encoded.len() > JSON_FRAME_BYTES);
        assert!(line("PutObjectsRequest", &frame).is_err());
        if let Some(wire::put_objects_request::Frame::Header(header)) = &mut frame.frame {
            for object in &mut header.objects {
                object.object_key = "x".repeat(1024);
            }
        }
        assert!(frame.encoded_len() > REQUEST_BYTES);
        assert!(batch_line(&frame).is_err());
        for length in [65536, 65537] {
            let frame = wire::PutObjectsRequest {
                frame: Some(wire::put_objects_request::Frame::Chunk(
                    wire::PutObjectsChunk {
                        index: 999,
                        frame: Some(wire::put_objects_chunk::Frame::Body(vec![0; length])),
                    },
                )),
            };
            assert_eq!(batch_line(&frame).is_ok(), length == 65536);
        }
    }
}
