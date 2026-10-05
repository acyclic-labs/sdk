//! Objects v2 loopback service backed by the production Rust provider.
//!
//! This service is used by generated remote consumers.  It keeps one
//! `MemoryObjects` authority for all three generated services, decodes the
//! complete client streams, and encodes provider results directly.

use acyclic_objects::{MemoryObjects, ObjectsProvider, wire};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

#[derive(Clone, Debug)]
pub struct TraceFrame {
    pub type_id: String,
    pub bytes_base64: String,
    pub raw_sha256: String,
}

#[derive(Clone, Debug)]
pub struct TraceRecord {
    pub correlation_id: u64,
    pub rpc: String,
    pub request_sha256: String,
    pub response_sha256: String,
    pub terminal_code: i32,
    pub request_frames: Vec<TraceFrame>,
    pub response_frames: Vec<TraceFrame>,
}

#[derive(Clone)]
pub struct ObjectsFixture {
    provider: Arc<MemoryObjects>,
    trace: Arc<Mutex<Vec<TraceRecord>>>,
    next_correlation_id: Arc<AtomicU64>,
}

impl ObjectsFixture {
    #[must_use]
    pub fn new() -> Self {
        let (provider, _) = MemoryObjects::with_default_bucket_clock(
            crate::fixtures::fixture_clock::objects_clock(),
        );
        Self {
            provider: Arc::new(provider),
            trace: Arc::new(Mutex::new(Vec::new())),
            next_correlation_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn trace_records(&self) -> Vec<TraceRecord> {
        self.trace.lock().expect("trace mutex poisoned").clone()
    }

    fn trace_frame<T: Message>(type_id: &str, value: &T) -> TraceFrame {
        let bytes = value.encode_to_vec();
        let mut hash = Sha256::new();
        hash.update(&bytes);
        TraceFrame {
            type_id: type_id.to_owned(),
            bytes_base64: encode_base64(&bytes),
            raw_sha256: format!("{:x}", hash.finalize()),
        }
    }

    fn trace<T: Message, U: Message>(&self, rpc: &str, request: &T, response: Result<&U, &Status>) {
        let request_bytes = request.encode_to_vec();
        let response_bytes = response
            .as_ref()
            .map_or_else(|_| Vec::new(), |value| value.encode_to_vec());
        let mut request_hash = Sha256::new();
        request_hash.update(&request_bytes);
        let mut response_hash = Sha256::new();
        response_hash.update(&response_bytes);
        self.trace
            .lock()
            .expect("trace mutex poisoned")
            .push(TraceRecord {
                correlation_id: self.next_correlation_id.fetch_add(1, Ordering::Relaxed),
                rpc: rpc.to_owned(),
                request_sha256: format!("{:x}", request_hash.finalize()),
                response_sha256: format!("{:x}", response_hash.finalize()),
                terminal_code: response
                    .as_ref()
                    .map_or_else(|status| status.code() as i32, |_| 0),
                request_frames: vec![Self::trace_frame(std::any::type_name::<T>(), request)],
                response_frames: response.as_ref().map_or_else(
                    |_| Vec::new(),
                    |value| vec![Self::trace_frame(std::any::type_name::<U>(), *value)],
                ),
            });
    }

    fn append_request_frame(&self, rpc: &str, frame: TraceFrame) {
        if let Some(record) = self
            .trace
            .lock()
            .expect("trace mutex poisoned")
            .iter_mut()
            .rev()
            .find(|record| record.rpc == rpc)
        {
            record.request_frames.push(frame);
        }
    }

    fn append_response_frame(&self, rpc: &str, frame: TraceFrame) {
        if let Some(record) = self
            .trace
            .lock()
            .expect("trace mutex poisoned")
            .iter_mut()
            .rev()
            .find(|record| record.rpc == rpc)
        {
            record.response_frames.push(frame);
        }
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::new();
    for chunk in bytes.chunks(3) {
        let value = u32::from(chunk[0]) << 16
            | u32::from(*chunk.get(1).unwrap_or(&0)) << 8
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((value >> 18) & 63) as usize] as char);
        output.push(TABLE[((value >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((value >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(value & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn status(error: acyclic_objects::Error) -> Status {
    use tonic::Code;
    let code = match error.code {
        wire::ErrorCode::Unspecified => Code::Unknown,
        wire::ErrorCode::InvalidArgument => Code::InvalidArgument,
        wire::ErrorCode::NotFound => Code::NotFound,
        wire::ErrorCode::AlreadyExists => Code::AlreadyExists,
        wire::ErrorCode::PreconditionFailed => Code::FailedPrecondition,
        wire::ErrorCode::IdempotencyMismatch => Code::Aborted,
        wire::ErrorCode::QuotaExceeded => Code::ResourceExhausted,
        wire::ErrorCode::Unsupported => Code::Unimplemented,
        wire::ErrorCode::Unavailable => Code::Unavailable,
        wire::ErrorCode::AccessDenied => Code::PermissionDenied,
        wire::ErrorCode::RangeNotSatisfiable => Code::OutOfRange,
        wire::ErrorCode::NotModified => Code::FailedPrecondition,
    };
    Status::new(code, error.to_string())
}

fn invalid(message: impl Into<String>) -> Status {
    Status::invalid_argument(message)
}

async fn collect_put(
    mut frames: tonic::Streaming<wire::PutObjectRequest>,
) -> Result<(wire::PutObjectHeader, Bytes, Vec<TraceFrame>), Status> {
    let mut header = None;
    let mut body = BytesMut::new();
    let mut complete = false;
    let mut observed = Vec::new();
    while let Some(frame) = frames.next().await {
        let frame = frame?;
        observed.push(ObjectsFixture::trace_frame(
            std::any::type_name::<wire::PutObjectRequest>(),
            &frame,
        ));
        match frame.frame {
            Some(wire::put_object_request::Frame::Header(value))
                if header.is_none() && !complete =>
            {
                header = Some(value);
            }
            Some(wire::put_object_request::Frame::Body(value)) if header.is_some() && !complete => {
                body.extend_from_slice(&value);
            }
            Some(wire::put_object_request::Frame::Complete(value))
                if value && header.is_some() && !complete =>
            {
                complete = true;
            }
            _ => {
                return Err(invalid(
                    "PutObject requires header, body, and completion frames",
                ));
            }
        }
    }
    if !complete {
        return Err(invalid("PutObject stream ended before completion"));
    }
    Ok((
        header.ok_or_else(|| invalid("PutObject header is missing"))?,
        body.freeze(),
        observed,
    ))
}

async fn collect_upload(
    mut frames: tonic::Streaming<wire::UploadPartRequest>,
) -> Result<(wire::UploadPartHeader, Bytes, Vec<TraceFrame>), Status> {
    let mut header = None;
    let mut body = BytesMut::new();
    let mut complete = false;
    let mut observed = Vec::new();
    while let Some(frame) = frames.next().await {
        let frame = frame?;
        observed.push(ObjectsFixture::trace_frame(
            std::any::type_name::<wire::UploadPartRequest>(),
            &frame,
        ));
        match frame.frame {
            Some(wire::upload_part_request::Frame::Header(value))
                if header.is_none() && !complete =>
            {
                header = Some(value);
            }
            Some(wire::upload_part_request::Frame::Body(value))
                if header.is_some() && !complete =>
            {
                body.extend_from_slice(&value);
            }
            Some(wire::upload_part_request::Frame::Complete(value))
                if value && header.is_some() && !complete =>
            {
                complete = true;
            }
            _ => {
                return Err(invalid(
                    "UploadPart requires header, body, and completion frames",
                ));
            }
        }
    }
    if !complete {
        return Err(invalid("UploadPart stream ended before completion"));
    }
    Ok((
        header.ok_or_else(|| invalid("UploadPart header is missing"))?,
        body.freeze(),
        observed,
    ))
}

#[tonic::async_trait]
impl wire::buckets_service_server::BucketsService for ObjectsFixture {
    async fn create_bucket(
        &self,
        request: Request<wire::CreateBucketRequest>,
    ) -> Result<Response<wire::Bucket>, Status> {
        let request = request.into_inner();
        match self.provider.create_bucket(request.clone()).await {
            Ok(value) => {
                self.trace("Buckets.CreateBucket", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::CreateBucketRequest, wire::Bucket>(
                    "Buckets.CreateBucket",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn head_bucket(
        &self,
        request: Request<wire::HeadBucketRequest>,
    ) -> Result<Response<wire::Bucket>, Status> {
        let request = request.into_inner();
        match self.provider.head_bucket(request.clone()).await {
            Ok(value) => {
                self.trace("Buckets.HeadBucket", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::HeadBucketRequest, wire::Bucket>(
                    "Buckets.HeadBucket",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn delete_bucket(
        &self,
        request: Request<wire::DeleteBucketRequest>,
    ) -> Result<Response<wire::DeleteBucketResponse>, Status> {
        let request = request.into_inner();
        match self.provider.delete_bucket(request.clone()).await {
            Ok(value) => {
                self.trace("Buckets.DeleteBucket", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::DeleteBucketRequest, wire::DeleteBucketResponse>(
                    "Buckets.DeleteBucket",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }
}

#[tonic::async_trait]
impl wire::objects_service_server::ObjectsService for ObjectsFixture {
    async fn put_object(
        &self,
        request: Request<tonic::Streaming<wire::PutObjectRequest>>,
    ) -> Result<Response<wire::ObjectInfo>, Status> {
        let (header, body, request_frames) = match collect_put(request.into_inner()).await {
            Ok(value) => value,
            Err(failure) => {
                let empty = wire::PutObjectRequest { frame: None };
                self.trace::<wire::PutObjectRequest, wire::ObjectInfo>(
                    "Objects.PutObject",
                    &empty,
                    Err(&failure),
                );
                return Err(failure);
            }
        };
        let request = wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Header(header.clone())),
        };
        match self.provider.put(header, body).await {
            Ok(value) => {
                self.trace("Objects.PutObject", &request, Ok(&value));
                for frame in request_frames.into_iter().skip(1) {
                    self.append_request_frame("Objects.PutObject", frame);
                }
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::PutObjectRequest, wire::ObjectInfo>(
                    "Objects.PutObject",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    type GetObjectStream =
        stream::Iter<std::vec::IntoIter<Result<wire::GetObjectResponse, Status>>>;

    async fn get_object(
        &self,
        request: Request<wire::GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let request = request.into_inner();
        let object = match self.provider.get(request.clone(), 64 * 1024 * 1024).await {
            Ok(value) => value,
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::GetObjectRequest, wire::GetObjectResponse>(
                    "Objects.GetObject",
                    &request,
                    Err(&failure),
                );
                return Err(failure);
            }
        };
        let response_header = wire::GetObjectResponse {
            frame: Some(wire::get_object_response::Frame::Header(
                object.header.clone(),
            )),
        };
        let response_body = wire::GetObjectResponse {
            frame: Some(wire::get_object_response::Frame::Body(object.body.to_vec())),
        };
        self.trace("Objects.GetObject", &request, Ok(&response_header));
        self.append_response_frame(
            "Objects.GetObject",
            Self::trace_frame(
                std::any::type_name::<wire::GetObjectResponse>(),
                &response_body,
            ),
        );
        Ok(Response::new(stream::iter(vec![
            Ok(response_header),
            Ok(response_body),
        ])))
    }

    async fn head_object(
        &self,
        request: Request<wire::HeadObjectRequest>,
    ) -> Result<Response<wire::HeadObjectResponse>, Status> {
        let request = request.into_inner();
        match self.provider.head(request.clone()).await {
            Ok(value) => {
                self.trace("Objects.HeadObject", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::HeadObjectRequest, wire::HeadObjectResponse>(
                    "Objects.HeadObject",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn delete_object(
        &self,
        request: Request<wire::DeleteObjectRequest>,
    ) -> Result<Response<wire::DeleteObjectResponse>, Status> {
        let request = request.into_inner();
        match self.provider.delete(request.clone()).await {
            Ok(value) => {
                self.trace("Objects.DeleteObject", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::DeleteObjectRequest, wire::DeleteObjectResponse>(
                    "Objects.DeleteObject",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn list_objects(
        &self,
        request: Request<wire::ListObjectsRequest>,
    ) -> Result<Response<wire::ListObjectsResponse>, Status> {
        let request = request.into_inner();
        match self.provider.list(request.clone()).await {
            Ok(value) => {
                self.trace("Objects.ListObjects", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::ListObjectsRequest, wire::ListObjectsResponse>(
                    "Objects.ListObjects",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }
}

#[tonic::async_trait]
impl wire::multipart_service_server::MultipartService for ObjectsFixture {
    async fn create_multipart(
        &self,
        request: Request<wire::CreateMultipartRequest>,
    ) -> Result<Response<wire::MultipartUpload>, Status> {
        let request = request.into_inner();
        match self.provider.create_multipart(request.clone()).await {
            Ok(value) => {
                self.trace("Multipart.CreateMultipart", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::CreateMultipartRequest, wire::MultipartUpload>(
                    "Multipart.CreateMultipart",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn upload_part(
        &self,
        request: Request<tonic::Streaming<wire::UploadPartRequest>>,
    ) -> Result<Response<wire::UploadedPart>, Status> {
        let (header, body, request_frames) = match collect_upload(request.into_inner()).await {
            Ok(value) => value,
            Err(failure) => {
                let empty = wire::UploadPartRequest { frame: None };
                self.trace::<wire::UploadPartRequest, wire::UploadedPart>(
                    "Multipart.UploadPart",
                    &empty,
                    Err(&failure),
                );
                return Err(failure);
            }
        };
        let request = wire::UploadPartRequest {
            frame: Some(wire::upload_part_request::Frame::Header(header.clone())),
        };
        match self.provider.upload_part(header, body).await {
            Ok(value) => {
                self.trace("Multipart.UploadPart", &request, Ok(&value));
                for frame in request_frames.into_iter().skip(1) {
                    self.append_request_frame("Multipart.UploadPart", frame);
                }
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::UploadPartRequest, wire::UploadedPart>(
                    "Multipart.UploadPart",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn list_parts(
        &self,
        request: Request<wire::ListPartsRequest>,
    ) -> Result<Response<wire::ListPartsResponse>, Status> {
        let request = request.into_inner();
        match self.provider.list_parts(request.clone()).await {
            Ok(value) => {
                self.trace("Multipart.ListParts", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::ListPartsRequest, wire::ListPartsResponse>(
                    "Multipart.ListParts",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn complete_multipart(
        &self,
        request: Request<wire::CompleteMultipartRequest>,
    ) -> Result<Response<wire::ObjectInfo>, Status> {
        let request = request.into_inner();
        match self.provider.complete_multipart(request.clone()).await {
            Ok(value) => {
                self.trace("Multipart.CompleteMultipart", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::CompleteMultipartRequest, wire::ObjectInfo>(
                    "Multipart.CompleteMultipart",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }

    async fn abort_multipart(
        &self,
        request: Request<wire::AbortMultipartRequest>,
    ) -> Result<Response<wire::AbortMultipartResponse>, Status> {
        let request = request.into_inner();
        match self.provider.abort_multipart(request.clone()).await {
            Ok(value) => {
                self.trace("Multipart.AbortMultipart", &request, Ok(&value));
                Ok(Response::new(value))
            }
            Err(error) => {
                let failure = status(error);
                self.trace::<wire::AbortMultipartRequest, wire::AbortMultipartResponse>(
                    "Multipart.AbortMultipart",
                    &request,
                    Err(&failure),
                );
                Err(failure)
            }
        }
    }
}
