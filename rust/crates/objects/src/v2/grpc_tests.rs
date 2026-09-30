use super::{Error, MemoryObjects, MemoryOptions, ObjectsProvider, grpc::GrpcObjects, wire};
use bytes::{Bytes, BytesMut};
use futures::stream;
use prost::Message;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

#[derive(Clone)]
struct Fixture(MemoryObjects);
fn authenticated<T>(request: Request<T>) -> Result<T, Status> {
    if request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        != Some("Bearer exact-token")
    {
        return Err(Status::unauthenticated("credential required"));
    }
    Ok(request.into_inner())
}
fn status(error: Error) -> Status {
    Status::with_details(
        tonic::Code::FailedPrecondition,
        "Objects request rejected",
        Bytes::from(
            wire::ErrorDetail {
                code: error.code as i32,
                request_id: "customer-request".into(),
            }
            .encode_to_vec(),
        ),
    )
}
#[tonic::async_trait]
impl wire::buckets_service_server::BucketsService for Fixture {
    async fn create_bucket(
        &self,
        request: Request<wire::CreateBucketRequest>,
    ) -> Result<Response<wire::Bucket>, Status> {
        self.0
            .create_bucket(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn head_bucket(
        &self,
        request: Request<wire::HeadBucketRequest>,
    ) -> Result<Response<wire::Bucket>, Status> {
        self.0
            .head_bucket(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn delete_bucket(
        &self,
        request: Request<wire::DeleteBucketRequest>,
    ) -> Result<Response<wire::DeleteBucketResponse>, Status> {
        self.0
            .delete_bucket(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
}
#[tonic::async_trait]
impl wire::objects_service_server::ObjectsService for Fixture {
    async fn put_object(
        &self,
        request: Request<tonic::Streaming<wire::PutObjectRequest>>,
    ) -> Result<Response<wire::ObjectInfo>, Status> {
        let mut frames = authenticated(request)?;
        let Some(wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Header(header)),
        }) = frames.message().await?
        else {
            return Err(Status::invalid_argument("header required"));
        };
        let mut body = BytesMut::new();
        let mut framing = super::request::UploadFraming::default();
        while let Some(frame) = frames.message().await? {
            match frame.frame {
                Some(wire::put_object_request::Frame::Body(bytes)) => {
                    framing.body(&bytes).map_err(status)?;
                    if body.len().saturating_add(bytes.len()) > 8 * 1024 * 1024 {
                        return Err(Status::resource_exhausted("body bound"));
                    }
                    body.extend_from_slice(&bytes);
                }
                Some(wire::put_object_request::Frame::Complete(complete)) => {
                    framing.complete(complete).map_err(status)?;
                }
                _ => return Err(Status::invalid_argument("invalid upload frame")),
            }
        }
        framing.finish().map_err(status)?;
        self.0
            .put(header, body.freeze())
            .await
            .map(Response::new)
            .map_err(status)
    }
    type GetObjectStream =
        stream::Iter<std::vec::IntoIter<Result<wire::GetObjectResponse, Status>>>;
    async fn get_object(
        &self,
        request: Request<wire::GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let query = authenticated(request)?;
        let malformed = query.object_key == "malformed";
        let terminal_error = query.object_key == "terminal-error";
        let selected = self.0.get(query, 8 * 1024 * 1024).await.map_err(status)?;
        let mut frames = vec![Ok(wire::GetObjectResponse {
            frame: Some(wire::get_object_response::Frame::Header(
                selected.header.clone(),
            )),
        })];
        if malformed {
            frames.push(Ok(wire::GetObjectResponse {
                frame: Some(wire::get_object_response::Frame::Header(selected.header)),
            }));
        }
        if terminal_error {
            frames.push(Ok(wire::GetObjectResponse {
                frame: Some(wire::get_object_response::Frame::Error(wire::ErrorDetail {
                    code: wire::ErrorCode::AccessDenied as i32,
                    request_id: "revoked".into(),
                })),
            }));
        }
        for bytes in selected.body.chunks(65536).filter(|_| !terminal_error) {
            frames.push(Ok(wire::GetObjectResponse {
                frame: Some(wire::get_object_response::Frame::Body(bytes.to_vec())),
            }));
        }
        Ok(Response::new(stream::iter(frames)))
    }
    async fn head_object(
        &self,
        request: Request<wire::HeadObjectRequest>,
    ) -> Result<Response<wire::HeadObjectResponse>, Status> {
        self.0
            .head(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn delete_object(
        &self,
        request: Request<wire::DeleteObjectRequest>,
    ) -> Result<Response<wire::DeleteObjectResponse>, Status> {
        self.0
            .delete(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn list_objects(
        &self,
        request: Request<wire::ListObjectsRequest>,
    ) -> Result<Response<wire::ListObjectsResponse>, Status> {
        self.0
            .list(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
}
#[tonic::async_trait]
impl wire::multipart_service_server::MultipartService for Fixture {
    async fn create_multipart(
        &self,
        request: Request<wire::CreateMultipartRequest>,
    ) -> Result<Response<wire::MultipartUpload>, Status> {
        self.0
            .create_multipart(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn upload_part(
        &self,
        request: Request<tonic::Streaming<wire::UploadPartRequest>>,
    ) -> Result<Response<wire::UploadedPart>, Status> {
        let mut frames = authenticated(request)?;
        let Some(wire::UploadPartRequest {
            frame: Some(wire::upload_part_request::Frame::Header(header)),
        }) = frames.message().await?
        else {
            return Err(Status::invalid_argument("header required"));
        };
        let mut body = BytesMut::new();
        let mut framing = super::request::UploadFraming::default();
        while let Some(frame) = frames.message().await? {
            match frame.frame {
                Some(wire::upload_part_request::Frame::Body(bytes)) => {
                    framing.body(&bytes).map_err(status)?;
                    if body.len().saturating_add(bytes.len()) > 8 * 1024 * 1024 {
                        return Err(Status::resource_exhausted("body bound"));
                    }
                    body.extend_from_slice(&bytes);
                }
                Some(wire::upload_part_request::Frame::Complete(complete)) => {
                    framing.complete(complete).map_err(status)?;
                }
                _ => return Err(Status::invalid_argument("invalid upload frame")),
            }
        }
        framing.finish().map_err(status)?;
        self.0
            .upload_part(header, body.freeze())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn list_parts(
        &self,
        request: Request<wire::ListPartsRequest>,
    ) -> Result<Response<wire::ListPartsResponse>, Status> {
        self.0
            .list_parts(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn complete_multipart(
        &self,
        request: Request<wire::CompleteMultipartRequest>,
    ) -> Result<Response<wire::ObjectInfo>, Status> {
        self.0
            .complete_multipart(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn abort_multipart(
        &self,
        request: Request<wire::AbortMultipartRequest>,
    ) -> Result<Response<wire::AbortMultipartResponse>, Status> {
        self.0
            .abort_multipart(authenticated(request)?)
            .await
            .map(Response::new)
            .map_err(status)
    }
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // One ordered lifecycle covers every RPC against the same TLS authority.
async fn tls_grpc_exercises_every_rpc_streaming_authentication_bounds_and_semantic_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let pem = certified.cert.pem();
    let key = certified.signing_key.serialize_pem();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let fixture = Fixture(MemoryObjects::new(MemoryOptions::default())?);
    let identity = Identity::from_pem(pem.clone(), key);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(wire::buckets_service_server::BucketsServiceServer::new(
                fixture.clone(),
            ))
            .add_service(wire::objects_service_server::ObjectsServiceServer::new(
                fixture.clone(),
            ))
            .add_service(wire::multipart_service_server::MultipartServiceServer::new(
                fixture,
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    assert!(
        GrpcObjects::connect(&endpoint, "exact-token", None)
            .await
            .is_err()
    );
    let client = GrpcObjects::connect(&endpoint, "exact-token", Some(pem.as_bytes())).await?;
    let unauthorized = GrpcObjects::connect(&endpoint, "wrong", Some(pem.as_bytes())).await?;
    assert_eq!(
        unauthorized
            .create_bucket(wire::CreateBucketRequest {
                name: "customer.inputs".into(),
                mutation: None
            })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    super::conformance::verify(&client, "conformance-grpc").await?;
    super::tests::exercise_provider(&client).await?;
    super::tests::exercise_uploads(
        &client,
        |query, body| Box::pin(client.put_stream(query, body)),
        |query, body| Box::pin(client.upload_part_stream(query, body)),
    )
    .await?;
    super::tests::exercise_streaming(&client, |query, maximum| {
        Box::pin(client.get_stream(query, maximum))
    })
    .await?;
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}
