use super::{Error, MemoryObjects, MemoryOptions, ObjectsProvider, grpc::GrpcObjects, wire};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream};
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
        let query = authenticated(request)?;
        if query.name == "deadline.metadata" {
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        }
        self.0
            .create_bucket(query)
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
    type GetObjectStream = stream::BoxStream<'static, Result<wire::GetObjectResponse, Status>>;
    async fn get_object(
        &self,
        request: Request<wire::GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let query = authenticated(request)?;
        let malformed = query.object_key == "malformed";
        let terminal_error = query.object_key == "terminal-error";
        let delayed = query.object_key == "delayed-body";
        let corrupt = query.object_key == "corrupt-codec";
        let truncated = query.object_key == "truncated";
        let mut selected = self.0.get(query, 8 * 1024 * 1024).await.map_err(status)?;
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
        if truncated {
            selected
                .body
                .truncate(selected.body.len().saturating_sub(1));
        }
        // Alternate codecs so every download reassembles mixed frames in order.
        for (index, bytes) in selected.body.chunks(65536).enumerate() {
            if terminal_error {
                break;
            }
            let body = if corrupt {
                wire::Body {
                    codec: wire::Codec::Zstd as i32,
                    decoded_length: bytes.len() as u64,
                    data: b"invalid-zstd".to_vec(),
                }
            } else if index % 2 == 0 {
                super::response::plain_body(bytes.to_vec())
            } else {
                super::response::zstd_body(bytes)
            };
            frames.push(Ok(wire::GetObjectResponse {
                frame: Some(wire::get_object_response::Frame::Body(body)),
            }));
        }
        Ok(Response::new(
            stream::iter(frames)
                .then(move |frame| async move {
                    if delayed
                        && matches!(
                            &frame,
                            Ok(wire::GetObjectResponse {
                                frame: Some(wire::get_object_response::Frame::Body(_))
                            })
                        )
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                    frame
                })
                .boxed(),
        ))
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
async fn tls_grpc_exercises_every_rpc_streaming_authentication_bounds_and_semantic_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let pem = certified.cert.pem();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let fixture = Fixture(MemoryObjects::new(MemoryOptions::default())?);
    let identity = Identity::from_pem(pem.clone(), certified.signing_key.serialize_pem());
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
    let bucket = wire::BucketRef {
        name: "namespace.inputs".into(),
    };
    client
        .create_bucket(wire::CreateBucketRequest {
            name: bucket.name.clone(),
            mutation: None,
        })
        .await?;
    let channel = tonic::transport::Endpoint::from_shared(endpoint.clone())?
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(pem.clone())),
        )?
        .connect()
        .await?;
    let mut obsolete = tonic::client::Grpc::new(channel);
    obsolete.ready().await?;
    let mut request = Request::new(wire::HeadBucketRequest {
        bucket: Some(bucket.clone()),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer exact-token".parse()?);
    let result: Result<Response<wire::Bucket>, Status> = obsolete
        .unary(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/acyclic.objects.v2.BucketsService/HeadBucket",
            ),
            tonic_prost::ProstCodec::default(),
        )
        .await;
    assert_eq!(
        result.err().map(|error| error.code()),
        Some(tonic::Code::Unimplemented)
    );
    client
        .delete_bucket(wire::DeleteBucketRequest {
            bucket: Some(bucket),
            mutation: None,
        })
        .await?;
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
    exercise_transfer_deadlines(&client).await?;
    let _ = shutdown_tx.send(());
    server.await?.map_err(Into::into)
}

async fn exercise_transfer_deadlines(
    client: &GrpcObjects,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::time::Duration;
    assert!(
        client
            .clone()
            .with_transfer_timeout(Duration::ZERO)
            .is_err()
    );
    assert!(
        client
            .clone()
            .with_transfer_timeout(Duration::from_secs(1801))
            .is_err()
    );
    let short = client
        .clone()
        .with_transfer_timeout(Duration::from_millis(80))?;
    // The fixture delays this metadata reply longer than the transfer budget.
    // It must still succeed using the independent ordinary request deadline.
    let bucket = short
        .create_bucket(wire::CreateBucketRequest {
            name: "deadline.metadata".into(),
            mutation: None,
        })
        .await?;
    let bucket = bucket.bucket.ok_or("missing deadline bucket")?;
    let long = client
        .clone()
        .with_transfer_timeout(Duration::from_secs(2))?;
    let header = |key: &str| wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        mutation: None,
        ..Default::default()
    };
    let delayed_upload = || {
        stream::once(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(Bytes::from_static(b"complete-source"))
        })
        .boxed()
    };
    let ack = long
        .put_stream(header("delayed-upload"), delayed_upload())
        .await?;
    assert_eq!(ack.size, 15);
    // This crosses the old channel's fixed 30-second ceiling, using the public
    // client and the same authenticated TLS RPC rather than a mock channel.
    let extended = client
        .clone()
        .with_transfer_timeout(Duration::from_secs(45))?;
    let ack = extended
        .put_stream(
            header("beyond-default-budget"),
            stream::once(async {
                tokio::time::sleep(Duration::from_secs(31)).await;
                Ok(Bytes::from_static(b"complete-source"))
            })
            .boxed(),
        )
        .await?;
    assert_eq!(ack.size, 15);
    assert_eq!(
        short
            .put_stream(header("cancelled-upload"), delayed_upload())
            .await
            .err()
            .ok_or("short transfer did not refuse delayed upload")?
            .code,
        wire::ErrorCode::Unavailable
    );
    assert_eq!(
        long.head(wire::HeadObjectRequest {
            bucket: Some(bucket.clone()),
            object_key: "cancelled-upload".into(),
            ..Default::default()
        })
        .await
        .err()
        .ok_or("cancelled upload published an object")?
        .code,
        wire::ErrorCode::NotFound
    );
    exercise_multipart_deadlines(client, &bucket).await?;
    exercise_download_deadline(client, bucket).await
}

async fn exercise_multipart_deadlines(
    client: &GrpcObjects,
    bucket: &wire::BucketRef,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::time::Duration;
    let extended = client
        .clone()
        .with_transfer_timeout(Duration::from_secs(45))?;
    let short = client
        .clone()
        .with_transfer_timeout(Duration::from_millis(80))?;
    let upload = extended
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "multipart-deadlines".into(),
            ..Default::default()
        })
        .await?;
    let header = |part_number| wire::UploadPartHeader {
        bucket: Some(bucket.clone()),
        object_key: "multipart-deadlines".into(),
        upload_id: upload.upload_id.clone(),
        part_number,
        mutation: None,
    };
    // Multipart has a separate authenticated request path. Cross the ordinary
    // 30-second metadata ceiling through the real TLS part-upload RPC too.
    let part = extended
        .upload_part_stream(
            header(1),
            stream::once(async {
                tokio::time::sleep(Duration::from_secs(31)).await;
                Ok(Bytes::from_static(b"part"))
            })
            .boxed(),
        )
        .await?;
    assert_eq!(part.size, 4);
    let incomplete = stream::iter([Ok(Bytes::from_static(b"partial"))])
        .chain(stream::once(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            Ok(Bytes::from_static(b"tail"))
        }))
        .boxed();
    assert_eq!(
        short
            .upload_part_stream(header(2), incomplete)
            .await
            .err()
            .ok_or("short multipart deadline did not cancel the part")?
            .code,
        wire::ErrorCode::Unavailable
    );
    // Even after the delayed source could have completed, only the exact
    // acknowledged first part is staged; partial input cannot publish part 2.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let parts = extended
        .list_parts(wire::ListPartsRequest {
            bucket: Some(bucket.clone()),
            object_key: "multipart-deadlines".into(),
            upload_id: upload.upload_id.clone(),
            ..Default::default()
        })
        .await?;
    assert_eq!(parts.parts, vec![part]);
    extended
        .abort_multipart(wire::AbortMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "multipart-deadlines".into(),
            upload_id: upload.upload_id,
            mutation: None,
        })
        .await?;
    Ok(())
}

async fn exercise_download_deadline(
    client: &GrpcObjects,
    bucket: wire::BucketRef,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::time::Duration;
    let long = client
        .clone()
        .with_transfer_timeout(Duration::from_secs(2))?;
    // Three body frames each arrive within 250ms. A 400ms *total* deadline
    // accepts the first frame and refuses the second; it cannot reset per frame.
    let bytes = Bytes::from(vec![41; 2 * 65536 + 1]);
    let info = long
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: "delayed-body".into(),
                mutation: None,
                ..Default::default()
            },
            bytes.clone(),
        )
        .await?;
    let query = || wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: "delayed-body".into(),
        ..Default::default()
    };
    let complete = long.get(query(), bytes.len() as u64).await?;
    assert_eq!(complete.body, bytes);
    let bounded = client
        .clone()
        .with_transfer_timeout(Duration::from_millis(400))?;
    let mut download = bounded.get_stream(query(), info.size).await?;
    assert_eq!(download.header.object.as_ref(), Some(&info));
    assert_eq!(
        download
            .body
            .next()
            .await
            .ok_or("missing first frame")??
            .len(),
        65536
    );
    assert_eq!(
        download
            .body
            .next()
            .await
            .ok_or("missing deadline refusal")?
            .err()
            .ok_or("download exceeded its total deadline")?
            .code,
        wire::ErrorCode::Unavailable
    );
    Ok(())
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "TLS admission failures and all RPCs share one authority"
)]
async fn mtls_tls13_grpc_exercises_every_rpc_and_rejects_invalid_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use futures::StreamExt;
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    };
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
    use tokio_rustls::{TlsAcceptor, rustls};

    let server_identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let server_pem = server_identity.cert.pem();
    let server_key = server_identity.signing_key.serialize_pem();
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca_params, ca_key);
    let client_key = KeyPair::generate()?;
    let mut client_params = CertificateParams::new(Vec::<String>::new())?;
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = client_params.signed_by(&client_key, &issuer)?;
    let client_pem = client_cert.pem();
    let client_key_pem = client_key.serialize_pem();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ca_cert.der().clone())?;
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        roots.into(),
        rustls::crypto::ring::default_provider().into(),
    )
    .build()?;
    let mut config = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_protocol_versions(&[&rustls::version::TLS13])?
    .with_client_cert_verifier(verifier)
    .with_single_cert(
        CertificateDer::pem_slice_iter(server_pem.as_bytes()).collect::<Result<Vec<_>, _>>()?,
        PrivateKeyDer::from_pem_slice(server_key.as_bytes())?,
    )?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(std::sync::Arc::new(config));
    let expected_leaf = client_cert.der().clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let incoming = TcpListenerStream::new(listener)
        .then(move |socket| {
            let acceptor = acceptor.clone();
            let expected_leaf = expected_leaf.clone();
            async move {
                let tls = acceptor.accept(socket?).await?;
                let session = tls.get_ref().1;
                assert_eq!(
                    session.protocol_version(),
                    Some(rustls::ProtocolVersion::TLSv1_3)
                );
                assert_eq!(session.alpn_protocol(), Some(b"h2".as_slice()));
                assert_eq!(
                    session.peer_certificates().and_then(|certs| certs.first()),
                    Some(&expected_leaf)
                );
                Ok::<_, std::io::Error>(tls)
            }
        })
        .filter_map(|result| async { result.ok().map(Ok::<_, std::io::Error>) });
    let fixture = Fixture(MemoryObjects::new(MemoryOptions::default())?);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(wire::buckets_service_server::BucketsServiceServer::new(
                fixture.clone(),
            ))
            .add_service(wire::objects_service_server::ObjectsServiceServer::new(
                fixture.clone(),
            ))
            .add_service(wire::multipart_service_server::MultipartServiceServer::new(
                fixture,
            ))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let ca = Some(server_pem.as_bytes());
    // TLS 1.3 peers can report an admission alert on the first RPC rather than connect.
    let missing = GrpcObjects::connect(&endpoint, "exact-token", ca).await;
    assert_transport_denied(missing).await;
    let untrusted = rcgen::generate_simple_self_signed(["untrusted".to_owned()])?;
    let wrong = GrpcObjects::connect_with_identity(
        &endpoint,
        "exact-token",
        ca,
        untrusted.cert.pem().as_bytes(),
        untrusted.signing_key.serialize_pem().as_bytes(),
    )
    .await;
    assert_transport_denied(wrong).await;
    for (cert, key) in [
        (&b""[..], client_key_pem.as_bytes()),
        (client_pem.as_bytes(), &b""[..]),
        (&b"invalid certificate"[..], client_key_pem.as_bytes()),
        (client_pem.as_bytes(), &b"invalid private key"[..]),
        (client_pem.as_bytes(), server_key.as_bytes()),
    ] {
        assert!(
            GrpcObjects::connect_with_identity(&endpoint, "exact-token", ca, cert, key)
                .await
                .is_err()
        );
    }
    let oversized = vec![b'a'; 64 * 1024 + 1];
    for (cert, key) in [
        (oversized.as_slice(), client_key_pem.as_bytes()),
        (client_pem.as_bytes(), oversized.as_slice()),
    ] {
        assert!(matches!(
            GrpcObjects::connect_with_identity(&endpoint, "exact-token", ca, cert, key).await,
            Err(super::grpc::ConnectError::InvalidConfiguration)
        ));
    }
    assert!(
        GrpcObjects::connect_with_identity(
            &endpoint,
            "exact-token",
            None,
            client_pem.as_bytes(),
            client_key_pem.as_bytes(),
        )
        .await
        .is_err()
    );
    let unauthorized = GrpcObjects::connect_with_identity(
        &endpoint,
        "wrong",
        ca,
        client_pem.as_bytes(),
        client_key_pem.as_bytes(),
    )
    .await?;
    assert_eq!(
        unauthorized
            .create_bucket(wire::CreateBucketRequest {
                name: "customer.inputs".into(),
                mutation: None,
            })
            .await
            .err()
            .map(|error| error.code),
        Some(wire::ErrorCode::AccessDenied)
    );
    let client = GrpcObjects::connect_with_identity(
        &endpoint,
        "exact-token",
        ca,
        client_pem.as_bytes(),
        client_key_pem.as_bytes(),
    )
    .await?;
    super::conformance::verify(&client, "conformance-mtls").await?;
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
    server.await?.map_err(Into::into)
}

async fn assert_transport_denied(client: Result<GrpcObjects, super::grpc::ConnectError>) {
    if let Ok(client) = client {
        assert!(
            client
                .head_bucket(wire::HeadBucketRequest {
                    bucket: Some(wire::BucketRef {
                        name: "customer.inputs".into()
                    }),
                })
                .await
                .is_err(),
            "unauthenticated TLS peer reached Objects"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one TLS fixture checks every streaming trace terminal transition"
)]
async fn grpc_download_trace_requires_validated_eof_and_records_cancellation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::body::tests::Capture;
    use futures::StreamExt;
    use tracing_subscriber::prelude::*;

    let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let pem = certified.cert.pem();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let fixture = Fixture(MemoryObjects::new(MemoryOptions::default())?);
    let identity = Identity::from_pem(pem.clone(), certified.signing_key.serialize_pem());
    let (shutdown, receive) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(wire::buckets_service_server::BucketsServiceServer::new(
                fixture.clone(),
            ))
            .add_service(wire::objects_service_server::ObjectsServiceServer::new(
                fixture,
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = receive.await;
            })
            .await
    });
    let client = GrpcObjects::connect(&endpoint, "exact-token", Some(pem.as_bytes())).await?;
    let bucket = Some(wire::BucketRef {
        name: "customer.inputs".into(),
    });
    client
        .create_bucket(wire::CreateBucketRequest {
            name: "customer.inputs".into(),
            mutation: None,
        })
        .await?;
    for key in [
        "ordinary",
        "malformed",
        "terminal-error",
        "corrupt-codec",
        "truncated",
    ] {
        client
            .put(
                wire::PutObjectHeader {
                    bucket: bucket.clone(),
                    object_key: key.into(),
                    ..Default::default()
                },
                Bytes::from(vec![b'x'; 128 * 1024]),
            )
            .await?;
    }
    let capture = Capture::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(capture.clone()));
    let query = |key: &str| wire::GetObjectRequest {
        bucket: bucket.clone(),
        object_key: key.into(),
        ..Default::default()
    };
    let span_name = "acyclic.objects.grpc.get";
    let mut download = client.get_stream(query("ordinary"), 512 * 1024).await?;
    assert!(capture.spans(span_name).is_empty());
    for _ in 0..2 {
        assert_eq!(
            download
                .body
                .next()
                .await
                .transpose()?
                .map(|bytes| bytes.len()),
            Some(65536)
        );
        assert!(capture.spans(span_name).is_empty());
    }
    assert!(download.body.next().await.is_none());
    let spans = capture.spans(span_name);
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans
            .first()
            .ok_or("missing completed span")?
            .fields
            .get("outcome")
            .map(String::as_str),
        Some("ok")
    );
    assert!(
        !spans
            .first()
            .ok_or("missing completed span")?
            .fields
            .contains_key("error.kind")
    );
    drop(download);

    for consumed in [0, 1, 2] {
        let before = capture.spans(span_name).len();
        let mut download = client.get_stream(query("ordinary"), 512 * 1024).await?;
        for _ in 0..consumed {
            assert!(download.body.next().await.transpose()?.is_some());
        }
        assert_eq!(capture.spans(span_name).len(), before);
        drop(download);
        let spans = capture.spans(span_name);
        assert_eq!(spans.len(), before + 1);
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("outcome")
                .map(String::as_str),
            Some("err")
        );
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("error.kind")
                .map(String::as_str),
            Some("cancelled")
        );
    }
    for key in ["malformed", "terminal-error", "corrupt-codec", "truncated"] {
        let before = capture.spans(span_name).len();
        let mut download = client.get_stream(query(key), 512 * 1024).await?;
        assert_eq!(capture.spans(span_name).len(), before);
        let error = loop {
            match download.body.next().await {
                Some(Ok(_)) => {}
                Some(Err(error)) => break error,
                None => return Err(format!("accepted invalid body: {key}").into()),
            }
        };
        assert_eq!(capture.spans(span_name).len(), before + 1);
        drop(download);
        let spans = capture.spans(span_name);
        assert_eq!(spans.len(), before + 1);
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("outcome")
                .map(String::as_str),
            Some("err")
        );
        assert_eq!(
            spans
                .get(before)
                .ok_or("missing terminal span")?
                .fields
                .get("error.kind")
                .map(String::as_str),
            Some(error.code.as_str_name())
        );
    }
    assert!(capture.spans(span_name).iter().all(|span| {
        span.fields
            .keys()
            .all(|key| matches!(key.as_str(), "rpc.code" | "outcome" | "error.kind"))
    }));
    let _ = shutdown.send(());
    server.await?.map_err(Into::into)
}
