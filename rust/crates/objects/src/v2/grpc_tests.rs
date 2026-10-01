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

#[tokio::test]
#[allow(clippy::too_many_lines)] // TLS admission failures and all RPCs share one authority.
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
    server.await??;
    Ok(())
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
