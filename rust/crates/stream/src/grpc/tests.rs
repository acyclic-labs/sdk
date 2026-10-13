use super::*;
use crate::{MemoryStream, OwnedBytes};
use std::sync::atomic::{AtomicUsize, Ordering};
use wire::stream_service_server::{StreamService, StreamServiceServer};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// An in-process HTTP/2 channel to one service.
fn channel<S: StreamService + Clone>(service: S) -> Channel {
    Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(tower::service_fn(
        move |_| {
            let service = service.clone();
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let incoming = stream::once(async { Ok::<_, std::io::Error>(server) });
                    let _ = tonic::transport::Server::builder()
                        .add_service(StreamServiceServer::new(service))
                        .serve_with_incoming(incoming)
                        .await;
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        },
    ))
}

fn client<P: StreamProvider>(provider: P) -> Result<Client, ConnectError> {
    Ok(Client::from_channel(
        channel(Service::new(Arc::new(provider))),
        authorization("fixture")?,
    ))
}

fn unconnected() -> Result<Client, ConnectError> {
    Ok(Client::from_channel(
        Endpoint::from_static("http://fixture.invalid").connect_lazy(),
        authorization("fixture")?,
    ))
}

/// Ends its first follow cleanly, then denies or delegates later follows.
struct EndingFollow {
    inner: MemoryStream,
    follows: AtomicUsize,
    deny: bool,
}

#[async_trait]
impl StreamProvider for EndingFollow {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        if self.follows.fetch_add(1, Ordering::SeqCst) == 0 {
            return Ok(stream::empty().boxed());
        }
        if self.deny {
            return Err(StreamError::AccessDenied);
        }
        self.inner.follow(path, from).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.inner.commit(request).await
    }
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}

#[tokio::test]
async fn grpc_transport_passes_the_public_suite() -> TestResult {
    let transport = client(MemoryStream::default())?;
    crate::conformance::verify(&transport).await?;
    let path = StreamPath::new("accounts/many")?;
    let values = (0..20_u8)
        .map(|value| OwnedBytes::from(vec![value; 64]))
        .collect::<Vec<_>>();
    transport
        .append(AppendRequest {
            path: path.clone(),
            records: values.clone(),
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let records = transport
        .read(ReadRequest {
            path,
            from: 2,
            limit: 17,
        })
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        records
            .into_iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        values[2..19].to_vec()
    );
    Ok(())
}

#[tokio::test]
async fn service_rejects_oversized_commands_before_provider_dispatch() {
    let service = Service::new(Arc::new(MemoryStream::default()));
    let request = wire::AppendRequest {
        path: "x".repeat(crate::MAX_PATH_BYTES),
        records: vec![Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]); 16],
        if_tail: None,
        idempotency_key: None,
    };
    let error = service.append(Request::new(request)).await.err();
    assert_eq!(
        error.as_ref().map(Status::code),
        Some(Code::InvalidArgument)
    );
    assert_eq!(error.as_ref().map(Status::message), Some("limit_exceeded"));
}

#[tokio::test]
async fn invalid_configuration_is_rejected_before_network_io() -> TestResult {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let long = "t".repeat(MAX_BEARER_TOKEN_BYTES + 1);
    for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
        assert!(matches!(
            Client::connect_eager(&endpoint, token).await,
            Err(ConnectError::InvalidCredential)
        ));
    }
    for pem in [Vec::new(), vec![b'x'; MAX_CA_CERTIFICATE_BYTES + 1]] {
        assert!(matches!(
            Client::connect_eager_with_ca_certificate(&endpoint, "fixture", pem).await,
            Err(ConnectError::InvalidCaCertificate)
        ));
    }
    assert!(matches!(
        Client::connect_eager("http://127.0.0.1:1", "fixture").await,
        Err(ConnectError::InsecureEndpoint)
    ));
    let accepted = tokio::time::timeout(Duration::from_millis(100), listener.accept()).await;
    assert!(accepted.is_err());
    Ok(())
}

#[tokio::test]
async fn follow_resumes_after_a_clean_end_of_stream() -> TestResult {
    let provider = EndingFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        deny: false,
    };
    let path = StreamPath::new("accounts/events")?;
    provider
        .inner
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"resumed")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let transport = client(provider)?;
    let mut records = transport.follow(path, 0).await?;
    let record = tokio::time::timeout(Duration::from_secs(1), records.next())
        .await?
        .ok_or("follow ended")??;
    assert_eq!(record.value.as_slice(), b"resumed");
    Ok(())
}

#[tokio::test]
async fn a_denied_follow_reports_access_denied_once_and_then_ends() -> TestResult {
    let transport = client(EndingFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        deny: true,
    })?;
    let items = tokio::time::timeout(
        Duration::from_secs(1),
        transport
            .follow(StreamPath::new("accounts/events")?, 0)
            .await?
            .collect::<Vec<_>>(),
    )
    .await?;
    assert_eq!(items, vec![Err(StreamError::AccessDenied)]);
    Ok(())
}

async fn assert_unary_retry(error: Status, retry: bool) -> TestResult {
    let client = unconnected()?;
    let body = Bytes::from_static(b"stable-key-and-body");
    let mut attempts = Vec::new();
    let mut first = Some(error);
    let result = client
        .unary(ATTEMPT_TIMEOUT, body.clone(), |_, request| {
            attempts.push(request.into_inner());
            let result = first.take().map_or_else(|| Ok(Response::new(())), Err);
            Box::pin(async move { result })
        })
        .await
        .and_then(|response| decode_response(response, Ok));
    assert_eq!(result.is_ok(), retry);
    let expected = if retry { 2 } else { 1 };
    assert_eq!(attempts, vec![body; expected]);
    Ok(())
}

#[tokio::test]
async fn transport_loss_retries_the_same_request_but_peer_errors_do_not() -> TestResult {
    let (client_io, _peer) = tokio::io::duplex(4096);
    let (mut sender, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(client_io),
    )
    .await?;
    drop(connection);
    let request = tonic::codegen::http::Request::new(tonic::body::Body::empty());
    let error = sender
        .send_request(request)
        .await
        .err()
        .ok_or("closed connection succeeded")?;
    assert_unary_retry(Status::from_error(Box::new(error)), true).await?;
    assert_unary_retry(Status::unknown("peer outcome unknown"), true).await?;
    assert_unary_retry(Status::unavailable("restarting"), true).await?;
    assert_unary_retry(Status::cancelled("application cancellation"), false).await?;
    assert_unary_retry(Status::permission_denied("denied"), false).await?;

    for kind in [
        std::io::ErrorKind::UnexpectedEof,
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::BrokenPipe,
    ] {
        let mut error = Status::unknown("response body interrupted");
        error.set_source(Arc::new(std::io::Error::from(kind)));
        assert!(retryable(&error), "{kind:?}");
    }
    for error in [
        Status::internal("protocol error"),
        Status::data_loss("corrupt record"),
        Status::from_error(Box::new(std::io::Error::from(
            std::io::ErrorKind::InvalidData,
        ))),
    ] {
        assert!(!retryable(&error), "{error:?}");
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn persistent_unknown_expires_at_the_operation_deadline() -> TestResult {
    let client = unconnected()?;
    let started = tokio::time::Instant::now();
    let result: Result<(), StreamError> = client
        .unary(ATTEMPT_TIMEOUT, (), |_, _| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Err(Status::unknown("outcome unavailable"))
            })
        })
        .await
        .and_then(|response| decode_response(response, Ok));
    assert_eq!(result, Err(StreamError::Unavailable));
    assert_eq!(tokio::time::Instant::now() - started, OPERATION_DEADLINE);
    Ok(())
}

#[tokio::test]
async fn retry_attempts_record_independent_spans() -> TestResult {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let client = unconnected()?;
    let mut first = true;
    let response = client
        .unary(ATTEMPT_TIMEOUT, (), |_, _| {
            let result = if std::mem::take(&mut first) {
                Err(Status::unavailable("retry"))
            } else {
                Ok(Response::new(()))
            };
            Box::pin(async move { result })
        })
        .await?;
    decode_response(response, Ok)?;
    let attempts = captured.all("acyclic.stream.grpc.call");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        (
            attempts[0]["attempt"].as_str(),
            attempts[0]["outcome"].as_str(),
            attempts[0]["rpc.code"].as_str()
        ),
        ("1", "err", "14")
    );
    assert_eq!(
        (
            attempts[1]["attempt"].as_str(),
            attempts[1]["outcome"].as_str(),
            attempts[1]["rpc.code"].as_str()
        ),
        ("2", "ok", "0")
    );
    Ok(())
}

#[tokio::test]
async fn platform_roots_and_exclusive_ca() -> TestResult {
    let Some((pem, mut server, unrelated, mode)) =
        acyclic_grpc_observability::tests::tls_fixture(module_path!())?
    else {
        return Ok(());
    };
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("https://localhost:{}", incoming.local_addr()?.port());
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        server
            .add_service(StreamServiceServer::new(Service::new(Arc::new(
                MemoryStream::default(),
            ))))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let result = async {
        let platform = Client::connect_eager(&endpoint, "exact-token").await;
        if mode == "file" || mode == "dir" {
            assert_eq!(
                platform?.tail(StreamPath::new("platform/roots")?).await,
                Err(StreamError::NotFound)
            );
        } else {
            assert!(platform.is_err(), "private endpoint unexpectedly trusted");
        }
        let pinned =
            Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", pem.as_bytes())
                .await?;
        assert_eq!(
            pinned.tail(StreamPath::new("explicit/roots")?).await,
            Err(StreamError::NotFound)
        );
        let denied =
            Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", &unrelated).await;
        assert!(
            denied.is_err(),
            "unrelated supplied CA unexpectedly trusted"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let _ = shutdown.send(());
    server.await??;
    result
}
