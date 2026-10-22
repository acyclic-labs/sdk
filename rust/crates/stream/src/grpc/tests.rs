use super::*;
use crate::{MemoryStream, OwnedBytes};
use std::sync::atomic::{AtomicUsize, Ordering};
use wire::stream_service_server::{StreamService, StreamServiceServer};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) mod retry_capture;

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

pub(crate) fn client<P: StreamProvider>(provider: P) -> Result<Client, ConnectError> {
    let provider = Arc::new(provider);
    let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
        tower::service_fn(move |_| {
            let provider = Arc::clone(&provider);
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let incoming = stream::once(async { Ok::<_, std::io::Error>(server) });
                    let _ = tonic::transport::Server::builder()
                        .add_service(StreamServiceServer::new(Service::new(Arc::clone(
                            &provider,
                        ))))
                        .add_service(
                            wire::stream_session_service_server::StreamSessionServiceServer::new(
                                crate::session::Service::new(
                                    provider,
                                    "fixture".into(),
                                    crate::session::Limits::default(),
                                ),
                            ),
                        )
                        .serve_with_incoming(incoming)
                        .await;
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        }),
    );
    Ok(Client::from_channel(channel, authorization("fixture")?))
}

fn unconnected() -> Result<Client, ConnectError> {
    Ok(Client::from_channel(
        Endpoint::from_static("http://fixture.invalid").connect_lazy(),
        authorization("fixture")?,
    ))
}

/// Ends its first follow cleanly, then denies or delegates later follows.
pub(super) struct EndingFollow {
    pub(super) inner: MemoryStream,
    pub(super) follows: AtomicUsize,
    pub(super) deny: bool,
    pub(super) finite: bool,
}

#[async_trait]
impl StreamProvider for EndingFollow {
    fn operation_claims(&self) -> &crate::OperationClaims {
        self.inner.operation_claims()
    }

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
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        let attempt = self.follows.fetch_add(1, Ordering::SeqCst);
        if self.finite {
            return if attempt == 0 {
                self.inner
                    .read(ReadRequest {
                        path,
                        from,
                        limit: u32::try_from(crate::MAX_ITEMS)
                            .map_err(|_| StreamError::LimitExceeded)?,
                    })
                    .await
            } else {
                Ok(stream::empty().boxed())
            };
        }
        if attempt == 0 {
            return Ok(stream::empty().boxed());
        }
        if self.deny {
            return Err(StreamError::AccessDenied);
        }
        self.inner.follow(path, from, filter).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        if self.deny {
            return Err(StreamError::AccessDenied);
        }
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
            records: values.iter().cloned().map(Into::into).collect(),
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
        values.iter().skip(2).take(17).cloned().collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn service_rejects_oversized_commands_before_provider_dispatch() {
    let service = Service::new(Arc::new(MemoryStream::default()));
    let request = wire::AppendRequest {
        path: "x".repeat(crate::MAX_PATH_BYTES),
        records: vec![
            wire::AppendRecord {
                value: Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]),
                keys: vec![],
                attributes: vec![]
            };
            16
        ],
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
        finite: false,
    };
    let path = StreamPath::new("accounts/events")?;
    provider
        .inner
        .append(AppendRequest {
            path: path.clone(),
            records: vec![(OwnedBytes::from_static(b"resumed")).into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let transport = client(provider)?;
    let mut records = transport
        .follow(path, 0, crate::FollowFilter::default())
        .await?;
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
        finite: false,
    })?;
    let items = tokio::time::timeout(
        Duration::from_secs(1),
        transport
            .follow(
                StreamPath::new("accounts/events")?,
                0,
                crate::FollowFilter::default(),
            )
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
    if retry_capture::isolate("retry_attempts_record_independent_spans")? {
        return Ok(());
    }
    use tracing::instrument::WithSubscriber as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let (_registration, subscriber) = retry_capture::dispatch(captured.clone());
    async {
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
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .with_subscriber(subscriber)
    .await?;
    let attempts = captured.all("acyclic.stream.grpc.call");
    assert_eq!(attempts.len(), 2);
    for (attempt, (number, outcome, code)) in
        attempts.iter().zip([("1", "err", "14"), ("2", "ok", "0")])
    {
        for (field, expected) in [
            ("attempt", number),
            ("outcome", outcome),
            ("rpc.code", code),
        ] {
            assert_eq!(attempt.get(field).map(String::as_str), Some(expected));
        }
    }
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

#[tokio::test]
async fn translating_client_does_not_ack_before_downstream_delivery() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    provider
        .append(AppendRequest {
            path: StreamPath::new("accounts/events")?,
            records: vec![(OwnedBytes::from(b"delivered".to_vec())).into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let mut service = Service::new(provider);
    service.test_delivery = Some((Bytes::from(vec![9; 32]), Arc::clone(&acknowledgements)));
    let direct = Client::from_channel(channel(service), authorization("fixture")?);
    let translating = direct.clone().without_delivery_acknowledgements();
    let request = ReadRequest {
        path: StreamPath::new("accounts/events")?,
        from: 0,
        limit: 1,
    };
    let record = direct
        .read(request.clone())
        .await?
        .next()
        .await
        .ok_or("empty read")??;
    assert_eq!(record.value, OwnedBytes::from(b"delivered".to_vec()));
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 1);
    let record = translating
        .read(request)
        .await?
        .next()
        .await
        .ok_or("empty read")??;
    assert_eq!(record.value, OwnedBytes::from(b"delivered".to_vec()));
    assert_eq!(acknowledgements.load(Ordering::Relaxed), 1);
    assert_eq!(
        direct.acknowledge_delivery(Bytes::new()).await,
        Err(StreamError::InvalidArgument)
    );
    assert_eq!(
        client(MemoryStream::default())?
            .acknowledge_delivery(Bytes::from(vec![9; 32]))
            .await,
        Err(StreamError::Unsupported)
    );
    Ok(())
}

#[tokio::test]
async fn source_eof_never_discards_records_queued_before_terminal_close() -> TestResult {
    let provider = EndingFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        deny: false,
        finite: true,
    };
    let path = StreamPath::new("finite")?;
    provider
        .inner
        .append(AppendRequest {
            path: path.clone(),
            records: vec![(OwnedBytes::from_static(b"record")).into(); 20],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let client = client(provider)?;
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        client
            .follow(path, 0, crate::FollowFilter::default())
            .await?
            .collect::<Vec<_>>(),
    )
    .await?;
    assert_eq!(result.len(), 21);
    for (sequence, item) in result.iter().take(20).enumerate() {
        assert_eq!(
            item.as_ref().map(|record| record.sequence),
            Ok(u64::try_from(sequence)?)
        );
    }
    assert_eq!(result.last(), Some(&Err(StreamError::Unavailable)));
    Ok(())
}

#[tokio::test]
async fn concurrent_retry_captures_keep_their_own_dispatcher() -> TestResult {
    if retry_capture::isolate("concurrent_retry_captures_keep_their_own_dispatcher")? {
        return Ok(());
    }
    use tracing::instrument::WithSubscriber as _;
    let first = acyclic_grpc_observability::tests::Capture::default();
    let second = acyclic_grpc_observability::tests::Capture::default();
    let capture = |captured: acyclic_grpc_observability::tests::Capture| async move {
        let (_registration, subscriber) = retry_capture::dispatch(captured);
        async {
            let client = unconnected()?;
            let mut first = true;
            let response = client
                .unary(ATTEMPT_TIMEOUT, (), |_, _| {
                    let result = if std::mem::take(&mut first) {
                        Err(Status::unavailable("retry"))
                    } else {
                        Ok(Response::new(()))
                    };
                    Box::pin(async move {
                        tokio::task::yield_now().await;
                        result
                    })
                })
                .await?;
            decode_response(response, Ok)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
        .with_subscriber(subscriber)
        .await
    };
    let (one, two) = tokio::join!(capture(first.clone()), capture(second.clone()));
    one?;
    two?;
    for capture in [first, second] {
        let attempts = capture.all("acyclic.stream.grpc.call");
        let [first, second] = attempts.as_slice() else {
            panic!("expected two attempts, found {}", attempts.len());
        };
        assert_eq!(first.get("attempt").map(String::as_str), Some("1"));
        assert_eq!(second.get("attempt").map(String::as_str), Some("2"));
    }
    Ok(())
}

proptest::proptest! {
    #[test]
    fn canonical_status_round_trip_preserves_every_stream_error(
        index in 0usize..StreamError::ALL.len(),
        context in ".{0,64}",
    ) {
        let error = StreamError::ALL
            .get(index)
            .ok_or_else(|| proptest::test_runner::TestCaseError::fail("no such error"))?;
        let wire = error_status(error);
        proptest::prop_assert_eq!(status(&wire), error.clone());
        // Only the contract's discriminating messages carry extra meaning.
        // Diagnostics for unambiguous status classes cannot change their code.
        if matches!(wire.code(), Code::NotFound | Code::AlreadyExists | Code::OutOfRange
            | Code::PermissionDenied | Code::ResourceExhausted | Code::Unavailable) {
            let annotated = Status::new(wire.code(), context);
            proptest::prop_assert_eq!(status(&annotated), error.clone());
        }
    }
}
