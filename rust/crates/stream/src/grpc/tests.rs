use super::*;
use crate::{MemoryStream, OwnedBytes, wire_codec};
use futures::TryFutureExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use wire::stream_service_server::{StreamService, StreamServiceServer};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) mod retry_capture;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TestClientError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Stream(#[from] StreamError),
}

pub(crate) fn client<P: StreamProvider>(provider: P) -> Result<Client, TestClientError> {
    client_with_interceptor(provider, Ok::<Request<()>, Status>)
}

fn client_with_interceptor<P, I>(provider: P, interceptor: I) -> Result<Client, TestClientError>
where
    P: StreamProvider,
    I: tonic::service::Interceptor + Clone + Send + Sync + 'static,
{
    let provider = Arc::new(provider);
    let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
        tower::service_fn(move |_| {
            let provider = Arc::clone(&provider);
            let interceptor = interceptor.clone();
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let incoming = stream::once(async { Ok::<_, std::io::Error>(server) });
                    let _ = tonic::transport::Server::builder()
                        .add_service(StreamServiceServer::with_interceptor(
                            Service::new(Arc::clone(&provider)),
                            interceptor,
                        ))
                        .add_service(
                            wire::stream_session_service_server::StreamSessionServiceServer::new(
                                crate::session::Service::new(
                                    provider,
                                    crate::session::testing::authority(),
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
    let client = Client::from_channel(channel, authorization("fixture")?);
    client
        .session_client()?
        .register_credential(0, crate::session::testing::credential())?;
    Ok(client)
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
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(self.inner.context().operation_claims())
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        self.inner.heads(request).await
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        self.inner.write(request)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        let attempt = self.follows.fetch_add(1, Ordering::SeqCst);
        if self.finite {
            return if attempt == 0 {
                self.inner
                    .read(ReadRequest {
                        key: None,
                        path: request.path,
                        from: request.from,
                        limit: u32::try_from(crate::MAX_ITEMS)
                            .map_err(|_| StreamError::LimitExceeded)?,
                        at_least: Default::default(),
                    })
                    .await
                    .map(|records| {
                        records
                            .map(|record| record.map(crate::FollowUpdate::Record))
                            .boxed()
                    })
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
        self.inner.follow(request).await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}

#[tokio::test]
async fn grpc_heads_refuse_an_oversized_view_without_poisoning_the_session() -> TestResult {
    let provider = MemoryStream::default();
    let mut request = crate::head_tests::large_retained_view(&provider).await?;
    let transport = client(provider)?;
    assert_eq!(
        transport.heads(request.clone()).await,
        Err(StreamError::LimitExceeded)
    );
    request.retained.truncate(1);
    request.limit = 1;
    transport
        .heads(request.clone())
        .await?
        .validate_for(&request)?;
    Ok(())
}

#[tokio::test]
async fn grpc_minimum_cut_refuses_both_incarnations_without_losing_connection_progress()
-> TestResult {
    let provider = MemoryStream::default();
    let path = StreamPath::new("minimum-incarnation")?;
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![OwnedBytes::from_static(b"original").into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let current = provider.wait_applied(&Default::default()).await?;
    let foreign = crate::Revision {
        incarnation: current.incarnation ^ 1,
        ..current.clone()
    };
    let mut transport =
        wire::stream_service_client::StreamServiceClient::new(client(provider)?.channel);
    let tail = |revisions: &[crate::Revision]| wire::TailRequest {
        path: path.to_string(),
        at_least: revisions.iter().map(wire_codec::revision_to_wire).collect(),
        ..Default::default()
    };
    for revisions in [
        [foreign.clone(), current.clone()],
        [current.clone(), foreign],
    ] {
        let error = transport.tail(tail(&revisions)).await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition);
        assert_eq!(error.message(), "revision_foreign");
    }
    let earlier = crate::Revision {
        index: 0,
        ..current.clone()
    };
    let response = transport
        .tail(tail(&[current.clone(), earlier]))
        .await?
        .into_inner();
    assert_eq!(response.tail, 1);
    assert_eq!(wire_codec::issued_from_wire(response.applied)?, current);
    Ok(())
}

#[tokio::test]
async fn grpc_transport_passes_the_public_suite() -> TestResult {
    let transport = client(MemoryStream::default())?;
    crate::conformance::verify(&crate::conformance::Fixture::new(transport.clone())).await?;
    let path = StreamPath::new("accounts/many")?;
    let values = (0..20_u8)
        .map(|value| OwnedBytes::from(vec![value; 64]))
        .collect::<Vec<_>>();
    transport
        .append(AppendRequest {
            path: path.clone(),
            records: values.iter().cloned().map(Into::into).collect(),
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let records = transport
        .read(ReadRequest {
            key: None,
            path,
            from: 2,
            limit: 17,
            at_least: Default::default(),
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
        idempotency_key: Bytes::new(),
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
    use futures::FutureExt as _;
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
    assert!(listener.accept().now_or_never().is_none());
    // The same listener can observe a real connection immediately after the
    // rejected configuration futures have settled.
    let _peer = tokio::net::TcpStream::connect(listener.local_addr()?).await?;
    let _accepted = listener.accept().await?;
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
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let transport = client(provider)?;
    let mut records = transport
        .follow(crate::FollowRequest::new(path, 0))
        .map_ok(crate::records_only)
        .await?;
    let record = records.next().await.ok_or("follow ended")??;
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
    let items = transport
        .follow(crate::FollowRequest::new(
            StreamPath::new("accounts/events")?,
            0,
        ))
        .map_ok(crate::records_only)
        .await?
        .collect::<Vec<_>>()
        .await;
    assert_eq!(items, vec![Err(StreamError::AccessDenied)]);
    Ok(())
}

async fn assert_unary_retry(error: Status, retry: bool) -> TestResult {
    let client = unconnected()?;
    let body = Bytes::from_static(b"stable-key-and-body");
    let mut attempts = Vec::new();
    let mut first = Some(error);
    let result = client
        .unary(body.clone(), |_, request| {
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

/// Captured from Hyper's actual HTTP/2 dispatch after its driver disappears.
pub(crate) async fn closed_transport() -> TestResult<Status> {
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
    Ok(Status::from_error(Box::new(error)))
}

#[tokio::test]
async fn transport_loss_retries_the_same_request_but_peer_errors_do_not() -> TestResult {
    assert_unary_retry(closed_transport().await?, true).await?;
    assert_unary_retry(Status::unknown("peer outcome unknown"), false).await?;
    assert_unary_retry(Status::unavailable("restarting"), false).await?;
    assert_unary_retry(Status::cancelled("application cancellation"), false).await?;
    assert_unary_retry(Status::deadline_exceeded("caller deadline"), false).await?;
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

#[tokio::test]
async fn application_unavailability_returns_without_retry_and_preserves_a_caller_resend()
-> TestResult {
    let attempts = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&attempts);
    let client = client_with_interceptor(MemoryStream::default(), move |request: Request<()>| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(Status::unavailable("application refusal"))
        } else {
            Ok(request)
        }
    })?;
    let key = IdempotencyKey::unique();
    assert_eq!(
        client.inspect_idempotency(key.clone()).await,
        Err(StreamError::Unavailable)
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    // The same live server accepts the caller's next request. An automatic
    // readiness retry would consume this response before returning the error.
    assert_eq!(client.inspect_idempotency(key).await, Ok(None));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn caller_cancellation_drops_an_in_flight_request() -> TestResult {
    use futures::FutureExt as _;
    let client = unconnected()?;
    let (dropped, mut completed) = tokio::sync::oneshot::channel::<()>();
    let mut dropped = Some(dropped);
    let call = client.unary((), |_, _| {
        let dropped = dropped.take().expect("one physical request");
        Box::pin(async move {
            let _dropped = dropped;
            futures::future::pending::<Result<Response<()>, Status>>().await
        })
    });
    let mut call = Box::pin(call);
    assert!(call.as_mut().now_or_never().is_none());
    assert!(
        matches!(
            completed.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "request must remain live before cancellation"
    );
    drop(call);
    assert!(
        completed.await.is_err(),
        "cancellation must drop the request"
    );
    Ok(())
}

#[tokio::test]
async fn retry_attempts_record_independent_spans() -> TestResult {
    if retry_capture::isolate("retry_attempts_record_independent_spans")? {
        return Ok(());
    }
    use tracing::instrument::WithSubscriber as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let subscriber = retry_capture::dispatch(captured.clone());
    let transport_error = closed_transport().await?;
    let retry_code = (transport_error.code() as u64).to_string();
    async {
        let client = unconnected()?;
        let mut first = true;
        let response = client
            .unary((), |_, _| {
                let result = if std::mem::take(&mut first) {
                    Err(transport_error.clone())
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
    for (attempt, (number, outcome, code)) in attempts
        .iter()
        .zip([("1", "err", retry_code.as_str()), ("2", "ok", "0")])
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
                platform?
                    .inspect_idempotency(IdempotencyKey::unique())
                    .await,
                Ok(None)
            );
        } else {
            assert!(platform.is_err(), "private endpoint unexpectedly trusted");
        }
        let pinned =
            Client::connect_eager_with_ca_certificate(&endpoint, "exact-token", pem.as_bytes())
                .await?;
        assert_eq!(
            pinned.inspect_idempotency(IdempotencyKey::unique()).await,
            Ok(None)
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
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let client = client(provider)?;
    let result = client
        .follow(crate::FollowRequest::new(path, 0))
        .map_ok(crate::records_only)
        .await?
        .collect::<Vec<_>>()
        .await;
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
        let subscriber = retry_capture::dispatch(captured);
        let transport_error = closed_transport().await?;
        async {
            let client = unconnected()?;
            let mut first = true;
            let response = client
                .unary((), |_, _| {
                    let result = if std::mem::take(&mut first) {
                        Err(transport_error.clone())
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
