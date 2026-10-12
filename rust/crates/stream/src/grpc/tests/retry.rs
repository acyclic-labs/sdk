use super::*;

#[tokio::test]
async fn hung_operation_fails_over_without_truncating_a_healthy_slow_response()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = Arc::new(FiniteFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        tail_delay: std::time::Duration::from_millis(600),
        denial: None,
    });
    provider
        .inner
        .append(AppendRequest {
            path: StreamPath::new("accounts/events")?,
            records: vec![OwnedBytes::from_static(b"seed")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"slow-tail"))?),
        })
        .await?;
    let transport = Client::from_channels(Arc::from([stalled_channel(), provider_channel(Service::new(provider))]), Client::authorization("fixture")?);
    let tail = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        transport.tail(StreamPath::new("accounts/events")?),
    )
    .await??;
    assert_eq!(tail, 1);
    Ok(())
}

#[tokio::test]
async fn a_denied_follow_reports_access_denied_once_and_then_ends()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = Arc::new(FiniteFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        tail_delay: std::time::Duration::ZERO,
        denial: Some(2),
    });
    provider
        .inner
        .append(AppendRequest {
            path: StreamPath::new("accounts/events")?,
            records: vec![OwnedBytes::from_static(b"one"), OwnedBytes::from_static(b"two")],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let admissions = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&admissions);
    let service = StreamServiceServer::with_interceptor(
        Service::new(Arc::clone(&provider)),
        move |request: Request<()>| {
            observed.fetch_add(1, Ordering::Relaxed);
            if request
                .metadata()
                .get("authorization")
                .is_some_and(|value| value == "Bearer denied-open")
            {
                return Err(Status::permission_denied("fixture authorization denied"));
            }
            Ok(request)
        },
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let bound = std::time::Duration::from_secs(2);
    let result = tokio::time::timeout(bound, async {
        let channel = Endpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?;
        let mut sequences = Vec::new();
        for token in ["fixture", "denied-open"] {
            let client = Client::from_channels(Arc::from([channel.clone()]), Client::authorization(token)?);
            let items = client
                .follow(StreamPath::new("accounts/events")?, 0)
                .await?
                .collect::<Vec<_>>()
                .await;
            sequences.push(
                items
                    .into_iter()
                    .map(|item| item.map(|record| record.sequence))
                    .collect::<Vec<_>>(),
            );
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(sequences)
    })
    .await;
    let _ = shutdown_tx.send(());
    let stopped = tokio::time::timeout(bound, &mut server).await;
    if stopped.is_err() {
        server.abort();
        let _ = tokio::time::timeout(bound, &mut server).await;
    }
    let sequences = result??;
    stopped???;
    assert_eq!(
        sequences,
        vec![
            vec![Ok(0), Ok(1), Err(StreamError::AccessDenied)],
            vec![Err(StreamError::AccessDenied)],
        ]
    );
    assert_eq!(admissions.load(Ordering::Relaxed), 2);
    assert_eq!(provider.follows.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn clean_follow_eof_advances_once_to_the_next_endpoint()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ended = Arc::new(FiniteFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        tail_delay: std::time::Duration::ZERO,
        denial: None,
    });
    let durable = Arc::new(MemoryStream::default());
    durable
        .append(AppendRequest {
            path: StreamPath::new("accounts/events")?,
            records: vec![OwnedBytes::from_static(b"resumed")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"resume"))?),
        })
        .await?;
    let transport = Client::from_channels(Arc::from([
            provider_channel(Service::new(Arc::clone(&ended))),
            provider_channel(Service::new(durable)),
        ]), Client::authorization("fixture")?);
    let preferred = Arc::clone(&transport.preferred);
    let stream = crate::Stream::new(Arc::new(transport), "accounts/events")?;

    let record = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        stream
            .follow(0)
            .await?
            .next()
            .await
            .ok_or(StreamError::Unavailable)?
    })
    .await??;
    assert_eq!(record.value.as_slice(), b"resumed");
    assert_eq!(ended.follows.load(Ordering::Relaxed), 1);
    assert_eq!(preferred.load(Ordering::Relaxed), 1);

    // A stale cursor cannot overwrite a newer successful endpoint choice.
    let stale = RecordCursor {
        client: Client::from_channels(Arc::from([unavailable_channel(), unavailable_channel()]), Client::authorization("fixture")?),
        path: StreamPath::new("accounts/events")?,
        next: 0,
        remaining: None,
        active: None,
        buffered: Vec::new().into_iter(),
        completion: Completion::new(tracing::Span::none()),
        acknowledgements: None,
    };
    stale.client.preferred.store(1, Ordering::Relaxed);
    stale.advance_follow(0);
    assert_eq!(stale.client.preferred.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn lost_connection_dispatch_retries_but_peer_cancellation_does_not()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (client_io, _peer) = tokio::io::duplex(4096);
    let (mut sender, connection) = hyper::client::conn::http2::handshake(
        hyper_util::rt::TokioExecutor::new(),
        hyper_util::rt::TokioIo::new(client_io),
    )
    .await?;
    drop(connection);
    let request = tonic::codegen::http::Request::new(tonic::body::Body::empty());
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        sender.send_request(request),
    )
    .await?
    .err()
    .ok_or("closed connection accepted a request")?;
    assert!(error.is_canceled());
    let error = Status::from_error(Box::new(error));
    assert_eq!(error.code(), Code::Cancelled);
    assert!(retryable(&error), "{error:?}");
    assert_unary_retry(error, true).await?;
    assert_unary_retry(Status::cancelled("application cancellation"), false).await?;
    Ok(())
}

#[tokio::test]
async fn incomplete_response_keeps_the_same_request_retryable()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tonic::codec::Codec;
    let mut codec =
        tonic_prost::ProstCodec::<wire::AppendResponse, wire::AppendResponse>::default();
    let mut response = tonic::Streaming::new_response(
        codec.decoder(),
        tonic::body::Body::empty(),
        tonic::codegen::http::StatusCode::OK,
        None,
        Some(1024),
    );
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), response.message())
        .await?
        .err()
        .ok_or("missing terminal status must not succeed")?;
    assert_eq!(error.code(), Code::Unknown);
    assert!(std::error::Error::source(&error).is_none());
    assert!(
        retryable(&error),
        "incomplete response must preserve the retry identity: {error}"
    );
    assert_unary_retry(error, true).await?;
    assert_unary_retry(Status::unknown("peer outcome unknown"), true).await?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn persistent_unknown_expires_without_changing_request_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::connect(["https://fixture.invalid"], "fixture", None).await?;
    let started = tokio::time::Instant::now();
    let mut attempts = 0;
    let result: Result<(), StreamError> = client
        .unary(OPERATION_ENDPOINT_ATTEMPT_TIMEOUT, Bytes::from_static(b"stable-key-and-body"), |_, request| {
            assert_eq!(
                request.into_inner(),
                Bytes::from_static(b"stable-key-and-body")
            );
            attempts += 1;
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                Err(Status::unknown("outcome unavailable"))
            })
        })
        .await
        .and_then(|response| decode_response(response, Ok));
    assert_eq!(result, Err(StreamError::Unavailable));
    assert_eq!(tokio::time::Instant::now() - started, OPERATION_DEADLINE);
    assert!((2..=100).contains(&attempts));
    Ok(())
}

#[tokio::test]
async fn remote_request_reset_is_retryable_without_retrying_protocol_errors()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for reason in [h2::Reason::CANCEL, h2::Reason::PROTOCOL_ERROR] {
        let mut tasks = tokio::task::JoinSet::new();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let (client_io, server_io) = tokio::io::duplex(4096);
            tasks.spawn(async move {
                let mut connection = h2::server::handshake(server_io).await?;
                let (_, mut response) = connection.accept().await.ok_or("request absent")??;
                response.send_reset(reason);
                connection.graceful_shutdown();
                while connection.accept().await.is_some() {}
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
            let (mut sender, connection) = h2::client::handshake(client_io).await?;
            tasks.spawn(async move {
                connection.await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            });
            let request = tonic::codegen::http::Request::builder()
                .uri("http://fixture.invalid/stream")
                .body(())?;
            let (response, _) = sender.send_request(request, true)?;
            response.await.err().ok_or_else(|| {
                Box::<dyn std::error::Error + Send + Sync>::from("reset response succeeded")
            })
        })
        .await;
        tasks.shutdown().await;
        let error = outcome??;
        assert!(error.is_remote());
        let error = Status::from_error(Box::new(error));
        assert_eq!(retryable(&error), reason == h2::Reason::CANCEL, "{error:?}");
        assert_unary_retry(error, reason == h2::Reason::CANCEL).await?;
    }
    Ok(())
}

#[test]
fn retries_transport_loss_but_not_peer_errors_or_corrupt_data() {
    for kind in [
        std::io::ErrorKind::UnexpectedEof,
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::ConnectionAborted,
        std::io::ErrorKind::BrokenPipe,
        std::io::ErrorKind::NotConnected,
        std::io::ErrorKind::TimedOut,
    ] {
        let mut error = Status::unknown("response body interrupted");
        error.set_source(Arc::new(std::io::Error::from(kind)));
        assert!(retryable(&error), "{kind:?}");
    }
    for error in [
        Status::internal("protocol error"),
        Status::data_loss("corrupt record"),
        Status::permission_denied("denied"),
        Status::cancelled("caller cancelled"),
        Status::from_error(Box::new(std::io::Error::from(
            std::io::ErrorKind::InvalidData,
        ))),
    ] {
        assert!(!retryable(&error), "{error:?}");
    }
}
