#[tokio::test]
async fn client_reassembles_mixed_compressed_and_plain_read_frames()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = MixedFrames(Service::new(Arc::new(MemoryStream::default())), None);
    let channel = Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(
        tower::service_fn(move |_| {
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
        }),
    );
    let transport = Client::from_channels(Arc::from([channel]), "fixture")?;
    let path = StreamPath::new("accounts/mixed")?;
    let values = (0..20_u8)
        .map(|value| Bytes::from(vec![value; 64]))
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
            path: path.clone(),
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
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (2..19).collect::<Vec<_>>()
    );
    assert_eq!(
        records
            .into_iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        values.get(2..19).unwrap_or_default().to_vec()
    );
    crate::conformance::verify(&transport).await?;
    Ok(())
}

#[tokio::test]
async fn service_rejects_oversized_wire_commands_before_provider_dispatch() {
    let service = Service::new(Arc::new(MemoryStream::default()));
    let request = wire::AppendRequest {
        path: "x".repeat(crate::MAX_PATH_BYTES),
        records: vec![bytes::Bytes::from(vec![0_u8; crate::MAX_RECORD_BYTES]); 16],
        if_tail: None,
        idempotency_key: None,
    };
    assert!(request.encoded_len() > crate::MAX_COMMAND_BYTES);
    let error = service.append(Request::new(request)).await.err();
    assert_eq!(
        error.as_ref().map(Status::code),
        Some(Code::InvalidArgument)
    );
    assert_eq!(error.as_ref().map(Status::message), Some("limit_exceeded"));
}

#[test]
fn private_ca_bundle_is_exactly_bounded() {
    assert!(matches!(
        bounded_ca_certificate(&[]),
        Err(ConnectError::InvalidCaCertificate)
    ));
    assert!(matches!(
        bounded_ca_certificate(&vec![b'x'; MAX_CA_CERTIFICATE_BYTES]),
        Ok(certificate) if certificate.len() == MAX_CA_CERTIFICATE_BYTES
    ));
    assert!(matches!(
        bounded_ca_certificate(&vec![b'x'; MAX_CA_CERTIFICATE_BYTES + 1]),
        Err(ConnectError::InvalidCaCertificate)
    ));
}

fn unavailable_channel() -> Channel {
    Endpoint::from_static("http://unavailable.invalid").connect_with_connector_lazy(
        tower::service_fn(|_| async {
            Err::<hyper_util::rt::TokioIo<tokio::io::DuplexStream>, _>(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "unavailable",
            ))
        }),
    )
}

fn stalled_channel() -> Channel {
    Endpoint::from_static("http://stalled.invalid").connect_with_connector_lazy(tower::service_fn(
        |_| async {
            let (client, server) = tokio::io::duplex(64 * 1024);
            tokio::spawn(async move {
                let _server = server;
                std::future::pending::<()>().await;
            });
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
        },
    ))
}

#[tokio::test]
async fn endpoint_pool_is_explicit_and_bounded()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shared = Client::connect_endpoints(
        ["https://data-a.invalid", "https://data-b.invalid"],
        "fixture",
    )
    .await?;
    assert_eq!(shared.channels.len(), 2);

    assert!(matches!(
        Client::connect_endpoints(std::iter::empty::<&str>(), "fixture").await,
        Err(ConnectError::NoEndpoints)
    ));
    assert!(matches!(
        Client::connect_endpoints(std::iter::repeat("https://data.invalid"), "fixture",).await,
        Err(ConnectError::EndpointLimit)
    ));
    let oversized = format!("https://{}.invalid", "x".repeat(MAX_ENDPOINT_URI_BYTES));
    assert!(matches!(
        Client::connect_endpoints([oversized], "fixture").await,
        Err(ConnectError::EndpointLimit)
    ));
    let long = "t".repeat(MAX_BEARER_TOKEN_BYTES + 1);
    for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
        assert!(matches!(
            Client::connect_endpoints(["https://data.invalid"], token).await,
            Err(ConnectError::InvalidCredential)
        ));
    }
    Ok(())
}

#[tokio::test]
async fn dropping_eager_connect_releases_a_stalled_tls_socket()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let certificate = generate_simple_self_signed(["localhost".to_owned()])?;
    let certificate_pem = certificate.cert.pem();
    let (accepted_sender, accepted_receiver) = tokio::sync::oneshot::channel();
    let (closed_sender, closed_receiver) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let _ = accepted_sender.send(());
        let mut buffer = [0_u8; 1024];
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        let _ = closed_sender.send(());
        Ok::<(), std::io::Error>(())
    });
    let pending = tokio::spawn(Client::connect_eager_with_ca_certificate(
        endpoint,
        "fixture",
        certificate_pem,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), accepted_receiver).await??;
    pending.abort();
    let _ = pending.await;
    tokio::time::timeout(std::time::Duration::from_secs(1), closed_receiver).await??;
    server.await??;
    Ok(())
}

#[tokio::test]
async fn malformed_bearer_is_rejected_before_eager_network_io()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let Err(error) = Client::connect_eager(endpoint, "\r\n").await else {
        return Err("malformed bearer unexpectedly connected".into());
    };
    assert!(matches!(error, ConnectError::InvalidCredential));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept(),)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn default_https_pool_installs_tls_before_first_use()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::connect("https://127.0.0.1:1", "fixture").await?;
    let mut service = Client::service(&client.channels, 0);
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        service.tail(Request::new(wire::TailRequest {
            path: "accounts/events".to_owned(),
        })),
    )
    .await?
    .err()
    .ok_or("closed local endpoint unexpectedly answered")?;
    assert!(
        !error.to_string().contains("TLS is not enabled"),
        "HTTPS channel was built without TLS: {error}"
    );
    Ok(())
}

#[tokio::test]
async fn private_ca_https_connection_preserves_ambient_roots_and_exact_bearer()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let certified = generate_simple_self_signed(["localhost".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let private_key_pem = certified.signing_key.serialize_pem();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_certificate_pem = certificate_pem.clone();
    let service = StreamServiceServer::with_interceptor(
        Service::new(Arc::new(MemoryStream::default())),
        |request: Request<()>| {
            if request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some("Bearer exact-token")
            {
                return Err(Status::unauthenticated("missing exact bearer credential"));
            }
            Ok(request)
        },
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
            )?
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let endpoint = format!("https://localhost:{}", address.port());
    let path = StreamPath::new("accounts/events")?;
    let ambient = Client::connect(&endpoint, "exact-token").await?;
    let mut ambient_service = Client::service(&ambient.channels, 0);
    assert!(
        ambient_service
            .tail(Request::new(wire::TailRequest {
                path: path.to_string(),
            }))
            .await
            .is_err()
    );
    let explicit =
        Client::connect_with_ca_certificate(endpoint, "exact-token", certificate_pem).await?;
    assert_eq!(
        explicit.tail(path.clone()).await,
        Err(StreamError::NotFound)
    );

    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

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
            records: vec![Bytes::from_static(b"seed")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"slow-tail"))?),
        })
        .await?;
    let transport = Client::from_channels(
        Arc::from([stalled_channel(), provider_channel(Service::new(provider))]),
        "fixture",
    )?;
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
            records: vec![Bytes::from_static(b"one"), Bytes::from_static(b"two")],
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
            let client = Client::from_channels(Arc::from([channel.clone()]), token)?;
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
            records: vec![Bytes::from_static(b"resumed")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"resume"))?),
        })
        .await?;
    let transport = Client::from_channels(
        Arc::from([
            provider_channel(Service::new(Arc::clone(&ended))),
            in_memory_channel(durable),
        ]),
        "fixture",
    )?;
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
    assert_eq!(record.value, Bytes::from_static(b"resumed"));
    assert_eq!(ended.follows.load(Ordering::Relaxed), 1);
    assert_eq!(preferred.load(Ordering::Relaxed), 1);

    // A stale cursor cannot overwrite a newer successful endpoint choice.
    let stale = RecordCursor {
        client: Client::from_channels(
            Arc::from([unavailable_channel(), unavailable_channel()]),
            "fixture",
        )?,
        path: StreamPath::new("accounts/events")?,
        next: 0,
        remaining: None,
        active: None,
        buffered: VecDeque::new(),
        completion: Completion::new(tracing::Span::none()),
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
    // Losing the driver closes Hyper's dispatch queue, independently of a peer status.
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

async fn assert_unary_retry(
    error: Status,
    retry: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::connect("https://fixture.invalid", "fixture").await?;
    let body = wire::AppendRequest {
        path: "/fixture".to_owned(),
        records: vec![bytes::Bytes::from_static(b"mutation")],
        if_tail: Some(3),
        idempotency_key: Some(bytes::Bytes::from_static(b"stable-key")),
    };
    let mut attempts = Vec::new();
    let mut first = Some(error);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.unary(body.clone(), |_, request| {
            attempts.push(request.into_inner());
            let result = first.take().map_or_else(|| Ok(Response::new(())), Err);
            Box::pin(async move { result })
        }),
    )
    .await?
    .and_then(|response| response.decode(Ok));
    if retry {
        assert_eq!(result, Ok(()));
        assert_eq!(attempts, vec![body.clone(), body]);
    } else {
        assert!(result.is_err());
        assert_eq!(attempts, vec![body]);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn persistent_unknown_expires_without_changing_request_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::connect("https://fixture.invalid", "fixture").await?;
    let started = tokio::time::Instant::now();
    let mut attempts = 0;
    let result: Result<(), StreamError> = client
        .unary(
            bytes::Bytes::from_static(b"stable-key-and-body"),
            |_, request| {
                assert_eq!(
                    request.into_inner(),
                    bytes::Bytes::from_static(b"stable-key-and-body")
                );
                attempts += 1;
                Box::pin(async {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    Err(Status::unknown("outcome unavailable"))
                })
            },
        )
        .await
        .and_then(|response| response.decode(Ok));
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

#[test]
fn inspection_rejects_an_observation_for_another_retry_identity() -> Result<(), StreamError> {
    let requested = IdempotencyKey::new(Bytes::from_static(b"requested"))?;
    let forged = wire::IdempotencyObservation {
        idempotency_key: bytes::Bytes::from_static(b"other"),
        request_digest: bytes::Bytes::from_static(&[7; 32]),
        outcome: Some(wire::idempotency_observation::Outcome::Append(
            append_outcome_to_wire(AppendOutcome::TailConflict { actual_tail: 3 }),
        )),
    };
    assert_eq!(
        bind_observation(&requested, Some(forged)),
        Err(StreamError::Unavailable)
    );
    Ok(())
}

#[tokio::test]
async fn service_maps_the_canonical_contract_without_an_alternate_state_machine()
-> Result<(), Status> {
    let service = Service::new(Arc::new(MemoryStream::default()));
    let append = service
        .append(Request::new(wire::AppendRequest {
            path: "accounts/events".to_owned(),
            records: vec![bytes::Bytes::from_static(b"one")],
            if_tail: Some(0),
            idempotency_key: Some(bytes::Bytes::from_static(b"wire-append")),
        }))
        .await?
        .into_inner();
    let Some(wire::append_response::Outcome::Committed(receipt)) = append.outcome else {
        return Err(Status::internal("append outcome missing"));
    };
    if receipt.start != 0 || receipt.end != 1 || receipt.commit_id.len() != 32 {
        return Err(Status::internal("append receipt changed"));
    }
    let observation = service
        .inspect_idempotency(Request::new(wire::InspectIdempotencyRequest {
            idempotency_key: bytes::Bytes::from_static(b"wire-append"),
        }))
        .await?
        .into_inner()
        .observation
        .ok_or_else(|| Status::internal("idempotency observation missing"))?;
    if observation.request_digest.len() != 32
        || !matches!(
            observation.outcome,
            Some(wire::idempotency_observation::Outcome::Append(_))
        )
    {
        return Err(Status::internal("idempotency observation changed"));
    }
    let mut read = service
        .read(Request::new(wire::ReadRequest {
            path: "accounts/events".to_owned(),
            from: 0,
            limit: 1,
        }))
        .await?
        .into_inner();
    let frame = read
        .next()
        .await
        .ok_or_else(|| Status::internal("read ended"))??;
    let record = read_response_records(frame)
        .map_err(|error| error_status(&error))?
        .into_iter()
        .next()
        .ok_or_else(|| Status::internal("record missing"))?;
    if record.sequence != 0 || record.value.as_ref() != b"one" {
        return Err(Status::internal("record changed"));
    }
    Ok(())
}
