use super::*;

#[tokio::test]
async fn served_body_completion_stays_with_its_origin() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    for (filtered, scenario) in [false, true].into_iter().flat_map(|filtered| {
        ["unpolled", "partial", "pending", "eof", "error"]
            .into_iter()
            .map(move |scenario| (filtered, scenario))
    }) {
        let captured = acyclic_grpc_observability::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default()
                .with(captured.clone())
                .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                    !filtered || !metadata.name().starts_with("acyclic.stream")
                })),
        );
        let provider = Arc::new(FiniteFollow {
            inner: MemoryStream::default(),
            follows: AtomicUsize::new(0),
            tail_delay: std::time::Duration::ZERO,
            denial: Some(1),
        });
        provider
            .inner
            .append(AppendRequest {
                path: StreamPath::new("events")?,
                records: vec![
                    OwnedBytes::from_static(b"one"),
                    OwnedBytes::from_static(b"two"),
                ],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let service = Service::new(Arc::clone(&provider));
        let caller = tracing::info_span!(
            "opening_caller",
            outcome = "caller",
            error.kind = "caller",
            rpc.code = 99
        );
        let mut body = async {
            assert_eq!(
                StreamService::tail(
                    &service,
                    Request::new(wire::TailRequest {
                        path: String::new(),
                    })
                )
                .await
                .unwrap_err()
                .code(),
                Code::InvalidArgument
            );
            match scenario {
                "error" => StreamService::follow(
                    &service,
                    Request::new(wire::FollowRequest {
                        path: "events".to_owned(),
                        from: 0,
                    }),
                )
                .await
                .map(Response::into_inner),
                "pending" => StreamService::follow(
                    &Service::new(Arc::new(provider.inner.clone())),
                    Request::new(wire::FollowRequest {
                        path: "events".to_owned(),
                        from: 0,
                    }),
                )
                .await
                .map(Response::into_inner),
                _ => StreamService::read(
                    &service,
                    Request::new(wire::ReadRequest {
                        path: "events".to_owned(),
                        from: 0,
                        limit: 2,
                    }),
                )
                .await
                .map(Response::into_inner),
            }
        }
        .instrument(caller)
        .await?;
        let operation = if matches!(scenario, "pending" | "error") {
            "acyclic.stream.grpc.serve.follow"
        } else {
            "acyclic.stream.grpc.serve.read"
        };
        assert!(!captured.fields(operation).contains_key("outcome"));
        let other = acyclic_grpc_observability::tests::Capture::default();
        let _polling = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let caller = tracing::info_span!(
            "polling_caller",
            outcome = "caller",
            error.kind = "caller",
            rpc.code = 99
        );
        async {
            if scenario != "unpolled" {
                assert!(body.next().await.unwrap().is_ok());
                if scenario == "error" {
                    assert_eq!(
                        body.next().await.unwrap().unwrap_err().code(),
                        Code::PermissionDenied
                    );
                    assert!(body.next().await.is_none());
                } else if matches!(scenario, "pending" | "eof") {
                    assert!(body.next().await.unwrap().is_ok());
                    if scenario == "pending" {
                        assert!(futures::poll!(body.next()).is_pending());
                    } else {
                        assert!(body.next().await.is_none());
                        assert!(body.next().await.is_none());
                    }
                }
            }
            drop(body);
        }
        .instrument(caller)
        .await;
        for (capture, name) in [(&captured, "opening_caller"), (&other, "polling_caller")] {
            let fields = capture.fields(name);
            assert_eq!(fields["outcome"], "caller");
            assert_eq!(fields["error.kind"], "caller");
            assert_eq!(fields["rpc.code"], "99");
        }
        assert!(other.all(operation).is_empty());
        if !filtered {
            let fields = captured.fields(operation);
            match scenario {
                "eof" => {
                    assert_eq!(fields["outcome"], "ok");
                    assert_eq!(fields["rpc.code"], "0");
                }
                "error" => {
                    assert_eq!(fields["outcome"], "err");
                    assert_eq!(fields["error.kind"], "access_denied");
                    assert_eq!(fields["rpc.code"], "7");
                }
                _ => {
                    assert_eq!(fields["outcome"], "err");
                    assert_eq!(fields["error.kind"], "cancelled");
                    assert!(!fields.contains_key("rpc.code"));
                }
            }
            assert_eq!(
                captured.fields("acyclic.stream.grpc.serve.tail")["error.kind"],
                "invalid_path"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn observed_body_terminal_and_drop_close_real_parent_under_origin() {
    use tracing_subscriber::layer::SubscriberExt as _;
    for scenario in ["pending", "error", "eof"] {
        let captured = acyclic_grpc_observability::tests::Capture::default();
        let mut body = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = tracing::info_span!("origin_parent");
                let span = tracing::info_span!(parent: &parent, "body", outcome = Empty, error.kind = Empty, rpc.code = Empty);
                let nested = span.in_scope(|| tracing::info_span!("nested_body"));
                let source = stream::poll_fn(move |_| {
                    let _retained = &nested;
                    match scenario {
                        "error" => {
                            Poll::Ready(Some(Err::<u8, _>(Status::permission_denied("denied"))))
                        }
                        "eof" => Poll::Ready(None),
                        _ => Poll::Pending,
                    }
                });
                observe_body(&span, source)
            },
        );
        let other = acyclic_grpc_observability::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_parent");
        let _entered = parent.enter();
        match scenario {
            "pending" => {
                assert!(futures::poll!(body.next()).is_pending());
            }
            "error" => {
                assert!(body.next().await.unwrap().is_err());
            }
            "eof" => {
                assert!(body.next().await.is_none());
            }
            _ => unreachable!(),
        }
        assert_eq!(captured.closed("origin_parent"), 0);
        drop(body);
        for name in ["nested_body", "body", "origin_parent"] {
            assert_eq!(captured.closed(name), 1, "{name}");
        }
        assert_eq!(other.closed("foreign_parent"), 0);
        let fields = captured.fields("body");
        assert_eq!(
            fields["outcome"],
            if scenario == "eof" { "ok" } else { "err" }
        );
        if scenario == "pending" {
            assert_eq!(fields["error.kind"], "cancelled");
        }
    }
}

#[tokio::test]
async fn cancelled_server_handler_records_owned_rpc_and_closes_origin_parent() {
    use tracing_subscriber::layer::SubscriberExt as _;
    let service = Service::new(Arc::new(FiniteFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        tail_delay: std::time::Duration::from_secs(60),
        denial: None,
    }));
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let future = tracing::subscriber::with_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
        || {
            let parent = obs::OperationSpan::new(tracing::info_span!("origin_parent"));
            parent.scope(StreamService::tail(
                &service,
                Request::new(wire::TailRequest {
                    path: "pending".to_owned(),
                }),
            ))
        },
    );
    let mut future = Box::pin(future);
    assert!(futures::poll!(future.as_mut()).is_pending());
    assert!(
        !captured
            .fields("acyclic.stream.grpc.serve.tail")
            .contains_key("outcome")
    );
    let other = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(other.clone()),
    );
    let parent = tracing::info_span!("foreign_parent");
    let _entered = parent.enter();
    drop(future);
    let fields = captured.fields("acyclic.stream.grpc.serve.tail");
    assert_eq!(fields["outcome"], "err");
    assert_eq!(fields["error.kind"], "cancelled");
    assert!(!fields.contains_key("rpc.code"));
    assert_eq!(captured.closed("acyclic.stream.grpc.serve.tail"), 1);
    assert_eq!(captured.closed("origin_parent"), 1);
    assert_eq!(other.closed("foreign_parent"), 0);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one adversarial matrix compares filtered and visible lifecycle evidence across body boundaries"
)]
async fn client_body_completion_observes_decode_trailers_and_drop()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    for (filtered, scenario) in [false, true].into_iter().flat_map(|filtered| {
        ["unpolled", "partial", "eof", "error", "malformed", "limit"]
            .into_iter()
            .map(move |scenario| (filtered, scenario))
    }) {
        let captured = acyclic_grpc_observability::tests::Capture::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::Registry::default()
                .with(captured.clone())
                .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                    !filtered || !metadata.name().starts_with("acyclic.stream")
                })),
        );
        let source = MixedFrames(
            Service::new(Arc::new(MemoryStream::default())),
            Some(match scenario {
                "partial" | "limit" => "pending",
                _ => scenario,
            }),
            None,
        );
        let client = Client::from_channels(
            Arc::from([provider_channel(source)]),
            Client::authorization("fixture")?,
        );
        let body = client
            .read(ReadRequest {
                path: StreamPath::new("events")?,
                from: 0,
                limit: if scenario == "limit" { 1 } else { 10 },
            })
            .with_subscriber(dispatch)
            .await?;
        assert!(
            captured.all("acyclic.stream.grpc.call").is_empty(),
            "creating a lazy cursor performs no RPC"
        );
        let other = acyclic_grpc_observability::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let caller = tracing::info_span!(
            "polling_caller",
            outcome = "caller",
            error.kind = "caller",
            rpc.code = 99
        );
        async {
            let mut body = body;
            match scenario {
                "unpolled" => {}
                "malformed" => {
                    assert_eq!(
                        body.next().await.unwrap().unwrap_err(),
                        StreamError::Unavailable
                    );
                    assert!(body.next().await.is_none());
                    assert!(body.next().await.is_none());
                }
                _ => {
                    assert!(body.next().await.unwrap().is_ok());
                    if !filtered {
                        assert!(
                            !captured
                                .fields("acyclic.stream.grpc.call")
                                .contains_key("outcome"),
                            "response headers and one delivered record do not observe trailers"
                        );
                    }
                    if scenario == "error" {
                        assert_eq!(
                            body.next().await.unwrap().unwrap_err(),
                            StreamError::AccessDenied
                        );
                        assert!(body.next().await.is_none());
                    }
                    if scenario == "eof" {
                        assert!(body.next().await.is_none());
                    }
                }
            }
            drop(body);
        }
        .instrument(caller)
        .await;
        assert!(
            other.all("acyclic.stream.grpc.call").is_empty(),
            "polling dispatch must not steal owned RPC spans"
        );
        assert_eq!(other.fields("polling_caller")["outcome"], "caller");
        assert_eq!(other.fields("polling_caller")["rpc.code"], "99");
        if filtered {
            assert!(captured.all("acyclic.stream.grpc.call").is_empty());
            continue;
        }
        let logical = captured.fields("acyclic.stream.grpc.read");
        let rpc = captured.fields("acyclic.stream.grpc.call");
        match scenario {
            "unpolled" => {
                assert_eq!(logical["error.kind"], "cancelled");
                assert!(rpc.is_empty());
            }
            "partial" => {
                assert_eq!(logical["error.kind"], "cancelled");
                assert_eq!(rpc["error.kind"], "cancelled");
                assert!(!rpc.contains_key("rpc.code"));
            }
            "limit" => {
                assert_eq!(logical["outcome"], "ok");
                assert_eq!(rpc["error.kind"], "cancelled");
                assert!(!rpc.contains_key("rpc.code"));
            }
            "eof" => {
                assert_eq!(logical["outcome"], "ok");
                assert_eq!(rpc["outcome"], "ok");
                assert_eq!(rpc["rpc.code"], "0");
            }
            "error" => {
                assert_eq!(logical["error.kind"], "access_denied");
                assert_eq!(rpc["error.kind"], "access_denied");
                assert_eq!(rpc["rpc.code"], "7");
            }
            "malformed" => {
                assert_eq!(logical["error.kind"], "invalid_response");
                assert_eq!(rpc["error.kind"], "invalid_response");
                assert!(!rpc.contains_key("rpc.code"));
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn eager_children_semantic_decode_failure_keeps_rpc_trailers_unobserved()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let service = MixedFrames(
        Service::new(Arc::new(MemoryStream::default())),
        Some("malformed_children"),
        None,
    );
    let client = Client::from_channels(
        Arc::from([provider_channel(service)]),
        Client::authorization("fixture")?,
    );
    assert!(matches!(
        client
            .children(ChildrenRequest {
                parent: None,
                limit: 10
            })
            .await,
        Err(StreamError::Unavailable)
    ));
    let fields = captured.fields("acyclic.stream.grpc.call");
    assert_eq!(fields["error.kind"], "invalid_response");
    assert!(!fields.contains_key("rpc.code"));
    Ok(())
}

#[tokio::test]
async fn client_follow_reconnects_close_rpc_spans_without_closing_cursor()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let ended = Arc::new(FiniteFollow {
        inner: MemoryStream::default(),
        follows: AtomicUsize::new(0),
        tail_delay: std::time::Duration::ZERO,
        denial: None,
    });
    let live = Arc::new(MemoryStream::default());
    let path = StreamPath::new("events")?;
    live.append(AppendRequest {
        path: path.clone(),
        records: vec![OwnedBytes::from_static(b"record")],
        if_tail: None,
        idempotency_key: None,
    })
    .await?;
    let client = Client::from_channels(
        Arc::from([
            provider_channel(Service::new(Arc::clone(&ended))),
            provider_channel(Service::new(live)),
        ]),
        Client::authorization("fixture")?,
    );
    let mut cursor = client.follow(path, 0).await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), cursor.next())
            .await?
            .unwrap()
            .is_ok()
    );
    assert_eq!(ended.follows.load(Ordering::Relaxed), 1);
    let rpcs = captured.all("acyclic.stream.grpc.call");
    assert_eq!(rpcs.len(), 2);
    assert_eq!(
        rpcs.iter()
            .filter(|fields| fields.get("outcome").is_some_and(|value| value == "ok"))
            .count(),
        1
    );
    assert_eq!(
        rpcs.iter()
            .filter(|fields| !fields.contains_key("outcome"))
            .count(),
        1
    );
    assert!(
        !captured
            .fields("acyclic.stream.grpc.follow")
            .contains_key("outcome")
    );
    drop(cursor);
    assert_eq!(captured.all("acyclic.stream.grpc.follow").len(), 1);
    assert_eq!(
        captured.fields("acyclic.stream.grpc.follow")["error.kind"],
        "cancelled"
    );
    let rpcs = captured.all("acyclic.stream.grpc.call");
    assert!(
        rpcs.iter()
            .any(|fields| fields.get("rpc.code").is_some_and(|value| value == "0"))
    );
    assert!(rpcs.iter().any(|fields| {
        fields
            .get("error.kind")
            .is_some_and(|value| value == "cancelled")
            && !fields.contains_key("rpc.code")
    }));
    Ok(())
}

#[tokio::test]
async fn unary_retry_attempts_finish_independently()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let client = Client::connect(["https://fixture.invalid"], "fixture", None).await?;
    let mut first = true;
    let response = client
        .unary(OPERATION_ENDPOINT_ATTEMPT_TIMEOUT, (), |_, _| {
            let result = if std::mem::take(&mut first) {
                Err(Status::unavailable("retry"))
            } else {
                Ok(Response::new(()))
            };
            Box::pin(async move { result })
        })
        .await?;
    let attempts = captured.all("acyclic.stream.grpc.call");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["attempt"], "1");
    assert_eq!(attempts[1]["attempt"], "2");
    assert_eq!(attempts[0]["outcome"], "err");
    assert_eq!(attempts[0]["rpc.code"], "14");
    assert!(!attempts[1].contains_key("outcome"));
    decode_response(response, Ok)?;
    let attempts = captured.all("acyclic.stream.grpc.call");
    assert_eq!(attempts[0]["outcome"], "err");
    assert_eq!(attempts[1]["outcome"], "ok");
    assert_eq!(attempts[1]["rpc.code"], "0");
    Ok(())
}

#[tokio::test]
async fn unary_headers_wait_for_decode_and_cancel_pending_requests()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let client = Client::connect(["https://fixture.invalid"], "fixture", None).await?;
    let response = client
        .unary(OPERATION_ENDPOINT_ATTEMPT_TIMEOUT, (), |_, _| {
            Box::pin(async { Ok(Response::new(wire::AppendResponse::default())) })
        })
        .await?;
    assert!(
        !captured
            .fields("acyclic.stream.grpc.call")
            .contains_key("outcome")
    );
    assert!(decode_response(response, append_outcome_from_wire).is_err());
    let fields = captured.fields("acyclic.stream.grpc.call");
    assert_eq!(fields["outcome"], "err");
    assert_eq!(fields["error.kind"], "invalid_response");
    assert_eq!(
        fields["rpc.code"], "0",
        "unary successful trailers remain distinct from semantic rejection"
    );
    let mut pending = Box::pin(
        client.unary(OPERATION_ENDPOINT_ATTEMPT_TIMEOUT, (), |_, _| {
            Box::pin(std::future::pending::<Result<Response<()>, Status>>())
        }),
    );
    assert!(futures::poll!(pending.as_mut()).is_pending());
    drop(pending);
    assert!(
        captured
            .all("acyclic.stream.grpc.call")
            .iter()
            .any(|fields| fields
                .get("error.kind")
                .is_some_and(|value| value == "cancelled"))
    );
    Ok(())
}
