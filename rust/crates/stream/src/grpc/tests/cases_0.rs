use super::*;
use crate::MemoryStream;
use rcgen::generate_simple_self_signed;
use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use wire::stream_service_server::{StreamService, StreamServiceServer};

#[tokio::test]
async fn served_body_errors_remain_on_owned_span_when_polled_by_another_caller()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    for filtered in [false, true] {
        let captured = obs::tests::Capture::default();
        let subscriber = tracing_subscriber::Registry::default()
            .with(captured.clone())
            .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                !filtered || !metadata.name().starts_with("acyclic.stream")
            }));
        let _default = tracing::subscriber::set_default(subscriber);
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
                records: vec![Bytes::from_static(b"one")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let service = Service::new(provider);
        let caller = tracing::info_span!(
            "opening_caller",
            outcome = "caller",
            error.kind = "caller",
            rpc.code = 99
        );
        let mut body = async {
            let invalid = StreamService::tail(
                &service,
                Request::new(wire::TailRequest {
                    path: String::new(),
                }),
            )
            .await;
            assert_eq!(invalid.unwrap_err().code(), Code::InvalidArgument);
            Ok::<_, Status>(
                StreamService::follow(
                    &service,
                    Request::new(wire::FollowRequest {
                        path: "events".to_owned(),
                        from: 0,
                    }),
                )
                .await?
                .into_inner(),
            )
        }
        .instrument(caller)
        .await?;
        let polling_caller = tracing::info_span!(
            "polling_caller",
            outcome = "caller",
            error.kind = "caller",
            rpc.code = 99
        );
        async {
            assert!(body.next().await.unwrap().is_ok());
            assert_eq!(
                body.next().await.unwrap().unwrap_err().code(),
                Code::PermissionDenied
            );
            drop(body);
        }
        .instrument(polling_caller)
        .await;
        for name in ["opening_caller", "polling_caller"] {
            let fields = captured.fields(name);
            assert_eq!(fields["outcome"], "caller");
            assert_eq!(fields["error.kind"], "caller");
            assert_eq!(fields["rpc.code"], "99");
        }
        if !filtered {
            let fields = captured.fields("acyclic.stream.grpc.serve.follow");
            assert_eq!(fields["outcome"], "err");
            assert_eq!(fields["error.kind"], "access_denied");
            assert_eq!(
                fields["rpc.code"],
                (Code::PermissionDenied as u64).to_string()
            );
            assert_eq!(
                captured.fields("acyclic.stream.grpc.serve.tail")["error.kind"],
                "invalid_path"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn observed_body_completes_only_at_terminal_delivery() {
    use tracing_subscriber::layer::SubscriberExt as _;
    for (filtered, scenario) in [false, true].into_iter().flat_map(|filtered| {
        [
            "unpolled",
            "partial",
            "pending",
            "exhausted",
            "error",
            "peer_cancelled",
        ]
        .into_iter()
        .map(move |scenario| (filtered, scenario))
    }) {
        let captured = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default()
                .with(captured.clone())
                .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                    !filtered || metadata.name() != "body"
                })),
        );
        let span = tracing::info_span!(
            "body",
            outcome = Empty,
            error.kind = Empty,
            rpc.code = Empty
        );
        let source: futures::stream::BoxStream<'static, Result<u8, Status>> = match scenario {
            "pending" => stream::pending().boxed(),
            "error" => stream::iter([Err(Status::permission_denied("denied"))]).boxed(),
            "peer_cancelled" => stream::iter([Err(Status::cancelled("peer cancelled"))]).boxed(),
            _ => stream::iter([Ok(1), Ok(2)]).boxed(),
        };
        let mut body = observe_body(&span, source);
        assert!(!captured.fields("body").contains_key("outcome"));
        let caller = tracing::info_span!(
            "polling",
            outcome = "caller",
            error.kind = "caller",
            rpc.code = 99
        );
        async {
            match scenario {
                "unpolled" => {}
                "partial" => {
                    assert_eq!(body.next().await.unwrap().unwrap(), 1);
                }
                "pending" => {
                    assert!(futures::poll!(body.next()).is_pending());
                }
                "exhausted" => {
                    while let Some(item) = body.next().await {
                        assert!(item.is_ok());
                    }
                }
                "error" | "peer_cancelled" => {
                    assert!(body.next().await.unwrap().is_err());
                    assert!(body.next().await.is_none());
                }
                _ => unreachable!(),
            }
            drop(body);
        }
        .instrument(caller)
        .await;
        let fields = captured.fields("body");
        if !filtered {
            match scenario {
                "exhausted" => {
                    assert_eq!(fields["outcome"], "ok");
                    assert_eq!(fields["rpc.code"], "0");
                }
                "error" => {
                    assert_eq!(fields["outcome"], "err");
                    assert_eq!(fields["error.kind"], "access_denied");
                    assert_eq!(fields["rpc.code"], "7");
                }
                "peer_cancelled" => {
                    assert_eq!(fields["outcome"], "err");
                    assert_eq!(fields["error.kind"], "cancelled");
                    assert_eq!(fields["rpc.code"], "1");
                }
                _ => {
                    assert_eq!(fields["outcome"], "err");
                    assert_eq!(fields["error.kind"], "cancelled");
                    assert!(!fields.contains_key("rpc.code"));
                }
            }
        }
        assert_eq!(captured.fields("polling")["outcome"], "caller");
        assert_eq!(captured.fields("polling")["rpc.code"], "99");
    }
}

#[tokio::test]
async fn observed_body_fuses_after_error_or_eof() {
    for error in [false, true] {
        let polls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&polls);
        let source = stream::poll_fn(move |_| {
            let polled = count.fetch_add(1, Ordering::SeqCst);
            if polled == 0 {
                if error {
                    Poll::Ready(Some(Err::<u8, _>(Status::permission_denied("denied"))))
                } else {
                    Poll::Ready(None)
                }
            } else {
                Poll::Ready(Some(Ok(99)))
            }
        });
        let mut body = observe_body(&tracing::Span::none(), source);
        if error {
            assert!(body.next().await.unwrap().is_err());
        } else {
            assert!(body.next().await.is_none());
        }
        assert!(body.next().await.is_none());
        assert!(body.next().await.is_none());
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "non-fused source must never be repolled after closure"
        );
    }
}

#[tokio::test]
async fn observed_body_reinstalls_owned_dispatch_even_when_span_is_filtered() {
    use tracing_subscriber::layer::SubscriberExt as _;
    for filtered in [false, true] {
        let original = obs::tests::Capture::default();
        let subscriber = tracing_subscriber::Registry::default()
            .with(original.clone())
            .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                !filtered || metadata.name() != "body"
            }));
        let (span, dispatch) = tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "body",
                outcome = Empty,
                error.kind = Empty,
                rpc.code = Empty
            );
            (span, tracing::dispatcher::get_default(Clone::clone))
        });
        let other = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let source = stream::poll_fn(|_| {
            let _child = tracing::info_span!("body_child");
            Poll::Ready(Some(Ok::<_, Status>(1_u8)))
        });
        // Enabled spans supply their original dispatch even if the guard is
        let mut body = if filtered {
            tracing::dispatcher::with_default(&dispatch, || observe_body(&span, source))
        } else {
            observe_body(&span, source)
        };
        let caller = tracing::info_span!("caller", outcome = "caller");
        async {
            assert_eq!(body.next().await.unwrap().unwrap(), 1);
            drop(body);
        }
        .instrument(caller)
        .await;
        assert_eq!(original.all("body_child").len(), 1);
        assert!(other.all("body_child").is_empty());
        assert_eq!(other.fields("caller")["outcome"], "caller");
        if !filtered {
            assert_eq!(original.fields("body")["error.kind"], "cancelled");
        }
    }
}

#[tokio::test]
async fn observed_body_terminal_and_drop_close_real_parent_under_origin() {
    use tracing_subscriber::layer::SubscriberExt as _;
    for scenario in ["pending", "error", "eof"] {
        let captured = obs::tests::Capture::default();
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
        let other = obs::tests::Capture::default();
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
    let captured = obs::tests::Capture::default();
    let future = tracing::subscriber::with_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
        || {
            let parent = obs::OwnedSpan::new(tracing::info_span!("origin_parent"));
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
    let other = obs::tests::Capture::default();
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

struct FiniteFollow {
    inner: MemoryStream,
    follows: AtomicUsize,
    tail_delay: std::time::Duration,
    /// `Some(n)`: Follow yields the first `n` records, then a denied credential;
    denial: Option<usize>,
}

#[async_trait]
impl StreamProvider for FiniteFollow {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        tokio::time::sleep(self.tail_delay).await;
        self.inner.tail(path).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        tokio::time::sleep(self.tail_delay).await;
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
        self.follows.fetch_add(1, Ordering::Relaxed);
        match self.denial {
            None => Ok(stream::empty().boxed()),
            Some(0) => Err(StreamError::AccessDenied),
            Some(count) => {
                let limit = u32::try_from(count).map_err(|_| StreamError::InvalidArgument)?;
                let records = self.inner.read(ReadRequest { path, from, limit }).await?;
                Ok(records
                    .chain(stream::once(async { Err(StreamError::AccessDenied) }))
                    .boxed())
            }
        }
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

fn in_memory_channel(provider: Arc<MemoryStream>) -> Channel {
    provider_channel(Service::new(provider))
}

fn provider_channel<S: StreamService + Clone>(service: S) -> Channel {
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

#[tokio::test]
async fn grpc_transport_passes_the_public_suite()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = in_memory_channel(Arc::new(MemoryStream::default()));
    let transport = Client::from_channels(Arc::from([channel.clone()]), "fixture")?;
    crate::conformance::verify(&transport).await?;
    let mut obsolete = tonic::client::Grpc::new(channel);
    obsolete.ready().await?;
    let result: Result<Response<wire::TailResponse>, Status> = obsolete
        .unary(
            Request::new(wire::TailRequest {
                path: "events".into(),
            }),
            tonic::codegen::http::uri::PathAndQuery::from_static(
                "/acyclic.stream.v2.StreamService/Tail",
            ),
            tonic_prost::ProstCodec::default(),
        )
        .await;
    assert_eq!(
        result.err().map(|error| error.code()),
        Some(tonic::Code::Unimplemented)
    );
    Ok(())
}

/// Regroups every Read frame into batches of three records that alternate
#[derive(Clone)]
struct MixedFrames(Service<MemoryStream>, Option<&'static str>);

#[async_trait]
impl StreamService for MixedFrames {
    type ReadStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type FollowStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type ChildrenStream =
        futures::stream::BoxStream<'static, Result<wire::ChildrenResponse, Status>>;

    async fn inspect_idempotency(
        &self,
        request: Request<wire::InspectIdempotencyRequest>,
    ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
        self.0.inspect_idempotency(request).await
    }
    async fn append(
        &self,
        request: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        self.0.append(request).await
    }
    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        self.0.tail(request).await
    }
    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        self.0.fork(request).await
    }
    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        if let Some(fault) = self.1 {
            let valid = read_response_wire(vec![wire::Record {
                commit_id: bytes::Bytes::from(vec![0; 32]),
                ..Default::default()
            }]);
            let frames: Self::ReadStream = match fault {
                "malformed" => {
                    stream::iter([Ok(read_response_wire(vec![wire::Record::default()]))]).boxed()
                }
                "error" => {
                    stream::iter([Ok(valid), Err(Status::permission_denied("denied"))]).boxed()
                }
                "pending" => stream::once(async move { Ok(valid) })
                    .chain(stream::pending())
                    .boxed(),
                _ => stream::iter([Ok(valid)]).boxed(),
            };
            return Ok(Response::new(frames));
        }
        let frames = self
            .0
            .read(request)
            .await?
            .into_inner()
            .collect::<Vec<_>>()
            .await;
        let mut records = Vec::new();
        for frame in frames {
            records.extend(read_response_records(frame?).map_err(|error| error_status(&error))?);
        }
        let regrouped = records
            .chunks(3)
            .enumerate()
            .map(|(index, chunk)| {
                Ok(if index % 2 == 0 {
                    crate::wire_codec::zstd_read_response(chunk.to_vec())
                } else {
                    read_response_wire(chunk.to_vec())
                })
            })
            .collect::<Vec<_>>();
        Ok(Response::new(stream::iter(regrouped).boxed()))
    }
    async fn follow(
        &self,
        request: Request<wire::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        self.0.follow(request).await
    }
    async fn children(
        &self,
        request: Request<wire::ChildrenRequest>,
    ) -> Result<Response<Self::ChildrenStream>, Status> {
        if self.1 == Some("malformed_children") {
            return Ok(Response::new(
                stream::iter([Ok(wire::ChildrenResponse { child: None })]).boxed(),
            ));
        }
        self.0.children(request).await
    }
    async fn children_page(
        &self,
        request: Request<wire::ChildrenPageRequest>,
    ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
        self.0.children_page(request).await
    }
    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        self.0.commit(request).await
    }
    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        self.0.read_commit(request).await
    }
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
        let captured = obs::tests::Capture::default();
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
        );
        let client = Client::from_channels(Arc::from([provider_channel(source)]), "fixture")?;
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
        let other = obs::tests::Capture::default();
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
    let captured = obs::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let service = MixedFrames(
        Service::new(Arc::new(MemoryStream::default())),
        Some("malformed_children"),
    );
    let client = Client::from_channels(Arc::from([provider_channel(service)]), "fixture")?;
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
    let captured = obs::tests::Capture::default();
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
        records: vec![Bytes::from_static(b"record")],
        if_tail: None,
        idempotency_key: None,
    })
    .await?;
    let client = Client::from_channels(
        Arc::from([
            provider_channel(Service::new(Arc::clone(&ended))),
            in_memory_channel(live),
        ]),
        "fixture",
    )?;
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
    let captured = obs::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let client = Client::connect("https://fixture.invalid", "fixture").await?;
    let mut first = true;
    let response = client
        .unary((), |_, _| {
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
    response.decode(Ok)?;
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
    let captured = obs::tests::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
    );
    let client = Client::connect("https://fixture.invalid", "fixture").await?;
    let response = client
        .unary((), |_, _| {
            Box::pin(async { Ok(Response::new(wire::AppendResponse::default())) })
        })
        .await?;
    assert!(
        !captured
            .fields("acyclic.stream.grpc.call")
            .contains_key("outcome")
    );
    assert!(response.decode(append_outcome_from_wire).is_err());
    let fields = captured.fields("acyclic.stream.grpc.call");
    assert_eq!(fields["outcome"], "err");
    assert_eq!(fields["error.kind"], "invalid_response");
    assert_eq!(
        fields["rpc.code"], "0",
        "unary successful trailers remain distinct from semantic rejection"
    );
    let mut pending = Box::pin(client.unary((), |_, _| {
        Box::pin(std::future::pending::<Result<Response<()>, Status>>())
    }));
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
