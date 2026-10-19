use super::*;
use std::sync::atomic::AtomicUsize;
use tonic::{Response, Streaming};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct Outage {
    calls: AtomicUsize,
    changed: tokio::sync::Notify,
    disconnect: tokio::sync::Notify,
    release: tokio::sync::Notify,
    recovered: AtomicBool,
    terminal: AtomicBool,
    outputs: AtomicUsize,
}
impl Outage {
    fn recover(&self) {
        self.recovered.store(true, Ordering::SeqCst);
        self.release.notify_waiters();
    }
    async fn until(&self, predicate: impl Fn() -> bool) {
        loop {
            let changed = self.changed.notified();
            if predicate() {
                return;
            }
            changed.await;
        }
    }
}
struct OutputOwner(Arc<Outage>);
impl Drop for OutputOwner {
    fn drop(&mut self) {
        self.0.outputs.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}
struct DisconnectingHost {
    service: acyclic_stream::session::Service<acyclic_stream::MemoryStream>,
    outage: Arc<Outage>,
}
#[tonic::async_trait]
impl StreamSessionService for DisconnectingHost {
    type SessionStream =
        futures::stream::BoxStream<'static, std::result::Result<wire::SessionFrame, Status>>;
    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> std::result::Result<Response<Self::SessionStream>, Status> {
        let call = self.outage.calls.fetch_add(1, Ordering::SeqCst);
        self.outage.changed.notify_one();
        if call > 0 {
            // Cross a whole handshake attempt group before the deterministic
            // recovery gate. Unavailable is not a terminal established-interest error.
            if call <= 4 {
                return Err(Status::unavailable("fixture outage"));
            }
            if self.outage.terminal.load(Ordering::SeqCst) {
                return Err(Status::permission_denied("fixture authorization revoked"));
            }
            loop {
                let released = self.outage.release.notified();
                if self.outage.recovered.load(Ordering::SeqCst) {
                    break;
                }
                released.await;
            }
        }
        let output = self.service.session(request).await?.into_inner();
        self.outage.outputs.fetch_add(1, Ordering::SeqCst);
        let owner = OutputOwner(Arc::clone(&self.outage));
        let outage = Arc::clone(&self.outage);
        let failure = futures::stream::once(async move {
            outage.disconnect.notified().await;
            Err(Status::unavailable("physical session disconnected"))
        });
        Ok(Response::new(
            futures::stream::select(output, failure)
                .inspect(move |_| { let _owner = &owner; })
                .boxed(),
        ))
    }
}

struct Fixture {
    provider: Arc<acyclic_stream::MemoryStream>,
    client: Arc<grpc::Client>,
    outage: Arc<Outage>,
    _server: ServerOwner,
}
struct ServerOwner(tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>);
impl Drop for ServerOwner {
    fn drop(&mut self) {
        // Also covers setup failure/unwind; no detached listening server survives.
        self.0.abort();
    }
}
impl Fixture {
    async fn open(terminal: bool) -> TestResult<Self> {
        let identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate = identity.cert.pem();
        let key = identity.signing_key.serialize_pem();
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let outage = Arc::new(Outage::default());
        outage.terminal.store(terminal, Ordering::SeqCst);
        let host = DisconnectingHost {
            service: acyclic_stream::session::Service::new(
                Arc::clone(&provider), "native".into(), Default::default(),
            ),
            outage: Arc::clone(&outage),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            listener.accept().await.ok()
                .map(|(socket, _)| (Ok::<_, std::io::Error>(socket), listener))
        });
        let server = tokio::spawn(
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(&certificate, key)))?
                .add_service(StreamSessionServiceServer::new(host))
                .serve_with_incoming(incoming),
        );
        // Own the abort guard before awaiting connection setup.
        let server = ServerOwner(server);
        let client = Arc::new(grpc::Client::connect_eager_with_ca_certificate(
            &endpoint, "fixture", &certificate,
        ).await?);
        Ok(Self { provider, client, outage, _server: server })
    }
    async fn append(&self, path: &str, value: u8) -> TestResult {
        self.provider.append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![vec![value].into()],
            if_tail: None,
            idempotency_key: None,
        }).await?;
        Ok(())
    }
    async fn children(&self, cancellation: Option<&NativeStreamCancellation>) -> TestResult<NativeStreamFollow> {
        operations::open_children(&self.client, Buffer::from(wire::ChildrenRequest {
            parent: Some("agents".into()), limit: 1,
        }.encode_to_vec()), cancellation).await.map_err(|error| error.message.into())
    }
    async fn follow(&self, cancellation: Option<&NativeStreamCancellation>) -> TestResult<NativeStreamFollow> {
        operations::open_follow(&self.client, Buffer::from(wire::FollowRequest {
            path: "events".into(), from: 0,
        }.encode_to_vec()), cancellation).await.map_err(|error| error.message.into())
    }
    async fn outage(&self) {
        self.outage.disconnect.notify_one();
        self.outage.until(|| self.outage.calls.load(Ordering::SeqCst) >= 6).await;
    }
    async fn retired(&self) {
        self.outage.until(|| self.outage.outputs.load(Ordering::SeqCst) == 0).await;
    }
    async fn stop(mut self) {
        self._server.0.abort();
        assert!((&mut self._server.0).await.unwrap_err().is_cancelled());
    }
}

async fn child(cursor: &NativeStreamFollow) -> TestResult<String> {
    let next = cursor.next_result().await?;
    assert!(next.error.is_none(), "recovery must retain the original native interest");
    let bytes = next.value.ok_or("children ended instead of recovering")?;
    Ok(wire::ChildrenResponse::decode(bytes.as_ref())?.child.ok_or("missing child")?.path)
}
async fn record(cursor: &NativeStreamFollow, sequence: u64, value: u8) -> TestResult {
    let next = cursor.next_result().await?;
    assert!(next.error.is_none(), "recovery must retain the original native interest");
    let bytes = next.value.ok_or("follow ended instead of recovering")?;
    let records = wire_codec::read_response_records(wire::ReadResponse::decode(bytes.as_ref())?)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].sequence, sequence);
    assert_eq!(records[0].value.as_ref(), &[value]);
    Ok(())
}
async fn end(cursor: &NativeStreamFollow) -> TestResult {
    let next = cursor.next_result().await?;
    assert!(next.value.is_none() && next.error.is_none());
    Ok(())
}
async fn recovered_consumers(terminal: bool) -> TestResult {
    let fixture = Fixture::open(terminal).await?;
    fixture.append("agents/first", 1).await?;
    fixture.append("events", 10).await?;
    let children = fixture.children(None).await?;
    let records = fixture.follow(None).await?;
    assert_eq!(child(&children).await?, "agents/first");
    record(&records, 0, 10).await?;
    tokio::select! {
        () = fixture.outage() => {}
        next = children.next_result() => {
            let next = next?;
            assert!(
                next.error.is_none() && next.value.is_some(),
                "recovery must retain the original native interest",
            );
            panic!("children replayed a consumed value during the unavailable gap");
        }
        next = records.next_result() => {
            let next = next?;
            assert!(
                next.error.is_none() && next.value.is_some(),
                "recovery must retain the original native interest",
            );
            panic!("follow replayed a consumed value during the unavailable gap");
        }
    }
    fixture.append("agents/second", 2).await?;
    fixture.append("events", 11).await?;
    fixture.append("events", 12).await?;
    fixture.outage.recover();
    // The same retained cursors must resume, not new subscriptions hiding loss.
    assert_eq!(child(&children).await?, "agents/second");
    record(&records, 1, 11).await?;
    record(&records, 2, 12).await?;
    fixture.append("events", 13).await?;
    record(&records, 3, 13).await?;
    children.close().await;
    records.close().await;
    assert!(children.state.records.lock().await.is_none());
    assert!(records.state.records.lock().await.is_none());
    end(&children).await?;
    end(&records).await?;
    fixture.retired().await;
    fixture.stop().await;
    Ok(())
}

#[tokio::test]
async fn native_children_and_follow_recover_exactly_once_after_unavailable_attempt_groups() {
    tokio::time::timeout(Duration::from_secs(3), recovered_consumers(false)).await.unwrap().unwrap();
}
#[tokio::test]
#[should_panic(expected = "recovery must retain the original native interest")]
async fn negative_control_terminal_rejection_cannot_satisfy_the_recovery_oracle() {
    tokio::time::timeout(Duration::from_secs(3), recovered_consumers(true)).await.unwrap().unwrap();
}

#[tokio::test]
async fn terminal_reconnect_rejection_is_typed_once_for_both_native_entrypoints() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let fixture = Fixture::open(true).await?;
        fixture.append("events", 10).await?;
        let children = fixture.children(None).await?;
        let records = fixture.follow(None).await?;
        record(&records, 0, 10).await?;
        fixture.outage().await;
        for cursor in [&children, &records] {
            let next = cursor.next_result().await?;
            assert!(next.value.is_none());
            assert_eq!(next.error.ok_or("terminal error became EOF")?.code, "access_denied");
            end(cursor).await?;
            assert!(cursor.state.records.lock().await.is_none());
        }
        fixture.retired().await;
        fixture.stop().await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }).await.unwrap().unwrap();
}

#[tokio::test]
async fn cancellation_close_and_drop_release_native_interests_during_recovery() {
    tokio::time::timeout(Duration::from_secs(3), async {
        for children in [false, true] {
            for action in 0..3 {
                let fixture = Fixture::open(false).await?;
                fixture.append("events", 10).await?;
                let cancellation = NativeStreamCancellation::new();
                let cursor = if children {
                    fixture.children(Some(&cancellation)).await?
                } else {
                    fixture.follow(Some(&cancellation)).await?
                };
                if !children { record(&cursor, 0, 10).await?; }
                fixture.outage().await;
                if action == 2 {
                    let owner = Arc::downgrade(&cursor.state.records);
                    drop(cursor);
                    assert!(owner.upgrade().is_none(), "drop retained the native cursor");
                } else {
                    let pending = cursor.next_result();
                    futures::pin_mut!(pending);
                    assert!(futures::poll!(&mut pending).is_pending());
                    if action == 0 {
                        cancellation.cancel();
                        let next = pending.await?;
                        assert!(next.value.is_none());
                        assert_eq!(next.error.ok_or("cancellation became EOF")?.code, "cancelled");
                    } else {
                        let (next, ()) = tokio::join!(pending, cursor.close());
                        let next = next?;
                        assert!(next.value.is_none() && next.error.is_none());
                    }
                    end(&cursor).await?;
                    assert!(cursor.state.records.lock().await.is_none());
                }
                // A live replacement must not inherit the abandoned dial. Its
                // released gate also lets a deliberately retained old dial race.
                fixture.outage.recover();
                let replacement = fixture.follow(None).await?;
                record(&replacement, 0, 10).await?;
                replacement.close().await;
                fixture.retired().await;
                fixture.stop().await;
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }).await.unwrap().unwrap();
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn error_after_any_buffered_prefix_is_observable(
        prefix in proptest::collection::vec(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..16), 0..8),
    ) {
        let frames: FollowFrames = futures::stream::iter(prefix.iter().cloned().map(Ok)
            .chain([Err(StreamError::Unavailable)])
            .collect::<Vec<_>>()).boxed();
        let follow = NativeStreamFollow {
            state: FollowState {
                records: Arc::new(Mutex::new(Some(frames))),
                token: CancellationToken::new(),
                closed: Arc::new(AtomicBool::new(false)),
            },
        };
        futures::executor::block_on(async {
            for expected in prefix {
                let next = follow.next_result().await.unwrap();
                proptest::prop_assert!(next.error.is_none());
                let value = next.value.unwrap();
                proptest::prop_assert_eq!(value.as_ref(), expected.as_slice());
            }
            let error = follow.next_result().await.unwrap();
            proptest::prop_assert!(error.value.is_none());
            proptest::prop_assert_eq!(error.error.expect("terminal failure was swallowed").code, "unavailable");
            let end = follow.next_result().await.unwrap();
            proptest::prop_assert!(end.value.is_none() && end.error.is_none());
            Ok(())
        })?;
    }
}
