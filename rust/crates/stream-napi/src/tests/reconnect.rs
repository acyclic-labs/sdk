use super::*;
use parking_lot::Mutex as SyncMutex;
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
    isolated: AtomicBool,
    replacement_release: tokio::sync::Notify,
    replacement_ready: AtomicBool,
    handshakes: SyncMutex<std::collections::BTreeMap<usize, HandshakeState>>,
    subscriptions: SyncMutex<std::collections::BTreeMap<usize, Vec<wire::SessionSubscription>>>,
}
impl Outage {
    fn accept_replacement(&self) {
        self.replacement_ready.store(true, Ordering::SeqCst);
        self.replacement_release.notify_waiters();
    }
    fn handshake(&self, call: usize) -> Option<HandshakeState> {
        self.handshakes.lock().get(&call).copied()
    }
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum HandshakeState {
    Pending,
    Answered,
    Dropped,
}
struct PendingHandshake {
    outage: Arc<Outage>,
    call: usize,
    answered: bool,
}
impl PendingHandshake {
    fn new(outage: &Arc<Outage>, call: usize) -> Self {
        outage
            .handshakes
            .lock()
            .insert(call, HandshakeState::Pending);
        outage.changed.notify_one();
        Self {
            outage: Arc::clone(outage),
            call,
            answered: false,
        }
    }
    fn answered(&mut self) {
        self.answered = true;
        self.outage
            .handshakes
            .lock()
            .insert(self.call, HandshakeState::Answered);
        self.outage.changed.notify_one();
    }
}
impl Drop for PendingHandshake {
    fn drop(&mut self) {
        if !self.answered {
            self.outage
                .handshakes
                .lock()
                .insert(self.call, HandshakeState::Dropped);
            self.outage.changed.notify_one();
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
    provider: Arc<acyclic_stream::MemoryStream>,
    successor: Arc<acyclic_stream::MemoryStream>,
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
        let successor = self.outage.isolated.load(Ordering::SeqCst) && call > 5;
        let mut pending = None;
        let mut incoming = request.into_inner();
        let mut buffered = Vec::new();
        if call > 0 {
            // Cross a whole handshake attempt group before the deterministic
            // recovery gate. Unavailable is not a terminal established-interest error.
            if call <= 4 {
                return Err(Status::unavailable("fixture outage"));
            }
            if self.outage.terminal.load(Ordering::SeqCst) {
                return Err(Status::permission_denied("fixture authorization revoked"));
            }
            pending = Some(PendingHandshake::new(&self.outage, call));
            loop {
                let released = if successor {
                    self.outage.replacement_release.notified()
                } else {
                    self.outage.release.notified()
                };
                let ready = if successor {
                    &self.outage.replacement_ready
                } else {
                    &self.outage.recovered
                };
                if ready.load(Ordering::SeqCst) {
                    break;
                }
                // Closing the client's Outbound can arrive as request EOS,
                // not destruction of tonic's pre-header service future. The
                // held peer must observe that lifetime while withholding its
                // response. Preserve every received frame for canonical serve.
                tokio::select! {
                    () = released => {}
                    frame = incoming.next() => match frame {
                        Some(Ok(frame)) => buffered.push(Ok(frame)),
                        Some(Err(error)) => return Err(error),
                        None => return Err(Status::cancelled("request withdrawn before Welcome")),
                    }
                }
            }
        }
        let outage = Arc::clone(&self.outage);
        let input = futures::stream::iter(buffered)
            .chain(incoming)
            .map(move |frame| {
                if let Ok(wire::SessionFrame {
                    body: Some(wire::session_frame::Body::Interest(interest)),
                    ..
                }) = &frame
                {
                    outage
                        .subscriptions
                        .lock()
                        .entry(call)
                        .or_default()
                        .extend(interest.upsert.iter().cloned());
                    outage.changed.notify_one();
                }
                frame.map_err(|_| StreamError::Unavailable)
            })
            .boxed();
        let output = acyclic_stream::session::serve(
            Arc::clone(if successor {
                &self.successor
            } else {
                &self.provider
            }),
            "native".into(),
            Default::default(),
            input,
        )
        .map_err(|error| Status::invalid_argument(error.code()))?
        .map(|frame| frame.map_err(|error| Status::failed_precondition(error.code())));
        if let Some(pending) = &mut pending {
            pending.answered();
        }
        self.outage.outputs.fetch_add(1, Ordering::SeqCst);
        let owner = OutputOwner(Arc::clone(&self.outage));
        let outage = Arc::clone(&self.outage);
        let failure = futures::stream::once(async move {
            outage.disconnect.notified().await;
            Err(Status::unavailable("physical session disconnected"))
        });
        Ok(Response::new(
            futures::stream::select(output, failure)
                .inspect(move |_| {
                    let _owner = &owner;
                })
                .boxed(),
        ))
    }
}

struct Fixture {
    provider: Arc<acyclic_stream::MemoryStream>,
    client: Arc<grpc::Client>,
    outage: Arc<Outage>,
    successor: Arc<acyclic_stream::MemoryStream>,
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
        let successor = Arc::new(acyclic_stream::MemoryStream::default());
        let outage = Arc::new(Outage::default());
        outage.terminal.store(terminal, Ordering::SeqCst);
        let host = DisconnectingHost {
            provider: Arc::clone(&provider),
            successor: Arc::clone(&successor),
            outage: Arc::clone(&outage),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            listener
                .accept()
                .await
                .ok()
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
        let client = Arc::new(
            grpc::Client::connect_eager_with_ca_certificate(&endpoint, "fixture", &certificate)
                .await?,
        );
        Ok(Self {
            provider,
            client,
            outage,
            successor,
            _server: server,
        })
    }
    async fn append(&self, path: &str, value: u8) -> TestResult {
        self.provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![vec![value].into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        Ok(())
    }
    async fn children(
        &self,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> TestResult<NativeStreamFollow> {
        operations::open_children(
            &self.client,
            Buffer::from(
                wire::ChildrenRequest {
                    parent: Some("agents".into()),
                    limit: 1,
                }
                .encode_to_vec(),
            ),
            cancellation,
        )
        .await
        .map_err(|error| error.message.into())
    }
    async fn follow(
        &self,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> TestResult<NativeStreamFollow> {
        self.follow_path("events", cancellation).await
    }
    async fn follow_path(
        &self,
        path: &str,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> TestResult<NativeStreamFollow> {
        operations::open_follow(
            &self.client,
            Buffer::from(
                wire::FollowRequest {
                    filter: None,
                    path: path.into(),
                    from: 0,
                }
                .encode_to_vec(),
            ),
            cancellation,
        )
        .await
        .map_err(|error| error.message.into())
    }
    async fn outage(&self) {
        self.outage.disconnect.notify_one();
        self.outage
            .until(|| self.outage.calls.load(Ordering::SeqCst) >= 6)
            .await;
    }
    async fn retired(&self) {
        self.outage
            .until(|| {
                self.outage.outputs.load(Ordering::SeqCst) == 0
                    && self
                        .outage
                        .handshakes
                        .lock()
                        .values()
                        .all(|state| *state != HandshakeState::Pending)
            })
            .await;
    }
    async fn replacement_after_retirement(
        &self,
        old: usize,
        evidence: &SyncMutex<CancellationEvidence>,
    ) -> TestResult<(NativeStreamFollow, usize)> {
        // Poll Open while the old gate is closed; observe a distinct RPC before
        // allowing any response from either gated physical handshake.
        let opening = self.follow_path("replacement", None);
        futures::pin_mut!(opening);
        tokio::select! {
            () = self.outage.until(|| self.outage.handshakes.lock()
                .iter().any(|(call, state)| *call != old && *state == HandshakeState::Pending)) => {}
            _ = &mut opening => panic!("replacement opened without its own pending handshake"),
        }
        let successor = *self
            .outage
            .handshakes
            .lock()
            .iter()
            .find(|(call, state)| **call != old && **state == HandshakeState::Pending)
            .ok_or("replacement handshake disappeared")?
            .0;
        evidence.lock().gate = "old RPC withdrawal";
        self.outage
            .until(|| self.outage.handshake(old) == Some(HandshakeState::Dropped))
            .await;
        assert!(!self.outage.recovered.load(Ordering::SeqCst));
        self.outage.accept_replacement();
        evidence.lock().gate = "successor registration";
        Ok((opening.await?, successor))
    }
    async fn stop(mut self) {
        self._server.0.abort();
        assert!((&mut self._server.0).await.unwrap_err().is_cancelled());
    }
}

async fn child(cursor: &NativeStreamFollow) -> TestResult<String> {
    let next = cursor.next_result().await?;
    assert!(
        next.error.is_none(),
        "recovery must retain the original native interest"
    );
    let bytes = next.value.ok_or("children ended instead of recovering")?;
    Ok(wire::ChildrenResponse::decode(bytes.as_ref())?
        .child
        .ok_or("missing child")?
        .path)
}
async fn record(cursor: &NativeStreamFollow, sequence: u64, value: u8) -> TestResult {
    let next = cursor.next_result().await?;
    assert!(
        next.error.is_none(),
        "recovery must retain the original native interest"
    );
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
    tokio::time::timeout(Duration::from_secs(3), recovered_consumers(false))
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
#[should_panic(expected = "recovery must retain the original native interest")]
async fn negative_control_terminal_rejection_cannot_satisfy_the_recovery_oracle() {
    tokio::time::timeout(Duration::from_secs(3), recovered_consumers(true))
        .await
        .unwrap()
        .unwrap();
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
            assert_eq!(
                next.error.ok_or("terminal error became EOF")?.code,
                "access_denied"
            );
            end(cursor).await?;
            assert!(cursor.state.records.lock().await.is_none());
        }
        fixture.retired().await;
        fixture.stop().await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await
    .unwrap()
    .unwrap();
}

// Deliberate counterfactual: preserve the real provider cursor when the adapter
// retires its wrapper. No counters or transport outcomes are forged.
struct RetainCursorOnDrop {
    frames: Option<FollowFrames>,
    retained: Arc<SyncMutex<Option<FollowFrames>>>,
}
impl futures::Stream for RetainCursorOnDrop {
    type Item = std::result::Result<Vec<u8>, StreamError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut()
            .frames
            .as_mut()
            .unwrap()
            .as_mut()
            .poll_next(cx)
    }
}
impl Drop for RetainCursorOnDrop {
    fn drop(&mut self) {
        *self.retained.lock() = self.frames.take();
    }
}

#[derive(Default)]
struct CancellationEvidence {
    children: bool,
    action: u8,
    gate: &'static str,
    outage: Option<Arc<Outage>>,
    started: Option<std::time::Instant>,
    completed: Option<Duration>,
}
impl CancellationEvidence {
    fn describe(&self) -> String {
        let handshakes = self
            .outage
            .as_ref()
            .map(|outage| outage.handshakes.lock().clone());
        let action = match self.action {
            0 => "cancel",
            1 => "close",
            _ => "drop",
        };
        let elapsed = self
            .completed
            .or_else(|| self.started.map(|started| started.elapsed()));
        format!(
            "children={}, action={action}, gate={}, elapsed={elapsed:?}, observed handshakes={handshakes:?}",
            self.children, self.gate,
        )
    }
}

async fn cancellation_case(
    children: bool,
    action: u8,
    retain: bool,
    evidence: &SyncMutex<CancellationEvidence>,
) -> TestResult {
    *evidence.lock() = CancellationEvidence {
        children,
        action,
        gate: "fixture connection",
        outage: None,
        started: Some(std::time::Instant::now()),
        completed: None,
    };
    let fixture = Fixture::open(false).await?;
    evidence.lock().outage = Some(Arc::clone(&fixture.outage));
    evidence.lock().gate = "initial native registration";
    fixture.outage.isolated.store(true, Ordering::SeqCst);
    fixture.append("events", 10).await?;
    fixture
        .successor
        .append(AppendRequest {
            path: StreamPath::new("replacement")?,
            records: vec![vec![91].into(), vec![92].into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let cancellation = NativeStreamCancellation::new();
    let cursor = if children {
        fixture.children(Some(&cancellation)).await?
    } else {
        fixture.follow(Some(&cancellation)).await?
    };
    if !children {
        record(&cursor, 0, 10).await?;
    }
    let original_channel = fixture
        .outage
        .subscriptions
        .lock()
        .values()
        .flatten()
        .find_map(|subscription| {
            subscription
                .channel
                .as_ref()
                .map(|channel| channel.channel_id)
        })
        .ok_or("original interest never reached the provider")?;
    let retained = Arc::new(SyncMutex::new(None));
    if retain {
        let frames = cursor.state.records.lock().await.take().unwrap();
        *cursor.state.records.lock().await = Some(
            RetainCursorOnDrop {
                frames: Some(frames),
                retained: Arc::clone(&retained),
            }
            .boxed(),
        );
    }
    evidence.lock().gate = "unavailable handshake group";
    fixture.outage().await;
    // Identify an actually pending server RPC, not an expected call count.
    let old = *fixture
        .outage
        .handshakes
        .lock()
        .iter()
        .find(|(_, state)| **state == HandshakeState::Pending)
        .ok_or("old recovery never entered its handshake gate")?
        .0;
    evidence.lock().gate = "native cursor retirement";
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
            assert_eq!(
                next.error.ok_or("cancellation became EOF")?.code,
                "cancelled"
            );
        } else {
            let (next, ()) = tokio::join!(pending, cursor.close());
            let next = next?;
            assert!(next.value.is_none() && next.error.is_none());
        }
        end(&cursor).await?;
        assert!(cursor.state.records.lock().await.is_none());
    }
    evidence.lock().gate = "successor RPC arrival";
    let (replacement, successor) = fixture.replacement_after_retirement(old, evidence).await?;
    evidence.lock().gate = "replacement payload";
    record(&replacement, 0, 91).await?;
    record(&replacement, 1, 92).await?;
    // The fresh payload alone is insufficient: a leaked original cursor could
    // share this session. Audit real inbound subscription identities as well.
    let subscriptions = fixture.outage.subscriptions.lock();
    let successor_interests = subscriptions
        .get(&successor)
        .ok_or("replacement has no interests")?;
    assert!(
        successor_interests.iter().all(|subscription| {
            subscription.channel.as_ref().is_some_and(|channel| channel.channel_id != original_channel)
                && matches!(
                    &subscription.kind,
                    Some(wire::session_subscription::Kind::Follow(follow)) if follow.resource == "streams:/replacement"
                )
        }),
        "retired native interest crossed into the replacement session",
    );
    drop(subscriptions);
    assert_eq!(fixture.outage.handshake(old), Some(HandshakeState::Dropped));
    // Only now release the obsolete gate. It cannot carry replacement data.
    fixture.outage.recover();
    replacement.close().await;
    drop(retained);
    evidence.lock().gate = "all peer resources retired";
    fixture.retired().await;
    evidence.lock().gate = "fixture server joined";
    fixture.stop().await;
    let mut evidence = evidence.lock();
    evidence.completed = evidence.started.map(|started| started.elapsed());
    evidence.gate = "completed";
    Ok(())
}

#[tokio::test]
async fn cancellation_close_and_drop_release_native_interests_during_recovery() {
    // Independent endpoints and owners must not consume one another's clock.
    // Keep the same whole matrix and deadline, just as in the counterfactual.
    let cases: Vec<_> = [false, true]
        .into_iter()
        .flat_map(|children| {
            (0..3).map(move |action| {
                (
                    children,
                    action,
                    SyncMutex::new(CancellationEvidence::default()),
                )
            })
        })
        .collect();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        futures::future::join_all(cases.iter().map(|(children, action, evidence)| {
            cancellation_case(*children, *action, false, evidence)
        })),
    )
    .await;
    match result {
        Ok(results) => {
            for result in results {
                result.unwrap();
            }
        }
        Err(error) => {
            let snapshots: Vec<_> = cases
                .iter()
                .map(|(_, _, evidence)| evidence.lock().describe())
                .collect();
            panic!(
                "native retirement deadline: {error}; {}",
                snapshots.join("; ")
            );
        }
    }
}

#[tokio::test]
async fn retained_provider_cursor_fails_the_same_cancellation_close_and_drop_oracle() {
    use futures::FutureExt as _;
    let results = tokio::time::timeout(
        Duration::from_secs(3),
        futures::future::join_all([false, true].into_iter().flat_map(|children| {
            (0..3).map(move |action| async move {
                let evidence = SyncMutex::new(CancellationEvidence::default());
                std::panic::AssertUnwindSafe(cancellation_case(children, action, true, &evidence))
                    .catch_unwind()
                    .await
            })
        })),
    )
    .await
    .expect("counterfactual must fail semantically, not by timeout");
    for result in results {
        let error = result.expect_err("retained original cursor escaped the lifecycle oracle");
        let message = error
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| error.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("retired native interest crossed into the replacement session"),
            "{message}"
        );
    }
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
