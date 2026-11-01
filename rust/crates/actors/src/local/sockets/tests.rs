use super::*;
use crate::conformance::Failure;
use crate::conformance::sockets::{self as conformance, Peer, SocketBackend, SocketPackage};
use crate::local::{Runtime, SystemClock};
use acyclic_auth::contract::{ResourceGrant, ResourcePattern};
use acyclic_stream::runtime::PlatformExecutor;
use acyclic_stream::session::{ActorSocket, Client, testing};
use acyclic_stream::{BoxProviderFuture, MemoryStream};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn root() -> Result<StreamPath, StreamError> {
    StreamPath::new("harness/actors")
}

/// One incarnation of the local host: its runtime, socket host and session.
struct Host {
    task: tokio::task::JoinHandle<()>,
    client: Client,
}

/// `local::Runtime` plus `local::Sockets` behind the socket suite, reached
/// through an in-process session exactly as a network peer would be.
struct LocalSockets {
    store: Store<MemoryStream>,
    host: fn(Sockets<MemoryStream>) -> Arc<dyn SocketHost>,
    /// The current host is last. Crashed hosts stay alive, holding their
    /// sockets, as a crashed process's peers would.
    hosts: std::sync::Mutex<Vec<Host>>,
}

impl LocalSockets {
    async fn start(host: fn(Sockets<MemoryStream>) -> Arc<dyn SocketHost>) -> Result<Self, Error> {
        let backend = Self {
            store: Store::new(Arc::new(MemoryStream::default()), root()?),
            host,
            hosts: std::sync::Mutex::new(Vec::new()),
        };
        backend.launch().await?;
        Ok(backend)
    }

    async fn launch(&self) -> Result<(), Error> {
        let provider = Arc::clone(self.store.provider());
        let sockets = Sockets::open(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(PlatformExecutor),
            Arc::new(SystemClock),
        )
        .await?;
        let package = SocketPackage::new((self.host)(sockets.clone()))
            .map_err(|_| Error::Invalid("socket package"))?;
        let runtime = Runtime::new(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(package),
            Arc::new(SystemClock),
        );
        let task = tokio::spawn(async move {
            let _outcome = runtime.run(|_, _| {}).await;
        });
        let client = Client::in_process_with_sockets(
            provider,
            testing::authority(),
            None,
            Arc::new(sockets),
            Arc::new(PlatformExecutor),
            None,
        );
        client.register_credential(
            0,
            testing::credential_with_grants(vec![ResourceGrant {
                resource: ResourcePattern {
                    name: "actors:/sockets".into(),
                    prefix: false,
                },
                actions: vec!["actors/invoke".into()],
            }]),
        )?;
        self.hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Host { task, client });
        Ok(())
    }

    fn client(&self) -> Result<Client, Error> {
        self.hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last()
            .map(|host| host.client.clone())
            .ok_or(Error::Invalid("socket host stopped"))
    }
}

struct SessionPeer(ActorSocket);

impl Peer for SessionPeer {
    fn send(&self, frame: SocketFrame) -> BoxProviderFuture<'_, Result<(), Error>> {
        Box::pin(async move { Ok(self.0.send(frame.encode()).await?) })
    }
    fn next(&mut self) -> BoxProviderFuture<'_, Result<Option<SocketFrame>, Error>> {
        Box::pin(async move {
            match self.0.next().await? {
                Some(frame) => Ok(Some(SocketFrame::decode(&frame)?)),
                None => Ok(None),
            }
        })
    }
}

impl SocketBackend for LocalSockets {
    type Provider = MemoryStream;
    fn store(&self) -> &Store<MemoryStream> {
        &self.store
    }
    fn account(&self) -> &[u8] {
        b"account"
    }
    fn clock(&self) -> &dyn Clock {
        &SystemClock
    }
    fn connect(&self, open: SocketOpen) -> BoxProviderFuture<'_, Result<Box<dyn Peer>, Error>> {
        Box::pin(async move {
            let socket = self.client()?.actor_socket(open.encode(), 0).await?;
            Ok(Box::new(SessionPeer(socket)) as Box<dyn Peer>)
        })
    }
    fn restart(&self) -> BoxProviderFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            let task = self
                .hosts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .last()
                .map(|host| host.task.abort_handle());
            // An aborted runtime is the crash being modelled; its socket host
            // and peers stay alive and are fenced by the next incarnation.
            if let Some(task) = task {
                task.abort();
            }
            self.launch().await
        })
    }
}

fn faithful(sockets: Sockets<MemoryStream>) -> Arc<dyn SocketHost> {
    Arc::new(sockets)
}

async fn local_backend() -> Result<LocalSockets, Error> {
    LocalSockets::start(faithful).await
}

conformance::suite!(socket_conformance_passes_on_local_runtime, local_backend);

#[tokio::test]
async fn socket_admission_binds_namespace_grants_to_the_certified_account() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let sockets = Sockets::open(
        Store::new(Arc::clone(&provider), root()?),
        Arc::new(PlatformExecutor),
        Arc::new(SystemClock),
    )
    .await?;
    let client = Client::in_process_with_sockets(
        provider,
        testing::authority(),
        None,
        Arc::new(sockets.clone()),
        Arc::new(PlatformExecutor),
        None,
    );
    client.register_credential(
        0,
        testing::credential_with_grants(vec![ResourceGrant {
            resource: ResourcePattern {
                name: "actors:/sockets".into(),
                prefix: false,
            },
            actions: vec!["actors/invoke".into()],
        }]),
    )?;
    let mut open = SocketOpen {
        actor: ActorReference::new(b"foreign", b"sockets", b"target")?,
        subscription: "inbox".into(),
        app_channel: None,
        subprotocols: Vec::new(),
        headers: Vec::new(),
        resume_after: 0,
        reconnected: false,
    };
    let next = sockets.shared.next.load(Ordering::Relaxed);
    let ledger = sockets.shared.layout.segment(1)?;
    assert!(matches!(
        records(&sockets.shared.store, &ledger).await,
        Err(Error::Stream(StreamError::NotFound))
    ));
    let refused = match client.actor_socket(open.encode(), 0).await {
        Err(error) => Some(error),
        Ok(mut socket) => socket.next().await.err(),
    };
    assert_eq!(refused, Some(StreamError::AccessDenied));
    assert_eq!(sockets.shared.next.load(Ordering::Relaxed), next);
    assert!(sockets.shared.lock().sockets.is_empty());
    assert!(sockets.shared.lock().actors.is_empty());
    assert!(matches!(
        records(&sockets.shared.store, &ledger).await,
        Err(Error::Stream(StreamError::NotFound))
    ));

    // The same signed credential admits its own account's namespace. Actor
    // existence is subsequently decided by the original Open publication.
    open.actor = ActorReference::new(b"account", b"sockets", b"target")?;
    let mut admitted = client.actor_socket(open.encode(), 0).await?;
    assert!(matches!(
        admitted
            .next()
            .await?
            .map(|frame| SocketFrame::decode(&frame)),
        Some(Ok(SocketFrame::Rejected { .. }))
    ));
    assert_eq!(sockets.shared.next.load(Ordering::Relaxed), next + 1);

    open.actor = ActorReference::new(b"account", b"ungranted", b"target")?;
    let refused = match client.actor_socket(open.encode(), 0).await {
        Err(error) => Some(error),
        Ok(mut socket) => socket.next().await.err(),
    };
    assert_eq!(refused, Some(StreamError::AccessDenied));
    assert_eq!(sockets.shared.next.load(Ordering::Relaxed), next + 1);
    Ok(())
}

/// A host that forgets the auto-response pair wakes the actor for every
/// keepalive: the broken behaviour the suite must reject.
struct ForgetsAutoResponse(Sockets<MemoryStream>);

impl SocketHost for ForgetsAutoResponse {
    fn accept_web_socket(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        tags: &[String],
    ) -> Result<(), GuestError> {
        self.0.accept_web_socket(actor, socket, tags)
    }
    fn get_web_sockets(
        &self,
        actor: &ActorReference,
        tag: Option<&str>,
    ) -> Result<Vec<WebSocketId>, GuestError> {
        self.0.get_web_sockets(actor, tag)
    }
    fn web_socket_send(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        payload: SocketPayload,
    ) -> Result<(), GuestError> {
        self.0.web_socket_send(actor, socket, payload)
    }
    fn web_socket_close(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        code: u16,
        reason: &str,
    ) -> Result<(), GuestError> {
        self.0.web_socket_close(actor, socket, code, reason)
    }
    fn serialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        bytes: Bytes,
    ) -> Result<(), GuestError> {
        self.0.serialize_attachment(actor, socket, bytes)
    }
    fn deserialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Option<Bytes>, GuestError> {
        self.0.deserialize_attachment(actor, socket)
    }
    fn set_web_socket_auto_response(
        &self,
        _: &ActorReference,
        _: Option<WebSocketAutoResponse>,
    ) -> Result<(), GuestError> {
        Ok(())
    }
    fn get_web_socket_auto_response(
        &self,
        actor: &ActorReference,
    ) -> Result<Option<WebSocketAutoResponse>, GuestError> {
        self.0.get_web_socket_auto_response(actor)
    }
    fn get_web_socket_auto_response_timestamp(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Option<u64>, GuestError> {
        self.0.get_web_socket_auto_response_timestamp(actor, socket)
    }
    fn get_tags(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Vec<String>, GuestError> {
        self.0.get_tags(actor, socket)
    }
}

#[tokio::test]
async fn socket_conformance_rejects_a_host_that_wakes_actors_for_keepalives() -> TestResult {
    let backend = LocalSockets::start(|sockets| Arc::new(ForgetsAutoResponse(sockets))).await?;
    match conformance::verify(&backend).await {
        Err(Failure::Violation { case: "auto", .. }) => Ok(()),
        other => Err(format!("broken auto-response was not rejected: {other:?}").into()),
    }
}

async fn records(store: &Store<MemoryStream>, path: &StreamPath) -> Result<Vec<Bytes>, Error> {
    Ok(store
        .provider()
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: 0,
            limit: PAGE,
            at_least: RevisionCut::default(),
        })
        .await?
        .map_ok(|record| Bytes::copy_from_slice(record.value.as_ref()))
        .try_collect()
        .await?)
}

#[tokio::test]
async fn compaction_keeps_only_open_sockets_for_recovery() -> TestResult {
    let store = || -> Result<Store<MemoryStream>, Error> {
        Ok(Store::new(Arc::new(MemoryStream::default()), root()?))
    };
    let first = store()?;
    let provider = Arc::clone(first.provider());
    let sockets = Sockets::open(first, Arc::new(PlatformExecutor), Arc::new(SystemClock)).await?;
    let shared = &sockets.shared;
    let layout = shared.layout.clone();
    let actor = ActorReference::new(b"account", b"sockets", b"compacted")?;
    let inbox = StreamPath::new("compacted/inbox")?;
    let opened = |connection: &str| {
        ledger_record(LedgerKind::Opened(OpenedWire {
            connection: connection.into(),
            actor: actor.encode().into(),
            inbox: inbox.as_str().into(),
        }))
    };
    // Three opens, two closes, and the one survivor's socket stream.
    let survivor = layout.socket("1-2")?;
    assert!(
        commit(
            &shared.store,
            &*shared.executor,
            layout.key(&[b"fixture"])?,
            vec![
                append(
                    layout.segment(1)?,
                    vec![
                        opened("1-0"),
                        opened("1-1"),
                        ledger_record(LedgerKind::Closed("1-0".into())),
                        opened("1-2"),
                        ledger_record(LedgerKind::Closed("1-1".into())),
                    ],
                    Vec::new(),
                ),
                append(
                    survivor.clone(),
                    vec![Bytes::from_static(b"opened").into()],
                    vec![CommitCondition::Absent {
                        path: survivor.clone(),
                    }],
                ),
            ],
        )
        .await?
    );
    shared.compact().await?;
    let compacted = records(&shared.store, &layout.segment(2)?).await?;
    assert_eq!(compacted.len(), 1, "compaction kept closed sockets");
    // Negative control: the retired segment still names all three opens.
    assert_eq!(records(&shared.store, &layout.segment(1)?).await?.len(), 5);
    drop(sockets);

    // The next incarnation closes exactly the survivor, from segment 2 only.
    let restarted = Store::new(provider, root()?);
    let recovered =
        Sockets::open(restarted, Arc::new(PlatformExecutor), Arc::new(SystemClock)).await?;
    let store = &recovered.shared.store;
    assert_eq!(records(store, &survivor).await?.len(), 2);
    let delivered = records(store, &inbox).await?;
    assert_eq!(
        delivered
            .iter()
            .map(|value| crate::sockets::input_kind(value))
            .collect::<Vec<_>>(),
        vec![Some("close")]
    );
    assert_eq!(recovered.shared.incarnation, 3);
    Ok(())
}

/// MemoryStream whose first two `heads` reads meet before either returns,
/// so two hosts opening at once observe the same epochs tail.
struct MeetingHeads {
    inner: MemoryStream,
    meet: tokio::sync::Barrier,
    reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl StreamProvider for MeetingHeads {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }
    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> Result<acyclic_stream::Heads, StreamError> {
        let heads = self.inner.heads(request).await;
        if self.reads.fetch_add(1, Ordering::AcqRel) < 2 {
            self.meet.wait().await;
        }
        heads
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn read(
        &self,
        request: ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }
    fn write(
        &self,
        request: acyclic_stream::WriteRequest,
    ) -> BoxProviderFuture<'_, Result<acyclic_stream::WriteOutcome, StreamError>> {
        self.inner.write(request)
    }
}

#[tokio::test]
async fn concurrent_hosts_cannot_share_an_incarnation() -> TestResult {
    let provider = Arc::new(MeetingHeads {
        inner: MemoryStream::default(),
        meet: tokio::sync::Barrier::new(2),
        reads: std::sync::atomic::AtomicUsize::new(0),
    });
    let open = || -> Result<_, Error> {
        Ok(Sockets::open(
            Store::new(Arc::clone(&provider), root()?),
            Arc::new(PlatformExecutor),
            Arc::new(SystemClock),
        ))
    };
    let (first, second) = tokio::join!(open()?, open()?);
    // Both fenced the same tail. Identical fence keys would let the second
    // replay the first's result and reuse its connection ids.
    match (first, second) {
        (Ok(_), Err(Error::Conflict)) | (Err(Error::Conflict), Ok(_)) => Ok(()),
        (first, second) => Err(format!(
            "concurrent opens did not exclude each other: {:?} {:?}",
            first.map(|sockets| sockets.shared.incarnation),
            second.map(|sockets| sockets.shared.incarnation),
        )
        .into()),
    }
}

/// Retains actual owned socket work until this deterministic executor runs or
/// drops it. All publications still use the original MemoryStream producer.
#[derive(Default)]
struct HeldCloseExecutor {
    tasks: std::sync::Mutex<VecDeque<BoxProviderFuture<'static, ()>>>,
}

impl Executor for HeldCloseExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        self.tasks.lock().unwrap().push_back(future);
    }

    fn sleep(&self, _milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(async { panic!("a decided socket operation must not retry") })
    }
}

impl HeldCloseExecutor {
    fn take(&self) -> Result<BoxProviderFuture<'static, ()>, &'static str> {
        self.tasks
            .lock()
            .unwrap()
            .pop_front()
            .ok_or("missing owned close task")
    }
}

/// Prepare an original actor and socket entry before its Open is submitted.
async fn close_fixture(
    sockets: &Sockets<MemoryStream>,
    label: &str,
) -> Result<
    (
        String,
        StreamPath,
        oneshot::Receiver<Bytes>,
        SocketOpen,
        mpsc::Receiver<Bytes>,
    ),
    Box<dyn std::error::Error>,
> {
    let shared = &sockets.shared;
    let actor = ActorReference::new(b"account", b"sockets", label.as_bytes())?;
    let inbox = StreamPath::new(format!("close/{label}"))?;
    let mut publication = PreparedPublish::foreign(IdempotencyKey::new(label.as_bytes().to_vec())?);
    for effect in shared.store.create(
        &actor,
        BTreeMap::from([(
            "inbox".into(),
            super::super::Subscription {
                path: inbox.clone(),
                cursor: 0,
            },
        )]),
        None,
    )? {
        publication.join(effect)?;
    }
    PreparedPublish::settle(
        &shared
            .store
            .provider()
            .commit(publication.into_request()?)
            .await?,
    )?;
    let connection = format!(
        "{}-{}",
        shared.incarnation,
        shared.next.fetch_add(1, Ordering::Relaxed)
    );
    let (outbound, frames) = mpsc::channel(acyclic_stream::MAX_ITEMS);
    let (final_frame, last) = oneshot::channel();
    let (abandon, _abandoned) = oneshot::channel();
    shared
        .lock()
        .actors
        .entry(actor.clone())
        .or_default()
        .insert(connection.clone());
    shared.lock().sockets.insert(
        connection.clone(),
        Entry {
            actor: actor.clone(),
            inbox: None,
            phase: Phase::Unborn,
            tags: Vec::new(),
            attachment: None,
            answered_at: None,
            outbound,
            final_frame: Some(final_frame),
            messages: 0,
            inbound: VecDeque::new(),
            draining: false,
            peer_closed: false,
            completion: None,
            completed: None,
            _abandon: abandon,
        },
    );
    let open = SocketOpen {
        actor,
        subscription: "inbox".into(),
        app_channel: None,
        subprotocols: Vec::new(),
        headers: Vec::new(),
        resume_after: 0,
        reconnected: false,
    };
    Ok((connection, inbox, last, open, frames))
}

/// The actual Open creates its invocation, socket stream and ledger. Only the
/// guest acceptance is supplied here; close custody starts afterwards.
async fn admitted_close_fixture(
    sockets: &Sockets<MemoryStream>,
    label: &str,
) -> Result<
    (
        String,
        StreamPath,
        oneshot::Receiver<Bytes>,
        mpsc::Receiver<Bytes>,
    ),
    Box<dyn std::error::Error>,
> {
    let (connection, inbox, last, open, frames) = close_fixture(sockets, label).await?;
    let shared = &sockets.shared;
    assert!(matches!(
        shared.commit_open(&connection, &open).await?,
        Opened::Committed(_)
    ));
    shared
        .lock()
        .sockets
        .get_mut(&connection)
        .ok_or(Error::Invalid("fixture lost its original open"))?
        .phase = Phase::Open;
    Ok((connection, inbox, last, frames))
}

#[tokio::test]
async fn automatic_reply_admission_preserves_the_open_socket_at_the_frame_bound() -> TestResult {
    let executor = Arc::new(HeldCloseExecutor::default());
    let sockets = Sockets::open(
        Store::new(Arc::new(MemoryStream::default()), root()?),
        executor.clone(),
        Arc::new(SystemClock),
    )
    .await?;
    let shared = &sockets.shared;
    let (connection, inbox, last, mut frames) =
        admitted_close_fixture(&sockets, "automatic-reply-bound").await?;
    let actor = shared
        .lock()
        .sockets
        .get(&connection)
        .unwrap()
        .actor
        .clone();
    let maximum = acyclic_stream::MAX_RECORD_BYTES;
    let seed = "x".repeat(maximum - 128);
    let overhead = SocketFrame::Message(SocketPayload::Text { text: seed.clone() })
        .encode()
        .len()
        - seed.len();
    let boundary = WebSocketAutoResponse {
        request: "ping".into(),
        response: "x".repeat(maximum - overhead),
    };
    let expected = SocketFrame::Message(SocketPayload::Text {
        text: boundary.response.clone(),
    });
    assert_eq!(expected.encode().len(), maximum);
    sockets.set_web_socket_auto_response(&actor, Some(boundary.clone()))?;
    for length in [maximum - overhead + 1, maximum] {
        assert_eq!(
            sockets.set_web_socket_auto_response(
                &actor,
                Some(WebSocketAutoResponse {
                    request: "replacement".into(),
                    response: "x".repeat(length),
                }),
            ),
            Err(host_error(
                "WebSocket frame exceeds the channel record bound"
            )),
            "a refused update cannot replace the original automatic reply"
        );
        shared.message(
            &connection,
            SocketPayload::Text {
                text: "ping".into(),
            },
        )?;
        let frame = frames.try_recv()?;
        assert_eq!(frame.len(), maximum);
        assert_eq!(SocketFrame::decode(&frame)?, expected);
        assert!(executor.tasks.lock().unwrap().is_empty());
        assert!(!shared.lock().sockets.get(&connection).unwrap().peer_closed);
    }
    // Empty text still carries the present oneof field; it is a valid reply.
    sockets.set_web_socket_auto_response(
        &actor,
        Some(WebSocketAutoResponse {
            request: "ping".into(),
            response: String::new(),
        }),
    )?;
    shared.message(
        &connection,
        SocketPayload::Text {
            text: "ping".into(),
        },
    )?;
    assert_eq!(
        SocketFrame::decode(&frames.try_recv()?)?,
        SocketFrame::Message(SocketPayload::Text {
            text: String::new()
        })
    );
    assert_eq!(records(&shared.store, &inbox).await?.len(), 1);
    assert!(executor.tasks.lock().unwrap().is_empty());
    shared.peer_close(
        &connection,
        Close::new(1000, "done", SocketCloseInitiator::Peer),
    )?;
    executor.take()?.await;
    assert!(matches!(
        SocketFrame::decode(&last.await?)?,
        SocketFrame::Close { code: 1000, .. }
    ));
    assert_eq!(records(&shared.store, &inbox).await?.len(), 2);
    assert!(sockets.is_empty());
    Ok(())
}

#[tokio::test]
async fn close_custody_and_peer_terminal_order_use_original_publications() -> TestResult {
    let executor = Arc::new(HeldCloseExecutor::default());
    let sockets = Sockets::open(
        Store::new(Arc::new(MemoryStream::default()), root()?),
        executor.clone(),
        Arc::new(SystemClock),
    )
    .await?;
    let shared = &sockets.shared;
    let (connection, inbox, last, _frames) = admitted_close_fixture(&sockets, "peer-order").await?;
    shared.message(
        &connection,
        SocketPayload::Text {
            text: "before".into(),
        },
    )?;
    let first = Close::new(1000, "first", SocketCloseInitiator::Peer);
    shared.peer_close(&connection, first)?;
    for _ in 0..100_000 {
        shared.peer_close(
            &connection,
            Close::new(1001, "later", SocketCloseInitiator::Peer),
        )?;
        shared.message(
            &connection,
            SocketPayload::Text {
                text: "after".into(),
            },
        )?;
    }
    {
        let table = shared.lock();
        let entry = table
            .sockets
            .get(&connection)
            .ok_or("lost queued peer close")?;
        assert_eq!(
            entry.inbound.len(),
            2,
            "only the preceding message and first Close survive"
        );
        assert_eq!(executor.tasks.lock().unwrap().len(), 1);
    }
    executor.take()?.await;
    assert!(
        matches!(SocketFrame::decode(&last.await?)?, SocketFrame::Close { code: 1000, reason, .. } if reason == "first")
    );
    assert_eq!(
        records(&shared.store, &inbox).await?.len(),
        3,
        "original open, message, Close"
    );
    assert_eq!(
        shared
            .store
            .provider()
            .tail(shared.layout.socket(&connection)?)
            .await?,
        2
    );
    assert!(sockets.is_empty());

    let (connection, inbox, last, _frames) =
        admitted_close_fixture(&sockets, "cancel-drain").await?;
    let mut first = Box::pin(sockets.drain());
    assert!(futures::poll!(&mut first).is_pending());
    drop(first);
    let mut later = Box::pin(sockets.drain());
    assert!(futures::poll!(&mut later).is_pending());
    assert_eq!(
        executor.tasks.lock().unwrap().len(),
        1,
        "late drain joins the original owner"
    );
    executor.take()?.await;
    assert!(later.await.is_ok());
    assert!(matches!(
        SocketFrame::decode(&last.await?)?,
        SocketFrame::Close { code: 1001, .. }
    ));
    assert_eq!(records(&shared.store, &inbox).await?.len(), 2);
    assert_eq!(
        shared
            .store
            .provider()
            .tail(shared.layout.socket(&connection)?)
            .await?,
        2
    );

    let (connection, inbox, last, _frames) =
        admitted_close_fixture(&sockets, "lost-producer").await?;
    let mut waiting = Box::pin(sockets.drain());
    assert!(futures::poll!(&mut waiting).is_pending());
    drop(executor.take()?); // Drop the actual close future before its first poll.
    assert!(matches!(
        waiting.await,
        Err(error) if matches!(error.as_ref(), Error::Stream(StreamError::Unavailable))
    ));
    assert!(
        last.await.is_err(),
        "producer loss cannot publish a successful Close"
    );
    assert_eq!(records(&shared.store, &inbox).await?.len(), 1);
    assert_eq!(
        shared
            .store
            .provider()
            .tail(shared.layout.socket(&connection)?)
            .await?,
        1
    );
    assert!(
        sockets.is_empty(),
        "lost close work must retire its live entry"
    );

    let (connection, inbox, last, _frames) =
        admitted_close_fixture(&sockets, "refused-close").await?;
    shared
        .store
        .provider()
        .append(acyclic_stream::AppendRequest {
            path: shared.layout.socket(&connection)?,
            records: vec![Bytes::from_static(b"foreign close").into()],
            if_tail: Some(1),
            idempotency_key: IdempotencyKey::new(b"foreign-close".to_vec())?,
        })
        .await?;
    let mut waiting = Box::pin(sockets.drain());
    assert!(futures::poll!(&mut waiting).is_pending());
    // Join while Closing, then let the original producer retire the entry
    // before observing its result. No later table lookup can recover this cut.
    let (owner, completion) = shared
        .claim_close(
            &mut shared.lock(),
            &connection,
            &Close::new(1001, "runtime draining", SocketCloseInitiator::Runtime),
        )
        .ok_or("closing socket lost its original completion")?;
    assert!(owner.is_none(), "joining cannot claim a second producer");
    executor.take()?.await;
    assert!(sockets.is_empty());
    let joined_error = completion.await.expect_err("original Close must refuse");
    let drain_error = waiting.await.expect_err("drain must retain the refusal");
    assert!(matches!(joined_error.as_ref(), Error::Conflict));
    assert!(Arc::ptr_eq(&joined_error, &drain_error));
    assert!(
        last.await.is_err(),
        "a refused publication cannot report a successful Close"
    );
    assert_eq!(records(&shared.store, &inbox).await?.len(), 1);
    assert!(sockets.is_empty());

    for lose_producer in [false, true] {
        let label = if lose_producer {
            "lost-opening"
        } else {
            "held-opening"
        };
        let (connection, inbox, last, open, _frames) = close_fixture(&sockets, label).await?;
        // Hold the original ledger cut before the actual Open can publish.
        let ledger = shared.ledger.write().await;
        let (verdict, _reply) = oneshot::channel();
        let (_keep_abandon, abandoned) = oneshot::channel();
        let owner = CloseOwner {
            shared: Arc::clone(shared),
            connection: connection.clone(),
            completed: None,
            retire: true,
        };
        executor.spawn(Box::pin(Arc::clone(shared).opening(
            connection.clone(),
            open,
            verdict,
            abandoned,
            owner,
        )));
        let mut opening = executor.take()?;
        assert!(futures::poll!(&mut opening).is_pending());
        let mut first = Box::pin(sockets.drain());
        assert!(
            futures::poll!(&mut first).is_pending(),
            "Open still owns the Close obligation"
        );
        drop(first);
        let mut later = Box::pin(sockets.drain());
        assert!(futures::poll!(&mut later).is_pending());
        if lose_producer {
            drop(opening);
            assert!(
                matches!(later.await, Err(error) if matches!(error.as_ref(), Error::Stream(StreamError::Unavailable)))
            );
            assert!(last.await.is_err());
            assert!(matches!(
                shared
                    .store
                    .provider()
                    .tail(shared.layout.socket(&connection)?)
                    .await,
                Err(StreamError::NotFound)
            ));
            drop(ledger);
        } else {
            drop(ledger);
            opening.await;
            assert!(
                futures::poll!(&mut later).is_pending(),
                "Open alone does not complete shutdown"
            );
            assert_eq!(executor.tasks.lock().unwrap().len(), 1);
            executor.take()?.await;
            assert!(later.await.is_ok());
            assert!(matches!(
                SocketFrame::decode(&last.await?)?,
                SocketFrame::Close { code: 1001, .. }
            ));
            assert_eq!(records(&shared.store, &inbox).await?.len(), 2);
            assert_eq!(
                shared
                    .store
                    .provider()
                    .tail(shared.layout.socket(&connection)?)
                    .await?,
                2
            );
        }
        assert!(sockets.is_empty());
    }

    let (connection, inbox, last, _frames) =
        admitted_close_fixture(&sockets, "record-bound").await?;
    let inbound = Inbound {
        shared: Arc::clone(shared),
        connection: connection.clone(),
    };
    let maximum = acyclic_stream::MAX_RECORD_BYTES;
    let frame = SocketFrame::Message(SocketPayload::Binary {
        binary: vec![0; maximum - 16].into(),
    })
    .encode();
    assert!(frame.len() <= maximum);
    assert_eq!(inbound.receive(frame), Err(StreamError::InvalidArgument));
    let reason = "x".repeat(maximum - 32);
    let frame = SocketFrame::Close {
        code: 1000,
        reason: reason.clone(),
        initiator: SocketCloseInitiator::Peer,
    }
    .encode();
    assert!(frame.len() <= maximum);
    assert_eq!(inbound.receive(frame), Err(StreamError::InvalidArgument));
    let actor = shared
        .lock()
        .sockets
        .get(&connection)
        .ok_or("lost original actor")?
        .actor
        .clone();
    assert!(
        sockets
            .web_socket_close(&actor, &WebSocketId(connection.clone()), 1000, &reason)
            .is_err()
    );
    {
        let table = shared.lock();
        let entry = table
            .sockets
            .get(&connection)
            .ok_or("lost open after refused frame")?;
        assert!(entry.inbound.is_empty());
        assert!(!entry.peer_closed);
        assert!(matches!(entry.phase, Phase::Open));
    }
    assert!(executor.tasks.lock().unwrap().is_empty());
    assert_eq!(records(&shared.store, &inbox).await?.len(), 1);
    // Derive the boundary from the actual marked record, including its exact
    // original connection identity and headers. The protobuf width is unchanged.
    let seed_len = maximum - 128;
    let seed = input_record(
        &connection,
        SocketInput::Message(SocketPayload::Binary {
            binary: vec![0; seed_len].into(),
        }),
    )?;
    let overhead = seed.payload_bytes() - seed_len;
    drop(seed);
    shared.message(
        &connection,
        SocketPayload::Binary {
            binary: vec![0; maximum - overhead].into(),
        },
    )?;
    {
        let table = shared.lock();
        let entry = table
            .sockets
            .get(&connection)
            .ok_or("lost admitted boundary frame")?;
        assert!(
            matches!(entry.inbound.front(), Some(PeerEvent::Message(record)) if record.payload_bytes() == maximum)
        );
    }
    executor.take()?.await;
    assert_eq!(records(&shared.store, &inbox).await?.len(), 2);
    inbound.receive(
        SocketFrame::Close {
            code: 1000,
            reason: "fits".into(),
            initiator: SocketCloseInitiator::Peer,
        }
        .encode(),
    )?;
    executor.take()?.await;
    assert!(
        matches!(SocketFrame::decode(&last.await?)?, SocketFrame::Close { code: 1000, reason, .. } if reason == "fits")
    );
    assert_eq!(records(&shared.store, &inbox).await?.len(), 3);
    assert!(sockets.is_empty());

    let (connection, inbox, last, mut frames) =
        admitted_close_fixture(&sockets, "outbound-bound").await?;
    let actor = shared
        .lock()
        .sockets
        .get(&connection)
        .ok_or("lost original outbound actor")?
        .actor
        .clone();
    let socket = WebSocketId(connection.clone());
    for payload in [
        SocketPayload::Binary {
            binary: vec![0; maximum].into(),
        },
        SocketPayload::Text {
            text: "x".repeat(maximum),
        },
    ] {
        assert_eq!(
            sockets.web_socket_send(&actor, &socket, payload),
            Err(host_error(
                "WebSocket frame exceeds the channel record bound"
            ))
        );
        assert!(frames.is_empty(), "refused output must not reach the peer");
        let table = shared.lock();
        let entry = table
            .sockets
            .get(&connection)
            .ok_or("refused output retired its socket")?;
        assert!(matches!(entry.phase, Phase::Open));
        assert!(!entry.peer_closed);
    }
    assert!(
        executor.tasks.lock().unwrap().is_empty(),
        "refusal cannot start a Close"
    );
    for binary in [false, true] {
        let seed_len = maximum - 128;
        let payload = |len| {
            if binary {
                SocketPayload::Binary {
                    binary: vec![0; len].into(),
                }
            } else {
                SocketPayload::Text {
                    text: "x".repeat(len),
                }
            }
        };
        let overhead = SocketFrame::Message(payload(seed_len)).encode().len() - seed_len;
        let expected = payload(maximum - overhead);
        sockets.web_socket_send(&actor, &socket, expected.clone())?;
        assert_eq!(
            frames.len(),
            1,
            "successful send must queue the actual frame"
        );
        let frame = frames
            .recv()
            .await
            .ok_or("accepted outbound frame lost its producer")?;
        assert_eq!(frame.len(), maximum);
        assert_eq!(SocketFrame::decode(&frame)?, SocketFrame::Message(expected));
    }
    assert_eq!(
        records(&shared.store, &inbox).await?.len(),
        1,
        "outbound sends perform no publication"
    );
    let mut outbound = Arc::clone(shared).outbound(Outbound::Open { frames, last });
    sockets.web_socket_send(
        &actor,
        &socket,
        SocketPayload::Text {
            text: "still open".into(),
        },
    )?;
    shared.peer_close(
        &connection,
        Close::new(1000, "done", SocketCloseInitiator::Peer),
    )?;
    executor.take()?.await;
    assert!(
        matches!(SocketFrame::decode(&outbound.next().await.ok_or("lost accepted output")??)?, SocketFrame::Message(SocketPayload::Text { text }) if text == "still open")
    );
    assert!(
        matches!(SocketFrame::decode(&outbound.next().await.ok_or("lost original Close")??)?, SocketFrame::Close { code: 1000, reason, .. } if reason == "done")
    );
    assert!(outbound.next().await.is_none());
    assert_eq!(records(&shared.store, &inbox).await?.len(), 2);
    assert!(sockets.is_empty());
    Ok(())
}
