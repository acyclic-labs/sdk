use super::*;
use crate::conformance::Failure;
use crate::conformance::sockets::{self as conformance, Peer, SocketBackend, SocketPackage};
use crate::local::{Runtime, SystemClock};
use acyclic_auth::contract::{ResourceGrant, ResourcePattern};
use acyclic_stream::MemoryStream;
use acyclic_stream::runtime::PlatformExecutor;
use acyclic_stream::session::{ActorSocket, Client, testing};

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
