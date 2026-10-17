//! In-process wire transport, durable recovery and JSON frame contracts.
use acyclic_harness::{
    Error, Result,
    client::{
        ClientSession, ClientStateStore, HarnessClient, MemoryClientState, ServiceConnector,
        SessionRecord, connect_or_spawn, jsonl,
    },
    core::{EventPayload, LifecycleState},
    wire,
    wire_api::{HarnessWireApi, OperationControlRequest, current_protocol, negotiate},
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[path = "support/client_command.rs"]
mod client_command;
use client_command::{command, owner};

fn session() -> ClientSession<MemoryClientState> {
    let mut session = ClientSession::open(MemoryClientState::default()).unwrap();
    session
        .track(wire::ReplayCursor {
            authority: Some(owner()),
            generation: "generation".into(),
            revision: 0,
        })
        .unwrap();
    session
}
#[tokio::test]
async fn plan_selection_follows_files_and_rejects_invalid_revisions_atomically() {
    use acyclic_harness::{
        client::{CommandCatalog, CommandQuery, CommandResolution, TaskFileQueryView},
        conversation::CompositeContentVerifier,
        task_volume::{Change, FileKey, Query, TaskVolume, Write},
    };
    use acyclic_stream::{MemoryStream, StreamPath};
    use futures::StreamExt as _;

    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("private").unwrap(),
    )
    .await
    .unwrap();
    let initial = volume.snapshot(&Query::Plans, 2).await.unwrap();
    let identity = (initial.cursor.volume, initial.cursor.query);
    let mut generation = initial.generation;
    let mut client = HarnessClient::connect(Arc::new(Host::default()), session())
        .await
        .unwrap();
    let reader = CompositeContentVerifier::new(Vec::new(), None).unwrap();
    let CommandResolution::Select { query } = CommandCatalog::builtins()
        .unwrap()
        .resolve("/plan", &reader, 128)
        .await
        .unwrap()
    else {
        panic!("plan did not select a view")
    };
    assert_eq!(query, CommandQuery::Plan);
    let mut follow = client
        .select_file_view(query, &volume, 2, 128)
        .await
        .unwrap();
    for text in ["- [ ] Build", "* [~] Build", "1. [x] Build"] {
        generation = volume
            .append(
                generation,
                &Write::new(FileKey::parse("plans/release.md").unwrap(), text.as_bytes()),
            )
            .await
            .unwrap();
        let change = follow.next().await.unwrap().unwrap();
        let file = change.file.as_ref().unwrap().clone();
        let cursor = change.cursor.clone();
        let payload = file.bytes.as_ptr();
        assert!(client.query_change(change).unwrap());
        let view = &client.session().views().queries[&identity];
        assert_eq!(view.files()[&file.key].bytes.as_ptr(), payload);
        assert_eq!(
            view.plan_lines(),
            ["plans/release.md", &text[text.find('[').unwrap()..]]
        );
        assert!(
            !client
                .query_change(Change {
                    file: Some(file.clone()),
                    files: Vec::new(),
                    removed: Vec::new(),
                    cursor: cursor.clone()
                })
                .unwrap()
        );
        let before = client.session().views().clone();
        for mutation in [
            "gap",
            "epoch",
            "query",
            "volume",
            "schema",
            "watermark",
            "bytes",
            "selection",
            "duplicate",
        ] {
            let mut invalid = Change {
                file: Some(file.clone()),
                files: Vec::new(),
                removed: Vec::new(),
                cursor: cursor.clone(),
            };
            if mutation != "duplicate" {
                invalid.cursor.position += 1;
                invalid.cursor.accepted += 1;
            }
            match mutation {
                "gap" => invalid.cursor.position += 1,
                "epoch" => invalid.cursor.epoch = uuid::Uuid::new_v4(),
                "query" => invalid.cursor.query[0] ^= 1,
                "volume" => invalid.cursor.volume[0] ^= 1,
                "schema" => invalid.cursor.schema += 1,
                "watermark" => invalid.cursor.durable = invalid.cursor.accepted + 1,
                "bytes" => invalid.file.as_mut().unwrap().bytes = vec![b'x'; 129].into(),
                "selection" => invalid.file.as_mut().unwrap().key = FileKey::Goal,
                "duplicate" => invalid.file.as_mut().unwrap().bytes = b"wrong".as_slice().into(),
                _ => unreachable!(),
            }
            assert!(client.query_change(invalid).is_err(), "{mutation}");
            assert_eq!(client.session().views(), &before, "{mutation}");
        }
    }
    let snapshot = volume.snapshot(&Query::Plans, 2).await.unwrap();
    assert!(TaskFileQueryView::from_snapshot(Query::File(FileKey::Goal), snapshot, 2, 128).is_err());
    let snapshot = volume.snapshot(&Query::Plans, 2).await.unwrap();
    assert!(TaskFileQueryView::from_snapshot(Query::Plans, snapshot, 2, 1).is_err());
    // Check both the bounded source and the client's independent revision validation.
    let mut volume_changes = volume
        .follow(
            Query::Plans,
            volume.snapshot(&Query::Plans, 2).await.unwrap().cursor,
        )
        .await
        .unwrap();
    for path in ["plans/second.md", "plans/third.md"] {
        generation = volume
            .append(
                generation,
                &Write::new(FileKey::parse(path).unwrap(), "- [x] Ship"),
            )
            .await
            .unwrap();
        let bounded = follow.next().await.unwrap();
        if path == "plans/second.md" {
            assert!(bounded.is_ok());
        } else {
            assert!(matches!(
                bounded,
                Err(acyclic_harness::task_volume::Error::LimitExceeded)
            ));
        }
        let change = volume_changes.next().await.unwrap().unwrap();
        let mut updates =
            futures::stream::iter([Ok(acyclic_harness::client::ViewUpdate::File(change))]);
        let before = client.session().views().clone();
        let result = client.next_update(&mut updates).await;
        if path == "plans/second.md" {
            assert!(result.unwrap());
            assert_eq!(client.session().views().queries[&identity].files().len(), 2);
        } else {
            assert!(result.is_err());
            assert_eq!(client.session().views(), &before);
        }
    }
    assert!(
        client
            .select_file_view(CommandQuery::Model, &volume, 2, 128)
            .await
            .is_err()
    );
}
#[cfg(feature = "filesystem")]
#[tokio::test]
async fn plan_command_selects_the_ordinary_filesystem_follow() {
    use acyclic_harness::{
        AgentId, OperationId,
        client::{CommandCatalog, CommandResolution},
        filesystem::MemoryHarnessStorage,
    };
    use futures::StreamExt as _;
    let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536)
        .await
        .unwrap();
    let source = storage
        .file_query(acyclic_fs::performance::BOUNDARY_BUDGET)
        .unwrap();
    let mut client = HarnessClient::connect(Arc::new(Host::default()), session())
        .await
        .unwrap();
    let CommandResolution::Select { query } = CommandCatalog::builtins()
        .unwrap()
        .resolve("/plan", storage.content_reader(), 128)
        .await
        .unwrap()
    else {
        panic!("plan did not select a query")
    };
    let mut follow = client
        .select_file_view(query, &source, 2, 128)
        .await
        .unwrap();
    let identity = *client.session().views().queries.keys().next().unwrap();
    for text in ["- [ ] Build", "* [~] Build", "1. [x] Build"] {
        storage
            .stage(
                OperationId::new(),
                "plans/release.md",
                text.as_bytes(),
                "text/markdown",
                "release.md",
            )
            .await
            .unwrap();
        let change = tokio::time::timeout(std::time::Duration::from_secs(5), follow.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut updates =
            futures::stream::iter([Ok(acyclic_harness::client::ViewUpdate::File(change))]);
        assert!(client.next_update(&mut updates).await.unwrap());
        assert_eq!(
            client.session().views().queries[&identity].plan_lines(),
            ["plans/release.md", &text[text.find('[').unwrap()..]]
        );
    }
}

fn delivery(from: u64, to: LifecycleState) -> wire::Delivery {
    let payload = EventPayload::LifecycleTransitioned {
        from: LifecycleState::Active,
        to,
        reason: None,
    };
    wire::Delivery {
        authority: Some(owner()),
        generation: "generation".into(),
        from_revision: from,
        through_revision: from + 1,
        live: true,
        events: vec![wire::EventEnvelope {
            protocol: Some(current_protocol()),
            authority: Some(owner()),
            revision: from + 1,
            operation_id: command().operation.unwrap().operation_id,
            event_type: "lifecycle_transitioned".into(),
            canonical_payload_json: serde_json::to_vec(&payload).unwrap(),
            ..Default::default()
        }],
    }
}
fn frame(delivery: wire::Delivery) -> wire::ServerFrame {
    wire::ServerFrame {
        frame: Some(wire::server_frame::Frame::Delivery(delivery)),
    }
}
fn admission(command: &wire::CommandEnvelope, state: wire::AdmissionState) -> wire::Admission {
    wire::Admission {
        operation: command.operation.clone(),
        state: state as i32,
        error: None,
    }
}

async fn submit<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    command: wire::CommandEnvelope,
) -> Result<Vec<wire::Admission>> {
    let mut responses = Vec::new();
    client
        .submit(command, |response| {
            responses.push(response);
            Ok(())
        })
        .await?;
    Ok(responses)
}

#[test]
fn replay_recovers_projection_and_cursor_and_deduplicates() {
    let mut session = session();
    let first = frame(delivery(0, LifecycleState::Active));
    let ack = session.receive(first.clone()).unwrap().unwrap();
    assert_eq!(ack.through_revision, 1);
    session.receive(first).unwrap();
    assert_eq!(session.into_store().load().unwrap().len(), 2);
    let mut original = self::session();
    original
        .receive(frame(delivery(0, LifecycleState::Completed)))
        .unwrap();
    let views = original.views().clone();
    let mut recovered = ClientSession::open(original.into_store()).unwrap();
    assert_eq!(recovered.views(), &Default::default());
    recovered
        .receive(frame(delivery(0, LifecycleState::Completed)))
        .unwrap();
    assert_eq!(recovered.views(), &views);
    assert_eq!(recovered.resume().cursors[0].revision, 1);
    assert_eq!(
        recovered.views().tasks["AGGREGATE_KIND_TASK:task"].status,
        LifecycleState::Completed
    );
}

#[test]
fn invalid_deliveries_never_advance_or_publish() {
    let mut session = session();
    let mut cases = Vec::new();
    cases.push(delivery(1, LifecycleState::Completed));
    let mut changed = delivery(0, LifecycleState::Completed);
    changed.generation = "other".into();
    cases.push(changed);
    let mut gap = delivery(0, LifecycleState::Completed);
    gap.events[0].revision = 2;
    cases.push(gap);
    let mut truncated = delivery(0, LifecycleState::Completed);
    truncated.through_revision = 2;
    cases.push(truncated);
    let mut foreign = delivery(0, LifecycleState::Completed);
    foreign.events[0].authority.as_mut().unwrap().id = "foreign".into();
    cases.push(foreign);
    let mut bad_payload = delivery(0, LifecycleState::Completed);
    bad_payload.events[0].canonical_payload_json = b"{}".to_vec();
    cases.push(bad_payload);
    for case in cases {
        assert!(session.receive(frame(case)).is_err());
    }
    assert_eq!(session.resume().cursors[0].revision, 0);
    assert!(session.views().tasks.is_empty());
    assert_eq!(session.into_store().load().unwrap().len(), 1);
}

struct FailingStore {
    inner: MemoryClientState,
    fail: Arc<AtomicBool>,
}
impl ClientStateStore for FailingStore {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        self.inner.load()
    }
    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(Error::Storage("disk full".into()));
        }
        self.inner.append(record)
    }
    fn replace(&mut self, record: &SessionRecord) -> Result<()> {
        if self.fail.load(Ordering::Relaxed) {
            return Err(Error::Storage("disk full".into()));
        }
        self.inner.replace(record)
    }
}

#[test]
fn storage_failure_precedes_publication_and_acknowledgement() {
    let fail = Arc::new(AtomicBool::new(false));
    let mut session = ClientSession::open(FailingStore {
        inner: self::session().into_store(),
        fail: fail.clone(),
    })
    .unwrap();
    fail.store(true, Ordering::Relaxed);
    assert!(session.queue(command()).is_err());
    assert_eq!(session.pending().count(), 0);
    assert!(
        session
            .receive(frame(delivery(0, LifecycleState::Completed)))
            .is_err()
    );
    assert!(session.views().tasks.is_empty());
    assert_eq!(session.resume().cursors[0].revision, 0);
    fail.store(false, Ordering::Relaxed);
    session
        .receive(frame(delivery(0, LifecycleState::Completed)))
        .unwrap();
    assert_eq!(session.resume().cursors[0].revision, 1);
}

#[test]
fn outbox_identity_conflicts_and_admission_are_exact() {
    let mut session = session();
    let command = command();
    session.queue(command.clone()).unwrap();
    session.queue(command.clone()).unwrap();
    let mut altered = command.clone();
    altered.canonical_action_json = b"[]".to_vec();
    assert!(session.queue(altered).is_err());
    let mut wrong = admission(&command, wire::AdmissionState::Accepted);
    wrong.operation.as_mut().unwrap().idempotency_key = "other".into();
    assert!(
        session
            .receive(wire::ServerFrame {
                frame: Some(wire::server_frame::Frame::Admission(wrong))
            })
            .is_err()
    );
    for state in [
        wire::AdmissionState::Indeterminate,
        wire::AdmissionState::Rejected,
    ] {
        session
            .receive(wire::ServerFrame {
                frame: Some(wire::server_frame::Frame::Admission(admission(
                    &command, state,
                ))),
            })
            .unwrap();
        assert_eq!(
            session.pending().count(),
            usize::from(state == wire::AdmissionState::Indeterminate)
        );
    }
    assert_eq!(
        ClientSession::open(session.into_store())
            .unwrap()
            .pending()
            .count(),
        0
    );
}

#[derive(Default)]
struct Host {
    commands: Mutex<Vec<wire::CommandEnvelope>>,
    lost_reply: AtomicBool,
    fail_submit: AtomicUsize,
    resumes: Mutex<Vec<wire::ResumeRequest>>,
    acknowledgements: Mutex<Vec<wire::ClientFrame>>,
    lost_ack: AtomicBool,
    hold_ack: AtomicBool,
    complete_on_ack: AtomicBool,
    trace: Mutex<Option<BoxProviderStream<'static, Result<wire::Delivery>>>>,
    state: Mutex<Option<wire::CompletionState>>,
    through: AtomicUsize,
    live_sender: Mutex<Option<futures::channel::mpsc::UnboundedSender<Result<wire::Delivery>>>>,
    submit_delivery: Mutex<Option<wire::Delivery>>,
    snapshot_history: Mutex<Option<Vec<wire::Delivery>>>,
    replay_page_size: AtomicUsize,
    status_sender: Mutex<
        Option<(
            wire::ObserveRequest,
            futures::channel::mpsc::UnboundedSender<Result<wire::OperationStatus>>,
        )>,
    >,
    requires_admission: AtomicBool,
    admission_state: Mutex<Option<wire::AdmissionState>>,
    ack_input_eof: Mutex<Option<futures::channel::oneshot::Sender<()>>>,
    ack_release: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
}
impl Host {
    fn snapshot(&self, request: wire::ObserveRequest) -> Result<wire::OperationStatus> {
        if self.requires_admission.load(Ordering::Relaxed)
            && !self.commands.lock().unwrap().iter().any(|command| {
                command
                    .operation
                    .as_ref()
                    .is_some_and(|identity| identity.operation_id == request.operation_id)
            })
        {
            return Err(Error::NotFound("operation".into()));
        }
        Ok(wire::OperationStatus {
            protocol: Some(current_protocol()),
            replay_through: Some(wire::ReplayCursor {
                authority: request.owner.clone(),
                generation: "generation".into(),
                revision: self.through.load(Ordering::Relaxed) as u64,
            }),
            owner: request.owner,
            operation: Some(wire::OperationIdentity {
                operation_id: request.operation_id,
                idempotency_key: String::new(),
            }),
            state: self
                .state
                .lock()
                .unwrap()
                .unwrap_or(wire::CompletionState::Succeeded) as i32,
            ..Default::default()
        })
    }
    fn complete(&self, state: wire::CompletionState) {
        let sender = self.status_sender.lock().unwrap();
        *self.state.lock().unwrap() = Some(state);
        if let Some((request, sender)) = sender.as_ref() {
            let _ = sender.unbounded_send(self.snapshot(request.clone()));
        }
    }
}
impl HarnessWireApi for Host {
    fn acknowledge(&self, request: wire::Acknowledge) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            let eof = self.ack_input_eof.lock().unwrap().take();
            if let Some(eof) = eof {
                let _ = eof.send(());
            }
            let release = self.ack_release.lock().unwrap().take();
            if let Some(release) = release {
                release.await.unwrap();
            }
            if self.hold_ack.load(Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            if request.through_revision == self.through.load(Ordering::Relaxed) as u64
                && self.complete_on_ack.swap(false, Ordering::Relaxed)
            {
                self.complete(wire::CompletionState::Succeeded);
            }
            self.acknowledgements
                .lock()
                .unwrap()
                .push(wire::ClientFrame {
                    frame: Some(wire::client_frame::Frame::Acknowledge(request)),
                });
            if self.lost_ack.swap(false, Ordering::Relaxed) {
                return Err(Error::Storage("lost acknowledgement".into()));
            }
            Ok(())
        })
    }
    fn handshake(
        &self,
        request: wire::HandshakeRequest,
    ) -> BoxProviderFuture<'_, Result<wire::HandshakeResponse>> {
        Box::pin(async move { negotiate(&request, &wire::CapabilitySet::default()) })
    }
    fn submit(
        &self,
        command: wire::CommandEnvelope,
    ) -> BoxProviderFuture<'_, Result<wire::Admission>> {
        Box::pin(async move {
            self.commands.lock().unwrap().push(command.clone());
            if self.lost_reply.swap(false, Ordering::Relaxed)
                || self.commands.lock().unwrap().len() == self.fail_submit.load(Ordering::Relaxed)
            {
                return Err(Error::Storage("connection lost after admission".into()));
            }
            if let Some(delivery) = self.submit_delivery.lock().unwrap().take() {
                self.through
                    .store(delivery.through_revision as usize, Ordering::Relaxed);
                match self.snapshot_history.lock().unwrap().as_mut() {
                    Some(history) => history.push(delivery),
                    None => self
                        .live_sender
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .unbounded_send(Ok(delivery))
                        .unwrap(),
                }
                self.complete(wire::CompletionState::Succeeded);
            }
            Ok(admission(
                &command,
                self.admission_state
                    .lock()
                    .unwrap()
                    .unwrap_or(wire::AdmissionState::Accepted),
            ))
        })
    }
    fn replay(
        &self,
        request: wire::ResumeRequest,
        _: acyclic_harness::wire_api::ReplayMode,
    ) -> BoxProviderFuture<'_, Result<BoxProviderStream<'static, Result<wire::Delivery>>>> {
        Box::pin(async move {
            let after = request.cursors[0].revision;
            self.resumes.lock().unwrap().push(request);
            if let Some(history) = self.snapshot_history.lock().unwrap().as_ref() {
                let frames = history
                    .iter()
                    .filter(|delivery| delivery.through_revision > after)
                    .take(match self.replay_page_size.load(Ordering::Relaxed) {
                        0 => usize::MAX,
                        size => size,
                    })
                    .cloned()
                    .map(Ok)
                    .collect::<Vec<_>>();
                return Ok(Box::pin(futures::stream::iter(frames))
                    as BoxProviderStream<'static, Result<wire::Delivery>>);
            }
            if let Some(trace) = self.trace.lock().unwrap().take() {
                return Ok(trace);
            }
            Ok(Box::pin(futures::stream::iter(
                (after..2).map(|revision| Ok(delivery(revision, LifecycleState::Completed))),
            ))
                as BoxProviderStream<'static, Result<wire::Delivery>>)
        })
    }
    fn authorize_operation_control<'a>(
        &'a self,
        _: &'a OperationControlRequest,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn observe(
        &self,
        request: wire::ObserveRequest,
    ) -> BoxProviderFuture<'_, Result<wire::OperationStatus>> {
        Box::pin(async move { self.snapshot(request) })
    }
    fn watch_operation(
        &self,
        request: wire::ObserveRequest,
    ) -> BoxProviderFuture<'_, Result<BoxProviderStream<'static, Result<wire::OperationStatus>>>>
    {
        Box::pin(async move {
            let mut slot = self.status_sender.lock().unwrap();
            let initial = self.snapshot(request.clone())?;
            let (sender, receiver) = futures::channel::mpsc::unbounded();
            sender.unbounded_send(Ok(initial)).unwrap();
            *slot = Some((request, sender));
            Ok(Box::pin(receiver)
                as BoxProviderStream<
                    'static,
                    Result<wire::OperationStatus>,
                >)
        })
    }
    fn cancel(
        &self,
        request: wire::CancelRequest,
    ) -> BoxProviderFuture<'_, Result<wire::CancelResponse>> {
        Box::pin(async move {
            self.through.store(1, Ordering::Relaxed);
            self.complete(wire::CompletionState::Cancelled);
            if let Some(sender) = self.live_sender.lock().unwrap().as_ref() {
                sender
                    .unbounded_send(Ok(delivery(0, LifecycleState::Cancelled)))
                    .unwrap();
            }
            let status = self
                .observe(wire::ObserveRequest {
                    operation_id: request.operation_id.clone(),
                    owner: request.owner,
                    protocol: request.protocol,
                    scope: request.scope,
                })
                .await?;
            Ok(wire::CancelResponse {
                status: Some(status),
                operation: Some(wire::OperationIdentity {
                    operation_id: request.operation_id,
                    idempotency_key: request.idempotency_key,
                }),
            })
        })
    }
}

#[tokio::test]
async fn reconnect_resends_verbatim_and_resumes_the_committed_cut() {
    let host = Arc::new(Host::default());
    host.lost_reply.store(true, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let command = command();
    assert!(submit(&mut client, command.clone()).await.is_err());
    assert_eq!(client.session().pending().count(), 1);
    let recovered = ClientSession::open(client.into_session().into_store()).unwrap();
    let mut client = HarnessClient::connect(host.clone(), recovered)
        .await
        .unwrap();
    let mut admissions = Vec::new();
    client
        .flush_outbox(|admission| {
            admissions.push(admission);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(admissions.len(), 1);
    assert_eq!(
        *host.commands.lock().unwrap(),
        vec![command.clone(), command]
    );
    assert_eq!(client.session().pending().count(), 0);
    client.replay().await.unwrap();
    client.next_delivery().await.unwrap().unwrap();
    let recovered = ClientSession::open(client.into_session().into_store()).unwrap();
    let mut client = HarnessClient::connect(host.clone(), recovered)
        .await
        .unwrap();
    client.replay().await.unwrap();
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    assert_eq!(host.resumes.lock().unwrap()[1].cursors[0].revision, 0);
    client.next_delivery().await.unwrap().unwrap();
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    client.next_delivery().await.unwrap().unwrap();
    assert!(client.next_delivery().await.unwrap().is_none());
    assert_eq!(client.session().resume().cursors[0].revision, 2);
}

struct Connector {
    host: Arc<Host>,
    exists: AtomicBool,
    spawns: AtomicUsize,
    unauthorized: bool,
}
impl ServiceConnector for Connector {
    fn connect(&self) -> BoxProviderFuture<'_, Result<Arc<dyn HarnessWireApi>>> {
        Box::pin(async move {
            if self.unauthorized {
                return Err(Error::Unauthorized("denied".into()));
            }
            if !self.exists.load(Ordering::Relaxed) {
                return Err(Error::NotFound("endpoint".into()));
            }
            Ok(self.host.clone() as Arc<dyn HarnessWireApi>)
        })
    }
    fn spawn(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            self.spawns.fetch_add(1, Ordering::Relaxed);
            self.exists.store(true, Ordering::Relaxed);
            Ok(())
        })
    }
}

#[tokio::test]
async fn autospawn_only_for_missing_service() {
    let connector = Connector {
        host: Arc::new(Host::default()),
        exists: AtomicBool::new(false),
        spawns: AtomicUsize::new(0),
        unauthorized: false,
    };
    connect_or_spawn(&connector, session()).await.unwrap();
    connect_or_spawn(&connector, session()).await.unwrap();
    assert_eq!(connector.spawns.load(Ordering::Relaxed), 1);
    let denied = Connector {
        unauthorized: true,
        ..connector
    };
    assert!(connect_or_spawn(&denied, session()).await.is_err());
    assert_eq!(denied.spawns.load(Ordering::Relaxed), 1);
}

#[path = "client_session_jsonl.rs"]
mod jsonl_tests;

#[path = "client_session_views.rs"]
mod views;

#[path = "client_session_recovery.rs"]
mod recovery;

#[path = "client_session_admissions.rs"]
mod admissions;

#[path = "client_session_review.rs"]
mod review;

fn tool_text(text: &str) -> acyclic_harness::model::ToolResultContent {
    acyclic_harness::model::ToolResultContent::Parts {
        parts: vec![acyclic_harness::model::ModelDataPart::Text { text: text.into() }],
    }
}

#[path = "support/client_files.rs"]
#[cfg(feature = "filesystem")]
mod client_files;
#[path = "client_session_files.rs"]
#[cfg(feature = "filesystem")]
mod files;

#[path = "client_session_usage.rs"]
#[cfg(all(feature = "filesystem", feature = "test-support"))]
mod usage;

#[path = "client_session_documents.rs"]
mod documents;
