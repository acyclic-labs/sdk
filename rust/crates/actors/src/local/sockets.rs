//! Hibernatable actor sockets for the local runtime, carried as virtual
//! channels on the stream session.
//!
//! An Open is an [`Invocation`](crate::invocation::Invocation) committed with
//! the socket's own stream `s/<connection>` (record 0, `Absent` first) and its
//! ledger entry; every later inbox event for the socket is conditioned on
//! `Tail{s/<connection>, 1}`, and the Close appends record 1 under that same
//! condition. So exactly one Close follows every delivered Open, and no event
//! follows the Close, whatever closes it: the peer, the actor, backpressure,
//! drain, or recovery after a crash.
//!
//! Sockets, tags, attachments and the auto-response pair are host state, so a
//! hibernated actor holding sockets owns no task, timer or heap; auto-response
//! answers keepalives without delivering anything.
//!
//! Recovery is O(open sockets): the ledger is a current segment of Opened and
//! Closed entries, compacted once it holds more than twice the open set, and
//! `epochs` names the segments that can still hold open sockets.
use super::{Clock, Error, PreparedPublish, Store};
use crate::addressing::ActorReference;
use crate::guest::{
    Effect, GuestError, GuestEventResult, SocketCloseInitiator, SocketHost, SocketPayload,
    WebSocketAutoResponse, WebSocketId, validate_web_socket_attachment,
    validate_web_socket_auto_response, validate_web_socket_tags,
};
use crate::sockets::{SocketFrame, SocketInput, SocketOpen, input_record, open_invocation};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::runtime::Executor;
use acyclic_stream::session::{CallAuthorization, SocketChannel, SocketInbound, SocketProvider};
use acyclic_stream::{
    AppendRecord, BoxProviderFuture, BoxProviderStream, CommitCondition, CommitMutation,
    CommitOutcome, IdempotencyKey, ReadRequest, RevisionCut, StreamError, StreamPath,
    StreamProvider,
};
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use prost::Message as _;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{RwLock, mpsc, oneshot};

/// Records per ledger read or write; it splits work and bounds nothing.
const PAGE: u32 = 256;
/// Ledger entries tolerated above twice the open set before compaction, so
/// compaction work stays amortized O(1) per open or close.
const SLACK: usize = 256;

#[derive(Clone, PartialEq, prost::Message)]
struct EpochWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    /// Highest allocated ledger segment, current for the latest incarnation.
    #[prost(uint64, tag = "2")]
    segment: u64,
    /// First segment that can still hold open sockets.
    #[prost(uint64, tag = "3")]
    from: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct OpenedWire {
    #[prost(string, tag = "1")]
    connection: String,
    #[prost(bytes = "bytes", tag = "2")]
    actor: Bytes,
    #[prost(string, tag = "3")]
    inbox: String,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum LedgerKind {
    #[prost(message, tag = "1")]
    Opened(OpenedWire),
    #[prost(string, tag = "2")]
    Closed(String),
}

#[derive(Clone, PartialEq, prost::Message)]
struct LedgerWire {
    #[prost(oneof = "LedgerKind", tags = "1, 2")]
    kind: Option<LedgerKind>,
}

fn invalid(reason: &'static str) -> Error {
    Error::Invalid(reason)
}

fn decode_canonical<M: prost::Message + Default>(bytes: &[u8]) -> Result<M, Error> {
    let wire = M::decode(bytes).map_err(|_| invalid("invalid socket ledger record"))?;
    if wire.encode_to_vec().as_slice() != bytes {
        return Err(invalid("invalid socket ledger record"));
    }
    Ok(wire)
}

/// The final close of one socket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Close {
    pub(crate) code: u16,
    pub(crate) reason: String,
    pub(crate) initiator: SocketCloseInitiator,
}

impl Close {
    fn new(code: u16, reason: &str, initiator: SocketCloseInitiator) -> Self {
        Self {
            code,
            reason: reason.into(),
            initiator,
        }
    }
}

/// Where a committed socket's events go.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Target {
    pub(crate) actor: ActorReference,
    pub(crate) inbox: StreamPath,
}

/// Durable paths of one sockets root.
#[derive(Clone, Debug)]
pub(crate) struct Layout {
    root: StreamPath,
}

impl Layout {
    pub(crate) fn new<P: StreamProvider + ?Sized>(store: &Store<P>) -> Result<Self, Error> {
        Ok(Self {
            root: StreamPath::new(format!("{}/_sockets/v1", store.root()))?,
        })
    }
    fn epochs(&self) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!("{}/epochs", self.root))?)
    }
    fn segment(&self, index: u64) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!("{}/segments/{index}", self.root))?)
    }
    pub(crate) fn socket(&self, connection: &str) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/s/{}",
            self.root,
            acyclic_auth::encode_component(connection.as_bytes())
        ))?)
    }
    fn key(&self, parts: &[&[u8]]) -> Result<IdempotencyKey, Error> {
        let mut hash = Sha256::new();
        hash.update(b"acyclic.actors.socket.v1");
        for part in std::iter::once(self.root.as_str().as_bytes()).chain(parts.iter().copied()) {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        Ok(IdempotencyKey::new(hash.finalize().to_vec())?)
    }
}

fn ledger_record(kind: LedgerKind) -> AppendRecord {
    LedgerWire { kind: Some(kind) }.encode_to_vec().into()
}

fn append(
    path: StreamPath,
    records: Vec<AppendRecord>,
    conditions: Vec<CommitCondition>,
) -> Effect {
    Effect {
        conditions,
        mutation: CommitMutation::Append { path, records },
    }
}

/// Commit `effects` once; `false` means a condition failed.
async fn commit<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    key: IdempotencyKey,
    effects: Vec<Effect>,
) -> Result<bool, Error> {
    let mut publication = PreparedPublish::foreign(key);
    for effect in effects {
        publication.join(effect)?;
    }
    Ok(matches!(
        store.provider().commit(publication.into_request()?).await?,
        CommitOutcome::Committed(_)
    ))
}

/// Commit the one Close of `connection`: record 1 of its socket stream, the
/// inbox event and the actor's wake, all under `Tail{socket, 1}`. `false`
/// means the socket was already closed. `segment` records the close in the
/// current ledger segment; recovery omits it because it retires the segment.
pub(crate) async fn commit_close<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    layout: &Layout,
    connection: &str,
    target: &Target,
    segment: Option<u64>,
    close: &Close,
) -> Result<bool, Error> {
    let socket = layout.socket(connection)?;
    let input = input_record(
        connection,
        &SocketInput::Close {
            code: close.code,
            reason: close.reason.clone(),
            initiator: close.initiator.clone(),
        },
    )?;
    let key = layout.key(&[connection.as_bytes(), b"close", &input.value])?;
    let mut effects = vec![
        append(
            socket.clone(),
            vec![input.value.clone().into()],
            vec![CommitCondition::Tail {
                path: socket,
                expected: 1,
            }],
        ),
        append(target.inbox.clone(), vec![input], Vec::new()),
        store.activation(&target.actor),
    ];
    if let Some(segment) = segment {
        effects.push(append(
            layout.segment(segment)?,
            vec![ledger_record(LedgerKind::Closed(connection.into()))],
            Vec::new(),
        ));
    }
    commit(store, key, effects).await
}

/// Commit one peer message, the `ordinal`th of `connection`. A message racing
/// the close loses on `Tail{socket, 1}`, so no event reaches the actor after
/// its Close; `false` reports that loss.
pub(crate) async fn commit_message<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    layout: &Layout,
    connection: &str,
    target: &Target,
    ordinal: u64,
    payload: SocketPayload,
) -> Result<bool, Error> {
    let record = input_record(connection, &SocketInput::Message(payload))?;
    commit(
        store,
        layout.key(&[connection.as_bytes(), b"message", &ordinal.to_be_bytes()])?,
        vec![
            append(
                target.inbox.clone(),
                vec![record],
                vec![CommitCondition::Tail {
                    path: layout.socket(connection)?,
                    expected: 1,
                }],
            ),
            store.activation(&target.actor),
        ],
    )
    .await
}

/// Possibly open sockets recorded in `segment`: Opened minus Closed. Closed is
/// a tombstone, so order within and across segments does not matter.
async fn fold<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    layout: &Layout,
    segment: u64,
    live: &mut BTreeMap<String, Target>,
    closed: &mut BTreeSet<String>,
) -> Result<(), Error> {
    let path = layout.segment(segment)?;
    let mut from = 0;
    loop {
        let page = match store
            .provider()
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from,
                limit: PAGE,
                at_least: RevisionCut::default(),
            })
            .await
        {
            Ok(page) => page.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let count = page.len();
        for record in page {
            match decode_canonical::<LedgerWire>(&record.value)?.kind {
                Some(LedgerKind::Opened(opened)) => {
                    if !closed.contains(&opened.connection) {
                        live.insert(
                            opened.connection,
                            Target {
                                actor: ActorReference::decode(&opened.actor)
                                    .map_err(|_| invalid("invalid socket ledger actor"))?,
                                inbox: StreamPath::new(opened.inbox)?,
                            },
                        );
                    }
                }
                Some(LedgerKind::Closed(connection)) => {
                    live.remove(&connection);
                    closed.insert(connection);
                }
                None => return Err(invalid("invalid socket ledger record")),
            }
            from = record.sequence + 1;
        }
        if count < PAGE as usize {
            return Ok(());
        }
    }
}

#[derive(Clone, Copy)]
struct Ledger {
    /// Segment that receives new entries.
    segment: u64,
    /// Highest durably allocated segment; a failed compaction never reuses it.
    allocated: u64,
    /// Tail of `epochs` that every open is conditioned on.
    epochs: u64,
}

enum Phase {
    /// Admitted; the open commit has not started.
    Unborn,
    /// The open commit is in flight or committed and awaiting the verdict.
    /// A close requested before the commit settles waits in `pending`.
    Opening {
        committed: bool,
        pending: Option<Close>,
    },
    Open,
    Closing,
}

struct Entry {
    actor: ActorReference,
    inbox: Option<StreamPath>,
    phase: Phase,
    tags: Vec<String>,
    attachment: Option<Bytes>,
    answered_at: Option<u64>,
    outbound: mpsc::Sender<Bytes>,
    final_frame: Option<oneshot::Sender<Bytes>>,
    messages: u64,
}

#[derive(Default)]
struct Table {
    sockets: HashMap<String, Entry>,
    actors: HashMap<ActorReference, BTreeSet<String>>,
    auto: HashMap<ActorReference, WebSocketAutoResponse>,
}

impl Table {
    fn remove(&mut self, connection: &str) -> Option<Entry> {
        let entry = self.sockets.remove(connection)?;
        if let Some(set) = self.actors.get_mut(&entry.actor) {
            set.remove(connection);
            if set.is_empty() {
                self.actors.remove(&entry.actor);
            }
        }
        Some(entry)
    }
}

struct Shared<P: StreamProvider + ?Sized> {
    store: Store<P>,
    layout: Layout,
    executor: Arc<dyn Executor>,
    clock: Arc<dyn Clock>,
    incarnation: u64,
    next: AtomicU64,
    table: std::sync::Mutex<Table>,
    /// Opens and closes hold it shared; compaction holds it exclusively.
    ledger: RwLock<Ledger>,
    live: AtomicUsize,
    written: AtomicUsize,
    compacting: AtomicBool,
}

/// The local runtime's socket host. Serve it as the session's
/// [`SocketProvider`], and hand it to actors as their [`SocketHost`].
pub struct Sockets<P: StreamProvider + ?Sized + 'static> {
    shared: Arc<Shared<P>>,
}

impl<P: StreamProvider + ?Sized + 'static> Clone for Sockets<P> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

fn stream_error(error: Error) -> StreamError {
    match error {
        Error::Stream(error) => error,
        Error::Conflict => StreamError::AlreadyExists,
        _ => StreamError::InvalidArgument,
    }
}

fn host_error(reason: &str) -> GuestError {
    GuestError::Handler(reason.into())
}

impl<P: StreamProvider + ?Sized + 'static> Sockets<P> {
    /// Open the socket host of `store`'s runtime. Every socket a previous
    /// incarnation left open receives Close(Runtime, 1012) first, in work
    /// proportional to those sockets, and that incarnation can open no more.
    pub async fn open(
        store: Store<P>,
        executor: Arc<dyn Executor>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        let layout = Layout::new(&store)?;
        let epochs = layout.epochs()?;
        let (tail, last) = match store.provider().head(epochs.clone()).await {
            Ok(head) => (head.tail, head.last),
            Err(StreamError::NotFound) => (0, None),
            Err(error) => return Err(error.into()),
        };
        let (segment, from) = match (tail, last) {
            (0, _) => (0, 1),
            (_, Some(record)) if record.sequence + 1 == tail => {
                let wire = decode_canonical::<EpochWire>(&record.value)?;
                if wire.version != 1 || wire.from > wire.segment + 1 {
                    return Err(invalid("invalid socket epoch"));
                }
                (wire.segment, wire.from)
            }
            _ => return Err(invalid("socket epochs history is incomplete")),
        };
        let incarnation = segment + 1;
        let epoch = |from| -> AppendRecord {
            EpochWire {
                version: 1,
                segment: incarnation,
                from,
            }
            .encode_to_vec()
            .into()
        };
        // Fence first: an open of an earlier incarnation commits under
        // `Tail{epochs}` and now fails, so the survivors below are final.
        let fenced = commit(
            &store,
            layout.key(&[b"fence", &incarnation.to_be_bytes()])?,
            vec![append(
                epochs.clone(),
                vec![epoch(from)],
                vec![CommitCondition::Tail {
                    path: epochs.clone(),
                    expected: tail,
                }],
            )],
        )
        .await?;
        if !fenced {
            return Err(Error::Conflict);
        }
        let mut live = BTreeMap::new();
        let mut closed = BTreeSet::new();
        for index in from..=segment {
            fold(&store, &layout, index, &mut live, &mut closed).await?;
        }
        let recovered = Close::new(1012, "runtime recovered", SocketCloseInitiator::Runtime);
        let (store_ref, layout_ref, recovered_ref) = (&store, &layout, &recovered);
        let mut closing =
            futures::stream::iter(live.into_iter().map(|(connection, target)| async move {
                commit_close(
                    store_ref,
                    layout_ref,
                    &connection,
                    &target,
                    None,
                    recovered_ref,
                )
                .await
            }))
            .buffer_unordered(PAGE as usize);
        while let Some(closed) = closing.next().await {
            closed?;
        }
        drop(closing);
        let settled = commit(
            &store,
            layout.key(&[b"recovered", &incarnation.to_be_bytes()])?,
            vec![append(
                epochs.clone(),
                vec![epoch(incarnation)],
                vec![CommitCondition::Tail {
                    path: epochs,
                    expected: tail + 1,
                }],
            )],
        )
        .await?;
        if !settled {
            return Err(Error::Conflict);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                store,
                layout,
                executor,
                clock,
                incarnation,
                next: AtomicU64::new(0),
                table: std::sync::Mutex::new(Table::default()),
                ledger: RwLock::new(Ledger {
                    segment: incarnation,
                    allocated: incarnation,
                    epochs: tail + 2,
                }),
                live: AtomicUsize::new(0),
                written: AtomicUsize::new(0),
                compacting: AtomicBool::new(false),
            }),
        })
    }

    /// Close every socket with Close(Runtime, 1001) and wait for each
    /// committed one; a socket whose open is still committing closes as soon
    /// as that commit settles.
    pub async fn drain(&self) {
        let close = Close::new(1001, "runtime draining", SocketCloseInitiator::Runtime);
        let connections: Vec<String> = self.shared.lock().sockets.keys().cloned().collect();
        let started: Vec<String> = connections
            .into_iter()
            .filter(|connection| self.shared.claim_close(connection, &close))
            .collect();
        futures::future::join_all(started.iter().map(|connection| {
            Arc::clone(&self.shared).finish_close(connection.clone(), close.clone())
        }))
        .await;
    }

    /// Open sockets in this incarnation, including ones still opening.
    pub fn len(&self) -> usize {
        self.shared.lock().sockets.len()
    }

    /// Whether no socket is open in this incarnation.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<P: StreamProvider + ?Sized + 'static> Shared<P> {
    fn lock(&self) -> std::sync::MutexGuard<'_, Table> {
        self.table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Move `connection` toward its one close. True when the caller must
    /// now run [`Self::finish_close`]; a close requested while the open is
    /// still committing runs when that commit settles.
    fn claim_close(&self, connection: &str, close: &Close) -> bool {
        let mut table = self.lock();
        let Some(entry) = table.sockets.get_mut(connection) else {
            return false;
        };
        match &mut entry.phase {
            Phase::Unborn => {
                table.remove(connection);
                false
            }
            Phase::Opening {
                committed: false,
                pending,
            } => {
                pending.get_or_insert_with(|| close.clone());
                false
            }
            Phase::Closing => false,
            phase @ (Phase::Opening {
                committed: true, ..
            }
            | Phase::Open) => {
                *phase = Phase::Closing;
                true
            }
        }
    }

    fn begin_close(self: &Arc<Self>, connection: &str, close: Close) {
        if self.claim_close(connection, &close) {
            let shared = Arc::clone(self);
            let connection = connection.to_owned();
            self.executor
                .spawn(Box::pin(shared.finish_close(connection, close)));
        }
    }

    async fn finish_close(self: Arc<Self>, connection: String, close: Close) {
        let target = {
            let table = self.lock();
            table.sockets.get(&connection).and_then(|entry| {
                entry.inbox.clone().map(|inbox| Target {
                    actor: entry.actor.clone(),
                    inbox,
                })
            })
        };
        if let Some(target) = target {
            let ledger = self.ledger.read().await;
            // A failed commit leaves the socket in the ledger, so the next
            // recovery closes it; the peer still receives its close now.
            if let Ok(true) = commit_close(
                &self.store,
                &self.layout,
                &connection,
                &target,
                Some(ledger.segment),
                &close,
            )
            .await
            {
                self.live.fetch_sub(1, Ordering::AcqRel);
                self.written.fetch_add(1, Ordering::AcqRel);
            }
            drop(ledger);
        }
        let entry = self.lock().remove(&connection);
        if let Some(sender) = entry.and_then(|mut entry| entry.final_frame.take()) {
            let _peer_gone = sender.send(
                SocketFrame::Close {
                    code: close.code,
                    reason: close.reason,
                    initiator: close.initiator,
                }
                .encode(),
            );
        }
        self.compact_if_due();
    }

    fn compact_if_due(self: &Arc<Self>) {
        let live = self.live.load(Ordering::Acquire);
        if self.written.load(Ordering::Acquire) < live.saturating_mul(2).saturating_add(SLACK)
            || self.compacting.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let shared = Arc::clone(self);
        self.executor.spawn(Box::pin(async move {
            // A failed compaction keeps the current segment, which stays
            // complete; the next open or close tries again.
            let _kept = shared.compact().await;
            shared.compacting.store(false, Ordering::Release);
        }));
    }

    /// Rewrite the open set into a fresh segment. The new segment is
    /// allocated durably before any record lands in it, and `from` moves only
    /// after it is complete, so a crash at any point loses no open socket.
    async fn compact(&self) -> Result<(), Error> {
        let mut ledger = self.ledger.write().await;
        let epochs = self.layout.epochs()?;
        let next = ledger.allocated + 1;
        let epoch = |from| -> AppendRecord {
            EpochWire {
                version: 1,
                segment: next,
                from,
            }
            .encode_to_vec()
            .into()
        };
        let allocate = |from: u64, expected: u64, label: &'static [u8]| -> Result<_, Error> {
            Ok((
                self.layout.key(&[label, &next.to_be_bytes()])?,
                vec![append(
                    epochs.clone(),
                    vec![epoch(from)],
                    vec![CommitCondition::Tail {
                        path: epochs.clone(),
                        expected,
                    }],
                )],
            ))
        };
        let (key, effects) = allocate(ledger.segment, ledger.epochs, b"allocate")?;
        if !commit(&self.store, key, effects).await? {
            return Err(Error::Conflict);
        }
        ledger.epochs += 1;
        ledger.allocated = next;
        let mut live = BTreeMap::new();
        fold(
            &self.store,
            &self.layout,
            ledger.segment,
            &mut live,
            &mut BTreeSet::new(),
        )
        .await?;
        let records: Vec<AppendRecord> = live
            .iter()
            .map(|(connection, target)| {
                ledger_record(LedgerKind::Opened(OpenedWire {
                    connection: connection.clone(),
                    actor: target.actor.encode().into(),
                    inbox: target.inbox.as_str().into(),
                }))
            })
            .collect();
        let segment = self.layout.segment(next)?;
        for (page, chunk) in records.chunks(PAGE as usize).enumerate() {
            let written = commit(
                &self.store,
                self.layout.key(&[
                    b"compact",
                    &next.to_be_bytes(),
                    &(page as u64).to_be_bytes(),
                ])?,
                vec![append(
                    segment.clone(),
                    chunk.to_vec(),
                    vec![CommitCondition::Tail {
                        path: segment.clone(),
                        expected: (page * PAGE as usize) as u64,
                    }],
                )],
            )
            .await?;
            if !written {
                return Err(Error::Conflict);
            }
        }
        let (key, effects) = allocate(next, ledger.epochs, b"compacted")?;
        if !commit(&self.store, key, effects).await? {
            return Err(Error::Conflict);
        }
        ledger.epochs += 1;
        ledger.segment = next;
        self.written.store(records.len(), Ordering::Release);
        self.live.store(records.len(), Ordering::Release);
        Ok(())
    }

    /// Commit the Open with its socket stream and ledger entry, then wait for
    /// the actor's verdict. Runs on the executor, so a peer that goes away
    /// cannot cancel a commit midway.
    async fn opening(
        self: Arc<Self>,
        connection: String,
        open: SocketOpen,
        verdict: oneshot::Sender<SocketFrame>,
    ) {
        {
            let mut table = self.lock();
            match table.sockets.get_mut(&connection) {
                Some(entry) if matches!(entry.phase, Phase::Unborn) => {
                    entry.phase = Phase::Opening {
                        committed: false,
                        pending: None,
                    };
                }
                _ => return,
            }
        }
        let ticket = match self.commit_open(&connection, &open).await {
            Ok(Some(ticket)) => ticket,
            outcome => {
                self.lock().remove(&connection);
                let (status, reason) = match outcome {
                    Err(Error::Invalid(reason)) => (404, reason),
                    _ => (503, "socket open did not commit"),
                };
                let _peer_gone = verdict.send(SocketFrame::Rejected {
                    status,
                    reason: reason.into(),
                });
                return;
            }
        };
        let pending = {
            let mut table = self.lock();
            let Some(entry) = table.sockets.get_mut(&connection) else {
                return;
            };
            let Phase::Opening { committed, pending } = &mut entry.phase else {
                return;
            };
            *committed = true;
            let pending = pending.take();
            if pending.is_some() {
                entry.phase = Phase::Closing;
            }
            pending
        };
        if let Some(close) = pending {
            drop(verdict);
            self.finish_close(connection, close).await;
            return;
        }
        let reply = self
            .store
            .wait_invocation(ticket)
            .await
            .map_err(|_| host_error("socket verdict is unavailable"))
            .and_then(|reply| crate::sockets::socket_verdict(reply.response()));
        let (frame, close) = match reply {
            Ok(GuestEventResult::SocketAccept {
                subprotocol,
                attachment,
                tags,
            }) => {
                let mut table = self.lock();
                if let Some(entry) = table.sockets.get_mut(&connection)
                    && let Phase::Opening { .. } = entry.phase
                {
                    entry.phase = Phase::Open;
                    entry.tags = tags;
                    entry.attachment = (!attachment.is_empty()).then_some(attachment);
                }
                (SocketFrame::Accepted { subprotocol }, None)
            }
            Ok(GuestEventResult::SocketReject { status, reason }) => (
                SocketFrame::Rejected {
                    status,
                    reason: reason.clone(),
                },
                Some(Close::new(1000, &reason, SocketCloseInitiator::Actor)),
            ),
            Ok(GuestEventResult::Reply { .. }) | Err(_) => (
                SocketFrame::Rejected {
                    status: 500,
                    reason: "actor did not answer the socket open".into(),
                },
                Some(Close::new(
                    1011,
                    "actor did not answer the socket open",
                    SocketCloseInitiator::Runtime,
                )),
            ),
        };
        // Claim the close before the peer can learn the verdict and leave, so
        // the actor's own close is the one delivered.
        if let Some(close) = close {
            self.begin_close(&connection, close);
        }
        let _peer_gone = verdict.send(frame);
    }

    async fn commit_open(
        &self,
        connection: &str,
        open: &SocketOpen,
    ) -> Result<Option<super::InvocationTicket>, Error> {
        let invocation = open_invocation(connection, open)?;
        let socket = self.layout.socket(connection)?;
        let ledger = self.ledger.read().await;
        let epochs = self.layout.epochs()?;
        let segment = self.layout.segment(ledger.segment)?;
        let actor: Bytes = open.actor.encode().into();
        let mut inbox = None;
        let ticket = self
            .store
            .enqueue_joined(&invocation, |input| {
                inbox = Some(input.clone());
                let opened = OpenedWire {
                    connection: connection.into(),
                    actor,
                    inbox: input.as_str().into(),
                };
                Ok(vec![
                    append(
                        socket.clone(),
                        vec![opened.encode_to_vec().into()],
                        vec![
                            CommitCondition::Absent { path: socket },
                            CommitCondition::Tail {
                                path: epochs,
                                expected: ledger.epochs,
                            },
                        ],
                    ),
                    append(
                        segment,
                        vec![ledger_record(LedgerKind::Opened(opened))],
                        Vec::new(),
                    ),
                ])
            })
            .await;
        let ticket = match ticket {
            Ok(ticket) => ticket,
            Err(Error::Conflict) => return Ok(None),
            Err(error) => return Err(error),
        };
        self.live.fetch_add(1, Ordering::AcqRel);
        self.written.fetch_add(1, Ordering::AcqRel);
        if let Some(entry) = self.lock().sockets.get_mut(connection) {
            entry.inbox = inbox;
        }
        drop(ledger);
        Ok(Some(ticket))
    }

    async fn message(
        self: &Arc<Self>,
        connection: &str,
        payload: SocketPayload,
    ) -> Result<(), StreamError> {
        let now = self.clock.now_unix_ms();
        let (target, ordinal) = {
            let mut table = self.lock();
            let Table { sockets, auto, .. } = &mut *table;
            let Some(entry) = sockets.get_mut(connection) else {
                return Ok(());
            };
            match entry.phase {
                Phase::Open => {}
                Phase::Closing => return Ok(()),
                // A peer sends only after Accepted.
                Phase::Unborn | Phase::Opening { .. } => return Err(StreamError::InvalidArgument),
            }
            if let SocketPayload::Text { text } = &payload
                && let Some(pair) = auto.get(&entry.actor)
                && pair.request == *text
            {
                entry.answered_at = Some(now);
                let frame = SocketFrame::Message(SocketPayload::Text {
                    text: pair.response.clone(),
                })
                .encode();
                if entry.outbound.try_send(frame).is_err() {
                    drop(table);
                    self.begin_close(
                        connection,
                        Close::new(1013, "outbound backpressure", SocketCloseInitiator::Runtime),
                    );
                }
                return Ok(());
            }
            entry.messages += 1;
            let Some(inbox) = entry.inbox.clone() else {
                return Err(StreamError::InvalidArgument);
            };
            (
                Target {
                    actor: entry.actor.clone(),
                    inbox,
                },
                entry.messages,
            )
        };
        commit_message(
            &self.store,
            &self.layout,
            connection,
            &target,
            ordinal,
            payload,
        )
        .await
        .map(|_| ())
        .map_err(stream_error)
    }

    /// Run `apply` on `actor`'s socket.
    fn with_entry<R>(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        apply: impl FnOnce(&mut Entry) -> Result<R, GuestError>,
    ) -> Result<R, GuestError> {
        let mut table = self.lock();
        match table.sockets.get_mut(&socket.0) {
            Some(entry) if entry.actor == *actor => apply(entry),
            _ => Err(host_error("unknown WebSocket")),
        }
    }
}

enum Outbound {
    Start {
        connection: String,
        open: SocketOpen,
        frames: mpsc::Receiver<Bytes>,
        last: oneshot::Receiver<Bytes>,
    },
    Open {
        frames: mpsc::Receiver<Bytes>,
        last: oneshot::Receiver<Bytes>,
    },
    Done,
}

impl<P: StreamProvider + ?Sized + 'static> Shared<P> {
    fn outbound(
        self: Arc<Self>,
        start: Outbound,
    ) -> BoxProviderStream<'static, Result<Bytes, StreamError>> {
        Box::pin(futures::stream::unfold(
            (self, start),
            |(shared, state)| async move {
                match state {
                    Outbound::Start {
                        connection,
                        open,
                        frames,
                        last,
                    } => {
                        let (sender, verdict) = oneshot::channel();
                        shared.executor.spawn(Box::pin(
                            Arc::clone(&shared).opening(connection, open, sender),
                        ));
                        match verdict.await {
                            Ok(frame @ SocketFrame::Accepted { .. }) => Some((
                                Ok(frame.encode()),
                                (shared, Outbound::Open { frames, last }),
                            )),
                            Ok(frame) => Some((Ok(frame.encode()), (shared, Outbound::Done))),
                            Err(_) => match last.await {
                                Ok(frame) => Some((Ok(frame), (shared, Outbound::Done))),
                                Err(_) => {
                                    Some((Err(StreamError::Unavailable), (shared, Outbound::Done)))
                                }
                            },
                        }
                    }
                    Outbound::Open {
                        mut frames,
                        mut last,
                    } => {
                        // Frames queued before the close always precede it.
                        tokio::select! {
                            biased;
                            Some(frame) = frames.recv() => {
                                Some((Ok(frame), (shared, Outbound::Open { frames, last })))
                            }
                            frame = &mut last => match frame {
                                Ok(frame) => Some((Ok(frame), (shared, Outbound::Done))),
                                Err(_) => None,
                            },
                        }
                    }
                    Outbound::Done => None,
                }
            },
        ))
    }
}

struct Inbound<P: StreamProvider + ?Sized + 'static> {
    shared: Arc<Shared<P>>,
    connection: String,
}

impl<P: StreamProvider + ?Sized + 'static> SocketInbound for Inbound<P> {
    fn receive(&self, frame: Bytes) -> BoxProviderFuture<'static, Result<(), StreamError>> {
        let shared = Arc::clone(&self.shared);
        let connection = self.connection.clone();
        Box::pin(async move {
            match SocketFrame::decode(&frame).map_err(|_| StreamError::InvalidArgument)? {
                SocketFrame::Message(payload) => shared.message(&connection, payload).await,
                SocketFrame::Close { code, reason, .. } => {
                    shared.begin_close(
                        &connection,
                        Close::new(code, &reason, SocketCloseInitiator::Peer),
                    );
                    Ok(())
                }
                SocketFrame::Accepted { .. } | SocketFrame::Rejected { .. } => {
                    Err(StreamError::InvalidArgument)
                }
            }
        })
    }
}

impl<P: StreamProvider + ?Sized + 'static> Drop for Inbound<P> {
    fn drop(&mut self) {
        self.shared.begin_close(
            &self.connection,
            Close::new(1006, "peer went away", SocketCloseInitiator::Peer),
        );
    }
}

impl<P: StreamProvider + ?Sized + 'static> SocketProvider for Sockets<P> {
    fn resource(&self, open: &[u8]) -> Result<String, StreamError> {
        Ok(SocketOpen::decode(open)
            .map_err(|_| StreamError::InvalidArgument)?
            .resource())
    }

    fn open(
        &self,
        open: Bytes,
        authorization: CallAuthorization,
    ) -> Result<SocketChannel, StreamError> {
        let open = SocketOpen::decode(&open).map_err(|_| StreamError::InvalidArgument)?;
        let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
            .map_err(|_| StreamError::InvalidArgument)?;
        authorization.authorize(
            &open.resource(),
            &[],
            &attributes,
            acyclic_auth::authority::PolicyDecision::Allow,
        )?;
        let shared = &self.shared;
        let connection = format!(
            "{}-{}",
            shared.incarnation,
            shared.next.fetch_add(1, Ordering::Relaxed)
        );
        let (outbound, frames) = mpsc::channel(acyclic_stream::MAX_ITEMS);
        let (final_frame, last) = oneshot::channel();
        {
            let mut table = shared.lock();
            table
                .actors
                .entry(open.actor.clone())
                .or_default()
                .insert(connection.clone());
            table.sockets.insert(
                connection.clone(),
                Entry {
                    actor: open.actor.clone(),
                    inbox: None,
                    phase: Phase::Unborn,
                    tags: Vec::new(),
                    attachment: None,
                    answered_at: None,
                    outbound,
                    final_frame: Some(final_frame),
                    messages: 0,
                },
            );
        }
        Ok(SocketChannel {
            outbound: Arc::clone(shared).outbound(Outbound::Start {
                connection: connection.clone(),
                open,
                frames,
                last,
            }),
            inbound: Box::new(Inbound {
                shared: Arc::clone(shared),
                connection,
            }),
        })
    }
}

impl<P: StreamProvider + ?Sized + 'static> SocketHost for Sockets<P> {
    fn accept_web_socket(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        tags: &[String],
    ) -> Result<(), GuestError> {
        validate_web_socket_tags(tags)?;
        self.shared.with_entry(actor, socket, |entry| {
            entry.tags = tags.to_vec();
            Ok(())
        })
    }

    fn get_web_sockets(
        &self,
        actor: &ActorReference,
        tag: Option<&str>,
    ) -> Result<Vec<WebSocketId>, GuestError> {
        let table = self.shared.lock();
        Ok(table
            .actors
            .get(actor)
            .into_iter()
            .flatten()
            .filter(|connection| {
                table.sockets.get(*connection).is_some_and(|entry| {
                    matches!(entry.phase, Phase::Open)
                        && tag.is_none_or(|tag| entry.tags.iter().any(|own| own == tag))
                })
            })
            .map(|connection| WebSocketId(connection.clone()))
            .collect())
    }

    fn web_socket_send(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        payload: SocketPayload,
    ) -> Result<(), GuestError> {
        let full = self.shared.with_entry(actor, socket, |entry| {
            if !matches!(entry.phase, Phase::Open) {
                return Err(host_error("WebSocket is not open"));
            }
            match entry
                .outbound
                .try_send(SocketFrame::Message(payload).encode())
            {
                Ok(()) => Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => Ok(true),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Err(host_error("WebSocket is not open"))
                }
            }
        })?;
        if full {
            // A peer that stops reading is closed, never buffered without end.
            self.shared.begin_close(
                &socket.0,
                Close::new(1013, "outbound backpressure", SocketCloseInitiator::Runtime),
            );
        }
        Ok(())
    }

    fn web_socket_close(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        code: u16,
        reason: &str,
    ) -> Result<(), GuestError> {
        self.shared.with_entry(actor, socket, |_| Ok(()))?;
        self.shared.begin_close(
            &socket.0,
            Close::new(code, reason, SocketCloseInitiator::Actor),
        );
        Ok(())
    }

    fn serialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        bytes: Bytes,
    ) -> Result<(), GuestError> {
        validate_web_socket_attachment(&bytes)?;
        self.shared.with_entry(actor, socket, |entry| {
            entry.attachment = Some(bytes);
            Ok(())
        })
    }

    fn deserialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Option<Bytes>, GuestError> {
        self.shared
            .with_entry(actor, socket, |entry| Ok(entry.attachment.clone()))
    }

    fn set_web_socket_auto_response(
        &self,
        actor: &ActorReference,
        value: Option<WebSocketAutoResponse>,
    ) -> Result<(), GuestError> {
        let mut table = self.shared.lock();
        match value {
            Some(value) => {
                validate_web_socket_auto_response(&value)?;
                table.auto.insert(actor.clone(), value);
            }
            None => {
                table.auto.remove(actor);
            }
        }
        Ok(())
    }

    fn get_web_socket_auto_response(
        &self,
        actor: &ActorReference,
    ) -> Result<Option<WebSocketAutoResponse>, GuestError> {
        Ok(self.shared.lock().auto.get(actor).cloned())
    }

    fn get_web_socket_auto_response_timestamp(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Option<u64>, GuestError> {
        self.shared
            .with_entry(actor, socket, |entry| Ok(entry.answered_at))
    }

    fn get_tags(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Vec<String>, GuestError> {
        self.shared
            .with_entry(actor, socket, |entry| Ok(entry.tags.clone()))
    }
}

#[cfg(all(test, loom))]
mod loom_tests;
#[cfg(all(test, not(loom)))]
mod tests;
