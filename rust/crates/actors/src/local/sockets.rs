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
//! Every durable socket operation is one keyed request retried, identically,
//! while its outcome is unknown, so a dropped acknowledgement neither loses a
//! Close nor forgets an Open that did commit.
//!
//! Recovery is O(open sockets): the ledger is a current segment of Opened and
//! Closed entries, compacted once it holds more than twice the open set, and
//! `epochs` names the segments that can still hold open sockets.
use super::{Clock, Error, PreparedPublish, Store};
use crate::addressing::ActorReference;
use crate::guest::{
    Effect, GuestError, GuestEventResult, SocketCloseInitiator, SocketHost, SocketPayload,
    WebSocketAutoResponse, WebSocketId,
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
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _};
use prost::Message as _;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use tokio::sync::{RwLock, mpsc, oneshot};

/// Records per ledger read or write; it splits work and bounds nothing.
const PAGE: u32 = 256;
/// Ledger entries tolerated above twice the open set before compaction, so
/// compaction work stays amortized O(1) per open or close.
const SLACK: usize = 256;
/// Longest pause between retries of a request whose outcome is unknown; it
/// paces transport recovery and bounds no work.
const BACKOFF_CEILING_MS: u32 = 1_000;

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
    input: Option<AppendRecord>,
}

impl Close {
    fn new(code: u16, reason: &str, initiator: SocketCloseInitiator) -> Self {
        Self {
            code,
            reason: reason.into(),
            initiator,
            input: None,
        }
    }

    fn prepare(&mut self, connection: &str) -> Result<(), GuestError> {
        if self.input.is_none() {
            self.input = Some(input_record(
                connection,
                SocketInput::Close {
                    code: self.code,
                    reason: self.reason.clone(),
                    initiator: self.initiator.clone(),
                },
            )?);
        }
        Ok(())
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

/// Pacing for retries of one keyed request whose outcome is unknown.
struct Undecided {
    delay: u32,
}

impl Undecided {
    const fn new() -> Self {
        Self { delay: 1 }
    }

    /// Wait before retrying the identical request when `error` leaves its
    /// outcome unknown, as [`Store::submit`] classifies it; any other error
    /// is a decided failure and is returned.
    async fn retry(&mut self, executor: &dyn Executor, error: Error) -> Result<(), Error> {
        if !matches!(
            error,
            Error::Stream(StreamError::Unavailable | StreamError::InvalidResponse)
        ) {
            return Err(error);
        }
        executor.sleep(self.delay).await;
        self.delay = self.delay.saturating_mul(2).min(BACKOFF_CEILING_MS);
        Ok(())
    }
}

/// Commit `effects` until the outcome is decided; `false` means a condition
/// failed. The key makes every retry return the original outcome.
async fn commit<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    executor: &dyn Executor,
    key: IdempotencyKey,
    effects: Vec<Effect>,
) -> Result<bool, Error> {
    let mut publication = PreparedPublish::foreign(key);
    for effect in effects {
        publication.join(effect)?;
    }
    let request = publication.into_request()?;
    let mut undecided = Undecided::new();
    loop {
        match store.provider().commit(request.clone()).await {
            Ok(outcome) => return Ok(matches!(outcome, CommitOutcome::Committed(_))),
            Err(error) => undecided.retry(executor, error.into()).await?,
        }
    }
}

/// Commit the one Close of `connection`: record 1 of its socket stream, the
/// inbox event and the actor's wake, all under `Tail{socket, 1}`. `false`
/// means the socket was already closed. `segment` records the close in the
/// current ledger segment; recovery omits it because it retires the segment.
pub(crate) async fn commit_close<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    executor: &dyn Executor,
    layout: &Layout,
    connection: &str,
    target: &Target,
    segment: Option<u64>,
    close: &Close,
) -> Result<bool, Error> {
    let socket = layout.socket(connection)?;
    let input = close.input.clone().map(Ok).unwrap_or_else(|| {
        input_record(
            connection,
            SocketInput::Close {
                code: close.code,
                reason: close.reason.clone(),
                initiator: close.initiator.clone(),
            },
        )
    })?;
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
    commit(store, executor, key, effects).await
}

/// Commit one peer message, the `ordinal`th of `connection`. A message racing
/// the close loses on `Tail{socket, 1}`, so no event reaches the actor after
/// its Close; `false` reports that loss.
pub(crate) async fn commit_message<P: StreamProvider + ?Sized>(
    store: &Store<P>,
    executor: &dyn Executor,
    layout: &Layout,
    connection: &str,
    target: &Target,
    ordinal: u64,
    record: AppendRecord,
) -> Result<bool, Error> {
    commit(
        store,
        executor,
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

/// What an attempted open commit decided.
enum Opened {
    Committed(super::InvocationTicket),
    /// A decided failure that may follow the commit, so the open may have
    /// reached the actor.
    Unsettled,
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
    /// Peer events accepted from the session, applied in order by one drain
    /// task that exists only while this queue is non-empty.
    inbound: VecDeque<PeerEvent>,
    draining: bool,
    /// A peer Close is terminal even while earlier messages are committing.
    peer_closed: bool,
    /// Observers share the original close producer; they never own its work.
    completion: Option<CloseCompletion>,
    completed: Option<oneshot::Sender<Result<(), Arc<Error>>>>,
    /// Dropped with the entry, which abandons a wait for the open's verdict.
    _abandon: oneshot::Sender<()>,
}

type CloseCompletion = futures::future::Shared<BoxProviderFuture<'static, Result<(), Arc<Error>>>>;

impl crate::guest::SocketTerminal for CloseCompletion {
    fn completion(
        self: Arc<Self>,
    ) -> BoxProviderFuture<'static, Result<(), Arc<crate::guest::SocketTerminalError>>> {
        Box::pin(async move {
            self.as_ref()
                .clone()
                .await
                .map_err(|error| error as Arc<crate::guest::SocketTerminalError>)
        })
    }
}

impl Entry {
    fn close_completion(&mut self) -> CloseCompletion {
        if let Some(completion) = &self.completion {
            return completion.clone();
        }
        let (completed, completion) = oneshot::channel();
        let completion: BoxProviderFuture<'static, Result<(), Arc<Error>>> = Box::pin(async move {
            completion
                .await
                .unwrap_or_else(|_| Err(Arc::new(Error::Stream(StreamError::Unavailable))))
        });
        let completion = completion.shared();
        self.completed = Some(completed);
        self.completion = Some(completion.clone());
        completion
    }
}

/// The claimed close owns retirement even if its executor drops the future.
struct CloseOwner<P: StreamProvider + ?Sized + 'static> {
    shared: Arc<Shared<P>>,
    connection: String,
    completed: Option<oneshot::Sender<Result<(), Arc<Error>>>>,
    retire: bool,
}

impl<P: StreamProvider + ?Sized + 'static> Drop for CloseOwner<P> {
    fn drop(&mut self) {
        if self.retire {
            self.shared.lock().remove(&self.connection);
            // Closing this original producer returns typed loss to every waiter.
        }
    }
}

/// One peer event awaiting its turn.
enum PeerEvent {
    Message(AppendRecord),
    Close(Close),
}

/// What a drain does next, decided under the table lock.
enum Step {
    Commit {
        target: Target,
        ordinal: u64,
        record: AppendRecord,
    },
    Close(Close),
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
/// Peer opens require `actors/invoke` for the namespace within the credential's
/// certified account, checked before allocating or publishing the connection.
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

fn host_error(reason: &str) -> GuestError {
    GuestError::Handler(reason.into())
}

impl<P: StreamProvider + ?Sized + 'static> Sockets<P> {
    /// Open the socket host of `store`'s runtime. Every socket a previous
    /// incarnation left open receives Close(Runtime, 1012) first, in work
    /// proportional to those sockets, and that incarnation can open no more.
    /// Of hosts opening concurrently, at most one succeeds; the others fail
    /// with [`Error::Conflict`].
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
        // Each ownership attempt has its own keys: a concurrent host fencing
        // the same tail conflicts instead of replaying this attempt's result.
        let attempt = uuid::Uuid::new_v4();
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
            &*executor,
            layout.key(&[b"fence", &incarnation.to_be_bytes(), attempt.as_bytes()])?,
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
        let (store_ref, executor_ref, layout_ref, recovered_ref) =
            (&store, &*executor, &layout, &recovered);
        let mut closing =
            futures::stream::iter(live.into_iter().map(|(connection, target)| async move {
                commit_close(
                    store_ref,
                    executor_ref,
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
            &*executor,
            layout.key(&[b"recovered", &incarnation.to_be_bytes(), attempt.as_bytes()])?,
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
    /// committed one, including a socket whose open is still committing.
    /// Cancelling this observer leaves each original close running. A decided
    /// publication refusal or loss of its executor returns the original error.
    pub async fn drain(&self) -> Result<(), Arc<Error>> {
        let close = Close::new(1001, "runtime draining", SocketCloseInitiator::Runtime);
        let claims = {
            let mut table = self.shared.lock();
            let connections: Vec<String> = table.sockets.keys().cloned().collect();
            connections
                .into_iter()
                .filter_map(|connection| {
                    self.shared
                        .claim_close(&mut table, &connection, &close)
                        .map(|claim| (connection, claim))
                })
                .collect::<Vec<_>>()
        };
        let completions = claims
            .into_iter()
            .map(|(connection, claim)| self.shared.start_close(&connection, close.clone(), claim));
        futures::future::try_join_all(completions).await?;
        Ok(())
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

    /// Pin the original completion while moving toward the one close under
    /// the same table lock. A pending Open retains the producer until it
    /// settles; otherwise only the returned owner may finish the close.
    fn claim_close(
        self: &Arc<Self>,
        table: &mut Table,
        connection: &str,
        close: &Close,
    ) -> Option<(Option<CloseOwner<P>>, CloseCompletion)> {
        let Some(entry) = table.sockets.get_mut(connection) else {
            return None;
        };
        match &mut entry.phase {
            Phase::Unborn => {
                table.remove(connection);
                None
            }
            Phase::Opening {
                committed: false,
                pending,
            } => {
                pending.get_or_insert_with(|| close.clone());
                Some((None, entry.close_completion()))
            }
            Phase::Closing => Some((None, entry.close_completion())),
            phase @ (Phase::Opening {
                committed: true, ..
            }
            | Phase::Open) => {
                *phase = Phase::Closing;
                let completion = entry.close_completion();
                Some((
                    Some(CloseOwner {
                        shared: Arc::clone(self),
                        connection: connection.to_owned(),
                        completed: entry.completed.take(),
                        retire: true,
                    }),
                    completion,
                ))
            }
        }
    }

    fn begin_close(self: &Arc<Self>, connection: &str, close: Close) -> Option<CloseCompletion> {
        let claim = self.claim_close(&mut self.lock(), connection, &close)?;
        Some(self.start_close(connection, close, claim))
    }

    fn start_close(
        self: &Arc<Self>,
        connection: &str,
        close: Close,
        (owner, completion): (Option<CloseOwner<P>>, CloseCompletion),
    ) -> CloseCompletion {
        if let Some(owner) = owner {
            let shared = Arc::clone(self);
            let connection = connection.to_owned();
            self.executor
                .spawn(Box::pin(shared.finish_close(connection, close, owner)));
        }
        completion
    }

    async fn finish_close(
        self: Arc<Self>,
        connection: String,
        close: Close,
        mut owner: CloseOwner<P>,
    ) {
        let target = {
            let table = self.lock();
            table.sockets.get(&connection).and_then(|entry| {
                entry.inbox.clone().map(|inbox| Target {
                    actor: entry.actor.clone(),
                    inbox,
                })
            })
        };
        let result = if let Some(target) = target {
            let ledger = self.ledger.read().await;
            let result = commit_close(
                &self.store,
                &*self.executor,
                &self.layout,
                &connection,
                &target,
                Some(ledger.segment),
                &close,
            )
            .await;
            if matches!(result, Ok(true)) {
                self.live.fetch_sub(1, Ordering::AcqRel);
                self.written.fetch_add(1, Ordering::AcqRel);
            }
            drop(ledger);
            result.and_then(|committed| committed.then_some(()).ok_or(Error::Conflict))
        } else {
            Err(Error::Invalid("closing socket has no input"))
        };
        let final_frame = self
            .lock()
            .remove(&connection)
            .and_then(|mut entry| entry.final_frame.take());
        owner.retire = false;
        if result.is_ok()
            && let Some(sender) = final_frame
        {
            let _peer_gone = sender.send(
                SocketFrame::Close {
                    code: close.code,
                    reason: close.reason,
                    initiator: close.initiator,
                }
                .encode(),
            );
        }
        if let Some(completed) = owner.completed.take() {
            let _observer_gone = completed.send(result.map_err(Arc::new));
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
        let executor = &*self.executor;
        let (key, effects) = allocate(ledger.segment, ledger.epochs, b"allocate")?;
        if !commit(&self.store, executor, key, effects).await? {
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
                executor,
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
        if !commit(&self.store, executor, key, effects).await? {
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
    /// cannot cancel a commit midway; the verdict wait ends when the socket
    /// does.
    async fn opening(
        self: Arc<Self>,
        connection: String,
        open: SocketOpen,
        verdict: oneshot::Sender<SocketFrame>,
        abandoned: oneshot::Receiver<()>,
        mut owner: CloseOwner<P>,
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
        let (ticket, unsettled) = match self.commit_open(&connection, &open).await {
            Ok(Opened::Committed(ticket)) => (Some(ticket), false),
            Ok(Opened::Unsettled) => (None, true),
            outcome => {
                // Refused by a condition, or never attempted: nothing
                // reached the actor.
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
        // Committed, or attempted with a decided failure that may follow the
        // commit: either way the socket now owes its one Close. Closing an
        // open that never committed fails its `Tail{socket, 1}` and does
        // nothing.
        let pending = {
            let mut table = self.lock();
            let Some(entry) = table.sockets.get_mut(&connection) else {
                return;
            };
            let Phase::Opening { committed, pending } = &mut entry.phase else {
                return;
            };
            *committed = true;
            let pending = pending.take().or_else(|| {
                unsettled.then(|| {
                    Close::new(
                        1011,
                        "socket open did not settle",
                        SocketCloseInitiator::Runtime,
                    )
                })
            });
            pending
        };
        if let Some(close) = pending {
            if unsettled {
                let _peer_gone = verdict.send(SocketFrame::Rejected {
                    status: 503,
                    reason: "socket open did not settle".into(),
                });
            } else {
                drop(verdict);
            }
            let _completion = self.begin_close(&connection, close);
            owner.retire = false;
            return;
        }
        let Some(ticket) = ticket else {
            return;
        };
        let wait = Box::pin(self.store.wait_invocation(ticket));
        let reply = match futures::future::select(wait, abandoned).await {
            futures::future::Either::Left((reply, _)) => reply,
            // The socket ended first, and its Close is already committed.
            futures::future::Either::Right(_) => return,
        };
        let reply = reply
            .map_err(|_| host_error("socket verdict is unavailable"))
            .and_then(|reply| crate::sockets::socket_verdict(reply.response(), &open));
        let (mut frame, close) = match reply {
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
        if let Some(mut close) = close {
            if close.prepare(&connection).is_err() {
                let reason = "socket verdict exceeds the inbox record bound";
                frame = SocketFrame::Rejected {
                    status: 500,
                    reason: reason.into(),
                };
                close = Close::new(1011, reason, SocketCloseInitiator::Runtime);
            }
            let _completion = self.begin_close(&connection, close);
        }
        let _peer_gone = verdict.send(frame);
        owner.retire = false;
    }

    /// Commit the Open, retrying it identically while its outcome is
    /// unknown. `Err` means it was never attempted or a condition refused it.
    async fn commit_open(&self, connection: &str, open: &SocketOpen) -> Result<Opened, Error> {
        let invocation = open_invocation(connection, open)?;
        let socket = self.layout.socket(connection)?;
        // Held across retries, so every attempt carries the same conditions.
        let ledger = self.ledger.read().await;
        let epochs = self.layout.epochs()?;
        let segment = self.layout.segment(ledger.segment)?;
        let actor: Bytes = open.actor.encode().into();
        let mut inbox = None;
        let mut undecided = Undecided::new();
        let outcome = loop {
            let attempt = self
                .store
                .enqueue_joined(&invocation, |input| {
                    inbox = Some(input.clone());
                    let opened = OpenedWire {
                        connection: connection.into(),
                        actor: actor.clone(),
                        inbox: input.as_str().into(),
                    };
                    Ok(vec![
                        append(
                            socket.clone(),
                            vec![opened.encode_to_vec().into()],
                            vec![
                                CommitCondition::Absent {
                                    path: socket.clone(),
                                },
                                CommitCondition::Tail {
                                    path: epochs.clone(),
                                    expected: ledger.epochs,
                                },
                            ],
                        ),
                        append(
                            segment.clone(),
                            vec![ledger_record(LedgerKind::Opened(opened))],
                            Vec::new(),
                        ),
                    ])
                })
                .await;
            match attempt {
                Ok(ticket) => break Ok(ticket),
                Err(error) => {
                    if let Err(error) = undecided.retry(&*self.executor, error).await {
                        break Err(error);
                    }
                }
            }
        };
        let opened = match (outcome, inbox) {
            (Ok(ticket), Some(inbox)) => (Opened::Committed(ticket), inbox),
            // `enqueue_joined` builds the request before committing, so
            // without an inbox nothing was attempted.
            (Ok(_), None) => return Err(Error::Invalid("socket open has no inbox")),
            // Never attempted, or refused by a condition: nothing reached
            // the actor.
            (Err(error), None) | (Err(error @ Error::Conflict), Some(_)) => return Err(error),
            (Err(_), Some(inbox)) => (Opened::Unsettled, inbox),
        };
        self.live.fetch_add(1, Ordering::AcqRel);
        self.written.fetch_add(1, Ordering::AcqRel);
        if let Some(entry) = self.lock().sockets.get_mut(connection) {
            entry.inbox = Some(opened.1);
        }
        drop(ledger);
        Ok(opened.0)
    }

    /// Accept one peer message without I/O: answer a keepalive in place, or
    /// queue the message behind the socket's earlier peer events.
    fn message(
        self: &Arc<Self>,
        connection: &str,
        payload: SocketPayload,
    ) -> Result<(), StreamError> {
        let now = self.clock.now_unix_ms();
        let mut table = self.lock();
        let Table { sockets, auto, .. } = &mut *table;
        let Some(entry) = sockets.get_mut(connection) else {
            return Ok(());
        };
        if entry.peer_closed {
            return Ok(());
        }
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
                let _completion = self.begin_close(
                    connection,
                    Close::new(1013, "outbound backpressure", SocketCloseInitiator::Runtime),
                );
            }
            return Ok(());
        }
        // A peer that outruns its socket's commits is closed, as one that
        // stops reading is, never buffered without end.
        if entry.inbound.len() >= acyclic_stream::MAX_ITEMS {
            drop(table);
            let _completion = self.begin_close(
                connection,
                Close::new(1013, "inbound backpressure", SocketCloseInitiator::Runtime),
            );
            return Ok(());
        }
        let record = input_record(connection, SocketInput::Message(payload))
            .map_err(|_| StreamError::InvalidArgument)?;
        entry.inbound.push_back(PeerEvent::Message(record));
        let start = !std::mem::replace(&mut entry.draining, true);
        drop(table);
        if start {
            self.executor
                .spawn(Box::pin(Arc::clone(self).drain(connection.to_owned())));
        }
        Ok(())
    }

    /// The peer's close follows every message it sent before it.
    fn peer_close(self: &Arc<Self>, connection: &str, mut close: Close) -> Result<(), StreamError> {
        {
            let mut table = self.lock();
            if let Some(entry) = table.sockets.get_mut(connection) {
                if entry.peer_closed {
                    return Ok(());
                }
                close
                    .prepare(connection)
                    .map_err(|_| StreamError::InvalidArgument)?;
                entry.peer_closed = true;
                if entry.draining {
                    entry.inbound.push_back(PeerEvent::Close(close));
                    return Ok(());
                }
            }
        }
        let _completion = self.begin_close(connection, close);
        Ok(())
    }

    /// Apply `connection`'s queued peer events in order, then exit.
    async fn drain(self: Arc<Self>, connection: String) {
        loop {
            match self.next_peer_event(&connection) {
                None => return,
                Some(Step::Close(close)) => {
                    let claim = self.claim_close(&mut self.lock(), &connection, &close);
                    if let Some((Some(owner), _completion)) = claim {
                        self.finish_close(connection, close, owner).await;
                    }
                    return;
                }
                Some(Step::Commit {
                    target,
                    ordinal,
                    record,
                }) => {
                    let committed = commit_message(
                        &self.store,
                        &*self.executor,
                        &self.layout,
                        &connection,
                        &target,
                        ordinal,
                        record,
                    )
                    .await;
                    if committed.is_err() {
                        let _completion = self.begin_close(
                            &connection,
                            Close::new(
                                1011,
                                "socket message did not commit",
                                SocketCloseInitiator::Runtime,
                            ),
                        );
                    }
                }
            }
        }
    }

    /// Take the next queued peer event, or end the drain: `None` clears the
    /// draining flag, so the next accepted event starts a new drain.
    fn next_peer_event(&self, connection: &str) -> Option<Step> {
        let mut table = self.lock();
        let entry = table.sockets.get_mut(connection)?;
        let event = if matches!(entry.phase, Phase::Open) {
            entry.inbound.pop_front()
        } else {
            // Closing drops what is left: it would lose on the socket's
            // `Tail{socket, 1}` anyway.
            None
        };
        let step = match (event, entry.inbox.clone()) {
            (Some(PeerEvent::Message(record)), Some(inbox)) => {
                entry.messages += 1;
                return Some(Step::Commit {
                    target: Target {
                        actor: entry.actor.clone(),
                        inbox,
                    },
                    ordinal: entry.messages,
                    record,
                });
            }
            (Some(PeerEvent::Close(close)), _) => Some(Step::Close(close)),
            // An open socket always has its inbox.
            (Some(PeerEvent::Message(_)) | None, _) => None,
        };
        entry.inbound.clear();
        entry.draining = false;
        step
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
        abandoned: oneshot::Receiver<()>,
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
                        abandoned,
                    } => {
                        let (sender, verdict) = oneshot::channel();
                        let owner = CloseOwner {
                            shared: Arc::clone(&shared),
                            connection: connection.clone(),
                            completed: None,
                            retire: true,
                        };
                        shared.executor.spawn(Box::pin(
                            Arc::clone(&shared).opening(connection, open, sender, abandoned, owner),
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
                                Err(_) => Some((Err(StreamError::Unavailable), (shared, Outbound::Done))),
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
    fn receive(&self, frame: Bytes) -> Result<(), StreamError> {
        match SocketFrame::decode(&frame).map_err(|_| StreamError::InvalidArgument)? {
            SocketFrame::Message(payload) => self.shared.message(&self.connection, payload),
            SocketFrame::Close { code, reason, .. } => self.shared.peer_close(
                &self.connection,
                Close::new(code, &reason, SocketCloseInitiator::Peer),
            ),
            SocketFrame::Accepted { .. } | SocketFrame::Rejected { .. } => {
                Err(StreamError::InvalidArgument)
            }
        }
    }
}

impl<P: StreamProvider + ?Sized + 'static> Drop for Inbound<P> {
    fn drop(&mut self) {
        let _ended = self.shared.peer_close(
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
        authorization.authorize_in_account(
            open.actor.account().as_bytes(),
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
        let (abandon, abandoned) = oneshot::channel();
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
                    inbound: VecDeque::new(),
                    draining: false,
                    peer_closed: false,
                    completion: None,
                    completed: None,
                    _abandon: abandon,
                },
            );
        }
        Ok(SocketChannel {
            outbound: Arc::clone(shared).outbound(Outbound::Start {
                connection: connection.clone(),
                open,
                frames,
                last,
                abandoned,
            }),
            inbound: Box::new(Inbound {
                shared: Arc::clone(shared),
                connection,
            }),
        })
    }
}

impl<P: StreamProvider + ?Sized + 'static> SocketHost for Sockets<P> {
    fn web_socket_terminal(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
    ) -> Result<Arc<dyn crate::guest::SocketTerminal>, GuestError> {
        self.shared.with_entry(actor, socket, |entry| {
            Ok(Arc::new(entry.close_completion()) as Arc<dyn crate::guest::SocketTerminal>)
        })
    }

    fn accept_web_socket(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        tags: &[String],
    ) -> Result<(), GuestError> {
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
            let frame = SocketFrame::Message(payload).encode();
            if frame.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(host_error(
                    "WebSocket frame exceeds the channel record bound",
                ));
            }
            match entry.outbound.try_send(frame) {
                Ok(()) => Ok(false),
                Err(mpsc::error::TrySendError::Full(_)) => Ok(true),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Err(host_error("WebSocket is not open"))
                }
            }
        })?;
        if full {
            // A peer that stops reading is closed, never buffered without end.
            let _completion = self.shared.begin_close(
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
        let mut close = Close::new(code, reason, SocketCloseInitiator::Actor);
        close.prepare(&socket.0)?;
        let _completion = self.shared.begin_close(&socket.0, close);
        Ok(())
    }

    fn serialize_attachment(
        &self,
        actor: &ActorReference,
        socket: &WebSocketId,
        bytes: Bytes,
    ) -> Result<(), GuestError> {
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
        if value
            .as_ref()
            .is_some_and(|value| value.response_frame_len() > acyclic_stream::MAX_RECORD_BYTES)
        {
            return Err(host_error(
                "WebSocket frame exceeds the channel record bound",
            ));
        }
        let mut table = self.shared.lock();
        match value {
            Some(value) => {
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
