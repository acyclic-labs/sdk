use super::sync::{Arc, AtomicU64, Mutex, Ordering};
use super::*;
use crate::{BoxProviderFuture, BoxProviderStream, RecordStream};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod native;
#[cfg(test)]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
use crate::grpc::ChannelConnector as Connect;
#[cfg(all(any(test, loom), feature = "grpc", not(target_arch = "wasm32")))]
use tonic::transport::Channel as TransportChannel;
pub mod transport;
use transport::Endpoint;

mod admissions;
mod application;
mod call;
mod data;
use admissions::Admissions;
pub use application::{ApplicationDelivery, ApplicationSubscription};
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod cache;
mod credit;
mod observation;
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) use cache::Cache;
use observation::Observation;
mod outbound;
use credit::{Credit, Released};
use outbound::Outbound;
type ReleaseSender = Arc<signal::Signal<Released>>;
mod link;
mod pool;
mod routes;
#[cfg(loom)]
pub use admissions::{verify_admission_binding, verify_admission_cancellation};
#[cfg(all(loom, feature = "grpc", not(target_arch = "wasm32")))]
pub use cache::verify_cache;
#[cfg(all(loom, feature = "grpc", not(target_arch = "wasm32")))]
pub use pool::model::{
    verify_abandoned_reconnect, verify_cancel_before_open, verify_follow_data,
    verify_interest_accounting, verify_link_isolation, verify_node_dials, verify_pool_lifetime,
    verify_registration_deadline, verify_terminal_cursor_dial,
};
#[cfg(all(loom, feature = "grpc", not(target_arch = "wasm32")))]
pub use pool::retry_model::verify_initial_retry_yield;
use routes::Routes;
mod progress;
use progress::Progress;
#[cfg(loom)]
pub use progress::verify_latest_progress;
// The client tests drive the native gRPC transport.
#[cfg(test)]
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod tests;
type Ready = (String, Link, Response);
type Dial = BoxProviderFuture<'static, (String, u64, Result<Ready, StreamError>)>;
type Response = transport::Responses;
type Read = BoxProviderFuture<'static, (String, u64, Response, Option<Result<Frame, StreamError>>)>;
const QUEUE: usize = 2;
const FRAME_BYTES: u64 = crate::MAX_RECORD_BYTES as u64 + 128;

/// Shared pool owning one bidirectional Streams session per authenticated node.
#[derive(Clone)]
pub struct Client {
    pool: PoolHandle,
    /// Requirements owned by this operation, independently of the transport.
    revisions: Arc<crate::RevisionTracker>,
}

/// The shared transport owners, without an operation's causal requirements.
#[derive(Clone)]
pub(crate) struct PoolHandle {
    connections: super::Connections,
    commands: mpsc::Sender<Command>,
    removed: Arc<signal::Signal<u64>>,
    next_id: Arc<AtomicU64>,
    active: Arc<AtomicU64>,
    admissions: Arc<Admissions>,
    attempt_epoch: Arc<AtomicU64>,
    abandoned: Arc<gate::Gate>,
}

impl PoolHandle {
    pub(crate) fn with_revisions(self, revisions: Arc<crate::RevisionTracker>) -> Client {
        Client {
            pool: self,
            revisions,
        }
    }

    fn abandon_pending(&self) -> Result<(), StreamError> {
        self.attempt_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                epoch.checked_add(1)
            })
            .map_err(|_| StreamError::Capacity)?;
        self.abandoned.set(true)
    }
}
struct Delivery {
    before: Option<wire::SessionFrame>,
    item: Frame,
    sequence: u64,
    generation: u64,
    credit: Credit,
}
impl Delivery {
    /// A skipped cut shares the next data envelope and retains its data credit.
    fn consume_before(&mut self, progress: &Progress) -> Result<Option<Frame>, StreamError> {
        let Some(frame) = self.before.take() else {
            return Ok(None);
        };
        let fresh = consume_frame(progress, self.generation, self.sequence, &frame)?;
        Ok(fresh.then_some(frame.into()))
    }
    fn consume(&self, progress: &Progress) -> Result<bool, StreamError> {
        consume_frame(progress, self.generation, self.sequence, self.item.wire())
    }
}
fn consume_frame(
    progress: &Progress,
    generation: u64,
    sequence: u64,
    frame: &wire::SessionFrame,
) -> Result<bool, StreamError> {
    match &frame.body {
        Some(Body::Data(data)) => consume_data(progress, generation, sequence, data),
        Some(Body::Watermark(mark)) => match &mark.after {
            Some(after) => progress.consume_progress(
                generation,
                after.next,
                mark.preceding_commit_id
                    .as_ref()
                    .try_into()
                    .map_err(|_| StreamError::Unavailable)?,
            ),
            None => progress.current(generation),
        },
        _ => Err(StreamError::Unavailable),
    }
}
fn consume_data(
    progress: &Progress,
    generation: u64,
    sequence: u64,
    data: &wire::SessionData,
) -> Result<bool, StreamError> {
    match &data.item {
        Some(Item::Record(record)) => progress.consume_record(
            generation,
            sequence,
            record
                .commit_id
                .as_ref()
                .try_into()
                .map_err(|_| StreamError::Unavailable)?,
        ),
        Some(
            Item::LatestValue(_) | Item::ActorCall(_) | Item::ChildrenPage(_) | Item::Heads(_),
        ) => progress.consume_latest(generation, sequence),
        _ => Err(StreamError::Unavailable),
    }
}

enum Command {
    Credential { slot: u32, credential: Bytes },
    Open(Box<OpenCommand>),
}
struct OpenCommand {
    id: u64,
    revisions: Arc<crate::RevisionTracker>,
    attempt_epoch: u64,
    acknowledge: bool,
    kind: Kind,
    filter: Option<wire::SessionFilter>,
    credential: u32,
    raw: bool,
    progress: Arc<Progress>,
    records: mpsc::Sender<Result<Delivery, StreamError>>,
    ready: oneshot::Sender<Result<u64, StreamError>>,
    observation: super::runtime::OperationSpan,
}

struct Route {
    node: String,
    revisions: Arc<crate::RevisionTracker>,
    attempt_epoch: u64,
    kind: Kind,
    filter: Option<wire::SessionFilter>,
    credential: u32,
    raw: bool,
    watermark: Option<wire::SessionFrame>,
    buffered_data: u32,
    generation: u64,
    placement_epoch: u64,
    received: u64,
    received_opaque: Bytes,
    acknowledge: bool,
    acknowledged: u64,
    opened: bool,
    credit: wire::SessionWindow,
    progress: Arc<Progress>,
    records: mpsc::Sender<Result<Delivery, StreamError>>,
    ready: Option<oneshot::Sender<Result<u64, StreamError>>>,
    observation: super::runtime::OperationSpan,
}
struct Link {
    opening_credentials: BTreeMap<u32, Bytes>,
    observation: Option<Observation>,
    epoch: u64,
    credit: wire::SessionWindow,
    channel: Endpoint,
    sender: Arc<Outbound<wire::SessionFrame>>,
    revision: u64,
    applied_revision: u64,
    released: wire::SessionWindow,
    /// Revisions this node has served on an open; it has applied them, so
    /// later opens on this session need not carry them.
    served: crate::RevisionCut,
}
struct Pool {
    credentials: BTreeMap<u32, Bytes>,
    connections: super::Connections,
    executor: std::sync::Arc<dyn crate::runtime::Executor>,
    identity: Bytes,
    connect: std::sync::Arc<transport::Connect>,
    home: String,
    routes: Routes,
    links: BTreeMap<String, Link>,
    reads: FuturesUnordered<Read>,
    dials: BTreeMap<String, Dial>,
    pending: VecDeque<(u64, u64)>,
    home_channel: Endpoint,
    next_link: u64,
    active: Arc<AtomicU64>,
    admissions: Arc<Admissions>,
    attempt_epoch: Arc<AtomicU64>,
    abandoned: Arc<gate::Gate>,
    dial_epoch: u64,
    commands: mpsc::Receiver<Command>,
    removed: Arc<signal::Signal<u64>>,
    released: ReleaseSender,
    release_sender: ReleaseSender,
}
impl Route {
    fn live(&self) -> bool {
        !self.progress.cancelled() && !self.records.is_closed()
    }
    /// Live follows resume from their cursor. An opened finite read cannot
    /// reopen against a different snapshot; a children page is reissued only
    /// before its first fragment. An Actors call is never replayed.
    fn resumable(&self) -> bool {
        match &self.kind {
            Kind::Follow(follow) => (follow.until == 0 && follow.key.is_none()) || !self.opened,
            Kind::ChildrenPage(_) | Kind::Heads(_) => self.received == 0,
            _ => false,
        }
    }
}
impl Drop for Link {
    fn drop(&mut self) {
        let _ = self.sender.close();
        if let Some(observation) = &mut self.observation {
            observation.finish(None);
        }
    }
}
impl Client {
    /// Selects one implicit authority while sharing this pool, credentials,
    /// channels and observations. Explicit minima are retained unchanged.
    #[must_use]
    pub fn with_revision_scope(&self, scope: crate::RevisionScope) -> Self {
        self.clone()
            .with_revisions(crate::RevisionTracker::scoped(&self.revisions, scope))
    }

    /// Uses an explicitly owned causal context on this existing Session pool.
    /// Clones sharing `revisions` share read-your-writes requirements; unrelated
    /// operations may supply distinct contexts without opening new connections.
    /// Keep the context through every dependent operation and reconnect. Ending
    /// its lifetime does not prove coverage or authorize discarding a minimum
    /// still needed elsewhere. Explicit request minima are never filtered.
    #[must_use]
    pub fn with_revisions(mut self, revisions: Arc<crate::RevisionTracker>) -> Self {
        self.revisions = revisions;
        self
    }

    #[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
    pub(crate) fn into_pool(self) -> PoolHandle {
        self.pool
    }

    /// Shares one protocol pool across platform endpoints and every logical follow.
    pub fn with_transport(
        channel: Endpoint,
        connect: std::sync::Arc<transport::Connect>,
        executor: std::sync::Arc<dyn crate::runtime::Executor>,
    ) -> Self {
        Self::tracking(channel, connect, executor, Arc::default())
    }

    /// Like [`Self::with_transport`], raising the supplied causal context.
    /// Retain it across pool or credential changes to keep read-your-writes.
    pub fn tracking(
        channel: Endpoint,
        connect: std::sync::Arc<transport::Connect>,
        executor: std::sync::Arc<dyn crate::runtime::Executor>,
        revisions: Arc<crate::RevisionTracker>,
    ) -> Self {
        let (commands, receiver) = mpsc::channel(crate::MAX_ITEMS);
        let connections = super::Connections::new();
        // Drop produces at most one removal per admitted channel; it must never block.
        let removed = Arc::new(signal::Signal::new());
        let release_sender = Arc::new(signal::Signal::new());
        let active = Arc::new(AtomicU64::new(0));
        let admissions = Arc::new(Admissions::new());
        let attempt_epoch = Arc::new(AtomicU64::new(0));
        let abandoned = Arc::new(gate::Gate::new());
        let mut pool = Pool {
            credentials: BTreeMap::new(),
            connections: connections.clone(),
            executor: executor.clone(),
            identity: crate::new_identity_bytes(),
            connect,
            home: String::new(),
            routes: Routes::new(),
            links: BTreeMap::new(),
            reads: FuturesUnordered::new(),
            dials: BTreeMap::new(),
            pending: VecDeque::new(),
            home_channel: channel,
            next_link: 1,
            active: Arc::clone(&active),
            admissions: Arc::clone(&admissions),
            attempt_epoch: Arc::clone(&attempt_epoch),
            abandoned: Arc::clone(&abandoned),
            dial_epoch: 0,
            commands: receiver,
            removed: Arc::clone(&removed),
            released: Arc::clone(&release_sender),
            release_sender,
        };
        executor.spawn(Box::pin(async move {
            if let Err(error) = pool.run().await {
                pool.fail(&error);
            }
        }));
        PoolHandle {
            connections,
            commands,
            removed,
            next_id: Arc::new(AtomicU64::new(1)),
            active,
            admissions,
            attempt_epoch,
            abandoned,
        }
        .with_revisions(revisions)
    }
    /// This operation's revision tracker; transports record their receipts
    /// here so its later reads and follows cover them.
    #[must_use]
    pub fn revisions(&self) -> &Arc<crate::RevisionTracker> {
        &self.revisions
    }
    /// Registers or renews a bounded capability slot on every physical session.
    /// Empty bytes drop it. Registration is ordered before subsequent opens;
    /// the server rechecks bound channels and reports refusal on those channels.
    pub fn register_credential(&self, slot: u32, credential: Bytes) -> Result<(), StreamError> {
        if slot as usize >= Limits::default().channels {
            return Err(StreamError::Capacity);
        }
        let frame = frame(
            0,
            0,
            Body::Credential(wire::SessionCredential {
                slot,
                credential: credential.clone(),
            }),
        );
        if frame.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        self.pool
            .commands
            .try_send(Command::Credential { slot, credential })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => StreamError::Capacity,
                mpsc::error::TrySendError::Closed(_) => StreamError::Unavailable,
            })
    }
    /// Observes successful handshakes without depending on further task records.
    #[must_use]
    pub fn connections(&self) -> super::Connections {
        self.pool.connections.clone()
    }

    /// Cancels pending handshakes when their credential scope ends.
    pub fn abandon_pending(&self) -> Result<(), StreamError> {
        self.pool.abandon_pending()
    }
    /// Opens a logical follow on the shared session, without a per-stream RPC.
    pub async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        self.follow_with_ack(request, true).await
    }
    /// Reads `[from, min(from + limit, tail at open))` as one bounded follow
    /// on the shared session, with the semantics of [`crate::StreamProvider::read`].
    /// An opened read fails on connection loss or placement moves rather than
    /// resuming against a different snapshot. The caller may start a new read.
    pub async fn read(&self, request: crate::ReadRequest) -> Result<RecordStream, StreamError> {
        self.read_with_ack(request, true).await
    }
    pub(crate) async fn read_with_ack(
        &self,
        request: crate::ReadRequest,
        acknowledge: bool,
    ) -> Result<RecordStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        let path = request.path.clone();
        // The pool adds the tracked floor on every physical open.
        let follow = wire::SessionFollow {
            key: wire_codec::read_key_to_wire(&request.key),
            read_limit: request.key.as_ref().map_or(0, |_| request.limit),
            bootstrap_head: false,
            when_absent: Default::default(),
            resource: crate::wire_codec::session::stream_resource(request.path.as_str()),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: Bytes::new(),
            until: request.key.as_ref().map_or_else(
                || request.from.saturating_add(u64::from(request.limit)),
                |key| key.before,
            ),
            at_least: crate::wire_codec::cut_to_wire(&request.at_least),
        };
        wire_codec::session::validate_follow(&follow, &cursor(request.from))?;
        wire_codec::cut_from_wire(follow.at_least.clone())?;
        let mut records = super::runtime::boxed(
            data::Data::new(
                self.open_kind(
                    Kind::Follow(follow),
                    request.from,
                    acknowledge,
                    Some(crate::FollowFilter::default().transport_filter()?),
                )
                .await?,
            )
            .map(move |item| item?.into_record(&path)),
        );
        // The open resolves the stream as every provider read does: an absent
        // path or a start beyond the tail fails here, not in the stream.
        let first = records.next().await;
        if let Some(Err(error)) = first {
            return Err(error);
        }
        Ok(super::runtime::boxed(
            futures::stream::iter(first).chain(records),
        ))
    }
    pub(crate) async fn follow_with_ack(
        &self,
        request: crate::FollowRequest,
        acknowledge: bool,
    ) -> Result<crate::FollowStream, StreamError> {
        Ok(super::runtime::boxed(
            self.follow_updates(request, acknowledge).await?,
        ))
    }
    /// Follows only records, boxed once and carrying no state beyond the
    /// subscription: the typed form of [`Self::follow`] without progress.
    pub async fn follow_records(
        &self,
        request: crate::FollowRequest,
    ) -> Result<RecordStream, StreamError> {
        let filter = Some(request.filter.transport_filter()?);
        let (from, path) = (request.from, request.path.clone());
        Ok(super::runtime::boxed(
            data::Data::new(
                self.open_subscription(follow_kind(request), cursor(from), true, false, filter, 0)
                    .await?,
            )
            .map(move |item| item?.into_record(&path)),
        ))
    }
    async fn follow_updates(
        &self,
        request: crate::FollowRequest,
        acknowledge: bool,
    ) -> Result<
        impl futures::Stream<Item = Result<crate::FollowUpdate, StreamError>> + 'static + use<>,
        StreamError,
    > {
        let filter = Some(request.filter.transport_filter()?);
        let (from, progress, path) = (request.from, request.progress, request.path.clone());
        let source = self
            .open_subscription(
                follow_kind(request),
                cursor(from),
                acknowledge,
                progress,
                filter,
                0,
            )
            .await?;
        Ok(source.filter_map(move |item| {
            future::ready(match item {
                Ok(frame) => match frame.wire().body.as_ref() {
                    Some(Body::Data(_)) => {
                        Some(frame.into_record(&path).map(crate::FollowUpdate::Record))
                    }
                    Some(Body::Watermark(mark)) if progress => mark.after.as_ref().map(|after| {
                        wire_codec::commit_id(&mark.preceding_commit_id).map(|preceding| {
                            crate::FollowUpdate::Progress {
                                next: after.next,
                                preceding,
                            }
                        })
                    }),
                    Some(Body::Watermark(_)) => None,
                    _ => Some(Err(StreamError::Unavailable)),
                },
                Err(error) => Some(Err(error)),
            })
        }))
    }
    /// Follows ALL records or LATEST values on the shared per-node session,
    /// retaining the exact data cursor and accepted/durable watermarks.
    pub async fn follow_data(
        &self,
        follow: wire::SessionFollow,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<BoxProviderStream<'static, Result<wire::SessionData, StreamError>>, StreamError>
    {
        Ok(super::runtime::boxed(
            self.follow_data_with_ack(follow, from, true, filter)
                .await?,
        ))
    }
    pub(crate) async fn follow_data_with_ack(
        &self,
        follow: wire::SessionFollow,
        from: u64,
        acknowledge: bool,
        filter: crate::FollowFilter,
    ) -> Result<
        impl futures::Stream<Item = Result<wire::SessionData, StreamError>> + 'static + use<>,
        StreamError,
    > {
        wire_codec::session::validate_follow(
            &follow,
            &wire::SessionCursor {
                next: from,
                opaque: Bytes::new(),
            },
        )?;
        wire_codec::cut_from_wire(follow.at_least.clone())?;
        Ok(data::Data::new(
            self.open_kind(
                Kind::Follow(follow),
                from,
                acknowledge,
                Some(filter.transport_filter()?),
            )
            .await?,
        )
        .map(|item| item?.into_data()))
    }

    async fn open_kind(
        &self,
        kind: Kind,
        from: u64,
        acknowledge: bool,
        filter: Option<wire::SessionFilter>,
    ) -> Result<
        impl futures::Stream<Item = Result<Frame, StreamError>> + 'static + use<>,
        StreamError,
    > {
        self.open_subscription(
            kind,
            wire::SessionCursor {
                next: from,
                opaque: Bytes::new(),
            },
            acknowledge,
            false,
            filter,
            0,
        )
        .await
    }

    /// Follows canonical ALL/LATEST data and standalone watermarks on the shared pool.
    /// Credit is returned only as frames are consumed or the subscription is dropped.
    pub async fn subscribe(
        &self,
        subscription: wire::SessionSubscription,
    ) -> Result<BoxProviderStream<'static, Result<Frame, StreamError>>, StreamError> {
        self.subscribe_with_ack(subscription, true).await
    }

    pub(crate) async fn subscribe_with_ack(
        &self,
        subscription: wire::SessionSubscription,
        acknowledge: bool,
    ) -> Result<BoxProviderStream<'static, Result<Frame, StreamError>>, StreamError> {
        let (kind, resume, filter, credential) = subscription_parts(subscription)?;
        Ok(super::runtime::boxed(
            self.open_subscription(kind, resume, acknowledge, true, filter, credential)
                .await?,
        ))
    }

    async fn open_subscription(
        &self,
        kind: Kind,
        resume: wire::SessionCursor,
        acknowledge: bool,
        raw: bool,
        filter: Option<wire::SessionFilter>,
        credential: u32,
    ) -> Result<
        impl futures::Stream<Item = Result<Frame, StreamError>> + 'static + use<>,
        StreamError,
    > {
        let cursor = self
            .open_cursor(kind, resume, acknowledge, raw, filter, credential)
            .await?;
        let operation = cursor.observation.operation.clone();
        Ok(
            operation.scope(stream::try_unfold(cursor, move |mut cursor| async move {
                loop {
                    let Some(mut delivery) = cursor.receive().await? else {
                        return Ok(None);
                    };
                    if let Some(before) = delivery.consume_before(&cursor.progress)? {
                        cursor.pending = Some(delivery);
                        return Ok(Some((before, cursor)));
                    }
                    // Delivery disposal returns capacity on every exit, including
                    // stale-generation suppression and cancellation of the cursor.
                    let fresh = delivery.consume(&cursor.progress)?;
                    drop(delivery.credit);
                    if fresh {
                        return Ok(Some((delivery.item, cursor)));
                    }
                }
            })),
        )
    }
    async fn open_cursor(
        &self,
        kind: Kind,
        resume: wire::SessionCursor,
        acknowledge: bool,
        raw: bool,
        filter: Option<wire::SessionFilter>,
        credential: u32,
    ) -> Result<Cursor, StreamError> {
        // One idle control slot; skipped cuts preceding data share that envelope.
        // The delivery window remains QUEUE even while a prefix is consumed.
        let (records, receiver) = mpsc::channel(QUEUE + 1);
        let (ready, opened) = oneshot::channel();
        let preceding = match &kind {
            Kind::Follow(follow) => {
                wire_codec::session::preceding_follow(follow, &resume)?.map(|id| *id.as_bytes())
            }
            _ => None,
        };
        let progress = Progress::from_cursor(&resume, preceding);
        let progress = Arc::new(
            if matches!(&kind, Kind::Follow(follow) if follow.key.is_some()) {
                progress.with_sparse_records()
            } else {
                progress
            },
        );
        let id = progress::allocate(&self.pool.next_id)?;
        // Install the drop guard before either await: cancellation during open
        // must remove its interest even when no record is ever appended.
        let mut cursor = self.cursor(id, Arc::clone(&progress), receiver)?;
        let operation = cursor.observation.operation.clone();
        let registration = async {
            self.pool
                .commands
                .send(Command::Open(Box::new(OpenCommand {
                    revisions: Arc::clone(&self.revisions),
                    id,
                    attempt_epoch: self.pool.attempt_epoch.load(Ordering::Acquire),
                    acknowledge,
                    kind,
                    filter,
                    credential,
                    raw,
                    progress,
                    records,
                    ready,
                    observation: operation.clone(),
                })))
                .await
                .map_err(|_| StreamError::Unavailable)?;
            let opened_id = opened.await.map_err(|_| StreamError::Unavailable)??;
            if opened_id != id {
                return Err(StreamError::Unavailable);
            }
            Ok(())
        };
        let registered = operation.clone().scope(registration).await;
        if let Err(error) = registered {
            cursor.observation.finish(Some(&error));
            return Err(error);
        }
        operation.record("registration", "opened");
        Ok(cursor)
    }
    fn cursor(
        &self,
        id: u64,
        progress: Arc<Progress>,
        receiver: mpsc::Receiver<Result<Delivery, StreamError>>,
    ) -> Result<Cursor, StreamError> {
        // Reserve before queueing Open: dispatch may observe a removal before
        // a concurrent Open, but must retain their shared initialization.
        self.pool
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map_err(|_| StreamError::Capacity)?;
        if let Err(error) = self.pool.admissions.insert(id) {
            self.pool.active.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(Cursor {
            observation: Observation::channel(id),
            id,
            client: self.clone(),
            progress,
            receiver,
            pending: None,
        })
    }
}
struct Cursor {
    pending: Option<Delivery>,
    observation: Observation,
    id: u64,
    client: Client,
    progress: Arc<Progress>,
    receiver: mpsc::Receiver<Result<Delivery, StreamError>>,
}
impl Cursor {
    async fn receive(&mut self) -> Result<Option<Delivery>, StreamError> {
        loop {
            if self.progress.cancelled() {
                return Ok(None);
            }
            let received = match self.pending.take() {
                Some(delivery) => Ok(delivery),
                None => match self.receiver.recv().await {
                    Some(received) => received,
                    None if self.progress.ended() => {
                        self.observation.finish(None);
                        return Ok(None);
                    }
                    None => Err(StreamError::Unavailable),
                },
            };
            let delivery = match received {
                Ok(delivery) => delivery,
                Err(error) => {
                    self.observation.finish(Some(&error));
                    return Err(error);
                }
            };
            if self.progress.current(delivery.generation)? {
                return Ok(Some(delivery));
            }
        }
    }
}
impl Drop for Cursor {
    fn drop(&mut self) {
        self.client.pool.admissions.cancel(self.id, &self.progress);
        self.receiver.close();
        self.client.pool.active.fetch_sub(1, Ordering::AcqRel);
        let _ = self.client.pool.removed.send(self.id);
    }
}

/// One follow's subscription kind. It carries only the caller's explicit
/// minimum; the pool adds the tracked floor on every physical open, so a
/// resume never lowers it.
fn follow_kind(request: crate::FollowRequest) -> Kind {
    Kind::Follow(wire::SessionFollow {
        key: None,
        read_limit: 0,
        bootstrap_head: false,
        when_absent: request.when_absent.to_wire(),
        resource: crate::wire_codec::session::stream_resource(request.path.as_str()),
        delivery: wire::SessionDelivery::All as i32,
        preceding_commit_id: request
            .preceding
            .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(id.as_bytes())),
        until: 0,
        at_least: crate::wire_codec::cut_to_wire(&request.at_least),
    })
}

fn subscription_parts(
    subscription: wire::SessionSubscription,
) -> Result<(Kind, wire::SessionCursor, Option<wire::SessionFilter>, u32), StreamError> {
    if subscription.channel.is_some() {
        return Err(StreamError::InvalidArgument);
    }
    let resume = subscription.resume.ok_or(StreamError::InvalidArgument)?;
    let mut kind = subscription.kind.ok_or(StreamError::InvalidArgument)?;
    match &mut kind {
        Kind::Follow(follow) => {
            wire_codec::session::validate_follow(follow, &resume)?;
            wire_codec::cut_from_wire(follow.at_least.clone())?;
        }
        _ => return Err(StreamError::Unsupported),
    }
    Ok((kind, resume, subscription.filter, subscription.credential))
}
