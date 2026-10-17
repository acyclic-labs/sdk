use super::sync::{Arc, AtomicU64, Mutex, Ordering};
use super::*;
use crate::{BoxProviderFuture, BoxProviderStream, RecordStream};
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(all(test, not(target_arch = "wasm32")))]
use native::Connect;
#[cfg(all(any(test, acyclic_stream_loom), not(target_arch = "wasm32")))]
use tonic::transport::Channel as TransportChannel;
pub mod transport;
use transport::Endpoint;

mod admissions;
use admissions::Admissions;
#[cfg(not(target_arch = "wasm32"))]
mod cache;
mod credit;
mod deadline;
mod observation;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use cache::Cache;
use observation::Observation;
mod outbound;
use credit::{Credit, Released};
use outbound::Outbound;
type ReleaseSender = Arc<signal::Signal<Released>>;
mod link;
mod pool;
#[cfg(acyclic_stream_loom)]
pub use admissions::verify_admission_binding;
#[cfg(acyclic_stream_loom)]
pub use cache::verify_cache;
#[cfg(acyclic_stream_loom)]
pub use pool::discovery_model::verify_discovery_admission;
#[cfg(acyclic_stream_loom)]
pub use pool::model::{
    verify_abandoned_reconnect, verify_cancel_before_open, verify_follow_data,
    verify_interest_accounting, verify_link_isolation, verify_node_dials, verify_pool_lifetime,
    verify_registration_deadline, verify_terminal_cursor_dial,
};
#[cfg(acyclic_stream_loom)]
pub use pool::retry_model::verify_initial_retry_yield;
mod progress;
use progress::Progress;
#[cfg(acyclic_stream_loom)]
pub use progress::verify_latest_progress;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;
type Ready = (String, Link, Response);
type Dial = BoxProviderFuture<'static, (String, u64, Result<Ready, StreamError>)>;
type Response = transport::Responses;
type Read = BoxProviderFuture<
    'static,
    (
        String,
        u64,
        Response,
        Option<Result<wire::SessionFrame, StreamError>>,
    ),
>;
const QUEUE: usize = 2;
const FRAME_BYTES: u64 = crate::MAX_RECORD_BYTES as u64 + 128;

/// Shared pool owning one bidirectional Streams session per authenticated node.
#[derive(Clone)]
pub struct Client {
    connections: super::Connections,
    executor: std::sync::Arc<dyn super::runtime::Executor>,
    commands: mpsc::Sender<Command>,
    removed: Arc<signal::Signal<u64>>,
    next_id: Arc<AtomicU64>,
    active: Arc<AtomicU64>,
    admissions: Arc<Admissions>,
    attempt_epoch: Arc<AtomicU64>,
    abandoned: Arc<gate::Gate>,
}
struct Delivery {
    item: Received,
    sequence: u64,
    opaque: Bytes,
    generation: u64,
    credit: Credit,
}
impl Delivery {
    /// Advance `progress` past this delivery; false means a stale generation.
    fn consume(&self, progress: &Progress) -> Result<bool, StreamError> {
        match &self.item {
            Received::Data(data)
            | Received::Frame(wire::SessionFrame {
                body: Some(Body::Data(data)),
                ..
            }) => match &data.item {
                Some(Item::Record(record)) => progress.consume_record(
                    self.generation,
                    self.sequence,
                    record
                        .commit_id
                        .as_ref()
                        .try_into()
                        .map_err(|_| StreamError::Unavailable)?,
                ),
                Some(Item::LatestValue(_)) => {
                    progress.consume_latest(self.generation, self.sequence)
                }
                Some(Item::Child(_)) => {
                    progress.consume_opaque(self.generation, self.sequence, &self.opaque)
                }
                _ => Err(StreamError::Unavailable),
            },
            Received::Child(_) => {
                progress.consume_opaque(self.generation, self.sequence, &self.opaque)
            }
            Received::Frame(wire::SessionFrame {
                body: Some(Body::Watermark(_)),
                ..
            }) => progress.current(self.generation),
            _ => Err(StreamError::Unavailable),
        }
    }
}
enum Received {
    Data(wire::SessionData),
    Child(crate::Child),
    Frame(wire::SessionFrame),
}
enum Command {
    Open {
        id: u64,
        attempt_epoch: u64,
        acknowledge: bool,
        kind: Kind,
        raw: bool,
        progress: Arc<Progress>,
        records: mpsc::Sender<Result<Delivery, StreamError>>,
        ready: oneshot::Sender<Result<u64, StreamError>>,
        observation: super::runtime::OperationSpan,
    },
}
struct Route {
    node: String,
    attempt_epoch: u64,
    kind: Kind,
    raw: bool,
    watermark: Option<wire::SessionFrame>,
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
    observation: Option<Observation>,
    epoch: u64,
    credit: wire::SessionWindow,
    channel: Endpoint,
    sender: Arc<Outbound<wire::SessionFrame>>,
    revision: u64,
    applied_revision: u64,
    released: wire::SessionWindow,
}
struct Pool {
    connections: super::Connections,
    executor: std::sync::Arc<dyn super::runtime::Executor>,
    identity: Bytes,
    connect: std::sync::Arc<transport::Connect>,
    home: String,
    routes: BTreeMap<u64, Route>,
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
    /// Shares one protocol pool across platform endpoints and every logical follow.
    pub fn with_transport(
        channel: Endpoint,
        connect: std::sync::Arc<transport::Connect>,
        executor: std::sync::Arc<dyn super::runtime::Executor>,
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
            connections: connections.clone(),
            executor: executor.clone(),
            identity: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            connect,
            home: String::new(),
            routes: BTreeMap::new(),
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
        Self {
            connections,
            executor,
            commands,
            removed,
            next_id: Arc::new(AtomicU64::new(1)),
            active,
            admissions,
            attempt_epoch,
            abandoned,
        }
    }
    /// Observes successful handshakes without depending on further task records.
    #[must_use]
    pub fn connections(&self) -> super::Connections {
        self.connections.clone()
    }

    /// Cancels pending handshakes when their credential scope ends.
    pub fn abandon_pending(&self) -> Result<(), StreamError> {
        self.attempt_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                epoch.checked_add(1)
            })
            .map_err(|_| StreamError::Capacity)?;
        self.abandoned.set(true)
    }
    /// Opens a logical follow on the shared session, without a per-stream RPC.
    pub async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.follow_with_ack(path, from, true).await
    }
    pub(crate) async fn follow_with_ack(
        &self,
        path: StreamPath,
        from: u64,
        acknowledge: bool,
    ) -> Result<RecordStream, StreamError> {
        self.follow_records(path, from, None, acknowledge).await
    }
    /// Atomically confirms the preceding history on a shared logical channel.
    pub async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: crate::CommitId,
    ) -> Result<RecordStream, StreamError> {
        self.follow_records(path, from, Some(preceding), true).await
    }
    pub(crate) async fn follow_records(
        &self,
        path: StreamPath,
        from: u64,
        preceding: Option<crate::CommitId>,
        acknowledge: bool,
    ) -> Result<RecordStream, StreamError> {
        let follow = wire::SessionFollow {
            path: path.to_string(),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: preceding
                .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(id.as_bytes())),
        };
        Ok(super::runtime::boxed(
            self.follow_data_with_ack(follow, from, acknowledge)
                .await?
                .map(|data| match data?.item {
                    Some(Item::Record(record)) => wire_codec::record(record),
                    _ => Err(StreamError::Unavailable),
                }),
        ))
    }
    /// Follows ALL records or LATEST values on the shared per-node session,
    /// retaining the exact data cursor and accepted/durable watermarks.
    pub async fn follow_data(
        &self,
        follow: wire::SessionFollow,
        from: u64,
    ) -> Result<BoxProviderStream<'static, Result<wire::SessionData, StreamError>>, StreamError>
    {
        Ok(super::runtime::boxed(
            self.follow_data_with_ack(follow, from, true).await?,
        ))
    }
    pub(crate) async fn follow_data_with_ack(
        &self,
        follow: wire::SessionFollow,
        from: u64,
        acknowledge: bool,
    ) -> Result<
        impl futures::Stream<Item = Result<wire::SessionData, StreamError>> + 'static + use<>,
        StreamError,
    > {
        StreamPath::validate(&follow.path)?;
        if !matches!(
            wire::SessionDelivery::try_from(follow.delivery),
            Ok(wire::SessionDelivery::All | wire::SessionDelivery::Latest)
        ) {
            return Err(StreamError::InvalidArgument);
        }
        Ok(self
            .open_kind(Kind::Follow(follow), from, acknowledge)
            .await?
            .map(|item| match item? {
                Received::Data(data) => Ok(data),
                _ => Err(StreamError::Unavailable),
            }))
    }
    /// Discovers direct children on the same per-node session as record follows.
    pub async fn children(
        &self,
        request: crate::ChildrenRequest,
    ) -> Result<crate::ChildStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        let kind = Kind::Children(wire::ChildrenRequest {
            parent: request.parent.map(|path| path.to_string()),
            limit: request.limit,
        });
        Ok(super::runtime::boxed(
            self.open_kind(kind, 0, true).await?.filter_map(|item| {
                future::ready(match item {
                    Ok(Received::Child(child)) => Some(Ok(child)),
                    Ok(Received::Frame(_)) => None,
                    Ok(_) => Some(Err(StreamError::Unavailable)),
                    Err(error) => Some(Err(error)),
                })
            }),
        ))
    }
    async fn open_kind(
        &self,
        kind: Kind,
        from: u64,
        acknowledge: bool,
    ) -> Result<
        impl futures::Stream<Item = Result<Received, StreamError>> + 'static + use<>,
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
        )
        .await
    }

    /// Follows canonical ALL/LATEST data and standalone watermarks on the shared pool.
    /// Credit is returned only as frames are consumed or the subscription is dropped.
    pub async fn subscribe(
        &self,
        subscription: wire::SessionSubscription,
    ) -> Result<BoxProviderStream<'static, Result<wire::SessionFrame, StreamError>>, StreamError>
    {
        if subscription.channel.is_some() {
            return Err(StreamError::InvalidArgument);
        }
        let resume = subscription.resume.ok_or(StreamError::InvalidArgument)?;
        let kind = subscription.kind.ok_or(StreamError::InvalidArgument)?;
        match &kind {
            Kind::Follow(follow) => wire_codec::session::validate_follow(follow, &resume)?,
            Kind::Children(request) => {
                wire_codec::children_from_wire(request.clone())?;
            }
            _ => return Err(StreamError::Unsupported),
        }
        Ok(super::runtime::boxed(
            self.open_subscription(kind, resume, true, true)
                .await?
                .map(|item| match item? {
                    Received::Frame(frame) => Ok(frame),
                    _ => Err(StreamError::Unavailable),
                }),
        ))
    }

    async fn open_subscription(
        &self,
        kind: Kind,
        resume: wire::SessionCursor,
        acknowledge: bool,
        raw: bool,
    ) -> Result<
        impl futures::Stream<Item = Result<Received, StreamError>> + 'static + use<>,
        StreamError,
    > {
        let (records, receiver) = mpsc::channel(QUEUE + usize::from(raw));
        let (ready, opened) = oneshot::channel();
        let preceding = match &kind {
            Kind::Follow(follow) => {
                wire_codec::session::preceding_follow(follow, &resume)?.map(|id| *id.as_bytes())
            }
            _ => None,
        };
        let progress = Arc::new(Progress::from_cursor(&resume, preceding));
        let id = progress::allocate(&self.next_id)?;
        // Install the drop guard before either await: cancellation during open
        // must remove its interest even when no record is ever appended.
        let mut cursor = self.cursor(id, Arc::clone(&progress), receiver)?;
        let operation = cursor.observation.operation.clone();
        let registration = async {
            self.commands
                .send(Command::Open {
                    id,
                    attempt_epoch: self.attempt_epoch.load(Ordering::Acquire),
                    acknowledge,
                    kind,
                    raw,
                    progress,
                    records,
                    ready,
                    observation: operation.clone(),
                })
                .await
                .map_err(|_| StreamError::Unavailable)?;
            let opened_id = opened.await.map_err(|_| StreamError::Unavailable)??;
            if opened_id != id {
                return Err(StreamError::Unavailable);
            }
            Ok(())
        };
        let registered = operation
            .clone()
            .scope(deadline::until(registration, self.executor.sleep(10_000)))
            .await
            .unwrap_or(Err(StreamError::DeadlineElapsed));
        if let Err(error) = registered {
            cursor.observation.finish(Some(&error));
            return Err(error);
        }
        operation.record("registration", "opened");
        Ok(
            operation.scope(stream::try_unfold(cursor, move |mut cursor| async move {
                loop {
                    let delivery = match cursor
                        .receiver
                        .recv()
                        .await
                        .unwrap_or(Err(StreamError::Unavailable))
                    {
                        Ok(delivery) => delivery,
                        Err(error) => {
                            cursor.observation.finish(Some(&error));
                            return Err(error);
                        }
                    };
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
    fn cursor(
        &self,
        id: u64,
        progress: Arc<Progress>,
        receiver: mpsc::Receiver<Result<Delivery, StreamError>>,
    ) -> Result<Cursor, StreamError> {
        // Reserve before queueing Open: dispatch may observe a removal before
        // a concurrent Open, but must retain their shared initialization.
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .map_err(|_| StreamError::Capacity)?;
        if let Err(error) = self.admissions.insert(id, Arc::clone(&progress)) {
            self.active.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(Cursor {
            observation: Observation::channel(id),
            id,
            client: self.clone(),
            progress,
            receiver,
        })
    }
}
struct Cursor {
    observation: Observation,
    id: u64,
    client: Client,
    progress: Arc<Progress>,
    receiver: mpsc::Receiver<Result<Delivery, StreamError>>,
}
impl Drop for Cursor {
    fn drop(&mut self) {
        self.progress.cancel();
        self.receiver.close();
        let _ = self.client.admissions.remove(self.id);
        self.client.active.fetch_sub(1, Ordering::AcqRel);
        let _ = self.client.removed.send(self.id);
    }
}
