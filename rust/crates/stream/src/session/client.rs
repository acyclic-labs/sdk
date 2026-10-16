use super::sync::{Arc, AtomicU64, Mutex, Ordering};
use super::*;
use crate::{RecordStream, grpc};
use futures::{future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::mpsc;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::Channel as TransportChannel,
};

mod credit;
use credit::{Credit, Released};
type ReleaseSender = Arc<signal::Signal<Released>>;
mod link;
mod pool;
mod progress;
use progress::Progress;
#[cfg(test)]
mod tests;
type Connect = Arc<
    dyn Fn(wire::SessionNode) -> BoxFuture<'static, Result<TransportChannel, StreamError>>
        + Send
        + Sync,
>;
type Ready = (String, Link, Response);
type Dial = BoxFuture<'static, (String, Result<Ready, StreamError>)>;
type Response = tonic::Streaming<wire::SessionFrame>;
type Read = BoxFuture<
    'static,
    (
        String,
        Response,
        Option<Result<wire::SessionFrame, tonic::Status>>,
    ),
>;
const QUEUE: usize = 2;
const FRAME_BYTES: u64 = crate::MAX_RECORD_BYTES as u64 + 128;

/// Shared pool owning one bidirectional Streams session per authenticated node.
#[derive(Clone)]
pub struct Client {
    commands: mpsc::Sender<Command>,
    removed: Arc<signal::Signal<u64>>,
    next_id: Arc<AtomicU64>,
}
struct Delivery {
    item: Received,
    sequence: u64,
    opaque: Bytes,
    generation: u64,
    credit: Credit,
}
enum Received {
    Record(crate::Record),
    Child(crate::Child),
}
enum Command {
    Open {
        id: u64,
        acknowledge: bool,
        kind: Kind,
        progress: Arc<Progress>,
        records: mpsc::Sender<Result<Delivery, StreamError>>,
        ready: oneshot::Sender<Result<u64, StreamError>>,
    },
}
struct Route {
    node: String,
    kind: Kind,
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
}
struct Link {
    epoch: u64,
    credit: wire::SessionWindow,
    channel: TransportChannel,
    sender: mpsc::Sender<wire::SessionFrame>,
    revision: u64,
    applied_revision: u64,
    released: wire::SessionWindow,
}
struct Pool {
    identity: Bytes,
    authorization: MetadataValue<Ascii>,
    connect: Connect,
    home: String,
    routes: BTreeMap<u64, Route>,
    links: BTreeMap<String, Link>,
    reads: FuturesUnordered<Read>,
    dials: FuturesUnordered<Dial>,
    connecting: BTreeSet<String>,
    pending: VecDeque<(u64, u64)>,
    home_channel: TransportChannel,
    next_link: u64,
    commands: mpsc::Receiver<Command>,
    removed: Arc<signal::Signal<u64>>,
    released: ReleaseSender,
    release_sender: ReleaseSender,
}
impl Client {
    pub(crate) async fn open(
        channel: TransportChannel,
        authorization: MetadataValue<Ascii>,
        ca: Option<Arc<[u8]>>,
    ) -> Result<Self, StreamError> {
        let connect: Connect = Arc::new(move |node| {
            let ca = ca.clone();
            Box::pin(async move {
                grpc::channel_tls(&node.endpoint, ca.as_deref())
                    .await
                    .map_err(|_| StreamError::Unavailable)
            })
        });
        Self::with_connector(channel, authorization, connect).await
    }
    async fn with_connector(
        channel: TransportChannel,
        authorization: MetadataValue<Ascii>,
        connect: Connect,
    ) -> Result<Self, StreamError> {
        let (commands, receiver) = mpsc::channel(crate::MAX_ITEMS);
        // Drop produces at most one removal per admitted channel; it must never block.
        let removed = Arc::new(signal::Signal::new());
        let release_sender = Arc::new(signal::Signal::new());
        let mut pool = Pool {
            identity: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            authorization,
            connect,
            home: String::new(),
            routes: BTreeMap::new(),
            links: BTreeMap::new(),
            reads: FuturesUnordered::new(),
            dials: FuturesUnordered::new(),
            connecting: BTreeSet::new(),
            pending: VecDeque::new(),
            home_channel: channel.clone(),
            next_link: 1,
            commands: receiver,
            removed: Arc::clone(&removed),
            released: Arc::clone(&release_sender),
            release_sender,
        };
        pool.home = pool.add_link(channel, "").await?;
        tokio::spawn(async move {
            if let Err(error) = pool.run().await {
                pool.fail(&error);
            }
        });
        Ok(Self {
            commands,
            removed,
            next_id: Arc::new(AtomicU64::new(1)),
        })
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
        let kind = Kind::Follow(wire::SessionFollow {
            path: path.to_string(),
            delivery: wire::SessionDelivery::All as i32,
        });
        Ok(self
            .open_kind(kind, from, acknowledge)
            .await?
            .map(|item| match item? {
                Received::Record(record) => Ok(record),
                _ => Err(StreamError::Unavailable),
            })
            .boxed())
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
        Ok(self
            .open_kind(kind, 0, true)
            .await?
            .map(|item| match item? {
                Received::Child(child) => Ok(child),
                _ => Err(StreamError::Unavailable),
            })
            .boxed())
    }
    async fn open_kind(
        &self,
        kind: Kind,
        from: u64,
        acknowledge: bool,
    ) -> Result<BoxStream<'static, Result<Received, StreamError>>, StreamError> {
        let (records, receiver) = mpsc::channel(QUEUE);
        let (ready, opened) = oneshot::channel();
        let progress = Arc::new(Progress::new(from));
        let id = progress::allocate(&self.next_id)?;
        // Install the drop guard before either await: cancellation during open
        // must remove its interest even when no record is ever appended.
        let cursor = Cursor {
            id,
            client: self.clone(),
            progress: Arc::clone(&progress),
            receiver,
        };
        self.commands
            .send(Command::Open {
                id,
                acknowledge,
                kind,
                progress,
                records,
                ready,
            })
            .await
            .map_err(|_| StreamError::Unavailable)?;
        let opened_id = opened.await.map_err(|_| StreamError::Unavailable)??;
        if opened_id != id {
            return Err(StreamError::Unavailable);
        }
        Ok(stream::try_unfold(cursor, |mut cursor| async move {
            loop {
                let delivery = cursor
                    .receiver
                    .recv()
                    .await
                    .ok_or(StreamError::Unavailable)??;
                // Delivery disposal returns capacity on every exit, including
                // stale-generation suppression and cancellation of the cursor.
                let fresh = if delivery.opaque.is_empty() {
                    cursor
                        .progress
                        .consume(delivery.generation, delivery.sequence)?
                } else {
                    cursor.progress.consume_opaque(
                        delivery.generation,
                        delivery.sequence,
                        &delivery.opaque,
                    )?
                };
                drop(delivery.credit);
                if fresh {
                    return Ok(Some((delivery.item, cursor)));
                }
            }
        })
        .boxed())
    }
}
struct Cursor {
    id: u64,
    client: Client,
    progress: Arc<Progress>,
    receiver: mpsc::Receiver<Result<Delivery, StreamError>>,
}
impl Drop for Cursor {
    fn drop(&mut self) {
        let _ = self.client.removed.send(self.id);
    }
}
