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

mod cache;
mod credit;
mod deadline;
mod observation;
pub(crate) use cache::Cache;
use observation::Observation;
mod outbound;
use credit::{Credit, Released};
use outbound::Outbound;
type ReleaseSender = Arc<signal::Signal<Released>>;
mod link;
mod pool;
#[cfg(acyclic_stream_loom)]
pub use cache::verify_cache;
#[cfg(acyclic_stream_loom)]
pub use pool::model::{
    verify_abandoned_reconnect, verify_interest_accounting, verify_link_isolation,
    verify_pool_lifetime, verify_registration_deadline,
};
mod progress;
use progress::Progress;
#[cfg(test)]
mod tests;
type Connect = std::sync::Arc<
    dyn Fn(wire::SessionNode) -> BoxFuture<'static, Result<TransportChannel, StreamError>>
        + Send
        + Sync,
>;
type Ready = (String, Link, Response);
type Dial = BoxFuture<'static, (String, u64, Result<Ready, StreamError>)>;
type Response = tonic::Streaming<wire::SessionFrame>;
type Read = BoxFuture<
    'static,
    (
        String,
        u64,
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
    active: Arc<AtomicU64>,
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
enum Received {
    Record(crate::Record),
    Child(crate::Child),
}
enum Command {
    Open {
        id: u64,
        attempt_epoch: u64,
        acknowledge: bool,
        kind: Kind,
        progress: Arc<Progress>,
        records: mpsc::Sender<Result<Delivery, StreamError>>,
        ready: oneshot::Sender<Result<u64, StreamError>>,
        observation: crate::obs::OperationSpan,
    },
}
struct Route {
    node: String,
    attempt_epoch: u64,
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
    observation: crate::obs::OperationSpan,
}
struct Link {
    observation: Option<Observation>,
    epoch: u64,
    credit: wire::SessionWindow,
    channel: TransportChannel,
    sender: Arc<Outbound<wire::SessionFrame>>,
    revision: u64,
    applied_revision: u64,
    released: wire::SessionWindow,
}
struct Pool {
    identity: Bytes,
    authorization: Option<MetadataValue<Ascii>>,
    credential: Option<grpc::CredentialProvider>,
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
    active: Arc<AtomicU64>,
    attempt_epoch: Arc<AtomicU64>,
    abandoned: Arc<gate::Gate>,
    dial_epoch: u64,
    commands: mpsc::Receiver<Command>,
    removed: Arc<signal::Signal<u64>>,
    released: ReleaseSender,
    release_sender: ReleaseSender,
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
    pub(crate) fn open(
        channel: TransportChannel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<grpc::CredentialProvider>,
        ca: Option<std::sync::Arc<[u8]>>,
        verifier: Option<std::sync::Arc<dyn rustls::client::danger::ServerCertVerifier>>,
    ) -> Self {
        let connect: Connect = std::sync::Arc::new(move |node| {
            let ca = ca.clone();
            let verifier = verifier.clone();
            Box::pin(async move {
                match verifier {
                    Some(verifier) => grpc::channel_with_verifier(&node.endpoint, verifier).await,
                    None => grpc::channel_tls(&node.endpoint, ca.as_deref()).await,
                }
                .map_err(|_| StreamError::Unavailable)
            })
        });
        Self::with_connector(channel, authorization, credential, connect)
    }
    fn with_connector(
        channel: TransportChannel,
        authorization: Option<MetadataValue<Ascii>>,
        credential: Option<grpc::CredentialProvider>,
        connect: Connect,
    ) -> Self {
        let (commands, receiver) = mpsc::channel(crate::MAX_ITEMS);
        // Drop produces at most one removal per admitted channel; it must never block.
        let removed = Arc::new(signal::Signal::new());
        let release_sender = Arc::new(signal::Signal::new());
        let active = Arc::new(AtomicU64::new(0));
        let attempt_epoch = Arc::new(AtomicU64::new(0));
        let abandoned = Arc::new(gate::Gate::new());
        let mut pool = Pool {
            identity: Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            authorization,
            credential,
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
            active: Arc::clone(&active),
            attempt_epoch: Arc::clone(&attempt_epoch),
            abandoned: Arc::clone(&abandoned),
            dial_epoch: 0,
            commands: receiver,
            removed: Arc::clone(&removed),
            released: Arc::clone(&release_sender),
            release_sender,
        };
        tokio::spawn(async move {
            if let Err(error) = pool.run().await {
                pool.fail(&error);
            }
        });
        Self {
            commands,
            removed,
            next_id: Arc::new(AtomicU64::new(1)),
            active,
            attempt_epoch,
            abandoned,
        }
    }
    pub(crate) fn abandon_pending(&self) -> Result<(), StreamError> {
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
        let kind = Kind::Follow(wire::SessionFollow {
            path: path.to_string(),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: preceding
                .map_or_else(Bytes::new, |id| Bytes::copy_from_slice(id.as_bytes())),
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
        let preceding = match &kind {
            Kind::Follow(follow) => wire_codec::session::preceding_follow(
                follow,
                &wire::SessionCursor {
                    next: from,
                    opaque: Bytes::new(),
                },
            )?
            .map(|id| *id.as_bytes()),
            _ => None,
        };
        let progress = Arc::new(match preceding {
            Some(preceding) => Progress::with_preceding(from, Some(preceding)),
            None => Progress::new(from),
        });
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
            .scope(deadline::until(
                registration,
                tokio::time::sleep(std::time::Duration::from_secs(10)),
            ))
            .await
            .unwrap_or(Err(StreamError::DeadlineElapsed));
        if let Err(error) = registered {
            cursor.observation.finish(Some(&error));
            return Err(error);
        }
        operation.record("registration", "opened");
        Ok(operation
            .scope(stream::try_unfold(cursor, |mut cursor| async move {
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
                    let fresh = match &delivery.item {
                        Received::Record(record) => cursor.progress.consume_record(
                            delivery.generation,
                            delivery.sequence,
                            *record.commit_id.as_bytes(),
                        )?,
                        Received::Child(_) => cursor.progress.consume_opaque(
                            delivery.generation,
                            delivery.sequence,
                            &delivery.opaque,
                        )?,
                    };
                    drop(delivery.credit);
                    if fresh {
                        return Ok(Some((delivery.item, cursor)));
                    }
                }
            }))
            .boxed())
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
        self.client.active.fetch_sub(1, Ordering::AcqRel);
        let _ = self.client.removed.send(self.id);
    }
}
