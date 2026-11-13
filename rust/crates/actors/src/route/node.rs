//! A node link is an ordinary authenticated Actors socket. Its lifecycle is
//! committed by the same handler on every host; no heartbeat or lease exists.
use super::{ActorReference, Error, NodeId, Paths};
use crate::{
    domain::{ActorLimits, CodeSha256, DomainError},
    guest::{
        AlarmChange, Checkpoint, CodecIdentity, Delivery, GuestError, GuestEventResult,
        GuestFuture, GuestSocketEvent, Handler, Manifest, Transition,
    },
    local::{Creation, Store, Subscription},
    sockets::{SocketFrame, SocketOpen, socket_reply},
};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitEffect, CommitMutation, CommitRequest, IdempotencyKey,
    StreamError, StreamPath, StreamProvider, StreamProviderExt as _,
    session::{ActorSocket, Client},
};
use bytes::Bytes;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;

const INBOX: &str = "node-link";
const PROTOCOL: &str = "acyclic.actors.route.v1";
const NAMESPACE: &str = "acyclic.route.node.";

/// One node's existing Actors socket on its authenticated session.
/// Dropping it produces the ordinary peer Close, including on transport loss.
pub type NodeLink = ActorSocket;

/// Own both durable pumps for one already accepted node link. Its terminal
/// frame or transport loss drops both follows and any in-flight callback;
/// reopening resumes from the original committed acknowledgement cursors.
/// The host supplies its authenticated account provider and ordinary delivery.
/// No reconnect, readiness probe or independent task is created here.
pub async fn run_link<P, A, R, F, D>(
    mut link: NodeLink,
    local: &P,
    account: &A,
    paths: &Paths,
    node: &NodeId,
    publish: R,
    admit: F,
    deliver: D,
) -> Result<std::convert::Infallible, Error>
where
    P: StreamProvider + ?Sized,
    A: StreamProvider + ?Sized,
    R: AsyncFnMut(super::Publication) -> Result<(), Error>,
    F: AsyncFnMut(super::Envelope) -> Result<super::Routed, Error>,
    D: AsyncFnMut(super::RelayRecord) -> Result<(), Error>,
{
    let outgoing = super::forward(local, paths, node, publish, admit);
    let incoming = super::drain(account, paths, node, deliver);
    let closed = async {
        match link.next().await? {
            Some(frame) if !matches!(SocketFrame::decode(&frame)?, SocketFrame::Close { .. }) => {
                Err(Error::InvalidRecord)
            }
            _ => Err(StreamError::Unavailable.into()),
        }
    };
    futures::pin_mut!(outgoing, incoming, closed);
    let pumps = futures::future::select(outgoing, incoming);
    futures::pin_mut!(pumps);
    match futures::future::select(closed, pumps).await {
        futures::future::Either::Left((result, _)) => result,
        futures::future::Either::Right((result, _)) => match result {
            futures::future::Either::Left((result, _))
            | futures::future::Either::Right((result, _)) => result,
        },
    }
}

fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "acyclic.actors.route.node".into(),
        version: "1".into(),
    }
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

/// Build the standard node socket request. The namespace is node-specific:
/// existing namespace-scoped socket credentials cannot open another node's
/// actor. This request contains identity only and grants no authority.
pub fn open_request(account: &[u8], node: &NodeId) -> Result<SocketOpen, Error> {
    let namespace = format!(
        "{NAMESPACE}{}",
        acyclic_auth::encode_component(node.as_bytes())
    );
    Ok(SocketOpen {
        actor: ActorReference::new(account, namespace.as_bytes(), b"link")?,
        subscription: INBOX.into(),
        app_channel: None,
        subprotocols: vec![PROTOCOL.into()],
        headers: Vec::new(),
        resume_after: 0,
        reconnected: false,
    })
}

/// Open through the existing authenticated session and await its committed
/// Open verdict. Credentials remain in the session, never in link records.
pub async fn open_link(
    client: &Client,
    credential: u32,
    account: &[u8],
    node: &NodeId,
) -> Result<NodeLink, Error> {
    let mut socket = client
        .actor_socket(open_request(account, node)?.encode(), credential)
        .await?;
    let frame = socket.next().await?.ok_or(StreamError::Unavailable)?;
    match SocketFrame::decode(&frame)? {
        SocketFrame::Accepted { subprotocol } if subprotocol.as_deref() == Some(PROTOCOL) => {
            Ok(socket)
        }
        SocketFrame::Rejected { status, reason } => Err(Error::LinkRejected { status, reason }),
        _ => Err(Error::InvalidRecord),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct PresenceWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    node: Bytes,
    #[prost(bool, tag = "3")]
    online: bool,
    #[prost(uint64, tag = "4")]
    generation: u64,
}

/// Node presence v1. A single head serves every visible row on that node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodePresence {
    /// Authenticated node identity, independent of individual sessions.
    pub node: NodeId,
    /// Whether the node's latest accepted socket remains open.
    pub online: bool,
    /// Monotonic accepted-Open generation; stale Close events cannot retire it.
    pub generation: u64,
}

impl NodePresence {
    /// Decode a committed presence event, without admitting any credential.
    pub fn decode(bytes: impl AsRef<[u8]>) -> Result<Self, Error> {
        let wire = PresenceWire::decode(bytes.as_ref()).map_err(|_| Error::InvalidRecord)?;
        if wire.version != 1 || (wire.online && wire.generation == 0) {
            return Err(Error::InvalidRecord);
        }
        Ok(Self {
            node: NodeId::new(&wire.node)?,
            online: wire.online,
            generation: wire.generation,
        })
    }

    /// Decode a presence record selected at its owning node's original path.
    /// This proves provenance, never caller authority to observe that path.
    pub fn from_record(
        record: &acyclic_stream::Record,
        path: &StreamPath,
        node: &NodeId,
    ) -> Result<Self, Error> {
        let presence = Self::decode(&record.value)?;
        if record.origin != *path || &presence.node != node {
            return Err(Error::InvalidRecord);
        }
        Ok(presence)
    }

    /// One bounded head observation, with no per-session query or timer.
    pub async fn read<P: StreamProvider + ?Sized>(
        provider: &P,
        paths: &Paths,
        node: &NodeId,
    ) -> Result<Option<Self>, Error> {
        let path = paths.presence(node)?;
        let head = match provider.head(path.clone()).await {
            Ok(head) => head,
            Err(error) if error.category() == StreamError::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record = head.last.ok_or(Error::InvalidRecord)?;
        Ok(Some(Self::from_record(&record, &path, node)?))
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct State {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    root: String,
    #[prost(bytes = "bytes", tag = "3")]
    node: Bytes,
    #[prost(string, optional, tag = "4")]
    connection: Option<String>,
    #[prost(uint64, tag = "5")]
    generation: u64,
}

impl State {
    fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            codec: codec(),
            bytes: self.encode_to_vec().into(),
        }
    }

    fn presence(&self) -> Result<CommitEffect, Error> {
        let node = NodeId::new(&self.node)?;
        Ok(CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: Paths::new(StreamPath::new(self.root.clone())?).presence(&node)?,
                records: vec![AppendRecord::from(
                    PresenceWire {
                        version: 1,
                        node: self.node.clone(),
                        online: self.connection.is_some(),
                        generation: self.generation,
                    }
                    .encode_to_vec(),
                )],
            }],
        ))
    }
}

/// Account-side handler for ordinary node sockets. Presence and the consumed
/// Open/Close share its checkpoint commit. It never hosts a session root.
pub struct NodeActor {
    manifest: Manifest,
}

impl NodeActor {
    /// Use the deployment's explicit actor resource policy, without SDK limits.
    pub fn new(limits: ActorLimits) -> Result<Self, DomainError> {
        Ok(Self {
            manifest: Manifest {
                code: CodeSha256::new(Sha256::digest(include_bytes!("node.rs")))?,
                codec: codec(),
                subscriptions: vec![INBOX.into()],
                interrupts: Vec::new(),
                bindings: Vec::new(),
                limits,
            },
        })
    }

    /// Atomically enroll the node actor, its inbox and initial offline row.
    /// Retry the original creation key; replay cannot reset a live socket.
    pub fn creation<P: StreamProvider + ?Sized>(
        &self,
        store: &Store<P>,
        paths: &Paths,
        account: &[u8],
        node: &NodeId,
        key: IdempotencyKey,
    ) -> Result<CommitRequest, Error> {
        let actor = open_request(account, node)?.actor;
        let inbox = paths.node_inbox(node)?;
        let state = State {
            version: 1,
            root: paths.0.as_str().into(),
            node: Bytes::copy_from_slice(node.as_bytes()),
            connection: None,
            generation: 0,
        };
        Ok(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: key,
            effects: vec![
                store.create(
                    None,
                    &actor,
                    Creation {
                        subscriptions: BTreeMap::from([(
                            INBOX.into(),
                            Subscription {
                                path: inbox.clone(),
                                cursor: 1,
                            },
                        )]),
                        checkpoint: Some(state.checkpoint()),
                        escrow: None,
                    },
                )?,
                CommitEffect::hard(
                    vec![CommitCondition::Absent {
                        path: inbox.clone(),
                    }],
                    vec![CommitMutation::Append {
                        path: inbox,
                        records: vec![Bytes::new().into()],
                    }],
                ),
                state.presence()?,
                store.activation(&actor)?,
            ],
        })
    }
}

impl Handler for NodeActor {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn accepts_actor(&self, actor: &ActorReference) -> bool {
        actor.id() == "link"
            && actor
                .namespace()
                .as_str()
                .strip_prefix(NAMESPACE)
                .is_some_and(|node| !node.is_empty())
    }

    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _cancellation: crate::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let checkpoint = checkpoint.ok_or_else(|| guest("node has no checkpoint"))?;
            if checkpoint.codec != codec() {
                return Err(guest("unsupported node codec"));
            }
            let mut state = State::decode(checkpoint.bytes.as_ref()).map_err(guest)?;
            let node = NodeId::new(&state.node).map_err(guest)?;
            if state.version != 1
                || open_request(delivery.actor.account().as_bytes(), &node)
                    .map_err(guest)?
                    .actor
                    != delivery.actor
                || state.connection.as_ref().is_some_and(String::is_empty)
            {
                return Err(guest("invalid node binding"));
            }
            let socket = delivery
                .socket()?
                .ok_or_else(|| guest("expected node socket event"))?;
            let mut effects = Vec::new();
            let mut response = None;
            match socket.event {
                GuestSocketEvent::Open { subprotocols, .. } => {
                    let verdict = if subprotocols.iter().any(|value| value == PROTOCOL) {
                        state.generation = state
                            .generation
                            .checked_add(1)
                            .ok_or_else(|| guest("node generation overflow"))?;
                        state.connection = Some(socket.connection_id);
                        effects.push(state.presence().map_err(guest)?);
                        GuestEventResult::SocketAccept {
                            subprotocol: Some(PROTOCOL.into()),
                            attachment: Bytes::new(),
                            tags: Vec::new(),
                        }
                    } else {
                        GuestEventResult::SocketReject {
                            status: 426,
                            reason: "node link protocol required".into(),
                        }
                    };
                    response = Some(socket_reply(&verdict)?);
                }
                GuestSocketEvent::Close { .. }
                    if state.connection.as_deref() == Some(socket.connection_id.as_str()) =>
                {
                    state.connection = None;
                    effects.push(state.presence().map_err(guest)?);
                }
                _ => {}
            }
            Ok(Transition {
                checkpoint: state.checkpoint(),
                response,
                effects,
                alarm: AlarmChange::Keep,
                pause: false,
                participant: None,
                subscriptions: Vec::new(),
            })
        })
    }
}
