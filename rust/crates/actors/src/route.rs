//! Location-transparent delivery composes ordinary Stream effects. Placement
//! is immutable, and an outbox append joins the sender's journal commit; neither
//! a record nor an actor reference is an authority credential.
use crate::addressing::{ActorReference, IdentityError, ResourceId};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitRequest, ReadRequest, StreamError,
    StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use prost::Message as _;

mod node;
pub use node::{NodeActor, NodeLink, NodePresence, open_link, open_request, run_link};
mod drain;
pub use drain::{drain, forward, receipt};
mod admission;
pub use admission::{Delivered, Rejection, Routed};
mod account;
pub use account::{AccountService, Admission, AdmissionPolicy, admit, relay_resource};
mod serving;
pub use serving::AccountNode;
mod registration;
pub use registration::{Publication, Registration, placement_resource, register};

const CALL_PROTOCOL: &str = "acyclic.actors.route.account.v1";

/// Opaque, nonempty node identity, encoded by the shared resource-ID primitive.
pub type NodeId = ResourceId;

/// Invalid persisted route data or a failed ordinary Stream operation.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Shared account, actor or node identity validation.
    #[error(transparent)]
    Identity(#[from] IdentityError),
    /// The provider refused or could not establish this operation's outcome.
    #[error(transparent)]
    Stream(#[from] StreamError),
    /// The existing actor store or publication boundary refused the operation.
    #[error(transparent)]
    Actor(#[from] crate::local::Error),
    /// Invalid actor socket data or checkpoint.
    #[error(transparent)]
    Guest(#[from] crate::guest::GuestError),
    /// The authenticated owner rejected the node link.
    #[error("node link rejected ({status}): {reason}")]
    LinkRejected {
        /// Original HTTP-style rejection status.
        status: u16,
        /// Original public rejection reason.
        reason: String,
    },
    /// An unsupported version, malformed body or mismatched record identity.
    #[error("invalid routing record")]
    InvalidRecord,
}

/// One account's routing subtree, selected by its authenticated host.
#[derive(Clone, Debug)]
pub struct Paths(StreamPath);

impl Paths {
    /// Recover the canonical routing subtree and node from a committed outbox
    /// path. Callers still prove ownership through their original publication.
    pub fn from_outbox(path: &StreamPath) -> Result<(Self, NodeId), Error> {
        let (parent, node) = path.as_str().rsplit_once('/').ok_or(Error::InvalidRecord)?;
        let root = parent.strip_suffix("/outbox").ok_or(Error::InvalidRecord)?;
        let node = acyclic_auth::decode_component(node).map_err(|_| Error::InvalidRecord)?;
        let node = NodeId::new(&node)?;
        let paths = Self::new(StreamPath::new(root)?);
        if paths.outbox(&node)? != *path {
            return Err(Error::InvalidRecord);
        }
        Ok((paths, node))
    }

    /// Bind routing records to the host's admitted account subtree.
    pub const fn new(root: StreamPath) -> Self {
        Self(root)
    }

    fn child(&self, kind: &str, identity: &[u8]) -> Result<StreamPath, StreamError> {
        StreamPath::new(format!(
            "{}/{kind}/{}",
            self.0,
            acyclic_auth::encode_component(identity)
        ))
    }

    /// Immutable placement of one fully qualified actor.
    pub fn placement(&self, actor: &ActorReference) -> Result<StreamPath, StreamError> {
        self.child("placement", &actor.encode())
    }

    /// The source node's single ordered durable outbox.
    pub fn outbox(&self, node: &NodeId) -> Result<StreamPath, StreamError> {
        self.child("outbox", node.as_bytes())
    }

    /// Sender-local forwarding progress, retained independently of a client.
    pub fn outbox_cursor(&self, node: &NodeId) -> Result<StreamPath, StreamError> {
        StreamPath::new(format!("{}/ack", self.outbox(node)?))
    }

    /// The destination node's single account-owned relay.
    pub fn relay(&self, node: &NodeId) -> Result<StreamPath, StreamError> {
        self.child("relay", node.as_bytes())
    }

    /// The node's durable consumed prefix. Its publication is the relay drain
    /// event used by account storage admission; it contains no message body.
    pub fn relay_cursor(&self, node: &NodeId) -> Result<StreamPath, StreamError> {
        StreamPath::new(format!("{}/ack", self.relay(node)?))
    }

    /// One node's socket-driven presence, shared by all its session rows.
    pub fn presence(&self, node: &NodeId) -> Result<StreamPath, StreamError> {
        self.child("presence", node.as_bytes())
    }

    /// The account-side node actor's ordinary socket and invocation inbox.
    pub fn node_inbox(&self, node: &NodeId) -> Result<StreamPath, StreamError> {
        self.child("node-inbox", node.as_bytes())
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct PlacementWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    actor: Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    node: Bytes,
}

/// Placement v1: a root remains on the node that created it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    /// Fully qualified actor, independent of its network location.
    pub actor: ActorReference,
    /// The node that owns all state and ordinary delivery for this actor.
    pub node: NodeId,
}

impl Placement {
    /// Join the immutable placement to the actor's creation commit. A second
    /// placement conflicts even when it names the same node; exact retries use
    /// the original creation request and its original idempotency key.
    pub fn effect(&self, paths: &Paths) -> Result<CommitEffect, Error> {
        let path = paths.placement(&self.actor)?;
        let record = PlacementWire {
            version: 1,
            actor: self.actor.encode().into(),
            node: Bytes::copy_from_slice(self.node.as_bytes()),
        }
        .encode_to_vec();
        Ok(CommitEffect::hard(
            vec![CommitCondition::Absent { path: path.clone() }],
            vec![CommitMutation::Append {
                path,
                records: vec![record.into()],
            }],
        ))
    }

    /// Resolve one immutable record with one bounded read. No polling, scan,
    /// placement cache or remote actor state is needed.
    pub async fn read<P: StreamProvider + ?Sized>(
        provider: &P,
        paths: &Paths,
        actor: &ActorReference,
    ) -> Result<Option<Self>, Error> {
        Ok(Self::record(provider, paths, actor)
            .await?
            .map(|(placement, _)| placement))
    }

    pub(super) async fn record<P: StreamProvider + ?Sized>(
        provider: &P,
        paths: &Paths,
        actor: &ActorReference,
    ) -> Result<Option<(Self, acyclic_stream::Record)>, Error> {
        let mut records = match provider
            .read(ReadRequest {
                key: None,
                path: paths.placement(actor)?,
                from: 0,
                limit: 2,
                at_least: Default::default(),
            })
            .await
        {
            Ok(records) => records,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record = records.try_next().await?.ok_or(Error::InvalidRecord)?;
        let value =
            PlacementWire::decode(record.value.as_ref()).map_err(|_| Error::InvalidRecord)?;
        let found = ActorReference::decode(&value.actor)?;
        if value.version != 1
            || record.sequence != 0
            || &found != actor
            || records.try_next().await?.is_some()
        {
            return Err(Error::InvalidRecord);
        }
        Ok(Some((
            Self {
                actor: found,
                node: NodeId::new(&value.node)?,
            },
            record,
        )))
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct EnvelopeWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    source: Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    sender: Bytes,
    #[prost(bytes = "bytes", tag = "4")]
    to: Bytes,
    #[prost(message, optional, tag = "5")]
    request: Option<acyclic_stream::wire::CommitRequest>,
    #[prost(string, tag = "6")]
    domain: String,
}

// Admission selects a node from this header before decoding the opaque inbox
// request. The same protobuf decoder skips the remaining length-delimited data.
#[derive(Clone, PartialEq, prost::Message)]
struct SourceWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    source: Bytes,
    #[prost(string, tag = "6")]
    domain: String,
}

/// The original sender, target and opaque ordinary-delivery request. Its intent
/// key and canonical semantic bytes survive every outbox and relay hop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    /// Node admitted to forward this sender's original outbox.
    pub source: NodeId,
    /// Fully qualified sender whose grant the admitting host verifies.
    pub sender: ActorReference,
    /// Actor whose owning node must run the ordinary delivery.
    pub to: ActorReference,
    /// Original conditional inbox operation, including its own intent key.
    pub request: CommitRequest,
}

impl Envelope {
    /// Canonical version-one transport record, without any credential material.
    pub fn encode(&self) -> Bytes {
        self.encode_as("")
    }

    fn encode_invocation(&self) -> Bytes {
        self.encode_as(CALL_PROTOCOL)
    }

    fn encode_as(&self, domain: &str) -> Bytes {
        self.wire(domain).encode_to_vec().into()
    }

    fn wire(&self, domain: &str) -> EnvelopeWire {
        EnvelopeWire {
            version: 1,
            source: Bytes::copy_from_slice(self.source.as_bytes()),
            sender: self.sender.encode().into(),
            to: self.to.encode().into(),
            request: Some(acyclic_stream::wire_codec::commit_to_wire(&self.request)),
            domain: domain.into(),
        }
    }

    /// Decode the exact shared CommitRequest format; this never admits a grant.
    pub fn decode(bytes: impl AsRef<[u8]>) -> Result<Self, Error> {
        Self::decode_as(bytes.as_ref(), "")
    }

    fn decode_invocation(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_as(bytes, CALL_PROTOCOL)
    }

    fn decode_as(bytes: &[u8], domain: &str) -> Result<Self, Error> {
        let value = EnvelopeWire::decode(bytes).map_err(|_| Error::InvalidRecord)?;
        Self::from_wire(value, domain)
    }

    fn from_wire(value: EnvelopeWire, domain: &str) -> Result<Self, Error> {
        if value.version != 1 || value.domain != domain {
            return Err(Error::InvalidRecord);
        }
        let sender = ActorReference::decode(&value.sender)?;
        let to = ActorReference::decode(&value.to)?;
        if sender.account() != to.account() {
            return Err(Error::InvalidRecord);
        }
        Ok(Self {
            source: NodeId::new(&value.source)?,
            sender,
            to,
            request: acyclic_stream::wire_codec::commit_from_wire(
                value.request.ok_or(Error::InvalidRecord)?,
            )?,
        })
    }

    /// Stage in the sender's journal transaction, never in a later background
    /// append. An aborted sender step therefore leaves no forwardable message.
    pub fn outbox_effect(&self, paths: &Paths) -> Result<CommitEffect, Error> {
        if self.sender.account() != self.to.account() {
            return Err(Error::InvalidRecord);
        }
        Publication::delivery_effect(self, paths)
    }

    /// The canonical source publication for a batch joined to the original
    /// journal commit. Relays retain the ordinary envelope encoding instead.
    pub fn outbox_record(&self) -> acyclic_stream::AppendRecord {
        Publication::delivery_record(self)
    }
}
