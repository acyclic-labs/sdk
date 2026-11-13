//! Root placement and message publication share one ordered node outbox.
use super::{ActorReference, Envelope, Error, NodeId, Paths, Placement};
use crate::addressing::AccountId;
use acyclic_auth::authority::PolicyDecision;
use acyclic_stream::{
    Attribute, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    RecordHeaders, StreamError, StreamProvider, StreamProviderExt as _,
    session::{CallAuthorization, Client},
};
use bytes::Bytes;
use prost::Message as _;
use sha2::{Digest as _, Sha256};

pub(super) const PROTOCOL: &str = "acyclic.actors.route.registration.v1";
const ORIGIN: &str = "acyclic.actors.route.placement-intent.v1";

/// Node-scoped placement publication; peer append authority never grants it.
pub fn placement_resource(account: &AccountId, node: &NodeId) -> String {
    format!(
        "actors:/placement/{}/{}",
        acyclic_auth::encode_component(account.as_bytes()),
        acyclic_auth::encode_component(node.as_bytes())
    )
}

#[derive(Clone, PartialEq, prost::Message)]
struct RegistrationWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    node: Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    actor: Bytes,
    #[prost(bytes = "bytes", tag = "4")]
    intent: Bytes,
    #[prost(string, tag = "6")]
    domain: String,
}

/// Placement published by the owning node under its original root-creation
/// identity. This is data, not a node credential or a peer grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Registration {
    /// Immutable root location.
    pub placement: Placement,
    /// Original root creation key, retained across disconnect and restart.
    pub intent: IdempotencyKey,
}

impl Registration {
    fn encode(&self, domain: &str) -> Bytes {
        self.wire(domain).encode_to_vec().into()
    }
    fn wire(&self, domain: &str) -> RegistrationWire {
        RegistrationWire {
            version: 1,
            node: Bytes::copy_from_slice(self.placement.node.as_bytes()),
            actor: self.placement.actor.encode().into(),
            intent: Bytes::copy_from_slice(self.intent.as_bytes()),
            domain: domain.into(),
        }
    }

    fn decode(bytes: &[u8], domain: &str) -> Result<Self, Error> {
        let wire = RegistrationWire::decode(bytes).map_err(|_| Error::InvalidRecord)?;
        Self::from_wire(wire, domain)
    }
    fn from_wire(wire: RegistrationWire, domain: &str) -> Result<Self, Error> {
        if wire.version != 1 || wire.domain != domain {
            return Err(Error::InvalidRecord);
        }
        Ok(Self {
            placement: Placement {
                actor: ActorReference::decode(&wire.actor)?,
                node: NodeId::new(&wire.node)?,
            },
            intent: IdempotencyKey::new(wire.intent.to_vec())?,
        })
    }

    fn key(&self, paths: &Paths) -> Result<IdempotencyKey, Error> {
        let mut hash = Sha256::new();
        hash.update(b"acyclic.actors.route.placement-publication.v1\0");
        for part in [
            paths.0.as_str().as_bytes(),
            self.placement.actor.encode().as_slice(),
            self.intent.as_bytes(),
        ] {
            hash.update((part.len() as u64).to_le_bytes());
            hash.update(part);
        }
        Ok(IdempotencyKey::new(hash.finalize().to_vec())?)
    }

    fn placement_effect(&self, paths: &Paths) -> Result<CommitEffect, Error> {
        let mut effect = self.placement.effect(paths)?;
        let [CommitMutation::Append { records, .. }] = effect.mutations.as_mut_slice() else {
            return Err(Error::InvalidRecord);
        };
        records[0].headers = RecordHeaders::new(
            Vec::new(),
            vec![Attribute {
                name: ORIGIN.into(),
                value: acyclic_stream::OwnedBytes::copy_from_slice(self.key(paths)?.as_bytes()),
            }],
        )?;
        Ok(effect)
    }

    /// Join both effects to the root's adoption transaction. The outbox order
    /// makes account registration precede every later message from this root.
    pub fn effects(&self, paths: &Paths) -> Result<[CommitEffect; 2], Error> {
        Ok([
            self.placement_effect(paths)?,
            Publication::effect(paths, &self.placement.node, Body::Placement(self.wire("")))?,
        ])
    }

    pub(super) async fn apply<P: StreamProvider + ?Sized>(
        bytes: &[u8],
        account: &AccountId,
        provider: &P,
        paths: &Paths,
        authorization: &CallAuthorization,
    ) -> Result<Bytes, Error> {
        let registration = Self::decode(bytes, PROTOCOL)?;
        if registration.placement.actor.account() != account {
            return Err(StreamError::AccessDenied.into());
        }
        let resource = placement_resource(account, &registration.placement.node);
        let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
            .map_err(|_| StreamError::InvalidArgument)?;
        let authorize = || {
            authorization.authorize_in_account(
                account.as_bytes(),
                &resource,
                &[],
                &attributes,
                PolicyDecision::Allow,
            )
        };
        authorize()?;
        let key = registration.key(paths)?;
        if let Some((placement, record)) =
            Placement::record(provider, paths, &registration.placement.actor).await?
        {
            let matches_origin = record.headers.attributes().iter().any(|attribute| {
                attribute.name == ORIGIN && attribute.value.as_ref() == key.as_bytes()
            });
            if !matches_origin || placement != registration.placement {
                return Err(StreamError::AccessDenied.into());
            }
        } else {
            authorize()?;
            let outcome = provider
                .commit(CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: key.clone(),
                    effects: vec![registration.placement_effect(paths)?],
                })
                .await?;
            if !matches!(outcome, CommitOutcome::Committed(_)) {
                return Err(crate::local::Error::Conflict.into());
            }
        }
        authorize()?;
        Ok(Bytes::copy_from_slice(key.as_bytes()))
    }
}

/// Publish one root's original placement on the authenticated account session.
/// Exact retries recover its original placement, including when the local node
/// is also the account and creation already committed that same record.
pub async fn register(
    client: &Client,
    credential: u32,
    paths: &Paths,
    registration: &Registration,
    maximum_reply_bytes: usize,
) -> Result<(), Error> {
    let reply = client
        .actor_call(
            &registration.encode(PROTOCOL),
            credential,
            maximum_reply_bytes,
        )
        .await?;
    if reply.as_ref() != registration.key(paths)?.as_bytes() {
        return Err(StreamError::InvalidResponse.into());
    }
    Ok(())
}

#[derive(Clone, PartialEq, prost::Message)]
struct PublicationWire {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(oneof = "Body", tags = "2, 3, 4")]
    body: Option<Body>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum Body {
    #[prost(message, tag = "2")]
    Delivery(super::EnvelopeWire),
    #[prost(message, tag = "3")]
    Placement(RegistrationWire),
    #[prost(message, tag = "4")]
    Call(CallWire),
}

#[derive(Clone, PartialEq, prost::Message)]
struct CallWire {
    #[prost(bytes = "bytes", tag = "1")]
    node: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    invocation: Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    reply: Bytes,
}

/// One durable finite application call on the same node-to-account session.
/// The application's original intent and authorization stay in its own format;
/// this record contains neither a credential nor permission to invoke a product.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallPublication {
    /// Source bound to the node outbox; the application checks its inner target.
    pub node: NodeId,
    /// Original product invocation, without re-encoding during forwarding.
    pub invocation: Bytes,
    /// Exact successful reply selected by the original publication.
    pub reply: Bytes,
}

impl CallPublication {
    fn wire(&self) -> CallWire {
        CallWire {
            node: Bytes::copy_from_slice(self.node.as_bytes()),
            invocation: self.invocation.clone(),
            reply: self.reply.clone(),
        }
    }

    /// Join this original call to the same source commit as its metadata.
    pub fn effect(&self, paths: &Paths) -> Result<CommitEffect, Error> {
        Publication::effect(paths, &self.node, Body::Call(self.wire()))
    }
}

/// One versioned node outbox record. Metadata and messages use the same follow
/// and acknowledgment position; only messages have a `Routed` receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Publication {
    /// The original opaque inbox request.
    Delivery(Envelope),
    /// The original immutable root registration.
    Placement(Registration),
    /// Original application metadata call, acknowledged without a send receipt.
    Call(CallPublication),
}
impl Publication {
    /// Decode one bounded original outbox record.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let wire = PublicationWire::decode(bytes).map_err(|_| Error::InvalidRecord)?;
        if wire.version != 1 {
            return Err(Error::InvalidRecord);
        }
        match wire.body.ok_or(Error::InvalidRecord)? {
            Body::Delivery(wire) => Ok(Self::Delivery(Envelope::from_wire(wire, "")?)),
            Body::Placement(wire) => Ok(Self::Placement(Registration::from_wire(wire, "")?)),
            Body::Call(wire) => Ok(Self::Call(CallPublication {
                node: NodeId::new(&wire.node)?,
                invocation: wire.invocation,
                reply: wire.reply,
            })),
        }
    }
    /// Original message; placement records cannot be used as send receipts.
    pub fn delivery(self) -> Result<Envelope, Error> {
        match self {
            Self::Delivery(envelope) => Ok(envelope),
            Self::Placement(_) | Self::Call(_) => Err(Error::InvalidRecord),
        }
    }

    /// Publish metadata on the original authenticated account session. The
    /// caller selects its existing node credential; no authority is persisted.
    /// Message admission uses `admit` and cannot be completed through this API.
    pub async fn publish(
        &self,
        client: &Client,
        credential: u32,
        paths: &Paths,
        maximum_reply_bytes: usize,
    ) -> Result<(), Error> {
        match self {
            Self::Placement(registration) => {
                register(client, credential, paths, registration, maximum_reply_bytes).await
            }
            Self::Call(call) => {
                let reply = client
                    .actor_call(&call.invocation, credential, maximum_reply_bytes)
                    .await?;
                if reply != call.reply {
                    return Err(StreamError::InvalidResponse.into());
                }
                Ok(())
            }
            Self::Delivery(_) => Err(Error::InvalidRecord),
        }
    }
    pub(super) fn delivery_effect(
        envelope: &Envelope,
        paths: &Paths,
    ) -> Result<CommitEffect, Error> {
        Self::effect(paths, &envelope.source, Body::Delivery(envelope.wire("")))
    }
    pub(super) fn delivery_record(envelope: &Envelope) -> acyclic_stream::AppendRecord {
        Self::record(Body::Delivery(envelope.wire("")))
    }
    fn record(body: Body) -> acyclic_stream::AppendRecord {
        PublicationWire {
            version: 1,
            body: Some(body),
        }
        .encode_to_vec()
        .into()
    }
    fn effect(paths: &Paths, node: &NodeId, body: Body) -> Result<CommitEffect, Error> {
        Ok(CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: paths.outbox(node)?,
                records: vec![Self::record(body)],
            }],
        ))
    }
}
