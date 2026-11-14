//! Account-side row admission on the existing authenticated Actors call channel.
use super::{Error, SessionRow};
use acyclic_actors::{
    addressing::{AccountId, ActorReference},
    route::{self, Paths, Placement},
};
use acyclic_auth::authority::PolicyDecision;
use acyclic_stream::{
    Attribute, BoxProviderFuture, CommitCondition, CommitEffect, CommitMutation, CommitOutcome,
    CommitRequest, IdempotencyKey, OwnedBytes, RecordHeaders, StreamError, StreamPath,
    StreamProvider, StreamProviderExt as _,
    session::{CallAuthorization, CallInput, CallProvider, CallTarget, Client},
};
use bytes::Bytes;
use prost::Message as _;
use std::sync::Arc;

const PROTOCOL: &str = "acyclic.harness.session-presence.v1";
const ORIGIN: &str = "acyclic.harness.session-presence.intent.v1";

#[derive(Clone, PartialEq, prost::Message)]
struct Header {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    node: Bytes,
    #[prost(string, tag = "6")]
    domain: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Invocation {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    node: Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    actor: Bytes,
    #[prost(bytes = "bytes", tag = "4")]
    row: Bytes,
    #[prost(bytes = "bytes", tag = "5")]
    intent: Bytes,
    #[prost(string, tag = "6")]
    domain: String,
}

/// One owning-node metadata publication under its original source intent.
/// It carries no credential, directory admission or file access authority.
#[derive(Clone, Debug)]
pub struct SessionPublication {
    /// Fully qualified member root.
    pub actor: ActorReference,
    /// Changed row, including removal.
    pub row: SessionRow,
    /// Original metadata or membership operation's identity.
    pub intent: IdempotencyKey,
}

impl SessionPublication {
    /// Canonical account directory, shared by every node and deployment.
    pub fn directory(account: &AccountId) -> Result<StreamPath, Error> {
        StreamPath::new(format!(
            "registry/{}/sessions",
            acyclic_auth::encode_component(account.as_bytes())
        ))
        .map_err(Error::Stream)
    }

    /// One fully qualified root's row. Namespace identities cannot collide.
    pub fn path(&self) -> Result<StreamPath, Error> {
        Self::row_path(&self.actor)
    }

    /// Canonical row location without loading or constructing public metadata.
    pub fn row_path(actor: &ActorReference) -> Result<StreamPath, Error> {
        StreamPath::new(format!(
            "{}/{}",
            Self::directory(actor.account())?,
            acyclic_auth::encode_component(&actor.encode())
        ))
        .map_err(Error::Stream)
    }

    /// Original source row and its monotonic revision fence. Absence and no-op
    /// observations must be fenced too: a concurrent membership change may
    /// otherwise publish a row between observation and the caller's commit.
    pub(crate) async fn observe<P: StreamProvider + ?Sized>(
        provider: &P,
        actor: &ActorReference,
        node: &route::NodeId,
    ) -> Result<(Option<SessionRow>, CommitCondition), Error> {
        let path = Self::row_path(actor)?;
        let row = match provider.head(path.clone()).await {
            Ok(head) => {
                let record = head.last.ok_or(Error::Record("session row is empty"))?;
                let row = SessionRow::from_record(&path, head.tail, &record)?;
                if &row.node != node {
                    return Err(Error::Record("session row ownership or revision differs"));
                }
                Some(row)
            }
            Err(error) if error.category() == StreamError::NotFound => None,
            Err(error) => return Err(Error::Stream(error)),
        };
        let expected = row.as_ref().map_or(0, |row| row.rev);
        Ok((row, CommitCondition::Tail { path, expected }))
    }

    fn key(&self) -> Result<IdempotencyKey, Error> {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.session-presence.v1");
        for part in [self.actor.encode().as_slice(), self.intent.as_bytes()] {
            hash.update(&(part.len() as u64).to_le_bytes());
            hash.update(part);
        }
        IdempotencyKey::new(hash.finalize().as_bytes().to_vec()).map_err(Error::Stream)
    }

    /// Join the revision-fenced row to the original source publication. Its
    /// provenance also lets a single-node account recover that exact record.
    pub fn effect(&self) -> Result<CommitEffect, Error> {
        let mut effect = self.row.effect(self.path()?)?;
        let [CommitMutation::Append { records, .. }] = effect.mutations.as_mut_slice() else {
            return Err(Error::Record("session publication has no row"));
        };
        let record = &mut records[0];
        let mut attributes: Vec<_> = record
            .headers
            .attributes()
            .iter()
            .map(|attribute| attribute.to_owned())
            .collect();
        let index = attributes.partition_point(|attribute| attribute.name.as_str() < ORIGIN);
        attributes.insert(
            index,
            Attribute {
                name: ORIGIN.into(),
                value: OwnedBytes::copy_from_slice(self.key()?.as_bytes()),
            },
        );
        record.headers = RecordHeaders::new(record.headers.keys().to_vec(), attributes)
            .map_err(Error::Stream)?;
        Ok(effect)
    }

    fn invocation(&self) -> Result<Bytes, Error> {
        Ok(Invocation {
            version: 1,
            node: Bytes::copy_from_slice(self.row.node.as_bytes()),
            actor: self.actor.encode().into(),
            row: serde_json::to_vec(&self.row)
                .map_err(|_| Error::Record("session row cannot encode"))?
                .into(),
            intent: Bytes::copy_from_slice(self.intent.as_bytes()),
            domain: PROTOCOL.into(),
        }
        .encode_to_vec()
        .into())
    }

    fn call(&self) -> Result<route::CallPublication, Error> {
        Ok(route::CallPublication {
            node: self.row.node.clone(),
            invocation: self.invocation()?,
            reply: Bytes::copy_from_slice(self.key()?.as_bytes()),
        })
    }

    /// The row and its account publication share the source's original commit.
    /// Root registration joins first, so forwarding cannot overtake placement.
    pub fn effects(&self, paths: &Paths) -> Result<[CommitEffect; 2], Error> {
        Ok([
            self.effect()?,
            self.call()?
                .effect(paths)
                .map_err(super::super::routing::error)?,
        ])
    }

    /// Use the account's existing authenticated session and caller reply budget.
    /// The host retains this same value across uncertain completion.
    pub async fn publish(
        &self,
        client: &Client,
        credential: u32,
        maximum_reply_bytes: Option<usize>,
    ) -> Result<(), Error> {
        self.call()?
            .publish(client, credential, maximum_reply_bytes)
            .await
            .map_err(super::super::routing::error)
    }
}

/// Registry calls composed with the host's ordinary products on one endpoint.
/// The account stores rows and placement, never a remote session's actor state.
pub struct SessionRegistry<P: StreamProvider + ?Sized> {
    account: AccountId,
    provider: Arc<P>,
    placements: Paths,
    products: Arc<dyn CallProvider>,
}

impl<P: StreamProvider + ?Sized> SessionRegistry<P> {
    /// Bind the account's original provider, immutable placement and products.
    pub fn new(
        account: AccountId,
        provider: Arc<P>,
        placements: Paths,
        products: Arc<dyn CallProvider>,
    ) -> Self {
        Self {
            account,
            provider,
            placements,
            products,
        }
    }

    fn handles(invocation: &[u8]) -> bool {
        Header::decode(invocation).is_ok_and(|header| header.domain == PROTOCOL)
    }
}

impl<P: StreamProvider + ?Sized> CallProvider for SessionRegistry<P> {
    fn target(&self, invocation: &[u8]) -> Result<CallTarget, StreamError> {
        let header = match Header::decode(invocation) {
            Ok(header) if header.domain == PROTOCOL => header,
            _ => return self.products.target(invocation),
        };
        if header.version != 1 {
            return Err(StreamError::InvalidArgument);
        }
        let node = route::NodeId::new(&header.node).map_err(|_| StreamError::InvalidArgument)?;
        CallTarget::new(
            route::placement_resource(&self.account, &node),
            "actors/invoke",
        )
    }

    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        if !Self::handles(&invocation) {
            return self.products.call(invocation, authorization);
        }
        let account = self.account.clone();
        let provider = self.provider.clone();
        let placements = self.placements.clone();
        Box::pin(async move {
            let wire = Invocation::decode(invocation).map_err(|_| StreamError::InvalidArgument)?;
            if wire.version != 1 || wire.domain != PROTOCOL {
                return Err(StreamError::InvalidArgument);
            }
            let publication = SessionPublication {
                actor: ActorReference::decode(&wire.actor)
                    .map_err(|_| StreamError::InvalidArgument)?,
                row: SessionRow::decode(&wire.row).map_err(|_| StreamError::InvalidArgument)?,
                intent: IdempotencyKey::new(wire.intent.to_vec())?,
            };
            if publication.actor.account() != &account
                || publication.row.node.as_bytes() != wire.node.as_ref()
            {
                return Err(StreamError::AccessDenied);
            }
            let resource = route::placement_resource(&account, &publication.row.node);
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
            let placement = Placement::read(provider.as_ref(), &placements, &publication.actor)
                .await
                .map_err(super::super::routing::error)
                .map_err(stream_error)?;
            if !placement.is_some_and(|placement| placement.node == publication.row.node) {
                return Err(StreamError::AccessDenied);
            }
            let path = publication.path().map_err(stream_error)?;
            let key = publication.key().map_err(stream_error)?;
            let sequence = publication
                .row
                .rev
                .checked_sub(1)
                .ok_or(StreamError::InvalidArgument)?;
            if let Some(record) = provider.record_at(path.clone(), sequence).await? {
                if SessionRow::from_record(&path, publication.row.rev, &record)
                    .map_err(stream_error)?
                    != publication.row
                    || !record.headers.attributes().iter().any(|attribute| {
                        attribute.name == ORIGIN && attribute.value.as_ref() == key.as_bytes()
                    })
                {
                    return Err(StreamError::AccessDenied);
                }
            } else {
                authorize()?;
                let outcome = provider
                    .commit(CommitRequest {
                        deadline_unix_millis: None,
                        idempotency_key: key.clone(),
                        effects: vec![publication.effect().map_err(stream_error)?],
                    })
                    .await?;
                if !matches!(outcome, CommitOutcome::Committed(_)) {
                    return Err(StreamError::InvalidArgument);
                }
            }
            authorize()?;
            Ok(Bytes::copy_from_slice(key.as_bytes()))
        })
    }

    fn call_input(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
        input: CallInput,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        if !Self::handles(&invocation) {
            return self.products.call_input(invocation, authorization, input);
        }
        Box::pin(async { Err(StreamError::Unsupported) })
    }
}

fn stream_error(error: Error) -> StreamError {
    match error {
        Error::Stream(error) => error,
        _ => StreamError::InvalidArgument,
    }
}
