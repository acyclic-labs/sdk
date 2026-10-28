//! Committed invocation is an ordinary inbox delivery with an atomic reply.
//! These bytes pass through the same Handler and strict guest wire as records.
use crate::{
    addressing::ActorReference,
    guest::{Delivery, GuestError, Trigger},
};
use acyclic_stream::{AppendRecord, Attribute, Record, RecordHeaders, StreamPath};
use bytes::Bytes;
use prost::Message;
use sha2::{Digest, Sha256};

const HEADER: &str = "acyclic.actors.invoke";
const VERSION: &[u8] = b"1";

#[derive(Clone, PartialEq, prost::Message)]
struct RequestWire {
    #[prost(bytes = "bytes", tag = "1")]
    actor: Bytes,
    #[prost(string, tag = "2")]
    subscription: String,
    #[prost(bytes = "bytes", tag = "3")]
    operation: Bytes,
    #[prost(bytes = "bytes", tag = "4")]
    request: Bytes,
    #[prost(string, tag = "5")]
    state_path: String,
}

/// Exact operation identity and request bytes admitted to one actor's inbox.
/// It carries no execution or storage authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invocation {
    actor: ActorReference,
    subscription: String,
    operation: Bytes,
    request: Bytes,
}

impl Invocation {
    /// Bind a nonempty operation identity to the actor, subscription and bytes.
    pub fn new(
        actor: ActorReference,
        subscription: String,
        operation: Bytes,
        request: Bytes,
    ) -> Result<Self, GuestError> {
        if subscription.is_empty() || operation.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            actor,
            subscription,
            operation,
            request,
        })
    }

    /// Complete target actor.
    pub const fn actor(&self) -> &ActorReference {
        &self.actor
    }
    /// Instance-bound manifest subscription.
    pub fn subscription(&self) -> &str {
        &self.subscription
    }
    /// Original caller-owned operation identity.
    pub const fn operation(&self) -> &Bytes {
        &self.operation
    }
    /// Original application request, including an empty request.
    pub const fn request(&self) -> &Bytes {
        &self.request
    }

    /// Canonical inbox record bound to the admitting Store's original state
    /// path. Ordinary records have no invocation marker.
    pub fn record(&self, state_path: &StreamPath) -> Result<AppendRecord, GuestError> {
        Ok(AppendRecord {
            value: RequestWire {
                actor: self.actor.encode().into(),
                subscription: self.subscription.clone(),
                operation: self.operation.clone(),
                request: self.request.clone(),
                state_path: state_path.as_str().into(),
            }
            .encode_to_vec()
            .into(),
            headers: RecordHeaders::new(
                Vec::new(),
                vec![Attribute {
                    name: HEADER.into(),
                    value: Bytes::from_static(VERSION).into(),
                }],
            )
            .map_err(|_| invalid())?,
        })
    }

    /// Decode only a marked canonical request; malformed marked records fail.
    /// Delivery additionally checks its original state-path binding.
    pub fn decode(record: &Record) -> Result<Option<Self>, GuestError> {
        Ok(Self::decode_bound(record)?.map(|(_, invocation)| invocation))
    }

    pub(crate) fn decode_bound(record: &Record) -> Result<Option<(StreamPath, Self)>, GuestError> {
        let attributes = record.headers.attributes();
        let Ok(index) = attributes.binary_search_by(|attribute| attribute.name.cmp(HEADER)) else {
            return Ok(None);
        };
        if attributes.get(index).ok_or_else(invalid)?.value.as_ref() != VERSION {
            return Err(invalid());
        }
        let wire = RequestWire::decode(record.value.as_ref()).map_err(|_| invalid())?;
        if wire.encode_to_vec().as_slice() != record.value.as_ref() {
            return Err(invalid());
        }
        let path = StreamPath::new(wire.state_path).map_err(|_| invalid())?;
        let invocation = Self::new(
            ActorReference::decode(&wire.actor).map_err(|_| invalid())?,
            wire.subscription,
            wire.operation,
            wire.request,
        )?;
        Ok(Some((path, invocation)))
    }

    pub(crate) fn identity(&self, state_path: &str) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"acyclic.actors.invoke.v1");
        let actor = self.actor.encode();
        for part in [
            state_path.as_bytes(),
            actor.as_slice(),
            self.operation.as_ref(),
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        hash.finalize().into()
    }
}

impl Delivery {
    /// Read the original committed invocation without replacing its input record.
    /// Alarms, ordinary records and invocations for another root/actor/subscription
    /// return `None`. Shared inputs still retain their original marked record.
    pub fn invocation(&self) -> Result<Option<Invocation>, GuestError> {
        let Some(record) = &self.record else {
            return Ok(None);
        };
        let Some((state_path, invocation)) = Invocation::decode_bound(record)? else {
            return Ok(None);
        };
        match &self.trigger {
            Trigger::Record {
                subscription,
                sequence,
            } if *sequence == record.sequence => Ok((invocation.actor == self.actor
                && invocation.subscription == *subscription
                && state_path == *self.subscriptions.state_path())
            .then_some(invocation)),
            _ => Err(invalid()),
        }
    }
}

fn invalid() -> GuestError {
    GuestError::Decode("invalid committed invocation".into())
}
