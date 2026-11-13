//! Admission outcomes share the original relay publication and retry identity.
use super::{Envelope, Error, NodeId, Paths};
use acyclic_stream::{
    AppendRecord, Attribute, CommitEffect, CommitMutation, CommitOutcome, CommitRequest,
    CommittedMutation, IdempotencyKey, IdempotencyOutcome, RecordHeaders, StreamError,
    StreamProvider, StreamProviderExt as _,
};
use bytes::Bytes;
use prost::Message as _;
use sha2::{Digest as _, Sha256};

const ADMISSION: &str = "acyclic.actors.route.admission.v1";

/// A successful ordinary delivery on the actor's owning node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Delivered {
    /// Original inbox sequence, unchanged by exact retries.
    pub inbox: u64,
}

/// A definitive recipient refusal, retained under the original intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, prost::Enumeration)]
#[repr(i32)]
pub enum Rejection {
    /// The owning node has closed the recipient.
    Closed = 0,
    /// No actor is registered at this address.
    NoSuchActor = 1,
}

/// Location-transparent delivery result; relay admission is not inbox delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Routed {
    /// The ordinary delivery committed on this node.
    Delivered(Delivered),
    /// The account durably admitted the original request to its destination relay.
    Relayed {
        /// Original caller intent key, independent of physical relay stages.
        key: IdempotencyKey,
        /// Original relay sequence, unchanged by exact retries.
        relay_seq: u64,
    },
    /// One rejection Notice was durably routed to the sender.
    Rejected(Rejection),
}

#[derive(Clone, PartialEq, prost::Message)]
struct Binding {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    intent_digest: Bytes,
    #[prost(enumeration = "Rejection", optional, tag = "3")]
    rejection: Option<i32>,
}

/// One decoder for the original admission proof, whether recovering the
/// account commit or consuming its rejection on the sender's node.
pub(super) fn rejection_binding(
    headers: &RecordHeaders,
    original: impl FnOnce() -> Bytes,
) -> Result<Option<Rejection>, Error> {
    let attributes = headers.attributes();
    let Some(header) = attributes.iter().find(|value| value.name == ADMISSION) else {
        return Ok(None);
    };
    let binding = Binding::decode(header.value.as_ref()).map_err(|_| Error::InvalidRecord)?;
    if binding.version != 1 {
        return Err(Error::InvalidRecord);
    }
    if binding.intent_digest.as_ref() != &Sha256::digest(original())[..] {
        return Err(StreamError::IdempotencyMismatch.into());
    }
    Ok(Some(
        Rejection::try_from(binding.rejection.ok_or(Error::InvalidRecord)?)
            .map_err(|_| Error::InvalidRecord)?,
    ))
}

fn outcome(rejection: Option<Rejection>, key: IdempotencyKey, relay_seq: u64) -> Routed {
    match rejection {
        Some(reason) => Routed::Rejected(reason),
        None => Routed::Relayed { key, relay_seq },
    }
}

impl Envelope {
    /// Prepare the original opaque relay append after host authority, membership
    /// and storage admission. The stage key binds recipient/sender/intent, not
    /// a changing routing decision. Preparation grants no authority.
    pub fn relay_request(
        &self,
        paths: &Paths,
        destination: &NodeId,
    ) -> Result<CommitRequest, Error> {
        self.publication(paths, destination, self, None)
    }

    fn publication(
        &self,
        paths: &Paths,
        destination: &NodeId,
        payload: &Self,
        rejection: Option<Rejection>,
    ) -> Result<CommitRequest, Error> {
        if self.sender.account() != self.to.account()
            || payload.sender.account() != payload.to.account()
            || payload.to.account() != self.sender.account()
        {
            return Err(Error::InvalidRecord);
        }
        let original = self.encode();
        let headers = match rejection {
            None => RecordHeaders::default(),
            Some(reason) => RecordHeaders::new(
                Vec::new(),
                vec![Attribute {
                    name: ADMISSION.into(),
                    value: Binding {
                        version: 1,
                        intent_digest: Sha256::digest(&original).to_vec().into(),
                        rejection: Some(reason as i32),
                    }
                    .encode_to_vec()
                    .into(),
                }],
            )?,
        };
        Ok(CommitRequest {
            deadline_unix_millis: None,
            idempotency_key: self.relay_key(paths)?,
            effects: vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: paths.relay(destination)?,
                    records: vec![AppendRecord {
                        value: if rejection.is_some() {
                            payload.encode().into()
                        } else {
                            original.into()
                        },
                        headers,
                    }],
                }],
            )],
        })
    }

    fn relay_key(&self, paths: &Paths) -> Result<IdempotencyKey, Error> {
        let mut key = Sha256::new();
        key.update(b"acyclic.actors.route.relay.v1\0");
        for part in [
            paths.0.as_str().as_bytes(),
            self.to.account().as_bytes(),
            self.to.namespace().as_str().as_bytes(),
            self.to.id().as_bytes(),
            self.sender.namespace().as_str().as_bytes(),
            self.sender.id().as_bytes(),
            self.request.idempotency_key.as_bytes(),
        ] {
            key.update((part.len() as u64).to_le_bytes());
            key.update(part);
        }
        Ok(IdempotencyKey::new(key.finalize().to_vec())?)
    }

    /// Recover accepted work before waiting for another membership or storage
    /// admission. The original Streams envelope is the only retained outcome.
    pub(super) async fn retained_admission<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        paths: &Paths,
    ) -> Result<Option<Routed>, Error> {
        let key = self.relay_key(paths)?;
        let Some(retained) = provider.inspect_idempotency(key.clone()).await? else {
            return Ok(None);
        };
        let prefix = format!("{}/relay/", paths.0);
        if retained.idempotency_key != key {
            return Err(Error::InvalidRecord);
        }
        let IdempotencyOutcome::Commit(envelope) = retained.outcome else {
            return Err(Error::InvalidRecord);
        };
        let original_bytes = self.encode();
        let mut original = None;
        for mutation in &envelope.mutations {
            let CommittedMutation::Append(append) = mutation else {
                continue;
            };
            let Some(node) = append.path.as_str().strip_prefix(&prefix) else {
                continue;
            };
            if node.is_empty() || node.contains('/') {
                continue;
            }
            let [record] = append.records.as_slice() else {
                return Err(Error::InvalidRecord);
            };
            if original.is_some() {
                return Err(Error::InvalidRecord);
            }
            let reason = rejection_binding(&record.headers, || original_bytes.clone())?;
            if reason.is_none() && record.value.as_ref() != original_bytes.as_ref() {
                return Err(StreamError::IdempotencyMismatch.into());
            }
            if reason.is_some() && Self::decode(&record.value)?.to != self.sender {
                return Err(Error::InvalidRecord);
            }
            original = Some(outcome(
                reason,
                self.request.idempotency_key.clone(),
                append.start,
            ));
        }
        original.map(Some).ok_or(Error::InvalidRecord)
    }

    /// Publish after authenticated account-host admission. Hard membership
    /// guards and a storage debit join the same commit. Changed context recovers
    /// the original outcome through Streams, without another intent ledger.
    /// First publication and exact retries need no inspection.
    pub async fn publish_relay<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        paths: &Paths,
        destination: &NodeId,
        admission: Vec<CommitEffect>,
    ) -> Result<Routed, Error> {
        self.publish(provider, paths, destination, self, None, admission)
            .await
    }

    /// Retain a definitive refusal and route its host-constructed Notice to the
    /// original sender in one account commit. The host, never the peer caller,
    /// constructs and authorizes `notice` through its ordinary Notice policy.
    /// A later retry cannot turn this refusal into a message delivery.
    pub async fn publish_rejection<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        paths: &Paths,
        reason: Rejection,
        notice: &Self,
        admission: Vec<CommitEffect>,
    ) -> Result<Routed, Error> {
        if notice.to != self.sender {
            return Err(Error::InvalidRecord);
        }
        self.publish(
            provider,
            paths,
            &self.source,
            notice,
            Some(reason),
            admission,
        )
        .await
    }

    async fn publish<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        paths: &Paths,
        destination: &NodeId,
        payload: &Self,
        rejection: Option<Rejection>,
        admission: Vec<CommitEffect>,
    ) -> Result<Routed, Error> {
        let prefix = format!("{}/relay/", paths.0);
        if admission.iter().any(|effect| effect.soft)
            || admission
                .iter()
                .flat_map(|effect| &effect.mutations)
                .any(|mutation| {
                    let path = mutation.path().as_str();
                    path.starts_with(&prefix)
                        || matches!(mutation, CommitMutation::Fork { .. })
                            && prefix
                                .strip_prefix(path)
                                .is_some_and(|suffix| suffix.starts_with('/'))
                })
        {
            return Err(Error::InvalidRecord);
        }
        let mut request = self.publication(paths, destination, payload, rejection)?;
        request.effects.extend(admission);
        match provider.commit(request).await {
            Ok(CommitOutcome::Committed(receipt)) => {
                let relay = paths.relay(destination)?;
                let append = receipt
                    .appends
                    .iter()
                    .find(|append| append.path == relay)
                    .ok_or(Error::InvalidRecord)?;
                if append.start.checked_add(1) != Some(append.tail) {
                    return Err(Error::InvalidRecord);
                }
                Ok(outcome(
                    rejection,
                    self.request.idempotency_key.clone(),
                    append.start,
                ))
            }
            Ok(CommitOutcome::Conflict(conflicts)) => {
                Err(crate::local::Error::PreconditionChanged(conflicts).into())
            }
            Err(StreamError::IdempotencyMismatch) => self
                .retained_admission(provider, paths)
                .await?
                .ok_or(Error::InvalidRecord),
            Err(error) => Err(error.into()),
        }
    }
}
