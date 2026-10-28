//! Invocation uses the existing input, checkpoint publication and stream replay.
use super::{AppliedCut, Error, Snapshot, State, Store, covers};
use crate::{
    guest::{Effect, Trigger},
    invocation::Invocation,
};
use acyclic_stream::{
    CommitCondition, CommitId, CommitMutation, CommitOutcome, CommittedMutation, FollowRequest,
    IdempotencyKey, ReadRequest, Record, Revision, RevisionCut, StreamError, StreamPath,
    StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, prost::Message)]
struct ReplyWire {
    #[prost(bytes = "bytes", tag = "1")]
    request_digest: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    input_commit: Bytes,
    #[prost(uint64, tag = "3")]
    input_sequence: u64,
    #[prost(uint64, tag = "4")]
    revision: u64,
    #[prost(bytes = "bytes", tag = "5")]
    response: Bytes,
}

/// Original committed input, retained while waiting or reconciling a reply.
/// Dropping this ticket does not cancel an already committed operation.
#[derive(Clone, Debug)]
pub struct InvocationTicket {
    invocation: Invocation,
    state: StreamPath,
    input: StreamPath,
    record: Record,
    revision: Revision,
}

// Publication identity survives physical reopen; a storage read floor is
// re-issued in the provider's current incarnation on keyed reconciliation.
impl PartialEq for InvocationTicket {
    fn eq(&self, other: &Self) -> bool {
        self.invocation == other.invocation
            && self.state == other.state
            && self.input == other.input
            && self.record == other.record
    }
}
impl Eq for InvocationTicket {}

impl InvocationTicket {
    /// Exact operation identity and request bytes.
    pub const fn invocation(&self) -> &Invocation {
        &self.invocation
    }
    /// Original immutable input, including its sequence and publication anchor.
    pub const fn record(&self) -> &Record {
        &self.record
    }
    /// The provider-issued publication revision, usable as a read floor.
    pub const fn storage_revision(&self) -> &Revision {
        &self.revision
    }
}

/// Original co-committed actor state and reply, with the original input.
/// This receipt observes a publication; it conveys no execution authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvocationReceipt {
    ticket: InvocationTicket,
    state: Record,
    reply: Record,
}

impl InvocationReceipt {
    /// Original accepted operation and input.
    pub const fn ticket(&self) -> &InvocationTicket {
        &self.ticket
    }
    /// Exact state publication, rather than a later state observation.
    pub const fn state(&self) -> &Record {
        &self.state
    }
    /// Exact response publication in the same original commit.
    pub const fn reply(&self) -> &Record {
        &self.reply
    }
    /// Shared original state/reply publication identity.
    pub const fn commit_id(&self) -> CommitId {
        self.reply.commit_id
    }
    /// Exclusive actor state revision, including control publications.
    pub const fn revision(&self) -> u64 {
        self.state.sequence + 1
    }
}

/// Result of one durably committed invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedInvocation {
    response: Bytes,
    receipt: InvocationReceipt,
}

impl CommittedInvocation {
    /// Original handler response, including an empty response.
    pub const fn response(&self) -> &Bytes {
        &self.response
    }
    /// Original publication evidence and committed revision.
    pub const fn receipt(&self) -> &InvocationReceipt {
        &self.receipt
    }
}

fn reply_path(state: &StreamPath, invocation: &Invocation) -> Result<StreamPath, Error> {
    Ok(StreamPath::new(format!(
        "{state}/replies/{}",
        acyclic_auth::encode_component(&invocation.identity(state.as_str()))
    ))?)
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// Append one keyed inbox operation and wait for its atomic reply. A retry
    /// with the identical operation and bytes reconciles the original result;
    /// different bytes under that identity fail with IdempotencyMismatch.
    /// This never drives a handler, so paused actors retain queued requests.
    pub async fn invoke(&self, invocation: &Invocation) -> Result<CommittedInvocation, Error> {
        let ticket = self.enqueue_invoke(invocation).await?;
        self.wait_invocation(ticket).await
    }

    /// Enqueue without waiting. Uncertain transport failures are retried with
    /// the identical Invocation, whose key and complete request stay stable.
    pub async fn enqueue_invoke(&self, invocation: &Invocation) -> Result<InvocationTicket, Error> {
        let snapshot = self.load(invocation.actor()).await?;
        let input = snapshot
            .state
            .as_ref()
            .and_then(|state| state.subscriptions.get(invocation.subscription()))
            .ok_or(Error::Invalid("invocation subscription is missing"))?
            .path
            .clone();
        let mut publication = super::PreparedPublish::foreign(IdempotencyKey::new(
            invocation.identity(snapshot.path.as_str()).to_vec(),
        )?);
        publication.join(Effect {
            conditions: Vec::new(),
            mutation: CommitMutation::Append {
                path: input.clone(),
                records: vec![invocation.record(&snapshot.path)?],
            },
        })?;
        publication.join(self.activation(invocation.actor()))?;
        let mut request = publication.into_request()?;
        let binding = acyclic_stream::request::commit_binding_with(&mut request, |_| {
            Ok::<(), StreamError>(())
        })??;
        let CommitOutcome::Committed(receipt) = self.provider.commit(request).await? else {
            return Err(Error::Conflict);
        };
        // Compact receipts carry no authenticated bodies. Recover the original
        // immutable fact and join it to the canonical request, never caller bytes.
        let envelope = self.provider.read_commit(receipt.commit_id).await?;
        if envelope.commit_id != receipt.commit_id
            || envelope.committed_at_micros != receipt.committed_at_micros
            || !binding.matches_with(&envelope, |_| Ok::<(), StreamError>(()))??
            || !receipt.forks.is_empty()
            || !receipt
                .appends
                .iter()
                .map(|range| (&range.path, range.start, range.tail))
                .eq(envelope
                    .mutations
                    .iter()
                    .filter_map(|mutation| match mutation {
                        CommittedMutation::Append(append) => {
                            Some((&append.path, append.start, append.tail))
                        }
                        CommittedMutation::Fork(_) => None,
                    }))
        {
            return Err(Error::Invalid("invocation receipt changes its input"));
        }
        let record = envelope
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                CommittedMutation::Append(append) if append.path == input => append.records.first(),
                _ => None,
            })
            .ok_or(Error::Invalid("invocation receipt omits its input"))?;
        Ok(InvocationTicket {
            invocation: invocation.clone(),
            state: snapshot.path,
            input,
            record: record.clone(),
            revision: receipt.revision,
        })
    }

    /// Observe the original reply after its explicit committed input cut. The
    /// state follow registers and rechecks through wait_applied; no polling,
    /// scheduler, input replay or second journal is introduced here.
    pub async fn wait_invocation(
        &self,
        ticket: InvocationTicket,
    ) -> Result<CommittedInvocation, Error> {
        if self.path(ticket.invocation.actor())? != ticket.state {
            return Err(Error::Conflict);
        }
        let cut = AppliedCut::through(
            ticket.invocation.subscription().into(),
            ticket.input.clone(),
            &ticket.record,
        )?
        .at_least(RevisionCut::of(ticket.revision.clone()));
        let applied = self.wait_applied(ticket.invocation.actor(), cut).await?;
        // Consumption and reply creation share a transaction, so this path
        // exists once the cut is applied; a missing path is a protocol failure.
        let reply = acyclic_stream::records_only(
            self.provider
                .follow(FollowRequest {
                    at_least: RevisionCut::of(ticket.revision.clone()),
                    ..FollowRequest::new(reply_path(&ticket.state, &ticket.invocation)?, 0)
                })
                .await?,
        )
        .try_next()
        .await?
        .ok_or(StreamError::Unavailable)?;
        let wire = ReplyWire::decode(reply.value.as_ref())
            .map_err(|_| Error::Invalid("invalid invocation reply"))?;
        if wire.encode_to_vec().as_slice() != reply.value.as_ref()
            || reply.sequence != 0
            || wire.request_digest.as_ref() != Sha256::digest(&ticket.record.value).as_slice()
            || wire.input_commit.as_ref() != ticket.record.commit_id.as_bytes()
            || wire.input_sequence != ticket.record.sequence
            || wire.revision == 0
            || wire.revision > applied.revision()
        {
            return Err(Error::Invalid(
                "invocation reply changes its input or revision",
            ));
        }
        let state = self
            .provider
            .read(ReadRequest {
                path: ticket.state.clone(),
                from: wire.revision - 1,
                limit: 1,
                at_least: RevisionCut::of(ticket.revision.clone()),
            })
            .await?
            .try_next()
            .await?
            .ok_or(Error::Invalid("invocation state receipt is missing"))?;
        if state.sequence.checked_add(1) != Some(wire.revision)
            || state.commit_id != reply.commit_id
            || state.committed_at_micros != reply.committed_at_micros
            || State::decode(&state.value)?
                .subscriptions
                .get(ticket.invocation.subscription())
                .filter(|subscription| subscription.path == ticket.input)
                .is_none_or(|subscription| subscription.cursor <= ticket.record.sequence)
        {
            return Err(Error::Invalid(
                "invocation reply is not co-committed with consumed input",
            ));
        }
        Ok(CommittedInvocation {
            response: wire.response,
            receipt: InvocationReceipt {
                ticket,
                state,
                reply,
            },
        })
    }

    pub(super) fn invocation_reply(
        &self,
        previous: &Snapshot,
        trigger: &Trigger,
        delivered: Option<&Record>,
        response: Option<&Bytes>,
        effects: &[Effect],
    ) -> Result<Option<(StreamPath, Effect)>, Error> {
        let invocation = delivered
            .map(Invocation::decode_bound)
            .transpose()?
            .flatten();
        let Some((state_path, invocation)) = invocation else {
            return if response.is_none() {
                Ok(None)
            } else {
                Err(Error::Invalid("response without an invocation"))
            };
        };
        let record = delivered.ok_or(Error::Invalid("invocation input is missing"))?;
        if state_path != previous.path
            || self.actor(&previous.path)? != *invocation.actor()
            || !matches!(trigger, Trigger::Record { subscription, sequence } if subscription == invocation.subscription() && *sequence == record.sequence)
        {
            return if response.is_none() {
                Ok(None)
            } else {
                Err(Error::Invalid(
                    "invocation input targets another actor or subscription",
                ))
            };
        }
        let path = reply_path(&previous.path, &invocation)?;
        if effects.iter().any(|effect| match &effect.mutation {
            CommitMutation::Append { path: target, .. } => target == &path,
            CommitMutation::Fork { destination, .. } => covers(destination, &path),
        }) {
            return Err(Error::Invalid(
                "guest effect targets the host's invocation reply",
            ));
        }
        let value = ReplyWire {
            request_digest: Sha256::digest(&record.value).to_vec().into(),
            input_commit: Bytes::copy_from_slice(record.commit_id.as_bytes()),
            input_sequence: record.sequence,
            revision: previous
                .tail
                .checked_add(1)
                .ok_or(Error::Invalid("invocation revision overflow"))?,
            response: response
                .ok_or(Error::Invalid("invocation has no response"))?
                .clone(),
        }
        .encode_to_vec();
        Ok(Some((
            path.clone(),
            Effect {
                conditions: vec![CommitCondition::Absent { path: path.clone() }],
                mutation: CommitMutation::Append {
                    path,
                    records: vec![value.into()],
                },
            },
        )))
    }
}
