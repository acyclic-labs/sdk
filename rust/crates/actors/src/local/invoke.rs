//! Invocation uses the existing input, checkpoint publication and stream replay.
use super::{Error, Snapshot, State, Store};
use crate::{
    call::{self, Entry, Journal, Outcome},
    guest::{GuestError, Trigger},
    invocation::Invocation,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    Attribute, CommitEffect, CommitId, CommitMutation, CommittedMutation, FollowRequest,
    IdempotencyKey, OwnedBytes, ReadRequest, Record, Revision, RevisionCut, StreamError,
    StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, prost::Message)]
struct CompletionBinding {
    #[prost(bytes = "bytes", tag = "1")]
    request_digest: Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    input_commit: Bytes,
    #[prost(uint64, tag = "3")]
    input_sequence: u64,
    #[prost(uint64, tag = "4")]
    revision: u64,
}

const COMPLETION: &str = "acyclic.actors.completion.v1";

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
    /// Shared call identity derived from this ticket's original admitted state
    /// path. Use its cancellation commit without resolving the target again.
    /// This performs no provider I/O and conveys no additional authority.
    pub fn call(&self) -> Result<call::CallPath, call::Error> {
        self.invocation.call(&self.state)
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
    terminal: Record,
    call: call::Receipt,
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
    pub const fn terminal(&self) -> &Record {
        &self.terminal
    }
    /// The shared call protocol's original terminal receipt.
    pub const fn call(&self) -> &call::Receipt {
        &self.call
    }
    /// Shared original state/reply publication identity.
    pub const fn commit_id(&self) -> CommitId {
        self.terminal.commit_id
    }
    /// Exclusive actor state revision, including control publications.
    pub const fn revision(&self) -> u64 {
        self.state.sequence + 1
    }
}

/// Result of one durably committed invocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedInvocation {
    response: OwnedBytes,
    receipt: InvocationReceipt,
}

impl CommittedInvocation {
    /// Original handler response, including an empty response.
    pub const fn response(&self) -> &OwnedBytes {
        &self.response
    }
    /// Original publication evidence and committed revision.
    pub const fn receipt(&self) -> &InvocationReceipt {
        &self.receipt
    }
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// Retire a cancelled inline delivery without adopting any guest writes.
    /// The original state anchor and call marker authorize this one consume.
    pub(super) fn prepare_settled_input(
        &self,
        previous: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        call: &call::CallPath,
        key: IdempotencyKey,
    ) -> Result<super::PreparedPublish, Error> {
        let original = super::owned(previous, epoch)?;
        let mut state = original.clone();
        super::consume(&mut state, &original.alarm, trigger)?;
        let mut publication = self.prepare(previous, state, Vec::new(), key)?;
        publication.require(vec![acyclic_stream::CommitCondition::Present {
            path: call.settled().clone(),
        }])?;
        Ok(publication)
    }

    /// Append one keyed inbox operation and wait for its atomic reply. A retry
    /// with the identical operation and bytes reconciles the original result;
    /// different bytes under that identity fail with IdempotencyMismatch.
    /// This never drives a handler, so paused actors retain queued requests.
    pub async fn invoke(&self, invocation: &Invocation) -> Result<CommittedInvocation, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.invoke",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let ticket = self.enqueue_invoke(invocation).await?;
                self.wait_invocation(ticket).await
            }
        )
    }

    /// Enqueue without waiting. Uncertain transport failures are retried with
    /// the identical Invocation, whose key and complete request stay stable.
    pub async fn enqueue_invoke(&self, invocation: &Invocation) -> Result<InvocationTicket, Error> {
        self.enqueue_joined(invocation, |_| Ok(Vec::new())).await
    }

    /// Publish an invocation and companion effects under its original identity.
    pub(super) async fn enqueue_joined(
        &self,
        invocation: &Invocation,
        joined: impl FnOnce(&StreamPath) -> Result<Vec<CommitEffect>, Error>,
    ) -> Result<InvocationTicket, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.enqueue_invoke",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
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
                publication.join(acyclic_stream::CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: input.clone(),
                        records: vec![invocation.record(&snapshot.path)?],
                    }],
                ))?;
                publication.join(self.activation(invocation.actor()))?;
                for effect in joined(&input)? {
                    publication.join(effect)?;
                }
                let mut request = publication.into_request()?;
                let binding = acyclic_stream::request::commit_binding_with(&mut request, |_| {
                    Ok::<(), StreamError>(())
                })??;
                let acyclic_stream::AcceptedCommitOutcome::Committed { receipt, envelope } =
                    self.provider.commit_accepted(request).await?
                else {
                    return Err(Error::Conflict);
                };
                // Both views come from the original acceptance or exact-key replay.
                // Join that fact to the request, never reconstructing caller bodies.
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
                        CommittedMutation::Append(append) if append.path == input => {
                            append.records.first()
                        }
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
        )
    }

    /// Observe the original reply after its explicit committed input cut.
    /// A committed guest failure returns [`Error::InvocationFailed`] with its
    /// original typed cause and receipt. The original reply follow waits for
    /// creation; its co-committed state proves consumption of the accepted
    /// input. No current-state probe, polling or input replay is needed.
    pub async fn wait_invocation(
        &self,
        ticket: InvocationTicket,
    ) -> Result<CommittedInvocation, Error> {
        acyclic_observability::async_operation!(
            "acyclic.actors.wait_invocation",
            Error::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if self.path(ticket.invocation.actor())? != ticket.state {
                    return Err(Error::Conflict);
                }
                // The private ticket retains the provider's original accepted
                // input. Wait on that operation's reply, whose original state
                // publication below proves its exact consumed cursor. A later
                // actor state is neither needed nor evidence of this result.
                let path = ticket.call()?;
                let mut records = acyclic_stream::records_only(
                    self.provider
                        .follow(FollowRequest {
                            at_least: RevisionCut::of(ticket.revision.clone()),
                            when_absent: acyclic_stream::WhenAbsent::Wait,
                            ..FollowRequest::new(path.log().clone(), 0)
                        })
                        .await?,
                );
                let mut journal = Journal::default();
                let (terminal, outcome) = loop {
                    let record = records.try_next().await?.ok_or(StreamError::Unavailable)?;
                    if let Entry::Terminal(outcome) = journal.apply(&record)? {
                        break (record, outcome);
                    }
                };
                let call = journal
                    .receipt(path.log())
                    .ok_or(Error::Invalid("call terminal receipt is missing"))?;
                if matches!(outcome, Outcome::Cancelled(_) | Outcome::Indeterminate(_)) {
                    return Err(Error::InvocationTerminated {
                        receipt: Box::new(call),
                    });
                }
                let attributes = terminal.headers.attributes();
                let binding = attributes
                    .binary_search_by(|attribute| attribute.name.cmp(COMPLETION))
                    .ok()
                    .and_then(|index| attributes.get(index))
                    .ok_or(Error::Invalid("invocation completion binding is missing"))?
                    .value;
                let wire = CompletionBinding::decode(binding.as_ref())
                    .map_err(|_| Error::Invalid("invalid invocation completion binding"))?;
                if wire.encode_to_vec().as_slice() != binding.as_ref()
                    || wire.request_digest.as_ref()
                        != Sha256::digest(&ticket.record.value).as_slice()
                    || wire.input_commit.as_ref() != ticket.record.commit_id.as_bytes()
                    || wire.input_sequence != ticket.record.sequence
                    || wire.revision == 0
                {
                    return Err(Error::Invalid(
                        "invocation reply changes its input or revision",
                    ));
                }
                let state = self
                    .provider
                    .read(ReadRequest {
                        key: None,
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
                    || state.commit_id != terminal.commit_id
                    || state.committed_at_micros != terminal.committed_at_micros
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
                let receipt = InvocationReceipt {
                    ticket,
                    state,
                    terminal,
                    call,
                };
                match outcome {
                    Outcome::Error(failure) => {
                        let source: GuestError = serde_json::from_str(&failure)
                            .map_err(|_| Error::Invalid("invalid invocation failure"))?;
                        if serde_json::to_string(&source)
                            .map_err(|_| Error::Invalid("invalid invocation failure"))?
                            != failure
                        {
                            return Err(Error::Invalid("non-canonical invocation failure"));
                        }
                        Err(Error::InvocationFailed {
                            source,
                            receipt: Box::new(receipt),
                        })
                    }
                    Outcome::Ok(response) => Ok(CommittedInvocation { response, receipt }),
                    Outcome::Cancelled(_) | Outcome::Indeterminate(_) => {
                        Err(Error::InvocationTerminated {
                            receipt: Box::new(receipt.call),
                        })
                    }
                }
            }
        )
    }

    /// A failed invocation consumes only its original input and retains every
    /// previously committed guest value. No failed-attempt effects can escape.
    pub(super) fn prepare_invocation_failure(
        &self,
        previous: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        delivered: Option<&Record>,
        failure: &GuestError,
        key: IdempotencyKey,
    ) -> Result<Option<super::PreparedPublish>, Error> {
        let Some((call, effects)) =
            self.invocation_completion(previous, trigger, delivered, Some(Err(failure)), &[])?
        else {
            return Ok(None);
        };
        let original = super::owned(previous, epoch)?;
        let mut state = original.clone();
        super::consume(&mut state, &original.alarm, trigger)?;
        let mut prepared = self.prepare(previous, state, effects, key)?;
        prepared.call = Some((call, trigger.clone()));
        Ok(Some(prepared))
    }

    pub(super) fn invocation_completion(
        &self,
        previous: &Snapshot,
        trigger: &Trigger,
        delivered: Option<&Record>,
        outcome: Option<Result<&Bytes, &GuestError>>,
        effects: &[CommitEffect],
    ) -> Result<Option<(call::CallPath, Vec<CommitEffect>)>, Error> {
        let invocation = delivered
            .map(Invocation::decode_bound)
            .transpose()?
            .flatten();
        let Some((state_path, invocation)) = invocation else {
            return if !matches!(outcome, Some(Ok(_))) {
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
            return if !matches!(outcome, Some(Ok(_))) {
                Ok(None)
            } else {
                Err(Error::Invalid(
                    "invocation input targets another actor or subscription",
                ))
            };
        }
        let call = invocation.call(&previous.path)?;
        if effects
            .iter()
            .flat_map(|effect| &effect.mutations)
            .any(|mutation| {
                [call.log(), call.settled()]
                    .into_iter()
                    .any(|path| super::mutation_targets(mutation, path))
            })
        {
            return Err(Error::Invalid(
                "guest effect targets the host's invocation reply",
            ));
        }
        let outcome = match outcome.ok_or(Error::Invalid("invocation has no response"))? {
            Ok(response) => Outcome::Ok(response.clone().into()),
            Err(failure) => Outcome::Error(
                serde_json::to_string(failure)
                    .map_err(|_| Error::Invalid("invocation failure does not encode"))?,
            ),
        };
        let binding = CompletionBinding {
            request_digest: Sha256::digest(&record.value).to_vec().into(),
            input_commit: Bytes::copy_from_slice(record.commit_id.as_bytes()),
            input_sequence: record.sequence,
            revision: previous
                .tail
                .checked_add(1)
                .ok_or(Error::Invalid("invocation revision overflow"))?,
        }
        .encode_to_vec();
        // Inline computation gains authority only at this step commit. Its
        // result, consumed input and state are sealed with the one call terminal.
        let completion = call.settle_with_attributes(
            None,
            &outcome,
            [Attribute {
                name: COMPLETION.into(),
                value: binding.into(),
            }],
        )?;
        Ok(Some((call, vec![completion.into()])))
    }
}
